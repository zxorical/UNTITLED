/**
 * @module autoJoin/manager
 * 
 * Premium AutoJoiner - PRODUCTION GRADE - MEMORY SAFE
 * 
 * FIXED:
 * - Session start timeout handling
 * - Proper client cleanup on failure
 * - Better error logging
 * - Retry logic with exponential backoff
 * - Session key now includes guildId to prevent conflicts
 * - Token validation without leaking clients
 * - Session error tracking and throttling
 * - Fixed missing methods that were cut off
 */

import { Client, Message, TextChannel, ClientOptions, Options, NewsChannel, PartialMessage } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import axios, { AxiosInstance } from 'axios';
import http from 'http';
import https from 'https';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../logger.js';
import {
  delay,
  exponentialBackoff,
  formatError,
  truncate,
  sanitizeForLog,
  formatTimestamp,
  hasGiveawayKeyword,
} from '../utils.js';
import {
  incrementTokenEntries,
  incrementTokenWins,
  updateTokenLastUsed,
  getPremiumUser,
  setTokenActive,
  getUserWebhook,
  getAllPremiumUsersAllGuilds,
  getAutoJoinEntry,
  saveAutoJoinEntry,
  updateAutoJoinEntryStatus,
  cleanupAutoJoinEntries,
  batchSaveJoinOutcomes,
  batchUpdateDetectionConfidence,
  archiveOldGiveaways,
  saveWatchlistMatch,
  getWatchlistKeywords,
  getDetectionProfiles,
  updateDetectionProfile,
  saveQueueState,
  loadQueueState,
} from '../database.js';
import { decryptToken } from '../premium/tokenManager.js';
import { CONFIG } from '../config.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GiveawayEntry {
  _id: string;
  userId: string;
  messageId: string;
  channelId: string;
  guildId: string;
  authorId: string;
  guildName: string;
  channelName: string;
  prize: string;
  buttonCustomId?: string;
  detectedAt: number;
  endsAt?: number;
  status: 'pending' | 'queued' | 'attempting' | 'success' | 'failed' | 'skipped' | 'dead_letter';
  attempts: number;
  lastAttemptAt?: number;
  lastError?: string;
  expiresAt: number;
  correlationId: string;
  detectionConfidence: number;
  detectionReasons: string[];
  crosspostSource?: string;
}

interface AutoJoinEntry {
  _id: string;
  userId: string;
  messageId: string;
  channelId: string;
  guildId: string;
  authorId: string;
  guildName: string;
  channelName: string;
  prize: string;
  buttonCustomId?: string;
  detectedAt: number;
  endsAt?: number;
  status: 'pending' | 'queued' | 'attempting' | 'success' | 'failed' | 'skipped' | 'dead_letter';
  attempts: number;
  lastAttemptAt?: number;
  lastError?: string;
  expiresAt: number;
  correlationId?: string;
  detectionConfidence?: number;
  detectionReasons?: string[];
  crosspostSource?: string;
  queuePosition?: number;
  entryTimeMs?: number;
  archived?: boolean;
  archivedAt?: number;
}

interface GiveawayButton {
  customId: string;
  label: string;
  disabled: boolean;
}

interface UserSession {
  client: Client;
  userId: string;
  guildId: string;
  label: string;
  startedAt: number;
  isActive: boolean;
  stats: SessionStats;
  heartbeatInterval?: NodeJS.Timeout;
  reconnectAttempts: number;
  maxReconnectAttempts: number;
  rateLimiter: TokenBucket;
  listeners: {
    messageCreate?: (message: Message) => void;
    messageUpdate?: (oldMessage: Message | PartialMessage, newMessage: Message | PartialMessage) => void;
    error?: (error: Error) => void;
    disconnect?: () => void;
  };
  sessionId: string;
  destroyed: boolean;
  lastHealthCheck: number;
  stallCount: number;
}

interface SessionStats {
  detected: number;
  entered: number;
  failed: number;
  wins: number;
  falsePositives: number;
  lastEntryAt?: number;
  queueWaitTimes: number[];
}

interface DetectionProfile {
  botId: string;
  botName: string;
  detectionCount: number;
  truePositives: number;
  falsePositives: number;
  averageConfidence: number;
  lastSeen: number;
  patterns: {
    buttonPatterns: string[];
    embedPatterns: string[];
    contentPatterns: string[];
  };
}

interface DetectionResult {
  isGiveaway: boolean;
  confidence: number;
  reasons: string[];
  button?: GiveawayButton;
  prize?: string;
  profile?: DetectionProfile;
}

interface QueueItem {
  entryId: string;
  userId: string;
  guildId: string;
  channelId: string;
  messageId: string;
  priority: number;
  addedAt: number;
  endsAt?: number;
  correlationId: string;
  attempts: number;
  maxAttempts: number;
  lastError?: string;
}

interface WatchlistKeyword {
  keyword: string;
  aliases: string[];
  matchCount: number;
  lastMatched: number;
  compiled: RegExp;
}

interface GuildStats {
  guildId: string;
  guildName: string;
  detected: number;
  entered: number;
  failed: number;
  wins: number;
  falsePositives: number;
  averageConfidence: number;
  averageQueueWaitMs: number;
}

interface AccountStats {
  userId: string;
  detected: number;
  entered: number;
  failed: number;
  wins: number;
  falsePositives: number;
  averageConfidence: number;
  averageDetectionMs: number;
  averageQueueWaitMs: number;
  reconnectCount: number;
}

// ---------------------------------------------------------------------------
// Constants - 8GB RAM Optimized
// ---------------------------------------------------------------------------

const PATTERNS = {
  ENTRY_BUTTON: /\b(enter|join|participate|raffle|sweepstakes|submit|claim|sign\s*up|go)\b|🎉|🎁|🏆|^\d[\d,]*$/i,
  BLOCKED_BUTTON: /\b(leave|quit|exit|unenter|withdraw|remove\s+entry|cancel\s+(entry|giveaway)|end\s+giveaway)\b/i,
  BLOCKED_CONTENT: /\b(already\s+entered|already\s+(?:in|participating)|already\s+joined|leave\s+giveaway)\b/i,
  WIN: /(?:congratulations?|you(?:(?:'ve|\s+have)\s+won| won\s| are|'re)|winner|has\s+won|won\s+(?:the\s+)?giveaway|won\s+(?:a\s+)?(?:prize|raffle))/i,
  TIMESTAMP: /<t:(\d{10,13})(?::[a-zA-Z])?>/,
  DRAFT_BUTTON: /\b(start|edit|cancel|preview|setup)\b/i,
  GIVEAWAY_KEYWORD: /\bgiveaway\b|\braffle\b|\bsweepstakes\b|\bwin\b|\bprize\b/i,
  CROSSPOST_REFERENCE: /https:\/\/discord\.com\/channels\/(\d+)\/(\d+)\/(\d+)/,
} as const;

const KNOWN_GIVEAWAY_BOT_IDS: ReadonlySet<string> = new Set([
  '294882584201003009', '739448630517039104', '515195524879237130',
  '235148962103951360', '282859044593598464', '270904126974590976',
  '508391840525975553', '530082442967646230',
]);

const TRUSTED_ENTRY_CUSTOM_IDS: ReadonlySet<string> = new Set([
  'giveaway_message', 'giveaway-enter', 'enter_giveaway',
  'giveaway_enter', 'join_giveaway', 'giveaway-join',
  'giveaway_participate', 'participate_giveaway', 'enter', 'participants',
]);

// Detection stages
const DETECTION_STAGES = {
  CHEAP_CACHE_CHECK: 0,
  CHEAP_KEYWORD_SCAN: 1,
  CHEAP_BOT_ID_MATCH: 2,
  EXPENSIVE_BUTTON_ANALYSIS: 3,
  EXPENSIVE_EMBED_PARSING: 4,
  EXPENSIVE_PROFILE_MATCH: 5,
} as const;

// Confidence thresholds
const CONFIDENCE_HIGH = 0.8;
const CONFIDENCE_MEDIUM = 0.5;
const CONFIDENCE_LOW = 0.3;

// Queue limits
const MAX_QUEUE_SIZE = 1000;
const MAX_QUEUE_PER_GUILD = 50;
const DEAD_LETTER_RETENTION_MS = 24 * 60 * 60 * 1000;
const QUEUE_PERSIST_INTERVAL_MS = 60000;

const ENTRY_TTL_MS = 2 * 60 * 60 * 1000;
const WIN_DEDUP_TTL_MS = 30 * 60 * 1000;
const COMPONENT_RETRY_DELAY_MS = 300;
const COMPONENT_RETRY_ATTEMPTS = 3;
const SESSION_REFRESH_INTERVAL_MS = 300_000;
const HEARTBEAT_INTERVAL_MS = 120_000;
const HEALTH_CHECK_INTERVAL_MS = 30000;
const STALL_TIMEOUT_MS = 60000;
const MAX_SESSIONS_PER_WORKER = 25;
const PROCESSING_CACHE_TTL_MS = 60000;
const MAX_RECONNECT_ATTEMPTS = 3;
const RECONNECT_DELAY_MS = 60000;
const INTERACTION_RETRY_ATTEMPTS = 3;
const INTERACTION_RETRY_DELAY_MS = 2000;
const NO_RESPONSE_COOLDOWN_MS = 5000;
const BATCH_DB_WRITE_INTERVAL_MS = 5000;
const ARCHIVE_AGE_MS = 7 * 24 * 60 * 60 * 1000;

// Cache sizes
const CACHE_PROCESSED_MESSAGES = 5000;
const CACHE_MAX_PROCESSING = 1000;
const CACHE_MAX_WINS = 200;
const CACHE_MAX_COOLDOWN = 100;
const CACHE_MAX_TOKEN = 50;
const CACHE_DETECTION_PROFILES = 100;
const CACHE_WATCHLISTS = 50;
const CACHE_CROSSPOST = 1000;

// Memory thresholds
const MEMORY_WARNING_THRESHOLD_MB = 3000;
const MEMORY_CRITICAL_THRESHOLD_MB = 4500;
const MEMORY_MAX_THRESHOLD_MB = 5500;

// Queue limits
const MAX_LOG_QUEUE_SIZE = 1000;
const MAX_SESSION_START_PROMISES = 50;

// HTTP pool
const HTTP_MAX_SOCKETS = 30;
const HTTP_MAX_FREE_SOCKETS = 15;

// Circuit breaker
const CIRCUIT_BREAKER_THRESHOLD = 10;
const CIRCUIT_BREAKER_TIMEOUT_MS = 30000;
const CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS = 3;

// Metrics
const METRICS_SAMPLE_SIZE = 100;

// Session login timeouts
const LOGIN_TIMEOUT_MS = 15000;
const READY_TIMEOUT_MS = 10000;
const SESSION_START_TIMEOUT_MS = 30000;

// ---------------------------------------------------------------------------
// LRU Cache Implementation
// ---------------------------------------------------------------------------

class LRUCache<K, V> {
  private cache: Map<K, { value: V; timestamp: number }>;
  private readonly maxSize: number;
  private readonly ttl: number;

  constructor(maxSize: number, ttlMs: number = 0) {
    this.cache = new Map();
    this.maxSize = Math.max(1, maxSize);
    this.ttl = ttlMs;
  }

  get(key: K): V | undefined {
    const entry = this.cache.get(key);
    if (!entry) return undefined;
    if (this.ttl > 0 && Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      return undefined;
    }
    this.cache.delete(key);
    this.cache.set(key, entry);
    return entry.value;
  }

  set(key: K, value: V): void {
    if (this.cache.has(key)) {
      this.cache.delete(key);
    } else if (this.cache.size >= this.maxSize) {
      const toDelete = Math.ceil(this.maxSize * 0.2);
      let count = 0;
      for (const k of this.cache.keys()) {
        if (count >= toDelete) break;
        this.cache.delete(k);
        count++;
      }
    }
    this.cache.set(key, { value, timestamp: Date.now() });
  }

  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  clear(): void {
    this.cache.clear();
  }

  get size(): number {
    return this.cache.size;
  }

  has(key: K): boolean {
    return this.cache.has(key);
  }

  clean(): number {
    if (this.ttl === 0) return 0;
    const now = Date.now();
    let removed = 0;
    for (const [key, entry] of this.cache) {
      if (now - entry.timestamp > this.ttl) {
        this.cache.delete(key);
        removed++;
      }
    }
    return removed;
  }
}

// ---------------------------------------------------------------------------
// Async Logger Queue
// ---------------------------------------------------------------------------

class AsyncLogger {
  private queue: Array<{ level: string; msg: string; meta?: Record<string, unknown> }> = [];
  private processing = false;
  private interval: NodeJS.Timeout | null = null;
  private droppedCount = 0;
  private totalLogged = 0;

  constructor() {
    this.interval = setInterval(() => this.flush(), 1000);
    if (this.interval.unref) this.interval.unref();
  }

  info(msg: string, meta?: Record<string, unknown>): void {
    this.enqueue('info', msg, meta);
  }

  warn(msg: string, meta?: Record<string, unknown>): void {
    this.enqueue('warn', msg, meta);
  }

  error(msg: string, meta?: Record<string, unknown>): void {
    this.enqueue('error', msg, meta);
  }

  debug(msg: string, meta?: Record<string, unknown>): void {
    this.enqueue('debug', msg, meta);
  }

  private enqueue(level: string, msg: string, meta?: Record<string, unknown>): void {
    if (this.queue.length >= MAX_LOG_QUEUE_SIZE) {
      this.droppedCount++;
      this.queue.shift();
    }
    this.queue.push({ level, msg, meta });
    this.totalLogged++;
    if (this.queue.length > 50) this.flush();
  }

  private async flush(): Promise<void> {
    if (this.processing || this.queue.length === 0) return;
    this.processing = true;

    const batch = this.queue.splice(0, 25);
    for (const item of batch) {
      try {
        switch (item.level) {
          case 'info': logger.info(item.msg, item.meta); break;
          case 'warn': logger.warn(item.msg, item.meta); break;
          case 'error': logger.error(item.msg, item.meta); break;
          case 'debug': logger.debug(item.msg, item.meta); break;
        }
      } catch {
        // Silently fail
      }
    }

    this.processing = false;
    if (this.queue.length > 0) this.flush();
  }

  shutdown(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    this.flush();
  }

  getStats(): { queueSize: number; droppedCount: number; totalLogged: number } {
    return { queueSize: this.queue.length, droppedCount: this.droppedCount, totalLogged: this.totalLogged };
  }
}

// ---------------------------------------------------------------------------
// Circuit Breaker
// ---------------------------------------------------------------------------

class CircuitBreaker {
  private failures = 0;
  private state: 'closed' | 'open' | 'half-open' = 'closed';
  private openUntil = 0;
  private halfOpenAttempts = 0;
  private lastFailureTime = 0;
  private totalFailures = 0;
  private totalSuccesses = 0;

  constructor(
    private readonly threshold = CIRCUIT_BREAKER_THRESHOLD,
    private readonly timeoutMs = CIRCUIT_BREAKER_TIMEOUT_MS,
    private readonly halfOpenMaxAttempts = CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS,
  ) {}

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.failures > 0 && Date.now() - this.lastFailureTime > 60000) {
      this.failures = Math.max(0, this.failures - 1);
    }

    if (this.state === 'open') {
      if (Date.now() > this.openUntil) {
        this.state = 'half-open';
        this.halfOpenAttempts = 0;
        this.failures = 0;
      } else {
        throw new Error(`Circuit breaker open (cooldown: ${Math.ceil((this.openUntil - Date.now()) / 1000)}s)`);
      }
    }

    try {
      const result = await fn();
      this.totalSuccesses++;
      if (this.state === 'half-open') {
        this.halfOpenAttempts++;
        if (this.halfOpenAttempts >= this.halfOpenMaxAttempts) {
          this.state = 'closed';
          this.failures = 0;
        }
      } else {
        this.failures = Math.max(0, this.failures - 1);
      }
      return result;
    } catch (error) {
      this.failures++;
      this.totalFailures++;
      this.lastFailureTime = Date.now();
      if (this.failures >= this.threshold) {
        this.state = 'open';
        this.openUntil = Date.now() + this.timeoutMs;
        this.failures = 0;
      }
      throw error;
    }
  }

  isOpen(): boolean {
    return this.state === 'open' || (this.state === 'half-open' && this.halfOpenAttempts >= this.halfOpenMaxAttempts);
  }

  reset(): void {
    this.failures = 0;
    this.state = 'closed';
    this.openUntil = 0;
    this.halfOpenAttempts = 0;
    this.lastFailureTime = 0;
  }

  getState(): string {
    return this.state;
  }

  getStats(): { failures: number; totalFailures: number; totalSuccesses: number; state: string } {
    return {
      failures: this.failures,
      totalFailures: this.totalFailures,
      totalSuccesses: this.totalSuccesses,
      state: this.state,
    };
  }
}

// ---------------------------------------------------------------------------
// Token Manager
// ---------------------------------------------------------------------------

class TokenManager {
  private decryptedCache = new LRUCache<string, { token: string; timestamp: number }>(CACHE_MAX_TOKEN, 30000);

  async getDecryptedToken(userId: string, guildId: string, encryptedToken: string): Promise<string> {
    const cacheKey = `${userId}:${guildId}`;
    const cached = this.decryptedCache.get(cacheKey);
    if (cached) return cached.token;

    const decrypted = decryptToken(encryptedToken);
    this.decryptedCache.set(cacheKey, { token: decrypted, timestamp: Date.now() });
    return decrypted;
  }

  clearCache(userId: string, guildId: string): void {
    this.decryptedCache.delete(`${userId}:${guildId}`);
  }

  clearAll(): void {
    this.decryptedCache.clear();
  }

  getCacheStats(): { size: number; maxSize: number } {
    return { size: this.decryptedCache.size, maxSize: CACHE_MAX_TOKEN };
  }
}

// ---------------------------------------------------------------------------
// Token Bucket
// ---------------------------------------------------------------------------

class TokenBucket {
  private tokens: number;
  private lastRefill: number;
  private totalConsumed = 0;
  private totalWaits = 0;

  constructor(
    private readonly maxTokens: number,
    private readonly refillIntervalMs: number,
  ) {
    this.tokens = maxTokens;
    this.lastRefill = Date.now();
  }

  async consume(): Promise<void> {
    this.refill();
    if (this.tokens <= 0) {
      this.totalWaits++;
      const waitMs = this.refillIntervalMs - (Date.now() - this.lastRefill);
      await delay(Math.max(waitMs, 50));
      this.refill();
    }
    this.tokens = Math.max(0, this.tokens - 1);
    this.totalConsumed++;
  }

  private refill(): void {
    const now = Date.now();
    const elapsed = now - this.lastRefill;
    const batches = Math.floor(elapsed / this.refillIntervalMs);
    if (batches > 0) {
      this.tokens = Math.min(this.maxTokens, this.tokens + batches * this.maxTokens);
      this.lastRefill = now;
    }
  }

  getStats(): { tokens: number; maxTokens: number; totalConsumed: number; totalWaits: number } {
    return {
      tokens: this.tokens,
      maxTokens: this.maxTokens,
      totalConsumed: this.totalConsumed,
      totalWaits: this.totalWaits,
    };
  }
}

// ---------------------------------------------------------------------------
// Metrics Collector
// ---------------------------------------------------------------------------

class MetricsCollector {
  private detectionTimes: number[] = [];
  private entryTimes: number[] = [];
  private apiLatencies: number[] = [];
  
  public totalMessagesProcessed = 0;
  public totalGiveawaysDetected = 0;
  public totalEntriesAttempted = 0;
  public totalEntriesSucceeded = 0;
  public totalEntriesFailed = 0;
  public totalWinsDetected = 0;
  public apiCalls = 0;
  public apiErrors = 0;
  public cacheHits = 0;
  public cacheMisses = 0;
  public dbQueries = 0;
  public startTime = Date.now();
  public lastStatsReset = Date.now();

  recordDetectionTime(ms: number): void {
    this.detectionTimes.push(ms);
    if (this.detectionTimes.length > METRICS_SAMPLE_SIZE) {
      this.detectionTimes.shift();
    }
  }

  recordEntryTime(ms: number): void {
    this.entryTimes.push(ms);
    if (this.entryTimes.length > METRICS_SAMPLE_SIZE) {
      this.entryTimes.shift();
    }
  }

  recordApiLatency(ms: number): void {
    this.apiLatencies.push(ms);
    if (this.apiLatencies.length > METRICS_SAMPLE_SIZE) {
      this.apiLatencies.shift();
    }
  }

  getAverageDetectionTime(): number {
    if (this.detectionTimes.length === 0) return 0;
    return Math.round(this.detectionTimes.reduce((a, b) => a + b, 0) / this.detectionTimes.length);
  }

  getAverageEntryTime(): number {
    if (this.entryTimes.length === 0) return 0;
    return Math.round(this.entryTimes.reduce((a, b) => a + b, 0) / this.entryTimes.length);
  }

  getAverageApiLatency(): number {
    if (this.apiLatencies.length === 0) return 0;
    return Math.round(this.apiLatencies.reduce((a, b) => a + b, 0) / this.apiLatencies.length);
  }

  getMetrics() {
    const mem = process.memoryUsage();
    return {
      totalMessagesProcessed: this.totalMessagesProcessed,
      totalGiveawaysDetected: this.totalGiveawaysDetected,
      totalEntriesAttempted: this.totalEntriesAttempted,
      totalEntriesSucceeded: this.totalEntriesSucceeded,
      totalEntriesFailed: this.totalEntriesFailed,
      totalWinsDetected: this.totalWinsDetected,
      averageDetectionTime: this.getAverageDetectionTime(),
      averageEntryTime: this.getAverageEntryTime(),
      apiCalls: this.apiCalls,
      apiErrors: this.apiErrors,
      cacheHits: this.cacheHits,
      cacheMisses: this.cacheMisses,
      dbQueries: this.dbQueries,
      memoryUsage: {
        heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
        heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
        rss: Math.round(mem.rss / 1024 / 1024),
      },
      startTime: this.startTime,
      lastStatsReset: this.lastStatsReset,
    };
  }

  reset(): void {
    this.detectionTimes = [];
    this.entryTimes = [];
    this.apiLatencies = [];
    this.totalMessagesProcessed = 0;
    this.totalGiveawaysDetected = 0;
    this.totalEntriesAttempted = 0;
    this.totalEntriesSucceeded = 0;
    this.totalEntriesFailed = 0;
    this.totalWinsDetected = 0;
    this.apiCalls = 0;
    this.apiErrors = 0;
    this.cacheHits = 0;
    this.cacheMisses = 0;
    this.dbQueries = 0;
    this.lastStatsReset = Date.now();
  }
}

// ---------------------------------------------------------------------------
// Structured Tracing - Correlation Tracker
// ---------------------------------------------------------------------------

class CorrelationTracker {
  private activeTraces: Map<string, {
    correlationId: string;
    startTime: number;
    stages: Map<string, { startTime: number; endTime?: number; metadata?: any }>;
    userId: string;
    messageId: string;
    status: 'active' | 'completed' | 'failed';
  }> = new Map();

  createTrace(userId: string, messageId: string): string {
    const correlationId = uuidv4();
    this.activeTraces.set(correlationId, {
      correlationId,
      startTime: Date.now(),
      stages: new Map(),
      userId,
      messageId,
      status: 'active',
    });

    if (this.activeTraces.size > 1000) {
      const toDelete = Array.from(this.activeTraces.keys()).slice(0, 100);
      toDelete.forEach(id => this.activeTraces.delete(id));
    }

    return correlationId;
  }

  startStage(correlationId: string, stage: string, metadata?: any): void {
    const trace = this.activeTraces.get(correlationId);
    if (trace) {
      trace.stages.set(stage, { startTime: Date.now(), metadata });
    }
  }

  endStage(correlationId: string, stage: string, metadata?: any): void {
    const trace = this.activeTraces.get(correlationId);
    if (trace) {
      const stageData = trace.stages.get(stage);
      if (stageData) {
        stageData.endTime = Date.now();
        if (metadata) stageData.metadata = { ...stageData.metadata, ...metadata };
      }
    }
  }

  completeTrace(correlationId: string, status: 'completed' | 'failed' = 'completed'): any {
    const trace = this.activeTraces.get(correlationId);
    if (!trace) return null;

    trace.status = status;
    const duration = Date.now() - trace.startTime;
    
    const stageDetails: any = {};
    trace.stages.forEach((stage, name) => {
      stageDetails[name] = {
        duration: stage.endTime ? stage.endTime - stage.startTime : Date.now() - stage.startTime,
        metadata: stage.metadata,
      };
    });

    const summary = {
      correlationId: trace.correlationId,
      userId: trace.userId,
      messageId: trace.messageId,
      totalDuration: duration,
      status,
      stages: stageDetails,
    };

    setTimeout(() => this.activeTraces.delete(correlationId), 5000);
    
    return summary;
  }

  getTrace(correlationId: string): any {
    return this.activeTraces.get(correlationId);
  }

  getActiveTraceCount(): number {
    return this.activeTraces.size;
  }
}

// ---------------------------------------------------------------------------
// Join Queue System
// ---------------------------------------------------------------------------

class JoinQueue {
  private queues: Map<string, QueueItem[]> = new Map();
  private deadLetterQueue: QueueItem[] = [];
  private totalProcessed = 0;
  private totalWaitTimes: number[] = [];

  enqueue(item: QueueItem): boolean {
    if (this.getTotalSize() >= MAX_QUEUE_SIZE) return false;

    const guildQueue = this.getGuildQueue(item.guildId);
    if (guildQueue.length >= MAX_QUEUE_PER_GUILD) return false;

    guildQueue.push(item);
    guildQueue.sort((a, b) => a.priority - b.priority);
    return true;
  }

  dequeue(guildId?: string): QueueItem | undefined {
    if (guildId) {
      const guildQueue = this.queues.get(guildId);
      if (guildQueue?.length) {
        this.totalProcessed++;
        const startWait = Date.now();
        const item = guildQueue.shift()!;
        this.totalWaitTimes.push(startWait - item.addedAt);
        return item;
      }
      return undefined;
    }

    let highestPriority: QueueItem | undefined;
    let highestPriorityGuild: string | undefined;

    for (const [guildId, guildQueue] of this.queues) {
      if (guildQueue.length && (!highestPriority || guildQueue[0].priority < highestPriority.priority)) {
        highestPriority = guildQueue[0];
        highestPriorityGuild = guildId;
      }
    }

    if (highestPriority && highestPriorityGuild) {
      this.queues.get(highestPriorityGuild)?.shift();
      this.totalProcessed++;
      const startWait = Date.now();
      this.totalWaitTimes.push(startWait - highestPriority.addedAt);
    }

    return highestPriority;
  }

  removeGuildEntries(guildId: string): number {
    const count = this.queues.get(guildId)?.length || 0;
    this.queues.delete(guildId);
    return count;
  }

  cancelGiveaway(messageId: string, channelId: string): boolean {
    for (const [guildId, guildQueue] of this.queues) {
      const index = guildQueue.findIndex(
        item => item.messageId === messageId && item.channelId === channelId
      );
      if (index !== -1) {
        guildQueue.splice(index, 1);
        return true;
      }
    }
    return false;
  }

  moveToDeadLetter(item: QueueItem, error: string): void {
    item.lastError = error;
    item.addedAt = Date.now();
    this.deadLetterQueue.push(item);
    
    const cutoff = Date.now() - DEAD_LETTER_RETENTION_MS;
    this.deadLetterQueue = this.deadLetterQueue.filter(dl => dl.addedAt > cutoff);
  }

  retryDeadLetter(correlationId: string): QueueItem | undefined {
    const index = this.deadLetterQueue.findIndex(item => item.correlationId === correlationId);
    if (index !== -1) {
      const item = this.deadLetterQueue.splice(index, 1)[0];
      if (this.enqueue(item)) return item;
      this.deadLetterQueue.push(item);
    }
    return undefined;
  }

  getGuildQueue(guildId: string): QueueItem[] {
    if (!this.queues.has(guildId)) {
      this.queues.set(guildId, []);
    }
    return this.queues.get(guildId)!;
  }

  getTotalSize(): number {
    let total = 0;
    for (const queue of this.queues.values()) {
      total += queue.length;
    }
    return total;
  }

  getAverageWaitTime(): number {
    if (this.totalWaitTimes.length === 0) return 0;
    return Math.round(
      this.totalWaitTimes.reduce((a, b) => a + b, 0) / this.totalWaitTimes.length
    );
  }

  getStats() {
    return {
      totalQueued: this.getTotalSize(),
      totalProcessed: this.totalProcessed,
      deadLetterCount: this.deadLetterQueue.length,
      averageWaitMs: this.getAverageWaitTime(),
      guildQueues: Array.from(this.queues.entries()).map(([guildId, queue]) => ({
        guildId,
        size: queue.length,
      })),
    };
  }

  async persist(): Promise<void> {
    try {
      const allItems: QueueItem[] = [];
      for (const queue of this.queues.values()) {
        allItems.push(...queue);
      }
      allItems.push(...this.deadLetterQueue);
      await saveQueueState(allItems);
    } catch (error) {
      // Silently fail
    }
  }

  async restore(): Promise<void> {
    try {
      const items = await loadQueueState();
      for (const item of items) {
        if (item.priority < 0) {
          this.deadLetterQueue.push(item);
        } else {
          this.enqueue(item);
        }
      }
    } catch (error) {
      // Start fresh
    }
  }
}

// ---------------------------------------------------------------------------
// Watchlist System
// ---------------------------------------------------------------------------

class WatchlistManager {
  private keywords: Map<string, WatchlistKeyword> = new Map();
  private compiledCache: Map<string, RegExp[]> = new Map();

  async loadKeywords(userId: string): Promise<void> {
    try {
      const keywords = await getWatchlistKeywords(userId);
      for (const kw of keywords) {
        this.addKeyword(kw.keyword, kw.aliases || []);
      }
    } catch (error) {
      this.addKeyword('giveaway', ['raffle', 'sweepstakes']);
      this.addKeyword('nitro', ['discord nitro', 'nitro classic']);
    }
  }

  addKeyword(keyword: string, aliases: string[] = []): void {
    const allTerms = [keyword, ...aliases];
    const compiled = allTerms.map(term => 
      new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i')
    );

    this.keywords.set(keyword.toLowerCase(), {
      keyword,
      aliases,
      matchCount: 0,
      lastMatched: 0,
      compiled: compiled[0],
    });

    this.compiledCache.set(keyword.toLowerCase(), compiled);
    this.compiledCache.clear();
  }

  getCompiledPatterns(): RegExp[] {
    const cacheKey = 'all_patterns';
    if (this.compiledCache.has(cacheKey)) {
      return this.compiledCache.get(cacheKey)!;
    }

    const patterns: RegExp[] = [];
    for (const kw of this.keywords.values()) {
      patterns.push(kw.compiled);
    }

    this.compiledCache.set(cacheKey, patterns);
    return patterns;
  }

  matchKeyword(text: string): { keyword: string; matched: boolean }[] {
    const results: { keyword: string; matched: boolean }[] = [];
    
    for (const [key, kw] of this.keywords) {
      if (kw.compiled.test(text)) {
        kw.matchCount++;
        kw.lastMatched = Date.now();
        results.push({ keyword: kw.keyword, matched: true });
      }
    }

    return results;
  }

  getTopKeywords(limit: number = 10): WatchlistKeyword[] {
    return Array.from(this.keywords.values())
      .sort((a, b) => b.matchCount - a.matchCount)
      .slice(0, limit);
  }

  getStats() {
    return {
      totalKeywords: this.keywords.size,
      topKeywords: this.getTopKeywords(5).map(kw => ({
        keyword: kw.keyword,
        matches: kw.matchCount,
        lastMatched: kw.lastMatched ? new Date(kw.lastMatched).toISOString() : 'never',
      })),
    };
  }
}

// ---------------------------------------------------------------------------
// Multi-Stage Detection Engine
// ---------------------------------------------------------------------------

class DetectionEngine {
  private profiles: Map<string, DetectionProfile> = new Map();
  private falsePositiveHistory: Map<string, number> = new Map();
  private detectionAccuracy: Map<string, { tp: number; fp: number }> = new Map();

  async loadProfiles(): Promise<void> {
    try {
      const profiles = await getDetectionProfiles();
      for (const profile of profiles) {
        this.profiles.set(profile.botId, profile);
      }
    } catch (error) {
      for (const botId of KNOWN_GIVEAWAY_BOT_IDS) {
        this.profiles.set(botId, {
          botId,
          botName: `Bot-${botId}`,
          detectionCount: 0,
          truePositives: 0,
          falsePositives: 0,
          averageConfidence: 0.9,
          lastSeen: 0,
          patterns: {
            buttonPatterns: ['giveaway', 'enter', 'join'],
            embedPatterns: ['giveaway', 'prize', 'winners'],
            contentPatterns: ['giveaway', '🎉'],
          },
        });
      }
    }
  }

  async detect(message: Message, correlationId: string): Promise<DetectionResult> {
    const reasons: string[] = [];
    let totalConfidence = 0;
    let stageCount = 0;

    // Stage 1: Cheap keyword scan
    const keywordScore = this.cheapKeywordScan(message);
    if (keywordScore > 0) {
      totalConfidence += keywordScore;
      stageCount++;
      reasons.push(`Keyword match: ${keywordScore.toFixed(2)}`);
    }

    // Stage 2: Cheap bot ID match
    const botMatch = this.cheapBotIdMatch(message);
    if (botMatch) {
      totalConfidence += 0.4;
      stageCount++;
      reasons.push(`Known bot: ${message.author?.username}`);
    }

    if (totalConfidence > 0 || stageCount > 0) {
      // Stage 3: Expensive button analysis
      const buttonResult = await this.expensiveButtonAnalysis(message, correlationId);
      if (buttonResult.score > 0) {
        totalConfidence += buttonResult.score;
        stageCount++;
        reasons.push(...buttonResult.reasons);
      }

      // Stage 4: Expensive embed parsing
      const embedResult = this.expensiveEmbedParsing(message);
      if (embedResult.score > 0) {
        totalConfidence += embedResult.score;
        stageCount++;
        reasons.push(...embedResult.reasons);
      }

      // Stage 5: Expensive profile match
      if (botMatch && message.author?.id) {
        const profileResult = this.expensiveProfileMatch(message);
        if (profileResult.score > 0) {
          totalConfidence += profileResult.score;
          stageCount++;
          reasons.push(...profileResult.reasons);
        }
      }
    }

    const finalConfidence = stageCount > 0 ? totalConfidence / Math.max(stageCount, 1) : 0;
    const isGiveaway = finalConfidence >= CONFIDENCE_LOW;

    let button: GiveawayButton | undefined;
    if (isGiveaway) {
      button = this.extractEntryButton(message);
    }

    return {
      isGiveaway,
      confidence: Math.min(finalConfidence, 1.0),
      reasons,
      button,
      prize: isGiveaway ? this.extractPrize(message) : undefined,
      profile: message.author?.id ? this.profiles.get(message.author.id) : undefined,
    };
  }

  private cheapKeywordScan(message: Message): number {
    let score = 0;
    const content = message.content || '';
    
    if (PATTERNS.GIVEAWAY_KEYWORD.test(content)) {
      score += 0.2;
    }

    if (message.embeds?.length) {
      for (const embed of message.embeds) {
        const embedText = [embed.title, embed.description, embed.footer?.text]
          .filter(Boolean)
          .join(' ');
        if (PATTERNS.GIVEAWAY_KEYWORD.test(embedText)) {
          score += 0.3;
          break;
        }
      }
    }

    return Math.min(score, 0.5);
  }

  private cheapBotIdMatch(message: Message): boolean {
    return !!(message.author?.bot && message.author.id && 
              KNOWN_GIVEAWAY_BOT_IDS.has(message.author.id));
  }

  private async expensiveButtonAnalysis(
    message: Message, 
    correlationId: string
  ): Promise<{ score: number; reasons: string[] }> {
    const reasons: string[] = [];
    let score = 0;

    try {
      if (!(message as any).components?.length) {
        await delay(COMPONENT_RETRY_DELAY_MS);
        try { await message.fetch(); } catch {}
      }

      const components = (message as any).components;
      if (!components?.length) return { score: 0, reasons: [] };

      for (const row of components) {
        const rowComps = row?.components;
        if (!rowComps?.length) continue;

        for (const comp of rowComps) {
          if (comp.type !== 2 || comp.style === 5) continue;

          const customId = comp.customId || comp.custom_id;
          const label = (comp.label || '').toLowerCase();

          if (customId && TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) {
            score += 0.3;
            reasons.push(`Trusted button ID: ${customId}`);
          }

          if (PATTERNS.ENTRY_BUTTON.test(label)) {
            score += 0.2;
            reasons.push(`Entry button: ${label}`);
          }

          if (PATTERNS.BLOCKED_BUTTON.test(label)) {
            score -= 0.1;
            reasons.push(`Blocked button: ${label}`);
          }
        }
      }
    } catch (error) {
      // Button analysis failed
    }

    return { score: Math.max(0, Math.min(score, 0.5)), reasons };
  }

  private expensiveEmbedParsing(message: Message): { score: number; reasons: string[] } {
    const reasons: string[] = [];
    let score = 0;

    if (!message.embeds?.length) return { score: 0, reasons: [] };

    const embed = message.embeds[0];
    
    if (embed.description) {
      const timestampMatch = embed.description.match(PATTERNS.TIMESTAMP);
      if (timestampMatch) {
        score += 0.3;
        reasons.push('Countdown timer found');
      }
    }

    if (embed.title || embed.description) {
      const text = (embed.title + ' ' + (embed.description || '')).toLowerCase();
      if (/\b(prize|reward|win|nitro|steam|gift card)\b/i.test(text)) {
        score += 0.2;
        reasons.push('Prize mentioned');
      }
    }

    if (embed.description && /\d+\s*(winner|winners)/i.test(embed.description)) {
      score += 0.2;
      reasons.push('Winner count specified');
    }

    return { score: Math.min(score, 0.5), reasons };
  }

  private expensiveProfileMatch(message: Message): { score: number; reasons: string[] } {
    const reasons: string[] = [];
    let score = 0;

    const profile = message.author?.id ? this.profiles.get(message.author.id) : undefined;
    if (!profile) return { score: 0, reasons: [] };

    const content = (message.content || '').toLowerCase();
    const embedText = message.embeds?.map(e => 
      [e.title, e.description, e.footer?.text].filter(Boolean).join(' ')
    ).join(' ') || '';

    for (const pattern of profile.patterns.buttonPatterns) {
      if (content.includes(pattern) || embedText.includes(pattern)) {
        score += 0.1;
        reasons.push(`Profile pattern match: ${pattern}`);
      }
    }

    const accuracy = profile.detectionCount > 0 
      ? profile.truePositives / profile.detectionCount 
      : 0.9;

    score *= accuracy;

    return { score: Math.min(score, 0.5), reasons };
  }

  private extractEntryButton(message: Message): GiveawayButton | undefined {
    const components = (message as any).components;
    if (!components?.length) return undefined;

    for (const row of components) {
      const rowComps = row?.components;
      if (!rowComps?.length) continue;

      for (const comp of rowComps) {
        if (comp.type !== 2 || comp.style === 5 || comp.disabled) continue;

        const customId = comp.customId || comp.custom_id;
        const label = comp.label || '';

        if (customId && TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) {
          return { customId, label, disabled: false };
        }

        if (PATTERNS.ENTRY_BUTTON.test(label)) {
          return { customId: customId || label, label, disabled: false };
        }
      }
    }

    return undefined;
  }

  private extractPrize(message: Message): string {
    const embed = message.embeds?.[0];
    if (embed?.title) return truncate(embed.title, 200);
    if (embed?.description) return truncate(embed.description, 200);
    if (message.content) return truncate(message.content, 200);
    return 'Unknown Prize';
  }

  recordResult(botId: string, isTruePositive: boolean, confidence: number): void {
    const profile = this.profiles.get(botId);
    if (profile) {
      profile.detectionCount++;
      if (isTruePositive) {
        profile.truePositives++;
      } else {
        profile.falsePositives++;
      }
      profile.averageConfidence = 
        (profile.averageConfidence * (profile.detectionCount - 1) + confidence) / 
        profile.detectionCount;
      profile.lastSeen = Date.now();
    }

    const accuracy = this.detectionAccuracy.get(botId) || { tp: 0, fp: 0 };
    if (isTruePositive) {
      accuracy.tp++;
    } else {
      accuracy.fp++;
    }
    this.detectionAccuracy.set(botId, accuracy);

    if (profile && profile.detectionCount % 10 === 0) {
      updateDetectionProfile(profile).catch(() => {});
    }
  }

  recordFalsePositive(pattern: string): void {
    const count = (this.falsePositiveHistory.get(pattern) || 0) + 1;
    this.falsePositiveHistory.set(pattern, count);
  }

  getProfile(botId: string): DetectionProfile | undefined {
    return this.profiles.get(botId);
  }

  getAccuracy(botId: string): number {
    const accuracy = this.detectionAccuracy.get(botId);
    if (!accuracy || (accuracy.tp + accuracy.fp) === 0) return 1.0;
    return accuracy.tp / (accuracy.tp + accuracy.fp);
  }

  getStats() {
    return {
      profilesTracked: this.profiles.size,
      falsePositivePatterns: this.falsePositiveHistory.size,
      botAccuracies: Array.from(this.detectionAccuracy.entries()).map(([botId, acc]) => ({
        botId,
        accuracy: acc.tp / (acc.tp + acc.fp) || 1.0,
        total: acc.tp + acc.fp,
      })),
    };
  }
}

// ---------------------------------------------------------------------------
// AutoJoinManager - Main Class
// ---------------------------------------------------------------------------

export class AutoJoinManager extends EventEmitter {
  // Sessions - key now includes guildId to prevent conflicts
  private sessions: Map<string, UserSession> = new Map();
  private sessionsByUserId: Map<string, UserSession> = new Map();
  
  // Caches
  private processedMessages: LRUCache<string, number>;
  private processingCache: LRUCache<string, number>;
  private recentWins: LRUCache<string, number>;
  private noResponseCooldown: LRUCache<string, number>;
  private crosspostCache: LRUCache<string, string>;
  
  // Enhanced systems
  private correlationTracker: CorrelationTracker;
  private joinQueue: JoinQueue;
  private detectionEngine: DetectionEngine;
  private watchlistManager: WatchlistManager;
  
  // Managers
  private tokenManager: TokenManager;
  private asyncLogger: AsyncLogger;
  private apiCircuitBreaker: CircuitBreaker;
  private metrics: MetricsCollector;
  
  // Intervals
  private refreshInterval: NodeJS.Timeout | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;
  private memoryCheckInterval: NodeJS.Timeout | null = null;
  private reconnectCheckInterval: NodeJS.Timeout | null = null;
  private cacheCleanInterval: NodeJS.Timeout | null = null;
  private metricsInterval: NodeJS.Timeout | null = null;
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private queuePersistInterval: NodeJS.Timeout | null = null;
  private stallCheckInterval: NodeJS.Timeout | null = null;
  private batchDbInterval: NodeJS.Timeout | null = null;
  private archiveInterval: NodeJS.Timeout | null = null;
  
  // State
  private isShuttingDown = false;
  private sessionStartPromises: Map<string, Promise<boolean>> = new Map();
  private workerId: string;
  private memoryWarningLogged = false;
  private healthStatus: 'healthy' | 'warning' | 'critical' = 'healthy';
  private sessionIdCounter = 0;
  private memoryCriticalLogged = false;
  private lastMemoryCheck = 0;
  private sessionStartErrors: Map<string, { count: number; lastError: string; lastAttempt: number }> = new Map();

  // Batch write buffers
  private joinOutcomeBuffer: any[] = [];
  private detectionConfidenceBuffer: any[] = [];
  
  // Per-guild and per-account stats
  private guildStats: Map<string, GuildStats> = new Map();
  private accountStats: Map<string, AccountStats> = new Map();
  private reconnectCount: Map<string, number> = new Map();

  // HTTP client
  private readonly http: AxiosInstance;

  constructor(workerId: string = 'main') {
    super();
    this.workerId = workerId;
    this.setMaxListeners(100);
    
    // Initialize caches
    this.processedMessages = new LRUCache<string, number>(CACHE_PROCESSED_MESSAGES, 300000);
    this.processingCache = new LRUCache<string, number>(CACHE_MAX_PROCESSING, PROCESSING_CACHE_TTL_MS);
    this.recentWins = new LRUCache<string, number>(CACHE_MAX_WINS, WIN_DEDUP_TTL_MS);
    this.noResponseCooldown = new LRUCache<string, number>(CACHE_MAX_COOLDOWN);
    this.crosspostCache = new LRUCache<string, string>(CACHE_CROSSPOST, 3600000);
    
    // Initialize enhanced systems
    this.correlationTracker = new CorrelationTracker();
    this.joinQueue = new JoinQueue();
    this.detectionEngine = new DetectionEngine();
    this.watchlistManager = new WatchlistManager();
    
    // Initialize managers
    this.tokenManager = new TokenManager();
    this.asyncLogger = new AsyncLogger();
    this.apiCircuitBreaker = new CircuitBreaker();
    this.metrics = new MetricsCollector();

    // HTTP client
    this.http = axios.create({
      timeout: 10_000,
      httpAgent: new http.Agent({
        keepAlive: true,
        keepAliveMsecs: 1000,
        maxSockets: HTTP_MAX_SOCKETS,
        maxFreeSockets: HTTP_MAX_FREE_SOCKETS,
        scheduling: 'lifo',
      }),
      httpsAgent: new https.Agent({
        keepAlive: true,
        keepAliveMsecs: 1000,
        maxSockets: HTTP_MAX_SOCKETS,
        maxFreeSockets: HTTP_MAX_FREE_SOCKETS,
        scheduling: 'lifo',
      }),
    });

    // Initialize systems
    this.initialize().catch(error => {
      this.asyncLogger.error('Failed to initialize AutoJoinManager', { error: formatError(error) });
    });

    // Start intervals
    this.startSessionRefresher();
    this.startCleanupInterval();
    this.startMemoryCheck();
    this.startReconnectChecker();
    this.startCacheCleaner();
    this.startMetricsInterval();
    this.startHealthChecker();
    this.startQueuePersister();
    this.startStallChecker();
    this.startBatchDbWriter();
    this.startArchiveInterval();

    this.asyncLogger.info('🚀 Enhanced AutoJoinManager initialized', {
      worker: this.workerId,
      features: {
        multiStageDetection: true,
        joinQueue: true,
        structuredTracing: true,
        watchlists: true,
        batchDb: true,
        healthChecks: true,
      },
      memory: this.getMemoryUsage(),
    });
  }

  private async initialize(): Promise<void> {
    await Promise.all([
      this.detectionEngine.loadProfiles(),
      this.joinQueue.restore(),
    ]);
  }

  // -------------------------------------------------------------------------
  // Memory Management
  // -------------------------------------------------------------------------

  private getMemoryUsage(): { heapUsedMB: number; heapTotalMB: number; rssMB: number } {
    const mem = process.memoryUsage();
    return {
      heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
      rssMB: Math.round(mem.rss / 1024 / 1024),
    };
  }

  private checkMemory(): boolean {
    const now = Date.now();
    if (now - this.lastMemoryCheck < 5000) return this.healthStatus !== 'critical';
    this.lastMemoryCheck = now;

    const mem = this.getMemoryUsage();
    
    if (mem.heapUsedMB > MEMORY_MAX_THRESHOLD_MB) {
      this.healthStatus = 'critical';
    } else if (mem.heapUsedMB > MEMORY_CRITICAL_THRESHOLD_MB) {
      this.healthStatus = 'warning';
    } else {
      this.healthStatus = 'healthy';
    }
    
    if (mem.heapUsedMB > MEMORY_MAX_THRESHOLD_MB) {
      if (!this.memoryCriticalLogged) {
        this.asyncLogger.error('🚨 CRITICAL: Memory exceeded limit', {
          worker: this.workerId,
          ...mem,
          threshold: MEMORY_MAX_THRESHOLD_MB,
        });
        this.memoryCriticalLogged = true;
      }
      this.forceCleanup();
      this.emit('memoryCritical', { ...mem, worker: this.workerId });
      return false;
    }
    
    if (mem.heapUsedMB > MEMORY_CRITICAL_THRESHOLD_MB) {
      if (!this.memoryWarningLogged) {
        this.asyncLogger.warn('⚠️ Memory critical, aggressive cleanup', {
          worker: this.workerId,
          ...mem,
        });
        this.memoryWarningLogged = true;
      }
      this.aggressiveCleanup();
      return true;
    }
    
    if (mem.heapUsedMB > MEMORY_WARNING_THRESHOLD_MB) {
      if (!this.memoryWarningLogged) {
        this.asyncLogger.warn('⚠️ Memory warning threshold reached', {
          worker: this.workerId,
          ...mem,
        });
        this.memoryWarningLogged = true;
      }
      this.processedMessages.clean();
      this.processingCache.clean();
      this.recentWins.clean();
      this.noResponseCooldown.clean();
    } else {
      this.memoryWarningLogged = false;
      this.memoryCriticalLogged = false;
    }
    
    return true;
  }

  private aggressiveCleanup(): void {
    this.processedMessages.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    this.noResponseCooldown.clear();
    this.crosspostCache.clear();
    this.tokenManager.clearAll();
    
    if (this.sessionStartPromises.size > 10) {
      this.sessionStartPromises.clear();
    }
    
    if (global.gc) {
      global.gc();
    }
  }

  private forceCleanup(): void {
    this.aggressiveCleanup();
    
    const mem = this.getMemoryUsage();
    if (mem.heapUsedMB > MEMORY_CRITICAL_THRESHOLD_MB && this.sessions.size > 1) {
      const toStop = Math.max(1, Math.floor(this.sessions.size * 0.3));
      let stopped = 0;
      for (const [key, session] of this.sessions) {
        if (stopped >= toStop) break;
        if (this.sessions.size <= 1) break;
        this.stopSession(session.userId, session.guildId);
        stopped++;
      }
      this.asyncLogger.warn(`Stopped ${stopped} sessions to free memory`, {
        worker: this.workerId,
        remaining: this.sessions.size,
      });
    }
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async startAllSessions(): Promise<void> {
    if (!this.checkMemory()) {
      this.asyncLogger.warn('Memory too high, skipping session start', {
        worker: this.workerId,
        memory: this.getMemoryUsage(),
      });
      return;
    }

    this.asyncLogger.info(`🚀 Starting AutoJoin sessions (worker: ${this.workerId})...`);

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const validUsers = allPremiumUsers.filter(u => u.token);
      
      this.asyncLogger.info(`Found ${validUsers.length} premium users with tokens`, {
        worker: this.workerId,
      });

      const usersToStart = validUsers.slice(0, MAX_SESSIONS_PER_WORKER);

      const concurrencyLimit = 3;
      let started = 0;
      let failed = 0;
      
      for (let i = 0; i < usersToStart.length; i += concurrencyLimit) {
        const batch = usersToStart.slice(i, i + concurrencyLimit);
        
        if (!this.checkMemory()) {
          this.asyncLogger.warn('Memory threshold reached, stopping session start', {
            worker: this.workerId,
            started,
            failed,
          });
          break;
        }
        
        const results = await Promise.allSettled(
          batch.map(user => this.startSession(user.userId, user.guildId))
        );
        
        for (const result of results) {
          if (result.status === 'fulfilled' && result.value) {
            started++;
          } else {
            failed++;
          }
        }
        
        await delay(1000);
      }

      this.asyncLogger.info(`✅ AutoJoin sessions started: ${started} active (${failed} failed)`, {
        worker: this.workerId,
        sessions: this.sessions.size,
        memory: this.getMemoryUsage(),
      });
    } catch (error) {
      this.asyncLogger.error('Failed to start AutoJoin sessions', {
        worker: this.workerId,
        error: formatError(error),
      });
    }
  }

  private async getAllPremiumUsersAcrossAllGuilds(): Promise<any[]> {
    try {
      const users = await getAllPremiumUsersAllGuilds();
      if (users.length === 0) {
        const { getAllPremiumUsers } = await import('../database.js');
        const guildId = process.env.GUILD_ID;
        if (guildId) return await getAllPremiumUsers(guildId);
      }
      return users;
    } catch (error) {
      this.asyncLogger.error('Failed to get premium users', { error: formatError(error) });
      return [];
    }
  }

  async startSession(userId: string, guildId: string): Promise<boolean> {
    // Session key now includes guildId to prevent conflicts
    const sessionKey = this.makeSessionKey(userId, guildId);
    
    if (this.sessions.has(sessionKey)) return true;
    if (this.sessions.size >= MAX_SESSIONS_PER_WORKER) {
      this.asyncLogger.warn(`Session limit reached (${MAX_SESSIONS_PER_WORKER})`, { userId, guildId });
      return false;
    }

    if (this.sessionStartPromises.size >= MAX_SESSION_START_PROMISES) {
      this.asyncLogger.warn('Too many pending session starts', { userId, guildId });
      return false;
    }

    if (this.sessionStartPromises.has(sessionKey)) {
      return this.sessionStartPromises.get(sessionKey)!;
    }

    // Check if this session has been failing repeatedly
    const errorHistory = this.sessionStartErrors.get(sessionKey);
    if (errorHistory) {
      const now = Date.now();
      if (errorHistory.count >= 5 && now - errorHistory.lastAttempt < 600000) { // 5 failures in 10 minutes
        this.asyncLogger.warn('Session start throttled due to repeated failures', {
          userId,
          guildId,
          failures: errorHistory.count,
          lastError: errorHistory.lastError,
        });
        return false;
      }
      // Reset if it's been a while
      if (now - errorHistory.lastAttempt > 600000) {
        this.sessionStartErrors.delete(sessionKey);
      }
    }

    const startPromise = this._startSessionInternal(userId, guildId);
    this.sessionStartPromises.set(sessionKey, startPromise);

    try {
      const result = await startPromise;
      if (result) {
        // Success - clear error history
        this.sessionStartErrors.delete(sessionKey);
      } else {
        // Track failure
        const history = this.sessionStartErrors.get(sessionKey) || { count: 0, lastError: '', lastAttempt: 0 };
        history.count++;
        history.lastError = 'Session start returned false';
        history.lastAttempt = Date.now();
        this.sessionStartErrors.set(sessionKey, history);
      }
      return result;
    } catch (error) {
      // Track failure with error
      const history = this.sessionStartErrors.get(sessionKey) || { count: 0, lastError: '', lastAttempt: 0 };
      history.count++;
      history.lastError = formatError(error);
      history.lastAttempt = Date.now();
      this.sessionStartErrors.set(sessionKey, history);
      return false;
    } finally {
      this.sessionStartPromises.delete(sessionKey);
    }
  }

  private async _startSessionInternal(userId: string, guildId: string): Promise<boolean> {
    // Session key now includes guildId
    const sessionKey = this.makeSessionKey(userId, guildId);

    try {
      this.asyncLogger.debug('Starting session', { userId, guildId, worker: this.workerId });

      const user = await getPremiumUser(userId, guildId);
      if (!user?.token) {
        this.asyncLogger.warn('No token found for user', { userId, guildId });
        return false;
      }

      let decryptedToken: string;
      try {
        decryptedToken = await this.tokenManager.getDecryptedToken(userId, guildId, user.token);
      } catch (error) {
        this.asyncLogger.error('Failed to decrypt token', { userId, guildId, error: formatError(error) });
        await setTokenActive(userId, guildId, false);
        return false;
      }

      // Validate token before creating session - this prevents leaking clients on invalid tokens
      const isValid = await this.validateToken(decryptedToken);
      if (!isValid) {
        this.asyncLogger.warn('Token validation failed', { userId, guildId });
        await setTokenActive(userId, guildId, false);
        this.tokenManager.clearCache(userId, guildId);
        return false;
      }

      const clientOptions: ClientOptions = {
        messageCacheLifetime: 60,
        messageSweepInterval: 300,
        restRequestTimeout: 15000,
        restGlobalRateLimit: 50,
        retryLimit: 3,
        allowedMentions: { parse: [] },
        partials: [],
        makeCache: Options.cacheWithLimits({
          PresenceManager: 0,
          ReactionManager: 0,
          ThreadManager: 0,
          VoiceStateManager: 0,
          StageInstanceManager: 0,
        }),
      };

      const client = new Client(clientOptions);
      client.setMaxListeners(50);
      
      this.sessionIdCounter++;
      const sessionId = `${userId}-${Date.now()}-${this.sessionIdCounter}`;
      
      const session: UserSession = {
        client,
        userId,
        guildId,
        label: user.tokenLabel || 'main',
        startedAt: Date.now(),
        isActive: true,
        stats: { detected: 0, entered: 0, failed: 0, wins: 0, falsePositives: 0, queueWaitTimes: [] },
        reconnectAttempts: 0,
        maxReconnectAttempts: MAX_RECONNECT_ATTEMPTS,
        rateLimiter: new TokenBucket(5, 5000),
        listeners: {},
        sessionId,
        destroyed: false,
        lastHealthCheck: Date.now(),
        stallCount: 0,
      };

      this.registerEvents(session);
      
      // Login with timeout and proper error handling
      try {
        await Promise.race([
          this.loginWithTimeout(client, decryptedToken),
          delay(LOGIN_TIMEOUT_MS).then(() => { throw new Error('Login timeout'); })
        ]);
      } catch (loginError) {
        this.asyncLogger.error('Login failed', { userId, guildId, error: formatError(loginError) });
        try { await client.destroy(); } catch {}
        await setTokenActive(userId, guildId, false);
        this.tokenManager.clearCache(userId, guildId);
        return false;
      }

      // Wait for ready with timeout
      try {
        await Promise.race([
          this.waitForReady(client),
          delay(READY_TIMEOUT_MS).then(() => { throw new Error('Ready timeout'); })
        ]);
      } catch (readyError) {
        this.asyncLogger.error('Ready timeout', { userId, guildId, error: formatError(readyError) });
        try { await client.destroy(); } catch {}
        await setTokenActive(userId, guildId, false);
        this.tokenManager.clearCache(userId, guildId);
        return false;
      }
      
      this.tokenManager.clearCache(userId, guildId);

      if (this.isShuttingDown) {
        this.asyncLogger.warn('Shutdown in progress, destroying newly created session', {
          userId,
          sessionId: session.sessionId,
        });
        try {
          await client.destroy();
        } catch {}
        return false;
      }

      // Store session with guild-specific key
      this.sessions.set(sessionKey, session);
      this.sessionsByUserId.set(`${userId}:${guildId}`, session);
      this.startHeartbeat(session);
      
      await setTokenActive(userId, guildId, true);
      await updateTokenLastUsed(userId, guildId);

      // Load watchlist for this user
      await this.watchlistManager.loadKeywords(userId);

      this.asyncLogger.info('✅ AutoJoin session started', {
        userId,
        label: session.label,
        username: client.user?.username,
        guilds: client.guilds.cache.size,
        worker: this.workerId,
        sessionId: session.sessionId,
        memory: this.getMemoryUsage(),
      });

      this.emit('sessionStarted', { userId, guildId });
      return true;

    } catch (error) {
      this.asyncLogger.error('Failed to start AutoJoin session', {
        userId,
        guildId,
        error: formatError(error),
        worker: this.workerId,
      });
      await setTokenActive(userId, guildId, false);
      this.tokenManager.clearCache(userId, guildId);
      return false;
    }
  }

  /**
   * Validate token without leaking client - CRITICAL FIX
   * This prevents client instances from being leaked on failed logins
   */
  private async validateToken(token: string): Promise<boolean> {
    const client = new Client();
    try {
      await Promise.race([
        client.login(token),
        delay(10000).then(() => { throw new Error('Login timeout'); })
      ]);
      // Check if client is ready
      if (client.isReady()) {
        return true;
      }
      return false;
    } catch {
      return false;
    } finally {
      // ALWAYS destroy the client to prevent memory leaks
      try {
        await client.destroy();
      } catch {
        // Ignore errors during cleanup
      }
    }
  }

  private async loginWithTimeout(client: Client, token: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Login timeout')), LOGIN_TIMEOUT_MS);
      client.login(token)
        .then(() => { clearTimeout(timeout); resolve(); })
        .catch((err) => { clearTimeout(timeout); reject(err); });
    });
  }

  private async waitForReady(client: Client): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Ready timeout')), READY_TIMEOUT_MS);
      
      if (client.isReady()) {
        clearTimeout(timeout);
        resolve();
        return;
      }
      
      const selfbotClient = client as any;
      selfbotClient.once('ready', () => { clearTimeout(timeout); resolve(); });
      selfbotClient.once('error', (err: Error) => { clearTimeout(timeout); reject(err); });
    });
  }

  // -------------------------------------------------------------------------
  // Heartbeat with reconnect
  // -------------------------------------------------------------------------

  private startHeartbeat(session: UserSession): void {
    if (session.heartbeatInterval) {
      clearInterval(session.heartbeatInterval);
      session.heartbeatInterval = undefined;
    }

    session.heartbeatInterval = setInterval(() => {
      if (this.isShuttingDown || !session.isActive || session.destroyed) return;

      try {
        const client = session.client as any;
        if (!client.isReady()) throw new Error('Client not ready');
        if (client.ws?.connection?.readyState !== 1) throw new Error('WebSocket not open');
      } catch (error) {
        if (session.heartbeatInterval) {
          clearInterval(session.heartbeatInterval);
          session.heartbeatInterval = undefined;
        }
        
        if (session.reconnectAttempts < session.maxReconnectAttempts) {
          session.reconnectAttempts++;
          
          const count = (this.reconnectCount.get(session.userId) || 0) + 1;
          this.reconnectCount.set(session.userId, count);
          
          this.asyncLogger.debug('Attempting reconnect', {
            userId: session.userId,
            guildId: session.guildId,
            attempt: session.reconnectAttempts,
            oldSessionId: session.sessionId,
          });
          
          try {
            (session.client as any).destroy();
          } catch {}
          this.cleanupSessionListeners(session);
          
          session.destroyed = true;
          
          // Remove old session
          const oldKey = this.makeSessionKey(session.userId, session.guildId);
          this.sessions.delete(oldKey);
          this.sessionsByUserId.delete(`${session.userId}:${session.guildId}`);
          
          // Attempt to reconnect with backoff
          const backoffMs = exponentialBackoff(session.reconnectAttempts - 1, 5000, 60000);
          setTimeout(() => {
            this._startSessionInternal(session.userId, session.guildId)
              .then(success => {
                if (success) {
                  const newSession = this.sessionsByUserId.get(`${session.userId}:${session.guildId}`);
                  if (newSession) {
                    newSession.reconnectAttempts = 0;
                    this.asyncLogger.debug('Reconnect successful', {
                      userId: session.userId,
                      guildId: session.guildId,
                      newSessionId: newSession.sessionId,
                    });
                  }
                }
              })
              .catch(() => {});
          }, backoffMs);
        } else {
          this.asyncLogger.error('Max reconnect attempts reached, stopping session', {
            userId: session.userId,
            guildId: session.guildId,
          });
          session.isActive = false;
          this.stopSession(session.userId, session.guildId);
        }
      }
    }, HEARTBEAT_INTERVAL_MS);

    if (session.heartbeatInterval.unref) {
      session.heartbeatInterval.unref();
    }
  }

  async restoreSessionsFromDatabase(): Promise<void> {
    if (!this.checkMemory()) {
      this.asyncLogger.warn('Memory too high, skipping restore', { worker: this.workerId });
      return;
    }

    this.asyncLogger.info('🔄 Restoring AutoJoin sessions from database...', { worker: this.workerId });
    
    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      let restored = 0, failed = 0, skipped = 0;
      
      for (const user of allPremiumUsers) {
        if (!this.checkMemory()) break;
        if (!user.token) { skipped++; continue; }
        const sessionKey = this.makeSessionKey(user.userId, user.guildId);
        if (this.sessions.has(sessionKey)) { skipped++; continue; }
        
        const success = await this.startSession(user.userId, user.guildId);
        if (success) restored++;
        else failed++;
        await delay(500);
      }
      
      this.asyncLogger.info(`✅ Restored ${restored} AutoJoin sessions (${failed} failed, ${skipped} skipped)`, {
        worker: this.workerId,
        total: this.sessions.size,
        memory: this.getMemoryUsage(),
      });
    } catch (error) {
      this.asyncLogger.error('Failed to restore AutoJoin sessions', { error: formatError(error) });
    }
  }

  async stopSession(userId: string, guildId: string): Promise<void> {
    const sessionKey = this.makeSessionKey(userId, guildId);
    const session = this.sessions.get(sessionKey);
    if (!session) return;

    session.destroyed = true;
    session.isActive = false;

    try {
      if (session.heartbeatInterval) {
        clearInterval(session.heartbeatInterval);
        session.heartbeatInterval = undefined;
      }
      
      this.cleanupSessionListeners(session);
      
      try {
        await (session.client as any).destroy();
      } catch {}
      
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(`${userId}:${guildId}`);
      this.tokenManager.clearCache(userId, guildId);
      
      await setTokenActive(userId, guildId, false);
      
      this.asyncLogger.info('⏹️ AutoJoin session stopped', { 
        userId, 
        guildId,
        sessionId: session.sessionId,
        memory: this.getMemoryUsage(),
      });
      
      this.emit('sessionStopped', { userId, guildId });
    } catch (error) {
      this.asyncLogger.error('Failed to stop AutoJoin session', { 
        userId, 
        guildId, 
        error: formatError(error) 
      });
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(`${userId}:${guildId}`);
    }
  }

  private cleanupSessionListeners(session: UserSession): void {
    const { client, listeners } = session;
    
    if (listeners.messageCreate) {
      client.off('messageCreate', listeners.messageCreate);
      delete listeners.messageCreate;
    }
    if (listeners.messageUpdate) {
      client.off('messageUpdate', listeners.messageUpdate);
      delete listeners.messageUpdate;
    }
    if (listeners.error) {
      client.off('error', listeners.error);
      delete listeners.error;
    }
    if (listeners.disconnect) {
      client.off('disconnect', listeners.disconnect);
      delete listeners.disconnect;
    }
    
    try {
      client.removeAllListeners();
    } catch {}
  }

  async refreshSessions(): Promise<void> {
    if (this.isShuttingDown) return;
    if (!this.checkMemory()) {
      this.asyncLogger.warn('Memory too high, skipping refresh', { worker: this.workerId });
      return;
    }

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const activeUserIds = new Set(allPremiumUsers.filter(u => u.token).map(u => `${u.userId}:${u.guildId}`));

      for (const [key, session] of this.sessions) {
        const sessionKey = `${session.userId}:${session.guildId}`;
        if (!activeUserIds.has(sessionKey)) {
          await this.stopSession(session.userId, session.guildId);
        }
      }

      for (const user of allPremiumUsers) {
        if (!this.checkMemory()) break;
        if (!user.token) continue;
        const sessionKey = this.makeSessionKey(user.userId, user.guildId);
        if (!this.sessions.has(sessionKey)) {
          await this.startSession(user.userId, user.guildId);
          await delay(500);
        }
      }

      this.logStats();
    } catch (error) {
      this.asyncLogger.error('Failed to refresh sessions', { error: formatError(error) });
    }
  }

  async retryFailedSessions(): Promise<void> {
    if (this.isShuttingDown) return;
    if (!this.checkMemory()) {
      this.asyncLogger.warn('Memory too high, skipping retry', { worker: this.workerId });
      return;
    }
    
    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const failedUsers = allPremiumUsers.filter(u => u.token && u.tokenActive === false);
      
      for (const user of failedUsers) {
        if (!this.checkMemory()) break;
        const sessionKey = this.makeSessionKey(user.userId, user.guildId);
        if (!this.sessions.has(sessionKey)) {
          // Check if we've been failing this session too much
          const errorHistory = this.sessionStartErrors.get(sessionKey);
          if (errorHistory && errorHistory.count >= 5) {
            const now = Date.now();
            if (now - errorHistory.lastAttempt < 300000) { // 5 minutes
              continue; // Skip if too many recent failures
            }
          }
          await this.startSession(user.userId, user.guildId);
          await delay(2000);
        }
      }
    } catch (error) {
      this.asyncLogger.error('Failed to retry sessions', { error: formatError(error) });
    }
  }

  private makeSessionKey(userId: string, guildId: string): string {
    return `${userId}:${guildId}`;
  }

  // -------------------------------------------------------------------------
  // Event Handlers
  // -------------------------------------------------------------------------

  private registerEvents(session: UserSession): void {
    const { client, userId, guildId } = session;

    const messageCreateHandler = async (message: Message) => {
      if (this.isShuttingDown || !session.isActive || session.destroyed) return;
      
      try {
        this.metrics.totalMessagesProcessed++;
        session.lastHealthCheck = Date.now();
        
        if (!message.guild) {
          await this.handleDmWin(message, userId);
          return;
        }
        if (message.author?.id === client.user?.id) return;
        
        await this.handleWin(message, userId);
        await this.handleMessage(message, session);
      } catch (error) {
        this.asyncLogger.error('Message handler error', { userId, error: formatError(error) });
      }
    };

    const messageUpdateHandler = async (_old: Message | PartialMessage, updated: Message | PartialMessage) => {
      if (this.isShuttingDown || !session.isActive || session.destroyed) return;
      try {
        const entryId = this.makeEntryId(session, updated as Message);
        if (!this.processedMessages.has(entryId)) {
          await this.handleMessage(updated as Message, session);
        }
      } catch (error) {
        this.asyncLogger.error('Message update handler error', { userId, error: formatError(error) });
      }
    };

    const errorHandler = (error: Error) => {
      this.asyncLogger.error('Client error', { userId, error: formatError(error) });
    };

    const disconnectHandler = () => {
      this.asyncLogger.warn('Client disconnected, will attempt reconnect', { userId });
    };

    session.listeners.messageCreate = messageCreateHandler;
    session.listeners.messageUpdate = messageUpdateHandler;
    session.listeners.error = errorHandler;
    session.listeners.disconnect = disconnectHandler;

    client.on('messageCreate', messageCreateHandler);
    client.on('messageUpdate', messageUpdateHandler);
    client.on('error', errorHandler);
    client.on('disconnect', disconnectHandler);
  }

  // -------------------------------------------------------------------------
  // Message Handling with structured tracing
  // -------------------------------------------------------------------------

  private async handleMessage(message: Message, session: UserSession): Promise<void> {
    if (CONFIG.monitoredChannels.length > 0 && 
        !CONFIG.monitoredChannels.includes(message.channel.id)) {
      return;
    }

    const entryId = this.makeEntryId(session, message);

    if (this.processedMessages.has(entryId)) {
      this.metrics.cacheHits++;
      return;
    }
    
    if (this.processingCache.get(entryId) !== undefined) {
      this.metrics.cacheHits++;
      return;
    }
    
    this.metrics.cacheMisses++;

    const crosspostId = this.extractCrosspostId(message);
    if (crosspostId && this.crosspostCache.has(crosspostId)) {
      this.asyncLogger.debug('Skipping cross-posted duplicate', {
        messageId: message.id,
        crosspostSource: this.crosspostCache.get(crosspostId),
      });
      return;
    }
    if (crosspostId) {
      this.crosspostCache.set(crosspostId, message.id);
    }

    if (!this.checkMemory()) return;

    const correlationId = this.correlationTracker.createTrace(session.userId, message.id);
    this.correlationTracker.startStage(correlationId, 'detection');

    const startTime = Date.now();
    const existing = await getAutoJoinEntry(session.userId, message.id, message.channel.id);
    this.metrics.dbQueries++;
    
    if (existing) {
      this.processedMessages.set(entryId, Date.now());
      this.correlationTracker.completeTrace(correlationId, 'completed');
      return;
    }

    this.processingCache.set(entryId, Date.now());
    this.correlationTracker.startStage(correlationId, 'processing');

    try {
      const detected = await this.detectionEngine.detect(message, correlationId);
      this.correlationTracker.endStage(correlationId, 'detection', {
        isGiveaway: detected.isGiveaway,
        confidence: detected.confidence,
        reasons: detected.reasons,
      });

      if (!detected.isGiveaway) {
        this.correlationTracker.completeTrace(correlationId, 'completed');
        this.processingCache.delete(entryId);
        return;
      }

      this.metrics.totalGiveawaysDetected++;
      const detectionTime = Date.now() - startTime;
      this.metrics.recordDetectionTime(detectionTime);

      const watchlistMatches = this.watchlistManager.matchKeyword(
        message.content + ' ' + 
        message.embeds?.map(e => e.title + ' ' + e.description).join(' ') || ''
      );
      
      if (watchlistMatches.length > 0) {
        this.correlationTracker.startStage(correlationId, 'watchlist', {
          matches: watchlistMatches.map(m => m.keyword),
        });
        
        for (const match of watchlistMatches) {
          if (match.matched) {
            saveWatchlistMatch(session.userId, match.keyword, message.id, message.channel.id)
              .catch(() => {});
          }
        }
        
        this.correlationTracker.endStage(correlationId, 'watchlist');
      }

      const entryData: Omit<GiveawayEntry, '_id'> = {
        userId: session.userId,
        messageId: message.id,
        channelId: message.channel.id,
        guildId: message.guild!.id,
        authorId: message.author?.id ?? '',
        guildName: message.guild!.name,
        channelName: (message.channel as { name?: string }).name ?? 'unknown',
        prize: detected.prize || 'Unknown Prize',
        buttonCustomId: detected.button?.customId,
        detectedAt: Date.now(),
        endsAt: this.extractEndTimestamp(message),
        status: 'pending',
        attempts: 0,
        expiresAt: Date.now() + ENTRY_TTL_MS,
        correlationId,
        detectionConfidence: detected.confidence,
        detectionReasons: detected.reasons,
        crosspostSource: this.extractCrosspostId(message),
      };

      await saveAutoJoinEntry(entryData as Omit<AutoJoinEntry, '_id'>);
      this.metrics.dbQueries++;
      
      this.detectionConfidenceBuffer.push({
        messageId: message.id,
        channelId: message.channel.id,
        confidence: detected.confidence,
        reasons: detected.reasons,
      });

      this.processedMessages.set(entryId, Date.now());
      session.stats.detected++;

      this.updateGuildStats(message.guild!.id, message.guild!.name, 'detected', detected.confidence);
      this.updateAccountStats(session.userId, 'detected', detected.confidence, detectionTime);

      this.correlationTracker.startStage(correlationId, 'queueing');

      this.asyncLogger.info('🎯 AutoJoin: Giveaway detected', {
        correlationId,
        userId: session.userId,
        prize: truncate(entryData.prize, 60),
        confidence: detected.confidence.toFixed(2),
        reasons: detected.reasons,
        guild: entryData.guildName,
        worker: this.workerId,
        detectionTime: `${detectionTime}ms`,
      });

      await this.queueOrEnter(entryId, session, entryData as GiveawayEntry, correlationId);

    } catch (error) {
      this.asyncLogger.error('AutoJoin: Handle message error', {
        correlationId,
        userId: session.userId,
        error: formatError(error),
        worker: this.workerId,
      });
      this.correlationTracker.completeTrace(correlationId, 'failed');
    } finally {
      this.processingCache.delete(entryId);
      await cleanupAutoJoinEntries(session.userId);
    }
  }

  // -------------------------------------------------------------------------
  // Queue or Enter Decision
  // -------------------------------------------------------------------------

  private async queueOrEnter(
    entryId: string, 
    session: UserSession, 
    entry: GiveawayEntry, 
    correlationId: string
  ): Promise<void> {
    const queueItem: QueueItem = {
      entryId,
      userId: session.userId,
      guildId: entry.guildId,
      channelId: entry.channelId,
      messageId: entry.messageId,
      priority: entry.endsAt ? Math.max(0, entry.endsAt - Date.now()) : 999999,
      addedAt: Date.now(),
      endsAt: entry.endsAt,
      correlationId,
      attempts: 0,
      maxAttempts: CONFIG.maxRetries + 1,
    };

    const enqueued = this.joinQueue.enqueue(queueItem);

    if (enqueued) {
      await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'queued', {
        queuePosition: this.joinQueue.getGuildQueue(entry.guildId).indexOf(queueItem),
      });
      
      this.correlationTracker.endStage(correlationId, 'queueing', {
        queued: true,
        queueSize: this.joinQueue.getGuildQueue(entry.guildId).length,
      });

      this.processQueue(session.userId, entry.guildId).catch(error => {
        this.asyncLogger.error('Queue processing error', {
          correlationId,
          error: formatError(error),
        });
      });
    } else {
      this.correlationTracker.endStage(correlationId, 'queueing', { queued: false, reason: 'queue_full' });
      await this.enterGiveaway(entryId, session);
    }
  }

  // -------------------------------------------------------------------------
  // Queue Processing
  // -------------------------------------------------------------------------

  private async processQueue(userId: string, guildId: string): Promise<void> {
    const session = this.sessionsByUserId.get(`${userId}:${guildId}`);
    if (!session || !session.isActive) return;

    let item = this.joinQueue.dequeue(guildId);
    while (item) {
      const correlationId = item.correlationId;
      this.correlationTracker.startStage(correlationId, 'queue_processing');

      if (item.endsAt && Date.now() > item.endsAt) {
        this.joinQueue.cancelGiveaway(item.messageId, item.channelId);
        await updateAutoJoinEntryStatus(userId, item.messageId, item.channelId, 'skipped', {
          lastError: 'giveaway_ended',
        });
        this.correlationTracker.completeTrace(correlationId, 'completed');
        item = this.joinQueue.dequeue(guildId);
        continue;
      }

      const queueWait = Date.now() - item.addedAt;
      if (session.stats.queueWaitTimes.length >= METRICS_SAMPLE_SIZE) {
        session.stats.queueWaitTimes.shift();
      }
      session.stats.queueWaitTimes.push(queueWait);

      this.correlationTracker.endStage(correlationId, 'queue_processing', {
        queueWaitMs: queueWait,
      });

      const entryId = this.makeEntryIdFromMessage(userId, item.channelId, item.messageId);
      await this.enterGiveaway(entryId, session);

      item = this.joinQueue.dequeue(guildId);
    }
  }

  // -------------------------------------------------------------------------
  // Enter Giveaway with structured tracing
  // -------------------------------------------------------------------------

  private async enterGiveaway(entryId: string, session: UserSession): Promise<void> {
    const parts = entryId.split(':');
    const userId = parts[0];
    const channelId = parts[1];
    const messageId = parts.slice(2).join(':');
    
    const entry = await getAutoJoinEntry(session.userId, messageId, channelId);
    this.metrics.dbQueries++;
    if (!entry) return;

    const correlationId = entry.correlationId || 
      this.correlationTracker.createTrace(session.userId, messageId);

    this.correlationTracker.startStage(correlationId, 'entry_attempt');

    await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'attempting', {});
    this.metrics.dbQueries++;

    const maxAttempts = CONFIG.maxRetries + 1;
    let entryStartTime = Date.now();

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const attemptNum = attempt + 1;
      this.metrics.totalEntriesAttempted++;
      
      if (attempt > 0) {
        const backoffMs = exponentialBackoff(attempt - 1, CONFIG.retryDelayMs, 30000);
        await delay(backoffMs);
      }

      if (attempt === 2) {
        try {
          const refreshedEntry = await this.refreshButtonData(entry as GiveawayEntry, session);
          if (refreshedEntry && refreshedEntry.buttonCustomId !== entry.buttonCustomId) {
            entry.buttonCustomId = refreshedEntry.buttonCustomId;
          }
        } catch {}
      }

      const cooldownEnd = this.noResponseCooldown.get(session.userId) || 0;
      if (Date.now() < cooldownEnd) {
        await delay(Math.min(cooldownEnd - Date.now(), 5000));
      }

      try {
        const skipped = await this.enterViaButton(entry as GiveawayEntry, session);
        if (skipped) {
          await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'skipped', {});
          this.metrics.dbQueries++;
          this.correlationTracker.completeTrace(correlationId, 'completed');
          return;
        }

        const entryTime = Date.now() - entryStartTime;
        this.metrics.recordEntryTime(entryTime);
        
        session.stats.entered++;
        session.stats.lastEntryAt = Date.now();
        this.metrics.totalEntriesSucceeded++;

        await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'success', { 
          attempts: attemptNum,
        });
        this.metrics.dbQueries++;
        await incrementTokenEntries(session.userId, session.guildId);
        await updateTokenLastUsed(session.userId, session.guildId);

        this.joinOutcomeBuffer.push({
          userId: session.userId,
          messageId: entry.messageId,
          channelId: entry.channelId,
          guildId: entry.guildId,
          status: 'success',
          attempts: attemptNum,
          entryTimeMs: entryTime,
          correlationId,
          timestamp: Date.now(),
        });

        this.updateGuildStats(entry.guildId, entry.guildName, 'entered', entry.detectionConfidence || 0);
        this.updateAccountStats(session.userId, 'entered', entry.detectionConfidence || 0);

        this.correlationTracker.completeTrace(correlationId, 'completed');

        this.asyncLogger.info('✅ AutoJoin: Entered giveaway', {
          correlationId,
          userId: session.userId,
          prize: truncate(entry.prize, 60),
          attempts: attemptNum,
          guild: entry.guildName,
          worker: this.workerId,
          time: `${entryTime}ms`,
        });

        this.emit('giveawayEntered', { entry, userId: session.userId, correlationId });
        return;

      } catch (error) {
        const errorMsg = formatError(error);
        const isNoResponse = errorMsg.includes('No response from Application') || 
                            errorMsg.includes('No response from Application');

        if (isNoResponse && attempt < maxAttempts - 1) {
          this.noResponseCooldown.set(session.userId, Date.now() + NO_RESPONSE_COOLDOWN_MS);
          await delay(2000);
          try { await this.refreshButtonData(entry as GiveawayEntry, session); } catch {}
          continue;
        }

        await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'attempting', { 
          attempts: attemptNum, 
          lastError: errorMsg 
        });
        this.metrics.dbQueries++;
        
        this.asyncLogger.warn(`AutoJoin: Attempt ${attemptNum}/${maxAttempts} failed`, {
          correlationId,
          userId: session.userId,
          entryId,
          error: errorMsg,
          worker: this.workerId,
        });
      }
    }

    // All retries exhausted - move to dead letter queue
    const queueItem: QueueItem = {
      entryId,
      userId: session.userId,
      guildId: entry.guildId,
      channelId: entry.channelId,
      messageId: entry.messageId,
      priority: -1,
      addedAt: Date.now(),
      correlationId,
      attempts: maxAttempts,
      maxAttempts,
      lastError: 'All retries exhausted',
    };

    this.joinQueue.moveToDeadLetter(queueItem, 'All retries exhausted');

    await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'dead_letter', {
      lastError: 'All retries exhausted',
      attempts: maxAttempts,
    });
    this.metrics.dbQueries++;
    session.stats.failed++;
    this.metrics.totalEntriesFailed++;

    this.correlationTracker.completeTrace(correlationId, 'failed');

    this.asyncLogger.error('❌ AutoJoin: All retries exhausted - moved to dead letter', {
      correlationId,
      userId: session.userId,
      prize: truncate(entry.prize, 60),
      attempts: entry.attempts,
      worker: this.workerId,
    });

    this.emit('giveawayFailed', { entry, userId: session.userId, correlationId });
  }

  // -------------------------------------------------------------------------
  // Button Interaction Methods
  // -------------------------------------------------------------------------

  private async refreshButtonData(entry: GiveawayEntry, session: UserSession): Promise<GiveawayEntry | null> {
    try {
      const message = await this.fetchMessage(session.client, entry.channelId, entry.messageId);
      if (!message) return null;
      
      const components = (message as any).components;
      if (!components?.length) return null;
      
      const isKnownBot = this.detectionEngine.getProfile(entry.authorId) !== undefined;
      const button = this.findEntryButton(components, isKnownBot);
      if (button && button.customId !== entry.buttonCustomId) {
        entry.buttonCustomId = button.customId;
        return entry;
      }
      return entry;
    } catch {
      return null;
    }
  }

  private findEntryButton(components: unknown[], isKnownBot: boolean): GiveawayButton | null {
    for (const row of components) {
      const rowAny = row as Record<string, unknown>;
      const rowComps = rowAny['components'] as unknown[] | undefined;
      if (!rowComps?.length) continue;

      for (const comp of rowComps) {
        const c = comp as Record<string, unknown>;
        if (c['type'] !== 2 && c['type'] !== 'BUTTON') continue;
        if (c['style'] === 5 || c['disabled'] === true) continue;

        const customId = (c['customId'] ?? c['custom_id']) as string | undefined;
        if (!customId) continue;

        const label = ((c['label'] as string | undefined) ?? '').trim();

        if (isKnownBot) {
          if (customId.includes('giveaway') || customId.includes('enter') || customId.includes('join')) {
            return { customId, label: label || customId, disabled: false };
          }
        }

        if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) {
          return { customId, label: label || customId, disabled: false };
        }

        if (PATTERNS.ENTRY_BUTTON.test(label)) {
          return { customId, label: label || 'Enter', disabled: false };
        }
      }
    }
    return null;
  }

  private async enterViaButton(entry: GiveawayEntry, session: UserSession): Promise<boolean> {
    if (!entry.buttonCustomId) throw new Error('No buttonCustomId set');

    if (CONFIG.buttonDelayMs > 0) await delay(CONFIG.buttonDelayMs);

    const message = await this.fetchMessage(session.client, entry.channelId, entry.messageId);
    if (!message) throw new Error(`Message ${entry.messageId} not found`);

    let button = this.findButtonById(message, entry.buttonCustomId);
    
    if (!button) {
      const components = (message as any).components;
      if (components?.length) {
        const isKnownBot = this.detectionEngine.getProfile(entry.authorId) !== undefined;
        const foundButton = this.findEntryButton(components, isKnownBot);
        if (foundButton) {
          button = foundButton;
          entry.buttonCustomId = button.customId;
        }
      }
    }

    if (!button || button.disabled) {
      return true;
    }

    await session.rateLimiter.consume();
    await this.clickButton(message, button, session);
    return false;
  }

  private async clickButton(message: Message, button: GiveawayButton, session: UserSession): Promise<void> {
    const selfbotMsg = message as Message & { clickButton?: (id: string) => Promise<unknown> };
    
    if (typeof selfbotMsg.clickButton === 'function') {
      try {
        await selfbotMsg.clickButton(button.customId);
        return;
      } catch (error) {
        const errorMsg = formatError(error);
        if (errorMsg.includes('No responsed from Application') || 
            errorMsg.includes('No response from Application')) {
          await this.postInteraction(message, button, session);
          return;
        }
        throw error;
      }
    }

    await this.postInteraction(message, button, session);
  }

  private async postInteraction(message: Message, button: GiveawayButton, session: UserSession): Promise<void> {
    if (this.apiCircuitBreaker.isOpen()) {
      throw new Error(`Circuit breaker is open (${this.apiCircuitBreaker.getState()})`);
    }

    await this.apiCircuitBreaker.execute(async () => {
      const client = message.client as any;
      const sessionId: string | undefined = client.ws?.shards?.first?.()?.sessionId
        ?? client.ws?.shards?.get?.(0)?.sessionId;

      if (!sessionId) {
        throw new Error('No active gateway session ID available');
      }
      
      const nonce = `${Date.now()}${Math.random().toString(36).slice(2, 8)}`;
      const applicationId = message.author?.id || 
        (message as any).applicationId || 
        (message as any).webhookId ||
        (message as any).interaction?.application_id;

      if (!applicationId) {
        throw new Error('Could not determine application ID for interaction');
      }

      const payload = {
        type: 3,
        nonce,
        guild_id: message.guild?.id ?? null,
        channel_id: message.channel.id,
        message_id: message.id,
        application_id: applicationId,
        session_id: sessionId,
        message_flags: 0,
        data: {
          component_type: 2,
          custom_id: button.customId,
        },
      };

      const token = (message.client as any).token as string;

      for (let attempt = 0; attempt < INTERACTION_RETRY_ATTEMPTS; attempt++) {
        try {
          if (attempt > 0) await delay(INTERACTION_RETRY_DELAY_MS * attempt);

          const startTime = Date.now();
          const response = await this.http.post('https://discord.com/api/v10/interactions', payload, {
            headers: {
              'Authorization': token,
              'Content-Type': 'application/json',
              'Accept': 'application/json',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
              'X-Discord-Locale': 'en-US',
            },
            timeout: 15000,
          });
          
          this.metrics.apiCalls++;
          this.metrics.recordApiLatency(Date.now() - startTime);

          if (response.status === 204 || response.status === 200 || response.status === 201) {
            return;
          }

        } catch (error) {
          this.metrics.apiCalls++;
          this.metrics.apiErrors++;
          
          const axiosErr = error as { response?: { status?: number; data?: { retry_after?: number; message?: string } } };
          const status = axiosErr.response?.status;
          const errorMessage = axiosErr.response?.data?.message;

          if (errorMessage?.includes('No response') || errorMessage?.includes('no response')) {
            if (attempt === INTERACTION_RETRY_ATTEMPTS - 1) {
              throw new Error(`No response from Application after ${INTERACTION_RETRY_ATTEMPTS} attempts`);
            }
            if (attempt === 1) {
              payload.nonce = `${Date.now()}${Math.random().toString(36).slice(2, 8)}`;
              payload.session_id = `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
            }
            continue;
          }

          if (status === 429) {
            const retryAfterMs = Math.ceil((axiosErr.response?.data?.retry_after ?? 1) * 1000);
            await delay(retryAfterMs);
            continue;
          }

          if (status === 401 || status === 403) {
            this.asyncLogger.error('Token appears to be blocked or invalid', { 
              userId: session.userId, 
              status 
            });
            await setTokenActive(session.userId, session.guildId, false);
            throw new Error(`Token ${status === 401 ? 'invalid' : 'blocked'}`);
          }

          if (status === 404 || errorMessage?.includes('unknown interaction')) {
            throw new Error('Interaction expired or button no longer exists');
          }

          if (status === 502 || status === 504 || status === 500) {
            if (attempt === INTERACTION_RETRY_ATTEMPTS - 1) throw error;
            continue;
          }

          if (attempt === INTERACTION_RETRY_ATTEMPTS - 1) throw error;
        }
      }

      throw new Error(`Failed to send interaction after ${INTERACTION_RETRY_ATTEMPTS} attempts`);
    });
  }

  private findButtonById(message: Message, customId: string): GiveawayButton | null {
    const msgAny = message as unknown as Record<string, unknown>;
    const components = msgAny['components'] as unknown[] | undefined;
    if (!components) return null;

    for (const row of components) {
      const rowAny = row as Record<string, unknown>;
      const rowComps = rowAny['components'] as unknown[] | undefined;
      if (!rowComps) continue;

      for (const comp of rowComps) {
        const c = comp as Record<string, unknown>;
        const id = (c['customId'] ?? c['custom_id']) as string | undefined;
        if (id !== customId) continue;
        return {
          customId: id,
          label: ((c['label'] as string | undefined) ?? ''),
          disabled: (c['disabled'] as boolean | undefined) ?? false,
        };
      }
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Win Detection
  // -------------------------------------------------------------------------

  private async handleWin(message: Message, userId: string): Promise<void> {
    if (!message.guild || !message.author?.bot) return;

    const myId = message.client.user?.id;
    if (!myId) return;

    const mentionedInUsers = message.mentions?.users?.has(myId) ?? false;
    const mentionedInContent = (message.content ?? '').includes(myId);
    if (!mentionedInUsers && !mentionedInContent) return;

    const allText = this.extractAllText(message);
    if (!PATTERNS.WIN.test(allText)) return;

    const dedupKey = `${message.channel.id}:${message.author?.id ?? 'unknown'}`;
    if (this.recentWins.get(dedupKey) !== undefined) return;
    this.recentWins.set(dedupKey, Date.now());

    const session = this.findSessionByUserId(userId);
    if (session) {
      session.stats.wins++;
      this.updateGuildStats(message.guild.id, message.guild.name, 'wins');
      this.updateAccountStats(userId, 'wins');
    }
    this.metrics.totalWinsDetected++;

    await incrementTokenWins(userId, session?.guildId || '');

    const prize = this.extractPrize(message);
    const sourceName = `#${(message.channel as { name?: string }).name ?? message.channel.id} in ${message.guild.name}`;

    this.asyncLogger.info('🏆 AutoJoin: WIN DETECTED!', {
      userId,
      prize,
      source: sourceName,
      guild: message.guild.name,
      worker: this.workerId,
    });

    await this.sendWinWebhook(message, prize, sourceName, userId);
    this.emit('giveawayWon', { message, prize, userId });
  }

  private async handleDmWin(message: Message, userId: string): Promise<void> {
    if (message.guild) return;

    const allText = this.extractAllText(message);
    if (!PATTERNS.WIN.test(allText)) return;

    const session = this.findSessionByUserId(userId);
    if (session) {
      session.stats.wins++;
      this.updateAccountStats(userId, 'wins');
    }
    this.metrics.totalWinsDetected++;

    await incrementTokenWins(userId, session?.guildId || '');

    const prize = this.extractPrize(message);

    this.asyncLogger.info('🏆 AutoJoin: WIN DETECTED (DM)!', { 
      userId, 
      prize,
      worker: this.workerId,
    });

    await this.sendWinWebhook(message, prize, 'Direct Message', userId);
    this.emit('giveawayWon', { message, prize, userId, source: 'dm' });
  }

  // -------------------------------------------------------------------------
  // Webhooks
  // -------------------------------------------------------------------------

  private async sendWinWebhook(message: Message, prize: string, sourceName: string, userId: string): Promise<void> {
    const session = this.findSessionByUserId(userId);
    const guildId = session?.guildId || '';

    let url: string | null = null;
    try {
      url = await getUserWebhook(userId, guildId);
    } catch {}

    if (!url) url = CONFIG.winWebhookUrl || CONFIG.webhookUrl || null;
    if (!url) return;

    const guildName = message.guild?.name ?? 'Direct Message';
    const jumpUrl = message.guild
      ? `https://discord.com/channels/${message.guild.id}/${message.channel.id}/${message.id}`
      : null;

    try {
      await this.http.post(url, {
        content: '@everyone',
        username: '🎉 AutoJoin WIN',
        embeds: [{
          title: '🏆 GIVEAWAY WIN!',
          description: jumpUrl ? `[Jump to message](${jumpUrl})` : 'Won via Direct Message',
          color: 0xFFD700,
          fields: [
            { name: '🎁 Prize', value: prize || 'Unknown', inline: false },
            { name: '🏠 Server', value: guildName, inline: true },
            { name: '📢 Source', value: sourceName, inline: true },
            { name: '👤 User', value: `<@${userId}>`, inline: true },
            { name: '⏰ Won At', value: formatTimestamp(Date.now()), inline: false },
          ],
          footer: { text: `AutoJoin • ${url === CONFIG.winWebhookUrl || url === CONFIG.webhookUrl ? 'Global' : 'Personal'} Webhook` },
          timestamp: new Date().toISOString(),
        }],
      }, { timeout: 8000 });
    } catch (error) {
      this.asyncLogger.warn('Win webhook failed', { userId, error: formatError(error) });
    }
  }

  // -------------------------------------------------------------------------
  // Stats Tracking
  // -------------------------------------------------------------------------

  private updateGuildStats(
    guildId: string, 
    guildName: string, 
    stat: 'detected' | 'entered' | 'failed' | 'wins' | 'falsePositives',
    confidence?: number
  ): void {
    if (!this.guildStats.has(guildId)) {
      this.guildStats.set(guildId, {
        guildId,
        guildName,
        detected: 0,
        entered: 0,
        failed: 0,
        wins: 0,
        falsePositives: 0,
        averageConfidence: 0,
        averageQueueWaitMs: 0,
      });
    }

    const stats = this.guildStats.get(guildId)!;
    stats[stat]++;

    if (confidence !== undefined && stat === 'detected') {
      stats.averageConfidence = 
        (stats.averageConfidence * (stats.detected - 1) + confidence) / stats.detected;
    }
  }

  private updateAccountStats(
    userId: string, 
    stat: 'detected' | 'entered' | 'failed' | 'wins' | 'falsePositives',
    confidence?: number,
    detectionMs?: number
  ): void {
    if (!this.accountStats.has(userId)) {
      this.accountStats.set(userId, {
        userId,
        detected: 0,
        entered: 0,
        failed: 0,
        wins: 0,
        falsePositives: 0,
        averageConfidence: 0,
        averageDetectionMs: 0,
        averageQueueWaitMs: 0,
        reconnectCount: 0,
      });
    }

    const stats = this.accountStats.get(userId)!;
    stats[stat]++;

    if (detectionMs !== undefined && stat === 'detected') {
      stats.averageDetectionMs = 
        (stats.averageDetectionMs * (stats.detected - 1) + detectionMs) / stats.detected;
    }
  }

  // -------------------------------------------------------------------------
  // Health Check System
  // -------------------------------------------------------------------------

  private startHealthChecker(): void {
    this.healthCheckInterval = setInterval(async () => {
      if (this.isShuttingDown) return;

      for (const [_, session] of this.sessions) {
        if (!session.isActive || session.destroyed) continue;

        try {
          const client = session.client as any;
          const isConnected = client.isReady() && 
                             client.ws?.connection?.readyState === 1;
          
          if (isConnected) {
            session.lastHealthCheck = Date.now();
            session.stallCount = 0;
          }
        } catch (error) {
          this.asyncLogger.error('Health check error', {
            userId: session.userId,
            error: formatError(error),
          });
        }
      }
    }, HEALTH_CHECK_INTERVAL_MS);

    if (this.healthCheckInterval.unref) this.healthCheckInterval.unref();
  }

  private startStallChecker(): void {
    this.stallCheckInterval = setInterval(() => {
      if (this.isShuttingDown) return;

      const now = Date.now();
      for (const [_, session] of this.sessions) {
        if (!session.isActive || session.destroyed) continue;

        if (now - session.lastHealthCheck > STALL_TIMEOUT_MS) {
          session.stallCount++;
          
          this.asyncLogger.error('Worker stall detected', {
            userId: session.userId,
            sessionId: session.sessionId,
            stallCount: session.stallCount,
          });

          if (session.stallCount >= 3) {
            this.asyncLogger.error('Auto-recovering stalled worker', {
              userId: session.userId,
              sessionId: session.sessionId,
            });
            
            this.stopSession(session.userId, session.guildId).then(() => {
              setTimeout(() => {
                this.startSession(session.userId, session.guildId);
              }, 5000);
            });
          }
        }
      }
    }, STALL_TIMEOUT_MS / 2);

    if (this.stallCheckInterval.unref) this.stallCheckInterval.unref();
  }

  // -------------------------------------------------------------------------
  // Batch Database Writer
  // -------------------------------------------------------------------------

  private startBatchDbWriter(): void {
    this.batchDbInterval = setInterval(async () => {
      if (this.isShuttingDown || this.joinOutcomeBuffer.length === 0) return;

      const outcomes = this.joinOutcomeBuffer.splice(0, this.joinOutcomeBuffer.length);
      const confidences = this.detectionConfidenceBuffer.splice(0, this.detectionConfidenceBuffer.length);

      try {
        if (outcomes.length > 0) {
          await batchSaveJoinOutcomes(outcomes);
          this.metrics.dbQueries++;
        }
        if (confidences.length > 0) {
          await batchUpdateDetectionConfidence(confidences);
          this.metrics.dbQueries++;
        }
      } catch (error) {
        this.asyncLogger.error('Batch DB write failed', { error: formatError(error) });
        this.joinOutcomeBuffer.push(...outcomes.slice(0, 100));
        this.detectionConfidenceBuffer.push(...confidences.slice(0, 100));
      }
    }, BATCH_DB_WRITE_INTERVAL_MS);

    if (this.batchDbInterval.unref) this.batchDbInterval.unref();
  }

  private startArchiveInterval(): void {
    this.archiveInterval = setInterval(async () => {
      if (this.isShuttingDown) return;
      
      try {
        const archived = await archiveOldGiveaways(ARCHIVE_AGE_MS);
        if (archived > 0) {
          this.asyncLogger.info(`Archived ${archived} old giveaways`);
        }
      } catch (error) {
        // Silently fail
      }
    }, 60 * 60 * 1000);

    if (this.archiveInterval.unref) this.archiveInterval.unref();
  }

  private startQueuePersister(): void {
    this.queuePersistInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      this.joinQueue.persist().catch(() => {});
    }, QUEUE_PERSIST_INTERVAL_MS);

    if (this.queuePersistInterval.unref) this.queuePersistInterval.unref();
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private extractCrosspostId(message: Message): string | undefined {
    const match = (message.content || '').match(PATTERNS.CROSSPOST_REFERENCE);
    if (match) {
      return `${match[1]}:${match[2]}:${match[3]}`;
    }
    
    if ((message as any).crosspostReference?.messageId) {
      const ref = (message as any).crosspostReference;
      return `${ref.guildId || ''}:${ref.channelId}:${ref.messageId}`;
    }
    
    return undefined;
  }

  private isKnownGiveawayBot(message: Message): boolean {
    return !!(message.author?.bot && message.author.id && KNOWN_GIVEAWAY_BOT_IDS.has(message.author.id));
  }

  private extractPrize(message: Message): string {
    const embed = message.embeds?.[0];
    if (embed?.title) return this.cleanText(embed.title);
    if (embed?.description) return this.cleanText(embed.description);
    if (message.content) return this.cleanText(message.content);
    return 'Unknown Prize';
  }

  private extractAllText(message: Message): string {
    return [
      message.content ?? '',
      ...message.embeds.flatMap(e => [
        e.title ?? '',
        e.description ?? '',
        e.footer?.text ?? '',
        ...(e.fields ?? []).flatMap(f => [f.name, f.value]),
      ]),
    ].join(' ');
  }

  private extractEndTimestamp(message: Message): number | undefined {
    const allText = this.extractAllText(message);
    const match = allText.match(PATTERNS.TIMESTAMP);
    if (!match?.[1]) return undefined;
    const raw = parseInt(match[1], 10);
    const tsMs = raw < 1e12 ? raw * 1000 : raw;
    return Number.isFinite(tsMs) && tsMs > Date.now() ? tsMs : undefined;
  }

  private cleanText(text: string): string {
    return truncate(sanitizeForLog(text), 200);
  }

  private async fetchMessage(client: Client, channelId: string, messageId: string): Promise<Message | null> {
    try {
      const channel = await client.channels.fetch(channelId);
      if (!channel || !('messages' in channel)) return null;
      return await (channel as TextChannel).messages.fetch(messageId);
    } catch {
      return null;
    }
  }

  private makeEntryId(session: UserSession, message: Message): string {
    return `${session.userId}:${message.channel.id}:${message.id}`;
  }

  private makeEntryIdFromMessage(userId: string, channelId: string, messageId: string): string {
    return `${userId}:${channelId}:${messageId}`;
  }

  private findSessionByUserId(userId: string): UserSession | null {
    // Find session by userId (any guild)
    for (const [key, session] of this.sessions) {
      if (session.userId === userId) {
        return session;
      }
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Interval Starters
  // -------------------------------------------------------------------------

  private startSessionRefresher(): void {
    this.refreshInterval = setInterval(() => {
      if (!this.isShuttingDown && this.checkMemory()) {
        this.refreshSessions().catch((error) => {
          this.asyncLogger.error('Session refresh failed', { error: formatError(error) });
        });
      }
    }, SESSION_REFRESH_INTERVAL_MS);
    if (this.refreshInterval.unref) this.refreshInterval.unref();
  }

  private startCleanupInterval(): void {
    this.cleanupInterval = setInterval(async () => {
      if (this.isShuttingDown) return;
      try {
        for (const [_, session] of this.sessions) {
          await cleanupAutoJoinEntries(session.userId);
        }
      } catch {}
    }, 5 * 60_000);
    if (this.cleanupInterval.unref) this.cleanupInterval.unref();
  }

  private startMemoryCheck(): void {
    this.memoryCheckInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      
      const mem = this.getMemoryUsage();
      
      if (Math.random() < 0.02) {
        this.asyncLogger.debug('💾 Memory status', {
          worker: this.workerId,
          ...mem,
          sessions: this.sessions.size,
          cacheSize: this.processedMessages.size,
        });
      }
      
      this.checkMemory();
    }, 30_000);
    if (this.memoryCheckInterval.unref) this.memoryCheckInterval.unref();
  }

  private startReconnectChecker(): void {
    this.reconnectCheckInterval = setInterval(() => {
      if (!this.isShuttingDown && this.checkMemory()) {
        this.retryFailedSessions().catch((error) => {
          this.asyncLogger.error('Reconnect check failed', { error: formatError(error) });
        });
      }
    }, RECONNECT_DELAY_MS);
    if (this.reconnectCheckInterval.unref) this.reconnectCheckInterval.unref();
  }

  private startCacheCleaner(): void {
    this.cacheCleanInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      
      const cleaned = [
        this.processedMessages.clean(),
        this.processingCache.clean(),
        this.recentWins.clean(),
        this.noResponseCooldown.clean(),
        this.crosspostCache.clean(),
      ].reduce((a, b) => a + b, 0);
      
      if (cleaned > 0) {
        this.asyncLogger.debug(`🧹 Cache cleaner: removed ${cleaned} expired entries`, {
          worker: this.workerId,
        });
      }
    }, 60_000);
    if (this.cacheCleanInterval.unref) this.cacheCleanInterval.unref();
  }

  private startMetricsInterval(): void {
    this.metricsInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      this.logStats();
    }, 5 * 60_000);
    if (this.metricsInterval.unref) this.metricsInterval.unref();
  }

  private logStats(): void {
    const stats = this.getStats();
    const mem = stats.memory;
    
    this.asyncLogger.info('📊 AutoJoin Stats', {
      worker: this.workerId,
      sessions: `${stats.activeSessions}/${stats.totalSessions} active`,
      memory: `${mem.heapUsedMB}MB / 8000MB (${mem.percentageUsed}%)`,
      detected: stats.totalDetected,
      entered: stats.totalEntered,
      wins: stats.totalWins,
      queue: stats.queue,
      caches: stats.caches,
      metrics: stats.metrics,
      health: stats.healthStatus,
      circuitBreaker: stats.circuitBreakerState,
      uptime: `${stats.uptime}m`,
      activeTraces: stats.correlation.activeTraces,
    });
  }

  // -------------------------------------------------------------------------
  // Stats and Health (already defined above)
  // -------------------------------------------------------------------------

  getStats() {
    const sessionStats: Array<{ userId: string; guildId: string; stats: SessionStats }> = [];
    let active = 0;
    let totalDetected = 0;
    let totalEntered = 0;
    let totalWins = 0;
    
    for (const [key, session] of this.sessions) {
      if (session.isActive && !session.destroyed) active++;
      sessionStats.push({ 
        userId: session.userId, 
        guildId: session.guildId,
        stats: { ...session.stats } 
      });
      totalDetected += session.stats.detected;
      totalEntered += session.stats.entered;
      totalWins += session.stats.wins;
    }
    
    const mem = this.getMemoryUsage();
    const metrics = this.metrics.getMetrics();
    
    return {
      totalSessions: this.sessions.size,
      activeSessions: active,
      totalDetected,
      totalEntered,
      totalWins,
      sessionStats,
      worker: this.workerId,
      healthStatus: this.healthStatus,
      circuitBreakerState: this.apiCircuitBreaker.getState(),
      caches: {
        processedMessages: this.processedMessages.size,
        processing: this.processingCache.size,
        recentWins: this.recentWins.size,
        noResponseCooldown: this.noResponseCooldown.size,
        tokenCache: this.tokenManager.getCacheStats(),
        crosspostCache: this.crosspostCache.size,
      },
      memory: {
        heapUsedMB: mem.heapUsedMB,
        heapTotalMB: mem.heapTotalMB,
        rssMB: mem.rssMB,
        percentageUsed: Math.round((mem.heapUsedMB / 8000) * 100),
      },
      metrics: {
        averageDetectionTime: metrics.averageDetectionTime,
        averageEntryTime: metrics.averageEntryTime,
        apiCalls: metrics.apiCalls,
        apiErrors: metrics.apiErrors,
        cacheHits: metrics.cacheHits,
        cacheMisses: metrics.cacheMisses,
        dbQueries: metrics.dbQueries,
      },
      logStats: this.asyncLogger.getStats(),
      sessionStartPromises: this.sessionStartPromises.size,
      uptime: Math.round((Date.now() - metrics.startTime) / 1000 / 60),
      
      // Enhanced monitoring
      queue: this.joinQueue.getStats(),
      detection: this.detectionEngine.getStats(),
      watchlist: this.watchlistManager.getStats(),
      correlation: {
        activeTraces: this.correlationTracker.getActiveTraceCount(),
      },
      guildStats: Array.from(this.guildStats.values()),
      accountStats: Array.from(this.accountStats.values()),
      reconnectCounts: Array.from(this.reconnectCount.entries()).map(([userId, count]) => ({
        userId,
        count,
      })),
      batchBuffers: {
        joinOutcomes: this.joinOutcomeBuffer.length,
        detectionConfidence: this.detectionConfidenceBuffer.length,
      },
    };
  }

  getHealth(): { status: string; details: any } {
    const stats = this.getStats();
    return {
      status: this.healthStatus,
      details: {
        sessions: stats.activeSessions,
        memory: stats.memory,
        circuitBreaker: stats.circuitBreakerState,
        queue: stats.queue,
        uptime: stats.uptime,
      },
    };
  }

  async shutdown(): Promise<void> {
    if (this.isShuttingDown) {
      this.asyncLogger.debug('Shutdown already in progress', { worker: this.workerId });
      return;
    }
    
    this.isShuttingDown = true;

    this.asyncLogger.info('🛑 Shutting down Enhanced AutoJoinManager...', {
      worker: this.workerId,
      sessions: this.sessions.size,
      queueSize: this.joinQueue.getTotalSize(),
      pendingStarts: this.sessionStartPromises.size,
      activeTraces: this.correlationTracker.getActiveTraceCount(),
    });

    // Clear all intervals first
    [
      this.refreshInterval, this.cleanupInterval, this.memoryCheckInterval,
      this.reconnectCheckInterval, this.cacheCleanInterval, this.metricsInterval,
      this.healthCheckInterval, this.queuePersistInterval, this.stallCheckInterval,
      this.batchDbInterval, this.archiveInterval,
    ].forEach(interval => {
      if (interval) { clearInterval(interval); }
    });

    // Persist queue state
    await this.joinQueue.persist();

    // Flush batch buffers
    if (this.joinOutcomeBuffer.length > 0 || this.detectionConfidenceBuffer.length > 0) {
      try {
        await batchSaveJoinOutcomes(this.joinOutcomeBuffer);
        await batchUpdateDetectionConfidence(this.detectionConfidenceBuffer);
      } catch {}
      this.joinOutcomeBuffer = [];
      this.detectionConfidenceBuffer = [];
    }

    // Wait for pending session starts
    if (this.sessionStartPromises.size > 0) {
      this.asyncLogger.info(`Waiting for ${this.sessionStartPromises.size} pending session starts...`);
      try {
        await Promise.race([
          Promise.allSettled(this.sessionStartPromises.values()),
          delay(5000),
        ]);
      } catch {}
      this.sessionStartPromises.clear();
    }

    // Stop all sessions
    const sessionsToStop = Array.from(this.sessions.values());
    
    for (const session of sessionsToStop) {
      session.destroyed = true;
      session.isActive = false;
      
      if (session.heartbeatInterval) {
        clearInterval(session.heartbeatInterval);
        session.heartbeatInterval = undefined;
      }
      
      this.cleanupSessionListeners(session);
      
      try {
        await (session.client as any).destroy();
      } catch {}
      
      this.sessions.delete(this.makeSessionKey(session.userId, session.guildId));
      this.sessionsByUserId.delete(`${session.userId}:${session.guildId}`);
      this.tokenManager.clearCache(session.userId, session.guildId);
    }

    // Clear everything
    this.sessions.clear();
    this.sessionsByUserId.clear();
    this.processedMessages.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    this.noResponseCooldown.clear();
    this.crosspostCache.clear();
    this.tokenManager.clearAll();
    this.sessionStartPromises.clear();
    this.guildStats.clear();
    this.accountStats.clear();
    this.reconnectCount.clear();
    
    this.asyncLogger.shutdown();
    
    if (global.gc) {
      global.gc();
    }
    
    this.asyncLogger.info('✅ AutoJoin shutdown complete', { 
      worker: this.workerId,
      memory: this.getMemoryUsage(),
    });
  }
}
