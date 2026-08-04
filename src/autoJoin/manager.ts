/**
 * @module autoJoin/manager
 * 
 * Premium AutoJoiner - PRODUCTION GRADE - MEMORY SAFE
 * 
 * FIXED: 🔥 MEMORY - No longer kills sessions due to memory pressure
 * FIXED: 🔥 SESSIONS - Auto-reactivation of inactive tokens
 * FIXED: 🔥 RETRY - Exponential backoff for failed logins
 * FIXED: 🔥 CACHING - Proper LRU cache management without session killing
 * FIXED: 🔥 PERSISTENCE - Tokens survive network hiccups
 * FIXED: 🔥 STABILITY - Session count remains stable over time
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

// SUPPRESS the token-unavailable flood from discord.js-selfbot-v13 internals
process.on('unhandledRejection', (reason: any) => {
  if (reason?.code === 500 && reason?.message?.includes('token was unavailable')) {
    return;
  }
  logger.error('[Process] Unhandled rejection', { reason: formatError(reason) });
});

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
  decryptedToken: string;
  loginFailures: number;
  lastLoginAttempt: number;
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
// Constants
// ---------------------------------------------------------------------------

const GIVEAWAY_BOT_ID = '530082442967646230';

const GIVEAWAY_BOT_NAMES = new Set(['GiveawayBot', 'Giveaway Bot']);

const KNOWN_GIVEAWAY_BOT_IDS: ReadonlySet<string> = new Set([
  '530082442967646230',
  '294882584201003009',
  '739448630517039104',
  '515195524879237130',
  '235148962103951360',
  '282859044593598464',
  '270904126974590976',
  '508391840525975553',
]);

const TRUSTED_ENTRY_CUSTOM_IDS: ReadonlySet<string> = new Set([
  'giveaway_message',
  'giveaway-enter',
  'enter_giveaway',
  'giveaway_enter',
  'join_giveaway',
  'giveaway-join',
  'giveaway_participate',
  'participate_giveaway',
  'enter',
  'participants',
]);

const BLOCKED_MESSAGE_PATTERNS: ReadonlyArray<RegExp> = [
  /already\s+entered\s+this\s+giveaway/i,
  /you(?:'ve|\s+have)\s+already\s+entered/i,
  /you\s+are\s+already\s+(?:in|entered|participating)/i,
  /you(?:'ve|\s+have)\s+already\s+(?:joined|joined\s+this)/i,
  /leave\s+giveaway/i,
];

const BLOCKED_BUTTON_PATTERNS: ReadonlyArray<RegExp> = [
  /\bleave\b/i,
  /\bquit\b/i,
  /\bexit\b/i,
  /\bunenter\b/i,
  /\bwithdraw\b/i,
  /remove\s+entry/i,
  /cancel\s+entry/i,
  /cancel\s+giveaway/i,
  /end\s+giveaway/i,
];

const ENTRY_BUTTON_PATTERNS: ReadonlyArray<RegExp> = [
  /\benter\b/i,
  /\bjoin\b/i,
  /\bparticipate\b/i,
  /\braffle\b/i,
  /\bsweepstakes\b/i,
  /\bsubmit\b/i,
  /count\s+me\s+in/i,
  /\bgiveaway\b/i,
  /🎉/,
  /🎁/,
  /🏆/,
  /^\d[\d,]*$/,
];

const WIN_PATTERNS: ReadonlyArray<RegExp> = [
  /congratulations?[^.!?\n]{0,60}(?:you|won)/i,
  /you(?:'ve|\s+have)\s+won/i,
  /you\s+won\s/i,
  /you\s+are\s+(?:a\s+)?(?:the\s+)?winner/i,
  /\bwinner[s]?\b/i,
  /has\s+won\s+(?:the\s+)?giveaway/i,
  /won\s+the\s+giveaway/i,
  /won\s+(?:a\s+)?(?:the\s+)?(?:prize|raffle|giveaway)/i,
  /🎉\s*congrat/i,
  /🏆\s*(?:congrat|winner|you)/i,
];

const PATTERNS = {
  TIMESTAMP: /<t:(\d{10,13})(?::[a-zA-Z])?>/,
} as const;

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
const MAX_RECONNECT_ATTEMPTS = 5; // 🔥 FIX: Increased from 3 to 5
const RECONNECT_DELAY_MS = 60000;
const INTERACTION_RETRY_ATTEMPTS = 3;
const INTERACTION_RETRY_DELAY_MS = 2000;
const NO_RESPONSE_COOLDOWN_MS = 5000;
const BATCH_DB_WRITE_INTERVAL_MS = 5000;
const ARCHIVE_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_QUEUE_SIZE = 1000;
const MAX_QUEUE_PER_GUILD = 50;
const DEAD_LETTER_RETENTION_MS = 24 * 60 * 60 * 1000;
const QUEUE_PERSIST_INTERVAL_MS = 60000;

const CACHE_PROCESSED_MESSAGES = 5000;
const CACHE_MAX_PROCESSING = 1000;
const CACHE_MAX_WINS = 200;
const CACHE_MAX_COOLDOWN = 100;
const CACHE_MAX_TOKEN = 50;
const CACHE_CROSSPOST = 1000;

const MEMORY_WARNING_THRESHOLD_MB = 3000;
const MEMORY_CRITICAL_THRESHOLD_MB = 4500;
const MEMORY_MAX_THRESHOLD_MB = 5500;

const MAX_LOG_QUEUE_SIZE = 1000;
const MAX_SESSION_START_PROMISES = 50;

const HTTP_MAX_SOCKETS = 30;
const HTTP_MAX_FREE_SOCKETS = 15;

const CIRCUIT_BREAKER_THRESHOLD = 20;
const CIRCUIT_BREAKER_TIMEOUT_MS = 30000;
const CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS = 3;

const METRICS_SAMPLE_SIZE = 100;

// 🔥 FIX: New constants for retry logic
const RETRY_CHECK_INTERVAL_MS = 5 * 60 * 1000; // Check every 5 minutes
const INITIAL_RETRY_DELAY_MS = 5 * 60 * 1000; // 5 minutes
const MAX_RETRY_DELAY_MS = 4 * 60 * 60 * 1000; // 4 hours
const TOKEN_REACTIVATION_THRESHOLD_MS = 30 * 60 * 1000; // 30 minutes

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
// AutoJoinManager - Main Class
// ---------------------------------------------------------------------------

export class AutoJoinManager extends EventEmitter {
  // Sessions
  private sessions: Map<string, UserSession> = new Map();
  private sessionsByUserId: Map<string, UserSession> = new Map();
  
  // Caches
  private processedMessages: LRUCache<string, number>;
  private processingCache: LRUCache<string, number>;
  private recentWins: LRUCache<string, number>;
  private noResponseCooldown: LRUCache<string, number>;
  private crosspostCache: LRUCache<string, string>;
  
  // Systems
  private joinQueue: JoinQueue;
  
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
  private statsCleanInterval: NodeJS.Timeout | null = null;
  
  // State
  private isShuttingDown = false;
  private sessionStartPromises: Map<string, Promise<boolean>> = new Map();
  private workerId: string;
  private memoryWarningLogged = false;
  private healthStatus: 'healthy' | 'warning' | 'critical' = 'healthy';
  private sessionIdCounter = 0;
  private memoryCriticalLogged = false;
  private lastMemoryCheck = 0;

  // Batch write buffers
  private joinOutcomeBuffer: any[] = [];
  
  // Stats (bounded LRU caches)
  private guildStatsCache: LRUCache<string, GuildStats>;
  private accountStatsCache: LRUCache<string, AccountStats>;
  private reconnectCountMap: Map<string, number> = new Map();

  // 🔥 FIX: Retry scheduler
  private retryScheduled: Map<string, NodeJS.Timeout> = new Map();
  private tokenFailureTracker: Map<string, { failures: number; lastAttempt: number }> = new Map();

  // HTTP client and agents
  private httpAgent: http.Agent;
  private httpsAgent: https.Agent;
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
    
    // Initialize systems
    this.joinQueue = new JoinQueue();
    
    // Initialize managers
    this.tokenManager = new TokenManager();
    this.asyncLogger = new AsyncLogger();
    this.apiCircuitBreaker = new CircuitBreaker();
    this.metrics = new MetricsCollector();

    // Stats caches
    this.guildStatsCache = new LRUCache<string, GuildStats>(2000, 24 * 60 * 60 * 1000);
    this.accountStatsCache = new LRUCache<string, AccountStats>(2000, 24 * 60 * 60 * 1000);

    // HTTP agents
    this.httpAgent = new http.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets: HTTP_MAX_SOCKETS,
      maxFreeSockets: HTTP_MAX_FREE_SOCKETS,
      scheduling: 'lifo',
    });
    
    this.httpsAgent = new https.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets: HTTP_MAX_SOCKETS,
      maxFreeSockets: HTTP_MAX_FREE_SOCKETS,
      scheduling: 'lifo',
    });

    this.http = axios.create({
      timeout: 10_000,
      httpAgent: this.httpAgent,
      httpsAgent: this.httpsAgent,
    });

    this.apiCircuitBreaker.reset();

    this.initialize().catch(error => {
      this.asyncLogger.error('Failed to initialize AutoJoinManager', { error: formatError(error) });
    });

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
    this.startStatsCleaner();

    this.asyncLogger.info('🚀 AutoJoinManager initialized', {
      worker: this.workerId,
      memory: this.getMemoryUsage(),
    });
  }

  private async initialize(): Promise<void> {
    await this.joinQueue.restore();
  }

  // -------------------------------------------------------------------------
  // Memory Management - FIXED: No session killing
  // -------------------------------------------------------------------------

  private getMemoryUsage(): { heapUsedMB: number; heapTotalMB: number; rssMB: number } {
    const mem = process.memoryUsage();
    return {
      heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
      rssMB: Math.round(mem.rss / 1024 / 1024),
    };
  }

  // 🔥 FIX: Memory check now ONLY cleans caches, NEVER kills sessions
  private checkMemory(): boolean {
    const now = Date.now();
    if (now - this.lastMemoryCheck < 5000) return true;
    this.lastMemoryCheck = now;

    const mem = this.getMemoryUsage();
    
    // Clean caches if memory is high
    if (mem.heapUsedMB > MEMORY_WARNING_THRESHOLD_MB) {
      this.processedMessages.clean();
      this.processingCache.clean();
      this.recentWins.clean();
      this.noResponseCooldown.clean();
      this.crosspostCache.clean();
      
      if (mem.heapUsedMB > MEMORY_CRITICAL_THRESHOLD_MB) {
        this.aggressiveCleanup();
        if (global.gc) {
          global.gc();
        }
      }
    }
    
    // 🔥 FIX: Log warning but NEVER kill sessions
    if (mem.heapUsedMB > MEMORY_CRITICAL_THRESHOLD_MB) {
      this.asyncLogger.warn('⚠️ High memory usage, cleaning caches aggressively', {
        heapUsedMB: mem.heapUsedMB,
        sessions: this.sessions.size,
        active: Array.from(this.sessions.values()).filter(s => s.isActive && !s.destroyed).length
      });
      this.healthStatus = 'warning';
    } else if (mem.heapUsedMB > MEMORY_MAX_THRESHOLD_MB) {
      this.healthStatus = 'critical';
      this.asyncLogger.error('⚠️ CRITICAL: Memory very high but preserving sessions', {
        heapUsedMB: mem.heapUsedMB,
        sessions: this.sessions.size
      });
      // Force aggressive cleanup but don't kill sessions
      this.aggressiveCleanup();
      if (global.gc) global.gc();
    } else {
      this.healthStatus = 'healthy';
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
    this.guildStatsCache.clean();
    this.accountStatsCache.clean();
    
    // Don't clear reconnectCountMap as it tracks session health
    
    if (global.gc) {
      global.gc();
    }
  }

  // 🔥 FIX: REMOVED forceCleanup - no longer kills sessions
  // The old forceCleanup has been replaced by aggressiveCleanup above

  // FIX: Clear Discord.js internal caches to free memory
  private clearClientCaches(client: Client): void {
    try {
      (client as any).guilds?.cache?.clear?.();
      (client as any).users?.cache?.clear?.();
      (client as any).channels?.cache?.clear?.();
      (client as any).emojis?.cache?.clear?.();
    } catch {
      // ignore
    }
  }

  // 🔥 MEMORY FIX: Remove message from cache after processing
  private purgeMessageFromCache(message: Message): void {
    try {
      (message.channel as TextChannel)?.messages?.cache?.delete(message.id);
    } catch {
      // ignore
    }
  }

  // 🔥 MEMORY FIX: Fetch message WITHOUT adding to Discord.js cache
  private async fetchMessageUncached(client: Client, channelId: string, messageId: string): Promise<Message | null> {
    try {
      const channel = await client.channels.fetch(channelId, { force: true, cache: false });
      if (!channel || !('messages' in channel)) return null;
      
      const message = await (channel as TextChannel).messages.fetch(messageId, {
        force: true,
        cache: false,
      }) as Message;
      
      // Immediately remove from cache if it was added anyway
      try {
        (channel as TextChannel).messages.cache.delete(messageId);
        (client as any).channels?.cache?.delete(channelId);
      } catch {
        // ignore
      }
      
      return message;
    } catch {
      return null;
    }
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async startAllSessions(): Promise<void> {
    if (!this.checkMemory()) return;

    this.asyncLogger.info(`🚀 Starting AutoJoin sessions (worker: ${this.workerId})...`);

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      
      // 🔥 FIX: Log what we found
      this.asyncLogger.info(`📊 Found ${allPremiumUsers.length} premium users`, {
        withTokens: allPremiumUsers.filter(u => u.token).length,
        active: allPremiumUsers.filter(u => u.tokenActive !== false).length
      });
      
      const validUsers = allPremiumUsers.filter(u => u.token);
      const usersToStart = validUsers.slice(0, MAX_SESSIONS_PER_WORKER);

      // 🔥 FIX: Increased concurrency for faster startup
      const concurrencyLimit = Math.min(5, usersToStart.length);
      let started = 0;
      let failed = 0;
      
      for (let i = 0; i < usersToStart.length; i += concurrencyLimit) {
        const batch = usersToStart.slice(i, i + concurrencyLimit);
        
        if (!this.checkMemory()) break;
        
        this.asyncLogger.info(`🔄 Starting batch ${Math.floor(i/concurrencyLimit) + 1}: ${batch.length} users`);
        
        const results = await Promise.allSettled(
          batch.map(user => this.startSession(user.userId, user.guildId))
        );
        
        for (let idx = 0; idx < results.length; idx++) {
          const result = results[idx];
          if (result.status === 'fulfilled' && result.value) {
            started++;
          } else {
            failed++;
            this.asyncLogger.warn(`❌ Failed to start ${batch[idx].userId}`, {
              error: result.status === 'rejected' ? formatError(result.reason) : 'Returned false'
            });
          }
        }
        
        // 🔥 FIX: Reduced delay between batches
        if (i + concurrencyLimit < usersToStart.length) {
          await delay(500);
        }
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
      return [];
    }
  }

  async startSession(userId: string, guildId: string): Promise<boolean> {
    const sessionKey = this.makeSessionKey(userId);
    
    if (this.sessions.has(sessionKey)) {
      const session = this.sessions.get(sessionKey);
      if (session && session.isActive && !session.destroyed) {
        return true;
      } else {
        // Session exists but is dead, remove it
        this.sessions.delete(sessionKey);
        this.sessionsByUserId.delete(userId);
      }
    }
    
    if (this.sessions.size >= MAX_SESSIONS_PER_WORKER) {
      this.asyncLogger.warn('⚠️ Max sessions reached', { 
        current: this.sessions.size, 
        max: MAX_SESSIONS_PER_WORKER 
      });
      return false;
    }

    if (this.sessionStartPromises.has(sessionKey)) {
      return this.sessionStartPromises.get(sessionKey)!;
    }

    const startPromise = this._startSessionInternal(userId, guildId);
    this.sessionStartPromises.set(sessionKey, startPromise);

    try {
      const result = await startPromise;
      return result;
    } finally {
      this.sessionStartPromises.delete(sessionKey);
    }
  }

  private async _startSessionInternal(userId: string, guildId: string): Promise<boolean> {
    const sessionKey = this.makeSessionKey(userId);

    try {
      const user = await getPremiumUser(userId, guildId);
      if (!user?.token) {
        this.asyncLogger.warn('No token found for user', { userId, guildId });
        return false;
      }

      let decryptedToken: string;
      try {
        decryptedToken = await this.tokenManager.getDecryptedToken(userId, guildId, user.token);
      } catch (error) {
        this.asyncLogger.error('❌ Failed to decrypt token', {
          userId, guildId, error: formatError(error), worker: this.workerId,
        });
        await setTokenActive(userId, guildId, false);
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
      
      try {
        await this.loginWithTimeout(client, decryptedToken);
        await this.waitForReady(client);
      } catch (loginError) {
        const errorMsg = formatError(loginError);
        
        // 🔥 FIX: Differentiate between permanent and temporary failures
        const isPermanent = 
          errorMsg.includes('Invalid token') ||
          errorMsg.includes('401') ||
          errorMsg.includes('Unauthorized') ||
          errorMsg.includes('incorrect login') ||
          errorMsg.includes('incorrect password');
        
        const isTemporary =
          errorMsg.includes('ETIMEDOUT') ||
          errorMsg.includes('ECONNRESET') ||
          errorMsg.includes('503') ||
          errorMsg.includes('502') ||
          errorMsg.includes('429') ||
          errorMsg.includes('Login timeout') ||
          errorMsg.includes('Ready timeout') ||
          errorMsg.includes('ECONNREFUSED');
        
        if (isPermanent) {
          // 🔥 PERMANENT: Mark inactive and never retry
          this.asyncLogger.error('❌ Permanent token failure - marking inactive', {
            userId, guildId, error: errorMsg, worker: this.workerId,
          });
          await setTokenActive(userId, guildId, false);
          this.tokenManager.clearCache(userId, guildId);
          this.emit('tokenRevoked', { userId, guildId, error: errorMsg });
        } else if (isTemporary) {
          // 🔥 TEMPORARY: Keep active, retry later
          this.asyncLogger.warn('⚠️ Temporary login failure, will retry later', { 
            userId, guildId, error: errorMsg, worker: this.workerId,
          });
          // Don't mark inactive, just schedule retry
          await this.scheduleRetry(userId, guildId);
        } else {
          // 🔥 UNKNOWN: Try again later
          this.asyncLogger.warn('⚠️ Unknown login failure, scheduling retry', { 
            userId, guildId, error: errorMsg, worker: this.workerId,
          });
          await this.scheduleRetry(userId, guildId);
        }
        
        this.clearClientCaches(client);
        try { client.removeAllListeners(); } catch {}
        try { await client.destroy(); } catch {}
        
        return false;
      }
      
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
        decryptedToken,
        loginFailures: 0,
        lastLoginAttempt: Date.now(),
      };

      this.registerEvents(session);
      
      this.tokenManager.clearCache(userId, guildId);

      if (this.isShuttingDown) {
        this.clearClientCaches(client);
        try { client.removeAllListeners(); } catch {}
        try { await client.destroy(); } catch {}
        return false;
      }

      this.sessions.set(sessionKey, session);
      this.sessionsByUserId.set(userId, session);
      this.startHeartbeat(session);
      
      // Clear failure tracking on successful login
      this.tokenFailureTracker.delete(userId);
      
      await setTokenActive(userId, guildId, true);
      await updateTokenLastUsed(userId, guildId);

      this.asyncLogger.info('✅ AutoJoin session started', {
        userId, label: session.label, username: client.user?.username,
        guilds: client.guilds.cache.size, worker: this.workerId,
        sessionId: session.sessionId, memory: this.getMemoryUsage(),
      });

      this.emit('sessionStarted', { userId, guildId });
      return true;

    } catch (error) {
      this.asyncLogger.error('Failed to start AutoJoin session', {
        userId, guildId, error: formatError(error), worker: this.workerId,
      });
      
      // Schedule retry for unknown errors
      await this.scheduleRetry(userId, guildId);
      return false;
    }
  }

  private async loginWithTimeout(client: Client, token: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Login timeout')), 15000);
      client.login(token)
        .then(() => { clearTimeout(timeout); resolve(); })
        .catch((err) => { clearTimeout(timeout); reject(err); });
    });
  }

  private async waitForReady(client: Client): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Ready timeout')), 10000);
      
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
  // Retry Scheduler - NEW
  // -------------------------------------------------------------------------

  private async scheduleRetry(userId: string, guildId: string): Promise<void> {
    const key = `${userId}:${guildId}`;
    
    // Clear existing schedule
    if (this.retryScheduled.has(key)) {
      clearTimeout(this.retryScheduled.get(key)!);
      this.retryScheduled.delete(key);
    }
    
    // Track failures
    const failureData = this.tokenFailureTracker.get(userId) || { failures: 0, lastAttempt: Date.now() };
    failureData.failures++;
    failureData.lastAttempt = Date.now();
    this.tokenFailureTracker.set(userId, failureData);
    
    // Calculate backoff
    const backoffMs = Math.min(
      INITIAL_RETRY_DELAY_MS * Math.pow(2, Math.min(failureData.failures - 1, 6)),
      MAX_RETRY_DELAY_MS
    );
    
    this.asyncLogger.info(`⏰ Scheduling retry for ${userId} in ${Math.round(backoffMs/1000)}s (attempt #${failureData.failures})`);
    
    const timeout = setTimeout(async () => {
      this.retryScheduled.delete(key);
      if (!this.isShuttingDown) {
        this.asyncLogger.info(`🔄 Retrying session for ${userId}`);
        const success = await this.startSession(userId, guildId);
        if (!success) {
          // If still failing, reschedule
          const data = this.tokenFailureTracker.get(userId);
          if (data && data.failures < 10) { // Max 10 retries
            await this.scheduleRetry(userId, guildId);
          } else {
            this.asyncLogger.error('❌ Max retry attempts reached for user', { userId });
            await setTokenActive(userId, guildId, false);
            this.tokenFailureTracker.delete(userId);
          }
        } else {
          // Success, clear failure tracking
          this.tokenFailureTracker.delete(userId);
        }
      }
    }, backoffMs);
    
    this.retryScheduled.set(key, timeout);
    if (timeout.unref) timeout.unref();
  }

  // -------------------------------------------------------------------------
  // Heartbeat with reconnect - FIXED
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
        
        session.lastHealthCheck = Date.now();
        session.stallCount = 0; // Reset stall count on successful heartbeat
        
      } catch (error) {
        if (session.heartbeatInterval) {
          clearInterval(session.heartbeatInterval);
          session.heartbeatInterval = undefined;
        }
        
        if (session.reconnectAttempts < session.maxReconnectAttempts) {
          session.reconnectAttempts++;
          
          const count = (this.reconnectCountMap.get(session.userId) || 0) + 1;
          this.reconnectCountMap.set(session.userId, count);
          
          this.asyncLogger.warn('🔄 Session heartbeat failed, reconnecting', {
            userId: session.userId,
            attempt: session.reconnectAttempts,
            maxAttempts: session.maxReconnectAttempts
          });
          
          this.clearClientCaches(session.client);
          this.cleanupSessionListeners(session);
          
          try { (session.client as any).destroy(); } catch {}
          
          session.destroyed = true;
          
          this.startSession(session.userId, session.guildId)
            .then(success => {
              if (success) {
                const newSession = this.sessionsByUserId.get(session.userId);
                if (newSession) newSession.reconnectAttempts = 0;
                this.asyncLogger.info('✅ Session reconnected successfully', { userId: session.userId });
              }
            })
            .catch(() => {});
        } else {
          this.asyncLogger.error('❌ Max reconnect attempts reached, scheduling retry', {
            userId: session.userId,
          });
          session.isActive = false;
          // 🔥 FIX: Schedule retry instead of permanent deactivation
          this.scheduleRetry(session.userId, session.guildId);
          this.stopSession(session.userId, session.guildId);
        }
      }
    }, HEARTBEAT_INTERVAL_MS);

    if (session.heartbeatInterval.unref) {
      session.heartbeatInterval.unref();
    }
  }

  async restoreSessionsFromDatabase(): Promise<void> {
    if (!this.checkMemory()) return;

    this.asyncLogger.info('🔄 Restoring AutoJoin sessions from database...', { worker: this.workerId });
    
    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      let restored = 0, failed = 0, skipped = 0;
      
      for (const user of allPremiumUsers) {
        if (!this.checkMemory()) break;
        if (!user.token) { skipped++; continue; }
        if (this.sessions.has(this.makeSessionKey(user.userId))) { skipped++; continue; }
        
        const success = await this.startSession(user.userId, user.guildId);
        if (success) restored++;
        else failed++;
        await delay(500);
      }
      
      this.asyncLogger.info(`✅ Restored ${restored} AutoJoin sessions (${failed} failed, ${skipped} skipped)`, {
        worker: this.workerId, total: this.sessions.size, memory: this.getMemoryUsage(),
      });
    } catch (error) {
      this.asyncLogger.error('Failed to restore AutoJoin sessions', { error: formatError(error) });
    }
  }

  async stopSession(userId: string, guildId: string): Promise<void> {
    const sessionKey = this.makeSessionKey(userId);
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
      this.clearClientCaches(session.client);
      
      try { 
        session.client.removeAllListeners();
        await (session.client as any).destroy(); 
      } catch {}
      
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(userId);
      this.tokenManager.clearCache(userId, guildId);
      
      this.asyncLogger.info('⏹️ AutoJoin session stopped', { 
        userId, guildId, sessionId: session.sessionId, memory: this.getMemoryUsage(),
      });
      
      this.emit('sessionStopped', { userId, guildId });
    } catch (error) {
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(userId);
    }
  }

  private cleanupSessionListeners(session: UserSession): void {
    const { client } = session;
    
    if (session.listeners.messageCreate) {
      client.off('messageCreate', session.listeners.messageCreate);
    }
    if (session.listeners.messageUpdate) {
      client.off('messageUpdate', session.listeners.messageUpdate);
    }
    if (session.listeners.error) {
      client.off('error', session.listeners.error);
    }
    if (session.listeners.disconnect) {
      client.off('disconnect', session.listeners.disconnect);
    }
    
    session.listeners.messageCreate = undefined;
    session.listeners.messageUpdate = undefined;
    session.listeners.error = undefined;
    session.listeners.disconnect = undefined;
    
    try { 
      client.removeAllListeners('messageCreate');
      client.removeAllListeners('messageUpdate');
      client.removeAllListeners('error');
      client.removeAllListeners('disconnect');
      client.removeAllListeners('ready');
      client.removeAllListeners('warn');
      client.removeAllListeners('debug');
      (client as any).ws?.removeAllListeners?.();
    } catch {}
  }

  async refreshSessions(): Promise<void> {
    if (this.isShuttingDown) return;
    if (!this.checkMemory()) return;

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const activeUserIds = new Set(allPremiumUsers.filter(u => u.token).map(u => u.userId));

      // Clean up sessions for users no longer premium
      for (const [key, session] of this.sessions) {
        if (!activeUserIds.has(session.userId)) {
          await this.stopSession(session.userId, session.guildId);
        }
      }

      // Start sessions for new premium users
      for (const user of allPremiumUsers) {
        if (!this.checkMemory()) break;
        if (!user.token) continue;
        const sessionKey = this.makeSessionKey(user.userId);
        if (!this.sessions.has(sessionKey)) {
          await this.startSession(user.userId, user.guildId);
          await delay(200);
        }
      }

      this.logStats();
    } catch (error) {
      this.asyncLogger.error('Failed to refresh sessions', { error: formatError(error) });
    }
  }

  // 🔥 FIX: Improved retry logic for failed sessions
  async retryFailedSessions(): Promise<void> {
    if (this.isShuttingDown) return;
    if (!this.checkMemory()) return;
    
    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const now = Date.now();
      
      for (const user of allPremiumUsers) {
        if (!user.token) continue;
        
        const sessionKey = this.makeSessionKey(user.userId);
        const hasSession = this.sessions.has(sessionKey);
        const session = this.sessions.get(sessionKey);
        
        // Check if session exists but is dead
        const isDeadSession = hasSession && session && (!session.isActive || session.destroyed);
        
        // Check if token is inactive
        const isInactive = user.tokenActive === false;
        
        // Check if we should retry (after cooldown)
        const lastAttempt = user.lastLoginAttempt || 0;
        const cooldownMs = TOKEN_REACTIVATION_THRESHOLD_MS;
        const shouldRetry = (isDeadSession || isInactive) && (now - lastAttempt > cooldownMs);
        
        if (shouldRetry) {
          this.asyncLogger.info(`🔄 Reactivating session for ${user.userId}`, {
            currentStatus: user.tokenActive,
            hasSession,
            isActive: session?.isActive,
            destroyed: session?.destroyed,
            lastAttempt: new Date(lastAttempt).toISOString()
          });
          
          // Clean up dead session
          if (isDeadSession) {
            this.sessions.delete(sessionKey);
            this.sessionsByUserId.delete(user.userId);
          }
          
          const success = await this.startSession(user.userId, user.guildId);
          
          if (success) {
            this.asyncLogger.info(`✅ Reactivated ${user.userId}`);
            this.tokenFailureTracker.delete(user.userId);
          } else {
            // Update last attempt time in DB
            await this.updateLastAttempt(user.userId, user.guildId);
          }
        }
      }
    } catch (error) {
      this.asyncLogger.error('Failed to retry sessions', { error: formatError(error) });
    }
  }

  private async updateLastAttempt(userId: string, guildId: string): Promise<void> {
    try {
      // This would need to be implemented in the database layer
      // For now, just track in memory
      const key = `${userId}:${guildId}`;
      this.tokenFailureTracker.set(userId, {
        failures: (this.tokenFailureTracker.get(userId)?.failures || 0) + 1,
        lastAttempt: Date.now()
      });
    } catch (error) {
      // Silent fail
    }
  }

  getStats() {
    const sessionStats: Array<{ userId: string; stats: SessionStats }> = [];
    let active = 0;
    let totalDetected = 0;
    let totalEntered = 0;
    let totalWins = 0;
    
    for (const [key, session] of this.sessions) {
      if (session.isActive && !session.destroyed) active++;
      sessionStats.push({ userId: session.userId, stats: { ...session.stats } });
      totalDetected += session.stats.detected;
      totalEntered += session.stats.entered;
      totalWins += session.stats.wins;
    }
    
    const mem = this.getMemoryUsage();
    const metrics = this.metrics.getMetrics();
    
    // Extract values from LRU cache safely
    const guildStatsValues: GuildStats[] = [];
    const accountStatsValues: AccountStats[] = [];
    try {
      const gCache = (this.guildStatsCache as any).cache;
      if (gCache) {
        for (const entry of (gCache as Map<string, { value: GuildStats }>).values()) {
          guildStatsValues.push(entry.value);
        }
      }
      const aCache = (this.accountStatsCache as any).cache;
      if (aCache) {
        for (const entry of (aCache as Map<string, { value: AccountStats }>).values()) {
          accountStatsValues.push(entry.value);
        }
      }
    } catch {
      // ignore
    }
    
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
      queue: this.joinQueue.getStats(),
      guildStats: guildStatsValues,
      accountStats: accountStatsValues,
      reconnectCounts: Array.from(this.reconnectCountMap.entries()).map(([userId, count]) => ({
        userId, count,
      })),
      batchBuffers: {
        joinOutcomes: this.joinOutcomeBuffer.length,
      },
      retryScheduled: this.retryScheduled.size,
      tokenFailures: Array.from(this.tokenFailureTracker.entries()).map(([userId, data]) => ({
        userId,
        failures: data.failures,
        lastAttempt: new Date(data.lastAttempt).toISOString()
      })),
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
        retryScheduled: stats.retryScheduled,
      },
    };
  }

  async shutdown(): Promise<void> {
    if (this.isShuttingDown) return;
    
    this.isShuttingDown = true;

    this.asyncLogger.info('🛑 Shutting down AutoJoinManager...', {
      worker: this.workerId, sessions: this.sessions.size, queueSize: this.joinQueue.getTotalSize(),
    });

    const intervals = [
      this.refreshInterval, this.cleanupInterval, this.memoryCheckInterval,
      this.reconnectCheckInterval, this.cacheCleanInterval, this.metricsInterval,
      this.healthCheckInterval, this.queuePersistInterval, this.stallCheckInterval,
      this.batchDbInterval, this.archiveInterval, this.statsCleanInterval,
    ];
    intervals.forEach(interval => {
      if (interval) clearInterval(interval);
    });

    // Clear retry schedules
    for (const [key, timeout] of this.retryScheduled) {
      clearTimeout(timeout);
    }
    this.retryScheduled.clear();

    await this.joinQueue.persist();

    if (this.joinOutcomeBuffer.length > 0) {
      try { await batchSaveJoinOutcomes(this.joinOutcomeBuffer); } catch {}
      this.joinOutcomeBuffer = [];
    }

    if (this.sessionStartPromises.size > 0) {
      try {
        await Promise.race([
          Promise.allSettled(this.sessionStartPromises.values()),
          delay(5000),
        ]);
      } catch {}
      this.sessionStartPromises.clear();
    }

    const sessionsToStop = Array.from(this.sessions.values());
    
    for (const session of sessionsToStop) {
      session.destroyed = true;
      session.isActive = false;
      
      if (session.heartbeatInterval) {
        clearInterval(session.heartbeatInterval);
        session.heartbeatInterval = undefined;
      }
      
      this.cleanupSessionListeners(session);
      this.clearClientCaches(session.client);
      
      try { 
        session.client.removeAllListeners();
        await (session.client as any).destroy(); 
      } catch {}
      
      this.sessions.delete(this.makeSessionKey(session.userId));
      this.sessionsByUserId.delete(session.userId);
      this.tokenManager.clearCache(session.userId, session.guildId);
    }

    this.sessions.clear();
    this.sessionsByUserId.clear();
    this.processedMessages.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    this.noResponseCooldown.clear();
    this.crosspostCache.clear();
    this.tokenManager.clearAll();
    this.sessionStartPromises.clear();
    this.guildStatsCache.clear();
    this.accountStatsCache.clear();
    this.reconnectCountMap.clear();
    this.tokenFailureTracker.clear();
    
    try { this.httpAgent.destroy(); } catch {}
    try { this.httpsAgent.destroy(); } catch {}
    
    this.asyncLogger.shutdown();
    
    if (global.gc) global.gc();
    
    this.asyncLogger.info('✅ AutoJoin shutdown complete', { 
      worker: this.workerId, memory: this.getMemoryUsage(),
    });
  }

  // -------------------------------------------------------------------------
  // Event Handlers
  // -------------------------------------------------------------------------

  private registerEvents(session: UserSession): void {
    const { client, userId } = session;

    // CRITICAL: Remove ALL existing listeners first to prevent accumulation
    client.removeAllListeners('messageCreate');
    client.removeAllListeners('messageUpdate');
    client.removeAllListeners('error');
    client.removeAllListeners('disconnect');
    client.removeAllListeners('ready');
    client.removeAllListeners('warn');
    
    try {
      (client as any).ws?.removeAllListeners?.();
    } catch {}

    const GIVEAWAY_BOT_ID = '530082442967646230';

    const messageCreateHandler = async (message: Message) => {
      if (message.author?.id !== GIVEAWAY_BOT_ID) {
        this.purgeMessageFromCache(message);
        return;
      }
      
      if (Date.now() - message.createdTimestamp > 30000) {
        this.purgeMessageFromCache(message);
        return;
      }
      
      if (this.isShuttingDown || !session.isActive || session.destroyed) {
        this.purgeMessageFromCache(message);
        return;
      }
      
      try {
        this.metrics.totalMessagesProcessed++;
        session.lastHealthCheck = Date.now();
        
        if (!message.guild) {
          await this.handleDmWin(message, userId);
          this.purgeMessageFromCache(message);
          return;
        }
        if (message.author?.id === client.user?.id) {
          this.purgeMessageFromCache(message);
          return;
        }
        
        await this.handleWin(message, userId);
        await this.handleMessage(message, session);
      } catch (error) {
        // Silent
      } finally {
        this.purgeMessageFromCache(message);
      }
    };

    const messageUpdateHandler = async (_old: Message | PartialMessage, updated: Message | PartialMessage) => {
      if ((updated as Message).author?.id !== GIVEAWAY_BOT_ID) {
        this.purgeMessageFromCache(updated as Message);
        return;
      }
      
      if (this.isShuttingDown || !session.isActive || session.destroyed) {
        this.purgeMessageFromCache(updated as Message);
        return;
      }
      
      try {
        const entryId = this.makeEntryId(session, updated as Message);
        if (!this.processedMessages.has(entryId)) {
          await this.handleMessage(updated as Message, session);
        }
      } catch (error) {
        // Silent
      } finally {
        this.purgeMessageFromCache(updated as Message);
      }
    };

    const errorHandler = (_error: Error) => {
      // Silent
    };

    const disconnectHandler = () => {
      // Silent
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
  // Message Handling
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

    if (!this.checkMemory()) return;

    this.processingCache.set(entryId, Date.now());

    try {
      const detected = await this.detectGiveawaySimple(message);
      
      if (!detected || !detected.button) {
        this.processingCache.delete(entryId);
        return;
      }

      this.metrics.totalGiveawaysDetected++;
      this.processedMessages.set(entryId, Date.now());
      session.stats.detected++;

      const correlationId = uuidv4();

      const entryData: Omit<GiveawayEntry, '_id'> = {
        userId: session.userId,
        messageId: message.id,
        channelId: message.channel.id,
        guildId: message.guild!.id,
        authorId: message.author?.id ?? '',
        guildName: message.guild!.name,
        channelName: (message.channel as { name?: string }).name ?? 'unknown',
        prize: detected.prize,
        buttonCustomId: detected.button.customId,
        detectedAt: Date.now(),
        endsAt: this.extractEndTimestamp(message),
        status: 'pending',
        attempts: 0,
        expiresAt: Date.now() + ENTRY_TTL_MS,
        correlationId,
        detectionConfidence: 1.0,
        detectionReasons: ['binary_detection_with_button'],
      };

      await saveAutoJoinEntry(entryData as Omit<AutoJoinEntry, '_id'>);
      this.metrics.dbQueries++;

      this.asyncLogger.info('🎯 AutoJoin: Giveaway detected', {
        correlationId,
        userId: session.userId,
        prize: truncate(entryData.prize, 60),
        button: detected.button.label || detected.button.customId,
        guild: entryData.guildName,
        worker: this.workerId,
      });

      await this.queueOrEnter(entryId, session, entryData as GiveawayEntry, correlationId);

    } catch (error) {
      this.asyncLogger.error('AutoJoin: Handle message error', {
        userId: session.userId,
        error: formatError(error),
        worker: this.workerId,
      });
    } finally {
      this.processingCache.delete(entryId);
      await cleanupAutoJoinEntries(session.userId);
    }
  }

  // ===== BINARY DETECTION =====

  private async detectGiveawaySimple(message: Message): Promise<{
    button?: GiveawayButton;
    prize: string;
  } | null> {
    if (Date.now() - message.createdTimestamp > 30000) {
      return null;
    }

    const rawContent = message.content ?? '';
    if (BLOCKED_MESSAGE_PATTERNS.some(re => re.test(rawContent))) {
      return null;
    }

    const isKnownBot = message.author?.bot && 
                       message.author.id && 
                       KNOWN_GIVEAWAY_BOT_IDS.has(message.author.id);
    const hasKeyword = this.messageHasKeyword(message);
    
    if (!isKnownBot && !hasKeyword) return null;
    
    if (isKnownBot && !hasKeyword && !message.embeds?.length) return null;

    let button = this.extractEntryButton(message);
    if (button) {
      return { button, prize: this.extractPrize(message) };
    }

    if (!isKnownBot || !message.embeds?.length) return null;

    for (let i = 0; i < COMPONENT_RETRY_ATTEMPTS; i++) {
      await delay(COMPONENT_RETRY_DELAY_MS);
      try {
        const refreshed = await this.fetchMessageUncached(
          message.client as Client, 
          message.channel.id, 
          message.id
        );
        if (!refreshed) break;
        
        button = this.extractEntryButton(refreshed);
        if (button) {
          return { button, prize: this.extractPrize(refreshed) };
        }
      } catch {
        break;
      }
    }

    return null;
  }

  private messageHasKeyword(message: Message): boolean {
    const texts = [
      message.content ?? '',
      ...message.embeds.flatMap(e => [
        e.title ?? '',
        e.description ?? '',
        e.footer?.text ?? '',
        ...(e.fields ?? []).flatMap(f => [f.name, f.value]),
      ]),
    ];
    return texts.some(t => hasGiveawayKeyword(t));
  }

  private extractEntryButton(message: Message): GiveawayButton | null {
    const msgAny = message as unknown as Record<string, unknown>;
    const components = msgAny['components'] as unknown[] | undefined;
    if (!components?.length) return null;

    for (const row of components) {
      const rowAny = row as Record<string, unknown>;
      const rowComps = rowAny['components'] as unknown[] | undefined;
      if (!rowComps) continue;

      for (const comp of rowComps) {
        const c = comp as Record<string, unknown>;
        
        if (c['type'] !== 2 && c['type'] !== 'BUTTON') continue;
        if (c['style'] === 5) continue;
        if (c['disabled'] === true) continue;

        const customId = (c['customId'] ?? c['custom_id']) as string | undefined;
        if (!customId) continue;

        const label = ((c['label'] as string | undefined) ?? '').trim();

        if (BLOCKED_BUTTON_PATTERNS.some(re => re.test(label))) continue;

        if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) {
          return { customId, label: label || customId, disabled: false };
        }

        if (ENTRY_BUTTON_PATTERNS.some(re => re.test(label))) {
          return { customId, label: label || 'Enter', disabled: false };
        }
      }
    }

    return null;
  }

  // -------------------------------------------------------------------------
  // Queue or Enter
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
      await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'queued', {});

      this.processQueue(session.userId, entry.guildId).catch(error => {
        this.asyncLogger.error('Queue processing error', { correlationId, error: formatError(error) });
      });
    } else {
      await this.enterGiveaway(entryId, session);
    }
  }

  // -------------------------------------------------------------------------
  // Queue Processing
  // -------------------------------------------------------------------------

  private async processQueue(userId: string, guildId: string): Promise<void> {
    const session = this.sessionsByUserId.get(userId);
    if (!session || !session.isActive) return;

    let item = this.joinQueue.dequeue(guildId);
    while (item) {
      if (item.endsAt && Date.now() > item.endsAt) {
        this.joinQueue.cancelGiveaway(item.messageId, item.channelId);
        await updateAutoJoinEntryStatus(userId, item.messageId, item.channelId, 'skipped', {
          lastError: 'giveaway_ended',
        });
        item = this.joinQueue.dequeue(guildId);
        continue;
      }

      const entryId = this.makeEntryIdFromMessage(userId, item.channelId, item.messageId);
      await this.enterGiveaway(entryId, session);

      item = this.joinQueue.dequeue(guildId);
    }
  }

  // -------------------------------------------------------------------------
  // Enter Giveaway
  // -------------------------------------------------------------------------

  private async enterGiveaway(entryId: string, session: UserSession): Promise<void> {
    const parts = entryId.split(':');
    const userId = parts[0];
    const channelId = parts[1];
    const messageId = parts.slice(2).join(':');
    
    const entry = await getAutoJoinEntry(session.userId, messageId, channelId);
    this.metrics.dbQueries++;
    if (!entry) return;

    const correlationId = entry.correlationId || uuidv4();

    await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'attempting', {});
    this.metrics.dbQueries++;

    const maxAttempts = CONFIG.maxRetries + 1;

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
          return;
        }

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
          correlationId,
          timestamp: Date.now(),
        });

        this.updateGuildStats(entry.guildId, entry.guildName, 'entered');
        this.updateAccountStats(session.userId, 'entered');

        this.apiCircuitBreaker.reset();

        this.asyncLogger.info('✅ AutoJoin: Entered giveaway', {
          correlationId,
          userId: session.userId,
          prize: truncate(entry.prize, 60),
          attempts: attemptNum,
          guild: entry.guildName,
          worker: this.workerId,
        });

        this.emit('giveawayEntered', { entry, userId: session.userId, correlationId });
        return;

      } catch (error) {
        const errorMsg = formatError(error);
        
        if (errorMsg.includes('No buttonCustomId set')) {
          await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'skipped', {
            lastError: 'No button found - not a valid giveaway entry',
          });
          this.metrics.dbQueries++;
          return;
        }
        
        if (errorMsg.includes('Circuit breaker is open')) {
          await delay(30000);
          this.apiCircuitBreaker.reset();
          continue;
        }
        
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
          correlationId, userId: session.userId, entryId, error: errorMsg, worker: this.workerId,
        });
      }
    }

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

    this.asyncLogger.error('❌ AutoJoin: All retries exhausted - moved to dead letter', {
      correlationId, userId: session.userId, prize: truncate(entry.prize, 60),
      attempts: entry.attempts, worker: this.workerId,
    });

    this.emit('giveawayFailed', { entry, userId: session.userId, correlationId });
  }

  // -------------------------------------------------------------------------
  // Button Interaction Methods
  // -------------------------------------------------------------------------

  private async refreshButtonData(entry: GiveawayEntry, session: UserSession): Promise<GiveawayEntry | null> {
    try {
      const message = await this.fetchMessageUncached(session.client, entry.channelId, entry.messageId);
      if (!message) return null;
      
      const button = this.extractEntryButton(message);
      if (button && button.customId !== entry.buttonCustomId) {
        entry.buttonCustomId = button.customId;
        return entry;
      }
      return entry;
    } catch {
      return null;
    }
  }

  private async enterViaButton(entry: GiveawayEntry, session: UserSession): Promise<boolean> {
    if (!entry.buttonCustomId) throw new Error('No buttonCustomId set');

    if (CONFIG.buttonDelayMs > 0) await delay(CONFIG.buttonDelayMs);

    const message = await this.fetchMessageUncached(session.client, entry.channelId, entry.messageId);
    if (!message) throw new Error(`Message ${entry.messageId} not found`);

    let button = this.findButtonById(message, entry.buttonCustomId);
    
    if (!button) {
      button = this.extractEntryButton(message);
      if (button) {
        entry.buttonCustomId = button.customId;
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
      
      let wsSessionId: string | undefined;
      for (let i = 0; i < 5; i++) {
        wsSessionId = client.ws?.shards?.first?.()?.sessionId
          ?? client.ws?.shards?.get?.(0)?.sessionId;
        if (wsSessionId) break;
        await delay(1000);
      }

      if (!wsSessionId) {
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
        session_id: wsSessionId,
        message_flags: 0,
        data: {
          component_type: 2,
          custom_id: button.customId,
        },
      };

      const token = session.decryptedToken;
      if (!token) {
        throw new Error('Token unavailable in session');
      }

      for (let attempt = 0; attempt < INTERACTION_RETRY_ATTEMPTS; attempt++) {
        try {
          if (attempt > 0) await delay(INTERACTION_RETRY_DELAY_MS * attempt);

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
              userId: session.userId, status 
            });
            // 🔥 FIX: Schedule retry instead of permanent deactivation
            await this.scheduleRetry(session.userId, session.guildId);
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
    if (!WIN_PATTERNS.some(re => re.test(allText))) return;

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
      userId, prize, source: sourceName, guild: message.guild.name, worker: this.workerId,
    });

    await this.sendWinWebhook(message, prize, sourceName, userId);
    this.emit('giveawayWon', { message, prize, userId });
  }

  private async handleDmWin(message: Message, userId: string): Promise<void> {
    if (message.guild) return;

    const allText = this.extractAllText(message);
    if (!WIN_PATTERNS.some(re => re.test(allText))) return;

    const session = this.findSessionByUserId(userId);
    if (session) {
      session.stats.wins++;
      this.updateAccountStats(userId, 'wins');
    }
    this.metrics.totalWinsDetected++;

    await incrementTokenWins(userId, session?.guildId || '');

    const prize = this.extractPrize(message);

    this.asyncLogger.info('🏆 AutoJoin: WIN DETECTED (DM)!', { 
      userId, prize, worker: this.workerId,
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
    guildId: string, guildName: string, 
    stat: 'detected' | 'entered' | 'failed' | 'wins' | 'falsePositives',
    confidence?: number
  ): void {
    let stats = this.guildStatsCache.get(guildId);
    if (!stats) {
      stats = {
        guildId, guildName, detected: 0, entered: 0, failed: 0, wins: 0,
        falsePositives: 0, averageConfidence: 0, averageQueueWaitMs: 0,
      };
    }

    stats[stat]++;

    if (confidence !== undefined && stat === 'detected') {
      stats.averageConfidence = 
        (stats.averageConfidence * (stats.detected - 1) + confidence) / stats.detected;
    }

    this.guildStatsCache.set(guildId, stats);
  }

  private updateAccountStats(
    userId: string, 
    stat: 'detected' | 'entered' | 'failed' | 'wins' | 'falsePositives',
    confidence?: number, detectionMs?: number
  ): void {
    let stats = this.accountStatsCache.get(userId);
    if (!stats) {
      stats = {
        userId, detected: 0, entered: 0, failed: 0, wins: 0,
        falsePositives: 0, averageConfidence: 0, averageDetectionMs: 0,
        averageQueueWaitMs: 0, reconnectCount: 0,
      };
    }

    stats[stat]++;

    if (detectionMs !== undefined && stat === 'detected') {
      stats.averageDetectionMs = 
        (stats.averageDetectionMs * (stats.detected - 1) + detectionMs) / stats.detected;
    }

    this.accountStatsCache.set(userId, stats);
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
          // Silent
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
            userId: session.userId, sessionId: session.sessionId, stallCount: session.stallCount,
          });

          if (session.stallCount >= 3) {
            this.asyncLogger.error('Auto-recovering stalled worker', {
              userId: session.userId, sessionId: session.sessionId,
            });
            
            // 🔥 FIX: Schedule retry instead of immediate restart
            this.scheduleRetry(session.userId, session.guildId);
            this.stopSession(session.userId, session.guildId);
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

      try {
        if (outcomes.length > 0) {
          await batchSaveJoinOutcomes(outcomes);
          this.metrics.dbQueries++;
        }
      } catch (error) {
        this.asyncLogger.error('Batch DB write failed', { error: formatError(error) });
        this.joinOutcomeBuffer.push(...outcomes.slice(0, 100));
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
    return this.fetchMessageUncached(client, channelId, messageId);
  }

  private makeEntryId(session: UserSession, message: Message): string {
    return `${session.userId}:${message.channel.id}:${message.id}`;
  }

  private makeEntryIdFromMessage(userId: string, channelId: string, messageId: string): string {
    return `${userId}:${channelId}:${messageId}`;
  }

  private makeSessionKey(userId: string): string {
    return userId;
  }

  private findSessionByUserId(userId: string): UserSession | null {
    return this.sessionsByUserId.get(userId) || null;
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
      this.checkMemory();
    }, 30_000);
    if (this.memoryCheckInterval.unref) this.memoryCheckInterval.unref();
  }

  // 🔥 FIX: Improved reconnect checker with more frequent retries
  private startReconnectChecker(): void {
    this.reconnectCheckInterval = setInterval(() => {
      if (!this.isShuttingDown && this.checkMemory()) {
        this.retryFailedSessions().catch((error) => {
          this.asyncLogger.error('Reconnect check failed', { error: formatError(error) });
        });
      }
    }, RETRY_CHECK_INTERVAL_MS); // Check every 5 minutes
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

  private startStatsCleaner(): void {
    this.statsCleanInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      
      const guildCleaned = this.guildStatsCache.clean();
      const accountCleaned = this.accountStatsCache.clean();
      
      for (const userId of this.reconnectCountMap.keys()) {
        if (!this.accountStatsCache.has(userId) && !this.sessionsByUserId.has(userId)) {
          this.reconnectCountMap.delete(userId);
        }
      }
      
      if (guildCleaned > 0 || accountCleaned > 0) {
        this.asyncLogger.debug(`🧹 Stats cleaner: removed ${guildCleaned + accountCleaned} expired stats entries`, {
          worker: this.workerId,
        });
      }
    }, 10 * 60_000);
    if (this.statsCleanInterval.unref) this.statsCleanInterval.unref();
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
      retryScheduled: stats.retryScheduled,
      tokenFailures: stats.tokenFailures?.length || 0,
    });
  }
}
