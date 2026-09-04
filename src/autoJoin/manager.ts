/**
 * @module autoJoin/manager
 * 
 * 🔥 ULTRA FAST AutoJoiner - PRODUCTION GRADE - MEMORY SAFE
 * 
 * CRITICAL FIXES:
 * 1. 🔥 FIXED: guild_id is NEVER null - Discord requires it for interactions
 * 2. 🔥 FIXED: Messages MUST have guild context before clicking buttons
 * 3. 🔥 FIXED: Null checks for message.guild throughout
 * 4. 🔥 FIXED: Rate limit reconnections with exponential backoff
 * 5. 🔥 FIXED: Prevent overlapping session starts with promise handling
 * 6. 🔥 FIXED: Memory leaks - proper cleanup on shutdown
 * 7. 🔥 FIXED: All methods restored for index.ts compatibility
 * 8. 🔥 FIXED: rateLimiter added back to UserSession interface
 * 
 * SPEED OPTIMIZATIONS:
 * 1. Parallel session startup - ALL sessions start at once
 * 2. No session cap - starts ALL premium users
 * 3. 2-second ready timeout (was 10s)
 * 4. Session ID caching - no waiting for gateway
 * 5. Message caching - no re-fetching from Discord
 * 6. Parallel queue processing - 5 entries at a time per account
 * 7. 50ms button delay (configurable)
 * 8. Faster retry logic - no waiting on "already entered"
 * 9. Batch DB writes - non-blocking
 * 10. Token bucket rate limiting - 10 requests per 5 seconds
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
  rateLimiter: TokenBucket; // 🔥 FIXED: Added back
  interactionCircuitBreaker: CircuitBreaker;
  listeners: {
    messageCreate?: (message: Message) => void;
    messageUpdate?: (oldMessage: Message | PartialMessage, newMessage: Message | PartialMessage) => void;
    error?: (error: Error) => void;
    disconnect?: () => void;
    ready?: () => void;
    reconnecting?: () => void;
    resumed?: () => void;
  };
  sessionId: string;
  destroyed: boolean;
  decryptedToken: string;
  loginFailures: number;
  lastLoginAttempt: number;
  gatewaySessionId: string | null;
  lastSessionIdFetch: number;
  reconnectAttempts: number;
  reconnectInProgress: boolean;
  lastDisconnectAt: number;
  lastReconnectAt: number;
  stableSince: number;
  lastPipelineActivityAt: number;
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
  buttonCustomId?: string;
  cachedButtonId?: string;
  cachedPrize?: string;
  cachedGuildName?: string;
  cachedChannelName?: string;
}

interface IngestQueueItem {
  message: Message;
  kind: 'create' | 'update';
  queuedAt: number;
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

interface CachedMessageData {
  buttonCustomId: string;
  prize: string;
  guildName: string;
  channelName: string;
  endsAt?: number;
  expiresAt: number;
}

// ---------------------------------------------------------------------------
// Constants - SPEED OPTIMIZED
// ---------------------------------------------------------------------------

const GIVEAWAY_BOT_ID = '530082442967646230';

const GIVEAWAY_BOT_NAMES = new Set(['GiveawayBot', 'Giveaway Bot']);

const KNOWN_GIVEAWAY_BOT_IDS: ReadonlySet<string> = new Set([
  '530082442967646230', // GiveawayBot
  '294882584201003009',
  '739448630517039104', // GiveawayBoat ✅
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

// ---------------------------------------------------------------------------
// Stability / memory limits
// ---------------------------------------------------------------------------
const ENTRY_TTL_MS = 5 * 60 * 1000;
const WIN_DEDUP_TTL_MS = 5 * 60 * 1000;
const WIN_INVITE_CACHE_TTL_MS = 30 * 60 * 1000;
const WIN_INVITE_CACHE_MAX_SIZE = 250;
const COMPONENT_RETRY_DELAY_MS = 75;
const COMPONENT_RETRY_ATTEMPTS = 2;
const SESSION_REFRESH_INTERVAL_MS = 2 * 60 * 1000;
const MAX_SESSIONS_PER_WORKER = 999999; // compatibility: no artificial feature cap
const SESSION_START_CONCURRENCY = 8;
const PROCESSING_CACHE_TTL_MS = 5000;
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY_MS = 5000;
const RECONNECT_GRACE_MS = 15000;
const RECONNECT_COOLDOWN_MS = 30000;
const LOGIN_TIMEOUT_MS = 30000;
const READY_TIMEOUT_MS = 15000;
const INTERACTION_RETRY_ATTEMPTS = 2;
const INTERACTION_RETRY_DELAY_MS = 150;
const NO_RESPONSE_COOLDOWN_MS = 2500;
const BATCH_DB_WRITE_INTERVAL_MS = 2000;
const ARCHIVE_AGE_MS = 24 * 60 * 60 * 1000;
const MAX_QUEUE_SIZE = 5000;
const MAX_QUEUE_PER_GUILD = 200;
const MAX_DEAD_LETTER_QUEUE = 1000;
const MAX_QUEUE_WAIT_SAMPLES = 1000;
const QUEUE_PERSIST_INTERVAL_MS = 30000;
const DEAD_LETTER_RETENTION_MS = 3600000;
const DEAD_LETTER_RESTORE_MAX_AGE_MS = 5 * 60 * 1000;
const PENDING_RESTORE_MAX_AGE_MS = 30 * 60 * 1000;

// Bounded application caches. The original values were unnecessarily large,
// especially because Discord message/embed objects are expensive to retain.
const CACHE_PROCESSED_MESSAGES = 5000;
const CACHE_MAX_PROCESSING = 1000;
const CACHE_MAX_WINS = 500;
const CACHE_MAX_COOLDOWN = 500;
const CACHE_MAX_TOKEN = 100;
const CACHE_CROSSPOST = 2000;
const CACHE_MESSAGES = 1500;

const MEMORY_WARNING_THRESHOLD_MB = 2500;
const MEMORY_CRITICAL_THRESHOLD_MB = 3500;
const MEMORY_MAX_THRESHOLD_MB = 5000;
const RSS_WARNING_THRESHOLD_MB = 4500;
const RSS_CRITICAL_THRESHOLD_MB = 6000;

const MAX_LOG_QUEUE_SIZE = 1000;
const MAX_SESSION_START_PROMISES = 100;
const MAX_JOIN_OUTCOME_BUFFER = 2000;
const MAX_TOKEN_FAILURE_TRACKER = 5000;

const HTTP_MAX_SOCKETS = 50;
const HTTP_MAX_FREE_SOCKETS = 10;

const CIRCUIT_BREAKER_THRESHOLD = 20;
const CIRCUIT_BREAKER_TIMEOUT_MS = 30000;
const CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS = 3;

const METRICS_SAMPLE_SIZE = 100;

const RETRY_CHECK_INTERVAL_MS = 5 * 60 * 1000;
const INITIAL_RETRY_DELAY_MS = 5000;
const MAX_RETRY_DELAY_MS = 60000;
const TOKEN_REACTIVATION_THRESHOLD_MS = 60 * 1000;
const HEALTH_CHECK_INTERVAL_MS = 15000;

const MAX_CONCURRENT_ENTRIES_PER_ACCOUNT = 2;
const DETECTION_CONCURRENCY_PER_SESSION = 3;
const MAX_INGEST_QUEUE_SIZE = 5000;
const INGEST_OPERATION_TIMEOUT_MS = 15000;
const ENTRY_OPERATION_TIMEOUT_MS = 20000;
const SESSION_PIPELINE_STALL_MS = 45000;
const MAX_DETECTION_MESSAGE_AGE_MS = 30 * 60 * 1000;

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
    if (this.getTotalSize() >= MAX_QUEUE_SIZE) {
      logger.warn('🚫 Queue full, dropping entry', {
        guildId: item.guildId,
        totalSize: this.getTotalSize(),
        maxSize: MAX_QUEUE_SIZE
      });
      return false;
    }

    const guildQueue = this.getGuildQueue(item.guildId);
    if (guildQueue.length >= MAX_QUEUE_PER_GUILD) {
      logger.warn('🚫 Guild queue full, dropping entry', {
        guildId: item.guildId,
        guildQueueSize: guildQueue.length,
        maxPerGuild: MAX_QUEUE_PER_GUILD
      });
      return false;
    }

    guildQueue.push(item);
    guildQueue.sort((a, b) => a.priority - b.priority);
    return true;
  }

  hasEntriesForUser(userId: string): boolean {
    if (!userId) return false;
    for (const queue of this.queues.values()) {
      if (queue.some(item => item.userId === userId)) return true;
    }
    return false;
  }

  dequeueForUser(userId: string): QueueItem | undefined {
    if (!userId) return undefined;

    let highestPriority: QueueItem | undefined;
    let highestPriorityGuild: string | undefined;
    let highestPriorityIndex = -1;

    for (const [guildId, guildQueue] of this.queues) {
      for (let i = 0; i < guildQueue.length; i++) {
        const item = guildQueue[i];
        if (item.userId !== userId) continue;
        if (!highestPriority || item.priority < highestPriority.priority) {
          highestPriority = item;
          highestPriorityGuild = guildId;
          highestPriorityIndex = i;
        }
        break;
      }
    }

    if (!highestPriority || !highestPriorityGuild || highestPriorityIndex < 0) {
      return undefined;
    }

    const queue = this.queues.get(highestPriorityGuild);
    if (!queue) return undefined;

    const [item] = queue.splice(highestPriorityIndex, 1);
    if (queue.length === 0) this.queues.delete(highestPriorityGuild);

    this.totalProcessed++;
    this.recordWaitTime(Date.now() - item.addedAt);
    return item;
  }

  dequeue(guildId?: string): QueueItem | undefined {
    if (guildId) {
      const guildQueue = this.queues.get(guildId);
      if (guildQueue?.length) {
        this.totalProcessed++;
        const startWait = Date.now();
        const item = guildQueue.shift()!;
        this.recordWaitTime(startWait - item.addedAt);
        if (guildQueue.length === 0) this.queues.delete(guildId);
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
      const queue = this.queues.get(highestPriorityGuild);
      queue?.shift();
      if (queue?.length === 0) this.queues.delete(highestPriorityGuild);
      this.totalProcessed++;
      const startWait = Date.now();
      this.recordWaitTime(startWait - highestPriority.addedAt);
    }

    return highestPriority;
  }

  dequeueBatch(guildId: string, count: number): QueueItem[] {
    const guildQueue = this.queues.get(guildId);
    if (!guildQueue || guildQueue.length === 0) return [];

    const batch: QueueItem[] = [];
    const itemsToRemove: number[] = [];

    for (let i = 0; i < Math.min(count, guildQueue.length); i++) {
      const item = guildQueue[i];
      if (item.endsAt && Date.now() > item.endsAt) {
        itemsToRemove.push(i);
        continue;
      }
      batch.push(item);
      itemsToRemove.push(i);
      if (batch.length >= count) break;
    }

    for (let i = itemsToRemove.length - 1; i >= 0; i--) {
      guildQueue.splice(itemsToRemove[i], 1);
    }

    this.totalProcessed += batch.length;
    const now = Date.now();
    for (const item of batch) {
      this.recordWaitTime(now - item.addedAt);
    }
    if (guildQueue.length === 0) this.queues.delete(guildId);

    return batch;
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
        if (guildQueue.length === 0) this.queues.delete(guildId);
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
    if (this.deadLetterQueue.length > MAX_DEAD_LETTER_QUEUE) {
      this.deadLetterQueue.splice(0, this.deadLetterQueue.length - MAX_DEAD_LETTER_QUEUE);
    }
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
    let queue = this.queues.get(guildId);
    if (!queue) {
      queue = [];
      this.queues.set(guildId, queue);
    }
    return queue;
  }

  private recordWaitTime(ms: number): void {
    if (!Number.isFinite(ms)) return;
    this.totalWaitTimes.push(Math.max(0, ms));
    if (this.totalWaitTimes.length > MAX_QUEUE_WAIT_SAMPLES) {
      this.totalWaitTimes.splice(0, this.totalWaitTimes.length - MAX_QUEUE_WAIT_SAMPLES);
    }
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

  emergencyDrain(maxAgeMs: number = 3600000): { clearedDeadLetters: number; clearedPending: number } {
    const cutoff = Date.now() - maxAgeMs;
    
    const oldDeadLetterCount = this.deadLetterQueue.length;
    this.deadLetterQueue = this.deadLetterQueue.filter(dl => dl.addedAt > cutoff);
    const clearedDeadLetters = oldDeadLetterCount - this.deadLetterQueue.length;
    
    let clearedPending = 0;
    for (const [guildId, queue] of this.queues) {
      const oldLength = queue.length;
      this.queues.set(guildId, queue.filter(item => item.addedAt > cutoff));
      clearedPending += oldLength - (this.queues.get(guildId)?.length || 0);
    }
    
    logger.info('🧹 Emergency queue drain complete', {
      clearedDeadLetters,
      clearedPending,
      remainingDeadLetters: this.deadLetterQueue.length,
      remainingPending: this.getTotalSize()
    });
    
    return { clearedDeadLetters, clearedPending };
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
      const now = Date.now();
      let restored = 0;
      let skippedDeadLetters = 0;
      let skippedPending = 0;
      
      for (const item of items) {
        if (item.priority < 0) {
          if (now - item.addedAt < DEAD_LETTER_RESTORE_MAX_AGE_MS) {
            this.deadLetterQueue.push(item);
            restored++;
          } else {
            skippedDeadLetters++;
          }
        } else {
          if (now - item.addedAt < PENDING_RESTORE_MAX_AGE_MS) {
            this.enqueue(item);
            restored++;
          } else {
            skippedPending++;
          }
        }
      }
      
      logger.info('📋 Queue restored', {
        restored,
        skippedDeadLetters,
        skippedPending,
        deadLetterQueueSize: this.deadLetterQueue.length,
        pendingQueueSize: this.getTotalSize()
      });
    } catch (error) {
      logger.warn('Failed to restore queue, starting fresh');
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
  private winInviteCache: LRUCache<string, string>;
  private pendingWinInvites: Map<string, Promise<string | null>> = new Map();
  private noResponseCooldown: LRUCache<string, number>;
  private crosspostCache: LRUCache<string, string>;
  private messageCache: LRUCache<string, CachedMessageData>;
  
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
  private ingestQueues: Map<string, IngestQueueItem[]> = new Map();
  private ingestQueuedKeys: Map<string, Set<string>> = new Map();
  private ingestWorkers: Map<string, Promise<void>> = new Map();
  private queueProcessorPromises: Map<string, Promise<void>> = new Map();
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

  // Retry scheduler
  private retryScheduled: Map<string, NodeJS.Timeout> = new Map();
  private tokenFailureTracker: Map<string, { failures: number; lastAttempt: number }> = new Map();
  private cleanupScheduled: Set<string> = new Set();

  // HTTP client and agents
  private httpAgent: http.Agent;
  private httpsAgent: https.Agent;
  private readonly http: AxiosInstance;

  constructor(workerId: string = 'main') {
    super();
    this.workerId = workerId;
    this.setMaxListeners(50);
    
    // Initialize caches
    this.processedMessages = new LRUCache<string, number>(CACHE_PROCESSED_MESSAGES, 180000);
    this.processingCache = new LRUCache<string, number>(CACHE_MAX_PROCESSING, PROCESSING_CACHE_TTL_MS);
    this.recentWins = new LRUCache<string, number>(CACHE_MAX_WINS, WIN_DEDUP_TTL_MS);
    this.winInviteCache = new LRUCache<string, string>(WIN_INVITE_CACHE_MAX_SIZE, WIN_INVITE_CACHE_TTL_MS);
    this.noResponseCooldown = new LRUCache<string, number>(CACHE_MAX_COOLDOWN, NO_RESPONSE_COOLDOWN_MS + 10000);
    this.crosspostCache = new LRUCache<string, string>(CACHE_CROSSPOST, 30 * 60 * 1000);
    this.messageCache = new LRUCache<string, CachedMessageData>(CACHE_MESSAGES, 30000);
    
    // Initialize systems
    this.joinQueue = new JoinQueue();
    
    // Initialize managers
    this.tokenManager = new TokenManager();
    this.asyncLogger = new AsyncLogger();
    this.apiCircuitBreaker = new CircuitBreaker();
    this.metrics = new MetricsCollector();

    // Stats caches
    this.guildStatsCache = new LRUCache<string, GuildStats>(2000, 6 * 60 * 60 * 1000);
    this.accountStatsCache = new LRUCache<string, AccountStats>(2000, 6 * 60 * 60 * 1000);

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
      timeout: 10000,
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
    
    const drainResult = this.joinQueue.emergencyDrain(3600000);
    if (drainResult.clearedDeadLetters > 0 || drainResult.clearedPending > 0) {
      this.asyncLogger.info('🧹 Startup queue drain complete', drainResult);
    }
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
    const heapHigh = mem.heapUsedMB >= MEMORY_WARNING_THRESHOLD_MB;
    const heapCritical = mem.heapUsedMB >= MEMORY_CRITICAL_THRESHOLD_MB;
    const heapMax = mem.heapUsedMB >= MEMORY_MAX_THRESHOLD_MB;
    const rssCritical = mem.rssMB >= RSS_CRITICAL_THRESHOLD_MB;

    if (heapMax || rssCritical) {
      this.healthStatus = 'critical';
      if (!this.memoryCriticalLogged) {
        this.memoryCriticalLogged = true;
        this.asyncLogger.error('🚨 Critical memory pressure; aggressively trimming application state', {
          worker: this.workerId,
          memory: mem,
          sessions: this.sessions.size,
        });
      }
      this.aggressiveCleanup();
      if (global.gc) {
        try { global.gc(); } catch {}
      }
      return false;
    }

    if (heapCritical || mem.rssMB >= RSS_WARNING_THRESHOLD_MB) {
      this.healthStatus = 'critical';
      this.aggressiveCleanup();
      if (global.gc) {
        try { global.gc(); } catch {}
      }
      return false;
    }

    if (heapHigh) {
      this.healthStatus = 'warning';
      this.processedMessages.clean();
      this.processingCache.clean();
      this.recentWins.clean();
      this.noResponseCooldown.clean();
      this.crosspostCache.clean();
      this.messageCache.clean();
      this.guildStatsCache.clean();
      this.accountStatsCache.clean();
      return true;
    }

    this.healthStatus = 'healthy';
    this.memoryCriticalLogged = false;
    return true;
  }

  private aggressiveCleanup(): void {
    // Keep active session credentials alive. Clearing them under memory pressure
    // causes avoidable decrypt/login churn and can contribute to reconnect storms.
    this.processedMessages.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    this.noResponseCooldown.clear();
    this.crosspostCache.clear();
    this.messageCache.clear();
    this.guildStatsCache.clean();
    this.accountStatsCache.clean();

    const cutoff = Date.now() - 10 * 60 * 1000;
    for (const [userId, data] of this.tokenFailureTracker) {
      if (data.lastAttempt < cutoff) this.tokenFailureTracker.delete(userId);
    }
    if (this.joinOutcomeBuffer.length > MAX_JOIN_OUTCOME_BUFFER) {
      this.joinOutcomeBuffer.splice(0, this.joinOutcomeBuffer.length - MAX_JOIN_OUTCOME_BUFFER);
    }
  }

  private clearClientCaches(client: Client): void {
    try {
      const channels = (client as any).channels?.cache;
      if (channels) {
        for (const channel of channels.values()) {
          try { channel?.messages?.cache?.clear?.(); } catch {}
          try { channel?.members?.cache?.clear?.(); } catch {}
        }
      }
      (client as any).guilds?.cache?.clear?.();
      (client as any).users?.cache?.clear?.();
      (client as any).channels?.cache?.clear?.();
      (client as any).emojis?.cache?.clear?.();
    } catch {
      // ignore
    }
  }

  private purgeMessageFromCache(message: Message): void {
    try {
      (message.channel as TextChannel)?.messages?.cache?.delete(message.id);
    } catch {
      // ignore
    }
  }

  private async fetchMessageUncached(client: Client, channelId: string, messageId: string): Promise<Message | null> {
    try {
      const channel = await client.channels.fetch(channelId, { force: true, cache: false });
      if (!channel || !('messages' in channel)) return null;
      
      const message = await (channel as TextChannel).messages.fetch(messageId, {
        force: true,
        cache: false,
      }) as Message;
      
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
  // Get Gateway Session ID
  // -------------------------------------------------------------------------

  private async getGatewaySessionId(client: Client): Promise<string | null> {
    try {
      const ws = client as any;
      if (!ws.ws) return null;
      
      const shards = ws.ws.shards;
      if (!shards) return null;
      
      const shard = shards.first?.() || shards.get?.(0);
      if (!shard) return null;
      
      const sessionId = shard.sessionId;
      if (sessionId) return sessionId;
      
      const state = shard._state || shard.state;
      if (state && state.sessionId) {
        return state.sessionId;
      }
      
      const connection = shard.connection;
      if (connection && connection.sessionId) {
        return connection.sessionId;
      }
      
      return null;
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
      
      this.asyncLogger.info(`📊 Found ${allPremiumUsers.length} premium users`, {
        withTokens: allPremiumUsers.filter(u => u.token).length,
        active: allPremiumUsers.filter(u => u.tokenActive !== false).length
      });
      
      const validUsers = allPremiumUsers.filter(u => u.token && u.tokenActive !== false);
      const usersToStart = validUsers.slice(0, MAX_SESSIONS_PER_WORKER);

      this.asyncLogger.info(`🚀 Starting ${usersToStart.length} sessions with bounded concurrency`, {
        concurrency: SESSION_START_CONCURRENCY,
      });

      let started = 0;
      let failed = 0;
      for (let i = 0; i < usersToStart.length; i += SESSION_START_CONCURRENCY) {
        if (this.isShuttingDown) break;
        const batch = usersToStart.slice(i, i + SESSION_START_CONCURRENCY);
        const results = await Promise.allSettled(
          batch.map(user => this.startSession(user.userId, user.guildId))
        );
        for (const result of results) {
          if (result.status === 'fulfilled' && result.value) {
            started++;
          } else {
            failed++;
            this.asyncLogger.warn('❌ Failed to start session', {
              error: result.status === 'rejected' ? formatError(result.reason) : 'Returned false',
            });
          }
        }
        if (i + SESSION_START_CONCURRENCY < usersToStart.length) {
          await delay(250);
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
        this.sessions.delete(sessionKey);
        this.sessionsByUserId.delete(userId);
      }
    }
    
    if (this.sessionStartPromises.has(sessionKey)) {
      this.asyncLogger.debug('Session start already in progress, waiting...', { userId });
      return this.sessionStartPromises.get(sessionKey)!;
    }

    if (this.sessionStartPromises.size >= MAX_SESSION_START_PROMISES) {
      this.asyncLogger.warn('Session start concurrency limit reached; deferring start', {
        userId,
        activeStarts: this.sessionStartPromises.size,
      });
      return false;
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
        // We process messages immediately; retaining thousands of message objects
        // is unnecessary and is one of the biggest sources of heap/RSS growth.
        messageCacheLifetime: 15,
        messageSweepInterval: 60,
        restRequestTimeout: 20000,
        restGlobalRateLimit: 50,
        retryLimit: 3,
        allowedMentions: { parse: [] },
        partials: [],
        makeCache: Options.cacheWithLimits({
          MessageManager: 100,
          UserManager: 1000,
          GuildMemberManager: 1000,
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
        
        const errorMsgLower = errorMsg.toLowerCase();
        const isPermanent = 
          errorMsgLower.includes('invalid token') ||
          errorMsgLower.includes('401') ||
          errorMsgLower.includes('unauthorized') ||
          errorMsgLower.includes('incorrect login') ||
          errorMsgLower.includes('incorrect password');
        
        const isTemporary =
          errorMsgLower.includes('etimedout') ||
          errorMsgLower.includes('econnreset') ||
          errorMsgLower.includes('503') ||
          errorMsgLower.includes('502') ||
          errorMsgLower.includes('429') ||
          errorMsgLower.includes('login timeout') ||
          errorMsgLower.includes('ready timeout') ||
          errorMsgLower.includes('econnrefused');
        
        if (isPermanent) {
          this.asyncLogger.error('❌ Permanent token failure - marking inactive', {
            userId, guildId, error: errorMsg, worker: this.workerId,
          });
          await setTokenActive(userId, guildId, false);
          this.tokenManager.clearCache(userId, guildId);
          this.emit('tokenRevoked', { userId, guildId, error: errorMsg });
        } else if (isTemporary) {
          this.asyncLogger.warn('⚠️ Temporary login failure, will retry later', { 
            userId, guildId, error: errorMsg, worker: this.workerId,
          });
          await this.scheduleRetry(userId, guildId);
        } else {
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
        rateLimiter: new TokenBucket(10, 5000), // 🔥 FIXED: Added back
        interactionCircuitBreaker: new CircuitBreaker(8, 10000, 2),
        listeners: {},
        sessionId,
        destroyed: false,
        decryptedToken,
        loginFailures: 0,
        lastLoginAttempt: Date.now(),
        gatewaySessionId: null,
        lastSessionIdFetch: 0,
        reconnectAttempts: 0,
        reconnectInProgress: false,
        lastDisconnectAt: 0,
        lastReconnectAt: 0,
        stableSince: Date.now(),
        lastPipelineActivityAt: Date.now(),
      };

      session.gatewaySessionId = await this.getGatewaySessionId(client);
      session.lastSessionIdFetch = Date.now();

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
      
      await this.scheduleRetry(userId, guildId);
      return false;
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
  // Retry Scheduler with exponential backoff
  // -------------------------------------------------------------------------

  private async scheduleRetry(userId: string, guildId: string): Promise<void> {
    const key = `${userId}:${guildId}`;
    
    if (this.retryScheduled.has(key)) {
      clearTimeout(this.retryScheduled.get(key)!);
      this.retryScheduled.delete(key);
    }
    
    const failureData = this.tokenFailureTracker.get(userId) || { failures: 0, lastAttempt: Date.now() };
    failureData.failures++;
    failureData.lastAttempt = Date.now();
    this.tokenFailureTracker.set(userId, failureData);
    if (this.tokenFailureTracker.size > MAX_TOKEN_FAILURE_TRACKER) {
      const oldest = Array.from(this.tokenFailureTracker.entries())
        .sort((a, b) => a[1].lastAttempt - b[1].lastAttempt)
        .slice(0, Math.max(1, this.tokenFailureTracker.size - MAX_TOKEN_FAILURE_TRACKER));
      for (const [oldUserId] of oldest) this.tokenFailureTracker.delete(oldUserId);
    }
    
    if (failureData.failures > 10) {
      this.asyncLogger.error('❌ Max retry attempts reached for user, permanently deactivating', { userId });
      await setTokenActive(userId, guildId, false);
      this.tokenFailureTracker.delete(userId);
      return;
    }
    
    const baseDelay = INITIAL_RETRY_DELAY_MS * Math.pow(2, Math.min(failureData.failures - 1, 6));
    const jitter = Math.random() * 2000;
    const backoffMs = Math.min(baseDelay + jitter, MAX_RETRY_DELAY_MS);
    
    this.asyncLogger.info(`⏰ Scheduling retry for ${userId} in ${Math.round(backoffMs/1000)}s (attempt #${failureData.failures})`);
    
    const timeout = setTimeout(async () => {
      this.retryScheduled.delete(key);
      if (!this.isShuttingDown) {
        this.asyncLogger.info(`🔄 Retrying session for ${userId}`);
        const success = await this.startSession(userId, guildId);
        if (!success) {
          const data = this.tokenFailureTracker.get(userId);
          if (data && data.failures < 10) {
            await this.scheduleRetry(userId, guildId);
          } else {
            this.asyncLogger.error('❌ Max retry attempts reached for user', { userId });
            await setTokenActive(userId, guildId, false);
            this.tokenFailureTracker.delete(userId);
          }
        } else {
          this.tokenFailureTracker.delete(userId);
        }
      }
    }, backoffMs);
    
    this.retryScheduled.set(key, timeout);
    if (timeout.unref) timeout.unref();
  }

  // -------------------------------------------------------------------------
  // Event Handlers
  // -------------------------------------------------------------------------

  private getMessageChannelId(message: Message | PartialMessage): string | null {
    const raw = message as any;
    const id = raw?.channelId ?? raw?.channel?.id;
    return typeof id === 'string' && id.length > 0 ? id : null;
  }

  private isPotentialGiveawayMessage(message: Message): boolean {
    if (!message?.guild) return false;
    const authorId = message.author?.id;
    if (authorId && KNOWN_GIVEAWAY_BOT_IDS.has(authorId)) return true;
    if (authorId === GIVEAWAY_BOT_ID) return true;
    const authorName = message.author?.username ?? message.author?.tag ?? '';
    if (GIVEAWAY_BOT_NAMES.has(authorName)) return true;
    if (this.messageHasKeyword(message)) return true;
    const components = (message as any)?.components;
    return Array.isArray(components) && components.length > 0;
  }

  private registerEvents(session: UserSession): void {
    const { client, userId } = session;

    // IMPORTANT: never remove the gateway's internal listeners here.
    // The previous ws.removeAllListeners() could break discord.js-selfbot's
    // own heartbeat/reconnect machinery and was a major cause of reconnect loops.
    this.cleanupSessionListeners(session);

    const messageCreateHandler = (message: Message) => {
      if (this.isShuttingDown || session.destroyed || !session.isActive) return;
      if (!message.guild) {
        void this.handleDmWin(message, session.userId).catch(error => {
          this.asyncLogger.warn('DM win detection failed', {
            userId: session.userId,
            messageId: message.id,
            error: formatError(error),
          });
        });
        return;
      }
      if (!this.getMessageChannelId(message)) return;
      if (message.author?.id === client.user?.id) return;
      void this.handleWin(message, session.userId).catch(error => {
        this.asyncLogger.warn('Win detection failed', {
          userId: session.userId,
          messageId: message.id,
          error: formatError(error),
        });
      });
      if (!this.isPotentialGiveawayMessage(message)) return;
      this.metrics.totalMessagesProcessed++;
      this.enqueueIncomingMessage(session, message, 'create');
    };
    const messageUpdateHandler = (oldMessage: Message | PartialMessage, updated: Message | PartialMessage) => {
      const message = updated as Message;
      if (this.isShuttingDown || session.destroyed || !session.isActive) return;
      if (!message.guild) return;
      if (!this.getMessageChannelId(message)) return;
      if (message.author?.id === client.user?.id) return;
      void this.handleWin(message, session.userId).catch(error => {
        this.asyncLogger.warn('Win detection failed on update', {
          userId: session.userId,
          messageId: message.id,
          error: formatError(error),
        });
      });
      if (!this.isPotentialGiveawayMessage(message)) return;
      this.metrics.totalMessagesProcessed++;
      this.enqueueIncomingMessage(session, message, 'update');
    };

    const readyHandler = () => {
      session.isActive = true;
      session.destroyed = false;
      session.gatewaySessionId = null;
      session.reconnectAttempts = 0;
      session.reconnectInProgress = false;
      session.lastReconnectAt = Date.now();
      session.stableSince = Date.now();
      session.lastPipelineActivityAt = Date.now();
      this.reconnectCountMap.delete(session.userId);
      this.tokenFailureTracker.delete(session.userId);
      this.restartSessionWorkers(session);
      this.asyncLogger.info('✅ Session ready', { userId: session.userId });
    };

    const disconnectHandler = () => {
      if (this.isShuttingDown || session.destroyed) return;
      session.isActive = false;
      session.gatewaySessionId = null;
      session.lastDisconnectAt = Date.now();
      session.reconnectAttempts = Math.min(MAX_RECONNECT_ATTEMPTS, session.reconnectAttempts + 1);
      session.lastPipelineActivityAt = Date.now();
      // A disconnect event does not mean our manual recovery is running. Let the
      // library reconnect/resume first; the health checker intervenes only later.
      session.reconnectInProgress = false;
      const count = (this.reconnectCountMap.get(session.userId) || 0) + 1;
      this.reconnectCountMap.set(session.userId, Math.min(count, 1000));
      const accountStats = this.accountStatsCache.get(session.userId);
      if (accountStats) {
        accountStats.reconnectCount = Math.min(accountStats.reconnectCount + 1, 1000000);
        this.accountStatsCache.set(session.userId, accountStats);
      }
      this.asyncLogger.warn('⚠️ Session disconnected; preserving client for gateway auto-reconnect', {
        userId: session.userId,
        attempt: session.reconnectAttempts,
      });
    };

    const reconnectingHandler = () => {
      if (this.isShuttingDown || session.destroyed) return;
      session.reconnectInProgress = true;
      session.lastReconnectAt = Date.now();
      this.asyncLogger.info('🔄 Session reconnecting...', {
        userId: session.userId,
        attempt: session.reconnectAttempts,
      });
    };

    const resumedHandler = () => {
      session.isActive = true;
      session.gatewaySessionId = null;
      session.reconnectInProgress = false;
      session.reconnectAttempts = 0;
      session.stableSince = Date.now();
      session.lastPipelineActivityAt = Date.now();
      this.restartSessionWorkers(session);
      this.asyncLogger.info('✅ Session resumed', { userId: session.userId });
    };

    const errorHandler = (error: Error) => {
      if (error.message?.includes('token')) {
        this.asyncLogger.error('Client error', { userId: session.userId, error: formatError(error) });
      }
    };

    session.listeners.messageCreate = messageCreateHandler;
    session.listeners.messageUpdate = messageUpdateHandler;
    session.listeners.ready = readyHandler;
    session.listeners.disconnect = disconnectHandler;
    session.listeners.reconnecting = reconnectingHandler;
    session.listeners.resumed = resumedHandler;
    session.listeners.error = errorHandler;

    client.on('messageCreate', messageCreateHandler);
    client.on('messageUpdate', messageUpdateHandler);
    client.on('ready', readyHandler);
    client.on('disconnect', disconnectHandler);
    client.on('reconnecting', reconnectingHandler);
    client.on('resumed', resumedHandler);
    client.on('error', errorHandler);
  }

  private enqueueIncomingMessage(session: UserSession, message: Message, kind: 'create' | 'update'): void {
    if (this.isShuttingDown || session.destroyed || !session.isActive) return;

    const channelId = this.getMessageChannelId(message);
    if (!message.guild || !channelId || !message.id) return;

    const userId = session.userId;
    let queue = this.ingestQueues.get(userId);
    if (!queue) {
      queue = [];
      this.ingestQueues.set(userId, queue);
    }

    const entryId = this.makeEntryId(session, message);
    if (this.processedMessages.has(entryId)) return;

    // If a messageUpdate arrives while the create event is still queued, replace
    // the queued object with the newest version. This is critical: GiveawayBot
    // frequently sends the message first and attaches/changes components shortly
    // afterwards. Dropping that update causes missed giveaways.
    const existingIndex = queue.findIndex(item => this.makeEntryId(session, item.message) === entryId);
    if (existingIndex !== -1) {
      const existing = queue[existingIndex];
      queue[existingIndex] = {
        message,
        kind: kind === 'update' ? 'update' : existing.kind,
        queuedAt: existing.queuedAt,
      };
      session.lastPipelineActivityAt = Date.now();
      return;
    }

    // Never allow the ingest queue to grow without bound. Prefer the newest event
    // because it is more likely to contain the current giveaway components.
    if (queue.length >= MAX_INGEST_QUEUE_SIZE) {
      const dropped = queue.shift();
      if (dropped) this.ingestQueuedKeys.get(userId)?.delete(this.makeEntryId(session, dropped.message));
      this.asyncLogger.warn('⚠️ Detection ingest queue saturated; dropped oldest event', {
        userId, queueSize: queue.length, maxQueueSize: MAX_INGEST_QUEUE_SIZE,
      });
    }

    let queuedKeys = this.ingestQueuedKeys.get(userId);
    if (!queuedKeys) {
      queuedKeys = new Set<string>();
      this.ingestQueuedKeys.set(userId, queuedKeys);
    }
    queuedKeys.add(entryId);
    queue.push({ message, kind, queuedAt: Date.now() });
    session.lastPipelineActivityAt = Date.now();

    this.ensureIngestWorker(session);
  }

  private ensureIngestWorker(session: UserSession): void {
    const userId = session.userId;
    if (this.isShuttingDown || session.destroyed || !session.isActive) return;
    if (this.ingestWorkers.has(userId)) return;
    if (!(this.ingestQueues.get(userId)?.length)) return;

    const worker = this.processIncomingMessageQueue(session)
      .catch(error => {
        this.asyncLogger.error('Incoming message queue failed', {
          userId,
          error: formatError(error),
        });
      })
      .finally(() => {
        this.ingestWorkers.delete(userId);
        // If the worker exited because the queue was temporarily interrupted,
        // immediately restart it. This avoids the "queue has work but no worker"
        // state that previously caused long detection stalls.
        if (!this.isShuttingDown && session.isActive && !session.destroyed &&
            (this.ingestQueues.get(userId)?.length || 0) > 0) {
          this.ensureIngestWorker(session);
        }
      });

    this.ingestWorkers.set(userId, worker);
  }

  private async processIncomingMessageQueue(session: UserSession): Promise<void> {
    const queue = this.ingestQueues.get(session.userId);
    if (!queue) return;

    const active = new Set<Promise<void>>();

    while (!this.isShuttingDown && session.isActive && !session.destroyed) {
      while (active.size < DETECTION_CONCURRENCY_PER_SESSION && queue.length > 0) {
        const item = queue.shift();
        if (!item) break;

        const entryId = this.makeEntryId(session, item.message);
        this.ingestQueuedKeys.get(session.userId)?.delete(entryId);

        const promise = (async () => {
          try {
            if (this.processedMessages.has(entryId)) return;
            if (!item.message.guild || !this.getMessageChannelId(item.message)) return;

            void this.handleWin(item.message, session.userId).catch(error => {
              this.asyncLogger.warn('Giveaway win handling failed', {
                userId: session.userId, messageId: item.message.id, error: formatError(error),
              });
            });

            session.lastPipelineActivityAt = Date.now();
            // Do not wrap this in Promise.race/withTimeout: that would free the
            // worker slot while handleMessage continues running in the background,
            // creating duplicate concurrent work during bursts. handleMessage is
            // intentionally non-blocking for database bookkeeping now.
            await this.handleMessage(item.message, session);
            session.lastPipelineActivityAt = Date.now();
          } catch (error) {
            this.asyncLogger.error('Incoming giveaway processing error', {
              userId: session.userId,
              guild: item.message.guild?.name,
              channelId: this.getMessageChannelId(item.message),
              messageId: item.message.id,
              error: formatError(error),
            });
          } finally {
            this.purgeMessageFromCache(item.message);
          }
        })().finally(() => active.delete(promise));

        active.add(promise);
      }

      if (active.size === 0) break;
      await Promise.race(active);
    }

    if (active.size > 0) await Promise.allSettled(active);

    if (queue.length === 0) {
      this.ingestQueues.delete(session.userId);
      this.ingestQueuedKeys.delete(session.userId);
    }
  }

  private cleanupSessionListeners(session: UserSession): void {
    const { client, listeners } = session;
    const handlers: Array<[string, ((...args: any[]) => any) | undefined]> = [
      ['messageCreate', listeners.messageCreate],
      ['messageUpdate', listeners.messageUpdate],
      ['error', listeners.error],
      ['disconnect', listeners.disconnect],
      ['ready', listeners.ready],
      ['reconnecting', listeners.reconnecting],
      ['resumed', listeners.resumed],
    ];

    for (const [event, handler] of handlers) {
      if (!handler) continue;
      try { client.off(event, handler as any); } catch {
        try { client.removeListener(event, handler as any); } catch {}
      }
    }

    // Never touch ws.removeAllListeners(): discord.js-selfbot owns those listeners.
    session.listeners = {};
  }

  // -------------------------------------------------------------------------
  // Message Handling
  // -------------------------------------------------------------------------

  private async handleMessage(message: Message, session: UserSession): Promise<void> {
    if (!message.guild) {
      return;
    }

    const channelId = this.getMessageChannelId(message);
    if (!channelId) return;

    if (CONFIG.monitoredChannels.length > 0 && 
        !CONFIG.monitoredChannels.includes(channelId)) {
      return;
    }
    if (Date.now() - message.createdTimestamp > MAX_DETECTION_MESSAGE_AGE_MS) return;

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

      const cacheKey = `${channelId}:${message.id}`;
      this.messageCache.set(cacheKey, {
        buttonCustomId: detected.button.customId,
        prize: detected.prize,
        guildName: message.guild!.name,
        channelName: (message.channel as { name?: string }).name ?? 'unknown',
        endsAt: this.extractEndTimestamp(message),
        expiresAt: Date.now() + 30000,
      });

      const entryData: Omit<GiveawayEntry, '_id'> = {
        userId: session.userId,
        messageId: message.id,
        channelId,
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

      void saveAutoJoinEntry(entryData as Omit<AutoJoinEntry, '_id'>)
        .then(() => { this.metrics.dbQueries++; })
        .catch(error => {
          this.asyncLogger.warn('AutoJoin: Background entry save failed', {
            userId: session.userId,
            entryId,
            error: formatError(error),
          });
        });

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
      this.scheduleCleanup(session.userId);
      // Keep freshly detected giveaway messages in the channel cache long enough
      // for the entry worker to click them without forcing another REST fetch.
      setTimeout(() => this.purgeMessageFromCache(message), 60000).unref?.();
    }
  }

  // ===== BINARY DETECTION =====

  private async detectGiveawaySimple(message: Message): Promise<{
    button?: GiveawayButton;
    prize: string;
  } | null> {
    if (Date.now() - message.createdTimestamp > MAX_DETECTION_MESSAGE_AGE_MS) {
      return null;
    }

    const rawContent = message.content ?? '';
    if (BLOCKED_MESSAGE_PATTERNS.some(re => re.test(rawContent))) {
      return null;
    }

    const authorId = message.author?.id;
    const isKnownBot = !!authorId && KNOWN_GIVEAWAY_BOT_IDS.has(authorId);
    const hasKeyword = this.messageHasKeyword(message);
    const hasComponents = Array.isArray((message as any)?.components) && (message as any).components.length > 0;
    const authorName = message.author?.username ?? message.author?.tag ?? '';
    const isGiveawayNamedBot = GIVEAWAY_BOT_NAMES.has(authorName);

    if (!isKnownBot && !isGiveawayNamedBot && !hasKeyword && !hasComponents) return null;

    let button = this.extractEntryButton(message);
    if (button) {
      return { button, prize: this.extractPrize(message) };
    }

    if ((!isKnownBot && !isGiveawayNamedBot) || !message.embeds?.length) return null;

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
      buttonCustomId: entry.buttonCustomId,
      cachedButtonId: entry.buttonCustomId,
      cachedPrize: entry.prize,
      cachedGuildName: entry.guildName,
      cachedChannelName: entry.channelName,
    };

    const enqueued = this.joinQueue.enqueue(queueItem);

    if (enqueued) {
      void updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'queued', {})
        .then(() => { this.metrics.dbQueries++; })
        .catch(() => {});

      // Start immediately. Database state is best-effort and must never block a
      // giveaway that may only have seconds remaining.
      this.startQueueProcessor(session.userId);
    } else {
      void this.enterGiveaway(entryId, session, entry).catch(error => {
        this.asyncLogger.warn('Direct giveaway entry failed', {
          userId: session.userId,
          messageId: entry.messageId,
          error: formatError(error),
        });
      });
    }
  }

  // -------------------------------------------------------------------------
  // Queue Processing - single processor per account
  // -------------------------------------------------------------------------

  private startQueueProcessor(userId: string): void {
    if (this.isShuttingDown) return;
    if (this.queueProcessorPromises.has(userId)) return;

    const processor = this.processQueueParallel(userId)
      .catch(error => {
        this.asyncLogger.error('Queue processing error', {
          userId,
          error: formatError(error),
        });
      })
      .finally(() => {
        this.queueProcessorPromises.delete(userId);
        if (!this.isShuttingDown && this.joinQueue.hasEntriesForUser(userId)) {
          this.startQueueProcessor(userId);
        }
      });

    this.queueProcessorPromises.set(userId, processor);
  }

  private async processQueueParallel(userId: string): Promise<void> {
    const session = this.sessionsByUserId.get(userId);
    if (!session || !session.isActive || session.destroyed) return;

    const CONCURRENT_LIMIT = MAX_CONCURRENT_ENTRIES_PER_ACCOUNT;
    const activePromises: Set<Promise<void>> = new Set();

    while (!this.isShuttingDown && session.isActive && !session.destroyed) {
      while (activePromises.size < CONCURRENT_LIMIT) {
        const item = this.joinQueue.dequeueForUser(userId);
        if (!item) break;

        if (item.endsAt && Date.now() > item.endsAt) {
          this.joinQueue.cancelGiveaway(item.messageId, item.channelId);
          await updateAutoJoinEntryStatus(userId, item.messageId, item.channelId, 'skipped', {
            lastError: 'giveaway_ended_before_processing',
          });
          continue;
        }

        const entryId = this.makeEntryIdFromMessage(userId, item.channelId, item.messageId);

        const entry: GiveawayEntry = {
          _id: entryId,
          userId: session.userId,
          messageId: item.messageId,
          channelId: item.channelId,
          guildId: item.guildId,
          authorId: '',
          guildName: item.cachedGuildName || '',
          channelName: item.cachedChannelName || '',
          prize: item.cachedPrize || '',
          buttonCustomId: item.buttonCustomId || item.cachedButtonId,
          detectedAt: item.addedAt,
          endsAt: item.endsAt,
          status: 'queued',
          attempts: item.attempts || 0,
          expiresAt: Date.now() + ENTRY_TTL_MS,
          correlationId: item.correlationId,
          detectionConfidence: 1.0,
          detectionReasons: [],
        };

        const cacheKey = `${item.channelId}:${item.messageId}`;
        const cached = this.messageCache.get(cacheKey);
        if (cached) {
          entry.buttonCustomId = cached.buttonCustomId || entry.buttonCustomId;
          entry.prize = cached.prize || entry.prize;
          entry.guildName = cached.guildName || entry.guildName;
          entry.channelName = cached.channelName || entry.channelName;
        }

        let promise!: Promise<void>;
        session.lastPipelineActivityAt = Date.now();
        const entryPromise = this.enterGiveaway(entryId, session, entry);
        promise = entryPromise
          .catch(error => {
            this.asyncLogger.error('Giveaway entry worker failed', {
              userId,
              messageId: entry.messageId,
              error: formatError(error),
            });
          })
          .finally(() => {
            activePromises.delete(promise);
            session.lastPipelineActivityAt = Date.now();
          });
        activePromises.add(promise);

        await delay(25);
      }

      if (activePromises.size === 0) break;
      await Promise.race(Array.from(activePromises));
    }

    if (activePromises.size > 0) {
      await Promise.allSettled(Array.from(activePromises));
    }
  }

  // -------------------------------------------------------------------------
  // Enter Giveaway
  // -------------------------------------------------------------------------

  private async enterGiveaway(
    entryId: string, 
    session: UserSession, 
    preFetchedEntry?: GiveawayEntry
  ): Promise<void> {
    let entry: AutoJoinEntry | null = null;
    
    if (preFetchedEntry && preFetchedEntry.buttonCustomId) {
      entry = {
        _id: preFetchedEntry._id || '',
        userId: preFetchedEntry.userId,
        messageId: preFetchedEntry.messageId,
        channelId: preFetchedEntry.channelId,
        guildId: preFetchedEntry.guildId,
        authorId: preFetchedEntry.authorId || '',
        guildName: preFetchedEntry.guildName || '',
        channelName: preFetchedEntry.channelName || '',
        prize: preFetchedEntry.prize || '',
        buttonCustomId: preFetchedEntry.buttonCustomId,
        detectedAt: preFetchedEntry.detectedAt || Date.now(),
        endsAt: preFetchedEntry.endsAt,
        status: (preFetchedEntry.status || 'pending') as AutoJoinEntry['status'],
        attempts: preFetchedEntry.attempts || 0,
        lastAttemptAt: preFetchedEntry.lastAttemptAt,
        lastError: preFetchedEntry.lastError,
        expiresAt: preFetchedEntry.expiresAt || Date.now() + ENTRY_TTL_MS,
        correlationId: preFetchedEntry.correlationId,
        detectionConfidence: preFetchedEntry.detectionConfidence,
        detectionReasons: preFetchedEntry.detectionReasons,
        crosspostSource: preFetchedEntry.crosspostSource,
      } as AutoJoinEntry;
    } else {
      const parts = entryId.split(':');
      const channelId = parts[1];
      const messageId = parts.slice(2).join(':');
      entry = await getAutoJoinEntry(session.userId, messageId, channelId);
      
      if (!entry) {
        this.asyncLogger.warn('⚠️ Entry not found in DB (possible race condition)', { 
          entryId,
          userId: session.userId 
        });
        return;
      }
    }
    
    this.metrics.dbQueries++;

    if (!entry) return;

    const correlationId = entry.correlationId || uuidv4();

    void updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'attempting', {})
      .then(() => { this.metrics.dbQueries++; })
      .catch(() => {});

    const maxAttempts = CONFIG.maxRetries + 1;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const attemptNum = attempt + 1;
      this.metrics.totalEntriesAttempted++;
      
      if (attempt > 0) {
        const remainingMs = entry.endsAt ? Math.max(0, entry.endsAt - Date.now()) : Number.POSITIVE_INFINITY;
        const configured = exponentialBackoff(attempt - 1, CONFIG.retryDelayMs, 5000);
        const fastRetry = Math.min(configured, 750 * Math.pow(2, attempt - 1));
        const backoffMs = Math.min(fastRetry, Math.max(0, remainingMs - 1500));
        if (backoffMs > 0) await delay(backoffMs);
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

        void updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'success', {
          attempts: attemptNum,
        }).then(() => { this.metrics.dbQueries++; }).catch(() => {});
        void incrementTokenEntries(session.userId, session.guildId).catch(() => {});
        void updateTokenLastUsed(session.userId, session.guildId).catch(() => {});

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
        if (this.joinOutcomeBuffer.length > MAX_JOIN_OUTCOME_BUFFER) {
          this.joinOutcomeBuffer.splice(0, this.joinOutcomeBuffer.length - MAX_JOIN_OUTCOME_BUFFER);
        }

        this.updateGuildStats(entry.guildId, entry.guildName, 'entered');
        this.updateAccountStats(session.userId, 'entered');

        session.interactionCircuitBreaker.reset();

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
        
        if (errorMsg.includes('already entered') || 
            errorMsg.includes('already joined') ||
            errorMsg.includes('already participating')) {
          await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'skipped', {
            lastError: 'Already entered',
          });
          this.metrics.dbQueries++;
          this.joinQueue.cancelGiveaway(entry.messageId, entry.channelId);
          return;
        }
        
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
        
        const isNoResponse = errorMsg.toLowerCase().includes('no response from application');

        if (isNoResponse && attempt < maxAttempts - 1) {
          this.noResponseCooldown.set(session.userId, Date.now() + NO_RESPONSE_COOLDOWN_MS);
          await delay(250);
          try { await this.refreshButtonData(entry as GiveawayEntry, session); } catch {}
          continue;
        }

        void updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'attempting', {
          attempts: attemptNum,
          lastError: errorMsg
        }).then(() => { this.metrics.dbQueries++; }).catch(() => {});
        
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
      buttonCustomId: entry.buttonCustomId,
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

    if (CONFIG.buttonDelayMs > 0) {
      await delay(Math.min(CONFIG.buttonDelayMs, 50));
    }

    const cacheKey = `${entry.channelId}:${entry.messageId}`;
    const cached = this.messageCache.get(cacheKey);
    
    let message: Message | null = null;
    
    if (cached) {
      try {
        const channel = session.client.channels.cache.get(entry.channelId) as TextChannel;
        if (channel) {
          message = await channel.messages.fetch(entry.messageId, { force: false, cache: true }) as Message;
        }
      } catch {}
    }
    
    if (!message) {
      message = await this.fetchMessageUncached(session.client, entry.channelId, entry.messageId);
    }
    
    if (!message) throw new Error(`Message ${entry.messageId} not found`);

    if (!message.guild) {
      throw new Error('Cannot enter giveaway in DM - buttons require guild context');
    }

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
    // Use the manager's controlled interaction path instead of the library's
    // internal RequestHandler. The library path is what was producing repeated
    // aborted /interactions requests under load.
    await this.postInteraction(message, button, session);
  }

  private async postInteraction(message: Message, button: GiveawayButton, session: UserSession): Promise<void> {
    if (!message.guild) {
      throw new Error('Cannot click buttons in DMs - guild context required');
    }

    if (session.interactionCircuitBreaker.isOpen()) {
      throw new Error(`Circuit breaker is open (${session.interactionCircuitBreaker.getState()})`);
    }

    await session.interactionCircuitBreaker.execute(async () => {
      const client = message.client as any;
      
      let wsSessionId = session.gatewaySessionId;
      
      if (!wsSessionId || (Date.now() - session.lastSessionIdFetch > 30000)) {
        wsSessionId = await this.getGatewaySessionId(client);
        session.gatewaySessionId = wsSessionId;
        session.lastSessionIdFetch = Date.now();
      }
      
      if (!wsSessionId) {
        // Do not force a gateway reconnect from an interaction request. That can
        // interrupt a healthy websocket and create reconnect storms. The gateway
        // client's own reconnect/resume machinery is authoritative.
        await delay(250);
        wsSessionId = await this.getGatewaySessionId(client);
        session.gatewaySessionId = wsSessionId;
        session.lastSessionIdFetch = Date.now();
      }

      if (!wsSessionId) {
        throw new Error('No active gateway session ID available; websocket is reconnecting');
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
        guild_id: message.guild!.id,  // ✅ ALWAYS a string
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
            timeout: 2500,
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
            await delay(Math.min(retryAfterMs, 1000));
            continue;
          }

          if (status === 401 || status === 403) {
            this.asyncLogger.error('Token appears to be blocked or invalid', { 
              userId: session.userId, status 
            });
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

    const channelId = this.getMessageChannelId(message);
    if (!channelId) return;
    const dedupKey = `${userId}:${channelId}:${message.id}`;
    if (this.recentWins.get(dedupKey) !== undefined) return;
    this.recentWins.set(dedupKey, Date.now());
    const session = this.findSessionByUserId(userId);
    if (session) {
      session.stats.wins++;
      this.updateGuildStats(message.guild.id, message.guild.name, 'wins');
      this.updateAccountStats(userId, 'wins');
    }
    this.metrics.totalWinsDetected++;
    await incrementTokenWins(userId, session?.guildId || '').catch(error => {
      this.asyncLogger.warn('Failed to record token win', { userId, error: formatError(error) });
    });
    const prize = this.extractWinPrize(message);
    const sourceName = `#${(message.channel as { name?: string }).name ?? channelId} in ${message.guild.name}`;
    this.asyncLogger.info('🏆 AutoJoin: WIN DETECTED!', {
      userId, premiumUserId: userId, prize, source: sourceName, guild: message.guild.name, worker: this.workerId,
    });
    await this.sendWinWebhook(message, prize, sourceName, userId);
    this.emit('giveawayWon', { message, prize, userId, source: sourceName });
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
    await incrementTokenWins(userId, session?.guildId || '').catch(error => {
      this.asyncLogger.warn('Failed to record DM token win', { userId, error: formatError(error) });
    });
    const prize = this.extractWinPrize(message);
    this.asyncLogger.info('🏆 AutoJoin: WIN DETECTED (DM)!', {
      userId, premiumUserId: userId, prize, worker: this.workerId,
    });
    await this.sendWinWebhook(message, prize, 'Direct Message', userId);
    this.emit('giveawayWon', { message, prize, userId, source: 'dm' });
  }

  // -------------------------------------------------------------------------
  // Webhooks - FIXED: Proper DM handling and prize text cleaning
  // -------------------------------------------------------------------------

  private async sendWinWebhook(message: Message, prize: string, sourceName: string, userId: string): Promise<void> {
    const session = this.findSessionByUserId(userId);
    const guildId = session?.guildId || message.guild?.id || '';

    let url: string | null = null;
    try {
      url = await getUserWebhook(userId, guildId);
    } catch {}

    if (!url) url = CONFIG.winWebhookUrl || null;
    if (!url) return;

    const premiumUserId = userId;
    
    // 🔥 FIXED: Clean prize text - remove markdown links, keep just the display text
    const safePrize = this.cleanWinPrize(prize);
    const article = this.getIndefiniteArticle(safePrize);

    const messageLink = message.guild
      ? `https://discord.com/channels/${message.guild.id}/${message.channel.id}/${message.id}`
      : null;

    let inviteLink: string | null = null;
    if (message.guild) {
      inviteLink = await this.getWinInvite(
        message.guild.id,
        message.channel.id,
        session?.client || (message as any).client,
      );
    }

    // 🔥 FIXED: Build location text properly for DMs vs guild messages
    let locationText: string;
    if (message.guild) {
      const guildName = message.guild.name;
      const channelName = `#${(message.channel as { name?: string }).name ?? message.channel.id}`;
      locationText = `**Server:** ${guildName}\n**Channel:** ${channelName}`;
    } else {
      // DM message - just show "Direct Message"
      locationText = 'Direct Message';
    }

    const components: any[] = [
      {
        type: 17,
        accent_color: 0x5865F2,
        components: [
          {
            type: 10,
            content: '# AutoJoin WIN',
          },
          {
            type: 14,
            divider: true,
            spacing: 1,
          },
          {
            type: 10,
            content: `<@${premiumUserId}> won ${article} **${safePrize}**`,
          },
          {
            type: 10,
            content: locationText,
          },
        ],
      },
    ];

    const linkButtons: any[] = [];
    if (messageLink) {
      linkButtons.push({
        type: 2,
        style: 5,
        label: 'Jump to Giveaway',
        url: messageLink,
      });
    }
    if (inviteLink) {
      linkButtons.push({
        type: 2,
        style: 5,
        label: 'Join Server',
        url: inviteLink,
      });
    }

    if (linkButtons.length > 0) {
      components[0].components.push({
        type: 14,
        divider: true,
        spacing: 1,
      });
      components[0].components.push({
        type: 1,
        components: linkButtons,
      });
    }

    try {
      await this.http.post(url, {
        flags: 32768,
        components,
        allowed_mentions: { users: [premiumUserId] },
        username: 'AutoJoin WIN',
      }, {
        timeout: 8000,
        params: {
          with_components: true,
        },
      });
    } catch (error: any) {
      this.asyncLogger.warn('Win webhook failed', {
        userId,
        status: error?.response?.status,
        response: error?.response?.data,
        error: formatError(error),
      });
    }
  }

  private async getWinInvite(
    guildId: string,
    preferredChannelId?: string,
    client?: Client,
  ): Promise<string | null> {
    const cached = this.winInviteCache.get(guildId);
    if (cached) return cached;

    const pending = this.pendingWinInvites.get(guildId);
    if (pending) return pending;

    const promise = this.resolveWinInvite(guildId, preferredChannelId, client);
    this.pendingWinInvites.set(guildId, promise);

    try {
      return await promise;
    } finally {
      this.pendingWinInvites.delete(guildId);
    }
  }

  private async resolveWinInvite(
    guildId: string,
    preferredChannelId?: string,
    client?: Client,
  ): Promise<string | null> {
    try {
      const guild = client?.guilds.cache.get(guildId);
      if (!guild) return null;

      try {
        const vanity = (guild as any).vanityURLCode as string | null | undefined;
        if (vanity) {
          const url = `https://discord.gg/${vanity}`;
          this.winInviteCache.set(guildId, url);
          return url;
        }
      } catch {}

      try {
        const invites = await guild.invites.fetch();
        if (invites?.size) {
          const best =
            invites.find(inv => inv.maxAge === 0 && inv.maxUses === 0) ??
            invites.find(inv => inv.maxAge === 0) ??
            invites.first();
          if (best?.url) {
            this.winInviteCache.set(guildId, best.url);
            return best.url;
          }
        }
      } catch {}

      const channels = Array.from(guild.channels.cache.values())
        .filter((channel): channel is TextChannel => channel.type === 'GUILD_TEXT');

      channels.sort((a, b) => {
        if (preferredChannelId === a.id) return -1;
        if (preferredChannelId === b.id) return 1;
        return 0;
      });

      for (const channel of channels) {
        try {
          const invite = await channel.createInvite({
            maxAge: 0,
            maxUses: 0,
            reason: 'AutoJoin win notifier',
            temporary: false,
          });
          if (invite?.url) {
            this.winInviteCache.set(guildId, invite.url);
            return invite.url;
          }
        } catch {}
      }
    } catch (error) {
      this.asyncLogger.debug('Failed to resolve win invite', {
        guildId,
        error: formatError(error),
      });
    }

    return null;
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

  private scheduleCleanup(userId: string): void {
    if (this.cleanupScheduled.has(userId)) return;
    this.cleanupScheduled.add(userId);
    const timeout = setTimeout(() => {
      this.cleanupScheduled.delete(userId);
      void cleanupAutoJoinEntries(userId).then(() => {
        this.metrics.dbQueries++;
      }).catch(error => {
        this.asyncLogger.debug('AutoJoin cleanup failed', { userId, error: formatError(error) });
      });
    }, 30000);
    timeout.unref?.();
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
    let timeout: NodeJS.Timeout | undefined;
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeout = setTimeout(() => reject(new Error(`Operation timeout: ${label} after ${timeoutMs}ms`)), timeoutMs);
      if (timeout.unref) timeout.unref();
    });
    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      if (timeout) clearTimeout(timeout);
    }
  }

  private restartSessionWorkers(session: UserSession): void {
    if (this.isShuttingDown || session.destroyed || !session.isActive) return;

    const ingestQueue = this.ingestQueues.get(session.userId);
    if (ingestQueue && ingestQueue.length > 0 && !this.ingestWorkers.has(session.userId)) {
      const worker = this.processIncomingMessageQueue(session)
        .catch(error => {
          this.asyncLogger.error('Resumed ingest worker failed', {
            userId: session.userId,
            error: formatError(error),
          });
        })
        .finally(() => {
          this.ingestWorkers.delete(session.userId);
          if (
            !this.isShuttingDown &&
            session.isActive &&
            !session.destroyed &&
            (this.ingestQueues.get(session.userId)?.length || 0) > 0 &&
            !this.ingestWorkers.has(session.userId)
          ) {
            this.restartSessionWorkers(session);
          }
        });
      this.ingestWorkers.set(session.userId, worker);
    }

    this.ensureIngestWorker(session);

    if (this.joinQueue.hasEntriesForUser(session.userId) && !this.queueProcessorPromises.has(session.userId)) {
      this.startQueueProcessor(session.userId);
    }
  }

  // -------------------------------------------------------------------------
  // Health Check System
  // -------------------------------------------------------------------------

  private startHealthChecker(): void {
    this.healthCheckInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      const now = Date.now();

      for (const session of this.sessions.values()) {
        if (session.destroyed) continue;

        try {
          const client = session.client as any;
          const readyState = client.ws?.connection?.readyState;
          const isReady = client.isReady?.() === true;
          const isConnected = isReady && readyState === 1;

          if (isConnected) {
            if (!session.reconnectInProgress) session.stableSince = session.stableSince || now;
            this.restartSessionWorkers(session);

            const pipelineAge = now - session.lastPipelineActivityAt;
            const ingestQueued = this.ingestQueues.get(session.userId)?.length || 0;
            const hasPendingWork =
              ingestQueued > 0 ||
              this.joinQueue.hasEntriesForUser(session.userId);

            if (ingestQueued > 0 && !this.ingestWorkers.has(session.userId)) {
              this.asyncLogger.warn('⚠️ Detection queue has work but no ingest worker; restarting', {
                userId: session.userId,
                queued: ingestQueued,
              });
              this.ensureIngestWorker(session);
            }

            if (hasPendingWork && pipelineAge > SESSION_PIPELINE_STALL_MS) {
              this.asyncLogger.warn('⚠️ Session pipeline appears stalled; restarting workers', {
                userId: session.userId,
                pipelineAgeMs: pipelineAge,
                ingestQueueSize: this.ingestQueues.get(session.userId)?.length || 0,
                hasJoinQueue: this.joinQueue.hasEntriesForUser(session.userId),
              });
              session.lastPipelineActivityAt = now;
              this.restartSessionWorkers(session);
            }
            continue;
          }

          if (session.lastDisconnectAt === 0) {
            session.lastDisconnectAt = now;
          }

          // Give the library's native reconnect/resume path time to work.
          if (now - session.lastDisconnectAt < RECONNECT_GRACE_MS) continue;
          if (session.reconnectInProgress) continue;
          if (now - session.lastReconnectAt < RECONNECT_COOLDOWN_MS) continue;

          if (session.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            this.asyncLogger.warn('⚠️ Session exceeded reconnect attempts; scheduling controlled replacement', {
              userId: session.userId,
              attempts: session.reconnectAttempts,
            });
            this.scheduleRetry(session.userId, session.guildId).catch(() => {});
            continue;
          }

          session.reconnectInProgress = true;
          session.reconnectAttempts++;
          session.lastReconnectAt = now;

          try {
            // Only intervene after the native reconnect has had a grace period.
            client.ws?.reconnect?.();
          } catch (error) {
            session.reconnectInProgress = false;
            this.asyncLogger.warn('Gateway reconnect request failed', {
              userId: session.userId,
              error: formatError(error),
            });
          }
        } catch {
          // Keep the health loop non-fatal.
        }
      }
    }, HEALTH_CHECK_INTERVAL_MS);

    if (this.healthCheckInterval.unref) this.healthCheckInterval.unref();
  }

  private startStallChecker(): void {
    this.stallCheckInterval = setInterval(() => {
      if (this.isShuttingDown) return;

      let stalled = 0;
      for (const [_, session] of this.sessions) {
        if (!session.isActive || session.destroyed) {
          stalled++;
          continue;
        }
        
        try {
          const client = session.client as any;
          if (!client.isReady() || client.ws?.connection?.readyState !== 1) {
            stalled++;
          }
        } catch {
          stalled++;
        }
      }
      
      if (stalled > 0) {
        this.asyncLogger.debug(`⚠️ ${stalled} sessions appear stalled`, {
          worker: this.workerId,
          totalSessions: this.sessions.size,
        });
      }

      for (const session of this.sessions.values()) {
        if (session.isActive && !session.destroyed) {
          this.restartSessionWorkers(session);
        }
      }
    }, 30000);
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
  // Helpers - FIXED: cleanWinPrize now properly removes markdown links
  // -------------------------------------------------------------------------

  private extractWinPrize(message: Message): string {
    const fields = message.embeds?.flatMap(embed => embed.fields ?? []) ?? [];
    const prizeField = fields.find(field => /^(prize|reward|item|gift|won|winner|you won)$/i.test(field.name.trim()));
    if (prizeField?.value) {
      const cleaned = this.cleanWinPrize(prizeField.value);
      if (cleaned) return cleaned;
    }
    const text = this.extractAllText(message);
    const patterns: RegExp[] = [
      /(?:you|you\s+have|you['’]ve)\s+won\s+(?:the\s+)?(?:giveaway\s+)?(?:for\s+)?(?:an?\s+)?(?:\*\*|__)?([^*\n.!?]+?)(?:\*\*|__)?(?=\s*(?:giveaway|raffle|prize)?[.!?]|$)/i,
      /(?:congratulations?|winner)[^\n]*?(?:won|wins?)\s+(?:the\s+)?(?:an?\s+)?(?:\*\*|__)?([^*\n.!?]+?)(?:\*\*|__)?(?=\s*(?:giveaway|raffle|prize)?[.!?]|$)/i,
      /(?:won|wins?)\s+(?:the\s+)?(?:an?\s+)?(?:\*\*|__)?([^*\n.!?]+?)(?:\*\*|__)?(?:\s+giveaway)?[.!?]?$/i,
    ];
    for (const pattern of patterns) {
      const match = text.match(pattern);
      if (match?.[1]) {
        const cleaned = this.cleanWinPrize(match[1]);
        if (cleaned && !/^(the )?(giveaway|prize|winner)$/i.test(cleaned)) return cleaned;
      }
    }
    for (const embed of message.embeds ?? []) {
      const title = this.cleanWinPrize(embed.title ?? '');
      if (title && !/^(congratulations?|you won|winner|giveaway|giveaway winner)[! .-]*$/i.test(title)) return title;
      const description = this.cleanWinPrize(embed.description ?? '');
      if (description && WIN_PATTERNS.some(re => re.test(description))) {
        const match = description.match(/(?:won|wins?)\s+(?:the\s+)?(?:an?\s+)?(?:\*\*)?([^*\n.!?]+)(?:\*\*)?/i);
        if (match?.[1]) return this.cleanWinPrize(match[1]);
      }
    }
    return this.extractPrize(message);
  }

  private cleanWinPrize(value: string): string {
    // 🔥 FIXED: More aggressive markdown link cleaning
    const cleaned = value
      .replace(/\[([^\]]+)\]\(https?:\/\/[^\s)]+\)/gi, '$1') // Extract text from markdown links
      .replace(/\[([^\]]+)\]\([^)]+\)/gi, '$1') // Extract text from any markdown link
      .replace(/https?:\/\/[^\s)]+/gi, '') // Remove raw URLs
      .replace(/\*\*|__|~~|`/g, '') // Remove formatting
      .replace(/^[:\-–—|\s]+|[:\-–—|\s]+$/g, '') // Trim punctuation
      .replace(/^(?:the\s+)?(?:giveaway|prize)\s*[:\-–—]?\s*/i, '') // Remove prefixes
      .replace(/\s{2,}/g, ' ') // Collapse spaces
      .trim();

    return this.cleanText(cleaned) || 'Unknown Prize';
  }

  private getIndefiniteArticle(prize: string): 'a' | 'an' {
    const word = prize.trim().split(/\s+/)[0] ?? '';
    if (/^\d/.test(word)) return /^[8]/.test(word) ? 'an' : 'a';
    if (/^[A-Z]{2,}$/.test(word)) return /^[AEFHILMNORSX]/.test(word) ? 'an' : 'a';
    return /^[aeiou]/i.test(word) ? 'an' : 'a';
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
    return this.fetchMessageUncached(client, channelId, messageId);
  }

  private makeEntryId(session: UserSession, message: Message): string {
    const channelId = this.getMessageChannelId(message) ?? 'unknown-channel';
    return `${session.userId}:${channelId}:${message.id}`;
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
    this.cleanupInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      const users = Array.from(this.sessions.values())
        .filter(s => s.isActive && !s.destroyed)
        .map(s => s.userId);
      for (let i = 0; i < users.length; i += 10) {
        const batch = users.slice(i, i + 10);
        Promise.allSettled(batch.map(userId => cleanupAutoJoinEntries(userId))).catch(() => {});
      }
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

  private startReconnectChecker(): void {
    this.reconnectCheckInterval = setInterval(() => {
      if (!this.isShuttingDown && this.checkMemory()) {
        this.retryFailedSessions().catch((error) => {
          this.asyncLogger.error('Reconnect check failed', { error: formatError(error) });
        });
      }
    }, RETRY_CHECK_INTERVAL_MS);
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
        this.messageCache.clean(),
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

  // ============================================================
  // 🔥 COMPLETE getStats() - Needed for index.ts
  // ============================================================

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
    
    return {
      totalSessions: this.sessions.size,
      activeSessions: active,
      totalDetected,
      totalEntered,
      totalWins,
      sessionStats,
      worker: this.workerId,
      healthStatus: this.healthStatus,
      circuitBreakerState: Array.from(this.sessions.values()).some(
        session => session.interactionCircuitBreaker.isOpen()
      ) ? 'open' : 'closed',
      caches: {
        processedMessages: this.processedMessages.size,
        processing: this.processingCache.size,
        recentWins: this.recentWins.size,
        noResponseCooldown: this.noResponseCooldown.size,
        tokenCache: this.tokenManager.getCacheStats(),
        crosspostCache: this.crosspostCache.size,
        messageCache: this.messageCache.size,
      },
      memory: {
        heapUsedMB: mem.heapUsedMB,
        heapTotalMB: mem.heapTotalMB,
        rssMB: mem.rssMB,
        percentageUsed: Math.round((mem.heapUsedMB / Math.max(mem.heapTotalMB, 1)) * 100),
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
      retryScheduled: this.retryScheduled.size,
      tokenFailures: Array.from(this.tokenFailureTracker.entries()).map(([userId, data]) => ({
        userId,
        failures: data.failures,
        lastAttempt: new Date(data.lastAttempt).toISOString()
      })),
    };
  }

  // ============================================================
  // 🔥 restoreSessionsFromDatabase() - Needed for index.ts
  // ============================================================

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
        await delay(200);
      }
      
      this.asyncLogger.info(`✅ Restored ${restored} AutoJoin sessions (${failed} failed, ${skipped} skipped)`, {
        worker: this.workerId, total: this.sessions.size, memory: this.getMemoryUsage(),
      });
    } catch (error) {
      this.asyncLogger.error('Failed to restore AutoJoin sessions', { error: formatError(error) });
    }
  }

  // ============================================================
  // 🔥 refreshSessions() - Needed for index.ts
  // ============================================================

  async refreshSessions(): Promise<void> {
    if (this.isShuttingDown) return;
    if (!this.checkMemory()) return;

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const activeUserIds = new Set(allPremiumUsers.filter(u => u.token).map(u => u.userId));

      for (const [key, session] of this.sessions) {
        if (!activeUserIds.has(session.userId)) {
          await this.stopSession(session.userId, session.guildId);
        }
      }

      for (const user of allPremiumUsers) {
        if (!this.checkMemory()) break;
        if (!user.token) continue;
        const sessionKey = this.makeSessionKey(user.userId);
        if (!this.sessions.has(sessionKey)) {
          await this.startSession(user.userId, user.guildId);
          await delay(100);
        }
      }

      this.logStats();
    } catch (error) {
      this.asyncLogger.error('Failed to refresh sessions', { error: formatError(error) });
    }
  }

  // ============================================================
  // 🔥 retryFailedSessions() - Needed for index.ts
  // ============================================================

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
        
        const isDeadSession = hasSession && session && (!session.isActive || session.destroyed);
        const isInactive = user.tokenActive === false;
        
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
          
          if (isDeadSession && session) {
            await this.stopSession(session.userId, session.guildId);
          }

          const success = await this.startSession(user.userId, user.guildId);
          
          if (success) {
            this.asyncLogger.info(`✅ Reactivated ${user.userId}`);
            this.tokenFailureTracker.delete(user.userId);
          } else {
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
      const key = `${userId}:${guildId}`;
      this.tokenFailureTracker.set(userId, {
        failures: (this.tokenFailureTracker.get(userId)?.failures || 0) + 1,
        lastAttempt: Date.now()
      });
    } catch (error) {
      // Silent fail
    }
  }

  // ============================================================
  // 🔥 stopSession() - Proper cleanup
  // ============================================================

  async stopSession(userId: string, guildId: string): Promise<void> {
    const sessionKey = this.makeSessionKey(userId);
    const session = this.sessions.get(sessionKey);
    if (!session) return;

    session.destroyed = true;
    session.isActive = false;

    this.ingestQueues.delete(userId);
    this.ingestQueuedKeys.delete(userId);
    this.ingestWorkers.delete(userId);
    this.queueProcessorPromises.delete(userId);

    try {
      this.cleanupSessionListeners(session);
      this.clearClientCaches(session.client);
      
      try {
        await session.client.destroy();
      } catch {}
      
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(userId);
      this.tokenManager.clearCache(userId, guildId);

      const retryKey = `${userId}:${guildId}`;
      const retryTimer = this.retryScheduled.get(retryKey);
      if (retryTimer) {
        clearTimeout(retryTimer);
        this.retryScheduled.delete(retryKey);
      }
      
      this.asyncLogger.info('⏹️ AutoJoin session stopped', { 
        userId, guildId, sessionId: session.sessionId, memory: this.getMemoryUsage(),
      });
      
      this.emit('sessionStopped', { userId, guildId });
    } catch (error) {
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(userId);
    }
  }

  // ============================================================
  // 🔥 shutdown() - Complete cleanup
  // ============================================================

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
    this.refreshInterval = null;
    this.cleanupInterval = null;
    this.memoryCheckInterval = null;
    this.reconnectCheckInterval = null;
    this.cacheCleanInterval = null;
    this.metricsInterval = null;
    this.healthCheckInterval = null;
    this.queuePersistInterval = null;
    this.stallCheckInterval = null;
    this.batchDbInterval = null;
    this.archiveInterval = null;
    this.statsCleanInterval = null;

    for (const [key, timeout] of this.retryScheduled) {
      clearTimeout(timeout);
    }
    this.retryScheduled.clear();

    this.ingestQueues.clear();
    this.ingestQueuedKeys.clear();

    if (this.queueProcessorPromises.size > 0 || this.ingestWorkers.size > 0) {
      try {
        await Promise.race([
          Promise.allSettled([
            ...this.queueProcessorPromises.values(),
            ...this.ingestWorkers.values(),
          ]),
          delay(5000),
        ]);
      } catch {}
    }
    this.queueProcessorPromises.clear();
    this.ingestWorkers.clear();

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
      
      this.cleanupSessionListeners(session);
      this.clearClientCaches(session.client);
      
      try { 
        session.client.removeAllListeners();
        await session.client.destroy(); 
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
    this.messageCache.clear();
    this.tokenManager.clearAll();
    this.sessionStartPromises.clear();
    this.ingestQueues.clear();
    this.ingestQueuedKeys.clear();
    this.ingestWorkers.clear();
    this.queueProcessorPromises.clear();
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
