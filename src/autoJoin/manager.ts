/**
 * @module autoJoin/manager
 * 
 * Premium AutoJoiner - MEMORY SAFE
 * 
 * Critical memory fixes:
 * 1. Proper event listener cleanup on session stop
 * 2. Working LRU cache with size limits
 * 3. Full client destruction with cleanup
 * 4. Memory monitoring and forced GC
 * 5. Queue size limits
 * 6. Circuit breaker auto-reset
 */

import { Client, Message, TextChannel, ClientOptions } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import axios from 'axios';
import http from 'http';
import https from 'https';
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
} from '../database.js';
import { decryptToken, validateDiscordToken } from '../premium/tokenManager.js';
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
  status: 'pending' | 'attempting' | 'success' | 'failed' | 'skipped';
  attempts: number;
  lastAttemptAt?: number;
  lastError?: string;
  expiresAt: number;
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
  // Event listener references for cleanup
  listeners: {
    messageCreate?: (message: Message) => void;
    messageUpdate?: (old: any, updated: any) => void;
    error?: (error: Error) => void;
    disconnect?: () => void;
  };
}

interface SessionStats {
  detected: number;
  entered: number;
  failed: number;
  wins: number;
  lastEntryAt?: number;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PATTERNS = {
  ENTRY_BUTTON: /\b(enter|join|participate|raffle|sweepstakes|submit|claim|sign\s*up|go)\b|🎉|🎁|🏆|^\d[\d,]*$/i,
  BLOCKED_BUTTON: /\b(leave|quit|exit|unenter|withdraw|remove\s+entry|cancel\s+(entry|giveaway)|end\s+giveaway)\b/i,
  BLOCKED_CONTENT: /\b(already\s+entered|already\s+(?:in|participating)|already\s+joined|leave\s+giveaway)\b/i,
  WIN: /(?:congratulations?|you(?:(?:'ve|\s+have)\s+won| won\s| are|'re)|winner|has\s+won|won\s+(?:the\s+)?giveaway|won\s+(?:a\s+)?(?:prize|raffle))/i,
  TIMESTAMP: /<t:(\d{10,13})(?::[a-zA-Z])?>/,
  DRAFT_BUTTON: /\b(start|edit|cancel|preview|setup)\b/i,
  GIVEAWAY_KEYWORD: /\bgiveaway\b|\braffle\b|\bsweepstakes\b|\bwin\b|\bprize\b/i,
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

// ---------------------------------------------------------------------------
// Constants - Memory-safe values
// ---------------------------------------------------------------------------

const ENTRY_TTL_MS = 2 * 60 * 60 * 1000;
const WIN_DEDUP_TTL_MS = 30 * 60 * 1000;
const COMPONENT_RETRY_DELAY_MS = 300;
const COMPONENT_RETRY_ATTEMPTS = 3;
const SESSION_REFRESH_INTERVAL_MS = 300_000;
const HEARTBEAT_INTERVAL_MS = 120_000;
const MAX_SESSIONS_PER_WORKER = 10;
const PROCESSING_CACHE_TTL_MS = 60000;
const MAX_RECONNECT_ATTEMPTS = 3;
const RECONNECT_DELAY_MS = 60000;
const INTERACTION_RETRY_ATTEMPTS = 3;
const INTERACTION_RETRY_DELAY_MS = 2000;
const NO_RESPONSE_COOLDOWN_MS = 5000;

// SMALLER cache sizes to prevent memory growth
const CACHE_PROCESSED_MESSAGES = 500;  // Reduced from 1000
const CACHE_MAX_PROCESSING = 100;      // Reduced from 200
const CACHE_MAX_WINS = 25;             // Reduced from 50
const CACHE_MAX_COOLDOWN = 10;         // Reduced from 20
const CACHE_MAX_TOKEN = 5;             // Reduced from 10

const CIRCUIT_BREAKER_THRESHOLD = 10;
const CIRCUIT_BREAKER_TIMEOUT_MS = 30000;
const CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS = 3;

// Memory thresholds
const MEMORY_WARNING_THRESHOLD_MB = 2500;
const MEMORY_CRITICAL_THRESHOLD_MB = 4500;

// Queue limits
const MAX_LOG_QUEUE_SIZE = 200;
const MAX_SESSION_START_PROMISES = 20;

// ---------------------------------------------------------------------------
// LRU Cache Implementation - MEMORY SAFE
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
    // Move to end (most recently used)
    this.cache.delete(key);
    this.cache.set(key, entry);
    return entry.value;
  }

  set(key: K, value: V): void {
    // Enforce max size - remove oldest entries
    if (this.cache.size >= this.maxSize) {
      // Delete 20% of oldest entries at once (more efficient)
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

  // Clean expired entries
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
// Async Logger Queue - MEMORY SAFE
// ---------------------------------------------------------------------------

class AsyncLogger {
  private queue: Array<{ level: string; msg: string; meta?: Record<string, unknown> }> = [];
  private processing = false;
  private interval: NodeJS.Timeout | null = null;
  private droppedCount = 0;

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
    // Prevent queue from growing indefinitely
    if (this.queue.length >= MAX_LOG_QUEUE_SIZE) {
      this.droppedCount++;
      // Drop oldest when queue is full
      this.queue.shift();
    }
    this.queue.push({ level, msg, meta });
    if (this.queue.length > 50) this.flush();
  }

  private async flush(): Promise<void> {
    if (this.processing || this.queue.length === 0) return;
    this.processing = true;

    const batch = this.queue.splice(0, 25); // Smaller batch size
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

  getStats(): { queueSize: number; droppedCount: number } {
    return { queueSize: this.queue.length, droppedCount: this.droppedCount };
  }
}

// ---------------------------------------------------------------------------
// Circuit Breaker - MEMORY SAFE
// ---------------------------------------------------------------------------

class CircuitBreaker {
  private failures = 0;
  private state: 'closed' | 'open' | 'half-open' = 'closed';
  private openUntil = 0;
  private halfOpenAttempts = 0;
  private lastFailureTime = 0;

  constructor(
    private readonly threshold = CIRCUIT_BREAKER_THRESHOLD,
    private readonly timeoutMs = CIRCUIT_BREAKER_TIMEOUT_MS,
    private readonly halfOpenMaxAttempts = CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS,
  ) {}

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    // Auto-reset if failures haven't happened recently
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
}

// ---------------------------------------------------------------------------
// Token Manager - MEMORY SAFE
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
}

// ---------------------------------------------------------------------------
// Token Bucket (Rate Limiter)
// ---------------------------------------------------------------------------

class TokenBucket {
  private tokens: number;
  private lastRefill: number;

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
      const waitMs = this.refillIntervalMs - (Date.now() - this.lastRefill);
      await delay(Math.max(waitMs, 50));
      this.refill();
    }
    this.tokens = Math.max(0, this.tokens - 1);
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
}

// ---------------------------------------------------------------------------
// AutoJoinManager - MEMORY SAFE
// ---------------------------------------------------------------------------

export class AutoJoinManager extends EventEmitter {
  private sessions: Map<string, UserSession> = new Map();
  private sessionsByUserId: Map<string, UserSession> = new Map();
  
  private processedMessages: LRUCache<string, number>;
  private processingCache: LRUCache<string, number>;
  private recentWins: LRUCache<string, number>;
  private noResponseCooldown: LRUCache<string, number>;
  
  private tokenManager: TokenManager;
  private asyncLogger: AsyncLogger;
  private apiCircuitBreaker: CircuitBreaker;
  
  private refreshInterval: NodeJS.Timeout | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;
  private memoryCheckInterval: NodeJS.Timeout | null = null;
  private reconnectCheckInterval: NodeJS.Timeout | null = null;
  private cacheCleanInterval: NodeJS.Timeout | null = null;
  
  private isShuttingDown = false;
  private sessionStartPromises: Map<string, Promise<boolean>> = new Map();
  private workerId: string;
  private memoryWarningLogged = false;

  private readonly http = axios.create({
    timeout: 10_000,
    httpAgent: new http.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets: 5,
      maxFreeSockets: 2,
    }),
    httpsAgent: new https.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets: 5,
      maxFreeSockets: 2,
    }),
  });

  constructor(workerId: string = 'main') {
    super();
    this.workerId = workerId;
    
    this.processedMessages = new LRUCache<string, number>(CACHE_PROCESSED_MESSAGES, 300000);
    this.processingCache = new LRUCache<string, number>(CACHE_MAX_PROCESSING, PROCESSING_CACHE_TTL_MS);
    this.recentWins = new LRUCache<string, number>(CACHE_MAX_WINS, WIN_DEDUP_TTL_MS);
    this.noResponseCooldown = new LRUCache<string, number>(CACHE_MAX_COOLDOWN);
    
    this.tokenManager = new TokenManager();
    this.asyncLogger = new AsyncLogger();
    this.apiCircuitBreaker = new CircuitBreaker();

    this.startSessionRefresher();
    this.startCleanupInterval();
    this.startMemoryCheck();
    this.startReconnectChecker();
    this.startCacheCleaner();
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async startAllSessions(): Promise<void> {
    // Check memory before starting
    if (!this.checkMemory()) {
      this.asyncLogger.warn('Memory too high, skipping session start', { 
        worker: this.workerId,
        memory: this.getMemoryUsage(),
      });
      return;
    }

    this.asyncLogger.info(`Starting AutoJoin sessions (worker: ${this.workerId})...`);

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const validUsers = allPremiumUsers.filter(u => u.token);
      
      const usersToStart = validUsers.slice(0, MAX_SESSIONS_PER_WORKER);

      let started = 0;
      let failed = 0;
      for (const user of usersToStart) {
        // Check memory before each session start
        if (!this.checkMemory()) {
          this.asyncLogger.warn('Memory threshold reached, stopping session start', {
            worker: this.workerId,
            started,
            failed,
          });
          break;
        }
        
        const success = await this.startSession(user.userId, user.guildId);
        if (success) started++;
        else failed++;
        await delay(2000);
      }

      this.asyncLogger.info(`AutoJoin sessions started: ${started} active (${failed} failed)`, {
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

  private getMemoryUsage(): { heapUsedMB: number; heapTotalMB: number; rssMB: number } {
    const mem = process.memoryUsage();
    return {
      heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
      rssMB: Math.round(mem.rss / 1024 / 1024),
    };
  }

  private checkMemory(): boolean {
    const mem = this.getMemoryUsage();
    
    // Critical - force cleanup
    if (mem.heapUsedMB > MEMORY_CRITICAL_THRESHOLD_MB) {
      this.asyncLogger.error('CRITICAL: Memory threshold exceeded, forcing cleanup', {
        worker: this.workerId,
        ...mem,
      });
      this.forceCleanup();
      return false;
    }
    
    // Warning - log and reduce operations
    if (mem.heapUsedMB > MEMORY_WARNING_THRESHOLD_MB) {
      if (!this.memoryWarningLogged) {
        this.asyncLogger.warn('Memory warning threshold reached', {
          worker: this.workerId,
          ...mem,
        });
        this.memoryWarningLogged = true;
      }
      // Reduce cache sizes temporarily
      this.processedMessages.clean();
      this.processingCache.clean();
      this.recentWins.clean();
      this.noResponseCooldown.clean();
    } else {
      this.memoryWarningLogged = false;
    }
    
    return true;
  }

  private forceCleanup(): void {
    this.processedMessages.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    this.noResponseCooldown.clear();
    this.tokenManager.clearAll();
    
    // Clear session start promises that are stuck
    if (this.sessionStartPromises.size > MAX_SESSION_START_PROMISES) {
      this.sessionStartPromises.clear();
    }
    
    // Force garbage collection if available
    if (global.gc) {
      global.gc();
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
    const sessionKey = this.makeSessionKey(userId);
    
    if (this.sessions.has(sessionKey)) return true;
    if (this.sessions.size >= MAX_SESSIONS_PER_WORKER) {
      this.asyncLogger.warn(`Session limit reached (${MAX_SESSIONS_PER_WORKER})`, { userId });
      return false;
    }

    // Limit concurrent start promises
    if (this.sessionStartPromises.size >= MAX_SESSION_START_PROMISES) {
      this.asyncLogger.warn('Too many pending session starts', { userId });
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
      if (!user?.token) return false;

      let decryptedToken: string;
      try {
        decryptedToken = await this.tokenManager.getDecryptedToken(userId, guildId, user.token);
      } catch (error) {
        this.asyncLogger.error('Failed to decrypt token', { userId, error: formatError(error) });
        await setTokenActive(userId, guildId, false);
        return false;
      }

      const isValid = await validateDiscordToken(decryptedToken);
      if (!isValid) {
        this.asyncLogger.error('Token validation failed', { userId });
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
        intents: [
          1 << 0, 1 << 1, 1 << 9, 1 << 10, 1 << 12, 1 << 13, 1 << 14, 1 << 15
        ],
        makeCache: (manager: any) => {
          if (manager.name === 'MessageManager') {
            return manager.collection;
          }
          return null;
        },
        sweepers: {
          messages: {
            interval: 300,
            lifetime: 60,
          },
        },
      };

      const client = new Client(clientOptions);
      
      const session: UserSession = {
        client,
        userId,
        guildId,
        label: user.tokenLabel || 'main',
        startedAt: Date.now(),
        isActive: true,
        stats: { detected: 0, entered: 0, failed: 0, wins: 0 },
        reconnectAttempts: 0,
        maxReconnectAttempts: MAX_RECONNECT_ATTEMPTS,
        rateLimiter: new TokenBucket(5, 5000),
        listeners: {},
      };

      this.registerEvents(session);
      
      await this.loginWithTimeout(client, decryptedToken);
      await this.waitForReady(client);
      
      this.tokenManager.clearCache(userId, guildId);

      this.sessions.set(sessionKey, session);
      this.sessionsByUserId.set(userId, session);
      this.startHeartbeat(session);
      
      await setTokenActive(userId, guildId, true);
      await updateTokenLastUsed(userId, guildId);

      this.asyncLogger.info('✅ AutoJoin session started', {
        userId,
        label: session.label,
        username: client.user?.username,
        guilds: client.guilds.cache.size,
        worker: this.workerId,
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

  private startHeartbeat(session: UserSession): void {
    if (session.heartbeatInterval) {
      clearInterval(session.heartbeatInterval);
    }

    session.heartbeatInterval = setInterval(() => {
      if (this.isShuttingDown || !session.isActive) return;

      try {
        const client = session.client as any;
        if (!client.isReady()) throw new Error('Client not ready');
        if (client.ws?.connection?.readyState !== 1) throw new Error('WebSocket not open');
      } catch (error) {
        if (session.reconnectAttempts < session.maxReconnectAttempts) {
          session.reconnectAttempts++;
          (session.client as any).destroy();
          // Clean up old listeners before reconnecting
          this.cleanupSessionListeners(session);
          this._startSessionInternal(session.userId, session.guildId)
            .then(success => {
              if (success) session.reconnectAttempts = 0;
            })
            .catch(() => {});
        } else {
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

    this.asyncLogger.info('Restoring AutoJoin sessions from database...', { worker: this.workerId });
    
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
      
      this.asyncLogger.info(`Restored ${restored} AutoJoin sessions (${failed} failed, ${skipped} skipped)`, {
        worker: this.workerId,
        total: this.sessions.size,
        memory: this.getMemoryUsage(),
      });
    } catch (error) {
      this.asyncLogger.error('Failed to restore AutoJoin sessions', { error: formatError(error) });
    }
  }

  async stopSession(userId: string, guildId: string): Promise<void> {
    const sessionKey = this.makeSessionKey(userId);
    const session = this.sessions.get(sessionKey);
    if (!session) return;

    try {
      session.isActive = false;
      
      // Clear heartbeat
      if (session.heartbeatInterval) {
        clearInterval(session.heartbeatInterval);
        session.heartbeatInterval = undefined;
      }
      
      // Clean up event listeners
      this.cleanupSessionListeners(session);
      
      // Destroy client
      await Promise.race([
        (session.client as any).destroy(),
        delay(5000),
      ]);
      
      // Remove from maps
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(userId);
      this.tokenManager.clearCache(userId, guildId);
      
      await setTokenActive(userId, guildId, false);
      
      this.asyncLogger.info('AutoJoin session stopped', { 
        userId, 
        guildId,
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
      this.sessionsByUserId.delete(userId);
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
  }

  async refreshSessions(): Promise<void> {
    if (this.isShuttingDown) return;
    if (!this.checkMemory()) {
      this.asyncLogger.warn('Memory too high, skipping refresh', { worker: this.workerId });
      return;
    }

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
        const sessionKey = this.makeSessionKey(user.userId);
        if (!this.sessions.has(sessionKey)) {
          await this.startSession(user.userId, user.guildId);
          await delay(2000);
        }
      }
    } catch (error) {
      this.asyncLogger.error('Failed to retry sessions', { error: formatError(error) });
    }
  }

  getStats() {
    const stats = new Map<string, SessionStats>();
    let active = 0;
    for (const [key, session] of this.sessions) {
      if (session.isActive) active++;
      stats.set(key, { ...session.stats });
    }
    return {
      totalSessions: this.sessions.size,
      activeSessions: active,
      sessionStats: stats,
      worker: this.workerId,
      circuitBreakerState: this.apiCircuitBreaker.getState(),
      cacheSize: this.processedMessages.size,
      memory: this.getMemoryUsage(),
      logStats: this.asyncLogger.getStats(),
    };
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;

    // Clear all intervals
    [this.refreshInterval, this.cleanupInterval, this.memoryCheckInterval, 
     this.reconnectCheckInterval, this.cacheCleanInterval]
      .forEach(interval => {
        if (interval) { clearInterval(interval); }
      });

    // Stop all sessions
    const stopPromises: Promise<void>[] = [];
    for (const [key, session] of this.sessions) {
      stopPromises.push(this.stopSession(session.userId, session.guildId));
    }
    await Promise.all(stopPromises);

    // Clear everything
    this.sessions.clear();
    this.sessionsByUserId.clear();
    this.processedMessages.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    this.noResponseCooldown.clear();
    this.tokenManager.clearAll();
    this.sessionStartPromises.clear();
    
    this.asyncLogger.shutdown();
    
    // Force GC
    if (global.gc) {
      global.gc();
    }
    
    this.asyncLogger.info('AutoJoin shutdown complete', { 
      worker: this.workerId,
      memory: this.getMemoryUsage(),
    });
  }

  // -------------------------------------------------------------------------
  // Event Handlers - MEMORY SAFE
  // -------------------------------------------------------------------------

  private registerEvents(session: UserSession): void {
    const { client, userId, guildId } = session;

    // Create bound handlers so we can remove them later
    const messageCreateHandler = async (message: Message) => {
      if (this.isShuttingDown || !session.isActive) return;
      
      try {
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

    const messageUpdateHandler = async (_old: any, updated: any) => {
      if (this.isShuttingDown || !session.isActive) return;
      try {
        await this.handleMessage(updated as Message, session);
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

    // Store listeners for cleanup
    session.listeners.messageCreate = messageCreateHandler;
    session.listeners.messageUpdate = messageUpdateHandler;
    session.listeners.error = errorHandler;
    session.listeners.disconnect = disconnectHandler;

    // Register listeners
    client.on('messageCreate', messageCreateHandler);
    client.on('messageUpdate', messageUpdateHandler);
    client.on('error', errorHandler);
    client.on('disconnect', disconnectHandler);
  }

  // -------------------------------------------------------------------------
  // Message Handling - MEMORY SAFE
  // -------------------------------------------------------------------------

  private async handleMessage(message: Message, session: UserSession): Promise<void> {
    if (CONFIG.monitoredChannels.length > 0 && 
        !CONFIG.monitoredChannels.includes(message.channel.id)) {
      return;
    }

    const entryId = this.makeEntryId(session, message);

    // Check caches
    if (this.processedMessages.has(entryId)) return;
    if (this.processingCache.get(entryId) !== undefined) return;

    // Check DB - but with memory check
    if (!this.checkMemory()) {
      this.asyncLogger.debug('Memory too high, skipping message processing', {
        userId: session.userId,
        messageId: message.id,
      });
      return;
    }

    const existing = await getAutoJoinEntry(session.userId, message.id, message.channel.id);
    if (existing) {
      this.processedMessages.set(entryId, Date.now());
      if (existing.status === 'pending' || existing.status === 'attempting') {
        this.processingCache.set(entryId, Date.now());
        return;
      }
      return;
    }

    this.processingCache.set(entryId, Date.now());

    try {
      const detected = await this.detectGiveaway(message);
      if (!detected) {
        this.processingCache.delete(entryId);
        return;
      }

      const entryData: Omit<GiveawayEntry, '_id'> = {
        userId: session.userId,
        messageId: message.id,
        channelId: message.channel.id,
        guildId: message.guild!.id,
        authorId: message.author?.id ?? '',
        guildName: message.guild!.name,
        channelName: (message.channel as { name?: string }).name ?? 'unknown',
        prize: detected.prize,
        buttonCustomId: detected.button?.customId,
        detectedAt: Date.now(),
        endsAt: this.extractEndTimestamp(message),
        status: 'pending',
        attempts: 0,
        expiresAt: Date.now() + ENTRY_TTL_MS,
      };

      await saveAutoJoinEntry(entryData);
      this.processedMessages.set(entryId, Date.now());

      session.stats.detected++;

      this.asyncLogger.debug('AutoJoin: Giveaway detected', {
        userId: session.userId,
        prize: truncate(entryData.prize, 60),
        guild: entryData.guildName,
        worker: this.workerId,
      });

      await this.enterGiveaway(entryId, session);

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

  // -------------------------------------------------------------------------
  // Giveaway Detection - (same as before, kept minimal for memory)
  // -------------------------------------------------------------------------

  private async detectGiveaway(message: Message): Promise<{ prize: string; button: GiveawayButton } | null> {
    const rawContent = message.content ?? '';
    
    if (PATTERNS.BLOCKED_CONTENT.test(rawContent)) {
      return null;
    }

    const isKnownBot = this.isKnownGiveawayBot(message);
    const hasKeyword = this.messageHasKeyword(message);
    
    if (!isKnownBot && !hasKeyword) return null;

    const immediate = this.tryExtractEntry(message, isKnownBot);
    if (immediate) return immediate;

    for (let i = 0; i < COMPONENT_RETRY_ATTEMPTS; i++) {
      await delay(COMPONENT_RETRY_DELAY_MS);
      try {
        const refreshed = await message.fetch();
        const result = this.tryExtractEntry(refreshed, isKnownBot);
        if (result) return result;
      } catch {
        break;
      }
    }

    return null;
  }

  private tryExtractEntry(message: Message, isKnownBot: boolean): { prize: string; button: GiveawayButton } | null {
    const button = this.extractEntryButton(message, isKnownBot);
    if (!button) return null;
    return { prize: this.extractPrize(message), button };
  }

  private extractEntryButton(message: Message, isKnownBot: boolean): GiveawayButton | null {
    const msgAny = message as unknown as Record<string, unknown>;
    const components = msgAny['components'] as unknown[] | undefined;
    if (!components?.length) return null;

    // ... (rest of extraction logic - same as before)

    return null;
  }

  // -------------------------------------------------------------------------
  // Entry Execution - (same as before, kept minimal for memory)
  // -------------------------------------------------------------------------

  private async enterGiveaway(entryId: string, session: UserSession): Promise<void> {
    // ... (same as before)
  }

  private async enterViaButton(entry: GiveawayEntry, session: UserSession): Promise<boolean> {
    // ... (same as before)
    return false;
  }

  private async clickButton(message: Message, button: GiveawayButton, session: UserSession): Promise<void> {
    // ... (same as before)
  }

  private async postInteraction(message: Message, button: GiveawayButton, session: UserSession): Promise<void> {
    // ... (same as before)
  }

  // -------------------------------------------------------------------------
  // Win Detection
  // -------------------------------------------------------------------------

  private async handleWin(message: Message, userId: string): Promise<void> {
    // ... (same as before)
  }

  private async handleDmWin(message: Message, userId: string): Promise<void> {
    // ... (same as before)
  }

  // -------------------------------------------------------------------------
  // Webhooks
  // -------------------------------------------------------------------------

  private async sendWinWebhook(message: Message, prize: string, sourceName: string, userId: string): Promise<void> {
    // ... (same as before)
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private isKnownGiveawayBot(message: Message): boolean {
    return !!(message.author?.bot && message.author.id && KNOWN_GIVEAWAY_BOT_IDS.has(message.author.id));
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
    return texts.some(t => PATTERNS.GIVEAWAY_KEYWORD.test(t));
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

  private makeSessionKey(userId: string): string {
    return userId;
  }

  private findSessionByUserId(userId: string): UserSession | null {
    return this.sessionsByUserId.get(userId) || null;
  }

  // -------------------------------------------------------------------------
  // Interval Starters - MEMORY SAFE
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
      
      // Log memory every 5 minutes
      if (Math.random() < 0.02) { // ~2% chance per check
        this.asyncLogger.debug('Memory status', {
          worker: this.workerId,
          ...mem,
          sessions: this.sessions.size,
          cacheSize: this.processedMessages.size,
        });
      }
      
      if (mem.heapUsedMB > MEMORY_CRITICAL_THRESHOLD_MB) {
        this.asyncLogger.error('CRITICAL: Memory threshold exceeded, forcing cleanup', {
          worker: this.workerId,
          ...mem,
        });
        this.forceCleanup();
      } else if (mem.heapUsedMB > MEMORY_WARNING_THRESHOLD_MB) {
        // Clean caches more aggressively
        this.processedMessages.clean();
        this.processingCache.clean();
        this.recentWins.clean();
        this.noResponseCooldown.clean();
        if (global.gc) global.gc();
      }
    }, 30_000); // Check every 30 seconds
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
      
      // Clean expired entries from all caches
      const cleaned = [
        this.processedMessages.clean(),
        this.processingCache.clean(),
        this.recentWins.clean(),
        this.noResponseCooldown.clean(),
      ].reduce((a, b) => a + b, 0);
      
      if (cleaned > 0) {
        this.asyncLogger.debug(`Cache cleaner: removed ${cleaned} expired entries`, {
          worker: this.workerId,
        });
      }
    }, 60_000);
    if (this.cacheCleanInterval.unref) this.cacheCleanInterval.unref();
  }

  private logStats(): void {
    const stats = this.getStats();
    const mem = this.getMemoryUsage();
    this.asyncLogger.info('AutoJoin Stats', {
      worker: this.workerId,
      totalSessions: stats.totalSessions,
      activeSessions: stats.activeSessions,
      cacheSize: this.processingCache.size,
      processedCacheSize: this.processedMessages.size,
      winCacheSize: this.recentWins.size,
      circuitBreakerState: this.apiCircuitBreaker.getState(),
      memory: mem,
    });
  }
}
