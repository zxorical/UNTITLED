/**
 * @module autoJoin/manager
 * 
 * Premium AutoJoiner - PRODUCTION GRADE
 * 
 * Optimizations:
 * 1. Direct processing (no batching latency)
 * 2. Critical data written instantly
 * 3. Worker-based architecture for scaling
 * 4. RAM cache for processed messages (no DB hit per message)
 * 5. O(1) session lookups with Map
 * 6. Optimized regex patterns (fewer, more efficient)
 * 7. Async logger queue (non-blocking)
 * 8. Circuit breakers for Discord API
 * 9. Token encryption at rest, decrypted only when needed
 * 10. SessionUserIdMap for O(1) lookups
 * 11. Connection pooling with HTTP/HTTPS agents
 * 12. Proper ClientOptions for discord.js-selfbot-v13
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
  token?: string;
  label: string;
  startedAt: number;
  isActive: boolean;
  stats: SessionStats;
  heartbeatInterval?: NodeJS.Timeout;
  reconnectAttempts: number;
  maxReconnectAttempts: number;
  rateLimiter: TokenBucket;
}

interface SessionStats {
  detected: number;
  entered: number;
  failed: number;
  wins: number;
  lastEntryAt?: number;
}

// ---------------------------------------------------------------------------
// Constants - Optimized regex
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
// Constants - Optimized values
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

const CACHE_PROCESSED_MESSAGES = 1000;
const CACHE_MAX_PROCESSING = 200;
const CACHE_MAX_WINS = 50;
const CACHE_MAX_COOLDOWN = 20;
const CACHE_MAX_TOKEN = 10;

const CIRCUIT_BREAKER_THRESHOLD = 10;
const CIRCUIT_BREAKER_TIMEOUT_MS = 30000;
const CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS = 3;

// ---------------------------------------------------------------------------
// LRU Cache Implementation
// ---------------------------------------------------------------------------

class LRUCache<K, V> {
  private cache: Map<K, { value: V; timestamp: number }>;
  private readonly maxSize: number;
  private readonly ttl: number;

  constructor(maxSize: number, ttlMs: number = 0) {
    this.cache = new Map();
    this.maxSize = maxSize;
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
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) this.cache.delete(firstKey);
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
}

// ---------------------------------------------------------------------------
// Async Logger Queue - Non-blocking
// ---------------------------------------------------------------------------

class AsyncLogger {
  private queue: Array<{ level: string; msg: string; meta?: Record<string, unknown> }> = [];
  private processing = false;
  private interval: NodeJS.Timeout | null = null;

  constructor() {
    this.interval = setInterval(() => this.flush(), 1000);
    if (this.interval.unref) this.interval.unref();
  }

  info(msg: string, meta?: Record<string, unknown>): void {
    this.queue.push({ level: 'info', msg, meta });
    if (this.queue.length > 100) this.flush();
  }

  warn(msg: string, meta?: Record<string, unknown>): void {
    this.queue.push({ level: 'warn', msg, meta });
  }

  error(msg: string, meta?: Record<string, unknown>): void {
    this.queue.push({ level: 'error', msg, meta });
  }

  debug(msg: string, meta?: Record<string, unknown>): void {
    this.queue.push({ level: 'debug', msg, meta });
  }

  private async flush(): Promise<void> {
    if (this.processing || this.queue.length === 0) return;
    this.processing = true;

    const batch = this.queue.splice(0, 50);
    for (const item of batch) {
      try {
        switch (item.level) {
          case 'info': logger.info(item.msg, item.meta); break;
          case 'warn': logger.warn(item.msg, item.meta); break;
          case 'error': logger.error(item.msg, item.meta); break;
          case 'debug': logger.debug(item.msg, item.meta); break;
        }
      } catch {
        // Silently fail - don't block on logging
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
}

// ---------------------------------------------------------------------------
// Circuit Breaker
// ---------------------------------------------------------------------------

class CircuitBreaker {
  private failures = 0;
  private state: 'closed' | 'open' | 'half-open' = 'closed';
  private openUntil = 0;
  private halfOpenAttempts = 0;

  constructor(
    private readonly threshold = CIRCUIT_BREAKER_THRESHOLD,
    private readonly timeoutMs = CIRCUIT_BREAKER_TIMEOUT_MS,
    private readonly halfOpenMaxAttempts = CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS,
  ) {}

  async execute<T>(fn: () => Promise<T>): Promise<T> {
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
  }

  getState(): string {
    return this.state;
  }
}

// ---------------------------------------------------------------------------
// Token Manager - Decrypt on demand only
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
// AutoJoinManager - Production Grade
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
  
  private isShuttingDown = false;
  private sessionStartPromises: Map<string, Promise<boolean>> = new Map();
  private workerId: string;

  private readonly http = axios.create({
    timeout: 10_000,
    // Use httpAgent/httpsAgent for connection pooling
    httpAgent: new http.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets: 10,
      maxFreeSockets: 5,
      scheduling: 'lifo',
    }),
    httpsAgent: new https.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets: 10,
      maxFreeSockets: 5,
      scheduling: 'lifo',
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
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async startAllSessions(): Promise<void> {
    this.asyncLogger.info(`Starting AutoJoin sessions (worker: ${this.workerId})...`);

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const validUsers = allPremiumUsers.filter(u => u.token);
      
      const usersToStart = validUsers.slice(0, MAX_SESSIONS_PER_WORKER);
      
      if (validUsers.length > MAX_SESSIONS_PER_WORKER) {
        this.asyncLogger.warn(`Too many premium users (${validUsers.length}), limiting to ${MAX_SESSIONS_PER_WORKER}`, {
          worker: this.workerId,
        });
      }

      let started = 0;
      let failed = 0;
      for (const user of usersToStart) {
        const success = await this.startSession(user.userId, user.guildId);
        if (success) started++;
        else failed++;
        await delay(2000);
      }

      this.asyncLogger.info(`AutoJoin sessions started: ${started} active (${failed} failed)`, {
        worker: this.workerId,
        sessions: this.sessions.size,
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
    const sessionKey = this.makeSessionKey(userId);
    
    if (this.sessions.has(sessionKey)) return true;
    if (this.sessions.size >= MAX_SESSIONS_PER_WORKER) {
      this.asyncLogger.warn(`Session limit reached (${MAX_SESSIONS_PER_WORKER})`, { userId });
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

      // Proper ClientOptions for discord.js-selfbot-v13
      const clientOptions: ClientOptions = {
        // Message caching - minimal to save memory
        messageCacheLifetime: 60,
        messageSweepInterval: 300,
        // Disable unused features
        restRequestTimeout: 15000,
        restGlobalRateLimit: 50,
        retryLimit: 3,
        // Disable voice and presence for self-bot
        allowedMentions: { parse: [] },
        partials: [],
        intents: [
          1 << 0, // GUILDS
          1 << 1, // GUILD_MEMBERS
          1 << 9, // GUILD_MESSAGES
          1 << 10, // GUILD_MESSAGE_REACTIONS
          1 << 12, // GUILD_MESSAGE_TYPING
          1 << 13, // DIRECT_MESSAGES
          1 << 14, // DIRECT_MESSAGE_REACTIONS
          1 << 15, // DIRECT_MESSAGE_TYPING
        ],
        // Disable cache for things we don't need
        makeCache: (manager: any) => {
          // Only cache messages, and only 10 per channel
          if (manager.name === 'MessageManager') {
            return manager.collection;
          }
          // Disable cache for other managers
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
    this.asyncLogger.info('Restoring AutoJoin sessions from database...', { worker: this.workerId });
    
    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      let restored = 0, failed = 0, skipped = 0;
      
      for (const user of allPremiumUsers) {
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
      if (session.heartbeatInterval) {
        clearInterval(session.heartbeatInterval);
        session.heartbeatInterval = undefined;
      }
      await Promise.race([(session.client as any).destroy(), delay(5000)]);
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(userId);
      this.tokenManager.clearCache(userId, guildId);
      await setTokenActive(userId, guildId, false);
      this.emit('sessionStopped', { userId, guildId });
    } catch (error) {
      this.asyncLogger.error('Failed to stop AutoJoin session', { userId, guildId, error: formatError(error) });
      this.sessions.delete(sessionKey);
      this.sessionsByUserId.delete(userId);
    }
  }

  async refreshSessions(): Promise<void> {
    if (this.isShuttingDown) return;

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const activeUserIds = new Set(allPremiumUsers.filter(u => u.token).map(u => u.userId));

      for (const [key, session] of this.sessions) {
        if (!activeUserIds.has(session.userId)) {
          await this.stopSession(session.userId, session.guildId);
        }
      }

      for (const user of allPremiumUsers) {
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
    
    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const failedUsers = allPremiumUsers.filter(u => u.token && u.tokenActive === false);
      
      for (const user of failedUsers) {
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
    };
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;

    [this.refreshInterval, this.cleanupInterval, this.memoryCheckInterval, this.reconnectCheckInterval]
      .forEach(interval => {
        if (interval) { clearInterval(interval); }
      });

    const stopPromises: Promise<void>[] = [];
    for (const [key, session] of this.sessions) {
      stopPromises.push(this.stopSession(session.userId, session.guildId));
    }
    await Promise.all(stopPromises);

    this.sessions.clear();
    this.sessionsByUserId.clear();
    this.processedMessages.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    this.noResponseCooldown.clear();
    this.tokenManager.clearAll();
    
    this.asyncLogger.shutdown();
    
    this.asyncLogger.info('AutoJoin shutdown complete', { worker: this.workerId });
  }

  // -------------------------------------------------------------------------
  // Event Handlers
  // -------------------------------------------------------------------------

  private registerEvents(session: UserSession): void {
    const { client, userId, guildId } = session;

    client.on('messageCreate', async (message: Message) => {
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
    });

    client.on('messageUpdate', async (_old: any, updated: any) => {
      if (this.isShuttingDown || !session.isActive) return;
      try {
        await this.handleMessage(updated as Message, session);
      } catch (error) {
        this.asyncLogger.error('Message update handler error', { userId, error: formatError(error) });
      }
    });

    client.on('error', (error) => {
      this.asyncLogger.error('Client error', { userId, error: formatError(error) });
    });

    client.on('disconnect', () => {
      this.asyncLogger.warn('Client disconnected, will attempt reconnect', { userId });
    });
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

    if (this.processedMessages.has(entryId)) return;
    if (this.processingCache.get(entryId) !== undefined) return;

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
  // Giveaway Detection
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

    if (isKnownBot) {
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
          
          if (PATTERNS.DRAFT_BUTTON.test(label)) continue;

          if (customId.includes('giveaway') || customId.includes('enter') || customId.includes('join')) {
            return { customId, label: label || customId, disabled: false };
          }

          if (PATTERNS.ENTRY_BUTTON.test(label)) {
            return { customId, label: label || 'Enter', disabled: false };
          }
        }
      }
      return null;
    }

    let hasDraftButton = false;
    for (const row of components) {
      const rowAny = row as Record<string, unknown>;
      const rowComps = rowAny['components'] as unknown[] | undefined;
      if (!rowComps) continue;
      for (const comp of rowComps) {
        const c = comp as Record<string, unknown>;
        if (c['type'] !== 2 && c['type'] !== 'BUTTON') continue;
        const label = ((c['label'] as string | undefined) ?? '').toLowerCase();
        if (PATTERNS.DRAFT_BUTTON.test(label)) {
          hasDraftButton = true;
          break;
        }
      }
      if (hasDraftButton) break;
    }

    if (hasDraftButton) {
      let hasEntry = false;
      for (const row of components) {
        const rowAny = row as Record<string, unknown>;
        const rowComps = rowAny['components'] as unknown[] | undefined;
        if (!rowComps) continue;
        for (const comp of rowComps) {
          const c = comp as Record<string, unknown>;
          if (c['type'] !== 2 && c['type'] !== 'BUTTON') continue;
          if (c['style'] === 5 || c['disabled'] === true) continue;
          
          const customId = (c['customId'] ?? c['custom_id']) as string | undefined;
          const label = ((c['label'] as string | undefined) ?? '').trim();
          
          if (customId && TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) {
            hasEntry = true;
            break;
          }
          if (PATTERNS.ENTRY_BUTTON.test(label)) {
            hasEntry = true;
            break;
          }
        }
        if (hasEntry) break;
      }
      if (!hasEntry) return null;
    }

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

        if (PATTERNS.DRAFT_BUTTON.test(label)) continue;

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

  // -------------------------------------------------------------------------
  // Entry Execution
  // -------------------------------------------------------------------------

  private async enterGiveaway(entryId: string, session: UserSession): Promise<void> {
    const parts = entryId.split(':');
    const userId = parts[0];
    const channelId = parts[1];
    const messageId = parts.slice(2).join(':');
    
    const entry = await getAutoJoinEntry(session.userId, messageId, channelId);
    if (!entry) return;

    await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'attempting', {});

    const maxAttempts = CONFIG.maxRetries + 1;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const attemptNum = attempt + 1;
      
      if (attempt > 0) {
        const backoffMs = exponentialBackoff(attempt - 1, CONFIG.retryDelayMs, 30000);
        await delay(backoffMs);
      }

      if (attempt === 2) {
        try {
          const refreshedEntry = await this.refreshButtonData(entry, session);
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
        const skipped = await this.enterViaButton(entry, session);
        if (skipped) {
          await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'skipped', {});
          return;
        }

        session.stats.entered++;
        session.stats.lastEntryAt = Date.now();

        await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'success', { attempts: attemptNum });
        await incrementTokenEntries(session.userId, session.guildId);
        await updateTokenLastUsed(session.userId, session.guildId);

        this.asyncLogger.info('✅ AutoJoin: Entered giveaway', {
          userId: session.userId,
          prize: truncate(entry.prize, 60),
          attempts: attemptNum,
          guild: entry.guildName,
          worker: this.workerId,
        });

        this.emit('giveawayEntered', { entry, userId: session.userId });
        return;

      } catch (error) {
        const errorMsg = formatError(error);
        const isNoResponse = errorMsg.includes('No responsed from Application') || 
                            errorMsg.includes('No response from Application');

        if (isNoResponse && attempt < maxAttempts - 1) {
          this.noResponseCooldown.set(session.userId, Date.now() + NO_RESPONSE_COOLDOWN_MS);
          await delay(2000);
          try { await this.refreshButtonData(entry, session); } catch {}
          continue;
        }

        await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'attempting', { 
          attempts: attemptNum, 
          lastError: errorMsg 
        });
        
        this.asyncLogger.warn(`AutoJoin: Attempt ${attemptNum}/${maxAttempts} failed`, {
          userId: session.userId,
          entryId,
          error: errorMsg,
          worker: this.workerId,
        });
      }
    }

    await updateAutoJoinEntryStatus(session.userId, entry.messageId, entry.channelId, 'failed', {});
    session.stats.failed++;

    this.asyncLogger.error('❌ AutoJoin: All retries exhausted', {
      userId: session.userId,
      prize: truncate(entry.prize, 60),
      attempts: entry.attempts,
      worker: this.workerId,
    });

    this.emit('giveawayFailed', { entry, userId: session.userId });
  }

  private async refreshButtonData(entry: GiveawayEntry, session: UserSession): Promise<GiveawayEntry | null> {
    try {
      const message = await this.fetchMessage(session.client, entry.channelId, entry.messageId);
      if (!message) return null;
      
      const components = (message as any).components;
      if (!components?.length) return null;
      
      const isKnownBot = this.isKnownGiveawayBot(message);
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
        const isKnownBot = this.isKnownGiveawayBot(message);
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

  // -------------------------------------------------------------------------
  // Interaction Helpers
  // -------------------------------------------------------------------------

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
      const clientAny = message.client as unknown as Record<string, unknown>;
      
      let sessionId = clientAny['sessionId'] as string | undefined;
      if (!sessionId) {
        const client = message.client as any;
        sessionId = client.sessionId || client.session_id || Date.now().toString();
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

          if (response.status === 204 || response.status === 200 || response.status === 201) {
            return;
          }

        } catch (error) {
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
    if (session) session.stats.wins++;

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
    if (session) session.stats.wins++;

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
  // Interval Starters
  // -------------------------------------------------------------------------

  private startSessionRefresher(): void {
    this.refreshInterval = setInterval(() => {
      this.refreshSessions().catch((error) => {
        this.asyncLogger.error('Session refresh failed', { error: formatError(error) });
      });
    }, SESSION_REFRESH_INTERVAL_MS);
    if (this.refreshInterval.unref) this.refreshInterval.unref();
  }

  private startCleanupInterval(): void {
    this.cleanupInterval = setInterval(async () => {
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
      const memUsage = process.memoryUsage();
      const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
      
      if (heapUsedMB > 300) {
        this.processedMessages.clear();
        this.processingCache.clear();
        this.recentWins.clear();
        this.noResponseCooldown.clear();
        if (global.gc) global.gc();
      }
    }, 60_000);
    if (this.memoryCheckInterval.unref) this.memoryCheckInterval.unref();
  }

  private startReconnectChecker(): void {
    this.reconnectCheckInterval = setInterval(() => {
      this.retryFailedSessions().catch((error) => {
        this.asyncLogger.error('Reconnect check failed', { error: formatError(error) });
      });
    }, RECONNECT_DELAY_MS);
    if (this.reconnectCheckInterval.unref) this.reconnectCheckInterval.unref();
  }

  private logStats(): void {
    const stats = this.getStats();
    this.asyncLogger.info('AutoJoin Stats', {
      worker: this.workerId,
      totalSessions: stats.totalSessions,
      activeSessions: stats.activeSessions,
      cacheSize: this.processingCache.size,
      processedCacheSize: this.processedMessages.size,
      winCacheSize: this.recentWins.size,
      circuitBreakerState: this.apiCircuitBreaker.getState(),
    });
  }
}
