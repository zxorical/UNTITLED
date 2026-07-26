/**
 * @module autoJoin/manager
 * 
 * Premium AutoJoiner - automatically enters giveaways using user-provided tokens.
 * BUTTON ONLY - no reaction support.
 * 
 * Flow:
 * 1. Users add their Discord token via Premium Panel → encrypted + stored in DB
 * 2. AutoJoiner reads all premium users with valid tokens
 * 3. Starts a self-bot session for each user
 * 4. Monitors giveaway messages in ALL servers the token has access to
 * 5. Auto-clicks entry buttons on detected giveaways
 * 6. Detects wins and sends webhook notifications
 */

import { Client, Message, TextChannel, Options } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import axios from 'axios';
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
  getAllPremiumUsers,
  incrementTokenEntries,
  incrementTokenWins,
  updateTokenLastUsed,
  getPremiumUser,
  setTokenActive,
  getUserWebhook,
  getAllPremiumUsersAllGuilds,
} from '../database.js';
import { decryptToken } from '../premium/tokenManager.js';
import { CONFIG } from '../config.js';
import { Db, Collection, MongoClient } from 'mongodb';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface StoredGiveawayEntry {
  _id: string; // entryId
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
  expiresAt: number; // For TTL index
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
  token: string;
  label: string;
  startedAt: number;
  isActive: boolean;
  stats: SessionStats;
  heartbeatInterval?: NodeJS.Timeout;
  cleanupInterval?: NodeJS.Timeout;
  lastGuildFetch?: number;
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

const MAX_CONCURRENT = 1;
const ENTRY_TTL_MS = 2 * 60 * 60 * 1000; // 2 hours
const WIN_DEDUP_TTL_MS = 30 * 60 * 1000;
const COMPONENT_RETRY_DELAY_MS = 300;
const COMPONENT_RETRY_ATTEMPTS = 3;
const SESSION_REFRESH_INTERVAL_MS = 120_000;
const HEARTBEAT_INTERVAL_MS = 60_000;
const MAX_SESSIONS = 5;
const PROCESSING_CACHE_TTL_MS = 60000; // 1 minute for processing set

// ... (rest of constants remain the same)

const KNOWN_GIVEAWAY_BOT_IDS: ReadonlySet<string> = new Set([
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
]);

const BLOCKED_BUTTON_LABELS: ReadonlyArray<RegExp> = [
  /\bleave\b/i,
  /\bquit\b/i,
  /\bexit\b/i,
  /\bunenter\b/i,
  /\bwithdraw\b/i,
  /remove\s+entry/i,
  /cancel\s+entry/i,
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

const BLOCKED_MESSAGE_CONTENT: ReadonlyArray<RegExp> = [
  /already\s+entered\s+this\s+giveaway/i,
  /you(?:'ve|\s+have)\s+already\s+entered/i,
  /you\s+are\s+already\s+(?:in|entered|participating)/i,
  /you(?:'ve|\s+have)\s+already\s+(?:joined|joined\s+this)/i,
  /leave\s+giveaway/i,
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

// ---------------------------------------------------------------------------
// MongoDB Collection for AutoJoin entries
// ---------------------------------------------------------------------------

let entriesCollection: Collection<StoredGiveawayEntry> | null = null;

async function getEntriesCollection(): Promise<Collection<StoredGiveawayEntry>> {
  if (entriesCollection) return entriesCollection;
  
  const MONGO_URI = process.env.MONGO_URI;
  if (!MONGO_URI) {
    throw new Error('MONGO_URI environment variable is required');
  }
  
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db('giveaway_tracker');
  entriesCollection = db.collection<StoredGiveawayEntry>('autojoin_entries');
  
  // Create TTL index to automatically delete expired entries
  await entriesCollection.createIndex(
    { expiresAt: 1 },
    { expireAfterSeconds: 0 }
  );
  
  // Create compound index for lookups
  await entriesCollection.createIndex(
    { userId: 1, messageId: 1, channelId: 1 },
    { unique: true }
  );
  
  // Create index for cleaning up old entries
  await entriesCollection.createIndex({ detectedAt: -1 });
  
  return entriesCollection;
}

// ---------------------------------------------------------------------------
// AutoJoinManager
// ---------------------------------------------------------------------------

export class AutoJoinManager extends EventEmitter {
  private sessions: Map<string, UserSession> = new Map();
  private recentWins: Map<string, number> = new Map();
  private processingCache: Map<string, number> = new Map(); // entryId -> timestamp
  private refreshInterval: NodeJS.Timeout | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;
  private memoryCheckInterval: NodeJS.Timeout | null = null;
  private isShuttingDown = false;
  private sessionStartPromises: Map<string, Promise<boolean>> = new Map();

  private readonly http = axios.create({
    timeout: 10_000,
  });

  constructor() {
    super();
    this.startSessionRefresher();
    this.startCleanupInterval();
    this.startMemoryCheck();
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Start auto-join sessions for all premium users with tokens
   */
  async startAllSessions(): Promise<void> {
    logger.info('Starting AutoJoin sessions for all premium users...', { component: 'AutoJoin' });

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      
      const validUsers = allPremiumUsers.filter(u => u.token && u.tokenActive !== false);
      
      const usersToStart = validUsers.slice(0, MAX_SESSIONS);
      
      if (validUsers.length > MAX_SESSIONS) {
        logger.warn(`Too many premium users (${validUsers.length}), limiting to ${MAX_SESSIONS}`, {
          component: 'AutoJoin',
        });
      }

      let started = 0;
      for (const user of usersToStart) {
        const success = await this.startSession(user.userId, user.guildId);
        if (success) started++;
        await delay(2000);
      }

      logger.info(`AutoJoin sessions started: ${started} active`, {
        component: 'AutoJoin',
        sessions: this.sessions.size,
      });
    } catch (error) {
      logger.error('Failed to start AutoJoin sessions', {
        component: 'AutoJoin',
        error: formatError(error),
      });
    }
  }

  private async getAllPremiumUsersAcrossAllGuilds(): Promise<any[]> {
    try {
      return await getAllPremiumUsersAllGuilds();
    } catch (error) {
      logger.error('Failed to get premium users', { error: formatError(error) });
      return [];
    }
  }

  async startSession(userId: string, guildId: string): Promise<boolean> {
    const sessionKey = this.makeSessionKey(userId);
    
    if (this.sessions.has(sessionKey)) {
      return true;
    }

    if (this.sessions.size >= MAX_SESSIONS) {
      logger.warn(`Session limit reached (${MAX_SESSIONS})`, { component: 'AutoJoin', userId });
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
        return false;
      }

      let decryptedToken: string;
      try {
        decryptedToken = decryptToken(user.token);
      } catch (error) {
        logger.error('Failed to decrypt token', { userId, error: formatError(error) });
        await setTokenActive(userId, guildId, false);
        return false;
      }

      // Create client with minimal caching
      const client = new Client({
        restGlobalTimeout: 30000,
        retryLimit: 3,
        // Disable message caching to save memory
        messageCacheLifetime: 60,
        messageSweepInterval: 120,
        // Disable guild caching for large guilds
        makeCache: Options.cacheWithLimits({
          MessageManager: { maxSize: 0 },
          GuildMemberManager: { maxSize: 0 },
          ChannelManager: { maxSize: 0 },
          GuildChannelManager: { maxSize: 0 },
          PresenceManager: { maxSize: 0 },
          UserManager: { maxSize: 0 },
          ReactionManager: { maxSize: 0 },
          ReactionUserManager: { maxSize: 0 },
        }),
      });
      
      const session: UserSession = {
        client,
        userId,
        guildId,
        token: decryptedToken,
        label: user.tokenLabel || 'main',
        startedAt: Date.now(),
        isActive: true,
        stats: {
          detected: 0,
          entered: 0,
          failed: 0,
          wins: 0,
        },
      };

      this.registerEvents(session);

      await this.loginWithTimeout(client, decryptedToken);
      await this.waitForReady(client);

      this.sessions.set(sessionKey, session);
      this.startHeartbeat(session);
      
      await setTokenActive(userId, guildId, true);
      await updateTokenLastUsed(userId, guildId);

      logger.info('AutoJoin session started', {
        userId,
        label: session.label,
        username: client.user?.username,
        guilds: client.guilds.cache.size,
      });

      this.emit('sessionStarted', { userId, guildId });
      return true;

    } catch (error) {
      logger.error('Failed to start AutoJoin session', {
        userId,
        guildId,
        error: formatError(error),
      });
      
      await setTokenActive(userId, guildId, false);
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
      client.once('ready', () => { clearTimeout(timeout); resolve(); });
      client.once('error', (err) => { clearTimeout(timeout); reject(err); });
    });
  }

  private startHeartbeat(session: UserSession): void {
    if (session.heartbeatInterval) {
      clearInterval(session.heartbeatInterval);
    }

    session.heartbeatInterval = setInterval(() => {
      if (this.isShuttingDown || !session.isActive) return;

      if (!session.client.isReady()) {
        logger.warn('Session client not ready, attempting reconnect', {
          component: 'AutoJoin',
          userId: session.userId,
        });
        session.client.destroy();
        this._startSessionInternal(session.userId, session.guildId)
          .catch(err => logger.error('Reconnect failed', { error: formatError(err) }));
      }
    }, HEARTBEAT_INTERVAL_MS);

    if (session.heartbeatInterval.unref) {
      session.heartbeatInterval.unref();
    }
  }

  /**
   * Restore sessions from database (for VPS restarts)
   */
  async restoreSessionsFromDatabase(): Promise<void> {
    logger.info('Restoring AutoJoin sessions from database...', { component: 'AutoJoin' });
    
    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      let restored = 0;
      
      for (const user of allPremiumUsers) {
        if (!user.token) continue;
        if (user.tokenActive === false) continue;
        
        const sessionKey = this.makeSessionKey(user.userId);
        if (this.sessions.has(sessionKey)) continue;
        
        const success = await this.startSession(user.userId, user.guildId);
        if (success) restored++;
        await delay(500);
      }
      
      logger.info(`Restored ${restored} AutoJoin sessions`, {
        component: 'AutoJoin',
        total: this.sessions.size,
      });
    } catch (error) {
      logger.error('Failed to restore AutoJoin sessions', {
        component: 'AutoJoin',
        error: formatError(error),
      });
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
      
      await Promise.race([
        session.client.destroy(),
        delay(5000),
      ]);
      
      this.sessions.delete(sessionKey);
      await setTokenActive(userId, guildId, false);

      logger.info('AutoJoin session stopped', { userId, guildId });
      this.emit('sessionStopped', { userId, guildId });
    } catch (error) {
      logger.error('Failed to stop AutoJoin session', {
        userId,
        guildId,
        error: formatError(error),
      });
      this.sessions.delete(sessionKey);
    }
  }

  async refreshSessions(): Promise<void> {
    if (this.isShuttingDown) return;

    try {
      const allPremiumUsers = await this.getAllPremiumUsersAcrossAllGuilds();
      const activeUserIds = new Set(
        allPremiumUsers
          .filter(u => u.token && u.tokenActive !== false)
          .map(u => u.userId)
      );

      for (const [key, session] of this.sessions) {
        if (!activeUserIds.has(session.userId)) {
          await this.stopSession(session.userId, session.guildId);
        }
      }

      for (const user of allPremiumUsers) {
        if (!user.token) continue;
        if (user.tokenActive === false) continue;
        
        const sessionKey = this.makeSessionKey(user.userId);
        if (!this.sessions.has(sessionKey)) {
          await this.startSession(user.userId, user.guildId);
        }
      }

      this.logStats();
    } catch (error) {
      logger.error('Failed to refresh sessions', {
        component: 'AutoJoin',
        error: formatError(error),
      });
    }
  }

  getStats(): {
    totalSessions: number;
    activeSessions: number;
    sessionStats: Map<string, SessionStats>;
  } {
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
    };
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;

    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    if (this.memoryCheckInterval) {
      clearInterval(this.memoryCheckInterval);
      this.memoryCheckInterval = null;
    }

    logger.info('Shutting down AutoJoin sessions...', { component: 'AutoJoin' });

    const stopPromises: Promise<void>[] = [];
    for (const [key, session] of this.sessions) {
      stopPromises.push(this.stopSession(session.userId, session.guildId));
    }

    await Promise.all(stopPromises);
    this.sessions.clear();
    this.processingCache.clear();
    this.recentWins.clear();
    
    logger.info('AutoJoin shutdown complete', { component: 'AutoJoin' });
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
        logger.error('Message handler error', {
          component: 'AutoJoin',
          userId,
          error: formatError(error),
        });
      }
    });

    client.on('messageUpdate', async (_old: any, updated: any) => {
      if (this.isShuttingDown || !session.isActive) return;
      
      try {
        await this.handleMessage(updated as Message, session);
      } catch (error) {
        logger.error('Message update handler error', {
          component: 'AutoJoin',
          userId,
          error: formatError(error),
        });
      }
    });

    client.on('error', (error) => {
      logger.error('Client error', {
        component: 'AutoJoin',
        userId,
        error: formatError(error),
      });
    });

    client.on('disconnect', () => {
      logger.warn('Client disconnected, will attempt reconnect', {
        component: 'AutoJoin',
        userId,
      });
    });
  }

  // -------------------------------------------------------------------------
  // Message Handling (Entry Detection - BUTTON ONLY)
  // -------------------------------------------------------------------------

  private async handleMessage(message: Message, session: UserSession): Promise<void> {
    if (
      CONFIG.monitoredChannels.length > 0 &&
      !CONFIG.monitoredChannels.includes(message.channel.id)
    ) {
      return;
    }

    const entryId = this.makeEntryId(message);

    // Check processing cache (in-memory, short-lived)
    if (this.processingCache.has(entryId)) {
      const timestamp = this.processingCache.get(entryId)!;
      if (Date.now() - timestamp < PROCESSING_CACHE_TTL_MS) {
        return;
      }
      this.processingCache.delete(entryId);
    }

    // Check MongoDB for existing entry
    const collection = await getEntriesCollection();
    const existing = await collection.findOne({ 
      _id: entryId,
      userId: session.userId,
      status: { $in: ['pending', 'attempting'] }
    });
    
    if (existing) {
      // Still being processed
      this.processingCache.set(entryId, Date.now());
      return;
    }

    // Check if entry was already processed (success, failed, skipped)
    const completed = await collection.findOne({
      _id: entryId,
      userId: session.userId,
      status: { $in: ['success', 'failed', 'skipped'] }
    });
    
    if (completed) {
      // Already processed, don't process again
      return;
    }

    this.processingCache.set(entryId, Date.now());

    try {
      const detected = await this.detectGiveaway(message);
      if (!detected) {
        this.processingCache.delete(entryId);
        return;
      }

      const entry: Omit<StoredGiveawayEntry, '_id'> = {
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

      // Store in MongoDB
      await collection.insertOne({
        ...entry,
        _id: entryId,
      });

      session.stats.detected++;

      logger.debug('AutoJoin: Giveaway detected', {
        component: 'AutoJoin',
        userId: session.userId,
        prize: truncate(entry.prize, 60),
        guild: entry.guildName,
        channel: `#${entry.channelName}`,
      });

      // Enter the giveaway
      await this.enterGiveaway(entryId, session);

    } catch (error) {
      logger.error('AutoJoin: Handle message error', {
        component: 'AutoJoin',
        userId: session.userId,
        error: formatError(error),
      });
    } finally {
      this.processingCache.delete(entryId);
      this.cleanupOldEntries(session.userId);
    }
  }

  // -------------------------------------------------------------------------
  // Entry Execution (BUTTON ONLY)
  // -------------------------------------------------------------------------

  private async enterGiveaway(entryId: string, session: UserSession): Promise<void> {
    const collection = await getEntriesCollection();
    const entry = await collection.findOne({ _id: entryId, userId: session.userId });
    
    if (!entry) return;

    await collection.updateOne(
      { _id: entryId },
      { $set: { status: 'attempting' } }
    );

    const maxAttempts = CONFIG.maxRetries + 1;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const attemptNum = attempt + 1;
      
      if (attempt > 0) {
        const backoffMs = exponentialBackoff(attempt - 1, CONFIG.retryDelayMs, 30000);
        await delay(backoffMs);
      }

      try {
        const skipped = await this.enterViaButton(entry, session);
        if (skipped) {
          await collection.updateOne(
            { _id: entryId },
            { $set: { status: 'skipped' } }
          );
          return;
        }

        session.stats.entered++;
        session.stats.lastEntryAt = Date.now();

        await collection.updateOne(
          { _id: entryId },
          { $set: { status: 'success', attempts: attemptNum } }
        );

        await incrementTokenEntries(session.userId, session.guildId);
        await updateTokenLastUsed(session.userId, session.guildId);

        logger.info('✅ AutoJoin: Entered giveaway', {
          component: 'AutoJoin',
          userId: session.userId,
          prize: truncate(entry.prize, 60),
          attempts: attemptNum,
          guild: entry.guildName,
        });

        this.emit('giveawayEntered', { entry, userId: session.userId });
        return;

      } catch (error) {
        const lastError = formatError(error);
        await collection.updateOne(
          { _id: entryId },
          { $set: { attempts: attemptNum, lastError } }
        );
        
        logger.warn(`AutoJoin: Attempt ${attemptNum}/${maxAttempts} failed`, {
          component: 'AutoJoin',
          userId: session.userId,
          entryId,
          error: lastError,
        });
      }
    }

    // All attempts failed
    await collection.updateOne(
      { _id: entryId },
      { $set: { status: 'failed' } }
    );
    
    session.stats.failed++;

    logger.error('❌ AutoJoin: All retries exhausted', {
      component: 'AutoJoin',
      userId: session.userId,
      prize: truncate(entry.prize, 60),
      attempts: entry.attempts,
      lastError: entry.lastError,
    });

    this.emit('giveawayFailed', { entry, userId: session.userId });
  }

  private async enterViaButton(entry: StoredGiveawayEntry, session: UserSession): Promise<boolean> {
    if (!entry.buttonCustomId) throw new Error('No buttonCustomId set');

    if (CONFIG.buttonDelayMs > 0) await delay(CONFIG.buttonDelayMs);

    const message = await this.fetchMessage(session.client, entry.channelId, entry.messageId);
    if (!message) throw new Error(`Message ${entry.messageId} not found`);

    const button = this.findButtonById(message, entry.buttonCustomId);
    if (!button || button.disabled) {
      return true; // Skip
    }

    await this.clickButton(message, button);
    return false;
  }

  // -------------------------------------------------------------------------
  // Rest of the helper methods (keep existing implementations)
  // -------------------------------------------------------------------------

  private async cleanupOldEntries(userId: string): Promise<void> {
    try {
      const collection = await getEntriesCollection();
      const cutoff = Date.now() - ENTRY_TTL_MS;
      
      // Delete old entries that are completed
      await collection.deleteMany({
        userId,
        status: { $in: ['success', 'failed', 'skipped'] },
        detectedAt: { $lt: cutoff }
      });
    } catch (error) {
      // Ignore cleanup errors
    }
  }

  private makeEntryId(message: Message): string {
    return `${message.channel.id}:${message.id}`;
  }

  private makeSessionKey(userId: string): string {
    return userId;
  }

  private findSessionByUserId(userId: string): UserSession | null {
    for (const [_, session] of this.sessions) {
      if (session.userId === userId) return session;
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Interval Starters
  // -------------------------------------------------------------------------

  private startSessionRefresher(): void {
    this.refreshInterval = setInterval(() => {
      this.refreshSessions().catch((error) => {
        logger.error('Session refresh failed', {
          component: 'AutoJoin',
          error: formatError(error),
        });
      });
    }, SESSION_REFRESH_INTERVAL_MS);

    if (this.refreshInterval.unref) {
      this.refreshInterval.unref();
    }
  }

  private startCleanupInterval(): void {
    this.cleanupInterval = setInterval(async () => {
      try {
        // Clean up expired MongoDB entries
        const collection = await getEntriesCollection();
        await collection.deleteMany({
          expiresAt: { $lt: Date.now() }
        });
      } catch (error) {
        logger.debug('Cleanup error', { error: formatError(error) });
      }
    }, 5 * 60_000); // Every 5 minutes

    if (this.cleanupInterval.unref) {
      this.cleanupInterval.unref();
    }
  }

  private startMemoryCheck(): void {
    this.memoryCheckInterval = setInterval(() => {
      const memUsage = process.memoryUsage();
      const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
      const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
      
      // Log memory usage
      logger.debug('AutoJoin Memory Usage', {
        component: 'AutoJoin',
        heapUsed: `${heapUsedMB}MB`,
        heapTotal: `${heapTotalMB}MB`,
        sessions: this.sessions.size,
        processingCache: this.processingCache.size,
        recentWins: this.recentWins.size,
      });

      // If memory is too high, force cleanup
      if (heapUsedMB > 300) {
        logger.warn('High memory usage detected, forcing cleanup', {
          component: 'AutoJoin',
          heapUsed: `${heapUsedMB}MB`,
        });
        
        this.processingCache.clear();
        this.recentWins.clear();
        
        // Force garbage collection if available
        if (global.gc) {
          global.gc();
        }
      }
    }, 60_000); // Every minute

    if (this.memoryCheckInterval.unref) {
      this.memoryCheckInterval.unref();
    }
  }

  private logStats(): void {
    const stats = this.getStats();
    logger.info('AutoJoin Stats', {
      component: 'AutoJoin',
      totalSessions: stats.totalSessions,
      activeSessions: stats.activeSessions,
      sessions: Array.from(stats.sessionStats.entries()).map(([key, s]) => ({
        userId: key,
        detected: s.detected,
        entered: s.entered,
        wins: s.wins,
        failed: s.failed,
      })),
    });
  }

  // -------------------------------------------------------------------------
  // Existing helper methods (keep these as-is)
  // -------------------------------------------------------------------------

  private async detectGiveaway(message: Message): Promise<{ prize: string; button: GiveawayButton } | null> {
    const rawContent = message.content ?? '';
    if (BLOCKED_MESSAGE_CONTENT.some(re => re.test(rawContent))) {
      return null;
    }

    const isKnownBot = this.isKnownGiveawayBot(message);
    const hasKeyword = this.messageHasKeyword(message);
    const hasSignal = isKnownBot || hasKeyword;

    if (!hasSignal) return null;

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

  private extractEntryButton(message: Message, _isKnownBot: boolean): GiveawayButton | null {
    const msgAny = message as unknown as Record<string, unknown>;
    const components = msgAny['components'] as unknown[] | undefined;
    if (!components?.length) return null;

    for (const row of components) {
      const rowAny = row as Record<string, unknown>;
      const rowComps = rowAny['components'] as unknown[] | undefined;
      if (!rowComps) continue;

      for (const comp of rowComps) {
        const c = comp as Record<string, unknown>;

        const type = c['type'];
        if (type !== 2 && type !== 'BUTTON') continue;
        if (c['style'] === 5) continue;
        if (c['disabled'] === true) continue;

        const customId = (c['customId'] ?? c['custom_id']) as string | undefined;
        if (!customId) continue;

        const label = ((c['label'] as string | undefined) ?? '').trim();

        if (BLOCKED_BUTTON_LABELS.some(re => re.test(label))) {
          continue;
        }

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

  private async clickButton(message: Message, button: GiveawayButton): Promise<void> {
    const selfbotMsg = message as Message & { clickButton?: (id: string) => Promise<unknown> };
    if (typeof selfbotMsg.clickButton === 'function') {
      await selfbotMsg.clickButton(button.customId);
      return;
    }

    await this.postInteraction(message, button);
  }

  private async postInteraction(message: Message, button: GiveawayButton): Promise<void> {
    const clientAny = message.client as unknown as Record<string, unknown>;
    const sessionId = (clientAny['sessionId'] ?? clientAny['session_id'] ?? Date.now().toString()) as string;
    const nonce = `${Date.now()}${Math.random().toString(36).slice(2, 8)}`;

    const payload = {
      type: 3,
      nonce,
      guild_id: message.guild?.id ?? null,
      channel_id: message.channel.id,
      message_id: message.id,
      application_id: message.author?.id,
      session_id: sessionId,
      data: { component_type: 2, custom_id: button.customId },
    };

    const token = (message.client as any).token as string;

    try {
      await axios.post('https://discord.com/api/v10/interactions', payload, {
        headers: {
          'Authorization': token,
          'Content-Type': 'application/json',
        },
        timeout: 10000,
      });
    } catch (error) {
      const axiosErr = error as { response?: { status?: number; data?: { retry_after?: number } } };
      const status = axiosErr.response?.status;

      if (status === 429) {
        const retryAfterMs = Math.ceil((axiosErr.response?.data?.retry_after ?? 1) * 1000);
        await delay(retryAfterMs);
        await axios.post('https://discord.com/api/v10/interactions', payload, {
          headers: {
            'Authorization': token,
            'Content-Type': 'application/json',
          },
          timeout: 10000,
        });
        return;
      }

      throw error;
    }
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
    return texts.some(t => hasGiveawayKeyword(t));
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
    const re = /<t:(\d{10,13})(?::[a-zA-Z])?>/;
    const allText = this.extractAllText(message);
    const match = allText.match(re);
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

  private async handleWin(message: Message, userId: string): Promise<void> {
    if (!message.guild) return;
    if (!message.author?.bot) return;

    const myId = message.client.user?.id;
    if (!myId) return;

    const mentionedInUsers = message.mentions?.users?.has(myId) ?? false;
    const mentionedInContent = (message.content ?? '').includes(myId);
    if (!mentionedInUsers && !mentionedInContent) return;

    const allText = this.extractAllText(message);
    if (!WIN_PATTERNS.some(re => re.test(allText))) return;

    const dedupKey = `${message.channel.id}:${message.author?.id ?? 'unknown'}`;
    const lastWin = this.recentWins.get(dedupKey);
    if (lastWin && Date.now() - lastWin < WIN_DEDUP_TTL_MS) {
      return;
    }
    this.recentWins.set(dedupKey, Date.now());

    const session = this.findSessionByUserId(userId);
    if (session) {
      session.stats.wins++;
    }

    await incrementTokenWins(userId, session?.guildId || '');

    const prize = this.extractPrize(message);
    const sourceName = `#${(message.channel as { name?: string }).name ?? message.channel.id} in ${message.guild.name}`;

    logger.info('🏆 AutoJoin: WIN DETECTED!', {
      component: 'AutoJoin',
      userId,
      prize,
      source: sourceName,
      guild: message.guild.name,
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
    }

    await incrementTokenWins(userId, session?.guildId || '');

    const prize = this.extractPrize(message);

    logger.info('🏆 AutoJoin: WIN DETECTED (DM)!', {
      component: 'AutoJoin',
      userId,
      prize,
    });

    await this.sendWinWebhook(message, prize, 'Direct Message', userId);
    this.emit('giveawayWon', { message, prize, userId, source: 'dm' });
  }

  private async sendWinWebhook(message: Message, prize: string, sourceName: string, userId: string): Promise<void> {
    const session = this.findSessionByUserId(userId);
    const guildId = session?.guildId || '';

    let url: string | null = null;
    try {
      url = await getUserWebhook(userId, guildId);
    } catch (error) {
      logger.debug('Failed to get user webhook', { userId, error: formatError(error) });
    }

    if (!url) {
      url = CONFIG.winWebhookUrl || null;
    }
    if (!url) {
      url = CONFIG.webhookUrl || null;
    }

    if (!url) {
      logger.debug('No webhook configured for win notification', { userId });
      return;
    }

    const guildName = message.guild?.name ?? 'Direct Message';
    const jumpUrl = message.guild
      ? `https://discord.com/channels/${message.guild.id}/${message.channel.id}/${message.id}`
      : null;

    try {
      await axios.post(url, {
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
          footer: { 
            text: `AutoJoin • ${url === CONFIG.winWebhookUrl || url === CONFIG.webhookUrl ? 'Global' : 'Personal'} Webhook`,
          },
          timestamp: new Date().toISOString(),
        }],
      }, { timeout: 8000 });

      logger.info('Win webhook sent successfully', {
        userId,
        prize: truncate(prize, 50),
        guild: guildName,
      });
    } catch (error) {
      logger.warn('Win webhook failed', { userId, error: formatError(error) });
    }
  }
}
