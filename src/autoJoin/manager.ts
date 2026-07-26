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
 * 4. Monitors giveaway messages and auto-clicks entry buttons
 * 5. Detects wins and sends webhook notifications (user's personal webhook or global fallback)
 */

import { Client, Message, TextChannel } from 'discord.js-selfbot-v13';
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
  formatDuration,
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
} from '../database.js';
import { decryptToken } from '../premium/tokenManager.js';
import { CONFIG } from '../config.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GiveawayEntry {
  entryId: string;
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
  status: EntryStatus;
  attempts: number;
  userId: string;
  lastAttemptAt?: number;
  lastError?: string;
}

enum EntryStatus {
  PENDING = 'pending',
  ATTEMPTING = 'attempting',
  SUCCESS = 'success',
  FAILED = 'failed',
  SKIPPED = 'skipped',
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
  entries: Map<string, GiveawayEntry>;
  processing: Set<string>;
  stats: SessionStats;
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

const MAX_CONCURRENT = 2;
const ENTRY_TTL_MS = 2 * 60 * 60 * 1000;
const WIN_DEDUP_TTL_MS = 30 * 60 * 1000;
const COMPONENT_RETRY_DELAY_MS = 300;
const COMPONENT_RETRY_ATTEMPTS = 3;
const SESSION_REFRESH_INTERVAL_MS = 60_000;

const KNOWN_GIVEAWAY_BOT_IDS: ReadonlySet<string> = new Set([
  '294882584201003009', // GiveawayBot
  '739448630517039104', // GiveawayBoat
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
// AutoJoinManager
// ---------------------------------------------------------------------------

export class AutoJoinManager extends EventEmitter {
  private sessions: Map<string, UserSession> = new Map();
  private recentWins: Map<string, number> = new Map();
  private refreshInterval: NodeJS.Timeout | null = null;
  private isShuttingDown = false;

  private readonly http = axios.create({
    timeout: 10_000,
  });

  constructor(private guildId: string) {
    super();
    this.startSessionRefresher();
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Start auto-join sessions for all premium users with tokens
   */
  async startAllSessions(): Promise<void> {
    logger.info('Starting AutoJoin sessions...', { component: 'AutoJoin' });

    try {
      const premiumUsers = await getAllPremiumUsers(this.guildId);
      
      for (const user of premiumUsers) {
        if (!user.token) continue;
        if (user.tokenActive === false) continue;
        
        await this.startSession(user.userId, user.guildId);
      }

      logger.info(`AutoJoin sessions started: ${this.sessions.size} active`, {
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

  /**
   * Start a single user session
   */
  async startSession(userId: string, guildId: string): Promise<boolean> {
    const sessionKey = this.makeSessionKey(userId, guildId);
    
    // Check if already running
    if (this.sessions.has(sessionKey)) {
      logger.debug('Session already running', { component: 'AutoJoin', userId, guildId });
      return true;
    }

    try {
      // Get user from DB
      const user = await getPremiumUser(userId, guildId);
      if (!user?.token) {
        logger.debug('No token found for user', { component: 'AutoJoin', userId });
        return false;
      }

      // Decrypt token
      let decryptedToken: string;
      try {
        decryptedToken = decryptToken(user.token);
      } catch (error) {
        logger.error('Failed to decrypt token', {
          component: 'AutoJoin',
          userId,
          error: formatError(error),
        });
        await setTokenActive(userId, guildId, false);
        return false;
      }

      // Validate token by trying to login
      const client = new Client();
      
      // Set up event handlers before login
      const session: UserSession = {
        client,
        userId,
        guildId,
        token: decryptedToken,
        label: user.tokenLabel || 'main',
        startedAt: Date.now(),
        isActive: true,
        entries: new Map(),
        processing: new Set(),
        stats: {
          detected: 0,
          entered: 0,
          failed: 0,
          wins: 0,
        },
      };

      // Register event handlers
      this.registerEvents(session);

      // Login
      await client.login(decryptedToken);
      
      // Wait for ready
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Login timeout')), 10000);
        client.once('ready', () => {
          clearTimeout(timeout);
          resolve();
        });
        client.once('error', (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });

      // Store session
      this.sessions.set(sessionKey, session);
      
      // Update DB
      await setTokenActive(userId, guildId, true);
      await updateTokenLastUsed(userId, guildId);

      logger.info('AutoJoin session started', {
        component: 'AutoJoin',
        userId,
        label: session.label,
        username: client.user?.username,
      });

      this.emit('sessionStarted', { userId, guildId });
      return true;

    } catch (error) {
      logger.error('Failed to start AutoJoin session', {
        component: 'AutoJoin',
        userId,
        guildId,
        error: formatError(error),
      });
      
      await setTokenActive(userId, guildId, false);
      return false;
    }
  }

  /**
   * Stop a single user session
   */
  async stopSession(userId: string, guildId: string): Promise<void> {
    const sessionKey = this.makeSessionKey(userId, guildId);
    const session = this.sessions.get(sessionKey);
    
    if (!session) return;

    try {
      session.isActive = false;
      session.client.destroy();
      this.sessions.delete(sessionKey);
      await setTokenActive(userId, guildId, false);

      logger.info('AutoJoin session stopped', {
        component: 'AutoJoin',
        userId,
        guildId,
      });

      this.emit('sessionStopped', { userId, guildId });
    } catch (error) {
      logger.error('Failed to stop AutoJoin session', {
        component: 'AutoJoin',
        userId,
        guildId,
        error: formatError(error),
      });
    }
  }

  /**
   * Refresh all sessions (reload from DB)
   */
  async refreshSessions(): Promise<void> {
    if (this.isShuttingDown) return;

    logger.debug('Refreshing AutoJoin sessions...', { component: 'AutoJoin' });

    try {
      const premiumUsers = await getAllPremiumUsers(this.guildId);
      const activeUserIds = new Set(premiumUsers.filter(u => u.token && u.tokenActive !== false).map(u => u.userId));

      // Stop sessions for users no longer premium or without token
      for (const [key, session] of this.sessions) {
        if (!activeUserIds.has(session.userId)) {
          await this.stopSession(session.userId, session.guildId);
        }
      }

      // Start sessions for new premium users
      for (const user of premiumUsers) {
        if (!user.token) continue;
        if (user.tokenActive === false) continue;
        
        const sessionKey = this.makeSessionKey(user.userId, user.guildId);
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

  /**
   * Get session stats
   */
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

  /**
   * Shutdown all sessions
   */
  async shutdown(): Promise<void> {
    this.isShuttingDown = true;

    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }

    logger.info('Shutting down AutoJoin sessions...', { component: 'AutoJoin' });

    const stopPromises: Promise<void>[] = [];
    for (const [key, session] of this.sessions) {
      stopPromises.push(this.stopSession(session.userId, session.guildId));
    }

    await Promise.all(stopPromises);
    this.sessions.clear();
    
    logger.info('AutoJoin shutdown complete', { component: 'AutoJoin' });
  }

  // -------------------------------------------------------------------------
  // Event Handlers
  // -------------------------------------------------------------------------

  private registerEvents(session: UserSession): void {
    const { client, userId, guildId } = session;

    client.on('messageCreate', async (message: Message) => {
      if (this.isShuttingDown) return;
      if (!session.isActive) return;

      try {
        // Handle DM wins
        if (!message.guild) {
          await this.handleDmWin(message, userId);
          return;
        }

        // Only process messages in the target guild
        if (message.guild.id !== guildId) return;
        if (message.author?.id === client.user?.id) return;

        // Check for wins first
        await this.handleWin(message, userId);

        // Then check for entry opportunities (BUTTON ONLY)
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
      if (this.isShuttingDown) return;
      if (!session.isActive) return;
      
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
    // Skip if not in monitored channels
    if (
      CONFIG.monitoredChannels.length > 0 &&
      !CONFIG.monitoredChannels.includes(message.channel.id)
    ) {
      return;
    }

    const entryId = this.makeEntryId(message);

    // Dedup checks
    if (session.entries.has(entryId)) return;
    if (session.processing.has(entryId)) return;

    session.processing.add(entryId);

    try {
      const detected = await this.detectGiveaway(message);
      if (!detected) {
        session.processing.delete(entryId);
        return;
      }

      const entry: GiveawayEntry = {
        entryId,
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
        status: EntryStatus.PENDING,
        attempts: 0,
        userId: session.userId,
      };

      session.entries.set(entryId, entry);
      session.stats.detected++;

      logger.debug('AutoJoin: Giveaway detected', {
        component: 'AutoJoin',
        userId: session.userId,
        prize: truncate(entry.prize, 60),
        guild: entry.guildName,
        channel: `#${entry.channelName}`,
      });

      // Enter the giveaway (BUTTON ONLY)
      await this.enterGiveaway(entry, session);

    } catch (error) {
      logger.error('AutoJoin: Handle message error', {
        component: 'AutoJoin',
        userId: session.userId,
        error: formatError(error),
      });
    } finally {
      session.processing.delete(entryId);
      this.pruneEntries(session);
    }
  }

  // -------------------------------------------------------------------------
  // Giveaway Detection (BUTTON ONLY)
  // -------------------------------------------------------------------------

  private async detectGiveaway(
    message: Message,
  ): Promise<{ prize: string; button: GiveawayButton } | null> {
    const rawContent = message.content ?? '';
    if (BLOCKED_MESSAGE_CONTENT.some(re => re.test(rawContent))) {
      return null;
    }

    const isKnownBot = this.isKnownGiveawayBot(message);
    const hasKeyword = this.messageHasKeyword(message);
    const hasSignal = isKnownBot || hasKeyword;

    if (!hasSignal) return null;

    // Try immediate extraction
    const immediate = this.tryExtractEntry(message, isKnownBot);
    if (immediate) return immediate;

    // Retry with delay (components may load late)
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

  private tryExtractEntry(
    message: Message,
    isKnownBot: boolean,
  ): { prize: string; button: GiveawayButton } | null {
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
        if (c['style'] === 5) continue; // link button - skip
        if (c['disabled'] === true) continue;

        const customId = (c['customId'] ?? c['custom_id']) as string | undefined;
        if (!customId) continue;

        const label = ((c['label'] as string | undefined) ?? '').trim();

        // Block leave/exit buttons
        if (BLOCKED_BUTTON_LABELS.some(re => re.test(label))) {
          continue;
        }

        // Check trusted custom IDs
        if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) {
          return { customId, label: label || customId, disabled: false };
        }

        // Check label patterns
        if (ENTRY_BUTTON_PATTERNS.some(re => re.test(label))) {
          return { customId, label: label || 'Enter', disabled: false };
        }
      }
    }

    return null;
  }

  // -------------------------------------------------------------------------
  // Entry Execution (BUTTON ONLY)
  // -------------------------------------------------------------------------

  private async enterGiveaway(entry: GiveawayEntry, session: UserSession): Promise<void> {
    const { entryId, userId } = entry;
    entry.status = EntryStatus.ATTEMPTING;

    const maxAttempts = CONFIG.maxRetries + 1;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      entry.attempts = attempt + 1;
      entry.lastAttemptAt = Date.now();

      if (attempt > 0) {
        const backoffMs = exponentialBackoff(attempt - 1, CONFIG.retryDelayMs, 30000);
        await delay(backoffMs);
      }

      try {
        const skipped = await this.enterViaButton(entry, session);
        if (skipped) {
          entry.status = EntryStatus.SKIPPED;
          return;
        }

        entry.status = EntryStatus.SUCCESS;
        session.stats.entered++;
        session.stats.lastEntryAt = Date.now();

        // Update database
        await incrementTokenEntries(userId, session.guildId);
        await updateTokenLastUsed(userId, session.guildId);

        logger.info('✅ AutoJoin: Entered giveaway', {
          component: 'AutoJoin',
          userId,
          prize: truncate(entry.prize, 60),
          attempts: entry.attempts,
          guild: entry.guildName,
        });

        this.emit('giveawayEntered', { entry, userId });
        return;

      } catch (error) {
        entry.lastError = formatError(error);
        logger.warn(`AutoJoin: Attempt ${attempt + 1}/${maxAttempts} failed`, {
          component: 'AutoJoin',
          userId,
          entryId,
          error: entry.lastError,
        });
      }
    }

    entry.status = EntryStatus.FAILED;
    session.stats.failed++;

    logger.error('❌ AutoJoin: All retries exhausted', {
      component: 'AutoJoin',
      userId,
      prize: truncate(entry.prize, 60),
      attempts: entry.attempts,
      lastError: entry.lastError,
    });

    this.emit('giveawayFailed', { entry, userId });
  }

  private async enterViaButton(entry: GiveawayEntry, session: UserSession): Promise<boolean> {
    if (!entry.buttonCustomId) throw new Error('No buttonCustomId set');

    if (CONFIG.buttonDelayMs > 0) await delay(CONFIG.buttonDelayMs);

    const message = await this.fetchMessage(session.client, entry.channelId, entry.messageId);
    if (!message) throw new Error(`Message ${entry.messageId} not found`);

    const button = this.findButtonById(message, entry.buttonCustomId);
    if (!button) {
      entry.status = EntryStatus.SKIPPED;
      return true;
    }

    if (button.disabled) {
      entry.status = EntryStatus.SKIPPED;
      return true;
    }

    await this.clickButton(message, button);
    return false;
  }

  // -------------------------------------------------------------------------
  // Interaction Helpers
  // -------------------------------------------------------------------------

  private async clickButton(message: Message, button: GiveawayButton): Promise<void> {
    const selfbotMsg = message as Message & { clickButton?: (id: string) => Promise<unknown> };
    if (typeof selfbotMsg.clickButton === 'function') {
      await selfbotMsg.clickButton(button.customId);
      return;
    }

    // Fallback: direct API call
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

  // -------------------------------------------------------------------------
  // Win Detection
  // -------------------------------------------------------------------------

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

    // Dedup
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

    await incrementTokenWins(userId, this.guildId);

    const prize = this.extractPrize(message);
    const sourceName = `#${(message.channel as { name?: string }).name ?? message.channel.id} in ${message.guild.name}`;

    logger.info('🏆 AutoJoin: WIN DETECTED!', {
      component: 'AutoJoin',
      userId,
      prize,
      source: sourceName,
    });

    // Send webhook notification (uses user's personal webhook or global fallback)
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

    await incrementTokenWins(userId, this.guildId);

    const prize = this.extractPrize(message);

    logger.info('🏆 AutoJoin: WIN DETECTED (DM)!', {
      component: 'AutoJoin',
      userId,
      prize,
    });

    await this.sendWinWebhook(message, prize, 'Direct Message', userId);
    this.emit('giveawayWon', { message, prize, userId, source: 'dm' });
  }

  // -------------------------------------------------------------------------
  // Webhooks - Priority: User Personal > WIN_WEBHOOK_URL > WEBHOOK_URL
  // -------------------------------------------------------------------------

  private async sendWinWebhook(
    message: Message,
    prize: string,
    sourceName: string,
    userId: string,
  ): Promise<void> {
    // PRIORITY 1: Get the user's personal webhook
    let url: string | null = null;
    try {
      url = await getUserWebhook(userId, this.guildId);
      if (url) {
        logger.debug('Using user\'s personal webhook', {
          component: 'AutoJoin',
          userId,
        });
      }
    } catch (error) {
      logger.debug('Failed to get user webhook', {
        component: 'AutoJoin',
        userId,
        error: formatError(error),
      });
    }

    // PRIORITY 2: Fallback to global WIN_WEBHOOK_URL
    if (!url) {
      url = CONFIG.winWebhookUrl || null;
      if (url) {
        logger.debug('Using global WIN_WEBHOOK_URL', {
          component: 'AutoJoin',
          userId,
        });
      }
    }

    // PRIORITY 3: Fallback to global WEBHOOK_URL
    if (!url) {
      url = CONFIG.webhookUrl || null;
      if (url) {
        logger.debug('Using global WEBHOOK_URL', {
          component: 'AutoJoin',
          userId,
        });
      }
    }

    // If no webhook at all, log and return
    if (!url) {
      logger.debug('No webhook configured for win notification', {
        component: 'AutoJoin',
        userId,
      });
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
        component: 'AutoJoin',
        userId,
        prize: truncate(prize, 50),
        webhookType: url === CONFIG.winWebhookUrl || url === CONFIG.webhookUrl ? 'global' : 'personal',
      });
    } catch (error) {
      logger.warn('Win webhook failed', {
        component: 'AutoJoin',
        userId,
        error: formatError(error),
      });
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

  private makeEntryId(message: Message): string {
    return `${message.channel.id}:${message.id}`;
  }

  private makeSessionKey(userId: string, guildId: string): string {
    return `${userId}:${guildId}`;
  }

  private findSessionByUserId(userId: string): UserSession | null {
    for (const [_, session] of this.sessions) {
      if (session.userId === userId) return session;
    }
    return null;
  }

  private pruneEntries(session: UserSession): void {
    const cutoff = Date.now() - ENTRY_TTL_MS;
    for (const [id, entry] of session.entries) {
      if (
        entry.detectedAt < cutoff &&
        (entry.status === EntryStatus.SUCCESS ||
         entry.status === EntryStatus.FAILED ||
         entry.status === EntryStatus.SKIPPED)
      ) {
        session.entries.delete(id);
      }
    }
  }

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

  private logStats(): void {
    const stats = this.getStats();
    logger.info('AutoJoin Stats', {
      component: 'AutoJoin',
      totalSessions: stats.totalSessions,
      activeSessions: stats.activeSessions,
      sessions: Array.from(stats.sessionStats.entries()).map(([key, s]) => ({
        userId: key.split(':')[0],
        detected: s.detected,
        entered: s.entered,
        wins: s.wins,
        failed: s.failed,
      })),
    });
  }
}
