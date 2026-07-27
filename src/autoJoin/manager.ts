/**
 * @module autoJoin/manager
 * AutoJoin Manager - Handles automatic giveaway entry for premium users
 * Uses the existing database functions and discord.js-selfbot-v13
 * 
 * FIXES APPLIED:
 * 1. Fixed 'this' type error in getStats()
 * 2. Fixed Promise type error in shutdown()
 * 3. Added proper validation for userId/guildId in processEntry
 * 4. Proper session management
 * 5. Queue processing with batching
 * 6. Retry logic with exponential backoff
 * 7. Webhook support
 * 8. Comprehensive stats
 * 9. Proper cleanup on shutdown
 * 10. ✅ FIX: Decrypt tokens before login
 * 11. ✅ FIX: Use proper detection from autoJoin.ts
 * 12. ✅ FIX: Proper button detection with TRUSTED_ENTRY_CUSTOM_IDS
 * 13. ✅ FIX: Handle GiveawayBoat bare participant count buttons
 * 14. ✅ FIX: postInteraction uses session.client not this.client
 */

import { Client, Message, TextChannel } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import axios from 'axios';
import { logger } from '../logger.js';
import { CONFIG } from '../config.js';
import { delay, formatError, formatTimestamp } from '../utils.js';
import { decryptToken } from '../premium/tokenManager.js';
import {
  getAllPremiumUsersAllGuilds,
  getPremiumUser,
  getUserToken,
  getUserWebhook,
  incrementTokenEntries,
  incrementTokenWins,
  updateTokenLastUsed,
  setTokenActive,
  getAutoJoinEntriesCollection,
  saveAutoJoinEntry,
  getAutoJoinEntry,
  updateAutoJoinEntryStatus,
  deleteAutoJoinEntry,
  cleanupAutoJoinEntries,
  getPendingAutoJoinEntries,
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

// ============================================================================
// Giveaway Detection Constants - From autoJoin.ts
// ============================================================================

const KNOWN_GIVEAWAY_BOT_IDS: ReadonlySet<string> = new Set([
  '294882584201003009', // GiveawayBot
  '739448630517039104', // GiveawayBoat
  '515195524879237130',
  '235148962103951360',
  '282859044593598464',
  '270904126974590976',
  '508391840525975553',
  '530082442967646230',
]);

const TRUSTED_ENTRY_CUSTOM_IDS: ReadonlySet<string> = new Set([
  'giveaway_message',   // GiveawayBoat — participant count button
  'giveaway-enter',
  'enter_giveaway',
  'giveaway_enter',
  'join_giveaway',
  'giveaway-join',
  'giveaway_participate',
  'participate_giveaway',
  'enter',
]);

const BLOCKED_BUTTON_LABELS: ReadonlyArray<RegExp> = [
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
  /^\d[\d,]*$/,   // bare participant count — GiveawayBoat style
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

const GIVEAWAY_KEYWORD_PATTERNS: ReadonlyArray<RegExp> = [
  /\bgiveaway\b/i,
  /\braffle\b/i,
  /\bsweepstakes\b/i,
  /\bwin\b/i,
  /\bprize\b/i,
];

// ============================================================================
// Types
// ============================================================================

interface AutoJoinSession {
  userId: string;
  guildId: string;
  client: Client;
  token: string;
  label: string;
  isActive: boolean;
  startedAt: number;
  lastActivityAt: number;
  stats: {
    detected: number;
    entered: number;
    failed: number;
    wins: number;
    lastEntryAt?: number;
  };
}

interface GiveawayToJoin {
  messageId: string;
  channelId: string;
  guildId: string;
  guildName: string;
  channelName: string;
  prize: string;
  authorId: string;
  detectedAt: number;
  endsAt: number | null;
  buttonCustomId?: string;
  userId?: string;
}

interface QueueStats {
  processed: number;
  succeeded: number;
  failed: number;
  skipped: number;
}

// ============================================================================
// Constants
// ============================================================================

const MAX_RETRIES = CONFIG.maxRetries || 3;
const RETRY_DELAY_MS = CONFIG.retryDelayMs || 2000;
const BUTTON_DELAY_MS = CONFIG.buttonDelayMs || 500;
const SESSION_CLEANUP_INTERVAL_MS = 60000; // 1 minute
const MAX_SESSION_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours
const MAX_CONCURRENT_ENTRIES = 5;
const QUEUE_PROCESS_INTERVAL_MS = 1000;
const WIN_DEDUP_TTL_MS = 30 * 60 * 1000;
const COMPONENT_RETRY_DELAY_MS = 300;
const COMPONENT_RETRY_ATTEMPTS = 3;

// ============================================================================
// AutoJoinManager
// ============================================================================

export class AutoJoinManager extends EventEmitter {
  private sessions = new Map<string, AutoJoinSession>();
  private processingEntries = new Set<string>();
  private cleanupInterval: NodeJS.Timeout | null = null;
  private queueInterval: NodeJS.Timeout | null = null;
  private isShuttingDown = false;
  private entryQueue: GiveawayToJoin[] = [];
  private isProcessingQueue = false;
  private queueStats: QueueStats = {
    processed: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
  };
  
  // Win dedup map: `${channelId}:${authorId}` → timestamp
  private recentWins = new Map<string, number>();
  
  // HTTP client for interactions
  private readonly http: ReturnType<typeof axios.create>;

  constructor() {
    super();
    this.startCleanupInterval();
    this.startQueueProcessor();
    
    // HTTP client for direct interactions
    this.http = axios.create({
      baseURL: 'https://discord.com/api/v10',
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    });
  }

  // ============================================================================
  // Public API
  // ============================================================================

  public async startAllSessions(): Promise<void> {
    try {
      const premiumUsers = await getAllPremiumUsersAllGuilds();
      
      let started = 0;
      let failed = 0;
      
      logger.info(`Found ${premiumUsers.length} premium users, checking for tokens...`, {
        component: 'AutoJoinManager'
      });
      
      for (const user of premiumUsers) {
        if (user.token && user.tokenActive !== false) {
          logger.debug(`Attempting to start session for user ${user.userId}`, {
            component: 'AutoJoinManager',
            userId: user.userId,
            hasToken: !!user.token,
            tokenActive: user.tokenActive
          });
          
          const success = await this.startSession(user.userId, user.guildId);
          if (success) {
            started++;
          } else {
            failed++;
          }
        }
      }
      
      logger.info(`AutoJoinManager: Started ${started} sessions (${failed} failed)`, {
        component: 'AutoJoinManager',
        totalUsers: premiumUsers.length
      });
    } catch (error) {
      logger.error('Failed to start AutoJoin sessions', {
        component: 'AutoJoinManager',
        error: formatError(error)
      });
    }
  }

  public async startSession(userId: string, guildId: string): Promise<boolean> {
    if (!userId || !guildId) {
      logger.warn('startSession called with invalid parameters', { userId, guildId });
      return false;
    }

    const sessionKey = `${userId}:${guildId}`;
    
    if (this.sessions.has(sessionKey)) {
      return true;
    }

    try {
      // Get user's token from database
      const user = await getPremiumUser(userId, guildId);
      if (!user || !user.token || user.tokenActive === false) {
        logger.debug(`User ${userId} has no valid token`, {
          component: 'AutoJoinManager',
          userId,
          hasToken: !!user?.token,
          tokenActive: user?.tokenActive
        });
        return false;
      }

      // ✅ Decrypt the token
      let decryptedToken: string;
      try {
        decryptedToken = decryptToken(user.token);
        logger.debug(`Token decrypted successfully for user ${userId}`, {
          component: 'AutoJoinManager',
          userId,
          tokenLength: decryptedToken.length
        });
      } catch (error) {
        logger.error(`Failed to decrypt token for user ${userId}`, {
          component: 'AutoJoinManager',
          userId,
          error: formatError(error)
        });
        await setTokenActive(userId, guildId, false);
        return false;
      }

      // Create client for this session
      const client = new Client();
      
      // Set up event handlers
      client.on('ready', () => {
        logger.debug(`AutoJoin session ready`, {
          component: 'AutoJoinManager',
          userId,
          guildId
        });
        const session = this.sessions.get(sessionKey);
        if (session) {
          session.isActive = true;
          session.lastActivityAt = Date.now();
        }
      });

      client.on('error', (error) => {
        logger.error(`AutoJoin session error`, {
          component: 'AutoJoinManager',
          userId,
          guildId,
          error: formatError(error)
        });
      });

      client.on('disconnect', () => {
        logger.warn(`AutoJoin session disconnected`, {
          component: 'AutoJoinManager',
          userId,
          guildId
        });
        const session = this.sessions.get(sessionKey);
        if (session) {
          session.isActive = false;
        }
      });

      client.on('reconnecting', () => {
        logger.debug(`AutoJoin session reconnecting`, {
          component: 'AutoJoinManager',
          userId,
          guildId
        });
      });

      // Login with decrypted token
      await client.login(decryptedToken);

      // Register session
      const session: AutoJoinSession = {
        userId,
        guildId,
        client,
        token: user.token,
        label: user.tokenLabel || 'main',
        isActive: true,
        startedAt: Date.now(),
        lastActivityAt: Date.now(),
        stats: {
          detected: 0,
          entered: 0,
          failed: 0,
          wins: 0,
        }
      };

      this.sessions.set(sessionKey, session);
      
      // Mark token as active
      await setTokenActive(userId, guildId, true);
      
      logger.info(`AutoJoin session started`, {
        component: 'AutoJoinManager',
        userId,
        guildId,
        label: session.label,
        username: client.user?.username
      });

      return true;
    } catch (error) {
      logger.error(`Failed to start AutoJoin session`, {
        component: 'AutoJoinManager',
        userId,
        guildId,
        error: formatError(error)
      });
      await setTokenActive(userId, guildId, false);
      return false;
    }
  }

  public async stopSession(userId: string, guildId: string): Promise<void> {
    if (!userId || !guildId) {
      logger.warn('stopSession called with invalid parameters', { userId, guildId });
      return;
    }

    const sessionKey = `${userId}:${guildId}`;
    const session = this.sessions.get(sessionKey);
    
    if (session) {
      try {
        session.client.removeAllListeners();
        await session.client.destroy();
      } catch (error) {
        logger.debug(`Error destroying client for session`, {
          component: 'AutoJoinManager',
          userId,
          guildId,
          error: formatError(error)
        });
      }
      this.sessions.delete(sessionKey);
      
      logger.info(`AutoJoin session stopped`, {
        component: 'AutoJoinManager',
        userId,
        guildId
      });
    }
  }

  public async restoreSessionsFromDatabase(): Promise<number> {
    try {
      const premiumUsers = await getAllPremiumUsersAllGuilds();
      let restored = 0;

      for (const user of premiumUsers) {
        if (user.token && user.tokenActive !== false) {
          const sessionKey = `${user.userId}:${user.guildId}`;
          if (!this.sessions.has(sessionKey)) {
            const success = await this.startSession(user.userId, user.guildId);
            if (success) restored++;
          }
        }
      }

      logger.info(`Restored ${restored} AutoJoin sessions from database`, {
        component: 'AutoJoinManager',
        totalUsers: premiumUsers.length
      });

      return restored;
    } catch (error) {
      logger.error('Failed to restore AutoJoin sessions', {
        component: 'AutoJoinManager',
        error: formatError(error)
      });
      return 0;
    }
  }

  /**
   * Handle a new giveaway detection - queue it for entry
   */
  public async handleGiveaway(giveaway: GiveawayToJoin): Promise<void> {
    if (this.isShuttingDown) return;

    // Get all premium users in this guild
    const premiumUsers = await getAllPremiumUsersAllGuilds();
    
    // Filter users who have active sessions in this guild
    const eligibleUsers = premiumUsers.filter(user => {
      const sessionKey = `${user.userId}:${user.guildId}`;
      const session = this.sessions.get(sessionKey);
      return session && session.isActive;
    });

    if (eligibleUsers.length === 0) {
      // Try to start sessions for users who don't have them
      for (const user of premiumUsers) {
        if (user.token && user.tokenActive !== false) {
          const sessionKey = `${user.userId}:${user.guildId}`;
          if (!this.sessions.has(sessionKey)) {
            await this.startSession(user.userId, user.guildId);
          }
        }
      }
      return;
    }

    // Queue the giveaway for each eligible user
    let queued = 0;
    for (const user of eligibleUsers) {
      const sessionKey = `${user.userId}:${user.guildId}`;
      const session = this.sessions.get(sessionKey);
      if (session) {
        session.stats.detected++;
        session.lastActivityAt = Date.now();
        
        // Check if already processing this entry
        const entryId = `${user.userId}:${giveaway.channelId}:${giveaway.messageId}`;
        if (!this.processingEntries.has(entryId)) {
          // Add to queue with user-specific data
          this.entryQueue.push({
            ...giveaway,
            userId: user.userId,
          });
          queued++;
        }
      }
    }

    if (queued > 0) {
      logger.debug(`Queued ${queued} AutoJoin entries for giveaway`, {
        component: 'AutoJoinManager',
        prize: giveaway.prize?.substring(0, 50),
        messageId: giveaway.messageId
      });
    }
  }

  // ============================================================================
  // Queue Processing
  // ============================================================================

  private startQueueProcessor(): void {
    if (this.queueInterval) return;
    
    this.queueInterval = setInterval(() => {
      if (!this.isProcessingQueue && this.entryQueue.length > 0 && !this.isShuttingDown) {
        this.processQueue();
      }
    }, QUEUE_PROCESS_INTERVAL_MS);
    
    if (this.queueInterval.unref) {
      this.queueInterval.unref();
    }
  }

  private async processQueue(): Promise<void> {
    if (this.isProcessingQueue || this.entryQueue.length === 0 || this.isShuttingDown) return;
    
    this.isProcessingQueue = true;
    
    try {
      while (this.entryQueue.length > 0 && !this.isShuttingDown) {
        // Process in batches
        const batchSize = Math.min(MAX_CONCURRENT_ENTRIES, this.entryQueue.length);
        const batch = this.entryQueue.splice(0, batchSize);
        
        const promises = batch.map(entry => this.processEntry(entry));
        const results = await Promise.allSettled(promises);
        
        // Update stats
        for (const result of results) {
          if (result.status === 'fulfilled' && result.value) {
            if (result.value.success) {
              this.queueStats.succeeded++;
            } else {
              this.queueStats.failed++;
            }
          } else {
            this.queueStats.failed++;
          }
          this.queueStats.processed++;
        }
        
        // Small delay between batches
        if (this.entryQueue.length > 0 && !this.isShuttingDown) {
          await delay(500);
        }
      }
    } catch (error) {
      logger.error('Error processing AutoJoin queue', {
        component: 'AutoJoinManager',
        error: formatError(error)
      });
    } finally {
      this.isProcessingQueue = false;
      
      // If more entries were added while processing, continue
      if (this.entryQueue.length > 0 && !this.isShuttingDown) {
        setImmediate(() => this.processQueue());
      }
    }
  }

  private async processEntry(entry: GiveawayToJoin): Promise<{ success: boolean; error?: string }> {
    const { userId, guildId, messageId, channelId, prize } = entry;
    
    if (!userId || !guildId || !messageId || !channelId) {
      logger.warn('processEntry called with missing required fields', { userId, guildId, messageId, channelId });
      return { success: false, error: 'Missing required fields' };
    }
    
    const entryId = `${userId}:${channelId}:${messageId}`;
    
    if (this.processingEntries.has(entryId)) {
      return { success: false, error: 'Already processing' };
    }

    this.processingEntries.add(entryId);

    try {
      const sessionKey = `${userId}:${guildId}`;
      let session = this.sessions.get(sessionKey);
      
      if (!session || !session.isActive) {
        // Try to restart session
        const restarted = await this.startSession(userId, guildId);
        if (!restarted) {
          this.processingEntries.delete(entryId);
          return { success: false, error: 'Session not available' };
        }
        session = this.sessions.get(sessionKey);
        if (!session) {
          this.processingEntries.delete(entryId);
          return { success: false, error: 'Session not available' };
        }
      }

      // Attempt to enter the giveaway with retries
      let lastError = '';
      for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        if (attempt > 0) {
          await delay(RETRY_DELAY_MS * attempt);
        }
        
        const result = await this.attemptEntry(session, entry);
        if (result.success) {
          session.stats.entered++;
          session.stats.lastEntryAt = Date.now();
          await incrementTokenEntries(userId, guildId);
          await updateTokenLastUsed(userId, guildId);
          
          // Update database entry
          await updateAutoJoinEntryStatus(
            userId,
            messageId,
            channelId,
            'success',
            { 
              attempts: attempt + 1,
              lastAttemptAt: Date.now()
            }
          );

          logger.info(`✅ AutoJoin entry successful`, {
            component: 'AutoJoinManager',
            userId,
            prize: prize?.substring(0, 50),
            messageId,
            attempts: attempt + 1
          });

          this.processingEntries.delete(entryId);
          return { success: true };
        } else {
          lastError = result.error || 'Unknown error';
          
          // Update database entry with attempt
          await updateAutoJoinEntryStatus(
            userId,
            messageId,
            channelId,
            'attempting',
            { 
              attempts: attempt + 1,
              lastError: lastError,
              lastAttemptAt: Date.now()
            }
          );
        }
      }

      // All retries failed
      session.stats.failed++;
      
      await updateAutoJoinEntryStatus(
        userId,
        messageId,
        channelId,
        'failed',
        { 
          attempts: MAX_RETRIES,
          lastError: lastError,
          lastAttemptAt: Date.now()
        }
      );

      logger.warn(`AutoJoin entry failed after ${MAX_RETRIES} attempts`, {
        component: 'AutoJoinManager',
        userId,
        prize: prize?.substring(0, 50),
        error: lastError
      });

      this.processingEntries.delete(entryId);
      return { success: false, error: lastError };
    } catch (error) {
      logger.error(`AutoJoin entry error`, {
        component: 'AutoJoinManager',
        userId,
        messageId,
        error: formatError(error)
      });
      
      await updateAutoJoinEntryStatus(
        userId,
        messageId,
        channelId,
        'failed',
        { 
          attempts: MAX_RETRIES,
          lastError: formatError(error),
          lastAttemptAt: Date.now()
        }
      );
      
      this.processingEntries.delete(entryId);
      return { success: false, error: formatError(error) };
    }
  }

  // ============================================================================
  // Entry Attempt - Using autoJoin.ts detection logic
  // ============================================================================

  private async attemptEntry(
    session: AutoJoinSession,
    entry: GiveawayToJoin
  ): Promise<{ success: boolean; error?: string }> {
    const { messageId, channelId, prize, buttonCustomId, guildId } = entry;

    try {
      // Get the channel
      const channel = session.client.channels.cache.get(channelId) as TextChannel;
      if (!channel) {
        return { success: false, error: 'Channel not found' };
      }

      // Get the message
      let message: Message;
      try {
        message = await channel.messages.fetch(messageId);
        logger.debug(`Message fetched for entry`, {
          component: 'AutoJoinManager',
          userId: session.userId,
          messageId,
          hasEmbed: !!message.embeds?.length,
          hasComponents: !!(message as any).components?.length
        });
      } catch (error) {
        return { success: false, error: `Message not found: ${formatError(error)}` };
      }

      // Check for blocked content
      const content = message.content || '';
      if (BLOCKED_MESSAGE_CONTENT.some(re => re.test(content))) {
        logger.debug(`Blocked content detected, skipping`, {
          component: 'AutoJoinManager',
          userId: session.userId,
          messageId
        });
        return { success: false, error: 'Blocked content detected' };
      }

      // Find entry button using autoJoin.ts logic
      let button = buttonCustomId;
      if (!button) {
        const foundButton = this.findEntryButton(message);
        if (foundButton) {
          button = foundButton.customId;
          logger.debug(`Found entry button: ${button} (label: ${foundButton.label})`, {
            component: 'AutoJoinManager',
            userId: session.userId
          });
        } else {
          // Try refreshing the message to get components
          for (let i = 0; i < COMPONENT_RETRY_ATTEMPTS; i++) {
            await delay(COMPONENT_RETRY_DELAY_MS);
            try {
              const refreshed = await channel.messages.fetch(messageId);
              const refreshedButton = this.findEntryButton(refreshed);
              if (refreshedButton) {
                button = refreshedButton.customId;
                message = refreshed;
                logger.debug(`Found entry button after refresh: ${button}`, {
                  component: 'AutoJoinManager',
                  userId: session.userId,
                  attempt: i + 1
                });
                break;
              }
            } catch {
              // Ignore
            }
          }
          
          if (!button) {
            logger.debug(`No entry button found after ${COMPONENT_RETRY_ATTEMPTS} attempts`, {
              component: 'AutoJoinManager',
              userId: session.userId,
              messageId
            });
            return { success: false, error: 'No entry button found' };
          }
        }
      }

      // Click the button
      await delay(BUTTON_DELAY_MS);
      
      logger.debug(`Attempting to click button: ${button}`, {
        component: 'AutoJoinManager',
        userId: session.userId
      });
      
      try {
        // Try the clickButton method first
        const selfbotMsg = message as Message & { clickButton?: (id: string) => Promise<unknown> };
        if (typeof selfbotMsg.clickButton === 'function') {
          await selfbotMsg.clickButton(button);
        } else {
          // Fallback: POST interaction directly
          await this.postInteraction(message, button, session);
        }
        
        logger.info(`✅ Entered giveaway`, {
          component: 'AutoJoinManager',
          userId: session.userId,
          prize: prize?.substring(0, 50),
          button
        });
        
        // Update last used time
        await updateTokenLastUsed(session.userId, session.guildId);
        
        // Check for win detection
        await this.checkForWin(message, session);
        
        // Save webhook if configured
        const webhookUrl = await getUserWebhook(session.userId, session.guildId);
        if (webhookUrl) {
          try {
            await fetch(webhookUrl, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                content: `✅ Entered giveaway: ${prize || 'Unknown prize'}`,
                username: 'AutoJoiner',
              }),
            });
          } catch {
            // Ignore webhook errors
          }
        }
        
        return { success: true };
      } catch (error) {
        logger.error(`Failed to click button: ${formatError(error)}`, {
          component: 'AutoJoinManager',
          userId: session.userId,
          button
        });
        return { 
          success: false, 
          error: `Failed to click button: ${formatError(error)}` 
        };
      }
    } catch (error) {
      return { 
        success: false, 
        error: formatError(error) 
      };
    }
  }

  // ============================================================================
  // Button Detection - From autoJoin.ts
  // ============================================================================

  private findEntryButton(message: Message): { customId: string; label: string } | null {
    const components = (message as any).components;
    if (!components?.length) {
      logger.debug(`No components found on message`, {
        component: 'AutoJoinManager',
        messageId: message.id
      });
      return null;
    }

    logger.debug(`Searching for entry button in ${components.length} component rows`, {
      component: 'AutoJoinManager',
      messageId: message.id
    });

    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;

      for (const comp of comps) {
        // Skip non-buttons
        if (comp.type !== 2 && comp.type !== 'BUTTON') continue;
        // Skip link buttons
        if (comp.style === 5) continue;
        // Skip disabled buttons
        if (comp.disabled === true) continue;

        const customId = comp.customId || comp.custom_id || '';
        const label = (comp.label || '').trim();

        logger.debug(`Checking button: customId=${customId}, label=${label}`, {
          component: 'AutoJoinManager'
        });

        // Skip blocked buttons (Leave, Withdraw, etc.)
        if (BLOCKED_BUTTON_LABELS.some(re => re.test(label))) {
          logger.debug(`Skipping blocked button: ${label}`, {
            component: 'AutoJoinManager'
          });
          continue;
        }

        // Check trusted custom IDs first (GiveawayBoat uses "giveaway_message")
        if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) {
          logger.debug(`Matched trusted customId entry button: ${customId} (label: ${label})`, {
            component: 'AutoJoinManager'
          });
          return { customId, label: label || customId };
        }

        // Check label patterns (including bare numbers for GiveawayBoat)
        if (ENTRY_BUTTON_PATTERNS.some(re => re.test(label))) {
          logger.debug(`Matched label entry button: ${label} (customId: ${customId})`, {
            component: 'AutoJoinManager'
          });
          return { customId, label: label || 'Enter' };
        }
      }
    }

    logger.debug(`No entry button found in any row`, {
      component: 'AutoJoinManager',
      messageId: message.id
    });
    return null;
  }

  // ============================================================================
  // Interaction POST - From autoJoin.ts (FIXED: uses session.client)
  // ============================================================================

  private async postInteraction(message: Message, customId: string, session: AutoJoinSession): Promise<void> {
    const clientAny = session.client as unknown as Record<string, unknown>;
    const sessionId = (clientAny['sessionId'] ?? clientAny['session_id'] ?? Date.now().toString()) as string;
    const nonce = `${Date.now()}${Math.random().toString(36).slice(2, 8)}`;
    const messageAny = message as unknown as Record<string, unknown>;
    const appId = (messageAny['applicationId'] ?? messageAny['application_id'] ?? message.author?.id) as string | undefined;

    if (!appId) {
      throw new Error('Could not determine application ID for interaction');
    }

    const payload = {
      type: 3,
      nonce,
      guild_id: message.guild?.id ?? null,
      channel_id: message.channel.id,
      message_id: message.id,
      application_id: appId,
      session_id: sessionId,
      data: {
        component_type: 2,
        custom_id: customId,
      },
    };

    try {
      const token = (session.client as any).token;
      await this.http.post('/interactions', payload, {
        headers: {
          'Authorization': token,
          'Content-Type': 'application/json',
        },
      });
    } catch (err: unknown) {
      const axiosErr = err as { response?: { status?: number; data?: { retry_after?: number } } };
      const status = axiosErr.response?.status;

      if (status === 429) {
        const retryAfterMs = Math.ceil((axiosErr.response?.data?.retry_after ?? 1) * 1000);
        logger.warn(`Interaction rate-limited, retrying after ${retryAfterMs}ms`, {
          component: 'AutoJoinManager',
          customId
        });
        await delay(retryAfterMs);
        await this.http.post('/interactions', payload, {
          headers: {
            'Authorization': (session.client as any).token,
            'Content-Type': 'application/json',
          },
        });
        return;
      }

      if (status === 404) throw new Error('Interaction 404 — message or channel no longer exists');
      if (status === 401 || status === 403) throw new Error(`Interaction ${status} — check token / permissions`);

      const errMsg = axiosErr instanceof Error ? axiosErr.message : String(err);
      throw new Error(`Interaction POST failed (HTTP ${status ?? 'unknown'}): ${errMsg}`);
    }
  }

  // ============================================================================
  // Win Detection - From autoJoin.ts
  // ============================================================================

  private async checkForWin(message: Message, session: AutoJoinSession): Promise<void> {
    if (!message.guild) return;
    
    const myId = session.client.user?.id;
    if (!myId) return;

    // Check if we're mentioned
    const mentionedInUsers = message.mentions?.users?.has(myId) ?? false;
    const mentionedInContent = (message.content ?? '').includes(myId);
    if (!mentionedInUsers && !mentionedInContent) return;

    // Check win patterns
    const allText = this.extractAllText(message);
    if (!WIN_PATTERNS.some(re => re.test(allText))) return;

    // Dedup
    const dedupKey = `${message.channel.id}:${message.author?.id ?? 'unknown'}`;
    if (this.recentWins.has(dedupKey)) {
      const lastWin = this.recentWins.get(dedupKey)!;
      if (Date.now() - lastWin < WIN_DEDUP_TTL_MS) {
        logger.debug(`Win dedup — suppressing duplicate notification`, {
          component: 'AutoJoinManager',
          userId: session.userId
        });
        return;
      }
    }
    this.recentWins.set(dedupKey, Date.now());

    // Process win
    session.stats.wins++;
    await incrementTokenWins(session.userId, session.guildId);

    const prize = this.extractPrize(message);
    const sourceName = `#${(message.channel as { name?: string }).name ?? message.channel.id} in ${message.guild.name}`;

    logger.info(`🏆 WIN DETECTED!`, {
      component: 'AutoJoinManager',
      userId: session.userId,
      prize,
      source: sourceName,
      guild: message.guild.name,
    });

    // Send win webhook
    await this.sendWinWebhook(message, prize, sourceName, session.userId);
    this.emit('giveawayWon', { message, prize, userId: session.userId });
  }

  // ============================================================================
  // Win Webhook - From autoJoin.ts
  // ============================================================================

  private async sendWinWebhook(
    message: Message,
    prize: string,
    sourceName: string,
    userId: string
  ): Promise<void> {
    const session = this.sessions.get(userId);
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
      await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
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
        }),
      });
    } catch (error) {
      logger.warn(`Win webhook failed`, {
        component: 'AutoJoinManager',
        userId,
        error: formatError(error)
      });
    }
  }

  // ============================================================================
  // Helpers - From autoJoin.ts
  // ============================================================================

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

  private cleanText(text: string): string {
    return text.substring(0, 200).trim();
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
    return texts.some(t => GIVEAWAY_KEYWORD_PATTERNS.some(re => re.test(t)));
  }

  // ============================================================================
  // Cleanup
  // ============================================================================

  private startCleanupInterval(): void {
    if (this.cleanupInterval) return;
    
    this.cleanupInterval = setInterval(() => {
      this.cleanupSessions();
    }, SESSION_CLEANUP_INTERVAL_MS);
    
    if (this.cleanupInterval.unref) {
      this.cleanupInterval.unref();
    }
  }

  private cleanupSessions(): void {
    const now = Date.now();
    let cleaned = 0;

    // Clean up win dedup map
    const cutoff = Date.now() - WIN_DEDUP_TTL_MS;
    for (const [key, ts] of this.recentWins) {
      if (ts < cutoff) {
        this.recentWins.delete(key);
        cleaned++;
      }
    }

    for (const [key, session] of this.sessions) {
      // Remove sessions older than 24 hours
      if (now - session.startedAt > MAX_SESSION_AGE_MS) {
        this.stopSession(session.userId, session.guildId);
        cleaned++;
        continue;
      }

      // Check if client is still connected
      if (session.isActive && !session.client.isReady()) {
        session.isActive = false;
        this.sessions.set(key, session);
        cleaned++;
      }
    }

    if (cleaned > 0) {
      logger.debug(`Cleaned up ${cleaned} AutoJoin sessions/entries`, {
        component: 'AutoJoinManager',
        remaining: this.sessions.size
      });
    }
  }

  // ============================================================================
  // Stats
  // ============================================================================

  public getStats(): {
    totalSessions: number;
    activeSessions: number;
    queueSize: number;
    processingEntries: number;
    queueStats: QueueStats;
    sessionStats: Map<string, AutoJoinSession['stats']>;
  } {
    let active = 0;
    const sessionStats = new Map<string, AutoJoinSession['stats']>();

    for (const [key, session] of this.sessions) {
      if (session.isActive) active++;
      sessionStats.set(key, session.stats);
    }

    return {
      totalSessions: this.sessions.size,
      activeSessions: active,
      queueSize: this.entryQueue.length,
      processingEntries: this.processingEntries.size,
      queueStats: { ...this.queueStats },
      sessionStats
    };
  }

  public getSession(userId: string, guildId: string): AutoJoinSession | null {
    if (!userId || !guildId) return null;
    const sessionKey = `${userId}:${guildId}`;
    return this.sessions.get(sessionKey) || null;
  }

  public isUserSessionActive(userId: string, guildId: string): boolean {
    if (!userId || !guildId) return false;
    const sessionKey = `${userId}:${guildId}`;
    const session = this.sessions.get(sessionKey);
    return session ? session.isActive : false;
  }

  public getQueueStats(): QueueStats {
    return { ...this.queueStats };
  }

  public clearQueue(): void {
    this.entryQueue = [];
    this.queueStats = {
      processed: 0,
      succeeded: 0,
      failed: 0,
      skipped: 0,
    };
  }

  // ============================================================================
  // Shutdown
  // ============================================================================

  public async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    
    if (this.queueInterval) {
      clearInterval(this.queueInterval);
      this.queueInterval = null;
    }

    // Clear queue
    this.entryQueue = [];
    this.isProcessingQueue = false;

    logger.info('Shutting down AutoJoinManager...', {
      component: 'AutoJoinManager',
      sessions: this.sessions.size,
      queueStats: this.queueStats
    });

    const stopPromises: Promise<void>[] = [];
    for (const [key, session] of this.sessions) {
      stopPromises.push(
        this.stopSession(session.userId, session.guildId)
      );
    }

    await Promise.allSettled(stopPromises);
    
    this.sessions.clear();
    this.processingEntries.clear();
    this.recentWins.clear();
    
    logger.info('AutoJoinManager shutdown complete', {
      component: 'AutoJoinManager',
      totalProcessed: this.queueStats.processed,
      succeeded: this.queueStats.succeeded,
      failed: this.queueStats.failed
    });
  }
}

export default AutoJoinManager;
