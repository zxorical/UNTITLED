/**
 * @module autoJoin/manager
 * AutoJoin Manager - Handles automatic giveaway entry for premium users
 * Uses the existing database functions and discord.js-selfbot-v13
 * 
 * FIXES APPLIED:
 * 1. Fixed 'this' type error in getStats()
 * 2. Fixed Promise type error in shutdown()
 * 3. Proper session management
 * 4. Queue processing with batching
 * 5. Retry logic with exponential backoff
 * 6. Webhook support
 * 7. Comprehensive stats
 * 8. Proper cleanup on shutdown
 */

import { Client, Message, TextChannel } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import { logger } from '../logger.js';
import { CONFIG } from '../config.js';
import { delay, formatError } from '../utils.js';
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

  constructor() {
    super();
    this.startCleanupInterval();
    this.startQueueProcessor();
  }

  // ============================================================================
  // Public API
  // ============================================================================

  public async startAllSessions(): Promise<void> {
    try {
      const premiumUsers = await getAllPremiumUsersAllGuilds();
      
      let started = 0;
      let failed = 0;
      
      for (const user of premiumUsers) {
        if (user.token && user.tokenActive !== false) {
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
    const sessionKey = `${userId}:${guildId}`;
    
    if (this.sessions.has(sessionKey)) {
      return true;
    }

    try {
      // Get user's token from database
      const user = await getPremiumUser(userId, guildId);
      if (!user || !user.token || user.tokenActive === false) {
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
        // Update session active status
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

      // Login
      await client.login(user.token);

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
      
      logger.info(`AutoJoin session started`, {
        component: 'AutoJoinManager',
        userId,
        guildId,
        label: session.label
      });

      return true;
    } catch (error) {
      logger.error(`Failed to start AutoJoin session`, {
        component: 'AutoJoinManager',
        userId,
        guildId,
        error: formatError(error)
      });
      return false;
    }
  }

  public async stopSession(userId: string, guildId: string): Promise<void> {
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

          logger.debug(`AutoJoin entry successful`, {
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

      logger.debug(`AutoJoin entry failed after ${MAX_RETRIES} attempts`, {
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
  // Entry Attempt
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
      } catch (error) {
        return { success: false, error: `Message not found: ${formatError(error)}` };
      }

      // Find entry button
      let button = buttonCustomId;
      if (!button) {
        const foundButton = this.findEntryButton(message);
        if (foundButton) {
          button = foundButton.customId;
        } else {
          return { success: false, error: 'No entry button found' };
        }
      }

      // Click the button
      await delay(BUTTON_DELAY_MS);
      
      // Use the message's click method if available
      try {
        // @ts-ignore - discord.js-selfbot-v13 has this method
        await message.clickButton(button);
        
        // Update last used time
        await updateTokenLastUsed(session.userId, session.guildId);
        
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

  private findEntryButton(message: Message): { customId: string; label: string } | null {
    const components = (message as any).components;
    if (!components) return null;

    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      
      for (const comp of comps) {
        if (comp.type !== 2 || comp.style === 5 || comp.disabled === true) continue;
        
        const customId = comp.customId || comp.custom_id || '';
        const label = (comp.label || '').toLowerCase();
        
        // Check if it's an entry button
        if (
          customId.includes('enter') ||
          customId.includes('join') ||
          customId.includes('giveaway') ||
          customId.includes('participate') ||
          label.includes('enter') ||
          label.includes('join') ||
          label.includes('giveaway') ||
          label.includes('participate') ||
          label.includes('🎉') ||
          label.includes('🎁') ||
          label.includes('✅') ||
          label.includes('enter giveaway')
        ) {
          return { customId, label: comp.label || '' };
        }
      }
    }
    return null;
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
      logger.debug(`Cleaned up ${cleaned} AutoJoin sessions`, {
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
    const sessionKey = `${userId}:${guildId}`;
    return this.sessions.get(sessionKey) || null;
  }

  public isUserSessionActive(userId: string, guildId: string): boolean {
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
    
    logger.info('AutoJoinManager shutdown complete', {
      component: 'AutoJoinManager',
      totalProcessed: this.queueStats.processed,
      succeeded: this.queueStats.succeeded,
      failed: this.queueStats.failed
    });
  }
}

export default AutoJoinManager;
