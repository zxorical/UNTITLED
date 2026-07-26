/**
 * @module giveawayManager
 * Reliable giveaway detector — scans everything, misses nothing.
 */

import {
  Client,
  Message,
  TextChannel,
} from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import { CONFIG } from './config.js';
import { logger, AppLogger } from './logger.js';
import {
  delay,
  formatError,
  truncate,
  sanitizeForLog,
} from './utils.js';
import {
  GiveawayData,
  DetectionSource,
  DetectedGiveaway,
} from './types.js';
import {
  insertGiveaway,
  wasNotifiedRecently,
  markNotified,
  updateLastSeen,
  getGiveaway,
  markEnded,
  getAllWatchlists,
} from './database.js';
import { BotManager } from './bot.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ALLOWED_GIVEAWAY_BOT_IDS: ReadonlySet<string> = new Set([
  '530082442967646230',
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
]);

const ENTRY_BUTTON_LABEL_PATTERNS: ReadonlyArray<RegExp> = [
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

const ENTRY_EMOJI_PATTERNS: ReadonlyArray<string> = [
  '🎉', '🎁', '🎊', '🎈', '🎀', '👍', '✅',
];

const BLOCKED_MESSAGE_CONTENT: ReadonlyArray<RegExp> = [
  /already\s+entered\s+this\s+giveaway/i,
  /you(?:'ve|\s+have)\s+already\s+entered/i,
  /you\s+are\s+already\s+(?:in|entered|participating)/i,
  /you(?:'ve|\s+have)\s+already\s+(?:joined|joined\s+this)/i,
  /leave\s+giveaway/i,
  /join(?:ed)?\s+success(?:fully)?/i,
  /entry\s+confirmed/i,
  /entered\s+successfully/i,
  /you're\s+entered/i,
  /withdraw\s+entry/i,
  /giveaway\s+(?:has\s+)?ended/i,
  /giveaway\s+(?:is\s+)?over/i,
  /winner(?:s)?\b.*\bselected/i,
  /congratulations\b/i,
  /you\s+won/i,
  /you\s+did\s+not\s+win/i,
  /results\s+are\s+in/i,
  /this\s+giveaway\s+is\s+now\s+closed/i,
  /thank\s+you\s+for\s+participating/i,
];

// DRAFT GIVEAWAY INDICATORS - messages that look like giveaways but aren't started yet
const DRAFT_GIVEAWAY_INDICATORS: ReadonlyArray<RegExp> = [
  /Review your giveaway/i,
  /click\s+"Start"\s+to/i,
  /this message expires in/i,
  /preview/i,
  /draft/i,
  /giveaway\s+preview/i,
  /configure\s+your\s+giveaway/i,
];

// ---------------------------------------------------------------------------
// Scoring System
// ---------------------------------------------------------------------------
enum GiveawaySignal {
  ENTRY_BUTTON = 3,
  ENTRY_REACTION = 2,
  TITLE_KEYWORD = 2,
  DESCRIPTION_KEYWORD = 1,
  FOOTER_ENDS = 2,
  FUTURE_TIMESTAMP = 3,
  EMBED_COLOR = 1,
  AUTHOR_KNOWN = 1,
  FIELD_GIVEAWAY = 2,
}

const GIVEAWAY_KEYWORDS: ReadonlyArray<RegExp> = [
  /\bgiveaway\b/i,
  /\braffle\b/i,
  /\bsweepstakes\b/i,
  /\bwin\b/i,
  /\bprize\b/i,
];

const MINIMUM_SCORE_THRESHOLD = 6;

// Creation detection threshold (lowered from 8 to 7)
const CREATION_SCORE_THRESHOLD = 7;

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------
interface ButtonInfo {
  customId: string;
  label: string;
}

interface ReactionInfo {
  emoji: string;
}

interface CreationResult {
  isCreation: boolean;
  score: number;
}

// ---------------------------------------------------------------------------
// GiveawayManager
// ---------------------------------------------------------------------------
export class GiveawayManager extends EventEmitter {
  private readonly client: Client;
  private readonly log: AppLogger;
  private readonly accountLabel: string;
  private readonly botManager: BotManager | null;
  private readonly userToken: string;

  private processingMessages = new Set<string>();

  private inviteCache = new Map<string, { url: string; expiresAt: number }>();
  private pendingInvites = new Map<string, Promise<string>>();

  // OPTIMIZATION: Cache watchlist items for faster lookups
  private watchlistCache: Map<string, string[]> = new Map();
  private watchlistCacheExpiry: number = 0;
  private readonly WATCHLIST_CACHE_TTL = 60000; // 60 seconds

  // OPTIMIZATION: Cache giveaway text to avoid rebuilding
  private giveawayTextCache = new Map<string, string>();

  // Cache creation detection results per message (keyed by message ID only - globally unique)
  private creationCache = new Map<string, { result: CreationResult; timestamp: number }>();
  private readonly CREATION_CACHE_TTL = 5000; // 5 seconds
  private creationCacheCleanupInterval: NodeJS.Timeout | null = null;

  private stats = {
    detected: 0,
    notified: 0,
    skipped: 0,
    errors: 0,
    falsePositivesBlocked: 0,
    watchlistMatches: 0,
    draftsSkipped: 0,
    startedAt: Date.now(),
  };

  // Track message edit versions to handle delayed embed population
  private messageEditTracker = new Map<string, { version: number; timestamp: number }>();

  // Invite refresher interval
  private inviteRefresherInterval: NodeJS.Timeout | null = null;

  constructor(
    client: Client,
    log: AppLogger,
    token: string,
    accountLabel: string,
    botManager: BotManager | null,
  ) {
    super();
    this.client = client;
    this.log = log;
    this.accountLabel = accountLabel;
    this.botManager = botManager;
    this.userToken = token;

    // Start the invite refresher
    this.startInviteRefresher();
    
    // Start creation cache cleanup (runs every 60 seconds)
    this.startCreationCacheCleanup();
  }

  // -------------------------------------------------------------------------
  // Cache Cleanup
  // -------------------------------------------------------------------------
  private startCreationCacheCleanup(): void {
    if (this.creationCacheCleanupInterval) {
      clearInterval(this.creationCacheCleanupInterval);
    }

    this.creationCacheCleanupInterval = setInterval(() => {
      const now = Date.now();
      let cleaned = 0;
      for (const [key, value] of this.creationCache) {
        if (now - value.timestamp > this.CREATION_CACHE_TTL) {
          this.creationCache.delete(key);
          cleaned++;
        }
      }
      if (cleaned > 0) {
        this.log.debug(`Cleaned ${cleaned} expired creation cache entries`);
      }
    }, 60000); // Clean every 60 seconds

    if (this.creationCacheCleanupInterval.unref) {
      this.creationCacheCleanupInterval.unref();
    }
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------
  public async handleMessage(message: Message): Promise<void> {
    const receivedAt = Date.now();

    if (!message.guild) return;
    if (message.author?.id === this.client.user?.id) return;

    if (
      CONFIG.monitoredChannels.length > 0 &&
      !CONFIG.monitoredChannels.includes(message.channel.id)
    ) {
      return;
    }

    // Check if it's a bot and if it's allowed (removed creation check from here)
    if (!message.author?.bot) return;
    if (!ALLOWED_GIVEAWAY_BOT_IDS.has(message.author.id)) return;

    const content = message.content || '';
    if (BLOCKED_MESSAGE_CONTENT.some(re => re.test(content))) {
      return;
    }

    for (const embed of message.embeds ?? []) {
      const text = [embed.title, embed.description].join(' ').toLowerCase();
      if (BLOCKED_MESSAGE_CONTENT.some(re => re.test(text))) {
        return;
      }
    }

    const key = `${message.id}-${message.channel.id}`;
    if (this.processingMessages.has(key)) {
      return;
    }
    this.processingMessages.add(key);

    try {
      // ================================================================
      // SCORE-BASED CREATION DETECTION with caching and smart refresh
      // ================================================================
      
      // Check cache first (keyed by message ID only - globally unique)
      const cacheKey = message.id;
      let cached = this.creationCache.get(cacheKey);
      let result: CreationResult;

      if (cached && (Date.now() - cached.timestamp) < this.CREATION_CACHE_TTL) {
        result = cached.result;
      } else {
        // Initial check
        result = this.isCreationMessage(message);
        
        // Track if we've seen this message before (for edit detection)
        const editKey = message.id;
        const editInfo = this.messageEditTracker.get(editKey);
        const version = editInfo ? editInfo.version + 1 : 1;
        this.messageEditTracker.set(editKey, { version, timestamp: Date.now() });
        
        // DEBUG: Log original message state
        console.log(`[DEBUG] Original message ${message.id}:`);
        console.log("CONTENT:", message.content || "(empty)");
        console.log("EMBEDS:", message.embeds.length);
        console.log("EMBEDS DATA:", message.embeds.map(e => ({
            title: e.title,
            description: e.description?.slice(0, 100),
            footer: e.footer?.text,
            fields: e.fields?.length
        })));
        console.log("COMPONENTS:", (message as any).components?.length || 0);
        console.log("COMPONENTS DATA:", (message as any).components ? 
            JSON.stringify((message as any).components, null, 2).slice(0, 500) : 
            "none"
        );
        console.log("SCORE:", result.score);
        console.log("IS_CREATION:", result.isCreation);
        console.log("---");
        
        // Smart refresh: only fetch if important data is missing
        if (!result.isCreation && this.shouldRefreshMessage(message)) {
          try {
            // Use channel.messages.fetch() for a fresh API request
            console.log(`[DEBUG] Fetching fresh version of message ${message.id}...`);
            const refreshed = await message.channel.messages.fetch(message.id);
            
            console.log(`[DEBUG] Fresh message ${message.id}:`);
            console.log("  Original embeds:", message.embeds.length);
            console.log("  Fetched embeds:", refreshed.embeds.length);
            console.log("  Original components:", (message as any).components?.length || 0);
            console.log("  Fetched components:", (refreshed as any).components?.length || 0);
            
            const refreshedResult = this.isCreationMessage(refreshed);
            // Only use refreshed result if it changed (score increased or became a creation)
            if (refreshedResult.isCreation || refreshedResult.score > result.score) {
              console.log(`[DEBUG] Using refreshed result: score ${result.score} -> ${refreshedResult.score}, isCreation ${result.isCreation} -> ${refreshedResult.isCreation}`);
              result = refreshedResult;
            } else {
              console.log(`[DEBUG] Refreshed result unchanged, keeping original`);
            }
          } catch (err) {
            console.log(`[DEBUG] Failed to fetch fresh message: ${formatError(err)}`);
            // If fetch fails, keep original result
          }
        }
        
        // Cache the result
        this.creationCache.set(cacheKey, {
          result,
          timestamp: Date.now(),
        });
      }
      
      // Log the score for debugging (only if score >= 4 to reduce noise)
      if (result.score >= 4) {
        const allText = this.getCachedGiveawayText(message);
        const components = (message as any).components as any[] | undefined;
        const buttonLabels = this.getButtonLabels(components);
        
        this.log.debug('Creation score', {
          messageId: message.id,
          score: result.score,
          threshold: CREATION_SCORE_THRESHOLD,
          isCreation: result.isCreation,
          textPreview: allText.slice(0, 150),
          buttons: buttonLabels,
          embeds: message.embeds?.length ?? 0,
          components: components?.length ?? 0,
        });
      }
      
      if (result.isCreation) {
        this.stats.draftsSkipped++;
        this.log.debug('Skipping giveaway creation (score-based detection)', {
          messageId: message.id,
          channelId: message.channel.id,
          score: result.score,
        });
        return;
      }

      // ================================================================
      // END CREATION DETECTION
      // ================================================================

      // Check for other draft indicators (fallback)
      if (this.isDraftGiveaway(message)) {
        this.stats.draftsSkipped++;
        this.log.debug('Skipping draft giveaway', {
          messageId: message.id,
          channelId: message.channel.id,
        });
        return;
      }

      const existing = await getGiveaway(message.id, message.channel.id);
      if (existing) {
        await updateLastSeen(message.id, message.channel.id);
        if (existing.status === 'active' && this.isEnded(message)) {
          await markEnded(message.id, message.channel.id);
        }
        return;
      }

      const detected = await this.detectGiveaway(message);
      if (!detected) {
        this.stats.falsePositivesBlocked++;
        return;
      }

      const detectionTime = Date.now() - receivedAt;

      // Get guild data for banner and icon
      const guild = message.guild;
      const guildIcon = guild?.iconURL({ size: 512 }) || null;
      const guildBanner = (guild as any)?.bannerURL?.({ size: 1024 }) || null;
      const memberCount = (guild as any)?.memberCount ?? null;

      const data: Omit<GiveawayData, 'id' | 'status' | 'notifiedAt' | 'lastSeenAt'> = {
        messageId: message.id,
        channelId: message.channel.id,
        guildId: message.guild.id,
        guildName: message.guild.name,
        channelName: (message.channel as any).name || 'unknown',
        authorId: message.author?.id || '',
        prize: detected.prize,
        detectedAt: receivedAt,
        endsAt: detected.endsAt,
        detectionTimeMs: detectionTime,
        guildIcon: guildIcon,
        guildBanner: guildBanner,
        memberCount: memberCount,
      };

      this.stats.detected++;

      if (await wasNotifiedRecently(message.id, message.channel.id, CONFIG.notificationCooldown)) {
        this.stats.skipped++;
        return;
      }

      const savePromise = insertGiveaway(data);
      const notifyPromise = this.sendNotification(data);

      const inserted = await savePromise;
      if (!inserted) {
        return;
      }

      const inviteUrl = await notifyPromise;

      // Check watchlist matches using cached data
      await this.checkWatchlistMatches(message, detected.prize, inviteUrl);

    } catch (error) {
      this.stats.errors++;
      this.log.error(`Error handling message ${message.id}: ${formatError(error)}`);
    } finally {
      this.processingMessages.delete(key);
      this.giveawayTextCache.delete(message.id);
    }
  }

  // -------------------------------------------------------------------------
  // Helper: Check if message should be refreshed
  // -------------------------------------------------------------------------
  private shouldRefreshMessage(message: Message): boolean {
    // If there are no embeds but the message likely should have them
    if (message.embeds.length === 0) {
      return true;
    }
    
    // If there are no components but the message likely should have them
    const components = (message as any).components;
    if (!components || components.length === 0) {
      return true;
    }
    
    // If the content is very short but has giveaway-like text
    const content = message.content || '';
    if (content.length < 50 && /\bgiveaway\b/i.test(content)) {
      return true;
    }
    
    return false;
  }

  // -------------------------------------------------------------------------
  // Helper: Get button labels
  // -------------------------------------------------------------------------
  private getButtonLabels(components: any[] | undefined): string[] {
    if (!components) return [];
    
    const labels: string[] = [];
    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2) {
          const label = (comp.label || '').toLowerCase().trim();
          labels.push(label);
        }
      }
    }
    return labels;
  }

  // -------------------------------------------------------------------------
  // SCORE-BASED CREATION DETECTION
  // -------------------------------------------------------------------------

  /**
   * Score-based detection for giveaway creation messages
   * Returns { isCreation: boolean, score: number }
   */
  private isCreationMessage(message: Message): CreationResult {
    // Use cached text to avoid rebuilding
    const allText = this.getCachedGiveawayText(message);
    const components = (message as any).components as any[] | undefined;
    
    let score = 0;
    const buttonLabels = this.getButtonLabels(components);

    // --- TEXT-BASED SIGNALS ---
    
    // High weight signals (5 points) - more specific patterns
    if (/Review your giveaway/i.test(allText)) score += 5;
    if (/this message expires in \d+ minutes?/i.test(allText)) score += 5;
    if (/click "Start" to start this giveaway/i.test(allText)) score += 5;
    if (/click 'Start' to start this giveaway/i.test(allText)) score += 5;
    if (/configure your giveaway/i.test(allText)) score += 5;

    // Medium weight signals (3 points)
    if (/giveaway preview/i.test(allText)) score += 3;
    if (/setup your giveaway/i.test(allText)) score += 3;
    if (/you can edit this/i.test(allText)) score += 3;
    if (/you can change/i.test(allText)) score += 3;
    // More specific: "Review" + "Start" combined in text
    if (/review.*start/i.test(allText)) score += 3;

    // Low weight signals (2 points)
    if (/create(?: a)? giveaway/i.test(allText)) score += 2;
    if (/select a channel/i.test(allText)) score += 2;
    if (/set (?:the )?(?:prize|duration|winners)/i.test(allText)) score += 2;

    // --- BUTTON-BASED SIGNALS ---
    // Check if any button label contains "start", "edit", "cancel", "preview", "setup"
    // This handles cases like "🎉 Start" or "✏️ Edit"
    const hasStart = buttonLabels.some(label => label.includes('start'));
    const hasEdit = buttonLabels.some(label => label.includes('edit'));
    const hasCancel = buttonLabels.some(label => label.includes('cancel'));
    const hasPreview = buttonLabels.some(label => label.includes('preview'));
    const hasSetup = buttonLabels.some(label => label.includes('setup'));

    if (hasStart) score += 3;
    if (hasEdit) score += 2;
    if (hasCancel) score += 2;
    if (hasPreview) score += 2;
    if (hasSetup) score += 2;

    // --- DURATION CHECK ---
    // Fixed regex with capturing group
    const durationMatch = allText.match(/(\d+)\s*(minute|min|m|hour|h)/i);
    if (durationMatch) {
      const value = parseInt(durationMatch[1], 10);
      const unit = (durationMatch[2] || '').toLowerCase();
      let minutes = value;
      if (unit.startsWith('h')) minutes = value * 60;
      // Creation messages often have short durations (1-15 minutes)
      if (minutes <= 15) score += 2;
    }

    const isCreation = score >= CREATION_SCORE_THRESHOLD;
    
    return { isCreation, score };
  }

  // -------------------------------------------------------------------------
  // OPTIMIZED Watchlist Matching
  // -------------------------------------------------------------------------
  private async checkWatchlistMatches(message: Message, prize: string, inviteUrl: string): Promise<void> {
    if (!this.botManager) return;

    try {
      const watchlistData = await this.getCachedWatchlists();
      if (watchlistData.size === 0) return;

      const text = this.getCachedGiveawayText(message);
      const lowerText = text.toLowerCase();

      const allItems = Array.from(watchlistData.values()).flat();
      const hasAnyMatch = allItems.some(item => lowerText.includes(item.toLowerCase()));
      if (!hasAnyMatch) return;

      const matchedUsers: string[] = [];

      for (const [userId, items] of watchlistData) {
        for (const item of items) {
          if (lowerText.includes(item.toLowerCase())) {
            matchedUsers.push(userId);
            break;
          }
        }
      }

      if (matchedUsers.length === 0) return;

      const uniqueUsers = [...new Set(matchedUsers)];
      this.stats.watchlistMatches += uniqueUsers.length;
      this.log.info(`Watchlist matches: ${uniqueUsers.length} users for "${prize}"`);

      const messageUrl = `https://discord.com/channels/${message.guild!.id}/${message.channel.id}/${message.id}`;
      const endsAt = this.extractEndTimestamp(message);

      await this.sendWatchlistDMs(uniqueUsers, prize, message, endsAt, messageUrl, inviteUrl);

    } catch (err) {
      this.log.error('Watchlist check error', { error: formatError(err) });
    }
  }

  // -------------------------------------------------------------------------
  // OPTIMIZED DM Sending with Smart Batching
  // -------------------------------------------------------------------------
  private async sendWatchlistDMs(
    users: string[],
    prize: string,
    message: Message,
    endsAt: number | null,
    messageUrl: string,
    inviteUrl: string
  ): Promise<void> {
    if (users.length === 0) return;

    let batchSize: number;
    let delayBetweenBatches: number;

    if (users.length <= 10) {
      batchSize = 5;
      delayBetweenBatches = 200;
    } else if (users.length <= 50) {
      batchSize = 10;
      delayBetweenBatches = 500;
    } else if (users.length <= 200) {
      batchSize = 15;
      delayBetweenBatches = 800;
    } else {
      batchSize = 20;
      delayBetweenBatches = 1000;
    }

    this.log.debug(`Sending ${users.length} DMs in batches of ${batchSize}`);

    let sent = 0;
    let failed = 0;

    const guild = message.guild!;
    const guildIcon = guild.iconURL({ size: 512 }) || null;
    const guildBanner = (guild as any).bannerURL?.({ size: 1024 }) || null;
    const memberCount = (guild as any).memberCount ?? null;
    const detectedAt = Date.now();

    for (let i = 0; i < users.length; i += batchSize) {
      const batch = users.slice(i, i + batchSize);

      try {
        const results = await Promise.allSettled(
          batch.map(userId =>
            this.botManager!.sendWatchlistDM(
              userId,
              prize,
              guild.name,
              (message.channel as any).name || 'unknown',
              endsAt,
              messageUrl,
              guild.id,
              guildIcon,
              detectedAt,
              inviteUrl,
              guildBanner,
              memberCount
            )
          )
        );

        for (const result of results) {
          if (result.status === 'fulfilled') sent++;
          else failed++;
        }

        if (users.length > 50 && (i + batchSize) % 50 === 0) {
          this.log.debug(`Watchlist DMs: ${Math.min(i + batchSize, users.length)}/${users.length} sent`);
        }

      } catch (err) {
        this.log.warn(`Batch failed for users ${i}-${i + batchSize}`, { error: formatError(err) });
        failed += batch.length;
      }

      if (i + batchSize < users.length) {
        const jitter = Math.random() * 200;
        await delay(delayBetweenBatches + jitter);
      }
    }

    this.log.debug(`Watchlist DMs complete: ${sent} sent, ${failed} failed`);
  }

  // -------------------------------------------------------------------------
  // Cached watchlist data
  // -------------------------------------------------------------------------
  private async getCachedWatchlists(): Promise<Map<string, string[]>> {
    const now = Date.now();
    
    if (this.watchlistCache.size > 0 && now < this.watchlistCacheExpiry) {
      return this.watchlistCache;
    }

    try {
      const watchlists = await getAllWatchlists();
      this.watchlistCache = new Map();
      
      for (const wl of watchlists) {
        if (wl.items && wl.items.length > 0) {
          this.watchlistCache.set(wl.userId, wl.items);
        }
      }
      
      this.watchlistCacheExpiry = now + this.WATCHLIST_CACHE_TTL;
      this.log.debug(`Watchlist cache refreshed: ${this.watchlistCache.size} users`);
    } catch (err) {
      this.log.error('Failed to refresh watchlist cache', { error: formatError(err) });
    }

    return this.watchlistCache;
  }

  // -------------------------------------------------------------------------
  // Cached giveaway text
  // -------------------------------------------------------------------------
  private getCachedGiveawayText(message: Message): string {
    const key = message.id;
    if (this.giveawayTextCache.has(key)) {
      return this.giveawayTextCache.get(key)!;
    }

    const text = this.getGiveawayText(message);
    this.giveawayTextCache.set(key, text);
    return text;
  }

  private getGiveawayText(message: Message): string {
    const parts = [message.content || ''];
    
    for (const embed of message.embeds || []) {
      if (embed.title) parts.push(embed.title);
      if (embed.description) parts.push(embed.description);
      if (embed.footer?.text) parts.push(embed.footer.text);
      // Include embed author
      if (embed.author?.name) parts.push(embed.author.name);
      if (embed.fields) {
        for (const field of embed.fields) {
          parts.push(field.name);
          parts.push(field.value);
        }
      }
    }
    
    return parts.join(' ');
  }

  // -------------------------------------------------------------------------
  // CREATION DETECTION METHODS (legacy - kept for backward compatibility)
  // -------------------------------------------------------------------------
  
  /**
   * Check if the message has both Edit AND Start buttons
   * This combination ONLY appears in giveaway creation/draft messages
   */
  private hasEditAndStartButtons(message: Message): boolean {
    const components = (message as any).components as any[] | undefined;
    if (!components) return false;

    let hasEdit = false;
    let hasStart = false;

    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2) { // Button
          const label = (comp.label || '').toLowerCase().trim();
          if (label === 'edit') hasEdit = true;
          if (label === 'start') hasStart = true;
        }
      }
    }

    return hasEdit && hasStart;
  }

  /**
   * Count management buttons (Edit, Start, Cancel, Preview, Setup)
   */
  private countManagementButtons(message: Message): number {
    const components = (message as any).components as any[] | undefined;
    if (!components) return 0;
    
    let count = 0;
    const managementLabels = ['edit', 'start', 'cancel', 'preview', 'setup'];
    
    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2) {
          const label = (comp.label || '').toLowerCase().trim();
          if (managementLabels.includes(label)) {
            count++;
          }
        }
      }
    }
    return count;
  }

  /**
   * Count entry buttons
   */
  private countEntryButtons(message: Message): number {
    const components = (message as any).components as any[] | undefined;
    if (!components) return 0;
    
    let count = 0;
    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2 && comp.style !== 5 && !comp.disabled) {
          if (this.isEntryButton(comp)) {
            count++;
          }
        }
      }
    }
    return count;
  }

  // -------------------------------------------------------------------------
  // Draft Giveaway Detection (fallback)
  // -------------------------------------------------------------------------
  
  /**
   * Check if this is a draft/pending giveaway creation message (fallback)
   */
  private isDraftGiveaway(message: Message): boolean {
    const content = message.content || '';
    const embed = message.embeds?.[0];
    
    // Check content for draft indicators
    if (DRAFT_GIVEAWAY_INDICATORS.some(re => re.test(content))) {
      return true;
    }

    if (embed) {
      const embedText = [
        embed.title || '',
        embed.description || '',
        embed.footer?.text || '',
        ...(embed.fields || []).flatMap(f => [f.name, f.value]),
      ].join(' ');

      if (DRAFT_GIVEAWAY_INDICATORS.some(re => re.test(embedText))) {
        return true;
      }
    }

    // Check for "Start" or "Edit" or "Cancel" buttons (draft management buttons)
    const components = (message as any).components as any[] | undefined;
    if (components) {
      let hasDraftButton = false;
      for (const row of components) {
        const comps = row.components as any[] | undefined;
        if (!comps) continue;
        for (const comp of comps) {
          if (comp.type === 2) { // Button
            const label = (comp.label || '').toLowerCase();
            if (['start', 'edit', 'cancel', 'preview'].includes(label)) {
              hasDraftButton = true;
              break;
            }
          }
        }
        if (hasDraftButton) break;
      }
      
      if (hasDraftButton) {
        // Check if there's also an entry button - if not, it's definitely a draft
        const hasEntryButton = this.hasEntryButton(message);
        if (!hasEntryButton) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Check if the message has an entry button
   */
  private hasEntryButton(message: Message): boolean {
    const components = (message as any).components as any[] | undefined;
    if (!components) return false;

    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2 && comp.style !== 5 && !comp.disabled) {
          const customId = comp.customId || comp.custom_id || '';
          const label = (comp.label || '').trim();
          
          if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) return true;
          if (ENTRY_BUTTON_LABEL_PATTERNS.some(re => re.test(label))) return true;
          
          for (const emoji of ENTRY_EMOJI_PATTERNS) {
            if (label.includes(emoji)) return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Check if a component is an entry button
   */
  private isEntryButton(comp: any): boolean {
    const customId = comp.customId || comp.custom_id || '';
    const label = (comp.label || '').trim();
    const lowerLabel = label.toLowerCase();

    // Skip management buttons (using includes to handle emoji prefixes like "🎉 Start")
    if (lowerLabel.includes('edit') || 
        lowerLabel.includes('start') || 
        lowerLabel.includes('cancel') || 
        lowerLabel.includes('preview') || 
        lowerLabel.includes('setup')) {
      return false;
    }

    if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) return true;
    if (ENTRY_BUTTON_LABEL_PATTERNS.some(re => re.test(label))) return true;
    
    for (const emoji of ENTRY_EMOJI_PATTERNS) {
      if (label.includes(emoji)) return true;
    }

    return false;
  }

  /**
   * Check if message has a Start/Edit/Cancel button
   */
  private hasDraftManagementButton(message: Message): boolean {
    const components = (message as any).components as any[] | undefined;
    if (!components) return false;

    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2) {
          const label = (comp.label || '').toLowerCase();
          if (['start', 'edit', 'cancel', 'preview'].includes(label)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  // -------------------------------------------------------------------------
  // Detection
  // -------------------------------------------------------------------------
  private async detectGiveaway(message: Message): Promise<DetectedGiveaway | null> {
    // Check if it's a creation message (uses cache)
    const cacheKey = message.id;
    const cached = this.creationCache.get(cacheKey);
    if (cached && cached.result.isCreation) {
      return null;
    }
    // If not in cache, check fresh
    if (!cached) {
      const result = this.isCreationMessage(message);
      if (result.isCreation) {
        return null;
      }
    }

    // If it has Edit+Start, it's a creation message - skip
    if (this.hasEditAndStartButtons(message)) {
      return null;
    }

    // If it has draft management buttons and no entry button, skip
    if (this.hasDraftManagementButton(message) && !this.hasEntryButton(message)) {
      this.log.debug('Skipping giveaway with draft management buttons (no entry button)', {
        messageId: message.id
      });
      return null;
    }

    let signals = this.collectSignalsSync(message);
    let score = Object.values(signals).reduce((sum, v) => sum + v, 0);
    let button = this.extractEntryButton(message);

    if (!button) {
      await delay(200);
      try {
        const refreshed = await message.channel.messages.fetch(message.id);
        signals = this.collectSignalsSync(refreshed);
        score = Object.values(signals).reduce((sum, v) => sum + v, 0);
        button = this.extractEntryButton(refreshed);
      } catch {
        // Keep original signals
      }
    }

    if (score < MINIMUM_SCORE_THRESHOLD) return null;

    const prize = this.extractPrize(message);
    const endsAt = this.extractEndTimestamp(message);

    if (endsAt && endsAt < Date.now()) return null;

    let source = DetectionSource.CONTENT;
    if (button) source = DetectionSource.COMPONENT;

    return { prize, source, endsAt, buttonCustomId: button?.customId };
  }

  // -------------------------------------------------------------------------
  // Signal collection
  // -------------------------------------------------------------------------
  private collectSignalsSync(message: Message): Record<string, number> {
    const signals: Record<string, number> = {};

    // Check if it's a creation message (uses cache)
    const cacheKey = message.id;
    const cached = this.creationCache.get(cacheKey);
    if (cached && cached.result.isCreation) {
      return {};
    }
    // If not in cache, check fresh
    if (!cached) {
      const result = this.isCreationMessage(message);
      if (result.isCreation) {
        return {};
      }
    }

    // If it has Edit+Start, skip entirely
    if (this.hasEditAndStartButtons(message)) {
      return {};
    }

    // If it has draft management buttons and no entry button, skip
    if (this.hasDraftManagementButton(message) && !this.hasEntryButton(message)) {
      return {};
    }

    const button = this.extractEntryButton(message);
    if (button) signals['ENTRY_BUTTON'] = GiveawaySignal.ENTRY_BUTTON;

    if (!button) {
      const entryReaction = this.extractEntryReaction(message);
      if (entryReaction) signals['ENTRY_REACTION'] = GiveawaySignal.ENTRY_REACTION;
    }

    const embed = message.embeds?.[0];
    if (embed) {
      const title = embed.title ?? '';
      const description = embed.description ?? '';

      if (title && GIVEAWAY_KEYWORDS.some(re => re.test(title)))
        signals['TITLE_KEYWORD'] = GiveawaySignal.TITLE_KEYWORD;

      if (description && GIVEAWAY_KEYWORDS.some(re => re.test(description)))
        signals['DESCRIPTION_KEYWORD'] = GiveawaySignal.DESCRIPTION_KEYWORD;

      if (embed.footer?.text && /\bends\b|ends\s+in|expires\b/i.test(embed.footer.text))
        signals['FOOTER_ENDS'] = GiveawaySignal.FOOTER_ENDS;

      if (embed.author?.name && /\bgiveaway\b/i.test(embed.author.name))
        signals['AUTHOR_KNOWN'] = GiveawaySignal.AUTHOR_KNOWN;

      if (embed.color && [0xF1C40F, 0x7289DA, 0x2ECC71, 0xE91E63].includes(embed.color))
        signals['EMBED_COLOR'] = GiveawaySignal.EMBED_COLOR;

      if (embed.fields) {
        for (const field of embed.fields) {
          if (/\b(?:ends?\s+in|winners?|time\s+remaining)\b/i.test(field.name)) {
            signals['FIELD_GIVEAWAY'] = GiveawaySignal.FIELD_GIVEAWAY;
            break;
          }
        }
      }
    }

    if (this.extractEndTimestamp(message) !== null) {
      signals['FUTURE_TIMESTAMP'] = GiveawaySignal.FUTURE_TIMESTAMP;
    }

    return signals;
  }

  // -------------------------------------------------------------------------
  // Button detection
  // -------------------------------------------------------------------------
  private extractEntryButton(message: Message): ButtonInfo | null {
    const components = (message as any).components as any[] | undefined;
    if (!components?.length) return null;

    // Check if it's a creation message (uses cache)
    const cacheKey = message.id;
    const cached = this.creationCache.get(cacheKey);
    if (cached && cached.result.isCreation) {
      return null;
    }
    // If not in cache, check fresh
    if (!cached) {
      const result = this.isCreationMessage(message);
      if (result.isCreation) {
        return null;
      }
    }

    // If it has Edit+Start, skip
    if (this.hasEditAndStartButtons(message)) {
      return null;
    }

    // Check if this has draft buttons - if so, skip
    let hasDraftButton = false;
    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2 && comp.style !== 5) {
          const label = (comp.label || '').toLowerCase();
          if (['start', 'edit', 'cancel', 'preview'].includes(label)) {
            hasDraftButton = true;
            break;
          }
        }
      }
      if (hasDraftButton) break;
    }

    if (hasDraftButton) {
      let hasEntry = false;
      for (const row of components) {
        const comps = row.components as any[] | undefined;
        if (!comps) continue;
        for (const comp of comps) {
          if (comp.type === 2 && comp.style !== 5 && !comp.disabled) {
            const customId = comp.customId || comp.custom_id || '';
            const label = (comp.label || '').trim();
            if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) hasEntry = true;
            if (ENTRY_BUTTON_LABEL_PATTERNS.some(re => re.test(label))) hasEntry = true;
            if (hasEntry) break;
          }
        }
        if (hasEntry) break;
      }
      
      if (!hasEntry) return null;
    }

    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps?.length) continue;

      for (const comp of comps) {
        if (comp.type !== 2 || comp.style === 5 || comp.disabled === true) continue;
        const customId = comp.customId || comp.custom_id;
        if (!customId) continue;

        const label = (comp.label || '').trim();
        const lowerLabel = label.toLowerCase();
        // Skip management buttons (using includes to handle emoji prefixes like "🎉 Start")
        if (lowerLabel.includes('edit') || 
            lowerLabel.includes('start') || 
            lowerLabel.includes('cancel') || 
            lowerLabel.includes('preview') || 
            lowerLabel.includes('setup')) {
          continue;
        }
        if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) return { customId, label: label || customId };
        if (ENTRY_BUTTON_LABEL_PATTERNS.some(re => re.test(label))) return { customId, label: label || 'Enter' };
      }
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Reaction emoji extraction
  // -------------------------------------------------------------------------
  private extractEntryReaction(message: Message): ReactionInfo | null {
    const embed = message.embeds?.[0];
    if (!embed) return null;
    const text = [embed.description, embed.footer?.text].filter(Boolean).join(' ');
    for (const emoji of ENTRY_EMOJI_PATTERNS) {
      if (text.includes(emoji)) return { emoji };
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Allowed bot check (SIMPLIFIED - no creation check)
  // -------------------------------------------------------------------------
  private isAllowedBot(message: Message): boolean {
    if (!message.author?.bot) return false;
    if (!message.author.id) return false;
    return ALLOWED_GIVEAWAY_BOT_IDS.has(message.author.id);
  }

  // -------------------------------------------------------------------------
  // Prize extraction
  // -------------------------------------------------------------------------
  private extractPrize(message: Message): string {
    const embed = message.embeds?.[0];
    if (embed) {
      if (embed.fields) {
        const prizeField = embed.fields.find(f => /\bprize\b/i.test(f.name));
        if (prizeField) return this.cleanText(prizeField.value);
      }
      if (embed.title) return this.cleanText(embed.title);
      if (embed.description) return this.cleanText(embed.description);
    }
    return this.cleanText(message.content || 'Unknown Prize');
  }

  // -------------------------------------------------------------------------
  // Timestamp extraction
  // -------------------------------------------------------------------------
  private extractEndTimestamp(message: Message): number | null {
    const re = /<t:(\d{10,13})(?::[a-zA-Z])?>/;
    const texts: string[] = [
      message.content || '',
      ...message.embeds.flatMap(e => [
        e.title || '',
        e.description || '',
        e.footer?.text || '',
        ...(e.fields || []).flatMap(f => [f.name, f.value]),
      ]),
    ];
    const joined = texts.join(' ');

    const matches = joined.matchAll(new RegExp(re.source, 'g'));
    let best: number | null = null;
    for (const match of matches) {
      const raw = parseInt(match[1], 10);
      const tsMs = raw < 1e12 ? raw * 1000 : raw;
      if (Number.isFinite(tsMs) && tsMs > Date.now()) {
        if (best === null || tsMs > best) best = tsMs;
      }
    }
    return best;
  }

  private isEnded(message: Message): boolean {
    const endsAt = this.extractEndTimestamp(message);
    if (endsAt === null) return false;
    return endsAt < Date.now();
  }

  // -------------------------------------------------------------------------
  // Utilities
  // -------------------------------------------------------------------------
  private cleanText(text: string): string {
    return truncate(sanitizeForLog(text), 200);
  }

  // -------------------------------------------------------------------------
  // INVITE GENERATION
  // -------------------------------------------------------------------------
  
  private getCachedInvite(guildId: string): string | null {
    const cached = this.inviteCache.get(guildId);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.url;
    }
    this.inviteCache.delete(guildId);
    return null;
  }

  private setCachedInvite(guildId: string, url: string): void {
    this.inviteCache.set(guildId, { url, expiresAt: Date.now() + 30 * 60 * 1000 });
  }

  private async fetchInviteForGuild(guildId: string): Promise<string> {
    const cached = this.getCachedInvite(guildId);
    if (cached) return cached;

    const pending = this.pendingInvites.get(guildId);
    if (pending) return pending;

    const promise = this.doFetchInvite(guildId);
    this.pendingInvites.set(guildId, promise);

    try {
      const url = await promise;
      if (url && !url.includes('unavailable') && !url.includes('not reachable')) {
        this.setCachedInvite(guildId, url);
      }
      return url;
    } finally {
      this.pendingInvites.delete(guildId);
    }
  }

  private async doFetchInvite(guildId: string): Promise<string> {
    try {
      const guild = this.client.guilds.cache.get(guildId);
      if (!guild) {
        this.log.warn(`Guild ${guildId} not found in cache`);
        return `https://discord.com/channels/${guildId}`;
      }

      this.log.debug(`Generating invite for guild: ${guild.name} (${guildId})`);

      try {
        const invites = await guild.invites.fetch();
        if (invites && invites.size > 0) {
          const permanent = invites.find(inv => inv.maxAge === 0 && inv.maxUses === 0);
          if (permanent) {
            this.log.debug(`Using permanent invite for ${guild.name}: ${permanent.url}`);
            return permanent.url;
          }
          
          const firstInvite = invites.first();
          if (firstInvite) {
            this.log.debug(`Using existing invite for ${guild.name}: ${firstInvite.url}`);
            return firstInvite.url;
          }
        }
      } catch (error) {
        this.log.debug(`Could not fetch existing invites for ${guild.name}: ${formatError(error)}`);
      }

      try {
        const vanityCode = (guild as any).vanityURLCode;
        if (vanityCode) {
          const vanityUrl = `https://discord.gg/${vanityCode}`;
          this.log.debug(`Using vanity URL for ${guild.name}: ${vanityUrl}`);
          return vanityUrl;
        }
      } catch (error) {
        this.log.debug(`No vanity URL for ${guild.name}: ${formatError(error)}`);
      }

      const textChannels = guild.channels.cache.filter(
        (ch): ch is TextChannel => ch.type === 'GUILD_TEXT'
      );

      if (textChannels.size === 0) {
        this.log.warn(`No text channels found in ${guild.name}`);
        return `https://discord.com/channels/${guildId}`;
      }

      const botMember = guild.members.cache.get(this.client.user?.id || '');
      if (!botMember) {
        this.log.warn(`Bot not found in ${guild.name}`);
        return `https://discord.com/channels/${guildId}`;
      }

      for (const [, channel] of textChannels) {
        try {
          const permissions = channel.permissionsFor(botMember);
          if (!permissions || !permissions.has('CREATE_INSTANT_INVITE')) {
            this.log.debug(`No CREATE_INSTANT_INVITE permission in #${channel.name}`);
            continue;
          }

          const invite = await channel.createInvite({
            maxAge: 0,
            maxUses: 0,
            reason: 'Giveaway tracker - auto-generated invite',
            temporary: false,
          });
          
          this.log.debug(`Created new invite for ${guild.name} in #${channel.name}: ${invite.url}`);
          return invite.url;
        } catch (error) {
          this.log.debug(`Failed to create invite in #${channel.name}: ${formatError(error)}`);
          continue;
        }
      }

      for (const [, channel] of textChannels) {
        try {
          const invite = await channel.createInvite({
            maxAge: 0,
            maxUses: 0,
            reason: 'Giveaway tracker - auto-generated invite (fallback)',
            temporary: false,
          });
          
          this.log.debug(`Created fallback invite for ${guild.name} in #${channel.name}: ${invite.url}`);
          return invite.url;
        } catch {
          continue;
        }
      }

      this.log.warn(`Could not create invite for ${guild.name}, using channel link fallback`);
      return `https://discord.com/channels/${guildId}`;

    } catch (error) {
      this.log.error(`Failed to generate invite for guild ${guildId}: ${formatError(error)}`);
      return `https://discord.com/channels/${guildId}`;
    }
  }

  // -------------------------------------------------------------------------
  // Invite Refresher
  // -------------------------------------------------------------------------
  
  private startInviteRefresher(): void {
    if (this.inviteRefresherInterval) {
      clearInterval(this.inviteRefresherInterval);
    }

    this.inviteRefresherInterval = setInterval(() => {
      this.refreshInvites().catch((err) => {
        this.log.debug(`Invite refresh error: ${formatError(err)}`);
      });
    }, 5 * 60 * 1000);

    if (this.inviteRefresherInterval.unref) {
      this.inviteRefresherInterval.unref();
    }
  }

  private async refreshInvites(): Promise<void> {
    const now = Date.now();
    const expired = Array.from(this.inviteCache.entries())
      .filter(([, cached]) => cached.expiresAt <= now);

    if (expired.length === 0) return;

    this.log.debug(`Refreshing ${expired.length} expired invites`);
    
    for (const [guildId] of expired) {
      this.inviteCache.delete(guildId);
      this.fetchInviteForGuild(guildId).catch((err) => {
        this.log.debug(`Failed to refresh invite for ${guildId}: ${formatError(err)}`);
      });
    }
  }

  // -------------------------------------------------------------------------
  // Notification - Returns the resolved invite URL
  // -------------------------------------------------------------------------
  
  private async sendNotification(
    data: Omit<GiveawayData, 'id' | 'status' | 'notifiedAt' | 'lastSeenAt'>
  ): Promise<string> {
    const guildId: string = data.guildId || '0';

    if (!this.botManager) return `https://discord.com/channels/${guildId}`;

    const messageId = data.messageId;
    const channelId = data.channelId;
    
    if (!messageId || !channelId) {
      this.log.warn('Cannot send notification: missing messageId or channelId');
      return `https://discord.com/channels/${guildId}`;
    }

    let inviteUrl: string;
    try {
      this.log.debug(`Generating invite for guild ${guildId} (${data.guildName})`);
      inviteUrl = await this.fetchInviteForGuild(guildId);
      
      if (!inviteUrl || 
          inviteUrl.includes('unavailable') || 
          inviteUrl.includes('not reachable') ||
          inviteUrl.includes('undefined')) {
        this.log.warn(`Invalid invite URL for guild ${guildId}, using channel link fallback`);
        inviteUrl = `https://discord.com/channels/${guildId}`;
      }
    } catch (error) {
      this.log.warn(`Failed to generate invite for guild ${guildId}: ${formatError(error)}`);
      inviteUrl = `https://discord.com/channels/${guildId}`;
    }

    this.log.debug(`Using invite URL for notification: ${inviteUrl}`);

    let guildIcon = (data as any).guildIcon || null;
    let guildBanner = (data as any).guildBanner || null;
    let memberCount = (data as any).memberCount || null;

    if (!guildIcon || !guildBanner) {
      const guild = this.client.guilds.cache.get(guildId);
      if (guild) {
        guildIcon = guildIcon || guild.iconURL({ size: 512 }) || null;
        guildBanner = guildBanner || (guild as any).bannerURL?.({ size: 1024 }) || null;
        memberCount = memberCount || (guild as any).memberCount ?? null;
      }
    }

    const fullData: GiveawayData = {
      ...data,
      id: undefined,
      status: 'active',
      notifiedAt: null,
      lastSeenAt: Date.now(),
      inviteUrl: inviteUrl,
      guildIcon: guildIcon,
      guildBanner: guildBanner,
      memberCount: memberCount,
    };

    try {
      const sent = await this.botManager.sendGiveawayNotification(fullData);
      if (sent) {
        this.stats.notified++;
        await markNotified(messageId, channelId);
        this.log.debug(`Notification sent successfully for ${data.prize}`);
      } else {
        this.stats.errors++;
        this.log.warn(`Failed to send notification for ${data.prize}`);
      }
    } catch (error) {
      this.stats.errors++;
      this.log.error(`Failed to send notification: ${formatError(error)}`);
    }

    return inviteUrl;
  }

  // -------------------------------------------------------------------------
  // Statistics and shutdown
  // -------------------------------------------------------------------------
  public getStats() {
    return { ...this.stats, uptime: Date.now() - this.stats.startedAt };
  }

  public logStats(): void {
    const s = this.stats;
    const uptime = (Date.now() - s.startedAt) / 1000;
    this.log.info(`── ${this.accountLabel} Stats ──────────────────────────`);
    this.log.info(`  Detected            : ${s.detected}`);
    this.log.info(`  Notified            : ${s.notified}`);
    this.log.info(`  Skipped (cooldown)  : ${s.skipped}`);
    this.log.info(`  Errors              : ${s.errors}`);
    this.log.info(`  False positives blocked: ${s.falsePositivesBlocked}`);
    this.log.info(`  Watchlist matches   : ${s.watchlistMatches}`);
    this.log.info(`  Drafts skipped      : ${s.draftsSkipped}`);
    this.log.info(`  Uptime              : ${Math.floor(uptime / 60)}m ${Math.floor(uptime % 60)}s`);
    this.log.info(`  Invites cached      : ${this.inviteCache.size}`);
    this.log.info(`────────────────────────────────────────────────────────`);
  }

  public resetStats(): void {
    this.stats = { 
      detected: 0, 
      notified: 0, 
      skipped: 0, 
      errors: 0, 
      falsePositivesBlocked: 0,
      watchlistMatches: 0,
      draftsSkipped: 0,
      startedAt: Date.now() 
    };
  }

  public async shutdown(): Promise<void> {
    if (this.inviteRefresherInterval) {
      clearInterval(this.inviteRefresherInterval);
      this.inviteRefresherInterval = null;
    }
    if (this.creationCacheCleanupInterval) {
      clearInterval(this.creationCacheCleanupInterval);
      this.creationCacheCleanupInterval = null;
    }

    this.log.info(`Shutting down ${this.accountLabel}...`);
    this.logStats();
  }
}

export default GiveawayManager;
