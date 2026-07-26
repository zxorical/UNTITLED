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
  /you can edit this/i,
  /you can change/i,
  /setup your giveaway/i,
  /create(?: a)? giveaway/i,
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

  // Track recently seen draft messages to prevent re-processing
  private draftMessageCache = new Set<string>();
  private readonly DRAFT_CACHE_MAX_SIZE = 100;

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

    if (!message.author?.bot) return;

    const key = `${message.id}-${message.channel.id}`;
    if (this.processingMessages.has(key)) {
      return;
    }
    this.processingMessages.add(key);

    try {
      // CHECK 1: Is this from an allowed giveaway bot?
      if (!ALLOWED_GIVEAWAY_BOT_IDS.has(message.author.id)) {
        return;
      }

      // CHECK 2: Is this a draft giveaway? (check BEFORE content filters)
      if (this.isDraftGiveaway(message)) {
        this.stats.draftsSkipped++;
        // Add to draft cache so we don't process it again
        if (this.draftMessageCache.size >= this.DRAFT_CACHE_MAX_SIZE) {
          // Remove oldest entry if cache is full
          const firstKey = this.draftMessageCache.values().next().value;
          if (firstKey) this.draftMessageCache.delete(firstKey);
        }
        this.draftMessageCache.add(key);
        this.log.debug('Skipping draft giveaway creation message', {
          messageId: message.id,
          channelId: message.channel.id,
        });
        return;
      }

      // CHECK 3: Filter out blocked content
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

      // CHECK 4: Existing giveaway?
      const existing = await getGiveaway(message.id, message.channel.id);
      if (existing) {
        await updateLastSeen(message.id, message.channel.id);
        if (existing.status === 'active' && this.isEnded(message)) {
          await markEnded(message.id, message.channel.id);
        }
        return;
      }

      // CHECK 5: Actually detect the giveaway
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
  // OPTIMIZED Watchlist Matching
  // -------------------------------------------------------------------------
  private async checkWatchlistMatches(message: Message, prize: string, inviteUrl: string): Promise<void> {
    if (!this.botManager) return;

    try {
      const watchlistData = await this.getCachedWatchlists();
      if (watchlistData.size === 0) return;

      const text = this.getCachedGiveawayText(message);
      const lowerText = text.toLowerCase();

      // Early exit - check if ANY item matches
      const allItems = Array.from(watchlistData.values()).flat();
      const hasAnyMatch = allItems.some(item => lowerText.includes(item.toLowerCase()));
      if (!hasAnyMatch) return;

      // Find matching users
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

      // SEND DMS WITH OPTIMAL BATCHING
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
  // Draft Giveaway Detection - IMPROVED
  // -------------------------------------------------------------------------
  
  /**
   * Check if this is a draft/pending giveaway creation message
   * More aggressive detection to prevent false positives
   */
  private isDraftGiveaway(message: Message): boolean {
    const content = message.content || '';
    const embed = message.embeds?.[0];
    const components = (message as any).components as any[] | undefined;
    
    // STEP 1: Check for draft indicators in content
    if (DRAFT_GIVEAWAY_INDICATORS.some(re => re.test(content))) {
      return true;
    }

    // STEP 2: Check embed text for draft indicators
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

      // STEP 3: Check if the embed has giveaway-like elements but NO timestamp
      // This helps catch drafts that don't have specific draft text
      const hasGiveawayKeywords = GIVEAWAY_KEYWORDS.some(re => re.test(embedText));
      const hasTimestamp = /<t:\d+:[a-zA-Z]>/.test(embedText);
      
      if (hasGiveawayKeywords && !hasTimestamp && components) {
        // If it has giveaway keywords but no timestamp, check for management buttons
        const hasManagementButton = this.hasDraftManagementButton(message);
        const hasEntryButton = this.hasEntryButton(message);
        
        // If there's a management button and no entry button, it's likely a draft
        if (hasManagementButton && !hasEntryButton) {
          return true;
        }
      }
    }

    // STEP 4: Check components for draft management buttons
    if (components) {
      let managementButtons = 0;
      let entryButtons = 0;
      
      for (const row of components) {
        const comps = row.components as any[] | undefined;
        if (!comps) continue;
        
        for (const comp of comps) {
          if (comp.type !== 2 || comp.style === 5) continue;
          
          const label = (comp.label || '').toLowerCase().trim();
          
          // Count management buttons
          if (['start', 'edit', 'cancel', 'preview', 'setup'].includes(label)) {
            managementButtons++;
          }
          
          // Count entry buttons
          if (this.isEntryButton(comp)) {
            entryButtons++;
          }
        }
      }
      
      // If there are management buttons but no entry buttons, it's a draft
      if (managementButtons > 0 && entryButtons === 0) {
        return true;
      }
      
      // If all buttons are management buttons, it's a draft
      if (managementButtons > 0 && managementButtons >= this.countTotalButtons(message)) {
        return true;
      }
    }

    // STEP 5: Check if the message looks like a setup/creation message
    // Messages with certain patterns are likely draft setup messages
    const setupPatterns = [
      /select\s+a?\s*channel/i,
      /set\s+(?:the\s+)?(?:prize|duration|winners?)/i,
      /choose\s+(?:a|the|your)\s+(?:prize|channel|duration)/i,
      /pick\s+(?:a|the|your)\s+(?:prize|channel|duration)/i,
    ];
    
    if (setupPatterns.some(re => re.test(content))) {
      return true;
    }
    
    if (embed) {
      const embedText = [
        embed.title || '',
        embed.description || '',
        ...(embed.fields || []).flatMap(f => [f.name, f.value]),
      ].join(' ');
      
      if (setupPatterns.some(re => re.test(embedText))) {
        return true;
      }
    }

    return false;
  }

  /**
   * Count total non-link buttons in a message
   */
  private countTotalButtons(message: Message): number {
    const components = (message as any).components as any[] | undefined;
    if (!components) return 0;
    
    let count = 0;
    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2 && comp.style !== 5) {
          count++;
        }
      }
    }
    return count;
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
          if (this.isEntryButton(comp)) return true;
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

    // Skip management buttons
    const lowerLabel = label.toLowerCase();
    if (['start', 'edit', 'cancel', 'preview', 'setup'].includes(lowerLabel)) {
      return false;
    }

    if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) return true;
    if (ENTRY_BUTTON_LABEL_PATTERNS.some(re => re.test(label))) return true;
    
    // Check for giveaway entry emojis
    for (const emoji of ENTRY_EMOJI_PATTERNS) {
      if (label.includes(emoji)) return true;
    }

    return false;
  }

  /**
   * Check if message has a Start/Edit/Cancel/Preview/Setup button
   */
  private hasDraftManagementButton(message: Message): boolean {
    const components = (message as any).components as any[] | undefined;
    if (!components) return false;

    for (const row of components) {
      const comps = row.components as any[] | undefined;
      if (!comps) continue;
      for (const comp of comps) {
        if (comp.type === 2 && comp.style !== 5) {
          const label = (comp.label || '').toLowerCase().trim();
          if (['start', 'edit', 'cancel', 'preview', 'setup'].includes(label)) {
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
    // Final draft check - if we somehow got here with a draft, reject it
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
        const refreshed = await message.fetch();
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

    // Quick draft check before collecting signals
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

    // First check if this has draft buttons - if so, skip
    if (this.hasDraftManagementButton(message) && !this.hasEntryButton(message)) {
      return null;
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
        
        // Skip management buttons
        if (['edit', 'start', 'cancel', 'preview', 'setup'].includes(lowerLabel)) {
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

    this.log.info(`Shutting down ${this.accountLabel}...`);
    this.logStats();
  }
}

export default GiveawayManager;
