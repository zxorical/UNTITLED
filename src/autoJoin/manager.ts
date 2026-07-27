/**
 * @module autoJoin/manager
 * 
 * GiveawayManager - Production stable
 * 
 * Key features:
 * - LRU caches (bounded)
 * - Global queue (shared across managers)
 * - Aggressive Discord cache clearing
 * - Clean shutdown with event removal
 */

import { Client, Message, TextChannel, Options } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import axios, { AxiosInstance } from 'axios';
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
} from '../utils.js';
import {
  getAutoJoinEntry,
  saveAutoJoinEntry,
  updateAutoJoinEntryStatus,
  incrementTokenEntries,
  incrementTokenWins,
  updateTokenLastUsed,
  batchSaveJoinOutcomes,
} from '../database.js';
import { CONFIG } from '../config.js';

// ---------------------------------------------------------------------------
// HTTP Client - Shared
// ---------------------------------------------------------------------------

const http: AxiosInstance = axios.create({
  baseURL: 'https://discord.com/api/v10',
  timeout: 10000,
  httpsAgent: new https.Agent({
    keepAlive: true,
    maxSockets: 10,
    maxFreeSockets: 5,
  }),
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GiveawayEntry {
  entryId: string;
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
  correlationId: string;
}

interface GiveawayButton {
  customId: string;
  label: string;
  disabled: boolean;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ENTRY_TTL_MS = 2 * 60 * 60 * 1000;
const WIN_DEDUP_TTL_MS = 30 * 60 * 1000;
const PROCESSED_TTL_MS = 5 * 60 * 1000;
const MAX_PROCESSED = 5000;
const MAX_DB_CHECKED = 5000;
const MAX_RETRIES = 3;
const MAX_QUEUE_SIZE = 1000;
const MAX_CONCURRENT = 3;

const KNOWN_BOTS = new Set([
  '294882584201003009', '739448630517039104', '515195524879237130',
  '235148962103951360', '282859044593598464', '270904126974590976',
  '508391840525975553', '530082442967646230',
]);

const TRUSTED_CUSTOM_IDS = new Set([
  'giveaway_message', 'giveaway-enter', 'enter_giveaway',
  'giveaway_enter', 'join_giveaway', 'giveaway-join',
]);

const BLOCKED_LABELS = [
  /\bleave\b/i, /\bquit\b/i, /\bexit\b/i, /\bunenter\b/i,
  /\bwithdraw\b/i, /remove\s+entry/i, /cancel\s+entry/i,
  /cancel\s+giveaway/i, /end\s+giveaway/i,
];

const ENTRY_PATTERNS = [
  /\benter\b/i, /\bjoin\b/i, /\bparticipate\b/i,
  /\braffle\b/i, /\bsweepstakes\b/i, /\bsubmit\b/i,
  /count\s+me\s+in/i, /\bgiveaway\b/i,
  /🎉/, /🎁/, /🏆/, /^\d[\d,]*$/,
];

const BLOCKED_CONTENT = [
  /already\s+entered\s+this\s+giveaway/i,
  /you(?:'ve|\s+have)\s+already\s+entered/i,
  /you\s+are\s+already\s+(?:in|entered|participating)/i,
  /you(?:'ve|\s+have)\s+already\s+(?:joined|joined\s+this)/i,
  /leave\s+giveaway/i,
];

const WIN_PATTERNS = [
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
// Global Queue
// ---------------------------------------------------------------------------

class GlobalQueue {
  private queue: Array<{ managerId: string; entry: GiveawayEntry }> = [];
  private active = 0;
  private running = false;

  enqueue(manager: GiveawayManager, entry: GiveawayEntry): void {
    if (this.queue.length >= MAX_QUEUE_SIZE) {
      this.queue.shift();
    }
    this.queue.push({ managerId: manager.getId(), entry });
    this.drain();
  }

  private drain(): void {
    if (this.running) return;
    this.running = true;

    while (this.queue.length > 0 && this.active < MAX_CONCURRENT) {
      const item = this.queue.shift()!;
      const manager = GiveawayManager.getManager(item.managerId);
      
      if (!manager || manager.isShuttingDown()) {
        continue;
      }

      this.active++;
      setImmediate(() => {
        manager.executeEntry(item.entry)
          .catch(() => {})
          .finally(() => {
            this.active--;
            this.running = false;
            this.drain();
          });
      });
    }

    this.running = false;
  }

  get stats() {
    return { length: this.queue.length, active: this.active };
  }
}

const globalQueue = new GlobalQueue();

// ---------------------------------------------------------------------------
// Rate Limiter
// ---------------------------------------------------------------------------

class TokenBucket {
  private tokens = 5;
  private lastRefill = Date.now();
  private readonly interval = 1000;

  async consume(): Promise<void> {
    this.refill();
    if (this.tokens <= 0) {
      await delay(this.interval - (Date.now() - this.lastRefill));
      this.refill();
    }
    this.tokens--;
  }

  private refill(): void {
    const now = Date.now();
    const elapsed = now - this.lastRefill;
    const batches = Math.floor(elapsed / this.interval);
    if (batches > 0) {
      this.tokens = Math.min(5, this.tokens + batches * 5);
      this.lastRefill = now;
    }
  }
}

const rateLimiter = new TokenBucket();

// ---------------------------------------------------------------------------
// Cleanup Service
// ---------------------------------------------------------------------------

class CleanupService {
  private managers: Set<GiveawayManager> = new Set();
  private timer: NodeJS.Timeout | null = null;

  register(manager: GiveawayManager): void {
    this.managers.add(manager);
    this.start();
  }

  unregister(manager: GiveawayManager): void {
    this.managers.delete(manager);
    if (this.managers.size === 0) this.stop();
  }

  private start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      for (const m of this.managers) {
        m.prune();
        m.sweepCache();
      }
    }, 60000);
    this.timer.unref();
  }

  private stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

const cleanup = new CleanupService();

// ---------------------------------------------------------------------------
// GiveawayManager
// ---------------------------------------------------------------------------

export class GiveawayManager extends EventEmitter {
  private static instances = new Map<string, GiveawayManager>();

  private readonly client: Client;
  private readonly userId: string;
  private readonly guildId: string;
  private readonly token: string;
  private readonly label: string;
  private readonly id: string;

  // LRU caches
  private processed = new Map<string, number>();
  private dbChecked = new Map<string, number>();
  private wins = new Map<string, number>();
  private entries = new Map<string, GiveawayEntry>();
  private processing = new Set<string>();

  private stats = {
    detected: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    duplicates: 0,
    wins: 0,
    started: Date.now(),
  };

  private shuttingDown = false;

  private constructor(
    client: Client,
    userId: string,
    guildId: string,
    token: string,
    label: string
  ) {
    super();
    this.client = client;
    this.userId = userId;
    this.guildId = guildId;
    this.token = token;
    this.label = label;
    this.id = `${userId}:${guildId}`;

    cleanup.register(this);
    this.sweepCache();
  }

  // ---------------------------------------------------------------------------
  // Factory
  // ---------------------------------------------------------------------------

  static create(
    client: Client,
    userId: string,
    guildId: string,
    token: string,
    label = 'main'
  ): GiveawayManager {
    const id = `${userId}:${guildId}`;
    const existing = this.instances.get(id);
    if (existing && !existing.shuttingDown) return existing;

    if (existing) {
      existing.shutdown().catch(() => {});
      this.instances.delete(id);
    }

    const manager = new GiveawayManager(client, userId, guildId, token, label);
    this.instances.set(id, manager);
    return manager;
  }

  static getManager(id: string): GiveawayManager | undefined {
    return this.instances.get(id);
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  getId(): string { return this.id; }
  isShuttingDown(): boolean { return this.shuttingDown; }

  async handleMessage(message: Message): Promise<void> {
    if (!this.shouldProcess(message)) return;

    const id = this.makeId(message);

    if (this.isDuplicate(id)) return;
    if (await this.isInDatabase(message)) {
      this.markProcessed(id);
      return;
    }

    const detection = await this.detect(message);
    if (!detection) {
      this.markProcessed(id);
      return;
    }

    const entry = this.buildEntry(message, detection, id);
    await this.save(entry);
    this.queue(entry);
  }

  async handleWin(message: Message): Promise<boolean> {
    if (!message.guild || !message.author?.bot) return false;
    if (!this.isMentioned(message)) return false;
    if (!this.hasWinText(message)) return false;
    return this.processWin(message, 'guild');
  }

  async handleDmWin(message: Message): Promise<boolean> {
    if (message.guild) return false;
    if (!this.hasWinText(message)) return false;
    return this.processWin(message, 'dm');
  }

  async executeEntry(entry: GiveawayEntry): Promise<void> {
    if (this.shuttingDown) return;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        await delay(exponentialBackoff(attempt - 1, 2000, 30000));
      }

      try {
        const skipped = await this.tryEntry(entry);
        if (skipped) {
          this.stats.skipped++;
          await this.markStatus(entry, 'skipped');
          return;
        }

        this.stats.succeeded++;
        await this.markStatus(entry, 'success');
        await incrementTokenEntries(this.userId, this.guildId);
        await updateTokenLastUsed(this.userId, this.guildId);

        logger.info('✅ Entered giveaway', {
          account: this.label,
          prize: truncate(entry.prize, 60),
          attempts: attempt + 1,
        });

        this.emit('giveawayEntered', entry);
        return;

      } catch (error) {
        const err = formatError(error);
        logger.warn(`Attempt ${attempt + 1} failed`, {
          account: this.label,
          error: err,
        });

        if (attempt === MAX_RETRIES) {
          this.stats.failed++;
          await this.markStatus(entry, 'failed', err);
          this.emit('giveawayFailed', entry);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Core Logic - Split into small functions
  // ---------------------------------------------------------------------------

  private shouldProcess(message: Message): boolean {
    if (!message.guild) return false;
    if (message.author?.id === this.client.user?.id) return false;
    if (this.shuttingDown) return false;
    
    if (CONFIG.monitoredChannels.length > 0 &&
        !CONFIG.monitoredChannels.includes(message.channel.id)) {
      return false;
    }
    return true;
  }

  private isDuplicate(id: string): boolean {
    if (this.entries.has(id) || this.processed.has(id) || this.processing.has(id)) {
      this.stats.duplicates++;
      return true;
    }
    this.processing.add(id);
    return false;
  }

  private async isInDatabase(message: Message): Promise<boolean> {
    const id = this.makeId(message);
    if (this.dbChecked.has(id)) return true;

    const existing = await getAutoJoinEntry(this.userId, message.id, message.channel.id);
    if (existing) {
      this.addDbChecked(id);
      return true;
    }
    return false;
  }

  private markProcessed(id: string): void {
    this.addProcessed(id);
    this.addDbChecked(id);
    this.processing.delete(id);
  }

  private async detect(
    message: Message
  ): Promise<{ prize: string; button?: GiveawayButton } | null> {
    const content = message.content || '';

    if (BLOCKED_CONTENT.some(r => r.test(content))) return null;

    const isKnown = this.isKnownBot(message);
    const hasKeyword = this.hasGiveawayKeyword(message);

    if (!isKnown && !hasKeyword) return null;

    let button = this.findButton(message);
    if (button) {
      return { prize: this.getPrize(message), button };
    }

    // Retry once if no components
    if (!(message as any).components?.length) {
      await delay(300);
      try {
        const refreshed = await message.fetch();
        button = this.findButton(refreshed);
        if (button) {
          return { prize: this.getPrize(refreshed), button };
        }
      } catch {}
    }

    return null;
  }

  private buildEntry(
    message: Message,
    detection: { prize: string; button?: GiveawayButton },
    id: string
  ): GiveawayEntry {
    return {
      entryId: id,
      userId: this.userId,
      messageId: message.id,
      channelId: message.channel.id,
      guildId: message.guild!.id,
      authorId: message.author?.id || '',
      guildName: message.guild!.name,
      channelName: (message.channel as any).name || 'unknown',
      prize: detection.prize,
      buttonCustomId: detection.button?.customId,
      detectedAt: Date.now(),
      endsAt: this.getEndTimestamp(message),
      status: 'pending',
      attempts: 0,
      correlationId: uuidv4(),
    };
  }

  private async save(entry: GiveawayEntry): Promise<void> {
    await saveAutoJoinEntry({
      userId: this.userId,
      messageId: entry.messageId,
      channelId: entry.channelId,
      guildId: entry.guildId,
      authorId: entry.authorId,
      guildName: entry.guildName,
      channelName: entry.channelName,
      prize: entry.prize,
      buttonCustomId: entry.buttonCustomId,
      detectedAt: entry.detectedAt,
      endsAt: entry.endsAt,
      status: 'pending',
      attempts: 0,
      expiresAt: Date.now() + ENTRY_TTL_MS,
      correlationId: entry.correlationId,
      detectionConfidence: 0.8,
      detectionReasons: ['giveaway_detected'],
    });

    this.entries.set(entry.entryId, entry);
    this.stats.detected++;
    this.markProcessed(entry.entryId);

    logger.info('🎯 Giveaway detected', {
      account: this.label,
      prize: truncate(entry.prize, 60),
      guild: entry.guildName,
    });

    this.emit('giveawayDetected', entry);
  }

  private queue(entry: GiveawayEntry): void {
    globalQueue.enqueue(this, entry);
  }

  private async tryEntry(entry: GiveawayEntry): Promise<boolean> {
    if (!entry.buttonCustomId) throw new Error('No button ID');

    if (CONFIG.buttonDelayMs) await delay(CONFIG.buttonDelayMs);

    const message = await this.fetchMessage(entry.channelId, entry.messageId);
    if (!message) throw new Error('Message not found');

    const button = this.findButtonById(message, entry.buttonCustomId);
    if (!button || button.disabled) {
      this.stats.skipped++;
      return true;
    }

    await rateLimiter.consume();
    await this.clickButton(message, button);
    return false;
  }

  private async clickButton(message: Message, button: GiveawayButton): Promise<void> {
    const selfbot = message as any;
    if (typeof selfbot.clickButton === 'function') {
      await selfbot.clickButton(button.customId);
      return;
    }

    await this.postInteraction(message, button);
  }

  private async postInteraction(message: Message, button: GiveawayButton): Promise<void> {
    const client = this.client as any;
    const sessionId = client.ws?.shards?.first?.()?.sessionId || 
                     client.sessionId || 
                     Date.now().toString();
    const appId = (message as any).applicationId || message.author?.id;

    if (!appId) throw new Error('No app ID');

    await http.post('/interactions', {
      type: 3,
      nonce: `${Date.now()}${Math.random().toString(36).slice(2, 8)}`,
      guild_id: message.guild?.id || null,
      channel_id: message.channel.id,
      message_id: message.id,
      application_id: appId,
      session_id: sessionId,
      data: { component_type: 2, custom_id: button.customId },
    }, {
      headers: { Authorization: this.token },
    });
  }

  private async markStatus(
    entry: GiveawayEntry,
    status: string,
    error?: string
  ): Promise<void> {
    await updateAutoJoinEntryStatus(
      this.userId,
      entry.messageId,
      entry.channelId,
      status,
      { attempts: entry.attempts + 1, lastError: error }
    );
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private isKnownBot(message: Message): boolean {
    return !!(message.author?.bot && message.author.id && KNOWN_BOTS.has(message.author.id));
  }

  private hasGiveawayKeyword(message: Message): boolean {
    const texts = [
      message.content || '',
      ...message.embeds.slice(0, 1).flatMap(e => [
        e.title || '',
        e.description || '',
        e.footer?.text || '',
        ...(e.fields || []).slice(0, 3).flatMap(f => [f.name, f.value]),
      ]),
    ];
    return texts.some(t => /\bgiveaway\b|\braffle\b|\bsweepstakes\b|\bwin\b|\bprize\b/i.test(t));
  }

  private findButton(message: Message): GiveawayButton | null {
    const components = (message as any).components;
    if (!components?.length) return null;

    for (const row of components) {
      for (const comp of row?.components || []) {
        if (comp.type !== 2 && comp.type !== 'BUTTON') continue;
        if (comp.style === 5 || comp.disabled) continue;

        const id = comp.customId || comp.custom_id;
        const label = (comp.label || '').trim();
        if (!id) continue;

        if (BLOCKED_LABELS.some(r => r.test(label))) continue;
        if (TRUSTED_CUSTOM_IDS.has(id)) {
          return { customId: id, label: label || id, disabled: false };
        }
        if (ENTRY_PATTERNS.some(r => r.test(label))) {
          return { customId: id, label: label || 'Enter', disabled: false };
        }
      }
    }
    return null;
  }

  private findButtonById(message: Message, customId: string): GiveawayButton | null {
    const components = (message as any).components;
    if (!components?.length) return null;

    for (const row of components) {
      for (const comp of row?.components || []) {
        const id = comp.customId || comp.custom_id;
        if (id === customId) {
          return {
            customId: id,
            label: comp.label || '',
            disabled: comp.disabled || false,
          };
        }
      }
    }
    return null;
  }

  private getPrize(message: Message): string {
    const embed = message.embeds?.[0];
    if (embed?.title) return truncate(embed.title, 200);
    if (embed?.description) return truncate(embed.description, 200);
    if (message.content) return truncate(message.content, 200);
    return 'Unknown Prize';
  }

  private getEndTimestamp(message: Message): number | undefined {
    const match = this.extractAllText(message).match(/<t:(\d{10,13})(?::[a-zA-Z])?>/);
    if (!match?.[1]) return undefined;
    const raw = parseInt(match[1], 10);
    const ts = raw < 1e12 ? raw * 1000 : raw;
    return Number.isFinite(ts) && ts > Date.now() ? ts : undefined;
  }

  private extractAllText(message: Message): string {
    return [
      message.content || '',
      ...message.embeds.flatMap(e => [
        e.title || '',
        e.description || '',
        e.footer?.text || '',
        ...(e.fields || []).flatMap(f => [f.name, f.value]),
      ]),
    ].join(' ');
  }

  private isMentioned(message: Message): boolean {
    const myId = this.client.user?.id;
    if (!myId) return false;
    return (message.mentions?.users?.has(myId) ?? false) ||
           (message.content || '').includes(myId);
  }

  private hasWinText(message: Message): boolean {
    return WIN_PATTERNS.some(r => r.test(this.extractAllText(message)));
  }

  private async processWin(message: Message, source: 'guild' | 'dm'): Promise<boolean> {
    const key = `${message.channel.id}:${message.author?.id || 'unknown'}`;
    if (this.wins.has(key)) {
      const last = this.wins.get(key)!;
      if (Date.now() - last < WIN_DEDUP_TTL_MS) return false;
    }
    this.wins.set(key, Date.now());

    const prize = this.getPrize(message);
    this.stats.wins++;

    await incrementTokenWins(this.userId, this.guildId);

    const sourceName = source === 'dm'
      ? 'Direct Message'
      : `#${(message.channel as any).name || message.channel.id} in ${message.guild?.name || 'unknown'}`;

    logger.info('🏆 WIN DETECTED!', {
      account: this.label,
      source: sourceName,
      prize,
    });

    await this.sendWebhook(message, prize, sourceName);
    this.emit('giveawayWon', { message, prize, source: sourceName });

    return true;
  }

  private async fetchMessage(channelId: string, messageId: string): Promise<Message | null> {
    try {
      const channel = await this.client.channels.fetch(channelId);
      if (!channel || !('messages' in channel)) return null;
      const msg = await (channel as TextChannel).messages.fetch(messageId);
      
      // Clear from cache immediately
      try {
        if (channel.messages?.cache) {
          channel.messages.cache.delete(messageId);
        }
      } catch {}
      
      return msg;
    } catch {
      return null;
    }
  }

  private makeId(message: Message): string {
    return `${message.channel.id}:${message.id}`;
  }

  // ---------------------------------------------------------------------------
  // LRU Caches
  // ---------------------------------------------------------------------------

  private addProcessed(id: string): void {
    if (this.processed.size >= MAX_PROCESSED) {
      const first = this.processed.keys().next().value;
      if (first) this.processed.delete(first);
    }
    this.processed.set(id, Date.now());
  }

  private addDbChecked(id: string): void {
    if (this.dbChecked.size >= MAX_DB_CHECKED) {
      const first = this.dbChecked.keys().next().value;
      if (first) this.dbChecked.delete(first);
    }
    this.dbChecked.set(id, Date.now());
  }

  // ---------------------------------------------------------------------------
  // Housekeeping
  // ---------------------------------------------------------------------------

  prune(): void {
    const cutoff = Date.now() - PROCESSED_TTL_MS;
    
    for (const [k, t] of this.processed) {
      if (t < cutoff) this.processed.delete(k);
    }
    for (const [k, t] of this.dbChecked) {
      if (t < cutoff) this.dbChecked.delete(k);
    }
    for (const [k, t] of this.wins) {
      if (t < cutoff) this.wins.delete(k);
    }
  }

  sweepCache(): void {
    try {
      if (this.client.channels?.cache) {
        const before = this.client.channels.cache.size;
        this.client.channels.cache.sweep(
          (c: any) => (c?.createdTimestamp || 0) < Date.now() - 300000
        );
      }
      
      const selfId = this.client.user?.id;
      if (this.client.users?.cache && selfId) {
        this.client.users.cache.sweep((u: any) => u.id !== selfId);
        if (this.client.users.cache.size > 10) {
          const keys = Array.from(this.client.users.cache.keys());
          for (let i = 0; i < keys.length - 10; i++) {
            this.client.users.cache.delete(keys[i]);
          }
        }
      }
    } catch {}
  }

  // ---------------------------------------------------------------------------
  // Webhooks
  // ---------------------------------------------------------------------------

  private async sendWebhook(message: Message, prize: string, source: string): Promise<void> {
    const url = CONFIG.winWebhookUrl || CONFIG.webhookUrl;
    if (!url) return;

    const jump = message.guild
      ? `https://discord.com/channels/${message.guild.id}/${message.channel.id}/${message.id}`
      : null;

    try {
      await http.post(url, {
        content: '@everyone',
        username: '🎉 AutoJoin WIN',
        embeds: [{
          title: '🏆 GIVEAWAY WIN!',
          description: jump ? `[Jump to message](${jump})` : 'Won via DM',
          color: 0xFFD700,
          fields: [
            { name: '🎁 Prize', value: prize || 'Unknown', inline: false },
            { name: '🏠 Server', value: message.guild?.name || 'DM', inline: true },
            { name: '📢 Source', value: source, inline: true },
            { name: '👤 Account', value: this.label, inline: true },
          ],
          footer: { text: `AutoJoin • ${this.label}` },
          timestamp: new Date().toISOString(),
        }],
      }, { timeout: 8000 });
    } catch (error) {
      logger.warn('Webhook failed', { account: this.label, error: formatError(error) });
    }
  }

  // ---------------------------------------------------------------------------
  // Shutdown
  // ---------------------------------------------------------------------------

  async shutdown(): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;

    cleanup.unregister(this);
    GiveawayManager.instances.delete(this.id);

    this.entries.clear();
    this.processing.clear();
    this.processed.clear();
    this.dbChecked.clear();
    this.wins.clear();

    this.removeAllListeners();

    logger.info('Shutdown complete', { account: this.label });
  }

  // ---------------------------------------------------------------------------
  // Stats
  // ---------------------------------------------------------------------------

  getStats() {
    return {
      ...this.stats,
      entries: this.entries.size,
      processed: this.processed.size,
      queue: globalQueue.stats,
    };
  }
}

export const getQueueStats = () => globalQueue.stats;
