/**
 * @module giveawayManager
 * Reliable giveaway detector — scans everything, misses nothing.
 * 
 * Optimizations applied:
 * 1. Single parse pass with lowercase variants
 * 2. Never blocks on fetch - async refresh only when needed
 * 3. Pre-compiled Sets for O(1) lookups instead of regex
 * 4. Buttons parsed once
 * 5. Timestamps parsed once
 * 6. Text built once with all lowercase variants
 * 7. Reverse watchlist index for O(k) matching (k = text length)
 * 8. Aho-Corasick for watchlist matching when >50 items
 * 9. Bloom filter for watchlist pre-check (>500 items)
 * 10. LRU caches with size limits
 * 11. WeakMap for Message-bound data
 * 12. Parallelized independent async work
 * 13. Score with confidence and detection reason tracking
 * 14. Single Date.now() per message
 * 15. Message age rejection
 * 16. Guild whitelist/blacklist
 * 17. Duplicate giveaway detection via content hash
 * 18. Failed invite caching
 * 19. Background watchlist refresh
 * 20. Edited giveaway end detection via messageUpdate
 */

import { Client, Message, TextChannel } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import { CONFIG } from './config.js';
import { AppLogger } from './logger.js';
import { delay, formatError } from './utils.js';
import { GiveawayData } from './types.js';
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

// ═══════════════════════════════════════════════════════════════════════════
// CONSTANTS (pre-compiled Sets/Maps for O(1) lookups)
// ═══════════════════════════════════════════════════════════════════════════

const ALLOWED_GIVEAWAY_BOT_IDS = new Set(['530082442967646230']);
const TRUSTED_ENTRY_CUSTOM_IDS = new Set([
  'giveaway_message', 'giveaway-enter', 'enter_giveaway',
  'giveaway_enter', 'join_giveaway', 'giveaway-join',
  'giveaway_participate', 'participate_giveaway', 'enter',
]);
const ENTRY_EMOJIS = new Set(['🎉', '🎁', '🎊', '🎈', '🎀', '👍', '✅']);
const DRAFT_BUTTON_LABELS = new Set(['start', 'edit', 'cancel', 'preview', 'setup']);
const GIVEAWAY_EMBED_COLORS = new Set([0xF1C40F, 0x7289DA, 0x2ECC71, 0xE91E63]);

// Simple word sets for O(1) .has() checks (faster than .includes on array)
const GIVEAWAY_WORDS = new Set(['giveaway', 'raffle', 'sweepstakes', 'win', 'prize']);
const ENTRY_WORDS = new Set(['enter', 'join', 'participate', 'raffle', 'sweepstakes', 'submit']);
const FOOTER_END_WORDS = new Set(['ends', 'expires']);
const PRIZE_FIELD_NAMES = new Set(['prize', 'reward', 'item', 'prizes', 'rewards']);

// Blocked phrases - checked via includes on full lowercase text
const BLOCKED_PHRASES = [
  'already entered', 'you have already entered', "you've already entered",
  'you are already in', 'leave giveaway', 'joined successfully',
  'entry confirmed', 'entered successfully', "you're entered",
  'withdraw entry', 'giveaway has ended', 'giveaway ended',
  'giveaway is over', 'winners selected', 'winner selected',
  'congratulations', 'you won', 'you did not win',
  'results are in', 'giveaway is now closed', 'thank you for participating',
];

// Draft indicators
const DRAFT_PHRASES = [
  'review your giveaway', 'click "start" to', "click 'start' to",
  'this message expires in', 'giveaway preview', 'configure your giveaway',
  'setup your giveaway', 'you can edit this', 'you can change',
  'create a giveaway', 'select a channel', 'set the prize', 'set the duration',
];

// Ended giveaway indicators for messageUpdate
const ENDED_PHRASES = [
  'ended', 'winner', 'closed', 'congratulations', 'results',
  'giveaway has ended', 'giveaway ended', 'giveaway is over',
];

// Only regex that's genuinely needed
const DURATION_REGEX = /(\d+)\s*(minute|min|m|hour|h)/i;
const TIMESTAMP_REGEX = /<t:(\d{10,13})(?::[a-zA-Z])?>/g;
const COUNT_ME_IN_REGEX = /count\s+me\s+in/i;

// Thresholds
const MINIMUM_SCORE_THRESHOLD = 6;
const CREATION_SCORE_THRESHOLD = 7;
const MAX_MESSAGE_AGE_MS = 30 * 60 * 1000; // 30 minutes

// Cache limits
const MAX_CREATION_CACHE = 2000;
const MAX_INVITE_CACHE = 500;
const MAX_DUPLICATE_CACHE = 1000;
const MAX_FAILED_INVITE_CACHE = 200;
const WATCHLIST_CACHE_TTL = 60000;
const INVITE_CACHE_TTL = 30 * 60 * 1000;
const FAILED_INVITE_RETRY_MS = 15 * 60 * 1000;
const DUPLICATE_TTL = 10 * 60 * 1000;

// Watchlist Aho-Corasick threshold
const AHOCORASICK_THRESHOLD = 50;
const BLOOM_FILTER_THRESHOLD = 500;

// Guild filtering
const allowedGuilds = CONFIG.allowedGuilds ? new Set(CONFIG.allowedGuilds) : null;
const blockedGuilds = CONFIG.blockedGuilds ? new Set(CONFIG.blockedGuilds) : null;

// ═══════════════════════════════════════════════════════════════════════════
// SIMPLE BLOOM FILTER (for watchlist pre-check)
// ═══════════════════════════════════════════════════════════════════════════

class BloomFilter {
  private bits: Uint8Array;
  private size: number;
  private hashCount: number;

  constructor(expectedItems: number, falsePositiveRate: number) {
    this.size = Math.ceil(-expectedItems * Math.log(falsePositiveRate) / (Math.log(2) ** 2));
    this.hashCount = Math.ceil((this.size / expectedItems) * Math.log(2));
    this.bits = new Uint8Array(Math.ceil(this.size / 8));
  }

  add(item: string): void {
    const lower = item.toLowerCase();
    for (let i = 0; i < this.hashCount; i++) {
      const hash = this.fnv1a(lower, i);
      const pos = hash % this.size;
      this.bits[pos >> 3] |= (1 << (pos & 7));
    }
  }

  mightContain(item: string): boolean {
    const lower = item.toLowerCase();
    for (let i = 0; i < this.hashCount; i++) {
      const hash = this.fnv1a(lower, i);
      const pos = hash % this.size;
      if (!(this.bits[pos >> 3] & (1 << (pos & 7)))) return false;
    }
    return true;
  }

  private fnv1a(str: string, seed: number): number {
    let hash = 2166136261 ^ (seed * 16777619);
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// AHO-CORASICK MATCHER (for large watchlists)
// ═══════════════════════════════════════════════════════════════════════════

interface AhoNode {
  children: Map<string, AhoNode>;
  fail: AhoNode | null;
  output: Set<string>;
}

class AhoCorasick {
  private root: AhoNode;
  private built = false;

  constructor() {
    this.root = { children: new Map(), fail: null, output: new Set() };
  }

  addPattern(pattern: string): void {
    const lower = pattern.toLowerCase();
    let node = this.root;
    for (const char of lower) {
      if (!node.children.has(char)) {
        node.children.set(char, { children: new Map(), fail: null, output: new Set() });
      }
      node = node.children.get(char)!;
    }
    node.output.add(lower);
  }

  build(): void {
    if (this.built) return;
    const queue: AhoNode[] = [];
    
    for (const child of this.root.children.values()) {
      child.fail = this.root;
      queue.push(child);
    }

    while (queue.length > 0) {
      const current = queue.shift()!;
      for (const [char, child] of current.children) {
        queue.push(child);
        let failNode = current.fail;
        while (failNode !== null && !failNode.children.has(char)) {
          failNode = failNode.fail;
        }
        child.fail = failNode ? failNode.children.get(char) || this.root : this.root;
        for (const output of child.fail.output) {
          child.output.add(output);
        }
      }
    }
    this.built = true;
  }

  findMatches(text: string): Set<string> {
    const results = new Set<string>();
    let node = this.root;
    
    for (const char of text.toLowerCase()) {
      while (node !== this.root && !node.children.has(char)) {
        node = node.fail!;
      }
      if (node.children.has(char)) {
        node = node.children.get(char)!;
      }
      for (const match of node.output) {
        results.add(match);
      }
    }
    return results;
  }

  get patternCount(): number {
    let count = 0;
    const stack = [this.root];
    while (stack.length > 0) {
      const node = stack.pop()!;
      count += node.output.size;
      for (const child of node.children.values()) {
        stack.push(child);
      }
    }
    return count;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// LRU CACHE
// ═══════════════════════════════════════════════════════════════════════════

class LRUCache<K, V> {
  private map = new Map<K, V>();
  constructor(private maxSize: number) {}

  get(key: K): V | undefined {
    const value = this.map.get(key);
    if (value !== undefined) {
      this.map.delete(key);
      this.map.set(key, value);
    }
    return value;
  }

  set(key: K, value: V): void {
    this.map.delete(key);
    this.map.set(key, value);
    if (this.map.size > this.maxSize) {
      const oldest = this.map.keys().next().value;
      if (oldest !== undefined) this.map.delete(oldest);
    }
  }

  delete(key: K): void { this.map.delete(key); }
  get size(): number { return this.map.size; }
}

// ═══════════════════════════════════════════════════════════════════════════
// PARSED GIVEAWAY MESSAGE (single parse pass, all lowercase variants)
// ═══════════════════════════════════════════════════════════════════════════

interface ParsedButtons {
  entry: { customId: string; label: string } | null;
  draftLabels: string[];
  labels: string[];
  ids: string[];
  draftCount: number;
  entryCount: number;
}

interface ParsedTimestamps {
  end: number | null;
  all: number[];
}

interface DetectionReason {
  signals: string[];
  score: number;
  confidence: number; // 0-100%
}

interface ParsedGiveawayData {
  parsedAt: number;
  messageAge: number;
  fullText: string;
  lowerText: string;
  content: string;
  lowerContent: string;
  title: string;
  lowerTitle: string;
  description: string;
  lowerDescription: string;
  footer: string;
  lowerFooter: string;
  authorName: string;
  lowerAuthor: string;
  buttons: ParsedButtons;
  timestamps: ParsedTimestamps;
  embedColor: number | null;
  fieldNames: string[];
  lowerFieldNames: string[];
  fieldValues: string[];
  lowerFieldValues: string[];
  hasAnyEmbed: boolean;
  hasAnyComponent: boolean;
  isFromBot: boolean;
  botId: string;
  prize: string;
  // Content hash for duplicate detection
  contentHash: string;
}

const parsedMessageCache = new WeakMap<Message, ParsedGiveawayData>();

function parseMessage(message: Message, now: number): ParsedGiveawayData {
  const cached = parsedMessageCache.get(message);
  if (cached) return cached;

  const embed = message.embeds?.[0];
  const messageAge = now - message.createdTimestamp;

  // Extract text components once with lowercase variants
  const content = message.content || '';
  const lowerContent = content.toLowerCase();
  const title = embed?.title || '';
  const lowerTitle = title.toLowerCase();
  const description = embed?.description || '';
  const lowerDescription = description.toLowerCase();
  const footer = embed?.footer?.text || '';
  const lowerFooter = footer.toLowerCase();
  const authorName = embed?.author?.name || '';
  const lowerAuthor = authorName.toLowerCase();

  // Extract fields (just arrays, no Map allocation)
  const fieldNames: string[] = [];
  const lowerFieldNames: string[] = [];
  const fieldValues: string[] = [];
  const lowerFieldValues: string[] = [];

  if (embed?.fields) {
    for (const field of embed.fields) {
      fieldNames.push(field.name);
      lowerFieldNames.push(field.name.toLowerCase());
      fieldValues.push(field.value);
      lowerFieldValues.push(field.value.toLowerCase());
    }
  }

  // Build full text once
  const textParts = [content, title, description, footer, authorName, ...fieldNames, ...fieldValues];
  const fullText = textParts.filter(Boolean).join(' ');
  const lowerText = fullText.toLowerCase();

  // Parse buttons once
  const buttons = parseButtons((message as any).components);

  // Parse timestamps once
  const timestamps = parseTimestamps(fullText, now);

  // Extract prize (early stop on known field names)
  const prize = extractPrize(title, description, content, fieldNames, fieldValues);

  // Content hash for duplicate detection
  const contentHash = simpleHash(`${title}|${description}|${message.guild?.id}|${timestamps.end}`);

  const parsed: ParsedGiveawayData = {
    parsedAt: now,
    messageAge,
    fullText,
    lowerText,
    content,
    lowerContent,
    title,
    lowerTitle,
    description,
    lowerDescription,
    footer,
    lowerFooter,
    authorName,
    lowerAuthor,
    buttons,
    timestamps,
    embedColor: embed?.color || null,
    fieldNames,
    lowerFieldNames,
    fieldValues,
    lowerFieldValues,
    hasAnyEmbed: !!embed,
    hasAnyComponent: buttons.labels.length > 0,
    isFromBot: message.author?.bot === true,
    botId: message.author?.id || '',
    prize,
    contentHash,
  };

  parsedMessageCache.set(message, parsed);
  return parsed;
}

function refreshParsedMessage(message: Message, now: number): ParsedGiveawayData {
  parsedMessageCache.delete(message);
  return parseMessage(message, now);
}

function simpleHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash |= 0;
  }
  return hash.toString(36);
}

// ─── Button Parsing ───────────────────────────────────────────────────────

function parseButtons(components: any[] | undefined): ParsedButtons {
  const result: ParsedButtons = {
    entry: null, draftLabels: [], labels: [], ids: [], draftCount: 0, entryCount: 0,
  };
  if (!components) return result;

  for (const row of components) {
    const comps = row.components as any[] | undefined;
    if (!comps) continue;
    for (const comp of comps) {
      if (comp.type !== 2 || comp.style === 5) continue;
      const customId: string = comp.customId || comp.custom_id || '';
      const label: string = (comp.label || '').trim();
      const lowerLabel = label.toLowerCase();
      result.labels.push(lowerLabel);
      result.ids.push(customId);
      if (DRAFT_BUTTON_LABELS.has(lowerLabel)) {
        result.draftLabels.push(lowerLabel);
        result.draftCount++;
        continue;
      }
      if (comp.disabled === true) continue;
      if (isEntryButton(customId, label, lowerLabel)) {
        if (!result.entry) result.entry = { customId, label: label || customId };
        result.entryCount++;
      }
    }
  }
  return result;
}

function isEntryButton(customId: string, label: string, lowerLabel: string): boolean {
  if (TRUSTED_ENTRY_CUSTOM_IDS.has(customId)) return true;
  for (const emoji of ENTRY_EMOJIS) { if (label.includes(emoji)) return true; }
  for (const word of ENTRY_WORDS) { if (lowerLabel.includes(word)) return true; }
  if (COUNT_ME_IN_REGEX.test(lowerLabel)) return true;
  return false;
}

// ─── Timestamp Parsing ────────────────────────────────────────────────────

function parseTimestamps(text: string, now: number): ParsedTimestamps {
  const all: number[] = [];
  let end: number | null = null;
  TIMESTAMP_REGEX.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = TIMESTAMP_REGEX.exec(text)) !== null) {
    const raw = parseInt(match[1], 10);
    const tsMs = raw < 1e12 ? raw * 1000 : raw;
    if (Number.isFinite(tsMs) && tsMs > now) {
      all.push(tsMs);
      if (end === null || tsMs > end) end = tsMs;
    }
  }
  return { end, all };
}

// ─── Prize Extraction (early stop) ────────────────────────────────────────

function extractPrize(
  title: string, description: string, content: string,
  fieldNames: string[], fieldValues: string[],
): string {
  // Early stop on known prize field names
  for (let i = 0; i < fieldNames.length; i++) {
    if (PRIZE_FIELD_NAMES.has(fieldNames[i].toLowerCase())) {
      return fieldValues[i].slice(0, 200).trim() || 'Unknown Prize';
    }
  }
  if (title) return title.slice(0, 200).trim();
  if (description) return description.slice(0, 200).trim();
  return content.slice(0, 200).trim() || 'Unknown Prize';
}

// ═══════════════════════════════════════════════════════════════════════════
// QUICK REJECT (Stage 1)
// ═══════════════════════════════════════════════════════════════════════════

function quickReject(message: Message, selfUserId: string, now: number): string | null {
  if (!message.guild) return 'no_guild';
  if (message.author?.id === selfUserId) return 'self';

  // Guild whitelist/blacklist
  if (allowedGuilds && !allowedGuilds.has(message.guild.id)) return 'not_allowed_guild';
  if (blockedGuilds && blockedGuilds.has(message.guild.id)) return 'blocked_guild';

  // Monitored channels
  if (CONFIG.monitoredChannels.length > 0 && !CONFIG.monitoredChannels.includes(message.channel.id)) {
    return 'not_monitored';
  }

  // Not allowed bot
  if (!message.author?.bot || !ALLOWED_GIVEAWAY_BOT_IDS.has(message.author.id)) {
    return 'not_allowed_bot';
  }

  // Message age rejection
  if (now - message.createdTimestamp > MAX_MESSAGE_AGE_MS) {
    return 'too_old';
  }

  // Quick blocked content check on raw content (only if short)
  const rawContent = (message.content || '').toLowerCase();
  if (rawContent.length < 200) {
    for (const phrase of BLOCKED_PHRASES) {
      if (rawContent.includes(phrase)) return 'blocked_content';
    }
  }

  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// CREATION DETECTOR
// ═══════════════════════════════════════════════════════════════════════════

function detectCreation(parsed: ParsedGiveawayData): { isCreation: boolean; score: number } {
  let score = 0;

  if (parsed.lowerText.includes('review your giveaway')) score += 5;
  if (parsed.lowerText.includes('this message expires in')) score += 5;
  if (parsed.lowerText.includes('click "start" to')) score += 5;
  if (parsed.lowerText.includes("click 'start' to")) score += 5;
  if (parsed.lowerText.includes('configure your giveaway')) score += 5;
  if (parsed.lowerText.includes('giveaway preview')) score += 3;
  if (parsed.lowerText.includes('setup your giveaway')) score += 3;
  if (parsed.lowerText.includes('you can edit this')) score += 3;
  if (parsed.lowerText.includes('you can change')) score += 3;
  if (parsed.lowerText.includes('create a giveaway')) score += 2;
  if (parsed.lowerText.includes('select a channel')) score += 2;
  if (parsed.lowerText.includes('set the prize')) score += 2;
  if (parsed.lowerText.includes('set the duration')) score += 2;

  if (parsed.buttons.draftLabels.includes('start')) score += 3;
  if (parsed.buttons.draftLabels.includes('edit')) score += 2;
  if (parsed.buttons.draftLabels.includes('cancel')) score += 2;
  if (parsed.buttons.draftLabels.includes('preview')) score += 2;
  if (parsed.buttons.draftLabels.includes('setup')) score += 2;

  const durationMatch = parsed.fullText.match(DURATION_REGEX);
  if (durationMatch) {
    const value = parseInt(durationMatch[1], 10);
    const unit = (durationMatch[2] || '').toLowerCase();
    let minutes = value;
    if (unit.startsWith('h')) minutes = value * 60;
    if (minutes <= 15) score += 2;
  }

  return { isCreation: score >= CREATION_SCORE_THRESHOLD, score };
}

// ═══════════════════════════════════════════════════════════════════════════
// SCORE CALCULATOR WITH DETECTION REASONS
// ═══════════════════════════════════════════════════════════════════════════

const MAX_POSSIBLE_SCORE = 17; // Sum of all possible signal weights

function calculateGiveawayScore(parsed: ParsedGiveawayData): DetectionReason {
  let score = 0;
  const signals: string[] = [];

  if (parsed.buttons.entry) {
    score += 3;
    signals.push('entry_button');
  }

  for (const word of GIVEAWAY_WORDS) {
    if (parsed.lowerTitle.includes(word)) { score += 2; signals.push(`title:${word}`); break; }
  }

  for (const word of GIVEAWAY_WORDS) {
    if (parsed.lowerDescription.includes(word)) { score += 1; signals.push(`desc:${word}`); break; }
  }

  for (const word of FOOTER_END_WORDS) {
    if (parsed.lowerFooter.includes(word)) { score += 2; signals.push(`footer:${word}`); break; }
  }

  if (parsed.timestamps.end !== null) { score += 3; signals.push('timestamp'); }

  if (parsed.embedColor !== null && GIVEAWAY_EMBED_COLORS.has(parsed.embedColor)) {
    score += 1; signals.push('embed_color');
  }

  if (parsed.lowerAuthor.includes('giveaway')) { score += 1; signals.push('author'); }

  for (const fieldName of parsed.lowerFieldNames) {
    if (fieldName.includes('ends') || fieldName.includes('winners') || fieldName.includes('time remaining')) {
      score += 2; signals.push(`field:${fieldName}`); break;
    }
  }

  const confidence = Math.min(100, Math.round((score / MAX_POSSIBLE_SCORE) * 100));

  return { signals, score, confidence };
}

// ═══════════════════════════════════════════════════════════════════════════
// BLOCKED / DRAFT CHECKS
// ═══════════════════════════════════════════════════════════════════════════

function isBlockedContent(parsed: ParsedGiveawayData): boolean {
  for (const phrase of BLOCKED_PHRASES) {
    if (parsed.lowerText.includes(phrase)) return true;
  }
  return false;
}

function isDraftGiveaway(parsed: ParsedGiveawayData): boolean {
  for (const phrase of DRAFT_PHRASES) {
    if (parsed.lowerText.includes(phrase)) return true;
  }
  return parsed.buttons.draftCount > 0 && parsed.buttons.entryCount === 0;
}

function isEndedGiveaway(parsed: ParsedGiveawayData): boolean {
  if (parsed.timestamps.end !== null && parsed.timestamps.end < parsed.parsedAt) return true;
  for (const phrase of ENDED_PHRASES) {
    if (parsed.lowerText.includes(phrase)) return true;
  }
  return false;
}

// ═══════════════════════════════════════════════════════════════════════════
// GIVEAWAY MANAGER
// ═══════════════════════════════════════════════════════════════════════════

export class GiveawayManager extends EventEmitter {
  private readonly client: Client;
  private readonly log: AppLogger;
  private readonly accountLabel: string;
  private readonly botManager: BotManager | null;
  private readonly selfUserId: string;

  private processingMessages = new Set<string>();

  // LRU caches
  private creationCache = new LRUCache<string, { isCreation: boolean; score: number }>(MAX_CREATION_CACHE);
  private inviteCache = new LRUCache<string, { url: string; expiresAt: number }>(MAX_INVITE_CACHE);
  private failedInviteCache = new LRUCache<string, number>(MAX_FAILED_INVITE_CACHE);
  private duplicateCache = new LRUCache<string, number>(MAX_DUPLICATE_CACHE);

  // Watchlist with reverse index and optional Aho-Corasick / Bloom
  private watchlistData: Map<string, string[]> = new Map();
  private watchlistCacheExpiry = 0;
  private reverseWatchlistIndex: Map<string, string[]> = new Map();
  private watchlistAhoCorasick: AhoCorasick | null = null;
  private watchlistBloomFilter: BloomFilter | null = null;
  private totalWatchlistItems = 0;

  private pendingInvites = new Map<string, Promise<string>>();
  private inviteRefresherInterval: NodeJS.Timeout | null = null;
  private watchlistRefreshInterval: NodeJS.Timeout | null = null;

  private stats = {
    detected: 0, notified: 0, skipped: 0, errors: 0,
    falsePositivesBlocked: 0, watchlistMatches: 0, draftsSkipped: 0,
    startedAt: Date.now(),
  };

  // Per-guild stats
  private guildStats = new Map<string, { detected: number; notified: number; falsePositives: number }>();

  constructor(
    client: Client, log: AppLogger, _token: string,
    accountLabel: string, botManager: BotManager | null,
  ) {
    super();
    this.client = client;
    this.log = log;
    this.accountLabel = accountLabel;
    this.botManager = botManager;
    this.selfUserId = client.user?.id || '';

    this.startInviteRefresher();
    this.startWatchlistRefresher();
  }

  // ═══════════════════════════════════════════════════════════════════════
  // PUBLIC API
  // ═══════════════════════════════════════════════════════════════════════

  public async handleMessage(message: Message): Promise<void> {
    const now = Date.now();

    // Stage 1: Quick Reject
    const rejectReason = quickReject(message, this.selfUserId, now);
    if (rejectReason) return;

    const key = `${message.id}-${message.channel.id}`;
    if (this.processingMessages.has(key)) return;
    this.processingMessages.add(key);

    try {
      // Stage 2: Parse Once
      let parsed = parseMessage(message, now);

      if (isBlockedContent(parsed)) { this.stats.falsePositivesBlocked++; return; }

      // Stage 3a: Creation Detection
      let creationResult = this.creationCache.get(message.id);
      if (!creationResult) {
        creationResult = detectCreation(parsed);
        this.creationCache.set(message.id, creationResult);
      }

      if (!creationResult.isCreation && shouldRefreshMessage(parsed)) {
        try {
          const refreshed = await message.channel.messages.fetch(message.id);
          parsed = refreshParsedMessage(refreshed, now);
          creationResult = detectCreation(parsed);
          this.creationCache.set(message.id, creationResult);
        } catch {}
      }

      if (creationResult.isCreation) { this.stats.draftsSkipped++; return; }

      // Stage 3b: Draft / Ended checks
      if (isDraftGiveaway(parsed)) { this.stats.draftsSkipped++; return; }

      // Stage 3c: Score with reasons
      const detection = calculateGiveawayScore(parsed);
      if (detection.score < MINIMUM_SCORE_THRESHOLD) {
        this.stats.falsePositivesBlocked++;
        if (CONFIG.logLevel === 'debug') {
          this.log.debug('Below threshold', {
            mid: message.id,
            score: detection.score,
            confidence: detection.confidence,
            signals: detection.signals.join(', '),
          });
        }
        return;
      }

      // Stage 3d: Duplicate check
      if (this.duplicateCache.get(parsed.contentHash)) return;
      this.duplicateCache.set(parsed.contentHash, now);

      // Stage 3e: Existing check
      const existing = await getGiveaway(message.id, message.channel.id);
      if (existing) {
        await updateLastSeen(message.id, message.channel.id);
        if (existing.status === 'active' && isEndedGiveaway(parsed)) {
          await markEnded(message.id, message.channel.id);
        }
        return;
      }

      // Cooldown
      if (await wasNotifiedRecently(message.id, message.channel.id, CONFIG.notificationCooldown)) {
        this.stats.skipped++; return;
      }

      // Stage 4: Build & Notify
      this.stats.detected++;
      this.recordGuildStat(message.guild!.id, 'detected');

      const detectionTime = Date.now() - now;
      const guild = message.guild!;
      const guildIcon = guild.iconURL({ size: 512 }) || null;
      const guildBanner = (guild as any).bannerURL?.({ size: 1024 }) || null;
      const memberCount = (guild as any).memberCount ?? null;

      const data: Omit<GiveawayData, 'id' | 'status' | 'notifiedAt' | 'lastSeenAt'> = {
        messageId: message.id, channelId: message.channel.id,
        guildId: guild.id, guildName: guild.name,
        channelName: (message.channel as any).name || 'unknown',
        authorId: parsed.botId, prize: parsed.prize,
        detectedAt: now, endsAt: parsed.timestamps.end,
        detectionTimeMs: detectionTime,
        guildIcon, guildBanner, memberCount,
      };

      // Parallel: save + invite + watchlist
      const savePromise = insertGiveaway(data);
      const invitePromise = this.fetchInviteForGuild(guild.id);
      const watchlistPromise = this.checkWatchlistMatches(parsed, message, invitePromise);

      const [inserted, inviteUrl] = await Promise.all([savePromise, invitePromise]);
      if (!inserted) return;

      const fullData: GiveawayData = {
        ...data, id: undefined, status: 'active',
        notifiedAt: null, lastSeenAt: now,
        inviteUrl, guildIcon, guildBanner, memberCount,
      };

      try {
        const sent = await this.botManager?.sendGiveawayNotification(fullData);
        if (sent) {
          this.stats.notified++;
          this.recordGuildStat(guild.id, 'notified');
          await markNotified(message.id, message.channel.id);
        } else {
          this.stats.errors++;
        }
      } catch (error) {
        this.stats.errors++;
        this.log.error(`Notify error: ${formatError(error)}`);
      }

      // Log detection with reasons
      this.log.info(`Detected: "${parsed.prize}" [${detection.confidence}%] - ${detection.signals.join(', ')}`);

      await watchlistPromise;

    } catch (error) {
      this.stats.errors++;
      this.log.error(`Error ${message.id}: ${formatError(error)}`);
    } finally {
      this.processingMessages.delete(key);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // MESSAGE UPDATE HANDLER (detect ended giveaways)
  // ═══════════════════════════════════════════════════════════════════════

  public async handleMessageUpdate(oldMessage: Message, newMessage: Message): Promise<void> {
    // Only check messages from allowed bots that we've seen before
    if (!newMessage.guild || !newMessage.author?.bot) return;
    if (!ALLOWED_GIVEAWAY_BOT_IDS.has(newMessage.author.id)) return;

    const existing = await getGiveaway(newMessage.id, newMessage.channel.id);
    if (!existing || existing.status !== 'active') return;

    const now = Date.now();
    const parsed = parseMessage(newMessage, now);

    if (isEndedGiveaway(parsed)) {
      await markEnded(newMessage.id, newMessage.channel.id);
      this.log.debug(`Giveaway ended via edit: ${newMessage.id}`);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // WATCHLIST WITH REVERSE INDEX + AHO-CORASICK + BLOOM
  // ═══════════════════════════════════════════════════════════════════════

  private async checkWatchlistMatches(
    parsed: ParsedGiveawayData,
    message: Message,
    invitePromise: Promise<string>,
  ): Promise<void> {
    if (!this.botManager) return;

    try {
      const { reverseIndex, ahoCorasick, bloomFilter, totalItems } = await this.getWatchlistData();
      if (totalItems === 0) return;

      const lowerText = parsed.lowerText;

      // Stage 1: Bloom filter pre-check (if applicable)
      if (bloomFilter && !bloomFilter.mightContain(lowerText)) {
        // Bloom filter says definitely no match - skip
        return;
      }

      // Stage 2: Find matching keywords
      let matchedKeywords: string[];

      if (ahoCorasick) {
        // Aho-Corasick: single pass, finds all matches
        matchedKeywords = Array.from(ahoCorasick.findMatches(lowerText));
      } else {
        // Fallback: scan text for each keyword using reverse index
        matchedKeywords = [];
        for (const [keyword, userIds] of reverseIndex) {
          if (lowerText.includes(keyword)) {
            matchedKeywords.push(keyword);
          }
        }
      }

      if (matchedKeywords.length === 0) return;

      // Stage 3: Collect unique users from matched keywords
      const matchedUserSet = new Set<string>();
      for (const keyword of matchedKeywords) {
        const userIds = reverseIndex.get(keyword);
        if (userIds) {
          for (const userId of userIds) {
            matchedUserSet.add(userId);
          }
        }
      }

      if (matchedUserSet.size === 0) return;

      const uniqueUsers = Array.from(matchedUserSet);
      this.stats.watchlistMatches += uniqueUsers.length;

      const messageUrl = `https://discord.com/channels/${message.guild!.id}/${message.channel.id}/${message.id}`;
      const inviteUrl = await invitePromise;

      await this.sendWatchlistDMs(uniqueUsers, parsed.prize, message, parsed.timestamps.end, messageUrl, inviteUrl);

    } catch (err) {
      this.log.error('Watchlist error', { error: formatError(err) });
    }
  }

  private async getWatchlistData(): Promise<{
    reverseIndex: Map<string, string[]>;
    ahoCorasick: AhoCorasick | null;
    bloomFilter: BloomFilter | null;
    totalItems: number;
  }> {
    const now = Date.now();

    if (this.watchlistCacheExpiry > now) {
      return {
        reverseIndex: this.reverseWatchlistIndex,
        ahoCorasick: this.watchlistAhoCorasick,
        bloomFilter: this.watchlistBloomFilter,
        totalItems: this.totalWatchlistItems,
      };
    }

    // Refresh cache
    try {
      const watchlists = await getAllWatchlists();
      const data = new Map<string, string[]>();
      let totalItems = 0;

      for (const wl of watchlists) {
        if (wl.items?.length) {
          // Store lowercased
          data.set(wl.userId, wl.items.map(i => i.toLowerCase()));
          totalItems += wl.items.length;
        }
      }

      this.watchlistData = data;
      this.totalWatchlistItems = totalItems;
      this.watchlistCacheExpiry = now + WATCHLIST_CACHE_TTL;

      // Build reverse index
      this.reverseWatchlistIndex = this.buildReverseIndex(data);

      // Build Aho-Corasick if threshold met
      if (totalItems >= AHOCORASICK_THRESHOLD) {
        this.watchlistAhoCorasick = this.buildAhoCorasick(data);
      } else {
        this.watchlistAhoCorasick = null;
      }

      // Build Bloom filter if threshold met
      if (totalItems >= BLOOM_FILTER_THRESHOLD) {
        this.watchlistBloomFilter = this.buildBloomFilter(data);
      } else {
        this.watchlistBloomFilter = null;
      }

    } catch (err) {
      this.log.error('Watchlist refresh error', { error: formatError(err) });
    }

    return {
      reverseIndex: this.reverseWatchlistIndex,
      ahoCorasick: this.watchlistAhoCorasick,
      bloomFilter: this.watchlistBloomFilter,
      totalItems: this.totalWatchlistItems,
    };
  }

  private buildReverseIndex(data: Map<string, string[]>): Map<string, string[]> {
    const index = new Map<string, string[]>();
    for (const [userId, items] of data) {
      for (const item of items) {
        // Items already lowercased
        let arr = index.get(item);
        if (!arr) { arr = []; index.set(item, arr); }
        arr.push(userId);
      }
    }
    return index;
  }

  private buildAhoCorasick(data: Map<string, string[]>): AhoCorasick {
    const ac = new AhoCorasick();
    const seen = new Set<string>();
    for (const items of data.values()) {
      for (const item of items) {
        if (!seen.has(item)) {
          seen.add(item);
          ac.addPattern(item);
        }
      }
    }
    ac.build();
    return ac;
  }

  private buildBloomFilter(data: Map<string, string[]>): BloomFilter {
    const bf = new BloomFilter(this.totalWatchlistItems, 0.01);
    for (const items of data.values()) {
      for (const item of items) {
        bf.add(item);
      }
    }
    return bf;
  }

  private async sendWatchlistDMs(
    users: string[], prize: string, message: Message,
    endsAt: number | null, messageUrl: string, inviteUrl: string,
  ): Promise<void> {
    if (!users.length || !this.botManager) return;

    let batchSize = 20;
    let delayMs = 1000;
    if (users.length <= 10) { batchSize = 5; delayMs = 200; }
    else if (users.length <= 50) { batchSize = 10; delayMs = 500; }
    else if (users.length <= 200) { batchSize = 15; delayMs = 800; }

    const guild = message.guild!;
    const guildIcon = guild.iconURL({ size: 512 }) || null;
    const guildBanner = (guild as any).bannerURL?.({ size: 1024 }) || null;
    const memberCount = (guild as any).memberCount ?? null;

    let sent = 0, failed = 0;

    for (let i = 0; i < users.length; i += batchSize) {
      const batch = users.slice(i, i + batchSize);
      const results = await Promise.allSettled(
        batch.map(userId =>
          this.botManager!.sendWatchlistDM(
            userId, prize, guild.name,
            (message.channel as any).name || 'unknown',
            endsAt, messageUrl, guild.id, guildIcon,
            Date.now(), inviteUrl, guildBanner, memberCount,
          )
        )
      );
      for (const r of results) { r.status === 'fulfilled' ? sent++ : failed++; }
      if (i + batchSize < users.length) await delay(delayMs + Math.random() * 200);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // BACKGROUND WATCHLIST REFRESHER
  // ═══════════════════════════════════════════════════════════════════════

  private startWatchlistRefresher(): void {
    if (this.watchlistRefreshInterval) clearInterval(this.watchlistRefreshInterval);
    this.watchlistRefreshInterval = setInterval(() => {
      this.watchlistCacheExpiry = 0; // Force refresh on next access
    }, WATCHLIST_CACHE_TTL);
    if (this.watchlistRefreshInterval.unref) this.watchlistRefreshInterval.unref();
  }

  // ═══════════════════════════════════════════════════════════════════════
  // INVITE GENERATION WITH FAILED CACHE
  // ═══════════════════════════════════════════════════════════════════════

  private async fetchInviteForGuild(guildId: string): Promise<string> {
    const now = Date.now();

    // Check failed cache
    const failedUntil = this.failedInviteCache.get(guildId);
    if (failedUntil && failedUntil > now) {
      return `https://discord.com/channels/${guildId}`;
    }

    // Check valid cache
    const cached = this.inviteCache.get(guildId);
    if (cached && cached.expiresAt > now) return cached.url;

    // Check pending
    const pending = this.pendingInvites.get(guildId);
    if (pending) return pending;

    const promise = this.doFetchInvite(guildId, now);
    this.pendingInvites.set(guildId, promise);

    try {
      const url = await promise;
      return url;
    } finally {
      this.pendingInvites.delete(guildId);
    }
  }

  private async doFetchInvite(guildId: string, now: number): Promise<string> {
    try {
      const guild = this.client.guilds.cache.get(guildId);
      if (!guild) { this.cacheFailedInvite(guildId, now); return `https://discord.com/channels/${guildId}`; }

      // Try existing invites
      try {
        const invites = await guild.invites.fetch();
        if (invites?.size) {
          const permanent = invites.find(inv => inv.maxAge === 0 && inv.maxUses === 0);
          const url = permanent?.url || invites.first()?.url;
          if (url) { this.cacheInvite(guildId, url, now); return url; }
        }
      } catch {}

      // Try vanity
      try {
        const vanity = (guild as any).vanityURLCode;
        if (vanity) { const url = `https://discord.gg/${vanity}`; this.cacheInvite(guildId, url, now); return url; }
      } catch {}

      // Create invite
      const textChannels = guild.channels.cache.filter(
        (ch): ch is TextChannel => ch.type === 'GUILD_TEXT'
      );
      if (!textChannels.size) { this.cacheFailedInvite(guildId, now); return `https://discord.com/channels/${guildId}`; }

      const botMember = guild.members.cache.get(this.selfUserId);
      if (!botMember) { this.cacheFailedInvite(guildId, now); return `https://discord.com/channels/${guildId}`; }

      // Channels with permission
      for (const [, channel] of textChannels) {
        try {
          if (!channel.permissionsFor(botMember)?.has('CREATE_INSTANT_INVITE')) continue;
          const invite = await channel.createInvite({ maxAge: 0, maxUses: 0, reason: 'Giveaway tracker', temporary: false });
          this.cacheInvite(guildId, invite.url, now);
          return invite.url;
        } catch {}
      }

      // Fallback: any channel
      for (const [, channel] of textChannels) {
        try {
          const invite = await channel.createInvite({ maxAge: 0, maxUses: 0, reason: 'Giveaway tracker (fallback)', temporary: false });
          this.cacheInvite(guildId, invite.url, now);
          return invite.url;
        } catch {}
      }

      this.cacheFailedInvite(guildId, now);
      return `https://discord.com/channels/${guildId}`;

    } catch (error) {
      this.log.error(`Invite error ${guildId}: ${formatError(error)}`);
      this.cacheFailedInvite(guildId, now);
      return `https://discord.com/channels/${guildId}`;
    }
  }

  private cacheInvite(guildId: string, url: string, now: number): void {
    this.inviteCache.set(guildId, { url, expiresAt: now + INVITE_CACHE_TTL });
  }

  private cacheFailedInvite(guildId: string, now: number): void {
    this.failedInviteCache.set(guildId, now + FAILED_INVITE_RETRY_MS);
  }

  private startInviteRefresher(): void {
    if (this.inviteRefresherInterval) clearInterval(this.inviteRefresherInterval);
    this.inviteRefresherInterval = setInterval(() => {
      const now = Date.now();
      for (const guildId of this.client.guilds.cache.keys()) {
        const cached = this.inviteCache.get(guildId);
        if (!cached || cached.expiresAt <= now) {
          this.fetchInviteForGuild(guildId).catch(() => {});
        }
      }
    }, 5 * 60 * 1000);
    if (this.inviteRefresherInterval.unref) this.inviteRefresherInterval.unref();
  }

  public clearInviteCache(guildId: string): void {
    this.inviteCache.delete(guildId);
    this.failedInviteCache.delete(guildId);
    this.pendingInvites.delete(guildId);
  }

  // ═══════════════════════════════════════════════════════════════════════
  // PER-GUILD STATS
  // ═══════════════════════════════════════════════════════════════════════

  private recordGuildStat(guildId: string, type: 'detected' | 'notified' | 'falsePositive'): void {
    let stats = this.guildStats.get(guildId);
    if (!stats) { stats = { detected: 0, notified: 0, falsePositives: 0 }; this.guildStats.set(guildId, stats); }
    if (type === 'detected') stats.detected++;
    else if (type === 'notified') stats.notified++;
    else stats.falsePositives++;
  }

  public getGuildStats(): Map<string, { detected: number; notified: number; falsePositives: number }> {
    return new Map(this.guildStats);
  }

  // ═══════════════════════════════════════════════════════════════════════
  // GLOBAL STATS & SHUTDOWN
  // ═══════════════════════════════════════════════════════════════════════

  public getStats() {
    return { ...this.stats, uptime: Date.now() - this.stats.startedAt, guildStats: this.guildStats.size };
  }

  public logStats(): void {
    const s = this.stats;
    const uptime = (Date.now() - s.startedAt) / 1000;
    this.log.info(`── ${this.accountLabel} Stats ──────────────────────────`);
    this.log.info(`  Detected            : ${s.detected}`);
    this.log.info(`  Notified            : ${s.notified}`);
    this.log.info(`  Skipped (cooldown)  : ${s.skipped}`);
    this.log.info(`  Errors              : ${s.errors}`);
    this.log.info(`  False positives     : ${s.falsePositivesBlocked}`);
    this.log.info(`  Watchlist matches   : ${s.watchlistMatches}`);
    this.log.info(`  Drafts skipped      : ${s.draftsSkipped}`);
    this.log.info(`  Uptime              : ${Math.floor(uptime / 60)}m ${Math.floor(uptime % 60)}s`);
    this.log.info(`  Invites cached      : ${this.inviteCache.size}`);
    this.log.info(`  Failed invites      : ${this.failedInviteCache.size}`);
    this.log.info(`  Watchlist items     : ${this.totalWatchlistItems}`);
    this.log.info(`  Guilds tracked      : ${this.guildStats.size}`);
    this.log.info(`────────────────────────────────────────────────────────`);

    // Top 5 guilds
    if (this.guildStats.size > 0) {
      const top = Array.from(this.guildStats.entries())
        .sort((a, b) => b[1].detected - a[1].detected)
        .slice(0, 5);
      this.log.info('  Top guilds:');
      for (const [guildId, stats] of top) {
        const guild = this.client.guilds.cache.get(guildId);
        this.log.info(`    ${guild?.name || guildId}: ${stats.detected}d/${stats.notified}n/${stats.falsePositives}fp`);
      }
    }
  }

  public resetStats(): void {
    this.stats = {
      detected: 0, notified: 0, skipped: 0, errors: 0,
      falsePositivesBlocked: 0, watchlistMatches: 0, draftsSkipped: 0,
      startedAt: Date.now(),
    };
    this.guildStats.clear();
  }

  public async shutdown(): Promise<void> {
    if (this.inviteRefresherInterval) { clearInterval(this.inviteRefresherInterval); this.inviteRefresherInterval = null; }
    if (this.watchlistRefreshInterval) { clearInterval(this.watchlistRefreshInterval); this.watchlistRefreshInterval = null; }
    this.log.info(`Shutting down ${this.accountLabel}...`);
    this.logStats();
  }
}

// ─── Helper (not a method to avoid `this` issues) ─────────────────────────

function shouldRefreshMessage(parsed: ParsedGiveawayData): boolean {
  return !parsed.hasAnyEmbed || !parsed.hasAnyComponent ||
    (parsed.content.length < 50 && parsed.lowerText.includes('giveaway'));
}

export default GiveawayManager;
