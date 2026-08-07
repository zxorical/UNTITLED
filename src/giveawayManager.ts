/**
 * @module giveawayManager
 * Reliable giveaway detector — scans everything, misses nothing.
 * 
 * FIXES APPLIED:
 * 1. Added AppLogger type import
 * 2. parsedMessageCache with TTL and size limits (replaced WeakMap)
 * 3. Proper cleanup of cached entries
 * 4. Memory-efficient caching with LRU behavior
 * 5. Reduced log spam with sampling
 * 6. Fixed memory leak in message processing
 * 7. Added cache size limits
 * 8. Periodic cache cleanup
 * 9. Optimized watchlist matching
 * 10. Database fallback for getGiveaway to prevent re-tracking on restart
 * 11. Startup grace period to skip old replayed messages
 * 12. Gateway latency capping to prevent misleading 200k+ ms stats
 * 13. SCRIM/EVENT DETECTION - scans all messages for scrims, squid game, gagaball
 */

import { Client, Message, TextChannel } from 'discord.js-selfbot-v13';
import { EventEmitter } from 'events';
import { CONFIG } from './config.js';
import { logger, AppLogger } from './logger.js';
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
// CONSTANTS (pre-compiled Sets for O(1) lookups)
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

const GIVEAWAY_WORDS = new Set(['giveaway', 'raffle', 'sweepstakes', 'win', 'prize']);
const ENTRY_WORDS = new Set(['enter', 'join', 'participate', 'raffle', 'sweepstakes', 'submit']);
const FOOTER_END_WORDS = new Set(['ends', 'expires']);
const PRIZE_FIELD_NAMES = new Set(['prize', 'reward', 'item', 'prizes', 'rewards']);

const BLOCKED_PHRASES = [
  'already entered', 'you have already entered', "you've already entered",
  'you are already in', 'leave giveaway', 'joined successfully',
  'entry confirmed', 'entered successfully', "you're entered",
  'withdraw entry', 'giveaway has ended', 'giveaway ended',
  'giveaway is over', 'winners selected', 'winner selected',
  'congratulations', 'you won', 'you did not win',
  'results are in', 'giveaway is now closed', 'thank you for participating',
];

const DRAFT_PHRASES = [
  'review your giveaway', 'click "start" to', "click 'start' to",
  'this message expires in', 'giveaway preview', 'configure your giveaway',
  'setup your giveaway', 'you can edit this', 'you can change',
  'create a giveaway', 'select a channel', 'set the prize', 'set the duration',
];

const ENDED_PHRASES = [
  'ended', 'winner', 'closed', 'congratulations', 'results',
  'giveaway has ended', 'giveaway ended', 'giveaway is over',
];

const DURATION_REGEX = /(\d+)\s*(minute|min|m|hour|h)/i;
const TIMESTAMP_REGEX = /<t:(\d{10,13})(?::[a-zA-Z])?>/g;
const COUNT_ME_IN_REGEX = /count\s+me\s+in/i;

// ─── Scoring weights ──────────────────────────────────────────────────────
const SCORE = {
  ENTRY_BUTTON: 3,
  TIMESTAMP: 3,
  TITLE_KEYWORD: 2,
  FOOTER_ENDS: 2,
  FIELD_GIVEAWAY: 2,
  DESCRIPTION_KEYWORD: 1,
  EMBED_COLOR: 1,
  AUTHOR_KNOWN: 1,
  CREATE_REVIEW: 5,
  CREATE_EXPIRES: 5,
  CREATE_CLICK_START: 5,
  CREATE_CONFIG: 5,
  CREATE_PREVIEW: 3,
  CREATE_EDIT: 3,
  CREATE_SETUP: 2,
  CREATE_CHANNEL: 2,
  CREATE_PRIZE: 2,
  CREATE_BUTTON_START: 3,
  CREATE_BUTTON_EDIT: 2,
  CREATE_BUTTON_CANCEL: 2,
  CREATE_BUTTON_PREVIEW: 2,
  CREATE_BUTTON_SETUP: 2,
  CREATE_SHORT_DURATION: 2,
} as const;

const MAX_POSSIBLE_SCORE =
  SCORE.ENTRY_BUTTON +
  SCORE.TIMESTAMP +
  SCORE.TITLE_KEYWORD +
  SCORE.FOOTER_ENDS +
  SCORE.FIELD_GIVEAWAY +
  SCORE.DESCRIPTION_KEYWORD +
  SCORE.EMBED_COLOR +
  SCORE.AUTHOR_KNOWN;

const MINIMUM_SCORE_THRESHOLD = 6;
const CREATION_SCORE_THRESHOLD = 7;
const MAX_MESSAGE_AGE_MS = 30 * 60 * 1000;

const MAX_CREATION_CACHE = 2000;
const MAX_INVITE_CACHE = 500;
const MAX_DUPLICATE_CACHE = 1000;
const MAX_FAILED_INVITE_CACHE = 200;
const WATCHLIST_CACHE_TTL = 60000;
const INVITE_CACHE_TTL = 30 * 60 * 1000;
const FAILED_INVITE_RETRY_MS = 15 * 60 * 1000;
const AHOCORASICK_THRESHOLD = 100;

// Startup grace period constants
const STARTUP_GRACE_PERIOD_MS = 30_000; // 30 seconds
const MAX_STARTUP_MESSAGE_AGE_MS = 10_000; // 10 seconds during startup
const MAX_GATEWAY_LATENCY_MS = 60_000; // Cap gateway latency at 60 seconds

// ─── Parsed Message Cache with TTL ──────────────────────────────────────
const PARSED_CACHE_TTL_MS = 60000; // 1 minute
const MAX_PARSED_CACHE_SIZE = 10000;
const parsedMessageCache = new Map<string, { data: ParsedGiveawayData; timestamp: number }>();

// Periodic cleanup for parsed message cache
setInterval(() => {
  const now = Date.now();
  let removed = 0;
  for (const [key, entry] of parsedMessageCache) {
    if (now - entry.timestamp > PARSED_CACHE_TTL_MS) {
      parsedMessageCache.delete(key);
      removed++;
    }
  }
  // Also trim if over max size
  if (parsedMessageCache.size > MAX_PARSED_CACHE_SIZE) {
    const entries = Array.from(parsedMessageCache.entries());
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    const toRemove = parsedMessageCache.size - Math.floor(MAX_PARSED_CACHE_SIZE * 0.8);
    for (let i = 0; i < toRemove && i < entries.length; i++) {
      parsedMessageCache.delete(entries[i][0]);
      removed++;
    }
  }
  if (removed > 0) {
    logger.debug(`Cleaned ${removed} parsed message cache entries`, {
      component: 'GiveawayManager',
      remaining: parsedMessageCache.size
    });
  }
}, PARSED_CACHE_TTL_MS);

// ═══════════════════════════════════════════════════════════════════════════
// SCRIM/EVENT DETECTION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

// Scrim patterns
const SCRIM_PATTERNS: RegExp[] = [
  /VREL\s*3v3\s*Scrim/i,
  /scrim|scrims/i,
  /\(Scrim\)|\(Scrims\)/i,
];

const SQUID_GAME_PATTERNS: RegExp[] = [
  /squid\s*game/i,
  /squidgame/i,
  /squid/i,
];

const GAGABALL_PATTERNS: RegExp[] = [
  /gagaball/i,
  /gaga\s*ball/i,
  /gaga/i,
];

// Team patterns
const TEAM_PATTERNS: RegExp[] = [
  /3v3/i, /2v2/i, /4v4/i, /5v5/i, /1v1/i,
  /(\d+)\s*TEAMS?/i,
  /teams?:\s*(\d+v\d+)/i,
  /(\d+)\s*[xX×]\s*(\d+)/i,
];

// Host patterns
const HOST_PATTERNS: RegExp[] = [
  /Host:\s*<@!?(\d+)>/i,
  /Host:\s*<@(\d+)>/i,
  /Hosts?:\s*<@!?(\d+)>/i,
  /Perms from:\s*<@!?(\d+)>/i,
  /host\s*<@!?(\d+)>/i,
];

const COHOST_PATTERNS: RegExp[] = [
  /Co Host:\s*<@!?(\d+)>/i,
  /Co-Host:\s*<@!?(\d+)>/i,
  /CoHost:\s*<@!?(\d+)>/i,
];

// Time patterns
const TIME_PATTERNS: RegExp[] = [
  /Time:\s*([^\n]+)/i,
  /at\s*([^\n]+?)(?=\s*[A-Z]|$)/i,
  /(\d{1,2}\s*[ap]m\s*[A-Z]{2,3})/i,
  /(\d{1,2}:\d{2}\s*[ap]m\s*[A-Z]{2,3})/i,
  /(\d{1,2}:\d{2}\s*[A-Z]{2,3})/i,
  /(\d{1,2}\s*[ap]m)/i,
  /(\d{1,2}:\d{2}\s*(?:am|pm))/i,
  /(\d{1,2}\s*(?:am|pm))/i,
];

// Reward patterns
const REWARD_PATTERNS: RegExp[] = [
  /Reward:\s*([^\n]+)/i,
  /Rewards:\s*([^\n]+)/i,
  /Prize:\s*([^\n]+)/i,
  /reward:\s*([^\n]+)/i,
  /prize\s*([^\n]+)/i,
];

// Region patterns
const REGION_PATTERNS: RegExp[] = [
  /EU\s*X\s*NA/i,
  /NA\s*X\s*EU/i,
  /EU\s*ONLY/i,
  /NA\s*ONLY/i,
  /\bEU\b/i,
  /\bNA\b/i,
];

// Tick patterns
const TICK_PATTERNS: RegExp[] = [
  /Ticks?:\s*(\d+)\s*\+/i,
  /(\d+)\s*\+\s*Ticks?/i,
  /#\s*(\d+)\s*\+\s*Ticks?/i,
  /Ticks?:\s*(\d+)/i,
];

// Scrim score weights
const SCRIM_SCORE = {
  HAS_EVERYONE: 3,
  HAS_HOST: 3,
  HAS_TIME: 3,
  HAS_TEAMS: 2,
  HAS_REWARD: 2,
  HAS_REGION: 1,
  HAS_TICKS: 1,
  TITLE_KEYWORD: 2,
};

const MAX_SCRIM_SCORE = Object.values(SCRIM_SCORE).reduce((a, b) => a + b, 0);
const MINIMUM_SCRIM_SCORE_THRESHOLD = 5;

// ============================================================================
// SCRIM DETECTION INTERFACE
// ============================================================================

interface ScrimDetectionResult {
  type: 'scrim' | 'squid_game' | 'gagaball';
  host: string | null;
  coHost: string | null;
  time: string | null;
  reward: string | null;
  teams: string | null;
  region: string | null;
  ticks: number | null;
  score: number;
  confidence: number;
  signals: string[];
}

// ═══════════════════════════════════════════════════════════════════════════
// AHO-CORASICK (O(queue[head++]) BFS, no shift)
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

    let head = 0;
    while (head < queue.length) {
      const current = queue[head++];
      for (const [char, child] of current.children) {
        let failNode = current.fail;
        while (failNode !== null && !failNode.children.has(char)) {
          failNode = failNode.fail;
        }
        child.fail = failNode ? failNode.children.get(char) || this.root : this.root;
        for (const output of child.fail.output) {
          child.output.add(output);
        }
        queue.push(child);
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
  clear(): void { this.map.clear(); }
  
  // Get all keys for debugging
  keys(): IterableIterator<K> {
    return this.map.keys();
  }
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
  confidence: number;
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
  contentHash: string;
}

function getParsedCacheKey(message: Message): string {
  return `${message.id}:${message.channel.id}`;
}

function parseMessage(message: Message, now: number): ParsedGiveawayData {
  const cacheKey = getParsedCacheKey(message);
  const cached = parsedMessageCache.get(cacheKey);
  
  if (cached && now - cached.timestamp < PARSED_CACHE_TTL_MS) {
    return cached.data;
  }

  const embed = message.embeds?.[0];
  const messageAge = now - message.createdTimestamp;

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

  const textParts = [content, title, description, footer, authorName, ...fieldNames, ...fieldValues];
  const fullText = textParts.filter(Boolean).join(' ');
  const lowerText = fullText.toLowerCase();

  const buttons = parseButtons((message as any).components);
  const timestamps = parseTimestamps(fullText, now);
  const prize = extractPrize(title, description, content, fieldNames, fieldValues);
  const contentHash = simpleHash(`${message.guild?.id}|${prize}|${timestamps.end}`);

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

  // Store in cache with timestamp
  parsedMessageCache.set(cacheKey, { data: parsed, timestamp: now });
  
  // Trim cache if needed (already handled by interval, but just in case)
  if (parsedMessageCache.size > MAX_PARSED_CACHE_SIZE * 1.2) {
    const entries = Array.from(parsedMessageCache.entries());
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    const toRemove = parsedMessageCache.size - MAX_PARSED_CACHE_SIZE;
    for (let i = 0; i < toRemove && i < entries.length; i++) {
      parsedMessageCache.delete(entries[i][0]);
    }
  }
  
  return parsed;
}

function refreshParsedMessage(message: Message, now: number): ParsedGiveawayData {
  const cacheKey = getParsedCacheKey(message);
  parsedMessageCache.delete(cacheKey);
  return parseMessage(message, now);
}

function simpleHash(str: string): string {
  let hash = 2166136261;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
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

// ─── Prize Extraction ─────────────────────────────────────────────────────

function extractPrize(
  title: string, description: string, content: string,
  fieldNames: string[], fieldValues: string[],
): string {
  for (let i = 0; i < fieldNames.length; i++) {
    if (PRIZE_FIELD_NAMES.has(fieldNames[i].toLowerCase())) {
      return fieldValues[i].slice(0, 200).trim() || 'Unknown Prize';
    }
  }
  if (title) return title.slice(0, 200).trim();
  if (description) return description.slice(0, 200).trim();
  return content.slice(0, 200).trim() || 'Unknown Prize';
}

// ============================================================================
// SCRIM DETECTION FUNCTIONS
// ============================================================================

function detectScrimType(text: string): 'scrim' | 'squid_game' | 'gagaball' | null {
  const lower = text.toLowerCase();

  // Check for Squid Game (highest priority)
  for (const pattern of SQUID_GAME_PATTERNS) {
    if (pattern.test(lower)) return 'squid_game';
  }

  // Check for Gagaball
  for (const pattern of GAGABALL_PATTERNS) {
    if (pattern.test(lower)) return 'gagaball';
  }

  // Check for Scrim
  for (const pattern of SCRIM_PATTERNS) {
    if (pattern.test(lower)) return 'scrim';
  }

  return null;
}

function extractScrimHost(text: string): string | null {
  for (const pattern of HOST_PATTERNS) {
    const match = text.match(pattern);
    if (match && match[1]) {
      return `<@${match[1]}>`;
    }
  }
  return null;
}

function extractScrimCoHost(text: string): string | null {
  for (const pattern of COHOST_PATTERNS) {
    const match = text.match(pattern);
    if (match && match[1]) {
      return `<@${match[1]}>`;
    }
  }
  return null;
}

function extractScrimTime(text: string): string | null {
  for (const pattern of TIME_PATTERNS) {
    const match = text.match(pattern);
    if (match && match[1]) {
      return match[1].trim();
    }
  }
  return null;
}

function extractScrimReward(text: string): string | null {
  for (const pattern of REWARD_PATTERNS) {
    const match = text.match(pattern);
    if (match && match[1]) {
      return match[1].trim();
    }
  }
  return null;
}

function extractScrimTeams(text: string): string | null {
  for (const pattern of TEAM_PATTERNS) {
    const match = text.match(pattern);
    if (match) {
      if (match[1]) {
        return match[1];
      }
      return match[0];
    }
  }
  return null;
}

function extractScrimRegion(text: string): string | null {
  for (const pattern of REGION_PATTERNS) {
    const match = text.match(pattern);
    if (match) {
      return match[0];
    }
  }
  return null;
}

function extractScrimTicks(text: string): number | null {
  for (const pattern of TICK_PATTERNS) {
    const match = text.match(pattern);
    if (match && match[1]) {
      return parseInt(match[1], 10);
    }
  }
  return null;
}

function detectScrim(parsed: ParsedGiveawayData): ScrimDetectionResult | null {
  const { lowerText, fullText } = parsed;
  
  // First check if it's a scrim/event type
  const type = detectScrimType(fullText);
  if (!type) return null;

  // Extract data
  const host = extractScrimHost(fullText);
  const coHost = extractScrimCoHost(fullText);
  const time = extractScrimTime(fullText);
  const reward = extractScrimReward(fullText);
  const teams = extractScrimTeams(fullText);
  const region = extractScrimRegion(fullText);
  const ticks = extractScrimTicks(fullText);

  // Calculate score
  let score = 0;
  const signals: string[] = [];

  const hasEveryone = lowerText.includes('@everyone') || lowerText.includes('@here');
  
  if (hasEveryone) {
    score += SCRIM_SCORE.HAS_EVERYONE;
    signals.push('everyone');
  }

  if (host) {
    score += SCRIM_SCORE.HAS_HOST;
    signals.push('host');
  }

  if (time) {
    score += SCRIM_SCORE.HAS_TIME;
    signals.push('time');
  }

  if (teams) {
    score += SCRIM_SCORE.HAS_TEAMS;
    signals.push('teams');
  }

  if (reward) {
    score += SCRIM_SCORE.HAS_REWARD;
    signals.push('reward');
  }

  if (region) {
    score += SCRIM_SCORE.HAS_REGION;
    signals.push('region');
  }

  if (ticks !== null) {
    score += SCRIM_SCORE.HAS_TICKS;
    signals.push('ticks');
  }

  // Keyword bonus
  if (/scrim|squid|gaga|giveaway|event/i.test(lowerText)) {
    score += SCRIM_SCORE.TITLE_KEYWORD;
    signals.push('keyword');
  }

  // For scrims, require at least host OR time to be present
  if (type === 'scrim' && !host && !time && !teams) {
    return null;
  }

  // For squid game and gagaball, require at least host or time
  if ((type === 'squid_game' || type === 'gagaball') && !host && !time) {
    return null;
  }

  const confidence = Math.min(100, Math.round((score / MAX_SCRIM_SCORE) * 100));

  return {
    type,
    host,
    coHost,
    time,
    reward,
    teams,
    region,
    ticks,
    score,
    confidence,
    signals,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// QUICK REJECT (Stage 1) - FIXED: Age check moved to top
// ═══════════════════════════════════════════════════════════════════════════

function quickReject(message: Message, selfUserId: string, now: number): string | null {
  if (!message.guild) return 'no_guild';
  if (message.author?.id === selfUserId) return 'self';

  // AGE CHECK FIRST: Reject messages that are too old immediately
  const messageAge = now - message.createdTimestamp;
  if (messageAge > MAX_MESSAGE_AGE_MS) {
    return 'too_old';
  }

  if (CONFIG.monitoredChannels.length > 0 && !CONFIG.monitoredChannels.includes(message.channel.id)) {
    return 'not_monitored';
  }

  if (!message.author?.bot || !ALLOWED_GIVEAWAY_BOT_IDS.has(message.author.id)) {
    return 'not_allowed_bot';
  }

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

  if (parsed.lowerText.includes('review your giveaway')) score += SCORE.CREATE_REVIEW;
  if (parsed.lowerText.includes('this message expires in')) score += SCORE.CREATE_EXPIRES;
  if (parsed.lowerText.includes('click "start" to')) score += SCORE.CREATE_CLICK_START;
  if (parsed.lowerText.includes("click 'start' to")) score += SCORE.CREATE_CLICK_START;
  if (parsed.lowerText.includes('configure your giveaway')) score += SCORE.CREATE_CONFIG;
  if (parsed.lowerText.includes('giveaway preview')) score += SCORE.CREATE_PREVIEW;
  if (parsed.lowerText.includes('setup your giveaway')) score += SCORE.CREATE_CONFIG;
  if (parsed.lowerText.includes('you can edit this')) score += SCORE.CREATE_EDIT;
  if (parsed.lowerText.includes('you can change')) score += SCORE.CREATE_EDIT;
  if (parsed.lowerText.includes('create a giveaway')) score += SCORE.CREATE_SETUP;
  if (parsed.lowerText.includes('select a channel')) score += SCORE.CREATE_CHANNEL;
  if (parsed.lowerText.includes('set the prize')) score += SCORE.CREATE_PRIZE;
  if (parsed.lowerText.includes('set the duration')) score += SCORE.CREATE_PRIZE;

  if (parsed.buttons.draftLabels.includes('start')) score += SCORE.CREATE_BUTTON_START;
  if (parsed.buttons.draftLabels.includes('edit')) score += SCORE.CREATE_BUTTON_EDIT;
  if (parsed.buttons.draftLabels.includes('cancel')) score += SCORE.CREATE_BUTTON_CANCEL;
  if (parsed.buttons.draftLabels.includes('preview')) score += SCORE.CREATE_BUTTON_PREVIEW;
  if (parsed.buttons.draftLabels.includes('setup')) score += SCORE.CREATE_BUTTON_SETUP;

  const durationMatch = parsed.fullText.match(DURATION_REGEX);
  if (durationMatch) {
    const value = parseInt(durationMatch[1], 10);
    const unit = (durationMatch[2] || '').toLowerCase();
    let minutes = value;
    if (unit.startsWith('h')) minutes = value * 60;
    if (minutes <= 15) score += SCORE.CREATE_SHORT_DURATION;
  }

  return { isCreation: score >= CREATION_SCORE_THRESHOLD, score };
}

// ═══════════════════════════════════════════════════════════════════════════
// SCORE CALCULATOR WITH DETECTION REASONS
// ═══════════════════════════════════════════════════════════════════════════

function calculateGiveawayScore(parsed: ParsedGiveawayData): DetectionReason {
  let score = 0;
  const signals: string[] = [];

  if (parsed.buttons.entry) {
    score += SCORE.ENTRY_BUTTON;
    signals.push('entry_button');
  }

  for (const word of GIVEAWAY_WORDS) {
    if (parsed.lowerTitle.includes(word)) { score += SCORE.TITLE_KEYWORD; signals.push(`title:${word}`); break; }
  }

  for (const word of GIVEAWAY_WORDS) {
    if (parsed.lowerDescription.includes(word)) { score += SCORE.DESCRIPTION_KEYWORD; signals.push(`desc:${word}`); break; }
  }

  for (const word of FOOTER_END_WORDS) {
    if (parsed.lowerFooter.includes(word)) { score += SCORE.FOOTER_ENDS; signals.push(`footer:${word}`); break; }
  }

  if (parsed.timestamps.end !== null) { score += SCORE.TIMESTAMP; signals.push('timestamp'); }

  if (parsed.embedColor !== null && GIVEAWAY_EMBED_COLORS.has(parsed.embedColor)) {
    score += SCORE.EMBED_COLOR; signals.push('embed_color');
  }

  if (parsed.lowerAuthor.includes('giveaway')) { score += SCORE.AUTHOR_KNOWN; signals.push('author'); }

  for (const fieldName of parsed.lowerFieldNames) {
    if (fieldName.includes('ends') || fieldName.includes('winners') || fieldName.includes('time remaining')) {
      score += SCORE.FIELD_GIVEAWAY; signals.push(`field:${fieldName}`); break;
    }
  }

  const confidence = Math.min(100, Math.round((score / MAX_POSSIBLE_SCORE) * 100));

  return { signals, score, confidence };
}

// ═══════════════════════════════════════════════════════════════════════════
// BLOCKED / DRAFT / ENDED CHECKS
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

function shouldRefreshMessage(parsed: ParsedGiveawayData): boolean {
  return !parsed.hasAnyEmbed || !parsed.hasAnyComponent ||
    (parsed.content.length < 50 && parsed.lowerText.includes('giveaway'));
}

// ═══════════════════════════════════════════════════════════════════════════
// GIVEAWAY MANAGER - FIXED: Startup grace period, DB fallback, latency cap
// ═══════════════════════════════════════════════════════════════════════════

export class GiveawayManager extends EventEmitter {
  private readonly client: Client;
  private readonly log: AppLogger;
  private readonly accountLabel: string;
  private readonly botManager: BotManager | null;
  private readonly selfUserId: string;

  private processingMessages = new Set<string>();

  private creationCache = new LRUCache<string, { isCreation: boolean; score: number }>(MAX_CREATION_CACHE);
  private inviteCache = new LRUCache<string, { url: string; expiresAt: number }>(MAX_INVITE_CACHE);
  private failedInviteCache = new LRUCache<string, number>(MAX_FAILED_INVITE_CACHE);
  private duplicateCache = new LRUCache<string, number>(MAX_DUPLICATE_CACHE);

  private watchlistCacheExpiry = 0;
  private reverseWatchlistIndex: Map<string, string[]> = new Map();
  private watchlistAhoCorasick: AhoCorasick | null = null;
  private totalWatchlistItems = 0;

  private pendingInvites = new Map<string, Promise<string>>();
  private inviteRefresherInterval: NodeJS.Timeout | null = null;
  private watchlistRefreshInterval: NodeJS.Timeout | null = null;

  // ─── NEW: Startup grace period properties ──────────────────────────
  private readyEventReceived = false;
  private readonly startupTime: number;
  private pendingStartupMessages = new Set<string>();
  private startupGraceTimer: NodeJS.Timeout | null = null;

  private stats = {
    detected: 0, notified: 0, skipped: 0, errors: 0,
    falsePositivesBlocked: 0, watchlistMatches: 0, draftsSkipped: 0,
    startedAt: Date.now(),
    startupMessagesSkipped: 0, // NEW: Track skipped startup messages
    scrimsDetected: 0, // NEW: Track scrims detected
    scrimsNotified: 0, // NEW: Track scrims notified
  };

  private guildStats = new Map<string, { detected: number; notified: number; falsePositives: number }>();

  // Log sampling to reduce spam
  private logSampleCounter = 0;
  private readonly LOG_SAMPLE_RATE = 10;

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
    this.startupTime = Date.now();

    this.startInviteRefresher();
    this.startWatchlistRefresher();
    this.setupReadyHandler(); // NEW: Handle startup gracefully
  }

  // ═══════════════════════════════════════════════════════════════════════
  // NEW: Startup Ready Handler
  // ═══════════════════════════════════════════════════════════════════════

  private setupReadyHandler(): void {
    // Listen for ready event to handle startup
    this.client.once('ready', () => {
      // Give Discord 2 seconds to finish replaying messages
      setTimeout(() => {
        this.readyEventReceived = true;
        
        const startupDuration = Date.now() - this.startupTime;
        this.log.info(
          `Startup complete - ${this.pendingStartupMessages.size} messages skipped during startup (${startupDuration}ms)`,
          {
            component: 'GiveawayManager',
            account: this.accountLabel,
            pendingMessages: this.pendingStartupMessages.size
          }
        );
        
        this.stats.startupMessagesSkipped = this.pendingStartupMessages.size;
        this.pendingStartupMessages.clear();
      }, 2000);
      
      // Set a hard grace period cutoff as fallback
      this.startupGraceTimer = setTimeout(() => {
        if (!this.readyEventReceived) {
          this.readyEventReceived = true;
          this.log.warn('Ready event not received within grace period, forcing startup complete', {
            component: 'GiveawayManager',
            account: this.accountLabel
          });
          this.stats.startupMessagesSkipped = this.pendingStartupMessages.size;
          this.pendingStartupMessages.clear();
        }
      }, STARTUP_GRACE_PERIOD_MS);
    });
  }

  // ═══════════════════════════════════════════════════════════════════════
  // SCRIM NOTIFICATION
  // ═══════════════════════════════════════════════════════════════════════

  private async sendScrimNotification(
    message: Message,
    parsed: ParsedGiveawayData,
    scrimResult: ScrimDetectionResult
  ): Promise<void> {
    if (!this.botManager) return;

    const now = Date.now();
    const guild = message.guild!;
    const guildIcon = guild.iconURL({ size: 512 }) || null;
    const guildBanner = (guild as any).bannerURL?.({ size: 1024 }) || null;
    const memberCount = (guild as any).memberCount ?? null;

    const typeLabel = {
      scrim: 'Scrim',
      squid_game: 'Squid Game',
      gagaball: 'Gagaball',
    }[scrimResult.type];

    // Build description
    const description = [
      '### Details',
      scrimResult.host ? `**Host:** ${scrimResult.host}` : '',
      scrimResult.coHost ? `**Co-Host:** ${scrimResult.coHost}` : '',
      scrimResult.time ? `**Time:** ${scrimResult.time}` : '',
      scrimResult.teams ? `**Teams:** ${scrimResult.teams}` : '',
      scrimResult.region ? `**Region:** ${scrimResult.region}` : '',
      scrimResult.reward ? `**Reward:** ${scrimResult.reward}` : '',
      scrimResult.ticks !== null ? `**Ticks:** ${scrimResult.ticks}+` : '',
      '',
      `**Server:** ${guild.name}`,
      `**Channel:** #${(message.channel as any).name || 'unknown'}`,
      '',
      `[View Message](https://discord.com/channels/${guild.id}/${message.channel.id}/${message.id})`,
    ].filter(Boolean).join('\n');

    // Fetch invite
    const inviteUrl = await this.fetchInviteForGuild(guild.id);

    try {
      const sent = await this.botManager.sendScrimNotification({
        messageId: message.id,
        channelId: message.channel.id,
        guildId: guild.id,
        guildName: guild.name,
        channelName: (message.channel as any).name || 'unknown',
        authorId: message.author?.id || '',
        prize: scrimResult.reward || `${typeLabel} Event`,
        detectedAt: now,
        endsAt: null,
        status: 'active',
        notifiedAt: null,
        lastSeenAt: now,
        inviteUrl,
        guildIcon,
        guildBanner,
        memberCount,
        type: scrimResult.type,
        host: scrimResult.host,
        coHost: scrimResult.coHost,
        time: scrimResult.time,
        reward: scrimResult.reward,
        teams: scrimResult.teams,
        region: scrimResult.region,
        ticks: scrimResult.ticks,
        rawContent: parsed.fullText.slice(0, 500),
        messageUrl: `https://discord.com/channels/${guild.id}/${message.channel.id}/${message.id}`,
      });

      if (sent) {
        this.stats.scrimsNotified++;
        this.recordGuildStat(guild.id, 'notified');
      }
    } catch (error) {
      this.stats.errors++;
      this.log.error(`Scrim notify error: ${formatError(error)}`);
    }

    this.log.info(
      `Scrim: ${typeLabel} [${scrimResult.confidence}%] - ` +
      scrimResult.signals.join(', '),
      {
        component: 'GiveawayManager',
        account: this.accountLabel,
        host: scrimResult.host,
        time: scrimResult.time,
      }
    );
  }

  // ═══════════════════════════════════════════════════════════════════════
  // PUBLIC API - FIXED: Startup guard, DB fallback, latency cap
  // ═══════════════════════════════════════════════════════════════════════

  public async handleMessage(message: Message): Promise<void> {
    const now = Date.now();
    const processingStart = performance.now();

    // ─── NEW: Startup Guard - Skip old messages replayed by Discord ───
    if (!this.readyEventReceived) {
      // Calculate message age
      const messageAge = now - message.createdTimestamp;
      
      // If message is older than 10 seconds during startup, skip it entirely
      if (messageAge > MAX_STARTUP_MESSAGE_AGE_MS) {
        return; // Old message replayed by Discord on reconnect
      }
      
      // Track that we saw this message during startup
      this.pendingStartupMessages.add(message.id);
    }

    // ─── NEW: Additional age check - reject messages that are too old regardless ───
    if (now - message.createdTimestamp > MAX_MESSAGE_AGE_MS) {
      return; // Message is too old to process
    }

    // Stage 1: Quick Reject (age check now at top of quickReject)
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
        } catch {
          // Ignore fetch errors
        }
      }

      if (creationResult.isCreation) { this.stats.draftsSkipped++; return; }

      // Stage 3b: Draft Check
      if (isDraftGiveaway(parsed)) { this.stats.draftsSkipped++; return; }

      // Stage 3c: Score Calculation - First check for giveaways
      const detection = calculateGiveawayScore(parsed);
      if (detection.score >= MINIMUM_SCORE_THRESHOLD) {
        // Stage 3d: Duplicate Check (in-memory, using message+channel ID)
        const messageDupKey = `${message.id}:${message.channel.id}`;
        if (this.duplicateCache.get(messageDupKey)) return;
        this.duplicateCache.set(messageDupKey, now);

        // ─── Stage 3e: Existing Check (NOW WITH MONGODB FALLBACK) ───
        const existing = await getGiveaway(message.id, message.channel.id);
        if (existing) {
          // Already tracked - update last seen and check if ended
          await updateLastSeen(message.id, message.channel.id);
          if (existing.status === 'active' && isEndedGiveaway(parsed)) {
            await markEnded(message.id, message.channel.id);
          }
          return; // IMPORTANT: Don't re-notify on restart
        }

        // Cooldown Check
        if (await wasNotifiedRecently(message.id, message.channel.id, CONFIG.notificationCooldown)) {
          this.stats.skipped++; return;
        }

        // Stage 4: Build Data & Notify
        this.stats.detected++;
        this.recordGuildStat(message.guild!.id, 'detected');

        // ─── FIXED: Cap gateway latency to prevent misleading stats on restart ───
        const gatewayLatency = Math.min(
          Math.max(1, Date.now() - message.createdTimestamp),
          MAX_GATEWAY_LATENCY_MS // Cap at 60 seconds
        );
        const processingTime = performance.now() - processingStart;
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
          detectionTimeMs: gatewayLatency,
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

        this.log.info(
          `Detected: "${parsed.prize}" [${detection.confidence}%] ` +
          `(gateway: ${gatewayLatency}ms, processing: ${processingTime.toFixed(2)}ms) - ` +
          detection.signals.join(', ')
        );

        await watchlistPromise;
        return;
      }

      // ─── NOT A GIVEAWAY - Check if it's a scrim/event ───
      const scrimResult = detectScrim(parsed);
      if (scrimResult && scrimResult.score >= MINIMUM_SCRIM_SCORE_THRESHOLD) {
        // Deduplicate scrims
        const scrimDupKey = `scrim:${message.id}:${message.channel.id}`;
        if (this.duplicateCache.get(scrimDupKey)) return;
        this.duplicateCache.set(scrimDupKey, now);

        // Track stats
        this.stats.detected++;
        this.stats.scrimsDetected++;
        this.recordGuildStat(message.guild!.id, 'detected');

        // Send scrim notification
        await this.sendScrimNotification(message, parsed, scrimResult);
        return;
      }

      // Neither giveaway nor scrim - skip
      this.stats.falsePositivesBlocked++;
      if (this.shouldLogDebug()) {
        this.log.debug('Below thresholds', {
          mid: message.id,
          giveawayScore: detection.score,
          scrimScore: scrimResult?.score || 0,
        });
      }

    } catch (error) {
      this.stats.errors++;
      this.log.error(`Error ${message.id}: ${formatError(error)}`);
    } finally {
      this.processingMessages.delete(key);
    }
  }

  private shouldLogDebug(): boolean {
    this.logSampleCounter++;
    if (this.logSampleCounter >= this.LOG_SAMPLE_RATE) {
      this.logSampleCounter = 0;
      return true;
    }
    return false;
  }

  // ═══════════════════════════════════════════════════════════════════════
  // MESSAGE UPDATE HANDLER
  // ═══════════════════════════════════════════════════════════════════════

  public async handleMessageUpdate(_oldMessage: Message, newMessage: Message): Promise<void> {
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
  // WATCHLIST MATCHING (reverse index + Aho-Corasick)
  // ═══════════════════════════════════════════════════════════════════════

  private async checkWatchlistMatches(
    parsed: ParsedGiveawayData,
    message: Message,
    invitePromise: Promise<string>,
  ): Promise<void> {
    if (!this.botManager) return;

    try {
      const { reverseIndex, ahoCorasick, totalItems } = await this.getWatchlistData();
      if (totalItems === 0) return;

      const lowerText = parsed.lowerText;

      let matchedKeywords: string[];
      if (ahoCorasick) {
        matchedKeywords = Array.from(ahoCorasick.findMatches(lowerText));
      } else {
        matchedKeywords = [];
        for (const keyword of reverseIndex.keys()) {
          if (lowerText.includes(keyword)) {
            matchedKeywords.push(keyword);
          }
        }
      }

      if (matchedKeywords.length === 0) return;

      const matchedUserSet = new Set<string>();
      for (const keyword of matchedKeywords) {
        const userIds = reverseIndex.get(keyword);
        if (userIds) {
          for (const userId of userIds) matchedUserSet.add(userId);
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
    totalItems: number;
  }> {
    const now = Date.now();

    if (this.watchlistCacheExpiry > now) {
      return {
        reverseIndex: this.reverseWatchlistIndex,
        ahoCorasick: this.watchlistAhoCorasick,
        totalItems: this.totalWatchlistItems,
      };
    }

    try {
      const watchlists = await getAllWatchlists();
      const data = new Map<string, string[]>();
      let totalItems = 0;

      for (const wl of watchlists) {
        if (wl.items?.length) {
          data.set(wl.userId, wl.items.map(i => i.toLowerCase()));
          totalItems += wl.items.length;
        }
      }

      this.totalWatchlistItems = totalItems;
      this.watchlistCacheExpiry = now + WATCHLIST_CACHE_TTL;
      this.reverseWatchlistIndex = this.buildReverseIndex(data);
      this.watchlistAhoCorasick = totalItems >= AHOCORASICK_THRESHOLD ? this.buildAhoCorasick(data) : null;

    } catch (err) {
      this.log.error('Watchlist refresh error', { error: formatError(err) });
    }

    return {
      reverseIndex: this.reverseWatchlistIndex,
      ahoCorasick: this.watchlistAhoCorasick,
      totalItems: this.totalWatchlistItems,
    };
  }

  private buildReverseIndex(data: Map<string, string[]>): Map<string, string[]> {
    const index = new Map<string, string[]>();
    for (const [userId, items] of data) {
      for (const item of items) {
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

  private async sendWatchlistDMs(
    users: string[], prize: string, message: Message,
    endsAt: number | null, messageUrl: string, inviteUrl: string,
  ): Promise<void> {
    if (!users.length || !this.botManager) return;

    // Adaptive batching based on user count
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
    
    if (sent > 0) {
      this.log.debug(`Sent ${sent} watchlist DMs (${failed} failed)`, {
        component: 'GiveawayManager',
        account: this.accountLabel
      });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // BACKGROUND REFRESHERS
  // ═══════════════════════════════════════════════════════════════════════

  private startWatchlistRefresher(): void {
    if (this.watchlistRefreshInterval) clearInterval(this.watchlistRefreshInterval);
    this.watchlistRefreshInterval = setInterval(() => {
      this.watchlistCacheExpiry = 0;
    }, WATCHLIST_CACHE_TTL);
    if (this.watchlistRefreshInterval.unref) this.watchlistRefreshInterval.unref();
  }

  // ═══════════════════════════════════════════════════════════════════════
  // INVITE GENERATION WITH FAILED CACHE
  // ═══════════════════════════════════════════════════════════════════════

  private async fetchInviteForGuild(guildId: string): Promise<string> {
    const now = Date.now();

    const failedUntil = this.failedInviteCache.get(guildId);
    if (failedUntil && failedUntil > now) {
      return `https://discord.com/channels/${guildId}`;
    }

    const cached = this.inviteCache.get(guildId);
    if (cached && cached.expiresAt > now) return cached.url;

    const pending = this.pendingInvites.get(guildId);
    if (pending) return pending;

    const promise = this.doFetchInvite(guildId, now);
    this.pendingInvites.set(guildId, promise);

    try {
      return await promise;
    } finally {
      this.pendingInvites.delete(guildId);
    }
  }

  private async doFetchInvite(guildId: string, now: number): Promise<string> {
    try {
      const guild = this.client.guilds.cache.get(guildId);
      if (!guild) { this.cacheFailedInvite(guildId, now); return `https://discord.com/channels/${guildId}`; }

      try {
        const invites = await guild.invites.fetch();
        if (invites?.size) {
          const permanent = invites.find(inv => inv.maxAge === 0 && inv.maxUses === 0);
          const url = permanent?.url || invites.first()?.url;
          if (url) { this.cacheInvite(guildId, url, now); return url; }
        }
      } catch {
        // Ignore fetch errors
      }

      try {
        const vanity = (guild as any).vanityURLCode;
        if (vanity) { const url = `https://discord.gg/${vanity}`; this.cacheInvite(guildId, url, now); return url; }
      } catch {
        // Ignore
      }

      const textChannels = guild.channels.cache.filter(
        (ch): ch is TextChannel => ch.type === 'GUILD_TEXT'
      );
      if (!textChannels.size) { this.cacheFailedInvite(guildId, now); return `https://discord.com/channels/${guildId}`; }

      let botMember = guild.members.cache.get(this.selfUserId);
      if (!botMember) {
        try {
          botMember = await guild.members.fetch(this.selfUserId);
        } catch (fetchErr) {
          this.log.debug(`Could not fetch self member for invite in ${guildId}: ${formatError(fetchErr)}`);
          this.cacheFailedInvite(guildId, now);
          return `https://discord.com/channels/${guildId}`;
        }
      }
      if (!botMember) { this.cacheFailedInvite(guildId, now); return `https://discord.com/channels/${guildId}`; }

      for (const [, channel] of textChannels) {
        try {
          if (!channel.permissionsFor(botMember)?.has('CREATE_INSTANT_INVITE')) continue;
          const invite = await channel.createInvite({ maxAge: 0, maxUses: 0, reason: 'Giveaway tracker', temporary: false });
          this.cacheInvite(guildId, invite.url, now);
          return invite.url;
        } catch {
          // Ignore
        }
      }

      for (const [, channel] of textChannels) {
        try {
          const invite = await channel.createInvite({ maxAge: 0, maxUses: 0, reason: 'Giveaway tracker (fallback)', temporary: false });
          this.cacheInvite(guildId, invite.url, now);
          return invite.url;
        } catch {
          // Ignore
        }
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
  // STATS
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

  public getStats() {
    return { 
      ...this.stats, 
      uptime: Date.now() - this.stats.startedAt, 
      guildStats: this.guildStats.size,
      readyEventReceived: this.readyEventReceived,
    };
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
    this.log.info(`  Startup skipped     : ${s.startupMessagesSkipped}`);
    this.log.info(`  Scrims detected     : ${s.scrimsDetected}`);
    this.log.info(`  Scrims notified     : ${s.scrimsNotified}`);
    this.log.info(`  Uptime              : ${Math.floor(uptime / 60)}m ${Math.floor(uptime % 60)}s`);
    this.log.info(`  Invites cached      : ${this.inviteCache.size}`);
    this.log.info(`  Failed invites      : ${this.failedInviteCache.size}`);
    this.log.info(`  Watchlist items     : ${this.totalWatchlistItems}`);
    this.log.info(`  Guilds tracked      : ${this.guildStats.size}`);
    this.log.info(`  Parse cache size    : ${parsedMessageCache.size}`);
    this.log.info(`  Ready received      : ${this.readyEventReceived}`);
    this.log.info(`────────────────────────────────────────────────────────`);

    if (this.guildStats.size > 0) {
      const top = Array.from(this.guildStats.entries())
        .sort((a, b) => b[1].detected - a[1].detected)
        .slice(0, 5);
      this.log.info('  Top guilds:');
      for (const [guildId, gs] of top) {
        const guild = this.client.guilds.cache.get(guildId);
        this.log.info(`    ${guild?.name || guildId}: ${gs.detected}d/${gs.notified}n/${gs.falsePositives}fp`);
      }
    }
  }

  public resetStats(): void {
    this.stats = {
      detected: 0, notified: 0, skipped: 0, errors: 0,
      falsePositivesBlocked: 0, watchlistMatches: 0, draftsSkipped: 0,
      startedAt: Date.now(),
      startupMessagesSkipped: 0,
      scrimsDetected: 0,
      scrimsNotified: 0,
    };
    this.guildStats.clear();
  }

  public getCacheStats(): {
    creationCacheSize: number;
    inviteCacheSize: number;
    failedInviteCacheSize: number;
    duplicateCacheSize: number;
    parseCacheSize: number;
  } {
    return {
      creationCacheSize: this.creationCache.size,
      inviteCacheSize: this.inviteCache.size,
      failedInviteCacheSize: this.failedInviteCache.size,
      duplicateCacheSize: this.duplicateCache.size,
      parseCacheSize: parsedMessageCache.size,
    };
  }

  // ═══════════════════════════════════════════════════════════════════════
  // SHUTDOWN - FIXED: Clean up startup timer
  // ═══════════════════════════════════════════════════════════════════════

  public async shutdown(): Promise<void> {
    if (this.inviteRefresherInterval) { 
      clearInterval(this.inviteRefresherInterval); 
      this.inviteRefresherInterval = null; 
    }
    if (this.watchlistRefreshInterval) { 
      clearInterval(this.watchlistRefreshInterval); 
      this.watchlistRefreshInterval = null; 
    }
    if (this.startupGraceTimer) { 
      clearTimeout(this.startupGraceTimer);
      this.startupGraceTimer = null; 
    }
    
    // Clear caches
    this.creationCache.clear();
    this.inviteCache.clear();
    this.failedInviteCache.clear();
    this.duplicateCache.clear();
    this.processingMessages.clear();
    this.pendingStartupMessages.clear();
    
    // Clear parsed message cache
    parsedMessageCache.clear();
    
    this.log.info(`Shutting down ${this.accountLabel}...`, { 
      component: 'GiveawayManager',
      stats: this.stats 
    });
    this.logStats();
  }
}

export default GiveawayManager;
