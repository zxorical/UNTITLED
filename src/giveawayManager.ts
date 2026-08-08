/**
 * @module giveawayManager
 * Reliable giveaway detector — scans everything, misses nothing.
 * 
 * FIXES APPLIED:
 * 1-19. [All original fixes preserved]
 * 20. FIXED: NA false positives - contextual NA matching only, anti-patterns, proximity validation
 * 21. FIXED: Giveaways ONLY process from allowed giveaway bot (530082442967646230)
 * 22. FIXED: Scrims have keyword pre-filter for performance
 * 23. FIXED: Detection time uses actual processing time (performance.now)
 * 24. FIXED: Watchlist DM passes correct processing time
 * 25. SECURITY: Removed rawContent from scrim notification payload
 * 26. FIXED: Scrim throttling per channel to prevent detection spam
 * 27. FIXED: Region scoring split into confirmed/weak
 * 28. FIXED: Scrim threshold increased from 6 to 7
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

// Region patterns - FIXED: No loose \bNA\b, only contextual NA matching
const REGION_PATTERNS: RegExp[] = [
  /EU\s*[Xx×]\s*NA/i,
  /NA\s*[Xx×]\s*EU/i,
  /EU\s*ONLY/i,
  /NA\s*ONLY/i,
  /Region:\s*(?:EU|NA)\b/i,
  /Server(?:s)?:\s*(?:EU|NA)\b/i,
  /\b(?:EU|NA)\s+(?:EST|EDT|PST|PDT|CST|CDT|GMT|UTC)\b/i,
  /\bEU\b/i,
  /\bNA\s+(?:Region|Server|Host|Team|Scrim)\b/i,
];

// Tick patterns
const TICK_PATTERNS: RegExp[] = [
  /Ticks?:\s*(\d+)\s*\+/i,
  /(\d+)\s*\+\s*Ticks?/i,
  /#\s*(\d+)\s*\+\s*Ticks?/i,
  /Ticks?:\s*(\d+)/i,
];

// Scrim score weights - FIXED: Split region into confirmed/weak
const SCRIM_SCORE = {
  HAS_EVERYONE: 3,
  HAS_HOST: 3,
  HAS_TIME: 3,
  HAS_TEAMS: 2,
  HAS_REWARD: 2,
  HAS_REGION_CONFIRMED: 2,
  HAS_REGION_WEAK: 1,
  HAS_TICKS: 1,
  TITLE_KEYWORD: 2,
};

const MAX_SCRIM_SCORE = Object.values(SCRIM_SCORE).reduce((a, b) => a + b, 0);
const MINIMUM_SCRIM_SCORE_THRESHOLD = 5;

// ─── NA False Positive Anti-Patterns ────────────────────────────────────
const NA_FALSE_POSITIVE_PATTERNS: RegExp[] = [
  /(?:gon|wan|gun|can|don|isn|aren|doesn|didn|haven|hasn|wouldn|couldn|shouldn|mightn|mustn)'?na\b/i,
  /\bna\s+(?:maybe|perhaps|possibly|idk|not\s+sure)\b/i,
  /\b(?:is|are|was|were)\s+there\s+(?:any|a)\s+na\b/i,
  /\bna\s+(?:bro|man|dude|fam|mate|buddy|bruh)\b/i,
  /\bna\s+(?:that|this|what|why|how|when|where)\b/i,
  /\bna\s+bc\b/i,
  /\bna\s+fr\b/i,
  /^na[\s,.]/i,
  /[\s,.]na$/i,
  /^na$/i,
  /^(?:yeah|yes|no|ok|okay|maybe|idk)[\s,]+na$/i,
  /\bN\.?\s*A\.?\b/,
  /\bsodium\b/i,
  /\bna\s*\+/i,
  /\bna\s*-/i,
  /\bna\s*cl\b/i,
];

// ─── Event Context Keywords ─────────────────────────────────────────────
const EVENT_CONTEXT_WORDS = [
  'host:', 'hosts:', 'reward:', 'prize:', 'time:',
  'teams:', 'team:', 'region:', '3v3', '2v2', '4v4',
  'ticks:', '#', 'perms', '@everyone', 'winners',
  'join', 'participate', 'sign up', 'register',
  'tournament', 'event', 'competition',
];

const REGION_CONTEXT_KEYWORDS = [
  'region', 'server', 'host', 'team', 'scrim',
  'eu', 'na x', 'x na', 'only', 'reward', 'time',
  'tournament', 'event', 'sign', 'register', 'join',
];

// ─── Tracker indicators for blocking other trackers ──────────────────────
const TRACKER_INDICATORS = [
  'giveaway tracker',
  'worth joining',
  'custom giveaway ping',
  'type:',
  'winners:',
  'server invite',
  'jump to giveaway',
  'made by',
  'detected in',
  'votes:',
  'created by',
  'powered by',
];

// ─── Timezone Offsets ────────────────────────────────────────────────────
const TZ_OFFSETS: Record<string, number> = {
  'EST': -5, 'EDT': -4,
  'PST': -8, 'PDT': -7,
  'CST': -6, 'CDT': -5,
  'MT': -7, 'MDT': -6,
  'GMT': 0, 'UTC': 0,
  'UK': 0, 'EU': 1,
  'ET': -5, 'PT': -8, 'CT': -6,
};

// ─── Scrim Throttling ────────────────────────────────────────────────────
const SCRIM_THROTTLE_MAX = 10;
const SCRIM_THROTTLE_WINDOW_MS = 3600000; // 1 hour

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
// TIME PARSER FOR SCRIM/EVENT NOTIFICATIONS
// ============================================================================

/**
 * Parse a human-written time string into a Unix timestamp
 * Supports: "6 pm EST", "15:00 uk time", "8:45 am est", "Tonight at 8:00 PM EU", "12:30", "8:00 PM EU"
 */
function parseScrimTime(timeStr: string): number | null {
  if (!timeStr || timeStr.length < 2) return null;
  
  const clean = timeStr.trim().toLowerCase();
  
  let hour: number | null = null;
  let minute: number | null = null;
  let isPM: boolean | null = null;
  let timezone: string | null = null;
  
  // Extract timezone
  const tzMatch = clean.match(/\b(est|edt|pst|pdt|cst|cdt|gmt|utc|uk|eu|et|pt|ct|mt)\b/i);
  if (tzMatch) {
    timezone = tzMatch[1].toUpperCase();
  }
  
  // Try 12-hour format: "6 pm", "8:45 am"
  const twelveHourMatch = clean.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i);
  if (twelveHourMatch) {
    hour = parseInt(twelveHourMatch[1], 10);
    minute = twelveHourMatch[2] ? parseInt(twelveHourMatch[2], 10) : 0;
    isPM = twelveHourMatch[3].toLowerCase() === 'pm';
  }
  
  // Try 24-hour format: "15:00", "14:30"
  if (hour === null) {
    const twentyFourMatch = clean.match(/(\d{1,2}):(\d{2})/);
    if (twentyFourMatch) {
      hour = parseInt(twentyFourMatch[1], 10);
      minute = parseInt(twentyFourMatch[2], 10);
      isPM = hour >= 12;
    }
  }
  
  // Try single hour with am/pm: "8pm"
  if (hour === null) {
    const singleHourMatch = clean.match(/(\d{1,2})\s*(am|pm)/i);
    if (singleHourMatch) {
      hour = parseInt(singleHourMatch[1], 10);
      minute = 0;
      isPM = singleHourMatch[2].toLowerCase() === 'pm';
    }
  }
  
  if (hour === null) {
    return null;
  }
  
  // Convert to 24-hour
  if (isPM && hour < 12) {
    hour += 12;
  } else if (!isPM && hour === 12) {
    hour = 0;
  }
  
  // Determine date
  const now = new Date();
  const targetDate = new Date(now);
  targetDate.setHours(hour, minute || 0, 0, 0);
  
  // If time is in the past, assume it's tomorrow
  if (targetDate.getTime() < now.getTime()) {
    targetDate.setDate(targetDate.getDate() + 1);
  }
  
  // Apply timezone offset
  if (timezone) {
    const offsetHours = TZ_OFFSETS[timezone];
    if (offsetHours !== undefined) {
      targetDate.setHours(targetDate.getHours() - offsetHours);
    }
  }
  
  return Math.floor(targetDate.getTime() / 1000);
}

/**
 * Format a time string into a Discord relative timestamp
 */
function formatScrimTime(timeStr: string | null): string {
  if (!timeStr) return 'Unknown';
  const timestamp = parseScrimTime(timeStr);
  if (timestamp === null) {
    return timeStr;
  }
  return `<t:${timestamp}:R>`;
}

/**
 * Format a time string into a Discord full date timestamp
 */
function formatScrimTimeFull(timeStr: string | null): string {
  if (!timeStr) return 'Unknown';
  const timestamp = parseScrimTime(timeStr);
  if (timestamp === null) {
    return timeStr;
  }
  return `<t:${timestamp}:F>`;
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
      const timeStr = match[1].trim();
      // Validate it actually looks like a time
      if (/\d/.test(timeStr) && (timeStr.includes(':') || timeStr.includes('am') || timeStr.includes('pm') || timeStr.includes('EST') || timeStr.includes('UTC') || timeStr.includes('GMT') || timeStr.includes('UK') || timeStr.includes('EU'))) {
        return timeStr;
      }
    }
  }
  return null;
}

function extractScrimReward(text: string): string | null {
  for (const pattern of REWARD_PATTERNS) {
    const match = text.match(pattern);
    if (match && match[1]) {
      const rewardStr = match[1].trim();
      // Validate it's not a single character or generic
      if (rewardStr.length > 2 && rewardStr !== 'W' && rewardStr !== 'N/A' && rewardStr !== 'TBD') {
        return rewardStr;
      }
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

// ─── FIXED: Region extraction with NA validation ─────────────────────────

function isNAFalsePositive(text: string): boolean {
  for (const pattern of NA_FALSE_POSITIVE_PATTERNS) {
    if (pattern.test(text)) return true;
  }
  return false;
}

function isValidRegionContext(text: string, regionMatch: string): boolean {
  const lowerText = text.toLowerCase();
  const regionPos = lowerText.indexOf(regionMatch.toLowerCase());
  
  if (regionPos === -1) return false;
  
  // Check for scrim-related keywords within 80 characters of the region match
  const contextWindow = lowerText.substring(
    Math.max(0, regionPos - 80),
    Math.min(lowerText.length, regionPos + 80)
  );
  
  let keywordCount = 0;
  for (const keyword of REGION_CONTEXT_KEYWORDS) {
    if (contextWindow.includes(keyword)) keywordCount++;
    if (keywordCount >= 2) return true;
  }
  
  return false;
}

function extractScrimRegion(text: string): string | null {
  for (const pattern of REGION_PATTERNS) {
    const match = text.match(pattern);
    if (match) {
      const regionText = match[0];
      
      // Skip if the match is from an NA false positive pattern
      if (isNAFalsePositive(text)) {
        continue;
      }
      
      // Validate region appears in proper scrim context
      if (!isValidRegionContext(text, regionText)) {
        continue;
      }
      
      return regionText;
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

// ─── FIXED: Event context and structure validation ──────────────────────

function hasEventContext(text: string): boolean {
  const lowerText = text.toLowerCase();
  let matchCount = 0;
  for (const word of EVENT_CONTEXT_WORDS) {
    if (lowerText.includes(word)) matchCount++;
    if (matchCount >= 2) return true;
  }
  return false;
}

function hasScrimStructure(text: string, type: 'scrim' | 'squid_game' | 'gagaball'): boolean {
  const lines = text.split('\n').filter(line => line.trim().length > 0);
  
  // Scrim announcements usually have multiple lines of structured info
  if (lines.length < 3) return false;
  
  // Check for common structural patterns (colon-separated fields)
  const colonLines = lines.filter(line => /[A-Z][a-z]+:\s*.+/i.test(line));
  
  if (type === 'scrim') {
    return colonLines.length >= 2;
  }
  // squid_game and gagaball
  return colonLines.length >= 1;
}

function isOwnNotification(message: Message): boolean {
  const content = message.content || '';
  
  // Check for our notification titles
  if (content.includes('Scrim Detected') || 
      content.includes('Event Detected') || 
      content.includes('Squid Game Detected') ||
      content.includes('Gagaball Detected') ||
      content.includes('New Giveaway')) {
    return true;
  }
  
  const embed = message.embeds?.[0];
  if (embed) {
    // Our notification colors
    const botColors = [0x5865F2, 0xFF6B6B, 0x4ECDC4, 0x00AAFF, 0xFFD700];
    
    // Check color
    if (embed.color !== null && botColors.includes(embed.color)) {
      // Check footer - our notifications have "Detected in Xms"
      if (embed.footer?.text && 
          embed.footer.text.includes('Detected in') && 
          embed.footer.text.includes('ms')) {
        return true;
      }
      
      // Check author - our notifications have "Scrim Detected" etc.
      if (embed.author?.name && 
          (embed.author.name.includes('Detected') || 
           embed.author.name.includes('Giveaway'))) {
        return true;
      }
    }
    
    // Check description for our patterns
    if (embed.description) {
      const desc = embed.description.toLowerCase();
      if (desc.includes('server:') && 
          (desc.includes('view message') || desc.includes('join server'))) {
        return true;
      }
    }
  }
  
  return false;
}

function isTrackerMessage(message: Message): boolean {
  const content = (message.content || '').toLowerCase();
  const embed = message.embeds?.[0];
  
  // Check content
  for (const indicator of TRACKER_INDICATORS) {
    if (content.includes(indicator)) {
      return true;
    }
  }
  
  // Check embed
  if (embed) {
    const embedText = [
      embed.title || '',
      embed.description || '',
      embed.footer?.text || '',
      ...(embed.fields?.map(f => f.name + ' ' + f.value) || [])
    ].join(' ').toLowerCase();
    
    for (const indicator of TRACKER_INDICATORS) {
      if (embedText.includes(indicator)) {
        return true;
      }
    }
  }
  
  return false;
}

// ─── FIXED: Complete scrim detection with all validations ────────────────

function detectScrim(parsed: ParsedGiveawayData): ScrimDetectionResult | null {
  const { lowerText, fullText } = parsed;
  
  // Skip if it's obviously a bot notification
  if (lowerText.includes('scrim detected') || lowerText.includes('event detected')) return null;
  if (lowerText.includes('view message') && lowerText.includes('join server')) return null;
  if (lowerText.includes('detected in') && lowerText.includes('ms')) return null;
  
  // Skip if it's obviously a giveaway
  if (lowerText.includes('giveaway') && (lowerText.includes('winner') || lowerText.includes('entered'))) {
    return null;
  }

  // First check if it's a scrim/event type
  const type = detectScrimType(fullText);
  if (!type) return null;

  // NEW: Check for event context - must have at least 2 event-related words
  if (!hasEventContext(fullText)) return null;

  // NEW: Check for scrim announcement structure
  if (!hasScrimStructure(fullText, type)) return null;

  // Extract data
  const host = extractScrimHost(fullText);
  const coHost = extractScrimCoHost(fullText);
  const time = extractScrimTime(fullText);
  const reward = extractScrimReward(fullText);
  const teams = extractScrimTeams(fullText);
  const region = extractScrimRegion(fullText);
  const ticks = extractScrimTicks(fullText);
  const hasEveryone = lowerText.includes('@everyone') || lowerText.includes('@here');

  // ─── TYPE-SPECIFIC VALIDATION ───
  
  // SCRIM: Require 2+ of: Host, Time, Teams, @everyone
  if (type === 'scrim') {
    let fields = 0;
    if (host) fields++;
    if (time) fields++;
    if (teams) fields++;
    if (hasEveryone) fields++;
    if (region) fields++;
    
    // Must have at least 2 fields
    if (fields < 2) return null;
  }

  // SQUID GAME: Require (Host OR Time) AND Reward
  if (type === 'squid_game') {
    if (!host && !time) return null;
    if (!reward) return null;
  }

  // GAGABALL: Require Time AND Reward
  if (type === 'gagaball') {
    if (!time) return null;
    if (!reward) return null;
    
    // NEW: Verify the time actually parses to a valid future timestamp
    const parsedTimestamp = parseScrimTime(time);
    if (parsedTimestamp === null) return null;
    
    // NEW: Time must be within reasonable range (not years in future)
    const maxFutureMs = 7 * 24 * 60 * 60 * 1000; // 7 days max
    if (parsedTimestamp * 1000 - Date.now() > maxFutureMs) return null;
  }

  // ─── CALCULATE SCORE ───
  let score = 0;
  const signals: string[] = [];

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

  // FIXED: Split region scoring into confirmed vs weak
  if (region) {
    const isConfirmedRegion = /(?:EU|NA)\s*[Xx×]\s*(?:NA|EU)|(?:EU|NA)\s+ONLY|Region:\s*(?:EU|NA)|Server(?:s)?:\s*(?:EU|NA)/i.test(fullText);
    
    if (isConfirmedRegion) {
      score += SCRIM_SCORE.HAS_REGION_CONFIRMED;
      signals.push('region_confirmed');
    } else {
      score += SCRIM_SCORE.HAS_REGION_WEAK;
      signals.push('region_weak');
    }
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

  // ─── FINAL THRESHOLD CHECK ───
  // FIXED: Increased scrim threshold from 6 to 7
  const requiredScore = type === 'scrim' ? 7 : 5;
  
  if (score < requiredScore) {
    return null;
  }

  // Skip if it's just a single word mention
  const words = fullText.split(/\s+/).filter(w => w.length > 2 && !/^<@!?\d+>$/.test(w));
  if (words.length < 4 && score < 7) {
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
// QUICK REJECT (Stage 1) - FIXED: Smart bot filter + webhook detection
// ═══════════════════════════════════════════════════════════════════════════

function quickReject(message: Message, selfUserId: string, now: number): string | null {
  if (!message.guild) return 'no_guild';
  
  // Skip self messages (the bot's own messages)
  if (message.author?.id === selfUserId) return 'self';
  
  // Skip if it's the bot's own notification
  if (isOwnNotification(message)) return 'self_notification';

  // ─── BLOCK ALL WEBHOOK MESSAGES ───
  // Webhooks are almost always automated messages from other bots/trackers
  // We want to block ALL webhooks to prevent detecting other trackers
  if (message.webhookId) {
    return 'webhook_blocked';
  }

  // ─── SMART BOT FILTER ───
  if (message.author?.bot) {
    // ALLOW: The official giveaway bot (530082442967646230)
    if (ALLOWED_GIVEAWAY_BOT_IDS.has(message.author.id)) {
      // Allow this bot - it's the real giveaway bot
    } else {
      // BLOCK: All other bots
      return 'not_allowed_bot';
    }
  }

  // ─── AGE CHECK ───
  const messageAge = now - message.createdTimestamp;
  if (messageAge > MAX_MESSAGE_AGE_MS) {
    return 'too_old';
  }

  if (CONFIG.monitoredChannels.length > 0 && !CONFIG.monitoredChannels.includes(message.channel.id)) {
    return 'not_monitored';
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
// GIVEAWAY MANAGER - FIXED: Two-tier detection, accurate timing, throttling
// ═══════════════════════════════════════════════════════════════════════════

interface ScrimHistoryEntry {
  recentDetections: number[];
  falsePositiveCount: number;
  cooldownUntil: number;
}

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

  // ─── FIXED: Scrim throttling history ────────────────────────────────
  private scrimHistory = new Map<string, ScrimHistoryEntry>();
  private scrimCleanupInterval: NodeJS.Timeout | null = null;

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
    startupMessagesSkipped: 0,
    scrimsDetected: 0,
    scrimsNotified: 0,
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
    this.startScrimCleanup();
    this.setupReadyHandler();
  }

  // ═══════════════════════════════════════════════════════════════════════
  // NEW: Startup Ready Handler
  // ═══════════════════════════════════════════════════════════════════════

  private setupReadyHandler(): void {
    this.client.once('ready', () => {
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
  // SCRIM THROTTLING - FIXED: Dynamic cooldown per channel
  // ═══════════════════════════════════════════════════════════════════════

  private shouldThrottleScrim(guildId: string, channelId: string): boolean {
    const key = `${guildId}:${channelId}`;
    const now = Date.now();
    
    let history = this.scrimHistory.get(key);
    if (!history) {
      history = { recentDetections: [], falsePositiveCount: 0, cooldownUntil: 0 };
      this.scrimHistory.set(key, history);
    }
    
    // Check if channel is in cooldown
    if (now < history.cooldownUntil) {
      return true;
    }
    
    // Clean old detections (older than 1 hour)
    history.recentDetections = history.recentDetections.filter(
      ts => now - ts < SCRIM_THROTTLE_WINDOW_MS
    );
    
    // If too many detections in short time, increase cooldown
    if (history.recentDetections.length > SCRIM_THROTTLE_MAX) {
      const cooldownMs = Math.min(
        300000, // Max 5 minutes
        history.recentDetections.length * 30000 // 30 seconds per detection
      );
      history.cooldownUntil = now + cooldownMs;
      
      // Track as potential false positive burst
      history.falsePositiveCount++;
      
      this.log.debug(`Throttling scrim detection in channel ${channelId}`, {
        detectionRate: history.recentDetections.length,
        cooldownMs,
        falsePositiveCount: history.falsePositiveCount
      });
      
      return true;
    }
    
    // Record this detection
    history.recentDetections.push(now);
    return false;
  }

  private startScrimCleanup(): void {
    this.scrimCleanupInterval = setInterval(() => {
      const now = Date.now();
      for (const [key, history] of this.scrimHistory) {
        // Remove channels with no recent activity
        if (history.recentDetections.length === 0 && 
            now > history.cooldownUntil) {
          this.scrimHistory.delete(key);
        }
        
        // Reset false positive count for channels with no recent issues
        if (history.falsePositiveCount > 0 && 
            history.recentDetections.every(ts => now - ts > 7200000)) { // 2 hours
          history.falsePositiveCount = Math.max(0, history.falsePositiveCount - 1);
        }
      }
    }, 600000); // Run every 10 minutes
    
    if (this.scrimCleanupInterval.unref) {
      this.scrimCleanupInterval.unref();
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // SCRIM NOTIFICATION - FIXED: Accurate processing time, removed rawContent
  // ═══════════════════════════════════════════════════════════════════════

  private async sendScrimNotification(
    message: Message,
    parsed: ParsedGiveawayData,
    scrimResult: ScrimDetectionResult,
    processingTime: number
  ): Promise<void> {
    if (!this.botManager) return;

    const guild = message.guild!;
    const guildIcon = guild.iconURL({ size: 512 }) || null;
    const guildBanner = (guild as any).bannerURL?.({ size: 1024 }) || null;
    const memberCount = (guild as any).memberCount ?? null;

    const typeLabel = {
      scrim: 'Scrim',
      squid_game: 'Squid Game',
      gagaball: 'Gagaball',
    }[scrimResult.type];

    const formattedTime = scrimResult.time ? formatScrimTime(scrimResult.time) : '';
    const formattedTimeFull = scrimResult.time ? formatScrimTimeFull(scrimResult.time) : '';

    const description = [
      '### Details',
      scrimResult.host ? `**Host:** ${scrimResult.host}` : '',
      scrimResult.coHost ? `**Co-Host:** ${scrimResult.coHost}` : '',
      scrimResult.time ? `**Time:** ${formattedTime}` : '',
      scrimResult.time ? `**When:** ${formattedTimeFull}` : '',
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

    const inviteUrl = await this.fetchInviteForGuild(guild.id);

    try {
      // FIXED: Use processingTime for accurate detection time, removed rawContent
      const sent = await this.botManager.sendScrimNotification({
        messageId: message.id,
        channelId: message.channel.id,
        guildId: guild.id,
        guildName: guild.name,
        channelName: (message.channel as any).name || 'unknown',
        authorId: message.author?.id || '',
        prize: scrimResult.reward || `${typeLabel} Event`,
        detectedAt: message.createdTimestamp,
        detectionTimeMs: processingTime,
        endsAt: null,
        status: 'active',
        notifiedAt: null,
        lastSeenAt: message.createdTimestamp,
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
      `Scrim: ${typeLabel} [${scrimResult.confidence}%] ` +
      `(processing: ${processingTime}ms) - ` +
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
  // PUBLIC API - FIXED: Two-tier detection, accurate timing
  // ═══════════════════════════════════════════════════════════════════════

  public async handleMessage(message: Message): Promise<void> {
    const now = Date.now();
    const processingStart = performance.now();

    if (!this.readyEventReceived) {
      const messageAge = now - message.createdTimestamp;
      if (messageAge > MAX_STARTUP_MESSAGE_AGE_MS) {
        return;
      }
      this.pendingStartupMessages.add(message.id);
    }

    if (now - message.createdTimestamp > MAX_MESSAGE_AGE_MS) {
      return;
    }

    const rejectReason = quickReject(message, this.selfUserId, now);
    if (rejectReason) return;

    const key = `${message.id}-${message.channel.id}`;
    if (this.processingMessages.has(key)) return;
    this.processingMessages.add(key);

    try {
      let parsed = parseMessage(message, now);

      // ─── TIER 1: GIVEAWAY DETECTION (giveaway bot only) ──────────
      const isGiveawayBot = ALLOWED_GIVEAWAY_BOT_IDS.has(message.author?.id || '');
      
      if (isGiveawayBot && message.author?.bot) {
        if (isBlockedContent(parsed)) { this.stats.falsePositivesBlocked++; return; }

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
        if (isDraftGiveaway(parsed)) { this.stats.draftsSkipped++; return; }

        const detection = calculateGiveawayScore(parsed);
        if (detection.score >= MINIMUM_SCORE_THRESHOLD) {
          const messageDupKey = `${message.id}:${message.channel.id}`;
          if (this.duplicateCache.get(messageDupKey)) return;
          this.duplicateCache.set(messageDupKey, now);

          const existing = await getGiveaway(message.id, message.channel.id);
          if (existing) {
            await updateLastSeen(message.id, message.channel.id);
            if (existing.status === 'active' && isEndedGiveaway(parsed)) {
              await markEnded(message.id, message.channel.id);
            }
            return;
          }

          if (await wasNotifiedRecently(message.id, message.channel.id, CONFIG.notificationCooldown)) {
            this.stats.skipped++; return;
          }

          this.stats.detected++;
          this.recordGuildStat(message.guild!.id, 'detected');

          // FIXED: Use actual processing time
          const processingTime = Math.round(performance.now() - processingStart);
          const guild = message.guild!;
          const guildIcon = guild.iconURL({ size: 512 }) || null;
          const guildBanner = (guild as any).bannerURL?.({ size: 1024 }) || null;
          const memberCount = (guild as any).memberCount ?? null;

          const data: Omit<GiveawayData, 'id' | 'status' | 'notifiedAt' | 'lastSeenAt'> = {
            messageId: message.id, channelId: message.channel.id,
            guildId: guild.id, guildName: guild.name,
            channelName: (message.channel as any).name || 'unknown',
            authorId: parsed.botId, prize: parsed.prize,
            detectedAt: message.createdTimestamp,
            endsAt: parsed.timestamps.end,
            detectionTimeMs: processingTime,
            guildIcon, guildBanner, memberCount,
          };

          const savePromise = insertGiveaway(data);
          const invitePromise = this.fetchInviteForGuild(guild.id);
          const watchlistPromise = this.checkWatchlistMatches(parsed, message, invitePromise, processingTime);

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
            `(processing: ${processingTime}ms) - ` +
            detection.signals.join(', ')
          );

          await watchlistPromise;
          return;
        }
        
        // Giveaway bot message but didn't meet threshold
        this.stats.falsePositivesBlocked++;
        return;
      }

      // ─── TIER 2: SCRIM/EVENT DETECTION (non-bot messages) ─────────
      if (!parsed.isFromBot) {
        // Fast pre-check: skip if no scrim-related keywords at all
        if (!/scrim|squid|gaga|event|host|reward|prize|team|region/i.test(parsed.lowerText)) {
          this.stats.falsePositivesBlocked++;
          return;
        }

        const scrimResult = detectScrim(parsed);
        if (scrimResult && scrimResult.score >= MINIMUM_SCRIM_SCORE_THRESHOLD) {
          const scrimDupKey = `scrim:${message.id}:${message.channel.id}`;
          if (this.duplicateCache.get(scrimDupKey)) return;
          
          // FIXED: Check for scrim throttling
          if (this.shouldThrottleScrim(message.guild!.id, message.channel.id)) {
            this.stats.falsePositivesBlocked++;
            return;
          }
          
          this.duplicateCache.set(scrimDupKey, now);

          this.stats.detected++;
          this.stats.scrimsDetected++;
          this.recordGuildStat(message.guild!.id, 'detected');

          const processingTime = Math.round(performance.now() - processingStart);
          await this.sendScrimNotification(message, parsed, scrimResult, processingTime);
          return;
        }

        this.stats.falsePositivesBlocked++;
        if (this.shouldLogDebug()) {
          this.log.debug('Below thresholds', {
            mid: message.id,
            scrimScore: scrimResult?.score || 0,
          });
        }
        return;
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
    processingTime: number,
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

      // FIXED: Pass processingTime instead of Date.now()
      await this.sendWatchlistDMs(uniqueUsers, parsed.prize, message, parsed.timestamps.end, messageUrl, inviteUrl, processingTime);

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
    processingTime: number,
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
            processingTime, inviteUrl, guildBanner, memberCount,
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
  // SHUTDOWN - FIXED: Clean up startup timer + scrim cleanup
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
    if (this.scrimCleanupInterval) { 
      clearInterval(this.scrimCleanupInterval); 
      this.scrimCleanupInterval = null; 
    }
    if (this.startupGraceTimer) { 
      clearTimeout(this.startupGraceTimer);
      this.startupGraceTimer = null; 
    }
    
    this.creationCache.clear();
    this.inviteCache.clear();
    this.failedInviteCache.clear();
    this.duplicateCache.clear();
    this.processingMessages.clear();
    this.pendingStartupMessages.clear();
    this.scrimHistory.clear();
    
    parsedMessageCache.clear();
    
    this.log.info(`Shutting down ${this.accountLabel}...`, { 
      component: 'GiveawayManager',
      stats: this.stats 
    });
    this.logStats();
  }
}

export default GiveawayManager;
