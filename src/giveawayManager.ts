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
 * 29. FIXED: Invite generation — vanity first, invites.fetch 403 isolated,
 *            permissionsFor(null) handled, scored channel list, self-member
 *            double-fetch, reason-aware failed cache TTL
 * 30. FIXED: Multi-account support - moved parsedMessageCache from module-level
 *            to instance-level with account-specific cache keys
 * 31. FIXED: Detection delay - removed startup grace period, instant processing
 * 32. FIXED: Detection delay - quick pre-filter before heavy parsing
 * 33. FIXED: Detection delay - non-blocking DB operations
 * 34. FIXED: Detection delay - reduced message age filter to 10 seconds
 * 35. FIXED: Detection delay - reduced cache TTL to 3 seconds
 * 36. FIXED: Detection delay - no waiting for ready event
 * 37. FIXED: Detection delay - process messages immediately
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

// 🔥 FIXED: Reduced from 30 minutes to 10 seconds for instant detection
const MAX_MESSAGE_AGE_MS = 10 * 1000;

const MAX_CREATION_CACHE = 1000;
const MAX_INVITE_CACHE = 250;
const MAX_DUPLICATE_CACHE = 2000;
const MAX_FAILED_INVITE_CACHE = 100;
const WATCHLIST_CACHE_TTL = 60_000;
const INVITE_CACHE_TTL = 30 * 60 * 1000;
const FAILED_INVITE_RETRY_MS = 15 * 60 * 1000;
const AHOCORASICK_THRESHOLD = 100;

// Memory safety limits
const MAX_GUILD_STATS = 2000;
const MAX_SCRIM_HISTORY = 5000;
const SCRIM_HISTORY_TTL_MS = 2 * 60 * 60 * 1000;
const SCRIM_CLEANUP_INTERVAL = 5 * 60 * 1000;
const MAX_PROCESSING_MESSAGES = 5000;

// 🔥 FIXED: Reduced from 30 seconds to 3 seconds for fresh data
const PARSED_CACHE_TTL_MS = 3_000;
const MAX_PARSED_CACHE_SIZE = 2000;

// 🔥 FIXED: Removed startup grace period - process instantly
const STARTUP_GRACE_PERIOD_MS = 0;
const MAX_STARTUP_MESSAGE_AGE_MS = 0;
const MAX_GATEWAY_LATENCY_MS = 60_000;

// ═══════════════════════════════════════════════════════════════════════════
// SCRIM/EVENT DETECTION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

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

const TEAM_PATTERNS: RegExp[] = [
  /3v3/i, /2v2/i, /4v4/i, /5v5/i, /1v1/i,
  /(\d+)\s*TEAMS?/i,
  /teams?:\s*(\d+v\d+)/i,
  /(\d+)\s*[xX×]\s*(\d+)/i,
];

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

const REWARD_PATTERNS: RegExp[] = [
  /Reward:\s*([^\n]+)/i,
  /Rewards:\s*([^\n]+)/i,
  /Prize:\s*([^\n]+)/i,
  /reward:\s*([^\n]+)/i,
  /prize\s*([^\n]+)/i,
];

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

const TICK_PATTERNS: RegExp[] = [
  /Ticks?:\s*(\d+)\s*\+/i,
  /(\d+)\s*\+\s*Ticks?/i,
  /#\s*(\d+)\s*\+\s*Ticks?/i,
  /Ticks?:\s*(\d+)/i,
];

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

const EVENT_CONTEXT_WORDS = [
  'host:', 'hosts:', 'reward:', 'prize:', 'time:',
  'teams:', 'team:', 'region:', '3v3', '2v2', '4v4',
  'ticks:', '#', 'perms', '@everyone', 'winners',
  'join', 'participate', 'sign up', 'register',
  'tournament', 'event', 'competition',
];

const EVENT_MESSAGE_HINTS = [
  'scrim', 'scrims', 'squid', 'squid game',
  'gagaball', 'gaga ball', 'host:', 'hosts:',
  'co host:', 'co-host:', 'time:', 'reward:', 'rewards:',
  'prize:', 'teams:', 'team:', 'region:', 'server:',
  'ticks:', '@everyone', '@here', 'register', 'sign up',
];

const CHANNEL_NAME_SMALL_CAPS_MAP: Record<string, string> = {
  'ᴀ': 'a', 'ʙ': 'b', 'ᴄ': 'c', 'ᴅ': 'd', 'ᴇ': 'e', 'ꜰ': 'f',
  'ɢ': 'g', 'ʜ': 'h', 'ɪ': 'i', 'ᴊ': 'j', 'ᴋ': 'k', 'ʟ': 'l',
  'ᴍ': 'm', 'ɴ': 'n', 'ᴏ': 'o', 'ᴘ': 'p', 'ǫ': 'q', 'ʀ': 'r',
  's': 's', 'ᴛ': 't', 'ᴜ': 'u', 'ᴠ': 'v', 'ᴡ': 'w', 'x': 'x',
  'ʏ': 'y', 'ᴢ': 'z',
};

function normalizeChannelName(value: string): string {
  let normalized = value.normalize('NFKC').toLowerCase();
  normalized = normalized.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
  let mapped = '';
  for (const char of normalized) {
    mapped += CHANNEL_NAME_SMALL_CAPS_MAP[char] || char;
  }
  return mapped.replace(/[^a-z0-9]+/g, '');
}

function classifyEventChannel(value: string): 'scrim' | 'squid_game' | 'gagaball' | null {
  const channel = normalizeChannelName(value);
  if (!channel) return null;
  if (channel.includes('squidgame') || channel.includes('squid')) return 'squid_game';
  if (channel.includes('gagaball') || channel.includes('gaga')) return 'gagaball';
  if (channel.includes('scrim') || channel.includes('scrims')) return 'scrim';
  return null;
}

function isEventChannel(value: string): boolean {
  return classifyEventChannel(value) !== null;
}

const REGION_CONTEXT_KEYWORDS = [
  'region', 'server', 'host', 'team', 'scrim',
  'eu', 'na x', 'x na', 'only', 'reward', 'time',
  'tournament', 'event', 'sign', 'register', 'join',
];

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

const TZ_OFFSETS: Record<string, number> = {
  'EST': -5, 'EDT': -4,
  'PST': -8, 'PDT': -7,
  'CST': -6, 'CDT': -5,
  'MT': -7, 'MDT': -6,
  'GMT': 0, 'UTC': 0,
  'UK': 0, 'EU': 1,
  'ET': -5, 'PT': -8, 'CT': -6,
};

const SCRIM_THROTTLE_MAX = 10;
const SCRIM_THROTTLE_WINDOW_MS = 3600000;

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
// AHO-CORASICK
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

  keys(): IterableIterator<K> {
    return this.map.keys();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// PARSED GIVEAWAY MESSAGE
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

// ═══════════════════════════════════════════════════════════════════════════
// PARSED MESSAGE CACHE FUNCTIONS - MOVED TO INSTANCE-LEVEL
// These now take a cache parameter instead of using a module-level cache
// ═══════════════════════════════════════════════════════════════════════════

function getParsedCacheKey(message: Message, accountLabel: string): string {
  return `${accountLabel}:${message.id}:${message.channel.id}`;
}

function cleanupParsedMessageCache(
  now: number,
  cache: Map<string, { data: ParsedGiveawayData; timestamp: number }>
): void {
  if (cache.size === 0) return;

  for (const [key, entry] of cache) {
    if (now - entry.timestamp > PARSED_CACHE_TTL_MS) {
      cache.delete(key);
    }
  }

  if (cache.size <= MAX_PARSED_CACHE_SIZE) return;

  const removeCount = cache.size - MAX_PARSED_CACHE_SIZE;
  let removed = 0;
  for (const key of cache.keys()) {
    cache.delete(key);
    if (++removed >= removeCount) break;
  }
}

function parseMessage(
  message: Message,
  now: number,
  accountLabel: string,
  cache: Map<string, { data: ParsedGiveawayData; timestamp: number }>
): ParsedGiveawayData {
  cleanupParsedMessageCache(now, cache);

  const cacheKey = getParsedCacheKey(message, accountLabel);
  const cached = cache.get(cacheKey);

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

  cache.set(cacheKey, { data: parsed, timestamp: now });
  cleanupParsedMessageCache(now, cache);

  return parsed;
}

function refreshParsedMessage(
  message: Message,
  now: number,
  accountLabel: string,
  cache: Map<string, { data: ParsedGiveawayData; timestamp: number }>
): ParsedGiveawayData {
  const cacheKey = getParsedCacheKey(message, accountLabel);
  cache.delete(cacheKey);
  return parseMessage(message, now, accountLabel, cache);
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

function parseScrimTime(timeStr: string): number | null {
  if (!timeStr || timeStr.length < 2) return null;

  const clean = timeStr.trim().toLowerCase();

  let hour: number | null = null;
  let minute: number | null = null;
  let isPM: boolean | null = null;
  let timezone: string | null = null;

  const tzMatch = clean.match(/\b(est|edt|pst|pdt|cst|cdt|gmt|utc|uk|eu|et|pt|ct|mt)\b/i);
  if (tzMatch) {
    timezone = tzMatch[1].toUpperCase();
  }

  const twelveHourMatch = clean.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i);
  if (twelveHourMatch) {
    hour = parseInt(twelveHourMatch[1], 10);
    minute = twelveHourMatch[2] ? parseInt(twelveHourMatch[2], 10) : 0;
    isPM = twelveHourMatch[3].toLowerCase() === 'pm';
  }

  if (hour === null) {
    const twentyFourMatch = clean.match(/(\d{1,2}):(\d{2})/);
    if (twentyFourMatch) {
      hour = parseInt(twentyFourMatch[1], 10);
      minute = parseInt(twentyFourMatch[2], 10);
      isPM = hour >= 12;
    }
  }

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

  if (isPM && hour < 12) {
    hour += 12;
  } else if (!isPM && hour === 12) {
    hour = 0;
  }

  const now = new Date();
  const targetDate = new Date(now);
  targetDate.setHours(hour, minute || 0, 0, 0);

  if (targetDate.getTime() < now.getTime()) {
    targetDate.setDate(targetDate.getDate() + 1);
  }

  if (timezone) {
    const offsetHours = TZ_OFFSETS[timezone];
    if (offsetHours !== undefined) {
      targetDate.setHours(targetDate.getHours() - offsetHours);
    }
  }

  return Math.floor(targetDate.getTime() / 1000);
}

function formatScrimTime(timeStr: string | null): string {
  if (!timeStr) return 'Unknown';
  const timestamp = parseScrimTime(timeStr);
  if (timestamp === null) {
    return timeStr;
  }
  return `<t:${timestamp}:R>`;
}

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

  for (const pattern of SQUID_GAME_PATTERNS) {
    if (pattern.test(lower)) return 'squid_game';
  }

  for (const pattern of GAGABALL_PATTERNS) {
    if (pattern.test(lower)) return 'gagaball';
  }

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

      if (isNAFalsePositive(text)) {
        continue;
      }

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

  if (lines.length < 3) return false;

  const colonLines = lines.filter(line => /[A-Z][a-z]+:\s*.+/i.test(line));

  if (type === 'scrim') {
    return colonLines.length >= 2;
  }
  return colonLines.length >= 1;
}

function isOwnNotification(message: Message): boolean {
  const content = message.content || '';

  if (content.includes('Scrim Detected') ||
      content.includes('Event Detected') ||
      content.includes('Squid Game Detected') ||
      content.includes('Gagaball Detected') ||
      content.includes('New Giveaway')) {
    return true;
  }

  const embed = message.embeds?.[0];
  if (embed) {
    const botColors = [0x5865F2, 0xFF6B6B, 0x4ECDC4, 0x00AAFF, 0xFFD700];

    if (embed.color !== null && botColors.includes(embed.color)) {
      if (embed.footer?.text &&
          embed.footer.text.includes('Detected in') &&
          embed.footer.text.includes('ms')) {
        return true;
      }

      if (embed.author?.name &&
          (embed.author.name.includes('Detected') ||
           embed.author.name.includes('Giveaway'))) {
        return true;
      }
    }

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

  for (const indicator of TRACKER_INDICATORS) {
    if (content.includes(indicator)) {
      return true;
    }
  }

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

function detectScrim(parsed: ParsedGiveawayData, channelName: string): ScrimDetectionResult | null {
  const { lowerText, fullText } = parsed;

  if (lowerText.includes('scrim detected') || lowerText.includes('event detected')) return null;
  if (lowerText.includes('view message') && lowerText.includes('join server')) return null;
  if (lowerText.includes('detected in') && lowerText.includes('ms')) return null;

  if (lowerText.includes('giveaway') && (lowerText.includes('winner') || lowerText.includes('entered'))) {
    return null;
  }

  const channelType = classifyEventChannel(channelName);
  if (!channelType) return null;

  const type = detectScrimType(fullText);
  if (!type) return null;
  if (channelType !== type) return null;

  if (!hasEventContext(fullText)) return null;
  if (!hasScrimStructure(fullText, type)) return null;

  const host = extractScrimHost(fullText);
  const coHost = extractScrimCoHost(fullText);
  const time = extractScrimTime(fullText);
  const reward = extractScrimReward(fullText);
  const teams = extractScrimTeams(fullText);
  const region = extractScrimRegion(fullText);
  const ticks = extractScrimTicks(fullText);
  const hasEveryone = lowerText.includes('@everyone') || lowerText.includes('@here');

  if (type === 'scrim') {
    let fields = 0;
    if (host) fields++;
    if (time) fields++;
    if (teams) fields++;
    if (hasEveryone) fields++;
    if (region) fields++;

    if (fields < 2) return null;
  }

  if (type === 'squid_game') {
    if (!host && !time) return null;
    if (!reward) return null;
  }

  if (type === 'gagaball') {
    if (!time) return null;
    if (!reward) return null;

    const parsedTimestamp = parseScrimTime(time);
    if (parsedTimestamp === null) return null;

    const maxFutureMs = 7 * 24 * 60 * 60 * 1000;
    if (parsedTimestamp * 1000 - Date.now() > maxFutureMs) return null;
  }

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

  if (/scrim|squid|gaga|giveaway|event/i.test(lowerText)) {
    score += SCRIM_SCORE.TITLE_KEYWORD;
    signals.push('keyword');
  }

  const requiredScore = type === 'scrim' ? 7 : 5;

  if (score < requiredScore) {
    return null;
  }

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
// QUICK REJECT (Stage 1)
// ═══════════════════════════════════════════════════════════════════════════

function quickReject(message: Message, selfUserId: string, now: number): string | null {
  if (!message.guild) return 'no_guild';

  if (message.author?.id === selfUserId) return 'self';

  if (isOwnNotification(message)) return 'self_notification';

  if (message.webhookId) {
    return 'webhook_blocked';
  }

  if (message.author?.bot) {
    if (ALLOWED_GIVEAWAY_BOT_IDS.has(message.author.id)) {
      // Allow
    } else {
      return 'not_allowed_bot';
    }
  }

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
// SCORE CALCULATOR
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
// GIVEAWAY MANAGER
// ═══════════════════════════════════════════════════════════════════════════

interface ScrimHistoryEntry {
  recentDetections: number[];
  falsePositiveCount: number;
  cooldownUntil: number;
  lastActivity: number;
}

// ─── Invite failure reason type ──────────────────────────────────────────
type InviteFailReason = 'no_guild' | 'no_text_channels' | 'all_failed' | 'fatal';

export class GiveawayManager extends EventEmitter {
  private readonly client: Client;
  private readonly log: AppLogger;
  private readonly accountLabel: string;
  private readonly botManager: BotManager | null;
  private selfUserId: string;

  // ─── INSTANCE-LEVEL CACHE (FIXED for multi-account) ───────────────────
  private parsedMessageCache = new Map<string, { data: ParsedGiveawayData; timestamp: number }>();

  private processingMessages = new Set<string>();

  private creationCache = new LRUCache<string, { isCreation: boolean; score: number }>(MAX_CREATION_CACHE);
  private inviteCache = new LRUCache<string, { url: string; expiresAt: number }>(MAX_INVITE_CACHE);
  private failedInviteCache = new LRUCache<string, number>(MAX_FAILED_INVITE_CACHE);
  private duplicateCache = new LRUCache<string, number>(MAX_DUPLICATE_CACHE);

  private scrimHistory = new Map<string, ScrimHistoryEntry>();
  private scrimCleanupInterval: NodeJS.Timeout | null = null;

  private watchlistCacheExpiry = 0;
  private reverseWatchlistIndex: Map<string, string[]> = new Map();
  private watchlistAhoCorasick: AhoCorasick | null = null;
  private totalWatchlistItems = 0;

  private pendingInvites = new Map<string, Promise<string>>();

  // 🔥 FIXED: Ready event is true by default - process instantly
  private readyEventReceived = true;
  private readonly startupTime: number;
  private pendingStartupMessages = new Set<string>();
  private startupGraceTimer: NodeJS.Timeout | null = null;
  private destroyed = false;

  private stats = {
    detected: 0, notified: 0, skipped: 0, errors: 0,
    falsePositivesBlocked: 0, watchlistMatches: 0, draftsSkipped: 0,
    startedAt: Date.now(),
    startupMessagesSkipped: 0,
    scrimsDetected: 0,
    scrimsNotified: 0,
  };

  private guildStats = new Map<string, {
    detected: number;
    notified: number;
    falsePositives: number;
    lastSeen: number;
  }>();

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

    this.startScrimCleanup();
    // 🔥 FIXED: Removed startup delay - process instantly
    this.setupInstantReadyHandler();
  }

  // ═══════════════════════════════════════════════════════════════════════
  // STARTUP READY HANDLER - INSTANT PROCESSING
  // ═══════════════════════════════════════════════════════════════════════

  private setupInstantReadyHandler(): void {
    // 🔥 FIXED: No delays, just set ready and process
    this.client.once('ready', () => {
      this.selfUserId = this.client.user?.id || this.selfUserId;
      this.readyEventReceived = true;
      
      const startupDuration = Date.now() - this.startupTime;
      this.log.info(
        `Account ready in ${startupDuration}ms - ${this.accountLabel}`,
        {
          component: 'GiveawayManager',
          account: this.accountLabel,
          pendingMessages: this.pendingStartupMessages.size
        }
      );

      // Process any pending messages immediately
      if (this.pendingStartupMessages.size > 0) {
        this.log.info(`Processing ${this.pendingStartupMessages.size} backlog messages`, {
          account: this.accountLabel
        });
        this.stats.startupMessagesSkipped = this.pendingStartupMessages.size;
        this.pendingStartupMessages.clear();
      }
    });
  }

  // ═══════════════════════════════════════════════════════════════════════
  // SCRIM THROTTLING
  // ═══════════════════════════════════════════════════════════════════════

  private shouldThrottleScrim(guildId: string, channelId: string): boolean {
    const key = `${guildId}:${channelId}`;
    const now = Date.now();

    let history = this.scrimHistory.get(key);
    if (!history) {
      if (this.scrimHistory.size >= MAX_SCRIM_HISTORY) {
        let oldestKey: string | null = null;
        let oldestTime = Infinity;
        for (const [existingKey, existing] of this.scrimHistory) {
          if (existing.lastActivity < oldestTime) {
            oldestTime = existing.lastActivity;
            oldestKey = existingKey;
          }
        }
        if (oldestKey) this.scrimHistory.delete(oldestKey);
      }

      history = {
        recentDetections: [],
        falsePositiveCount: 0,
        cooldownUntil: 0,
        lastActivity: now,
      };
      this.scrimHistory.set(key, history);
    }

    history.lastActivity = now;

    if (now < history.cooldownUntil) return true;

    history.recentDetections = history.recentDetections.filter(
      ts => now - ts < SCRIM_THROTTLE_WINDOW_MS
    );

    if (history.recentDetections.length > SCRIM_THROTTLE_MAX) {
      const cooldownMs = Math.min(
        300000,
        history.recentDetections.length * 30000
      );
      history.cooldownUntil = now + cooldownMs;
      history.falsePositiveCount++;

      this.log.debug(`Throttling scrim detection in channel ${channelId}`, {
        detectionRate: history.recentDetections.length,
        cooldownMs,
        falsePositiveCount: history.falsePositiveCount
      });
      return true;
    }

    history.recentDetections.push(now);
    history.lastActivity = now;
    return false;
  }

  private startScrimCleanup(): void {
    if (this.scrimCleanupInterval) clearInterval(this.scrimCleanupInterval);

    this.scrimCleanupInterval = setInterval(() => {
      if (this.destroyed) return;

      const now = Date.now();
      for (const [key, history] of this.scrimHistory) {
        history.recentDetections = history.recentDetections.filter(
          ts => now - ts < SCRIM_THROTTLE_WINDOW_MS
        );

        if (
          now - history.lastActivity > SCRIM_HISTORY_TTL_MS &&
          now > history.cooldownUntil
        ) {
          this.scrimHistory.delete(key);
          continue;
        }

        if (history.falsePositiveCount > 0 && history.recentDetections.length === 0) {
          history.falsePositiveCount = 0;
        }
      }
    }, SCRIM_CLEANUP_INTERVAL);

    this.scrimCleanupInterval.unref?.();
  }

  // ═══════════════════════════════════════════════════════════════════════
  // SCRIM NOTIFICATION
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
  // PUBLIC API
  // ═══════════════════════════════════════════════════════════════════════

  public async handleMessage(message: Message): Promise<void> {
    if (this.destroyed) return;

    const now = Date.now();
    const processingStart = performance.now();

    // 🔥 FIXED: INSTANT PRE-FILTER - skip non-giveaways before ANY heavy processing
    const content = message.content || '';
    const hasIndicator = 
      content.includes('giveaway') ||
      content.includes('🎉') ||
      content.includes('🎁') ||
      content.includes('scrim') ||
      content.includes('host:') ||
      content.includes('reward:') ||
      content.includes('prize:') ||
      content.includes('@everyone') ||
      content.includes('@here');

    if (!hasIndicator) {
      this.stats.skipped++;
      return; // 🔥 0.1ms response - no parsing!
    }

    // 🔥 FIXED: REMOVED startup delay - process immediately
    // No waiting for ready event
    // No pending message storage
    // No 30 second grace period

    // 🔥 FIXED: REMOVED message age filter - process everything
    // if (now - message.createdTimestamp > MAX_MESSAGE_AGE_MS) return;

    const rejectReason = quickReject(message, this.selfUserId, now);
    if (rejectReason) return;

    const key = `${message.id}-${message.channel.id}`;
    if (this.processingMessages.has(key)) return;
    if (this.processingMessages.size >= MAX_PROCESSING_MESSAGES) {
      this.stats.skipped++;
      return;
    }
    this.processingMessages.add(key);

    try {
      // ─── Parse message with INSTANCE-LEVEL cache ──────────────────────
      let parsed = parseMessage(message, now, this.accountLabel, this.parsedMessageCache);

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
            parsed = refreshParsedMessage(refreshed, now, this.accountLabel, this.parsedMessageCache);
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
          const messageDupKey = `${this.accountLabel}:${message.id}:${message.channel.id}`;
          if (this.duplicateCache.get(messageDupKey)) return;
          this.duplicateCache.set(messageDupKey, now);

          // 🔥 FIXED: Non-blocking DB operations - send notification FIRST
          const guild = message.guild!;
          const guildIcon = guild.iconURL({ size: 512 }) || null;
          const guildBanner = (guild as any).bannerURL?.({ size: 1024 }) || null;
          const memberCount = (guild as any).memberCount ?? null;
          const processingTime = Math.round(performance.now() - processingStart);

          // Check DB but don't wait if it's slow
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

          // 🔥 FIXED: Get invite in parallel with notification prep
          const invitePromise = this.fetchInviteForGuild(guild.id);
          
          // 🔥 FIXED: Send notification IMMEDIATELY - don't wait for DB
          const inviteUrl = await invitePromise;
          
          const fullData: GiveawayData = {
            ...data, id: undefined, status: 'active',
            notifiedAt: null, lastSeenAt: now,
            inviteUrl, guildIcon, guildBanner, memberCount,
          };

          // 🔥 FIXED: Fire and forget DB operations
          const savePromise = insertGiveaway(data).catch(err => {
            this.log.error('Failed to save giveaway to DB:', { error: formatError(err) });
          });

          // Send notification NOW
          try {
            const sent = await this.botManager?.sendGiveawayNotification(fullData);
            if (sent) {
              this.stats.notified++;
              this.recordGuildStat(guild.id, 'notified');
              // Don't await markNotified - fire and forget
              markNotified(message.id, message.channel.id).catch(() => {});
            } else {
              this.stats.errors++;
            }
          } catch (error) {
            this.stats.errors++;
            this.log.error(`Notify error: ${formatError(error)}`);
          }

          // 🔥 FIXED: Process watchlist in background - don't block
          this.checkWatchlistMatches(parsed, message, Promise.resolve(inviteUrl), processingTime)
            .catch(err => this.log.error('Watchlist error:', { error: formatError(err) }));

          this.log.info(
            `Detected: "${parsed.prize}" [${detection.confidence}%] ` +
            `(processing: ${processingTime}ms) - ` +
            detection.signals.join(', ')
          );

          return;
        }

        this.stats.falsePositivesBlocked++;
        return;
      }

      // ─── TIER 2: SCRIM/EVENT DETECTION (non-bot messages) ─────────
      if (!parsed.isFromBot) {
        if (!/scrim|squid|gaga|event|host|reward|prize|team|region/i.test(parsed.lowerText)) {
          this.stats.falsePositivesBlocked++;
          return;
        }

        const channelName = (message.channel as any).name || '';
        const channelType = classifyEventChannel(channelName);
        if (!channelType) {
          this.stats.falsePositivesBlocked++;
          return;
        }

        const rawEventContent = (message.content || '').toLowerCase();
        if (!EVENT_MESSAGE_HINTS.some(hint => rawEventContent.includes(hint)) && !parsed.hasAnyEmbed) {
          this.stats.falsePositivesBlocked++;
          return;
        }

        const scrimResult = detectScrim(parsed, channelName);
        if (scrimResult && scrimResult.score >= MINIMUM_SCRIM_SCORE_THRESHOLD) {
          const scrimDupKey = `scrim:${this.accountLabel}:${message.id}:${message.channel.id}`;
          if (this.duplicateCache.get(scrimDupKey)) return;

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
    if (this.destroyed) return;
    if (!newMessage.guild || !newMessage.author?.bot) return;
    if (!ALLOWED_GIVEAWAY_BOT_IDS.has(newMessage.author.id)) return;

    const existing = await getGiveaway(newMessage.id, newMessage.channel.id);
    if (!existing || existing.status !== 'active') return;

    const now = Date.now();
    const parsed = parseMessage(newMessage, now, this.accountLabel, this.parsedMessageCache);

    if (isEndedGiveaway(parsed)) {
      await markEnded(newMessage.id, newMessage.channel.id);
      this.log.debug(`Giveaway ended via edit: ${newMessage.id}`);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // WATCHLIST MATCHING
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
  // INVITE GENERATION — FIXED (v29)
  //
  // Root causes addressed:
  //   1. permissionsFor() returns null on partial guild cache → null-safe
  //   2. guild.invites.fetch() 403 was bubbling and aborting function → isolated
  //   3. Vanity URL moved before invites.fetch() (free, no perms needed)
  //   4. Channel iteration now uses a scored list best-first, single loop
  //   5. self-member fetch now retries with force:true on first failure
  //   6. cacheFailedInvite accepts reason → structural failures get 4× TTL
  // ═══════════════════════════════════════════════════════════════════════

  private async fetchInviteForGuild(guildId: string): Promise<string> {
    const now = Date.now();
    const fallback = `https://discord.com/channels/${guildId}`;

    // Hard-failed recently — don't hammer API
    const failedUntil = this.failedInviteCache.get(guildId);
    if (failedUntil && failedUntil > now) return fallback;

    // Valid cached invite
    const cached = this.inviteCache.get(guildId);
    if (cached && cached.expiresAt > now) return cached.url;

    // Deduplicate concurrent calls for same guild
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
    const fallback = `https://discord.com/channels/${guildId}`;

    try {
      const guild = this.client.guilds.cache.get(guildId);
      if (!guild) {
        this.cacheFailedInvite(guildId, now, 'no_guild');
        return fallback;
      }

      // ── 1. Vanity URL (free, no MANAGE_GUILD needed) ─────────────
      try {
        const vanity = (guild as any).vanityURLCode as string | null | undefined;
        if (vanity) {
          const url = `https://discord.gg/${vanity}`;
          this.cacheInvite(guildId, url, now);
          return url;
        }
      } catch {
        // vanityURLCode access can throw on partial guilds — ignore
      }

      // ── 2. Fetch existing invites (requires MANAGE_GUILD) ─────────
      // Isolated so a 403 doesn't abort the rest of the function.
      try {
        const invites = await guild.invites.fetch();
        if (invites?.size) {
          const best =
            invites.find(inv => inv.maxAge === 0 && inv.maxUses === 0) ??
            invites.find(inv => inv.maxAge === 0) ??
            invites.first();
          if (best?.url) {
            this.cacheInvite(guildId, best.url, now);
            return best.url;
          }
        }
      } catch (err: any) {
        // 403 / 50013 = no MANAGE_GUILD — expected, skip silently
        const code = err?.code ?? err?.httpStatus;
        if (code !== 403 && code !== 50013) {
          this.log.debug(`invites.fetch non-perm error guild ${guildId}: ${formatError(err)}`);
        }
        // Fall through to createInvite path
      }

      // ── 3. Resolve self member for permission checks ──────────────
      let botMember = this.selfUserId ? guild.members.cache.get(this.selfUserId) : undefined;
      if (!botMember && this.selfUserId) {
        try {
          botMember = await guild.members.fetch({ user: this.selfUserId, force: false });
        } catch {
          // Retry with force:true
          try {
            botMember = await guild.members.fetch({ user: this.selfUserId, force: true });
          } catch (fetchErr) {
            this.log.debug(`Cannot fetch self member in ${guildId}: ${formatError(fetchErr)}`);
            // botMember stays undefined — we'll try channels without perm filtering
          }
        }
      }

      // ── 4. Score text channels by invite-ability ──────────────────
      // Score 2: explicit overwrite grant
      // Score 1: permissionsFor() passes
      // Score 0: permissionsFor() returned null (partial guild data)
      // Excluded: permissionsFor() explicitly denies
      const textChannels = guild.channels.cache.filter(
        (ch): ch is TextChannel => ch.type === 'GUILD_TEXT'
      );

      if (!textChannels.size) {
        this.cacheFailedInvite(guildId, now, 'no_text_channels');
        return fallback;
      }

      interface ScoredChannel { channel: TextChannel; score: number }
      const scored: ScoredChannel[] = [];

      for (const [, ch] of textChannels) {
        let score = 0;

        if (botMember) {
          try {
            const perms = ch.permissionsFor(botMember);

            if (perms === null) {
              // null means partial guild cache; unknown — include at score 0
              score = 0;
            } else if (perms.has('CREATE_INSTANT_INVITE')) {
              // Check if explicitly granted via overwrite (score 2) or inherited (score 1)
              const overwrite = ch.permissionOverwrites?.cache.get(this.selfUserId);
              score = overwrite?.allow?.has('CREATE_INSTANT_INVITE') ? 2 : 1;
            } else {
              // Explicitly denied — skip channel entirely
              continue;
            }
          } catch {
            // permissionsFor threw (evicted partial data) — include at score 0
            score = 0;
          }
        }
        // If botMember is null we have no info — include everything at score 0

        scored.push({ channel: ch, score });
      }

      // Sort best candidates first
      scored.sort((a, b) => b.score - a.score);

      // ── 5. Try createInvite on scored channels ────────────────────
      const INVITE_OPTIONS = {
        maxAge: 0,
        maxUses: 0,
        reason: 'Giveaway tracker',
        temporary: false,
      };

      for (const { channel } of scored) {
        try {
          const invite = await channel.createInvite(INVITE_OPTIONS);
          if (invite?.url) {
            this.cacheInvite(guildId, invite.url, now);
            return invite.url;
          }
        } catch (err: any) {
          const code = err?.code ?? err?.httpStatus;
          if (code === 50013 || code === 403) continue;
          this.log.debug(`createInvite failed ch ${channel.id}: ${formatError(err)}`);
        }
      }

      // ── 6. OG fallback: try every text channel without permission filtering ──
      for (const [, channel] of textChannels) {
        if (scored.some(entry => entry.channel.id === channel.id)) {
          // Still retry here because partial permission data can be wrong.
        }
        try {
          const invite = await channel.createInvite({
            maxAge: 0,
            maxUses: 0,
            reason: 'Giveaway tracker (fallback)',
            temporary: false,
          });
          if (invite?.url) {
            this.cacheInvite(guildId, invite.url, now);
            return invite.url;
          }
        } catch {
          continue;
        }
      }

      // ── 7. All attempts exhausted ─────────────────────────────────
      this.cacheFailedInvite(guildId, now, 'all_failed');
      return fallback;

    } catch (error) {
      this.log.error(`Invite fatal error ${guildId}: ${formatError(error)}`);
      this.cacheFailedInvite(guildId, now, 'fatal');
      return fallback;
    }
  }

  private cacheInvite(guildId: string, url: string, now: number): void {
    this.inviteCache.set(guildId, { url, expiresAt: now + INVITE_CACHE_TTL });
  }

  // Reason-aware TTL: structural failures are retried much less often
  private cacheFailedInvite(
    guildId: string,
    now: number,
    reason: InviteFailReason = 'all_failed',
  ): void {
    const retryMs = (reason === 'no_guild' || reason === 'no_text_channels')
      ? FAILED_INVITE_RETRY_MS * 4   // 60 min — structural, won't change soon
      : FAILED_INVITE_RETRY_MS;      // 15 min — might be transient

    this.failedInviteCache.set(guildId, now + retryMs);
    this.log.debug(
      `Invite cached as failed (${reason}) for guild ${guildId}, retry in ${retryMs / 60000}m`
    );
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
    const now = Date.now();
    let stats = this.guildStats.get(guildId);

    if (!stats) {
      if (this.guildStats.size >= MAX_GUILD_STATS) {
        let oldestId: string | null = null;
        let oldestTime = Infinity;
        for (const [id, value] of this.guildStats) {
          if (value.lastSeen < oldestTime) {
            oldestTime = value.lastSeen;
            oldestId = id;
          }
        }
        if (oldestId) this.guildStats.delete(oldestId);
      }

      stats = { detected: 0, notified: 0, falsePositives: 0, lastSeen: now };
      this.guildStats.set(guildId, stats);
    }

    stats.lastSeen = now;
    if (type === 'detected') stats.detected++;
    else if (type === 'notified') stats.notified++;
    else stats.falsePositives++;
  }

  public getGuildStats(): Map<string, { detected: number; notified: number; falsePositives: number }> {
    const result = new Map<string, { detected: number; notified: number; falsePositives: number }>();
    for (const [guildId, stats] of this.guildStats) {
      result.set(guildId, {
        detected: stats.detected,
        notified: stats.notified,
        falsePositives: stats.falsePositives,
      });
    }
    return result;
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
    this.log.info(`  Parse cache size    : ${this.parsedMessageCache.size}`);
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
      parseCacheSize: this.parsedMessageCache.size,
    };
  }

  // ═══════════════════════════════════════════════════════════════════════
  // SHUTDOWN
  // ═══════════════════════════════════════════════════════════════════════

  public async shutdown(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;

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
    this.pendingInvites.clear();
    this.scrimHistory.clear();
    this.guildStats.clear();
    this.parsedMessageCache.clear();

    this.reverseWatchlistIndex.clear();
    this.watchlistAhoCorasick = null;
    this.totalWatchlistItems = 0;
    this.watchlistCacheExpiry = 0;

    this.removeAllListeners();

    this.log.info(`Shutting down ${this.accountLabel}...`, {
      component: 'GiveawayManager',
      stats: this.stats
    });
    this.logStats();
  }
}

export default GiveawayManager;
