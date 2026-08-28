/**
 * @module types
 * All shared TypeScript types
 */

// ============================================================
// App Configuration
// ============================================================

export interface AppConfig {
  tokens: string[];
  botToken: string;
  trackerChannelId: string;
  scrimChannelId: string;
  eventChannelId: string;
  monitoredChannels: string[];
  dbPath: string;
  logLevel: string;
  logDir: string;
  notificationCooldown: number;
  statsIntervalMs: number;
  adminUserIds: string[];

  // AutoJoin settings
  maxRetries: number;
  retryDelayMs: number;
  buttonDelayMs: number;
  reactionDelayMs: number;
  winWebhookUrl: string;
}

// ============================================================
// Giveaway Data
// ============================================================

export interface GiveawayData {
  id?: string;
  messageId: string;
  channelId: string;
  guildId: string;
  guildName: string;
  channelName: string;
  authorId: string;
  prize: string;
  detectedAt: number;
  endsAt: number | null;
  status: 'active' | 'ended';
  notifiedAt: number | null;
  lastSeenAt: number;
  inviteUrl?: string;
  notificationMessageId?: string;
  detectionTimeMs?: number;
  guildIcon?: string | null;
  guildBanner?: string | null;
  memberCount?: number | null;
}

// ============================================================
// Scrim/Event Notification Data
// ============================================================

export interface ScrimNotificationData extends GiveawayData {
  type: 'scrim' | 'squid_game' | 'gagaball';
  host: string | null;
  coHost: string | null;
  time: string | null;
  reward: string | null;
  teams: string | null;
  region: string | null;
  ticks: number | null;
  rawContent: string;
  messageUrl: string;
}

// ============================================================
// Stats
// ============================================================

export interface GiveawayStats {
  totalDetected: number;
  activeGiveaways: number;
  serversWithGiveaways: number;
  lastDetected: number | null;
}

// ============================================================
// Detection
// ============================================================

export enum DetectionSource {
  CONTENT = 'content',
  EMBED = 'embed',
  COMPONENT = 'component',
}

export interface DetectedGiveaway {
  prize: string;
  source: DetectionSource;
  endsAt: number | null;
  buttonCustomId?: string;
}

export interface GiveawayMessage {
  content?: string;
  embeds?: {
    title?: string;
    description?: string;
    footer?: {
      text?: string;
    };
    fields?: {
      name: string;
      value: string;
    }[];
  }[];
  buttons?: {
    customId?: string;
    label?: string;
    disabled?: boolean;
    style?: number;
  }[];
}

// ============================================================
// Watchlist
// ============================================================

export interface UserWatchlist {
  userId: string;
  items: string[];
  createdAt: number;
  updatedAt: number;
}

// ============================================================
// License System
// ============================================================

export interface LicenseKey {
  key: string;
  used: boolean;
  usedBy: string | null;
  createdAt: number;
  createdBy: string;
}

// ============================================================
// AutoJoin Types
// ============================================================

export enum EntryMethod {
  BUTTON = 'button',
  REACTION = 'reaction',
}

export enum EntryStatus {
  PENDING = 'pending',
  ATTEMPTING = 'attempting',
  SUCCESS = 'success',
  FAILED = 'failed',
  SKIPPED = 'skipped',
}

export interface GiveawayEntry {
  entryId: string;
  messageId: string;
  channelId: string;
  guildId: string;
  authorId: string;
  guildName: string;
  channelName: string;
  prize: string;
  entryMethod: EntryMethod;
  buttonCustomId?: string;
  reactionEmoji?: string;
  detectionSource: DetectionSource;
  detectedAt: number;
  endsAt?: number;
  status: EntryStatus;
  attempts: number;
  userId: string;
  lastAttemptAt?: number;
  lastError?: string;
}

export interface GiveawayButton {
  customId: string;
  label: string;
  disabled: boolean;
}

export interface AutoJoinSessionStats {
  detected: number;
  entered: number;
  failed: number;
  wins: number;
  lastEntryAt?: number;
}

export interface AutoJoinManagerStats {
  totalSessions: number;
  activeSessions: number;
  sessionStats: Map<string, AutoJoinSessionStats>;
}

export interface ManagerState {
  entries: Map<string, GiveawayEntry>;
  processing: Set<string>;
  stats: GiveawayStats;
}
