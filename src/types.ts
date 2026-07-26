/**
 * @module types
 * All shared TypeScript types
 */

export interface AppConfig {
  tokens: string[];
  botToken: string;
  trackerChannelId: string;
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
  webhookUrl: string;
  winWebhookUrl: string;
}

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

export interface GiveawayStats {
  totalDetected: number;
  activeGiveaways: number;
  serversWithGiveaways: number;
  lastDetected: number | null;
}

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

export interface UserWatchlist {
  userId: string;
  items: string[];
  createdAt: number;
  updatedAt: number;
}

// License System Types
export interface LicenseKey {
  key: string;
  used: boolean;
  usedBy: string | null;
  createdAt: number;
  createdBy: string;
}

// AutoJoin Types
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

export interface GiveawayButton {
  customId: string;
  label: string;
  disabled: boolean;
}

export interface GiveawayStats {
  totalDetected: number;
  totalSucceeded: number;
  totalFailed: number;
  totalSkipped: number;
  totalDuplicates: number;
  totalWins: number;
  serversJoined: number;
  serversJoinFailed: number;
  startedAt: number;
  lastDetectedAt?: number;
  lastSuccessAt?: number;
}

export interface ManagerState {
  entries: Map<string, GiveawayEntry>;
  processing: Set<string>;
  stats: GiveawayStats;
}
