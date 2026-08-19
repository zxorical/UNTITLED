/**
 * @module bot
 * Production Discord bot.
 * Public-facing VRFS features are presented as native product functionality.
 */
import {
  Client,
  GatewayIntentBits,
  REST,
  Routes,
  SlashCommandBuilder,
  EmbedBuilder,
  ChatInputCommandInteraction,
  Interaction,
  CacheType,
  TextChannel,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ChannelType,
  ButtonInteraction,
  ActivityType,
  Collection,
  Invite,
} from 'discord.js';
import { CONFIG } from './config.js';
import { logger } from './logger.js';
import { formatTimestamp, truncate, formatError } from './utils.js';
import { GiveawayData } from './types.js';
import {
  getStats,
  getTotalDetected,
  getActiveGiveaways,
  resetDatabase,
  getAllGiveaways,
  purgeEndedGiveaways,
  setNotificationMessageId,
  addItem,
  removeItem,
  getItems,
  clearItems,
  useLicenseKey,
  setPremiumUser,
  removePremiumUser,
  getPremiumUser,
  isPremiumUser,
  setBoosterPremium,
  getBoosterPremium,
  getActiveBoosters,
  removeBoosterPremium,
  updateBoosterPremiumStatus,
  getBoosterPremiumStats,
  getAllPremiumUsers,
  getPremiumStats,
  getLicenseStats,
  listLicenseKeys,
  updateUserToken,
  updateUserWebhook,
  getAutoJoinEntriesCollection,
  getScrimStats,
  getActiveScrims,
  getScrimsByType,
  getScrimsByGuild,
} from './database.js';
import { KeyPanel } from './license/keyPanel.js';
import { PremiumPanel } from './premium/premiumPanel.js';
import { AdminPanel } from './premium/adminPanel.js';
import {
  isPremium,
  requirePremium,
  setClient,
  assignPremiumRole,
  addPremiumUser,
  removePremiumUser as removePremiumUserService,
  checkPremium,
  clearPremiumCache,
} from './license/licenseMiddleware.js';
import {
  vrfs,
  seby,
  getSku,
  getItemName,
  isItemFree,
  getMarketplaceActive,
  getMarketplaceOwners,
  type VRFSItem,
  type VRFSMarketplaceItem,
  type VRFSOutfit,
} from './middleware/api/vrfs.js';
declare function updateNotificationStatus(
  messageId: string,
  channelId: string,
  fields: Record<string, unknown>
): Promise<void>;
class MetricsCollector {
  giveawaysDetected = 0;
  notificationsSent = 0;
  notificationsFailed = 0;
  retryAttempts = 0;
  detectionToNotifyLatency: number[] = [];
  mongoLatency: number[] = [];
  discordLatency: number[] = [];
  recordDetection(latencyMs: number) {
    this.giveawaysDetected++;
    this.detectionToNotifyLatency.push(latencyMs);
    if (this.detectionToNotifyLatency.length > 100) this.detectionToNotifyLatency.shift();
  }
  recordNotification(success: boolean, latencyMs: number) {
    if (success) this.notificationsSent++;
    else this.notificationsFailed++;
    this.discordLatency.push(latencyMs);
    if (this.discordLatency.length > 100) this.discordLatency.shift();
  }
  recordRetry() {
    this.retryAttempts++;
  }
  recordMongoLatency(ms: number) {
    this.mongoLatency.push(ms);
    if (this.mongoLatency.length > 100) this.mongoLatency.shift();
  }
  getSnapshot() {
    return {
      giveawaysDetected: this.giveawaysDetected,
      notificationsSent: this.notificationsSent,
      notificationsFailed: this.notificationsFailed,
      retryAttempts: this.retryAttempts,
      avgDetectionLatency: this.avg(this.detectionToNotifyLatency),
      avgMongoLatency: this.avg(this.mongoLatency),
      avgDiscordLatency: this.avg(this.discordLatency),
    };
  }
  private avg(arr: number[]): number {
    if (arr.length === 0) return 0;
    return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length);
  }
}
interface NotificationJob {
  data: GiveawayData;
  attempt: number;
  maxRetries: number;
  messageId: string;
}
const DEDUP_TTL_MS = 10 * 60 * 1000;
const DEDUP_SWEEP_INTERVAL_MS = 5 * 60 * 1000;
class NotificationService {
  private queue: NotificationJob[] = [];
  private processing = false;
  private dedupMap = new Map<string, number>();
  private dedupSweepInterval: NodeJS.Timeout | null = null;
  private bot: Client;
  private metrics: MetricsCollector;
  constructor(bot: Client, metrics: MetricsCollector) {
    this.bot = bot;
    this.metrics = metrics;
    this.dedupSweepInterval = setInterval(() => this.sweepDedup(), DEDUP_SWEEP_INTERVAL_MS);
    if (this.dedupSweepInterval.unref) this.dedupSweepInterval.unref();
  }
  private sweepDedup(): void {
    const now = Date.now();
    let removed = 0;
    for (const [messageId, addedAt] of this.dedupMap) {
      if (now - addedAt > DEDUP_TTL_MS) {
        this.dedupMap.delete(messageId);
        removed++;
      }
    }
    if (removed > 0) {
      logger.debug('Notification dedup cache swept', {
        removed,
        remaining: this.dedupMap.size,
      });
    }
  }
  shutdown(): void {
    if (this.dedupSweepInterval) {
      clearInterval(this.dedupSweepInterval);
      this.dedupSweepInterval = null;
    }
  }
  enqueue(data: GiveawayData, inviteUrl: string) {
    const existing = this.dedupMap.get(data.messageId);
    if (existing !== undefined && Date.now() - existing < DEDUP_TTL_MS) {
      logger.debug('Notification duplicate prevented', {
        messageId: data.messageId,
      });
      return;
    }
    this.dedupMap.set(data.messageId, Date.now());
    (data as any).cachedInviteUrl = inviteUrl;
    this.queue.push({
      data,
      attempt: 1,
      maxRetries: 3,
      messageId: data.messageId,
    });
    this.processQueue();
  }
  private async processQueue() {
    if (this.processing) return;
    this.processing = true;
    while (this.queue.length > 0) {
      const job = this.queue.shift()!;
      try {
        await this.sendWithRetry(job);
      } catch (err) {
        logger.error('Notification failed after retries', {
          messageId: job.messageId,
          error: formatError(err),
        });
        try {
          await updateNotificationStatus?.(job.messageId, job.data.channelId, {
            notificationStatus: 'failed',
            notificationError: formatError(err),
          });
        } catch {}
      }
    }
    this.processing = false;
  }
  private async sendWithRetry(job: NotificationJob): Promise<void> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= job.maxRetries; attempt++) {
      try {
        job.attempt = attempt;
        await this.sendOne(job);
        return;
      } catch (err) {
        lastError = err;
        this.metrics.recordRetry();
        logger.warn(`Notification attempt ${attempt}/${job.maxRetries} failed`, {
          messageId: job.messageId,
          error: formatError(err),
        });
        if (attempt < job.maxRetries) {
          const wait = Math.min(1000 * Math.pow(2, attempt - 1), 8000);
          await new Promise(r => setTimeout(r, wait));
        }
      }
    }
    throw lastError;
  }
  private async sendOne(job: NotificationJob): Promise<void> {
    const channel = this.bot.channels.cache.get(CONFIG.trackerChannelId) as TextChannel | undefined;
    if (!channel) throw new Error('Tracker channel not found');
    const data = job.data;
    const guild = this.bot.guilds.cache.get(data.guildId);
    const guildName = guild?.name || data.guildName || 'Unknown';
    const guildIcon = (data as any).guildIcon || guild?.iconURL({ size: 512 }) || null;
    const guildBanner = (data as any).guildBanner || guild?.bannerURL({ size: 1024 }) || null;
    const memberCount = ((data as any).memberCount || guild?.memberCount) ?? null;
    let inviteUrl = (data as any).cachedInviteUrl || data.inviteUrl || 'No invite available';
    if (inviteUrl === 'No invite available' && data.guildId) {
      try {
        const targetGuild = this.bot.guilds.cache.get(data.guildId);
        if (targetGuild) {
          const invites = await targetGuild.invites.fetch().catch(() => new Collection<string, Invite>());
          const existingInvite = invites.find((inv: Invite) => inv.channelId === data.channelId && inv.maxUses === 0);
          if (existingInvite) {
            inviteUrl = existingInvite.url;
          } else {
            const targetChannel = targetGuild.channels.cache.get(data.channelId);
            if (targetChannel && targetChannel.isTextBased() && 'createInvite' in targetChannel) {
              const perms = targetChannel.permissionsFor(this.bot.user?.id || '');
              if (perms?.has('CreateInstantInvite')) {
                const newInvite = await targetChannel.createInvite({
                  maxAge: 86400,
                  maxUses: 0,
                  reason: 'Giveaway notification',
                });
                inviteUrl = newInvite.url;
              }
            }
          }
        }
      } catch (err) {
        logger.debug(`Could not generate invite for notification: ${formatError(err)}`);
      }
    }
    const endsAt = data.endsAt || Date.now() + 3600000;
    const endTimestamp = Math.floor(endsAt / 1000);
    const winnerCount = extractWinnerCount(data.prize);
    const pingMention = process.env.PING_ROLE_ID
      ? `<@&${process.env.PING_ROLE_ID}>`
      : '@everyone';
    const description = [
      `### Details`,
      `**Server:** ${guildName}`,
      `**Channel:** #${data.channelName}`,
      `**Winners:** ${winnerCount}`,
      ``,
      `### Time`,
      `**Ends:** <t:${endTimestamp}:F>`,
      `**Countdown:** <t:${endTimestamp}:R>`,
      ``,
      `### Links`,
      `**Invite:** ${inviteUrl}`,
      memberCount ? `**Members:** ${memberCount.toLocaleString()}` : '',
    ].filter(Boolean).join('\n');
    const embed = new EmbedBuilder()
      .setAuthor({
        name: 'New Giveaway',
        iconURL: this.bot.user?.displayAvatarURL(),
      })
      .setTitle(data.prize || 'Unknown Prize')
      .setDescription(description)
      .setColor(0x5865F2);
    if (guildIcon) embed.setThumbnail(guildIcon);
    if (guildBanner) embed.setImage(guildBanner);
    const messageUrl = `https://discord.com/channels/${data.guildId}/${data.channelId}/${data.messageId}`;
    const row = new ActionRowBuilder<ButtonBuilder>();
    if (inviteUrl.startsWith('http')) {
      row.addComponents(
        new ButtonBuilder()
          .setLabel('Join Server')
          .setStyle(ButtonStyle.Link)
          .setURL(inviteUrl),
      );
    }
    row.addComponents(
      new ButtonBuilder()
        .setLabel('Message')
        .setStyle(ButtonStyle.Link)
        .setURL(messageUrl),
    );
    const start = Date.now();
    const sentMessage = await channel.send({
      content: pingMention,
      embeds: [embed],
      components: [row],
    });
    this.metrics.recordNotification(true, Date.now() - start);
    await setNotificationMessageId(data.messageId, data.channelId, sentMessage.id);
    try {
      await updateNotificationStatus?.(data.messageId, data.channelId, {
        notificationStatus: 'sent',
        notificationSentAt: Date.now(),
        notificationMessageId: sentMessage.id,
      });
    } catch {}
  }
}
function extractWinnerCount(prize: string): string {
  const match = prize.match(/(\d+)\s*[xX×]/);
  if (match) return match[1];
  if (/\b(?:one|1)\s*(?:winner|win|giveaway)/i.test(prize)) return '1';
  const m = prize.match(/(\d+)\s*(?:winners?)/i);
  if (m) return m[1];
  return '1';
}
async function deferReply(interaction: ChatInputCommandInteraction<CacheType>, ephemeral = true) {
  await interaction.deferReply({ ephemeral });
}
function isAdmin(userId: string): boolean {
  return CONFIG.adminUserIds.includes(userId);
}
function isOwner(userId: string): boolean {
  return userId === process.env.OWNER_ID;
}
async function requireAdmin(interaction: ChatInputCommandInteraction<CacheType>): Promise<boolean> {
  if (!isAdmin(interaction.user.id)) {
    await interaction.reply({
      content: 'No permission.',
      ephemeral: true,
    });
    return false;
  }
  return true;
}
async function requireOwner(interaction: ChatInputCommandInteraction<CacheType>): Promise<boolean> {
  if (!isOwner(interaction.user.id)) {
    await interaction.reply({
      content: 'You do not have permission to use this command.',
      ephemeral: true,
    });
    return false;
  }
  return true;
}
interface UserNotificationSettings {
  giveaways: boolean;
  scrims: boolean;
  events: boolean;
}
const notificationSettingsCache = new Map<string, UserNotificationSettings>();
function getDefaultSettings(): UserNotificationSettings {
  return {
    giveaways: true,
    scrims: true,
    events: true,
  };
}
async function getUserNotificationSettings(userId: string): Promise<UserNotificationSettings> {
  const cached = notificationSettingsCache.get(userId);
  if (cached) return cached;
  const items = await getItems(userId);
  const settings: UserNotificationSettings = getDefaultSettings();
  if (items.includes('notif:giveaways:off')) settings.giveaways = false;
  if (items.includes('notif:scrims:off')) settings.scrims = false;
  if (items.includes('notif:events:off')) settings.events = false;
  notificationSettingsCache.set(userId, settings);
  return settings;
}
async function updateUserNotificationSetting(
  userId: string,
  type: 'giveaways' | 'scrims' | 'events',
  enabled: boolean,
): Promise<void> {
  const settings = await getUserNotificationSettings(userId);
  settings[type] = enabled;
  notificationSettingsCache.set(userId, settings);
  const keyOff = `notif:${type}:off`;
  if (enabled) await removeItem(userId, keyOff);
  else await addItem(userId, keyOff);
}
interface ProductPaginationSession {
  type: 'items' | 'marketplace' | 'locker';
  items: Array<VRFSItem | VRFSMarketplaceItem>;
  page: number;
  pageSize: number;
  title: string;
  description?: string;
  id?: number;
  createdAt: number;
}
const productPagination = new Map<string, ProductPaginationSession>();
const PRODUCT_PAGE_TTL_MS = 10 * 60 * 1000;
const PRODUCT_PAGE_SIZE = 10;
const LOCKER_PAGE_SIZE = 10;
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
function productNumber(value: unknown): string {
  const number = Number(value);
  return Number.isFinite(number) ? number.toLocaleString() : '0';
}
function productNormalize(value: unknown): string {
  return String(value ?? '').trim().toLowerCase();
}
function productTrim(value: unknown, max: number): string {
  const text = String(value ?? '');
  return text.length > max ? `${text.slice(0, Math.max(0, max - 3))}...` : text;
}
function productSafeImage(value: unknown): string | null {
  if (typeof value !== 'string' || !value.trim()) return null;
  try {
    const url = new URL(value.trim());
    if (url.protocol !== 'https:' && url.protocol !== 'http:') return null;
    return url.toString();
  } catch {
    return null;
  }
}
function productAvatar(id: number | string): string {
  return `https://userpic.vrfs.org/avatar/avatar-pics/${encodeURIComponent(String(id))}.png`;
}
function productSection(item: VRFSItem): string {
  return String(item.section ?? item.category ?? item.category_name ?? 'Other').trim() || 'Other';
}
function productCredits(item: VRFSItem): number | null {
  const values = [item.coins, item.coins_price, item.credits];
  for (const value of values) {
    if (value === undefined || value === null || String(value).trim() === '') continue;
    const number = Number(value);
    if (Number.isFinite(number)) return number;
  }
  const match = String(item.price ?? '').match(/[\d,]+(?:\.\d+)?/);
  if (match) {
    const number = Number(match[0].replace(/,/g, ''));
    if (Number.isFinite(number)) return number;
  }
  return null;
}
function productPrice(item: VRFSItem): string {
  if (isItemFree(item)) return 'Free';
  const credits = productCredits(item);
  return credits !== null ? `${credits.toLocaleString()} Credits` : 'Paid';
}
function productCatalogImage(item: VRFSItem): string | null {
  const candidates = [
    item.image,
    item.image_url,
    item.thumbnail_url,
    item.texture_url,
    item.thumbnail,
  ];
  for (const value of candidates) {
    const image = productSafeImage(value);
    if (image) return image;
  }
  if (item.thumb) {
    return `https://vrfs.sebyplay.xyz/lockerchecker/assets/thumbs/${encodeURIComponent(item.thumb)}`;
  }
  return null;
}
function productMarketplaceImage(item: VRFSMarketplaceItem): string | null {
  const candidates = [
    item.thumbnail_url,
    item.texture_url,
    item.thumbnail,
    item.image_url,
  ];
  for (const value of candidates) {
    const image = productSafeImage(value);
    if (image) return image;
  }
  return null;
}
function productMarketplaceCreator(item: VRFSMarketplaceItem): string {
  return String(
    item.author?.nickname ??
    item.author?.username ??
    item.author?.name ??
    item.creator ??
    'Unknown',
  ).trim() || 'Unknown';
}
function productMarketplaceCreatorId(item: VRFSMarketplaceItem): string | null {
  const value = item.author?.uid ?? item.owner_uid ?? item.creator_uid;
  if (value === undefined || value === null || String(value).trim() === '') return null;
  return String(value);
}
function productMarketplaceType(item: VRFSMarketplaceItem): string {
  const category = String(item.category_id ?? item.category ?? '');
  const labels: Record<string, string> = {
    '1': 'Boots',
    '2': 'Glasses',
    '3': 'Gloves',
    '4': 'Hat',
    '5': 'Mask',
    '6': 'Scarf',
    '7': 'Other',
  };
  if (labels[category]) return labels[category];
  const sku = productNormalize(getSku(item));
  if (/boot|shoe/.test(sku)) return 'Boots';
  if (/glass|goggle/.test(sku)) return 'Glasses';
  if (/glove|hand/.test(sku)) return 'Gloves';
  if (/hat|cap|helmet/.test(sku)) return 'Hat';
  if (/mask|face/.test(sku)) return 'Mask';
  if (/scarf|neck/.test(sku)) return 'Scarf';
  return 'Other';
}
function productMarketplacePrice(item: VRFSMarketplaceItem): string {
  const price = Number(item.coins_price);
  return Number.isFinite(price) ? `${price.toLocaleString()} Credits` : 'Unknown';
}
function productCatalogEmbed(item: VRFSItem): EmbedBuilder {
  const embed = new EmbedBuilder()
    .setColor(0x5865F2)
    .setTitle(productTrim(getItemName(item), 256))
    .setDescription(`\`${productTrim(getSku(item), 512)}\``)
    .addFields(
      {
        name: 'Section',
        value: productTrim(productSection(item), 1024),
        inline: true,
      },
      {
        name: 'Price',
        value: productPrice(item),
        inline: true,
      },
      {
        name: 'Item ID',
        value: item.id !== undefined && item.id !== null ? String(item.id) : 'N/A',
        inline: true,
      },
    );
  const image = productCatalogImage(item);
  if (image) embed.setImage(image);
  return embed;
}
function productMarketplaceEmbed(item: VRFSMarketplaceItem): EmbedBuilder {
  const creator = productMarketplaceCreator(item);
  const creatorId = productMarketplaceCreatorId(item);
  const embed = new EmbedBuilder()
    .setColor(getMarketplaceActive(item) ? 0x2ECC71 : 0xE74C3C)
    .setTitle(productTrim(item.title || item.name || getSku(item) || 'Marketplace Item', 256))
    .setDescription(`Marketplace ID \`#${item.id}\`\n\`${productTrim(getSku(item), 512)}\``)
    .addFields(
      {
        name: 'Type',
        value: productMarketplaceType(item),
        inline: true,
      },
      {
        name: 'Owners',
        value: productNumber(getMarketplaceOwners(item)),
        inline: true,
      },
      {
        name: 'Price',
        value: productMarketplacePrice(item),
        inline: true,
      },
      {
        name: 'Gifts',
        value: `${productNumber(item.gifts_left)} left`,
        inline: true,
      },
      {
        name: 'Creator',
        value: creatorId ? `${productTrim(creator, 256)}\nID \`${creatorId}\`` : productTrim(creator, 256),
        inline: false,
      },
      {
        name: 'Status',
        value: getMarketplaceActive(item) ? 'Active' : 'Inactive',
        inline: true,
      },
    );
  const image = productMarketplaceImage(item);
  if (image) embed.setImage(image);
  return embed;
}
function productCatalogMatches(items: VRFSItem[], query: string): VRFSItem[] {
  const q = productNormalize(query);
  if (!q) return [];
  return items
    .map(item => {
      const name = productNormalize(getItemName(item));
      const sku = productNormalize(getSku(item));
      const section = productNormalize(productSection(item));
      const id = productNormalize(item.id);
      let score = 0;
      if (id === q || sku === q || name === q) score = 1000;
      else if (name.startsWith(q)) score = 800;
      else if (sku.startsWith(q)) score = 700;
      else if (name.includes(q)) score = 500;
      else if (sku.includes(q)) score = 400;
      else if (section.includes(q)) score = 200;
      return { item, score };
    })
    .filter(result => result.score > 0)
    .sort((a, b) => b.score - a.score || getItemName(a.item).localeCompare(getItemName(b.item)))
    .map(result => result.item);
}
function productMarketplaceMatches(items: VRFSMarketplaceItem[], query: string): VRFSMarketplaceItem[] {
  const q = productNormalize(query);
  if (!q) return [];
  return items
    .map(item => {
      const name = productNormalize(item.title ?? item.name ?? getItemName(item));
      const sku = productNormalize(getSku(item));
      const creator = productNormalize(productMarketplaceCreator(item));
      const id = productNormalize(item.id);
      let score = 0;
      if (id === q) score = 1000;
      else if (name === q || sku === q) score = 900;
      else if (name.startsWith(q)) score = 700;
      else if (name.includes(q)) score = 500;
      else if (sku.includes(q)) score = 400;
      else if (creator.includes(q)) score = 200;
      return { item, score };
    })
    .filter(result => result.score > 0)
    .sort((a, b) => b.score - a.score)
    .map(result => result.item);
}
function productPaginationToken(data: Omit<ProductPaginationSession, 'createdAt' | 'page'>): string {
  const token = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 9)}`;
  productPagination.set(token, {
    ...data,
    page: 0,
    createdAt: Date.now(),
  });
  return token;
}
function productPaginationGet(token: string): ProductPaginationSession | null {
  const session = productPagination.get(token);
  if (!session) return null;
  if (Date.now() - session.createdAt > PRODUCT_PAGE_TTL_MS) {
    productPagination.delete(token);
    return null;
  }
  return session;
}
function productPaginationCleanup(): void {
  const now = Date.now();
  for (const [token, session] of productPagination) {
    if (now - session.createdAt > PRODUCT_PAGE_TTL_MS) productPagination.delete(token);
  }
}
setInterval(productPaginationCleanup, 60_000).unref();
function productPaginationRow(token: string, page: number, totalPages: number): ActionRowBuilder<ButtonBuilder> {
  return new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder()
      .setCustomId(`product:page:${token}:prev`)
      .setLabel('Previous')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(page <= 0),
    new ButtonBuilder()
      .setCustomId(`product:page:${token}:current`)
      .setLabel(`${page + 1} / ${totalPages}`)
      .setStyle(ButtonStyle.Primary)
      .setDisabled(true),
    new ButtonBuilder()
      .setCustomId(`product:page:${token}:next`)
      .setLabel('Next')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(page >= totalPages - 1),
  );
}
function productCatalogListEmbed(
  title: string,
  description: string,
  items: VRFSItem[],
  page: number,
  pageSize = PRODUCT_PAGE_SIZE,
): { embed: EmbedBuilder; page: number; totalPages: number } {
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const safePage = Math.max(0, Math.min(page, totalPages - 1));
  const pageItems = items.slice(safePage * pageSize, safePage * pageSize + pageSize);
  const embed = new EmbedBuilder()
    .setColor(0x5865F2)
    .setTitle(title)
    .setDescription(description || 'Items');
  const lines = pageItems.map((item, index) => {
    const position = safePage * pageSize + index + 1;
    return [
      `**${position}. ${productTrim(getItemName(item), 80)}**`,
      `\`${productTrim(getSku(item), 120)}\` · ${productSection(item)} · ${productPrice(item)}`,
    ].join('\n');
  });
  embed.addFields({
    name: 'Items',
    value: lines.join('\n\n') || 'No items found.',
    inline: false,
  });
  return {
    embed,
    page: safePage,
    totalPages,
  };
}
function productMarketplaceListEmbed(
  title: string,
  description: string,
  items: VRFSMarketplaceItem[],
  page: number,
  pageSize = PRODUCT_PAGE_SIZE,
): { embed: EmbedBuilder; page: number; totalPages: number } {
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const safePage = Math.max(0, Math.min(page, totalPages - 1));
  const pageItems = items.slice(safePage * pageSize, safePage * pageSize + pageSize);
  const embed = new EmbedBuilder()
    .setColor(0x5865F2)
    .setTitle(title)
    .setDescription(description || 'Marketplace');
  const lines = pageItems.map((item, index) => {
    const position = safePage * pageSize + index + 1;
    return [
      `**${position}. ${productTrim(item.title || item.name || getSku(item), 80)}**`,
      `#${item.id} · ${productMarketplaceType(item)} · ${productNumber(getMarketplaceOwners(item))} owners`,
    ].join('\n');
  });
  embed.addFields({
    name: 'Items',
    value: lines.join('\n\n') || 'No items found.',
    inline: false,
  });
  return {
    embed,
    page: safePage,
    totalPages,
  };
}
function productLockerListEmbed(session: ProductPaginationSession, page: number): { embed: EmbedBuilder; page: number; totalPages: number } {
  const items = session.items as VRFSItem[];
  const totalPages = Math.max(1, Math.ceil(items.length / session.pageSize));
  const safePage = Math.max(0, Math.min(page, totalPages - 1));
  const pageItems = items.slice(safePage * session.pageSize, safePage * session.pageSize + session.pageSize);
  const embed = new EmbedBuilder()
    .setColor(0x2ECC71)
    .setTitle(session.title)
    .setDescription(session.description || 'Owned items');
  const lines = pageItems.map((item, index) => {
    const position = safePage * session.pageSize + index + 1;
    return [
      `**${position}. ${productTrim(getItemName(item), 80)}**`,
      `\`${productTrim(getSku(item), 120)}\` · ${productSection(item)} · ${productPrice(item)}`,
    ].join('\n');
  });
  embed.addFields({
    name: 'Collection',
    value: lines.join('\n\n') || 'No owned items.',
    inline: false,
  });
  session.page = safePage;
  return {
    embed,
    page: safePage,
    totalPages,
  };
}
function productErrorEmbed(message: string): EmbedBuilder {
  return new EmbedBuilder()
    .setColor(0xE74C3C)
    .setTitle('Request Failed')
    .setDescription(productTrim(message || 'Something went wrong.', 3800));
}
function productSimpleErrorEmbed(title: string, message: string): EmbedBuilder {
  return new EmbedBuilder()
    .setColor(0xE74C3C)
    .setTitle(title)
    .setDescription(message);
}
function productServiceValue(ok: boolean): string {
  return ok ? 'Available' : 'Unavailable';
}
export class BotManager {
  private client: Client;
  private commandsRegistered = false;
  private presenceInterval: NodeJS.Timeout | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;
  private verificationInterval: NodeJS.Timeout | null = null;
  public metrics = new MetricsCollector();
  public notifications: NotificationService;
  private commands = new Map<string, (interaction: ChatInputCommandInteraction<CacheType>) => Promise<void>>();
  constructor(private readonly botToken: string) {
    this.client = new Client({
      intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildMembers,
        GatewayIntentBits.DirectMessages,
      ],
    });
    setClient(this.client);
    this.notifications = new NotificationService(this.client, this.metrics);
    this.commands.set('stats', this.statsCommand.bind(this));
    this.commands.set('active', this.activeCommand.bind(this));
    this.commands.set('recent', this.recentCommand.bind(this));
    this.commands.set('setchannel', this.setchannelCommand.bind(this));
    this.commands.set('reset', this.resetCommand.bind(this));
    this.commands.set('status', this.statusCommand.bind(this));
    this.commands.set('metrics', this.metricsCommand.bind(this));
    this.commands.set('help', this.helpCommand.bind(this));
    this.commands.set('purge', this.purgeCommand.bind(this));
    this.commands.set('giveawaytrack', this.giveawayTrackCommand.bind(this));
    this.commands.set('eventtrack', this.eventTrackCommand.bind(this));
    this.commands.set('licenseadmin', this.licenseAdminCommand.bind(this));
    this.commands.set('revoke', this.revokeCommand.bind(this));
    this.commands.set('vrfs', this.vrfsCommand.bind(this));
    this.client.on('guildMemberUpdate', this.handleGuildMemberUpdate.bind(this));
    this.client.on('guildMemberAdd', this.handleGuildMemberAdd.bind(this));
    this.client.once('ready', async () => {
      logger.info(`Logged in as ${this.client.user?.tag}`, { component: 'BotManager' });
      await this.updatePresence();
      this.presenceInterval = setInterval(() => this.updatePresence(), 30_000);
      this.presenceInterval.unref?.();
      await this.purgeAndUpdatePresence();
      this.cleanupInterval = setInterval(() => this.purgeAndUpdatePresence(), 60_000);
      this.cleanupInterval.unref?.();
      await this.registerCommands();
      await this.sendNotificationPanel();
      await this.sendLicensePanel();
      await this.sendPremiumPanel();
      await this.assignPremiumToExistingBoosters();
      this.verificationInterval = setInterval(() => this.verifyAllPremiumRoles(), 300000);
      this.verificationInterval.unref?.();
    });
    this.client.on('interactionCreate', async (interaction: Interaction) => {
      try {
        if (interaction.isButton() && interaction.customId.startsWith('product:page:')) {
          await this.handleProductPagination(interaction);
          return;
        }
        if (interaction.isButton()) {
          if (interaction.customId === 'toggle_giveaway') {
            await this.handleNotificationToggle(interaction, 'giveaways');
            return;
          }
          if (interaction.customId === 'toggle_scrim') {
            await this.handleNotificationToggle(interaction, 'scrims');
            return;
          }
          if (interaction.customId === 'toggle_event') {
            await this.handleNotificationToggle(interaction, 'events');
            return;
          }
          if (interaction.customId === 'license_activate') {
            const channel = interaction.channel as TextChannel;
            const panel = new KeyPanel(channel);
            await panel.handleInteraction(interaction);
            return;
          }
          if (interaction.customId === 'premium_autojoiner') {
            const channel = interaction.channel as TextChannel;
            const panel = new PremiumPanel(channel);
            await panel.handleInteraction(interaction);
            return;
          }
          if (['admin_generate_key', 'admin_list_keys', 'admin_refresh'].includes(interaction.customId)) {
            if (!isOwner(interaction.user.id)) {
              await interaction.reply({
                content: 'No permission.',
                ephemeral: true,
              });
              return;
            }
            const panel = new AdminPanel();
            await panel.handleInteraction(interaction);
            return;
          }
          if (['activate_premium', 'check_premium', 'generate_key', 'list_keys', 'refresh_stats'].includes(interaction.customId)) {
            const channel = interaction.channel as TextChannel;
            const panel = new KeyPanel(channel);
            await panel.handleInteraction(interaction);
            return;
          }
          return;
        }
        if (interaction.isModalSubmit()) {
          if (interaction.customId === 'license_activate_modal') {
            const channel = interaction.channel as TextChannel;
            const panel = new KeyPanel(channel);
            await panel.handleModalSubmit(interaction);
            return;
          }
          if (interaction.customId === 'premium_autojoiner_modal') {
            const channel = interaction.channel as TextChannel;
            const panel = new PremiumPanel(channel);
            await panel.handleModalSubmit(interaction);
            return;
          }
          if (interaction.customId === 'activate_premium_modal') {
            const channel = interaction.channel as TextChannel;
            const panel = new KeyPanel(channel);
            await panel.handleModalSubmit(interaction);
            return;
          }
          return;
        }
        if (!interaction.isChatInputCommand()) return;
        const handler = this.commands.get(interaction.commandName);
        if (!handler) {
          await interaction.reply({
            content: 'Unknown command.',
            ephemeral: true,
          });
          return;
        }
        try {
          await handler(interaction);
        } catch (err) {
          logger.error(`Command error: ${interaction.commandName}`, {
            error: formatError(err),
          });
          const reply = interaction.replied || interaction.deferred
            ? interaction.editReply.bind(interaction)
            : interaction.reply.bind(interaction);
          await reply({
            content: 'Something went wrong.',
            ephemeral: true,
          });
        }
      } catch (err) {
        logger.error('Interaction handler failure', {
          error: formatError(err),
        });
      }
    });
    this.client.on('error', (err) => logger.error('Client error', { error: err }));
  }
  public async start(): Promise<void> {
    const LOGIN_TIMEOUT_MS = 10000;
    logger.info('BotManager: attempting login...', { component: 'BotManager' });
    try {
      await Promise.race([
        this.client.login(this.botToken),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Login timed out after 10s')), LOGIN_TIMEOUT_MS),
        ),
      ]);
      await Promise.race([
        new Promise<void>((resolve) => {
          if (this.client.isReady()) resolve();
          else this.client.once('ready', () => resolve());
        }),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Ready event timed out after 10s')), LOGIN_TIMEOUT_MS),
        ),
      ]);
      logger.info('BotManager started successfully', { component: 'BotManager' });
    } catch (err) {
      logger.error(`BotManager start failed: ${formatError(err)}`, {
        component: 'BotManager',
      });
      throw err;
    }
  }
  public async destroy(): Promise<void> {
    if (this.presenceInterval) {
      clearInterval(this.presenceInterval);
      this.presenceInterval = null;
    }
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    if (this.verificationInterval) {
      clearInterval(this.verificationInterval);
      this.verificationInterval = null;
    }
    this.notifications.shutdown();
    await this.client.destroy();
  }
  public async sendGiveawayNotification(data: GiveawayData & { inviteUrl?: string }): Promise<boolean> {
    this.notifications.enqueue(data, data.inviteUrl || '');
    this.metrics.recordDetection(Date.now() - data.detectedAt);
    await this.updatePresence();
    return true;
  }
  private async sendNotificationPanel(): Promise<void> {
    const panelChannelId = process.env.PANEL_CHANNEL_ID || CONFIG.trackerChannelId;
    const channel = this.client.channels.cache.get(panelChannelId) as TextChannel | undefined;
    if (!channel) {
      logger.warn('Notification panel channel not found', {
        component: 'BotManager',
        channelId: panelChannelId,
      });
      return;
    }
    try {
      const messages = await channel.messages.fetch({ limit: 20 });
      const oldPanel = messages.find(m =>
        m.author.id === this.client.user?.id &&
        m.embeds.length > 0 &&
        m.embeds[0]?.title === 'Notifications'
      );
      if (oldPanel) await oldPanel.delete().catch(() => {});
    } catch {}
    const embed = new EmbedBuilder()
      .setColor(0x5865F2)
      .setTitle('Notifications')
      .setDescription('Choose the notifications you want to receive.')
      .addFields(
        {
          name: 'Giveaways',
          value: 'Receive notifications for new giveaways.',
          inline: false,
        },
        {
          name: 'Scrims',
          value: 'Receive notifications for scrim announcements.',
          inline: false,
        },
        {
          name: 'Events',
          value: 'Receive notifications for events.',
          inline: false,
        },
      );
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder()
        .setCustomId('toggle_giveaway')
        .setLabel('Giveaway')
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId('toggle_scrim')
        .setLabel('Scrim')
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId('toggle_event')
        .setLabel('Event')
        .setStyle(ButtonStyle.Primary),
    );
    await channel.send({
      embeds: [embed],
      components: [row],
    });
    logger.info('Notification panel sent', { channelId: panelChannelId });
  }
  private async handleNotificationToggle(
    interaction: ButtonInteraction,
    type: 'giveaways' | 'scrims' | 'events',
  ): Promise<void> {
    await interaction.deferReply({ ephemeral: true });
    const userId = interaction.user.id;
    const settings = await getUserNotificationSettings(userId);
    const currentState = settings[type];
    const newState = !currentState;
    await updateUserNotificationSetting(userId, type, newState);
    let roleId: string | undefined;
    const typeLabel = {
      giveaways: 'Giveaway',
      scrims: 'Scrim',
      events: 'Event',
    }[type];
    if (type === 'giveaways') roleId = process.env.PING_ROLE_ID;
    else if (type === 'scrims') roleId = process.env.SCRIM_ROLE_ID;
    else if (type === 'events') roleId = process.env.EVENT_ROLE_ID;
    if (roleId && interaction.guild) {
      try {
        const member = await interaction.guild.members.fetch(userId);
        const role = interaction.guild.roles.cache.get(roleId);
        if (role) {
          if (newState) await member.roles.add(role);
          else await member.roles.remove(role);
        }
      } catch (error) {
        logger.error(`Failed to ${newState ? 'add' : 'remove'} role for ${type}`, {
          userId,
          error: String(error),
        });
      }
    }
    await interaction.editReply({
      content: `${typeLabel} notifications ${newState ? 'enabled' : 'disabled'} for you.`,
    });
  }
  public async sendScrimNotification(data: any): Promise<boolean> {
    let channelId: string;
    let channelName: string;
    if (data.type === 'scrim') {
      channelId = CONFIG.scrimChannelId || CONFIG.trackerChannelId;
      channelName = 'Scrim';
    } else {
      channelId = CONFIG.eventChannelId || CONFIG.trackerChannelId;
      channelName = 'Event';
    }
    const channel = this.client.channels.cache.get(channelId) as TextChannel | undefined;
    if (!channel) {
      logger.warn(`${channelName} channel not found for notification`, {
        component: 'BotManager',
        channelId,
      });
      const fallbackChannel = this.client.channels.cache.get(CONFIG.trackerChannelId) as TextChannel | undefined;
      if (!fallbackChannel) {
        logger.error('No channel available for notification', {
          component: 'BotManager',
        });
        return false;
      }
      return this.sendScrimToChannel(data, fallbackChannel);
    }
    return this.sendScrimToChannel(data, channel);
  }
  private async sendScrimToChannel(data: any, channel: TextChannel): Promise<boolean> {
    const typeLabel = {
      scrim: 'Scrim',
      squid_game: 'Squid Game',
      gagaball: 'Gagaball',
    }[data.type] || 'Event';
    const typeColor = {
      scrim: 0x5865F2,
      squid_game: 0xFF6B6B,
      gagaball: 0x4ECDC4,
    }[data.type] || 0x5865F2;
    const guild = this.client.guilds.cache.get(data.guildId);
    const guildName = guild?.name || data.guildName || 'Unknown';
    const guildIcon = data.guildIcon || guild?.iconURL({ size: 512 }) || null;
    const guildBanner = data.guildBanner || guild?.bannerURL({ size: 1024 }) || null;
    const memberCount = (data.memberCount || guild?.memberCount) ?? null;
    let inviteUrl = data.inviteUrl || 'No invite available';
    if (inviteUrl === 'No invite available' && data.guildId) {
      try {
        const targetGuild = this.client.guilds.cache.get(data.guildId);
        if (targetGuild) {
          const invites = await targetGuild.invites.fetch().catch(() => new Collection<string, Invite>());
          const existingInvite = invites.find((inv: Invite) => inv.channelId === data.channelId && inv.maxUses === 0);
          if (existingInvite) {
            inviteUrl = existingInvite.url;
          } else {
            const targetChannel = targetGuild.channels.cache.get(data.channelId);
            if (targetChannel && targetChannel.isTextBased() && 'createInvite' in targetChannel) {
              const perms = targetChannel.permissionsFor(this.client.user?.id || '');
              if (perms?.has('CreateInstantInvite')) {
                const newInvite = await targetChannel.createInvite({
                  maxAge: 86400,
                  maxUses: 0,
                  reason: 'Scrim notification',
                });
                inviteUrl = newInvite.url;
              }
            }
          }
        }
      } catch (err) {
        logger.debug(`Could not generate invite for notification: ${formatError(err)}`);
      }
    }
    let pingMention = '@everyone';
    if (data.type === 'scrim') {
      const scrimRoleId = process.env.SCRIM_ROLE_ID;
      if (scrimRoleId) pingMention = `<@&${scrimRoleId}>`;
    } else {
      const eventRoleId = process.env.EVENT_ROLE_ID;
      if (eventRoleId) pingMention = `<@&${eventRoleId}>`;
    }
    const description = [
      `### Details`,
      `**Server:** ${guildName}`,
      `**Channel:** #${data.channelName}`,
      data.host ? `**Host:** ${data.host}` : '',
      data.coHost ? `**Co-Host:** ${data.coHost}` : '',
      data.time ? `**Time:** ${data.time}` : '',
      data.teams ? `**Teams:** ${data.teams}` : '',
      data.region ? `**Region:** ${data.region}` : '',
      data.reward ? `**Reward:** ${data.reward}` : '',
      data.ticks !== null ? `**Ticks:** ${data.ticks}+` : '',
      ``,
      `### Time`,
      `**Detected:** <t:${Math.floor(data.detectedAt / 1000)}:R>`,
      ``,
      `### Links`,
      `**Invite:** ${inviteUrl}`,
      memberCount ? `**Members:** ${memberCount.toLocaleString()}` : '',
    ].filter(Boolean).join('\n');
    const embed = new EmbedBuilder()
      .setAuthor({
        name: `${typeLabel} Detected`,
        iconURL: this.client.user?.displayAvatarURL(),
      })
      .setTitle(data.reward || `${typeLabel} Event`)
      .setDescription(description)
      .setColor(typeColor);
    if (guildIcon) embed.setThumbnail(guildIcon);
    if (guildBanner) embed.setImage(guildBanner);
    const messageUrl = `https://discord.com/channels/${data.guildId}/${data.channelId}/${data.messageId}`;
    const row = new ActionRowBuilder<ButtonBuilder>();
    if (inviteUrl.startsWith('http')) {
      row.addComponents(
        new ButtonBuilder()
          .setLabel('Join Server')
          .setStyle(ButtonStyle.Link)
          .setURL(inviteUrl),
      );
    }
    row.addComponents(
      new ButtonBuilder()
        .setLabel('Message')
        .setStyle(ButtonStyle.Link)
        .setURL(messageUrl),
    );
    try {
      await channel.send({
        content: pingMention,
        embeds: [embed],
        components: [row],
      });
      return true;
    } catch (error) {
      logger.error('Failed to send notification', {
        component: 'BotManager',
        error: formatError(error),
      });
      return false;
    }
  }
  private async deleteAllPremiumData(userId: string, guildId: string): Promise<void> {
    await removePremiumUser(userId, guildId);
    await removeBoosterPremium(userId, guildId);
    await updateUserToken(userId, guildId, '', '');
    await updateUserWebhook(userId, guildId, '');
    try {
      const autoJoinCol = await getAutoJoinEntriesCollection();
      await autoJoinCol.deleteMany({ userId });
    } catch (error) {
      logger.warn('Failed to delete auto-join entries', {
        userId,
        error: String(error),
      });
    }
    try {
      const { stopTokenSession } = await import('./premium/tokenManager.js');
      stopTokenSession(userId, guildId);
    } catch {}
    try {
      clearPremiumCache(userId);
    } catch {}
    logger.debug('All premium data deleted for user', {
      userId,
      guildId,
    });
  }
  private async verifyAllPremiumRoles(): Promise<void> {
    const guildId = process.env.GUILD_ID;
    const premiumRoleId = process.env.PREMIUM_ROLE_ID;
    if (!guildId || !premiumRoleId) return;
    try {
      const guild = await this.client.guilds.fetch(guildId);
      const members = await guild.members.fetch();
      const allPremiumUsers = await getAllPremiumUsers(guildId);
      const validUserIds = new Set(allPremiumUsers.map(u => u.userId));
      const boosterRoleId = process.env.BOOSTER_ROLE_ID;
      let fixed = 0;
      for (const [, member] of members) {
        const hasRole = member.roles.cache.has(premiumRoleId);
        const isBooster = boosterRoleId ? member.roles.cache.has(boosterRoleId) : false;
        const shouldHaveRole = validUserIds.has(member.id) || isBooster;
        if (hasRole && !shouldHaveRole) {
          await member.roles.remove(premiumRoleId);
          fixed++;
          logger.warn('Removed unauthorized premium role', {
            userId: member.id,
            username: member.user.username,
          });
        } else if (!hasRole && shouldHaveRole) {
          await member.roles.add(premiumRoleId);
          fixed++;
          logger.info('Added missing premium role', {
            userId: member.id,
            username: member.user.username,
          });
        }
      }
      if (fixed > 0) {
        logger.info(`Premium role verification fixed ${fixed} members`, {
          component: 'BotManager',
        });
      }
    } catch (error) {
      logger.error('Premium role verification failed', {
        component: 'BotManager',
        error: formatError(error),
      });
    }
  }
  private async resolveInviteUrl(
    guildId: string,
    channelId: string,
    fallbackInvite?: string | null,
  ): Promise<string> {
    if (fallbackInvite && fallbackInvite.startsWith('http')) return fallbackInvite;
    let inviteUrl = 'No invite available';
    try {
      const guild = this.client.guilds.cache.get(guildId);
      if (guild) {
        const invites = await guild.invites.fetch().catch(() => new Collection<string, Invite>());
        const existingInvite = invites.find((inv: Invite) => inv.channelId === channelId && inv.maxUses === 0);
        if (existingInvite) {
          inviteUrl = existingInvite.url;
        } else {
          const channel = guild.channels.cache.get(channelId);
          if (channel && channel.isTextBased() && 'createInvite' in channel) {
            const perms = channel.permissionsFor(this.client.user?.id || '');
            if (perms?.has('CreateInstantInvite')) {
              const newInvite = await channel.createInvite({
                maxAge: 86400,
                maxUses: 0,
                reason: 'Giveaway notification',
              });
              inviteUrl = newInvite.url;
            }
          }
        }
      }
    } catch (err) {
      logger.debug(`Failed to resolve invite: ${formatError(err)}`);
    }
    return inviteUrl;
  }
  public async sendWatchlistDM(
    userId: string,
    prize: string,
    guildName: string,
    channelName: string,
    endsAt: number | null,
    messageUrl: string,
    guildId?: string,
    guildIcon?: string | null,
    detectedAt?: number,
    inviteUrl?: string | null,
    guildBanner?: string | null,
    memberCount?: number | null,
  ): Promise<boolean> {
    try {
      let user;
      try {
        user = await this.client.users.fetch(userId);
      } catch {
        user = this.client.users.cache.get(userId);
        if (!user) return false;
      }
      let dmChannel;
      try {
        dmChannel = await user.createDM();
      } catch {
        return false;
      }
      if (!dmChannel) return false;
      const urlParts = messageUrl.split('/');
      const channelId = urlParts[5] || '';
      let resolvedInvite = 'No invite available';
      if (guildId && channelId) resolvedInvite = await this.resolveInviteUrl(guildId, channelId, inviteUrl);
      const endTimestamp = endsAt
        ? Math.floor(endsAt / 1000)
        : Math.floor((Date.now() + 3600000) / 1000);
      const winnerCount = extractWinnerCount(prize);
      const description = [
        `### Details`,
        `**Server:** ${guildName}`,
        `**Channel:** #${channelName}`,
        `**Winners:** ${winnerCount}`,
        ``,
        `### Time`,
        `**Ends:** <t:${endTimestamp}:F>`,
        `**Countdown:** <t:${endTimestamp}:R>`,
        ``,
        `### Links`,
        `**Invite:** ${resolvedInvite}`,
        memberCount ? `**Members:** ${memberCount.toLocaleString()}` : '',
      ].filter(Boolean).join('\n');
      const embed = new EmbedBuilder()
        .setAuthor({
          name: 'New Giveaway',
          iconURL: this.client.user?.displayAvatarURL(),
        })
        .setTitle(prize || 'Unknown Prize')
        .setDescription(description)
        .setColor(0x5865F2);
      if (guildIcon) embed.setThumbnail(guildIcon);
      if (guildBanner) embed.setImage(guildBanner);
      const row = new ActionRowBuilder<ButtonBuilder>();
      if (resolvedInvite.startsWith('http')) {
        row.addComponents(
          new ButtonBuilder()
            .setLabel('Join Server')
            .setStyle(ButtonStyle.Link)
            .setURL(resolvedInvite),
        );
      }
      row.addComponents(
        new ButtonBuilder()
          .setLabel('Message')
          .setStyle(ButtonStyle.Link)
          .setURL(messageUrl),
      );
      await dmChannel.send({
        embeds: [embed],
        components: [row],
      });
      return true;
    } catch (err) {
      const errorMsg = formatError(err);
      if (errorMsg.includes('Cannot send messages to this user')) {
        logger.debug(`User ${userId} has DMs disabled`);
      } else if (errorMsg.includes('rate limit')) {
        logger.warn(`Rate limit hit for user ${userId}`);
      } else {
        logger.debug(`Failed to send DM to ${userId}`, {
          error: errorMsg,
        });
      }
      return false;
    }
  }
  public async sendGiveawayEndedDM(
    userId: string,
    prize: string,
    guildName: string,
    channelName: string,
    messageUrl: string,
    guildIcon?: string | null,
  ): Promise<boolean> {
    try {
      let user;
      try {
        user = await this.client.users.fetch(userId);
      } catch {
        user = this.client.users.cache.get(userId);
        if (!user) return false;
      }
      let dmChannel;
      try {
        dmChannel = await user.createDM();
      } catch {
        return false;
      }
      if (!dmChannel) return false;
      const embed = new EmbedBuilder()
        .setAuthor({
          name: 'Giveaway Ended',
          iconURL: this.client.user?.displayAvatarURL(),
        })
        .setTitle(prize || 'Giveaway Ended')
        .setDescription([
          `**Server:** ${guildName}`,
          `**Channel:** #${channelName}`,
          '',
          'This giveaway has ended. Better luck next time.',
          '',
          `[View giveaway](${messageUrl})`,
        ].join('\n'))
        .setColor(0xE74C3C);
      if (guildIcon) embed.setThumbnail(guildIcon);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder()
          .setLabel('View Giveaway')
          .setStyle(ButtonStyle.Link)
          .setURL(messageUrl),
      );
      await dmChannel.send({
        embeds: [embed],
        components: [row],
      });
      return true;
    } catch {
      return false;
    }
  }
  private async giveawayTrackCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    const sub = interaction.options.getSubcommand();
    if (sub === 'add') await this.giveawayAdd(interaction);
    else if (sub === 'remove') await this.giveawayRemove(interaction);
    else if (sub === 'list') await this.giveawayList(interaction);
    else if (sub === 'clear') await this.giveawayClear(interaction);
  }
  private async giveawayAdd(interaction: ChatInputCommandInteraction<CacheType>) {
    const item = interaction.options.getString('item', true).trim().toLowerCase();
    if (item.length < 2 || item.length > 50) {
      await interaction.reply({
        content: 'Item must be 2-50 characters.',
        ephemeral: true,
      });
      return;
    }
    await addItem(interaction.user.id, item);
    const items = await getItems(interaction.user.id);
    await interaction.reply({
      content: `Tracking giveaway item **${item}**\n\nYour items:\n${items.map(i => `- ${i}`).join('\n')}`,
      ephemeral: true,
    });
  }
  private async giveawayRemove(interaction: ChatInputCommandInteraction<CacheType>) {
    const item = interaction.options.getString('item', true).trim().toLowerCase();
    const removed = await removeItem(interaction.user.id, item);
    if (!removed) {
      await interaction.reply({
        content: `"${item}" not in your tracked items.`,
        ephemeral: true,
      });
      return;
    }
    const items = await getItems(interaction.user.id);
    await interaction.reply({
      content: `Removed **${item}**\n\nYour items:\n${items.map(i => `- ${i}`).join('\n')}`,
      ephemeral: true,
    });
  }
  private async giveawayList(interaction: ChatInputCommandInteraction<CacheType>) {
    const items = await getItems(interaction.user.id);
    if (items.length === 0) {
      await interaction.reply({
        content: 'No tracked items. Use `/giveawaytrack add <item>` to start tracking giveaways.',
        ephemeral: true,
      });
      return;
    }
    await interaction.reply({
      content: `**Your tracked giveaway items (${items.length})**\n${items.map(i => `- ${i}`).join('\n')}`,
      ephemeral: true,
    });
  }
  private async giveawayClear(interaction: ChatInputCommandInteraction<CacheType>) {
    const items = await getItems(interaction.user.id);
    if (items.length === 0) {
      await interaction.reply({
        content: 'Tracked items list is empty.',
        ephemeral: true,
      });
      return;
    }
    await clearItems(interaction.user.id);
    await interaction.reply({
      content: `Cleared ${items.length} tracked items.`,
      ephemeral: true,
    });
  }
  private async eventTrackCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    const sub = interaction.options.getSubcommand();
    if (sub === 'add') await this.eventAdd(interaction);
    else if (sub === 'remove') await this.eventRemove(interaction);
    else if (sub === 'list') await this.eventList(interaction);
    else if (sub === 'clear') await this.eventClear(interaction);
  }
  private async eventAdd(interaction: ChatInputCommandInteraction<CacheType>) {
    const filter = interaction.options.getString('filter', true).trim().toLowerCase();
    const validFilters = ['scrim', 'squid', 'squid_game', 'gagaball', '2v2', '3v3', '4v4', '5v5', '1v1', 'vrll', 'vrel', 'vucl'];
    const matchedFilter = validFilters.find(f => filter.includes(f));
    if (!matchedFilter && filter.length < 2) {
      await interaction.reply({
        content: 'Invalid filter. Valid filters: scrim, squid, squid_game, gagaball, 2v2, 3v3, 4v4, 5v5, 1v1, vrll, vrel, vucl',
        ephemeral: true,
      });
      return;
    }
    const eventItem = `event:${filter}`;
    await addItem(interaction.user.id, eventItem);
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter(i => i.startsWith('event:'));
    await interaction.reply({
      content: `Tracking event filter **${filter}**\n\nYour event filters:\n${eventItems.map(i => `- ${i.replace('event:', '')}`).join('\n')}`,
      ephemeral: true,
    });
  }
  private async eventRemove(interaction: ChatInputCommandInteraction<CacheType>) {
    const filter = interaction.options.getString('filter', true).trim().toLowerCase();
    const eventItem = `event:${filter}`;
    const removed = await removeItem(interaction.user.id, eventItem);
    if (!removed) {
      await interaction.reply({
        content: `"${filter}" not in your event filters.`,
        ephemeral: true,
      });
      return;
    }
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter(i => i.startsWith('event:'));
    await interaction.reply({
      content: `Removed event filter **${filter}**\n\nYour event filters:\n${eventItems.map(i => `- ${i.replace('event:', '')}`).join('\n')}`,
      ephemeral: true,
    });
  }
  private async eventList(interaction: ChatInputCommandInteraction<CacheType>) {
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter(i => i.startsWith('event:'));
    if (eventItems.length === 0) {
      await interaction.reply({
        content: 'No event filters. Use `/eventtrack add <filter>` to start tracking events.',
        ephemeral: true,
      });
      return;
    }
    await interaction.reply({
      content: `**Your event filters (${eventItems.length})**\n${eventItems.map(i => `- ${i.replace('event:', '')}`).join('\n')}`,
      ephemeral: true,
    });
  }
  private async eventClear(interaction: ChatInputCommandInteraction<CacheType>) {
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter(i => i.startsWith('event:'));
    if (eventItems.length === 0) {
      await interaction.reply({
        content: 'Event filters list is empty.',
        ephemeral: true,
      });
      return;
    }
    for (const item of eventItems) {
      await removeItem(interaction.user.id, item);
    }
    await interaction.reply({
      content: `Cleared ${eventItems.length} event filters.`,
      ephemeral: true,
    });
  }
  private async licenseAdminCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    if (!await requireOwner(interaction)) return;
    await interaction.deferReply({ ephemeral: true });
    const panel = new AdminPanel();
    await panel.sendPanel(interaction);
  }
  private async revokeCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    if (!await requireAdmin(interaction)) return;
    const user = interaction.options.getUser('user', true);
    const guildId = interaction.guildId;
    if (!guildId) {
      await interaction.reply({
        content: 'This command must be used in a server.',
        ephemeral: true,
      });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    try {
      const hasPremium = await isPremiumUser(user.id, guildId);
      if (!hasPremium) {
        await interaction.editReply({
          content: `User <@${user.id}> does not have premium access.`,
        });
        return;
      }
      const premiumUser = await getPremiumUser(user.id, guildId);
      const source = premiumUser?.source || 'unknown';
      await this.deleteAllPremiumData(user.id, guildId);
      try {
        const guild = await this.client.guilds.fetch(guildId);
        const member = await guild.members.fetch(user.id).catch(() => null);
        if (member) {
          const premiumRoleId = process.env.PREMIUM_ROLE_ID;
          if (premiumRoleId && member.roles.cache.has(premiumRoleId)) {
            await member.roles.remove(premiumRoleId);
          }
        }
      } catch (roleError) {
        logger.warn('Could not remove premium role during revoke', {
          userId: user.id,
          error: String(roleError),
        });
      }
      logger.info('Premium revoked by admin', {
        adminId: interaction.user.id,
        userId: user.id,
        guildId,
        source,
      });
      await interaction.editReply({
        content: `Successfully revoked premium from <@${user.id}>. All associated data has been deleted.`,
      });
    } catch (error) {
      logger.error('Revoke command failed', {
        adminId: interaction.user.id,
        userId: user.id,
        error: String(error),
      });
      await interaction.editReply({
        content: `Failed to revoke premium: ${error instanceof Error ? error.message : 'Unknown error'}`,
      });
    }
  }
  private async statsCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    await deferReply(interaction, false);
    const stats = await getStats();
    const totalEver = await getTotalDetected();
    let scrimStats: Awaited<ReturnType<typeof getScrimStats>> | null = null;
    try {
      scrimStats = await getScrimStats();
    } catch {}
    const embed = new EmbedBuilder()
      .setColor(0x00AAFF)
      .setTitle('Tracker Stats')
      .addFields(
        {
          name: 'Total Giveaways Tracked',
          value: String(totalEver),
          inline: true,
        },
        {
          name: 'Active Giveaways',
          value: String(stats.activeGiveaways),
          inline: true,
        },
        {
          name: 'Servers',
          value: String(stats.serversWithGiveaways),
          inline: true,
        },
        {
          name: 'Last Detection',
          value: stats.lastDetected ? formatTimestamp(stats.lastDetected) : 'Never',
          inline: false,
        },
      );
    if (scrimStats) {
      embed.addFields(
        {
          name: 'Total Events',
          value: String(scrimStats.total),
          inline: true,
        },
        {
          name: 'Active Events',
          value: String(scrimStats.active),
          inline: true,
        },
        {
          name: 'Scrims',
          value: String(scrimStats.byType.scrim),
          inline: true,
        },
        {
          name: 'Squid Games',
          value: String(scrimStats.byType.squid_game),
          inline: true,
        },
        {
          name: 'Gagaballs',
          value: String(scrimStats.byType.gagaball),
          inline: true,
        },
      );
    }
    await interaction.editReply({ embeds: [embed] });
  }
  private async activeCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    await deferReply(interaction, false);
    const active = await getActiveGiveaways(10);
    if (active.length === 0) {
      await interaction.editReply({
        content: 'Nothing active right now.',
      });
      return;
    }
    const embed = new EmbedBuilder()
      .setColor(0xFFD700)
      .setTitle(`${active.length} Active Giveaways`);
    for (const g of active.slice(0, 10)) {
      const ends = g.endsAt ? `<t:${Math.floor(g.endsAt / 1000)}:R>` : 'Unknown';
      embed.addFields({
        name: truncate(g.prize, 50),
        value: `${g.guildName} - #${g.channelName}\nEnds: ${ends}`,
        inline: false,
      });
    }
    await interaction.editReply({ embeds: [embed] });
  }
  private async recentCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    await deferReply(interaction, false);
    const recent = await getAllGiveaways(10);
    if (recent.length === 0) {
      await interaction.editReply({
        content: 'Nothing yet.',
      });
      return;
    }
    const embed = new EmbedBuilder()
      .setColor(0x5865F2)
      .setTitle('Recent Giveaways');
    for (const g of recent) {
      embed.addFields({
        name: `${g.status === 'active' ? '[Active]' : '[Ended]'} ${truncate(g.prize, 40)}`,
        value: `${g.guildName}\n${formatTimestamp(g.detectedAt)}`,
        inline: false,
      });
    }
    await interaction.editReply({
      embeds: [embed],
    });
  }
  private async setchannelCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    if (!await requireAdmin(interaction)) return;
    const channel = interaction.options.getChannel('channel', true);
    if (![ChannelType.GuildText, ChannelType.GuildAnnouncement].includes(channel.type)) {
      await interaction.reply({
        content: 'Pick a text channel.',
        ephemeral: true,
      });
      return;
    }
    (CONFIG as any).trackerChannelId = channel.id;
    await interaction.reply({
      content: `Set to ${channel}`,
      ephemeral: true,
    });
  }
  private async resetCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    if (!await requireAdmin(interaction)) return;
    await deferReply(interaction, true);
    await resetDatabase();
    await interaction.editReply({
      content: 'Wiped.',
    });
  }
  private async statusCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    if (!await requireAdmin(interaction)) return;
    await deferReply(interaction, false);
    const stats = await getStats();
    const totalEver = await getTotalDetected();
    const embed = new EmbedBuilder()
      .setColor(0x00FF00)
      .setTitle('Running')
      .addFields(
        {
          name: 'Total Giveaways',
          value: String(totalEver),
          inline: true,
        },
        {
          name: 'Active',
          value: String(stats.activeGiveaways),
          inline: true,
        },
        {
          name: 'Servers',
          value: String(stats.serversWithGiveaways),
          inline: true,
        },
        {
          name: 'Channel',
          value: `<#${CONFIG.trackerChannelId}>`,
          inline: false,
        },
      );
    await interaction.editReply({
      embeds: [embed],
    });
  }
  private async metricsCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    if (!await requireAdmin(interaction)) return;
    await deferReply(interaction, false);
    const m = this.metrics.getSnapshot();
    const embed = new EmbedBuilder()
      .setColor(0x3498DB)
      .setTitle('Performance Metrics')
      .addFields(
        {
          name: 'Giveaways Detected',
          value: String(m.giveawaysDetected),
          inline: true,
        },
        {
          name: 'Notifications Sent',
          value: String(m.notificationsSent),
          inline: true,
        },
        {
          name: 'Failed Notifications',
          value: String(m.notificationsFailed),
          inline: true,
        },
        {
          name: 'Retry Attempts',
          value: String(m.retryAttempts),
          inline: true,
        },
        {
          name: 'Avg Detection to Notify',
          value: `${m.avgDetectionLatency}ms`,
          inline: true,
        },
        {
          name: 'Avg Discord Latency',
          value: `${m.avgDiscordLatency}ms`,
          inline: true,
        },
      );
    await interaction.editReply({
      embeds: [embed],
    });
  }
  private async helpCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    await deferReply(interaction, false);
    const embed = new EmbedBuilder()
      .setColor(0x9B59B6)
      .setTitle('Commands')
      .addFields(
        {
          name: 'Giveaways',
          value: '`/stats`\n`/active`\n`/recent`\n`/giveawaytrack`\n`/eventtrack`',
          inline: false,
        },
        {
          name: 'System',
          value: '`/status`\n`/metrics`\n`/setchannel`\n`/reset`\n`/purge`',
          inline: false,
        },
        {
          name: 'Premium',
          value: '`/revoke`\n`/licenseadmin`',
          inline: false,
        },
        {
          name: 'Player and Locker',
          value: '`/vrfs player`\n`/vrfs locker`\n`/vrfs locker-search`\n`/vrfs locker-section`\n`/vrfs locker-item`',
          inline: false,
        },
        {
          name: 'Items',
          value: '`/vrfs item`\n`/vrfs items`\n`/vrfs item-id`\n`/vrfs random-item`\n`/vrfs item-section`',
          inline: false,
        },
        {
          name: 'Marketplace',
          value: '`/vrfs marketplace`\n`/vrfs marketplace-search`\n`/vrfs marketplace-creator`\n`/vrfs marketplace-top`\n`/vrfs marketplace-new`\n`/vrfs marketplace-stats`\n`/vrfs marketplace-compare`',
          inline: false,
        },
      );
    await interaction.editReply({
      embeds: [embed],
    });
  }
  private async purgeCommand(interaction: ChatInputCommandInteraction<CacheType>) {
    if (!await requireAdmin(interaction)) return;
    const amount = interaction.options.getInteger('amount') || 50;
    await deferReply(interaction, true);
    const channel = interaction.channel as TextChannel;
    if (!channel) return;
    try {
      const messages = await channel.messages.fetch({ limit: 100 });
      const botMessages = messages.filter(m => m.author.id === this.client.user?.id);
      const toDelete = botMessages.first(amount);
      if (toDelete.length === 0) {
        await interaction.editReply({
          content: 'Nothing to delete.',
        });
        return;
      }
      await channel.bulkDelete(toDelete, true);
      await interaction.editReply({
        content: `Deleted ${toDelete.length}.`,
      });
    } catch {
      await interaction.editReply({
        content: 'Failed.',
      });
    }
  }
  private async sendLicensePanel(): Promise<void> {
    const panelChannelId = process.env.LICENSE_PANEL_CHANNEL_ID;
    if (!panelChannelId) {
      logger.warn('LICENSE_PANEL_CHANNEL_ID not set', {
        component: 'BotManager',
      });
      return;
    }
    const channel = this.client.channels.cache.get(panelChannelId) as TextChannel | undefined;
    if (!channel) {
      logger.warn('License panel channel not found', {
        component: 'BotManager',
        channelId: panelChannelId,
      });
      return;
    }
    try {
      const messages = await channel.messages.fetch({ limit: 20 });
      const oldPanel = messages.find(m =>
        m.author.id === this.client.user?.id &&
        m.embeds.length > 0 &&
        m.embeds[0]?.title === 'Premium Access'
      );
      if (oldPanel) await oldPanel.delete().catch(() => {});
      const panel = new KeyPanel(channel);
      await panel.sendPanel();
      logger.info('License panel sent', { channelId: panelChannelId });
    } catch (error) {
      logger.error('Failed to send license panel', {
        error: formatError(error),
      });
    }
  }
  private async sendPremiumPanel(): Promise<void> {
    const panelChannelId = process.env.PREMIUM_PANEL_CHANNEL_ID;
    if (!panelChannelId) {
      logger.warn('PREMIUM_PANEL_CHANNEL_ID not set', {
        component: 'BotManager',
      });
      return;
    }
    const channel = this.client.channels.cache.get(panelChannelId) as TextChannel | undefined;
    if (!channel) {
      logger.warn('Premium panel channel not found', {
        component: 'BotManager',
        channelId: panelChannelId,
      });
      return;
    }
    try {
      const messages = await channel.messages.fetch({ limit: 20 });
      const oldPanel = messages.find(m =>
        m.author.id === this.client.user?.id &&
        m.embeds.length > 0 &&
        m.embeds[0]?.title === 'Premium Panel'
      );
      if (oldPanel) await oldPanel.delete().catch(() => {});
      const panel = new PremiumPanel(channel);
      await panel.sendPanel();
      logger.info('Premium panel sent', { channelId: panelChannelId });
    } catch (error) {
      logger.error('Failed to send premium panel', {
        error: formatError(error),
      });
    }
  }
  private async handleGuildMemberUpdate(oldMember: any, newMember: any): Promise<void> {
    const guildId = process.env.GUILD_ID;
    if (!guildId) return;
    if (newMember.guild.id !== guildId) return;
    const boosterRoleId = process.env.BOOSTER_ROLE_ID;
    const premiumRoleId = process.env.PREMIUM_ROLE_ID;
    if (!boosterRoleId || !premiumRoleId) return;
    const oldRoles = oldMember.roles.cache;
    const newRoles = newMember.roles.cache;
    const hadBooster = oldRoles.has(boosterRoleId);
    const hasBooster = newRoles.has(boosterRoleId);
    if (!hadBooster && hasBooster) {
      try {
        await newMember.roles.add(premiumRoleId);
        await setPremiumUser(newMember.id, guildId, 'booster');
        await setBoosterPremium(newMember.id, guildId, true);
        logger.info('Premium role added to booster', {
          userId: newMember.id,
        });
      } catch (error) {
        logger.error('Failed to add premium role to booster', {
          userId: newMember.id,
          error: String(error),
        });
      }
      return;
    }
    if (hadBooster && !hasBooster) {
      try {
        await newMember.roles.remove(premiumRoleId);
        await this.deleteAllPremiumData(newMember.id, guildId);
        logger.info('Premium data fully deleted for unbooster', {
          userId: newMember.id,
          guildId,
        });
      } catch (error) {
        logger.error('Failed to remove premium data from unbooster', {
          userId: newMember.id,
          error: String(error),
        });
      }
      return;
    }
  }
  private async handleGuildMemberAdd(member: any): Promise<void> {
    const guildId = process.env.GUILD_ID;
    if (!guildId) return;
    if (member.guild.id !== guildId) return;
    const boosterRoleId = process.env.BOOSTER_ROLE_ID;
    const premiumRoleId = process.env.PREMIUM_ROLE_ID;
    if (!boosterRoleId || !premiumRoleId) return;
    const isBooster = member.roles.cache.has(boosterRoleId);
    if (isBooster) {
      try {
        const existing = await getPremiumUser(member.id, guildId);
        if (!existing || !existing.isPremium) {
          await member.roles.add(premiumRoleId);
          await setPremiumUser(member.id, guildId, 'booster');
          await setBoosterPremium(member.id, guildId, true);
          logger.info('Premium role added to booster on join', {
            userId: member.id,
          });
        }
      } catch (error) {
        logger.error('Failed to add premium role to booster on join', {
          userId: member.id,
          error: String(error),
        });
      }
    }
  }
  private async assignPremiumToExistingBoosters(): Promise<void> {
    const guildId = process.env.GUILD_ID;
    if (!guildId) return;
    const boosterRoleId = process.env.BOOSTER_ROLE_ID;
    const premiumRoleId = process.env.PREMIUM_ROLE_ID;
    if (!boosterRoleId || !premiumRoleId) return;
    try {
      const guild = this.client.guilds.cache.get(guildId);
      if (!guild) return;
      const members = await guild.members.fetch();
      let count = 0;
      for (const [, member] of members) {
        if (member.roles.cache.has(boosterRoleId)) {
          const existing = await getPremiumUser(member.id, guildId);
          if (!existing || !existing.isPremium) {
            try {
              await member.roles.add(premiumRoleId);
              await setPremiumUser(member.id, guildId, 'booster');
              await setBoosterPremium(member.id, guildId, true);
              count++;
            } catch (error) {
              logger.error('Failed to add premium role to existing booster', {
                userId: member.id,
                error: String(error),
              });
            }
          }
        }
      }
      if (count > 0) {
        logger.info(`Added premium role to ${count} existing boosters`, {
          component: 'BotManager',
        });
      }
    } catch (error) {
      logger.error('Failed to assign premium to existing boosters', {
        error: String(error),
      });
    }
  }
  private async updatePresence() {
    const totalEver = await getTotalDetected();
    this.client.user?.setPresence({
      activities: [
        {
          name: `${totalEver} giveaways tracked`,
          type: ActivityType.Watching,
        },
      ],
      status: 'online',
    });
  }
  private async purgeAndUpdatePresence() {
    const removed = await purgeEndedGiveaways();
    if (removed.length > 0) {
      const trackerChannel = this.client.channels.cache.get(CONFIG.trackerChannelId) as TextChannel | undefined;
      for (const giveaway of removed) {
        const notifMsgId = giveaway.notificationMessageId;
        if (notifMsgId && trackerChannel) {
          const msg = await trackerChannel.messages.fetch(notifMsgId).catch(() => null);
          if (msg && msg.embeds.length > 0) {
            const updatedEmbed = EmbedBuilder.from(msg.embeds[0])
              .setColor(0xE74C3C)
              .setAuthor({
                name: 'Giveaway Ended',
                iconURL: msg.embeds[0].author?.iconURL || undefined,
              });
            await msg.edit({
              embeds: [updatedEmbed],
            }).catch(() => {});
          }
        }
      }
      await this.updatePresence();
    }
  }
  private async vrfsCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const sub = interaction.options.getSubcommand();
    if (sub === 'player') return this.vrfsPlayerCommand(interaction);
    if (sub === 'locker') return this.vrfsLockerCommand(interaction);
    if (sub === 'locker-search') return this.vrfsLockerSearchCommand(interaction);
    if (sub === 'locker-section') return this.vrfsLockerSectionCommand(interaction);
    if (sub === 'locker-item') return this.vrfsLockerItemCommand(interaction);
    if (sub === 'item') return this.vrfsItemCommand(interaction);
    if (sub === 'items') return this.vrfsItemsCommand(interaction);
    if (sub === 'item-id') return this.vrfsItemIdCommand(interaction);
    if (sub === 'random-item') return this.vrfsRandomItemCommand(interaction);
    if (sub === 'item-section') return this.vrfsItemSectionCommand(interaction);
    if (sub === 'marketplace') return this.vrfsMarketplaceCommand(interaction);
    if (sub === 'marketplace-search') return this.vrfsMarketplaceSearchCommand(interaction);
    if (sub === 'marketplace-creator') return this.vrfsMarketplaceCreatorCommand(interaction);
    if (sub === 'marketplace-top') return this.vrfsMarketplaceTopCommand(interaction);
    if (sub === 'marketplace-new') return this.vrfsMarketplaceNewCommand(interaction);
    if (sub === 'marketplace-stats') return this.vrfsMarketplaceStatsCommand(interaction);
    if (sub === 'marketplace-compare') return this.vrfsMarketplaceCompareCommand(interaction);
    if (sub === 'stats') return this.vrfsStatsCommand(interaction);
    if (sub === 'status') return this.vrfsStatusCommand(interaction);
    if (sub === 'refresh') return this.vrfsRefreshCommand(interaction);
    if (sub === 'help') return this.vrfsHelpCommand(interaction);
  }
  private async vrfsPlayerCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const id = interaction.options.getInteger('id', true);
    await deferReply(interaction, false);
    try {
      const [usernameResult, profileResult, outfitsResult] = await Promise.allSettled([
        vrfs.getUsername(id),
        vrfs.getProfile(id),
        vrfs.getOutfits(id),
      ]);
      const username = usernameResult.status === 'fulfilled'
        ? usernameResult.value.username
        : `Player ${id}`;
      const profile = profileResult.status === 'fulfilled'
        ? profileResult.value
        : null;
      const outfits = outfitsResult.status === 'fulfilled'
        ? outfitsResult.value
        : [];
      const embed = new EmbedBuilder()
        .setColor(0x5865F2)
        .setTitle(username)
        .setDescription(`ID \`${id}\``)
        .setThumbnail(productAvatar(id))
        .addFields(
          {
            name: 'Country',
            value: profile?.profileCountry || 'N/A',
            inline: true,
          },
          {
            name: 'Followers',
            value: productNumber(profile?.followersCount),
            inline: true,
          },
          {
            name: 'Public Outfits',
            value: productNumber(outfits.length),
            inline: true,
          },
        );
      const socials = [
        ['User Tag', profile?.userTag],
        ['TikTok', profile?.tiktokName],
        ['YouTube', profile?.youtubeName],
        ['Twitch', profile?.twitchName],
        ['Instagram', profile?.instagramName],
      ].filter(([, value]) => typeof value === 'string' && value.trim());
      if (socials.length) {
        embed.addFields(
          socials.map(([name, value]) => ({
            name,
            value: productTrim(value, 1024),
            inline: true,
          })),
        );
      }
      const latest = outfits[0];
      if (latest?.slots && Object.keys(latest.slots).length > 0) {
        const lines = Object.entries(latest.slots).map(([slot, sku]) =>
          `**${slot}**\n\`${sku || 'Empty'}\``,
        );
        embed.addFields({
          name: latest.id !== undefined ? `Latest Outfit #${latest.id}` : 'Latest Outfit',
          value: productTrim(lines.join('\n'), 1024),
          inline: false,
        });
      }
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve that player right now.')],
      });
      logger.warn('Player command failed', {
        error: formatError(error),
        id,
      });
    }
  }
  private async vrfsLockerCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const id = interaction.options.getInteger('id', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const player = await vrfs.getUsername(id);
      if (!catalog.length) {
        await interaction.editReply({
          embeds: [productSimpleErrorEmbed('Locker', 'The item collection is currently unavailable.')],
        });
        return;
      }
      await interaction.editReply({
        embeds: [
          new EmbedBuilder()
            .setColor(0x5865F2)
            .setTitle('Locker')
            .setDescription(`Checking **${productNumber(catalog.length)}** items for **${productTrim(player.username, 100)}**.\nID \`${id}\``),
        ],
      });
      const result = await seby.checkOwnershipBatched(
        id,
        catalog.map(item => getSku(item)),
        {
          batchSize: 250,
          minBatchSize: 5,
          maxBatchSize: 500,
          delayMs: 150,
        },
      );
      const owned = catalog.filter(item => result.results[getSku(item)] === true);
      const unknown = catalog.filter(item => result.results[getSku(item)] === 'unknown');
      const free = owned.filter(isItemFree).length;
      const paid = owned.length - free;
      const sections = new Map<string, number>();
      for (const item of owned) {
        const section = productSection(item);
        sections.set(section, (sections.get(section) || 0) + 1);
      }
      const completion = catalog.length > 0
        ? (owned.length / catalog.length) * 100
        : 0;
      const sectionText = Array.from(sections.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 12)
        .map(([section, count]) => `**${productTrim(section, 100)}** — ${productNumber(count)}`)
        .join('\n') || 'No owned items.';
      const summary = new EmbedBuilder()
        .setColor(unknown.length ? 0xF1C40F : 0x2ECC71)
        .setTitle('Locker')
        .setDescription(
          `**${productTrim(player.username, 100)}**\nID \`${id}\`\n\n**${productNumber(owned.length)} / ${productNumber(catalog.length)}** items owned\n${completion.toFixed(completion >= 10 ? 1 : 2)}% collection completion`,
        )
        .setThumbnail(productAvatar(id))
        .addFields(
          {
            name: 'Free',
            value: productNumber(free),
            inline: true,
          },
          {
            name: 'Paid',
            value: productNumber(paid),
            inline: true,
          },
          {
            name: 'Unconfirmed',
            value: productNumber(unknown.length),
            inline: true,
          },
          {
            name: 'Sections',
            value: productNumber(sections.size),
            inline: true,
          },
          {
            name: 'Collection by Section',
            value: productTrim(sectionText, 1024),
            inline: false,
          },
        );
      const token = productPaginationToken({
        type: 'locker',
        items: owned,
        pageSize: LOCKER_PAGE_SIZE,
        title: 'Owned Items',
        description: `ID \`${id}\` · ${productNumber(owned.length)} owned items`,
        id,
      });
      const session = productPaginationGet(token)!;
      const page = productLockerListEmbed(session, 0);
      await interaction.editReply({
        embeds: owned.length > 0 ? [summary, page.embed] : [summary],
        components: owned.length > 0 && page.totalPages > 1
          ? [productPaginationRow(token, 0, page.totalPages)]
          : [],
      });
    } catch (error) {
      logger.warn('Locker command failed', {
        error: formatError(error),
        id,
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not check that locker right now.')],
      });
    }
  }
  private async vrfsLockerSearchCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const id = interaction.options.getInteger('id', true);
    const query = interaction.options.getString('query', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const matches = productCatalogMatches(catalog, query).slice(0, 100);
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Locker Search',
              `No items matched \`${productTrim(query, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      const result = await seby.checkOwnershipBatched(
        id,
        matches.map(item => getSku(item)),
        {
          batchSize: Math.min(250, matches.length),
          minBatchSize: 1,
          maxBatchSize: 500,
          delayMs: 150,
        },
      );
      const owned = matches.filter(item => result.results[getSku(item)] === true);
      const notOwned = matches.filter(item => result.results[getSku(item)] === false);
      const unknown = matches.filter(item => result.results[getSku(item)] === 'unknown');
      const embed = new EmbedBuilder()
        .setColor(unknown.length ? 0xF1C40F : 0x2ECC71)
        .setTitle('Locker Search')
        .setDescription(`ID \`${id}\`\nSearch \`${productTrim(query, 100)}\``)
        .addFields(
          {
            name: 'Owned',
            value: `${productNumber(owned.length)} / ${productNumber(matches.length)}`,
            inline: true,
          },
          {
            name: 'Not Owned',
            value: productNumber(notOwned.length),
            inline: true,
          },
          {
            name: 'Unconfirmed',
            value: productNumber(unknown.length),
            inline: true,
          },
          {
            name: 'Owned Items',
            value: productTrim(
              owned
                .slice(0, 20)
                .map(item =>
                  `**${productTrim(getItemName(item), 80)}**\n\`${getSku(item)}\` · ${productSection(item)} · ${productPrice(item)}`,
                )
                .join('\n\n') || 'No matching owned items.',
              4096,
            ),
            inline: false,
          },
        );
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Locker search command failed', {
        error: formatError(error),
        id,
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not search that locker right now.')],
      });
    }
  }
  private async vrfsLockerSectionCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const id = interaction.options.getInteger('id', true);
    const section = interaction.options.getString('section', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const matches = catalog.filter(
        item => productNormalize(productSection(item)) === productNormalize(section),
      );
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Locker Section',
              `No items were found in section \`${productTrim(section, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      const result = await seby.checkOwnershipBatched(
        id,
        matches.map(item => getSku(item)),
        {
          batchSize: Math.min(250, matches.length),
          minBatchSize: 1,
          maxBatchSize: 500,
          delayMs: 150,
        },
      );
      const owned = matches.filter(item => result.results[getSku(item)] === true);
      const unknown = matches.filter(item => result.results[getSku(item)] === 'unknown');
      const embed = new EmbedBuilder()
        .setColor(unknown.length ? 0xF1C40F : 0x2ECC71)
        .setTitle('Locker Section')
        .setDescription(`ID \`${id}\`\nSection **${productTrim(section, 100)}**`)
        .addFields(
          {
            name: 'Owned',
            value: `${productNumber(owned.length)} / ${productNumber(matches.length)}`,
            inline: true,
          },
          {
            name: 'Unconfirmed',
            value: productNumber(unknown.length),
            inline: true,
          },
          {
            name: 'Owned Items',
            value: productTrim(
              owned
                .slice(0, 20)
                .map(item =>
                  `**${productTrim(getItemName(item), 80)}**\n\`${getSku(item)}\` · ${productPrice(item)}`,
                )
                .join('\n\n') || 'No owned items.',
              4096,
            ),
            inline: false,
          },
        );
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Locker section command failed', {
        error: formatError(error),
        id,
        section,
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not check that section right now.')],
      });
    }
  }
  private async vrfsLockerItemCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const id = interaction.options.getInteger('id', true);
    const query = interaction.options.getString('item', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const matches = productCatalogMatches(catalog, query).slice(0, 10);
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Item',
              `No items matched \`${productTrim(query, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      if (matches.length > 1) {
        const token = productPaginationToken({
          type: 'items',
          items: matches,
          pageSize: PRODUCT_PAGE_SIZE,
          title: 'Matching Items',
          description: `Multiple items matched \`${productTrim(query, 100)}\`.`,
        });
        const result = productCatalogListEmbed(
          'Matching Items',
          `Multiple items matched \`${productTrim(query, 100)}\`.`,
          matches,
          0,
        );
        await interaction.editReply({
          embeds: [result.embed],
          components: result.totalPages > 1
            ? [productPaginationRow(token, 0, result.totalPages)]
            : [],
        });
        return;
      }
      const item = matches[0];
      const result = await seby.checkOwnership(id, [getSku(item)]);
      const ownership = result.results?.[getSku(item)];
      const embed = productCatalogEmbed(item)
        .setColor(
          ownership === true
            ? 0x2ECC71
            : ownership === false
              ? 0xE74C3C
              : 0xF1C40F,
        )
        .addFields({
          name: 'Ownership',
          value:
            ownership === true
              ? `Owned by ID \`${id}\``
              : ownership === false
                ? `Not owned by ID \`${id}\``
                : 'Could not be confirmed.',
          inline: false,
        });
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Locker item command failed', {
        error: formatError(error),
        id,
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not check that item right now.')],
      });
    }
  }
  private async vrfsItemCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const query = interaction.options.getString('query', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const matches = productCatalogMatches(catalog, query);
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Item',
              `No items matched \`${productTrim(query, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      if (matches.length === 1) {
        await interaction.editReply({
          embeds: [productCatalogEmbed(matches[0])],
        });
        return;
      }
      const token = productPaginationToken({
        type: 'items',
        items: matches.slice(0, 100),
        pageSize: PRODUCT_PAGE_SIZE,
        title: 'Item Lookup',
        description: `Multiple items matched \`${productTrim(query, 100)}\`.`,
      });
      const result = productCatalogListEmbed(
        'Item Lookup',
        `Multiple items matched \`${productTrim(query, 100)}\`.`,
        matches.slice(0, 100),
        0,
      );
      await interaction.editReply({
        embeds: [result.embed],
        components: result.totalPages > 1
          ? [productPaginationRow(token, 0, result.totalPages)]
          : [],
      });
    } catch (error) {
      logger.warn('Item command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve that item right now.')],
      });
    }
  }
  private async vrfsItemsCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const query = interaction.options.getString('query', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const matches = productCatalogMatches(catalog, query).slice(0, 100);
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Item Search',
              `Nothing matched \`${productTrim(query, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      const token = productPaginationToken({
        type: 'items',
        items: matches,
        pageSize: PRODUCT_PAGE_SIZE,
        title: 'Item Search',
        description: `Search results for \`${productTrim(query, 100)}\`.`,
      });
      const result = productCatalogListEmbed(
        'Item Search',
        `Search results for \`${productTrim(query, 100)}\`.`,
        matches,
        0,
      );
      await interaction.editReply({
        embeds: [result.embed],
        components: result.totalPages > 1
          ? [productPaginationRow(token, 0, result.totalPages)]
          : [],
      });
    } catch (error) {
      logger.warn('Items command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not search the item collection right now.')],
      });
    }
  }
  private async vrfsItemIdCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const id = interaction.options.getInteger('id', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const item = catalog.find(entry => Number(entry.id) === id);
      if (!item) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Item',
              `No item with ID \`${id}\` was found.`,
            ),
          ],
        });
        return;
      }
      await interaction.editReply({
        embeds: [productCatalogEmbed(item)],
      });
    } catch (error) {
      logger.warn('Item ID command failed', {
        error: formatError(error),
        id,
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve that item right now.')],
      });
    }
  }
  private async vrfsRandomItemCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      if (!catalog.length) {
        await interaction.editReply({
          embeds: [productSimpleErrorEmbed('Random Item', 'No items are currently available.')],
        });
        return;
      }
      const item = catalog[Math.floor(Math.random() * catalog.length)];
      await interaction.editReply({
        embeds: [productCatalogEmbed(item)],
      });
    } catch (error) {
      logger.warn('Random item command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve a random item right now.')],
      });
    }
  }
  private async vrfsItemSectionCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const section = interaction.options.getString('section', true);
    await deferReply(interaction, false);
    try {
      const catalog = await seby.getItems();
      const matches = catalog.filter(
        item => productNormalize(productSection(item)) === productNormalize(section),
      );
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Catalog Section',
              `No items were found in section \`${productTrim(section, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      const token = productPaginationToken({
        type: 'items',
        items: matches,
        pageSize: PRODUCT_PAGE_SIZE,
        title: 'Item Section',
        description: `Section **${productTrim(section, 100)}** · ${productNumber(matches.length)} items`,
      });
      const result = productCatalogListEmbed(
        'Item Section',
        `Section **${productTrim(section, 100)}** · ${productNumber(matches.length)} items`,
        matches,
        0,
      );
      await interaction.editReply({
        embeds: [result.embed],
        components: result.totalPages > 1
          ? [productPaginationRow(token, 0, result.totalPages)]
          : [],
      });
    } catch (error) {
      logger.warn('Item section command failed', {
        error: formatError(error),
        section,
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve that section right now.')],
      });
    }
  }
  private async vrfsMarketplaceCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const query = interaction.options.getString('query', true);
    await deferReply(interaction, false);
    try {
      const marketplace = await vrfs.getMarketplace();
      const matches = productMarketplaceMatches(marketplace, query);
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Marketplace',
              `No marketplace item matched \`${productTrim(query, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      if (matches.length === 1) {
        await interaction.editReply({
          embeds: [productMarketplaceEmbed(matches[0])],
        });
        return;
      }
      const token = productPaginationToken({
        type: 'marketplace',
        items: matches.slice(0, 100),
        pageSize: PRODUCT_PAGE_SIZE,
        title: 'Marketplace Search',
        description: `Multiple results matched \`${productTrim(query, 100)}\`.`,
      });
      const result = productMarketplaceListEmbed(
        'Marketplace Search',
        `Multiple results matched \`${productTrim(query, 100)}\`.`,
        matches.slice(0, 100),
        0,
      );
      await interaction.editReply({
        embeds: [result.embed],
        components: result.totalPages > 1
          ? [productPaginationRow(token, 0, result.totalPages)]
          : [],
      });
    } catch (error) {
      logger.warn('Marketplace command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve the marketplace right now.')],
      });
    }
  }
  private async vrfsMarketplaceSearchCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const query = interaction.options.getString('query', true);
    await deferReply(interaction, false);
    try {
      const marketplace = await vrfs.getMarketplace();
      const matches = productMarketplaceMatches(marketplace, query).slice(0, 100);
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Marketplace Search',
              `Nothing matched \`${productTrim(query, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      const token = productPaginationToken({
        type: 'marketplace',
        items: matches,
        pageSize: PRODUCT_PAGE_SIZE,
        title: 'Marketplace Search',
        description: `Search results for \`${productTrim(query, 100)}\`.`,
      });
      const result = productMarketplaceListEmbed(
        'Marketplace Search',
        `Search results for \`${productTrim(query, 100)}\`.`,
        matches,
        0,
      );
      await interaction.editReply({
        embeds: [result.embed],
        components: result.totalPages > 1
          ? [productPaginationRow(token, 0, result.totalPages)]
          : [],
      });
    } catch (error) {
      logger.warn('Marketplace search command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not search the marketplace right now.')],
      });
    }
  }
  private async vrfsMarketplaceCreatorCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const query = productNormalize(interaction.options.getString('name', true));
    await deferReply(interaction, false);
    try {
      const marketplace = await vrfs.getMarketplace();
      const matches = marketplace.filter(item =>
        productNormalize(productMarketplaceCreator(item)).includes(query),
      );
      if (!matches.length) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Marketplace Creator',
              `No creator matched \`${productTrim(query, 100)}\`.`,
            ),
          ],
        });
        return;
      }
      const creator = productMarketplaceCreator(matches[0]);
      const creatorId = productMarketplaceCreatorId(matches[0]);
      const active = matches.filter(getMarketplaceActive).length;
      const owners = matches.reduce((sum, item) => sum + getMarketplaceOwners(item), 0);
      const types = new Map<string, number>();
      for (const item of matches) {
        const type = productMarketplaceType(item);
        types.set(type, (types.get(type) || 0) + 1);
      }
      const typeText = Array.from(types.entries())
        .sort((a, b) => b[1] - a[1])
        .map(([type, count]) => `**${type}** — ${productNumber(count)}`)
        .join('\n') || 'No type data.';
      const embed = new EmbedBuilder()
        .setColor(0x5865F2)
        .setTitle(productTrim(creator, 256))
        .setDescription(creatorId ? `ID \`${creatorId}\`` : 'Marketplace creator')
        .addFields(
          {
            name: 'Items',
            value: productNumber(matches.length),
            inline: true,
          },
          {
            name: 'Active',
            value: productNumber(active),
            inline: true,
          },
          {
            name: 'Total Owners',
            value: productNumber(owners),
            inline: true,
          },
          {
            name: 'By Type',
            value: productTrim(typeText, 1024),
            inline: false,
          },
          {
            name: 'Items',
            value: productTrim(
              matches
                .slice(0, 20)
                .map(item =>
                  `**${productTrim(item.title || item.name || getSku(item), 80)}**\n#${item.id} · ${productNumber(getMarketplaceOwners(item))} owners`,
                )
                .join('\n\n') || 'No marketplace items.',
              4096,
            ),
            inline: false,
          },
        );
      const image = productMarketplaceImage(matches[0]);
      if (image) embed.setImage(image);
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Marketplace creator command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve that creator right now.')],
      });
    }
  }
  private async vrfsMarketplaceTopCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const type = interaction.options.getString('type') || 'all';
    await deferReply(interaction, false);
    try {
      const marketplace = await vrfs.getMarketplace();
      const items = marketplace
        .filter(item => type === 'all' || String(item.category_id ?? item.category ?? '') === type)
        .slice()
        .sort((a, b) => getMarketplaceOwners(b) - getMarketplaceOwners(a))
        .slice(0, 10);
      const title = type === 'all'
        ? 'Most Owned Items'
        : `${productMarketplaceType(items[0] || ({ id: 0 } as VRFSMarketplaceItem))} Items`;
      const embed = new EmbedBuilder()
        .setColor(0xF1C40F)
        .setTitle(title)
        .setDescription(`Top ${productNumber(items.length)}`)
        .addFields({
          name: 'Ranking',
          value:
            items
              .map((item, index) =>
                `**${index + 1}. ${productTrim(item.title || item.name || getSku(item), 80)}**\n#${item.id} · ${productNumber(getMarketplaceOwners(item))} owners`,
              )
              .join('\n\n') || 'No items found.',
          inline: false,
        });
      const image = items[0] ? productMarketplaceImage(items[0]) : null;
      if (image) embed.setImage(image);
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Marketplace top command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve the marketplace ranking right now.')],
      });
    }
  }
  private async vrfsMarketplaceNewCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const limit = interaction.options.getInteger('limit') || 10;
    await deferReply(interaction, false);
    try {
      const marketplace = await vrfs.getMarketplace();
      const items = marketplace
        .slice()
        .sort((a, b) => Number(b.id) - Number(a.id))
        .slice(0, limit);
      const embed = new EmbedBuilder()
        .setColor(0x2ECC71)
        .setTitle('Latest Items')
        .setDescription(`Showing ${productNumber(items.length)} items.`)
        .addFields({
          name: 'Latest',
          value:
            items
              .map((item, index) =>
                `**${index + 1}. ${productTrim(item.title || item.name || getSku(item), 80)}**\n#${item.id} · ${productMarketplaceType(item)} · ${productNumber(getMarketplaceOwners(item))} owners`,
              )
              .join('\n\n') || 'No items found.',
          inline: false,
        });
      const image = items[0] ? productMarketplaceImage(items[0]) : null;
      if (image) embed.setImage(image);
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Marketplace new command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve the latest items right now.')],
      });
    }
  }
  private async vrfsMarketplaceStatsCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    await deferReply(interaction, false);
    try {
      const marketplace = await vrfs.getMarketplace();
      const active = marketplace.filter(getMarketplaceActive).length;
      const creators = new Set(
        marketplace
          .map(productMarketplaceCreatorId)
          .filter((value): value is string => Boolean(value)),
      ).size;
      const owners = marketplace.reduce((sum, item) => sum + getMarketplaceOwners(item), 0);
      const types = new Map<string, number>();
      for (const item of marketplace) {
        const type = productMarketplaceType(item);
        types.set(type, (types.get(type) || 0) + 1);
      }
      const typeText = Array.from(types.entries())
        .sort((a, b) => b[1] - a[1])
        .map(([type, count]) => `**${type}** — ${productNumber(count)}`)
        .join('\n') || 'No type data.';
      const embed = new EmbedBuilder()
        .setColor(0x5865F2)
        .setTitle('Marketplace Statistics')
        .setDescription('Current marketplace data.')
        .addFields(
          {
            name: 'Items',
            value: productNumber(marketplace.length),
            inline: true,
          },
          {
            name: 'Active',
            value: productNumber(active),
            inline: true,
          },
          {
            name: 'Inactive',
            value: productNumber(marketplace.length - active),
            inline: true,
          },
          {
            name: 'Creators',
            value: productNumber(creators),
            inline: true,
          },
          {
            name: 'Total Owners',
            value: productNumber(owners),
            inline: true,
          },
          {
            name: 'By Type',
            value: productTrim(typeText, 1024),
            inline: false,
          },
        );
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Marketplace stats command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve marketplace statistics right now.')],
      });
    }
  }
  private async vrfsMarketplaceCompareCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    const id1 = interaction.options.getInteger('id1', true);
    const id2 = interaction.options.getInteger('id2', true);
    await deferReply(interaction, false);
    try {
      const marketplace = await vrfs.getMarketplace();
      const first = marketplace.find(item => Number(item.id) === id1);
      const second = marketplace.find(item => Number(item.id) === id2);
      if (!first || !second) {
        await interaction.editReply({
          embeds: [
            productSimpleErrorEmbed(
              'Comparison',
              `Could not find ${!first ? `marketplace item #${id1}` : `marketplace item #${id2}`}.`,
            ),
          ],
        });
        return;
      }
      const firstOwners = getMarketplaceOwners(first);
      const secondOwners = getMarketplaceOwners(second);
      const difference = firstOwners === secondOwners
        ? 'Both items have the same owner count.'
        : firstOwners > secondOwners
          ? `**${productTrim(first.title || first.name || getSku(first), 120)}** has **${productNumber(firstOwners - secondOwners)}** more owners.`
          : `**${productTrim(second.title || second.name || getSku(second), 120)}** has **${productNumber(secondOwners - firstOwners)}** more owners.`;
      const embed = new EmbedBuilder()
        .setColor(0x5865F2)
        .setTitle('Marketplace Comparison')
        .setDescription(`#${first.id} versus #${second.id}`)
        .addFields(
          {
            name: `${productTrim(first.title || first.name || getSku(first), 200)} · #${first.id}`,
            value: [
              `Type: ${productMarketplaceType(first)}`,
              `SKU: \`${getSku(first)}\``,
              `Owners: ${productNumber(firstOwners)}`,
              `Price: ${productMarketplacePrice(first)}`,
              `Creator: ${productMarketplaceCreator(first)}`,
              `Status: ${getMarketplaceActive(first) ? 'Active' : 'Inactive'}`,
            ].join('\n'),
            inline: true,
          },
          {
            name: `${productTrim(second.title || second.name || getSku(second), 200)} · #${second.id}`,
            value: [
              `Type: ${productMarketplaceType(second)}`,
              `SKU: \`${getSku(second)}\``,
              `Owners: ${productNumber(secondOwners)}`,
              `Price: ${productMarketplacePrice(second)}`,
              `Creator: ${productMarketplaceCreator(second)}`,
              `Status: ${getMarketplaceActive(second) ? 'Active' : 'Inactive'}`,
            ].join('\n'),
            inline: true,
          },
          {
            name: 'Owner Count',
            value: difference,
            inline: false,
          },
        );
      const firstImage = productMarketplaceImage(first);
      const secondImage = productMarketplaceImage(second);
      if (firstImage) embed.setImage(firstImage);
      if (secondImage) embed.setThumbnail(secondImage);
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Marketplace compare command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not compare those items right now.')],
      });
    }
  }
  private async vrfsStatsCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    await deferReply(interaction, false);
    try {
      const [items, marketplace] = await Promise.all([
        seby.getItems(),
        vrfs.getMarketplace(),
      ]);
      const free = items.filter(isItemFree).length;
      const marketplaceSkus = new Set(
        marketplace
          .map(item => productNormalize(getSku(item)))
          .filter(Boolean),
      );
      const matches = items.filter(
        item => marketplaceSkus.has(productNormalize(getSku(item))),
      ).length;
      const creators = new Set(
        marketplace
          .map(productMarketplaceCreatorId)
          .filter((value): value is string => Boolean(value)),
      ).size;
      const embed = new EmbedBuilder()
        .setColor(0x5865F2)
        .setTitle('Statistics')
        .setDescription('Current item and marketplace data.')
        .addFields(
          {
            name: 'Items',
            value: productNumber(items.length),
            inline: true,
          },
          {
            name: 'Free',
            value: productNumber(free),
            inline: true,
          },
          {
            name: 'Paid',
            value: productNumber(items.length - free),
            inline: true,
          },
          {
            name: 'Marketplace',
            value: productNumber(marketplace.length),
            inline: true,
          },
          {
            name: 'Marketplace Matches',
            value: productNumber(matches),
            inline: true,
          },
          {
            name: 'Creators',
            value: productNumber(creators),
            inline: true,
          },
        );
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Stats command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve statistics right now.')],
      });
    }
  }
  private async vrfsStatusCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    await deferReply(interaction, false);
    try {
      const [playerHealth, lockerHealth] = await Promise.all([
        vrfs.health(),
        seby.health(),
      ]);
      const embed = new EmbedBuilder()
        .setColor(playerHealth.ok && lockerHealth.ok ? 0x2ECC71 : 0xF1C40F)
        .setTitle('Service Status')
        .setDescription('Current service availability.')
        .addFields(
          {
            name: 'Player Data',
            value: productServiceValue(playerHealth.ok),
            inline: true,
          },
          {
            name: 'Marketplace',
            value: productServiceValue(playerHealth.ok),
            inline: true,
          },
          {
            name: 'Locker',
            value: productServiceValue(lockerHealth.ok),
            inline: true,
          },
          {
            name: 'Player Response',
            value: `${productNumber(playerHealth.latencyMs)} ms`,
            inline: true,
          },
          {
            name: 'Locker Response',
            value: `${productNumber(lockerHealth.latencyMs)} ms`,
            inline: true,
          },
        );
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Service status command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not retrieve service status right now.')],
      });
    }
  }
  private async vrfsRefreshCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    if (!isOwner(interaction.user.id)) {
      await interaction.reply({
        content: 'You do not have permission to use this command.',
        ephemeral: true,
      });
      return;
    }
    await deferReply(interaction, true);
    try {
      const [items, marketplace] = await Promise.all([
        seby.getItems(),
        vrfs.getMarketplace(),
      ]);
      const embed = new EmbedBuilder()
        .setColor(0x2ECC71)
        .setTitle('Data Refreshed')
        .setDescription([
          `Items: **${productNumber(items.length)}**`,
          `Marketplace: **${productNumber(marketplace.length)}**`,
          '',
          'The latest data is ready.',
        ].join('\n'));
      await interaction.editReply({
        embeds: [embed],
      });
    } catch (error) {
      logger.warn('Refresh command failed', {
        error: formatError(error),
      });
      await interaction.editReply({
        embeds: [productErrorEmbed('We could not refresh the data right now.')],
      });
    }
  }
  private async vrfsHelpCommand(interaction: ChatInputCommandInteraction<CacheType>): Promise<void> {
    await deferReply(interaction, false);
    const embed = new EmbedBuilder()
      .setColor(0x5865F2)
      .setTitle('Player and Item Tools')
      .setDescription('Browse players, lockers, items and the marketplace.')
      .addFields(
        {
          name: 'Player',
          value: '`/vrfs player id`',
          inline: false,
        },
        {
          name: 'Locker',
          value: '`/vrfs locker id`\n`/vrfs locker-search id query`\n`/vrfs locker-section id section`\n`/vrfs locker-item id item`',
          inline: false,
        },
        {
          name: 'Items',
          value: '`/vrfs item query`\n`/vrfs items query`\n`/vrfs item-id id`\n`/vrfs random-item`\n`/vrfs item-section section`',
          inline: false,
        },
        {
          name: 'Marketplace',
          value: '`/vrfs marketplace query`\n`/vrfs marketplace-search query`\n`/vrfs marketplace-creator name`\n`/vrfs marketplace-top`\n`/vrfs marketplace-new`\n`/vrfs marketplace-stats`\n`/vrfs marketplace-compare id1 id2`',
          inline: false,
        },
        {
          name: 'Other',
          value: '`/vrfs stats`\n`/vrfs status`\n`/vrfs refresh`',
          inline: false,
        },
      );
    await interaction.editReply({
      embeds: [embed],
    });
  }
  private async handleProductPagination(interaction: ButtonInteraction): Promise<void> {
    const parts = interaction.customId.split(':');
    if (parts.length !== 4) return;
    const token = parts[2];
    const direction = parts[3];
    const session = productPaginationGet(token);
    if (!session) {
      await interaction.reply({
        embeds: [
          productSimpleErrorEmbed(
            'Results Expired',
            'These results have expired. Run the command again.',
          ),
        ],
        ephemeral: true,
      });
      return;
    }
    if (direction === 'current') return;
    const totalPages = Math.max(1, Math.ceil(session.items.length / session.pageSize));
    const nextPage = direction === 'next'
      ? session.page + 1
      : session.page - 1;
    if (nextPage < 0 || nextPage >= totalPages) return;
    session.page = nextPage;
    try {
      if (session.type === 'items') {
        const result = productCatalogListEmbed(
          session.title,
          session.description || 'Items',
          session.items as VRFSItem[],
          nextPage,
          session.pageSize,
        );
        await interaction.update({
          embeds: [result.embed],
          components: result.totalPages > 1
            ? [productPaginationRow(token, result.page, result.totalPages)]
            : [],
        });
        return;
      }
      if (session.type === 'marketplace') {
        const result = productMarketplaceListEmbed(
          session.title,
          session.description || 'Marketplace',
          session.items as VRFSMarketplaceItem[],
          nextPage,
          session.pageSize,
        );
        await interaction.update({
          embeds: [result.embed],
          components: result.totalPages > 1
            ? [productPaginationRow(token, result.page, result.totalPages)]
            : [],
        });
        return;
      }
      const result = productLockerListEmbed(session, nextPage);
      await interaction.update({
        embeds: [result.embed],
        components: result.totalPages > 1
          ? [productPaginationRow(token, result.page, result.totalPages)]
          : [],
      });
    } catch (error) {
      logger.warn('Product pagination failed', {
        error: formatError(error),
      });
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({
          embeds: [productErrorEmbed('That page could not be loaded.')],
          ephemeral: true,
        }).catch(() => {});
      }
    }
  }
  private async registerCommands(): Promise<void> {
    if (this.commandsRegistered) return;
    const commandData = [
      new SlashCommandBuilder()
        .setName('stats')
        .setDescription('Tracker statistics'),
      new SlashCommandBuilder()
        .setName('active')
        .setDescription('Active giveaways'),
      new SlashCommandBuilder()
        .setName('recent')
        .setDescription('Recently detected'),
      new SlashCommandBuilder()
        .setName('setchannel')
        .setDescription('Set notification channel')
        .addChannelOption(opt =>
          opt
            .setName('channel')
            .setDescription('Target channel')
            .setRequired(true),
        )
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName('reset')
        .setDescription('Wipe database')
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName('status')
        .setDescription('Check if running')
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName('metrics')
        .setDescription('Performance metrics')
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName('purge')
        .setDescription('Delete bot messages')
        .addIntegerOption(opt =>
          opt
            .setName('amount')
            .setDescription('How many')
            .setMinValue(1)
            .setMaxValue(100),
        )
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName('help')
        .setDescription('List commands'),
      new SlashCommandBuilder()
        .setName('revoke')
        .setDescription('Revoke premium access from a user')
        .addUserOption(opt =>
          opt
            .setName('user')
            .setDescription('The user to revoke premium from')
            .setRequired(true),
        )
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName('giveawaytrack')
        .setDescription('Manage giveaway tracking')
        .addSubcommand(sub =>
          sub
            .setName('add')
            .setDescription('Add an item to track')
            .addStringOption(opt =>
              opt
                .setName('item')
                .setDescription('Item to track')
                .setRequired(true)
                .setMinLength(2)
                .setMaxLength(50),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('remove')
            .setDescription('Remove an item from tracking')
            .addStringOption(opt =>
              opt
                .setName('item')
                .setDescription('Item to remove')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('list')
            .setDescription('Show your tracked items'),
        )
        .addSubcommand(sub =>
          sub
            .setName('clear')
            .setDescription('Clear all tracked items'),
        ),
      new SlashCommandBuilder()
        .setName('eventtrack')
        .setDescription('Manage event tracking')
        .addSubcommand(sub =>
          sub
            .setName('add')
            .setDescription('Add an event filter')
            .addStringOption(opt =>
              opt
                .setName('filter')
                .setDescription('Event filter')
                .setRequired(true)
                .setMinLength(2)
                .setMaxLength(30),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('remove')
            .setDescription('Remove an event filter')
            .addStringOption(opt =>
              opt
                .setName('filter')
                .setDescription('Filter to remove')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('list')
            .setDescription('Show event filters'),
        )
        .addSubcommand(sub =>
          sub
            .setName('clear')
            .setDescription('Clear all event filters'),
        ),
      new SlashCommandBuilder()
        .setName('licenseadmin')
        .setDescription('Send admin license management panel')
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName('vrfs')
        .setDescription('Player, locker, item and marketplace tools')
        .addSubcommand(sub =>
          sub
            .setName('player')
            .setDescription('View a player profile')
            .addIntegerOption(opt =>
              opt
                .setName('id')
                .setDescription('Player ID')
                .setRequired(true)
                .setMinValue(1),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('locker')
            .setDescription('View a complete locker')
            .addIntegerOption(opt =>
              opt
                .setName('id')
                .setDescription('Player ID')
                .setRequired(true)
                .setMinValue(1),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('locker-search')
            .setDescription('Search a locker')
            .addIntegerOption(opt =>
              opt
                .setName('id')
                .setDescription('Player ID')
                .setRequired(true)
                .setMinValue(1),
            )
            .addStringOption(opt =>
              opt
                .setName('query')
                .setDescription('Item name or SKU')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('locker-section')
            .setDescription('View a locker section')
            .addIntegerOption(opt =>
              opt
                .setName('id')
                .setDescription('Player ID')
                .setRequired(true)
                .setMinValue(1),
            )
            .addStringOption(opt =>
              opt
                .setName('section')
                .setDescription('Section name')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('locker-item')
            .setDescription('Check one owned item')
            .addIntegerOption(opt =>
              opt
                .setName('id')
                .setDescription('Player ID')
                .setRequired(true)
                .setMinValue(1),
            )
            .addStringOption(opt =>
              opt
                .setName('item')
                .setDescription('Item name or SKU')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('item')
            .setDescription('Look up an item')
            .addStringOption(opt =>
              opt
                .setName('query')
                .setDescription('Item ID, name or SKU')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('items')
            .setDescription('Search items')
            .addStringOption(opt =>
              opt
                .setName('query')
                .setDescription('Item name, SKU or section')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('item-id')
            .setDescription('Look up an item by ID')
            .addIntegerOption(opt =>
              opt
                .setName('id')
                .setDescription('Item ID')
                .setRequired(true)
                .setMinValue(1),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('random-item')
            .setDescription('Show a random item'),
        )
        .addSubcommand(sub =>
          sub
            .setName('item-section')
            .setDescription('Browse an item section')
            .addStringOption(opt =>
              opt
                .setName('section')
                .setDescription('Section name')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('marketplace')
            .setDescription('Look up a marketplace item')
            .addStringOption(opt =>
              opt
                .setName('query')
                .setDescription('Marketplace ID, name or SKU')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('marketplace-search')
            .setDescription('Search the marketplace')
            .addStringOption(opt =>
              opt
                .setName('query')
                .setDescription('Search term')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('marketplace-creator')
            .setDescription('View a marketplace creator')
            .addStringOption(opt =>
              opt
                .setName('name')
                .setDescription('Creator name')
                .setRequired(true),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('marketplace-top')
            .setDescription('View the most-owned marketplace items')
            .addStringOption(opt =>
              opt
                .setName('type')
                .setDescription('Marketplace type')
                .addChoices(
                  { name: 'All', value: 'all' },
                  { name: 'Boots', value: '1' },
                  { name: 'Glasses', value: '2' },
                  { name: 'Gloves', value: '3' },
                  { name: 'Hat', value: '4' },
                  { name: 'Mask', value: '5' },
                  { name: 'Scarf', value: '6' },
                  { name: 'Other', value: '7' },
                ),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('marketplace-new')
            .setDescription('View the newest marketplace items')
            .addIntegerOption(opt =>
              opt
                .setName('limit')
                .setDescription('Number of items')
                .setMinValue(1)
                .setMaxValue(25),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('marketplace-stats')
            .setDescription('View marketplace statistics'),
        )
        .addSubcommand(sub =>
          sub
            .setName('marketplace-compare')
            .setDescription('Compare two marketplace items')
            .addIntegerOption(opt =>
              opt
                .setName('id1')
                .setDescription('First marketplace ID')
                .setRequired(true)
                .setMinValue(1),
            )
            .addIntegerOption(opt =>
              opt
                .setName('id2')
                .setDescription('Second marketplace ID')
                .setRequired(true)
                .setMinValue(1),
            ),
        )
        .addSubcommand(sub =>
          sub
            .setName('stats')
            .setDescription('View statistics'),
        )
        .addSubcommand(sub =>
          sub
            .setName('status')
            .setDescription('View service status'),
        )
        .addSubcommand(sub =>
          sub
            .setName('refresh')
            .setDescription('Refresh current data'),
        )
        .addSubcommand(sub =>
          sub
            .setName('help')
            .setDescription('View item and player commands'),
        ),
    ];
    const rest = new REST({ version: '10' }).setToken(this.botToken);
    try {
      await rest.put(
        Routes.applicationCommands(this.client.user!.id),
        {
          body: commandData.map(cmd => cmd.toJSON()),
        },
      );
      this.commandsRegistered = true;
      logger.info('Commands registered', {
        component: 'BotManager',
      });
    } catch (err) {
      logger.error('Command registration failed', {
        error: formatError(err),
      });
    }
  }
}
