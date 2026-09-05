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
  ContainerBuilder,
  TextDisplayBuilder,
  SectionBuilder,
  ThumbnailBuilder,
  MediaGalleryBuilder,
  MessageFlags,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
} from "discord.js";
import { CONFIG } from "./config.js";
import { logger } from "./logger.js";
import { formatTimestamp, truncate, formatError } from "./utils.js";
import { GiveawayData } from "./types.js";
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
} from "./database.js";
import { KeyPanel } from "./license/keyPanel.js";
import { PremiumPanel } from "./premium/premiumPanel.js";
import { AdminPanel } from "./premium/adminPanel.js";
import {
  isPremium,
  requirePremium,
  setClient,
  assignPremiumRole,
  addPremiumUser,
  removePremiumUser as removePremiumUserService,
  checkPremium,
  clearPremiumCache,
} from "./license/licenseMiddleware.js";
// [VRFS ARCHIVED] import {
// [VRFS ARCHIVED]   getUsername,
// [VRFS ARCHIVED]   getProfile,
// [VRFS ARCHIVED]   getOutfits,
// [VRFS ARCHIVED]   getPlayer,
// [VRFS ARCHIVED]   getMarketplace,
// [VRFS ARCHIVED]   getMarketplaceItem,
// [VRFS ARCHIVED]   searchMarketplace,
// [VRFS ARCHIVED]   getCatalog,
// [VRFS ARCHIVED]   getCatalogItem,
// [VRFS ARCHIVED]   searchCatalog,
// [VRFS ARCHIVED]   checkOwnership,
// [VRFS ARCHIVED]   getSku,
// [VRFS ARCHIVED]   getItemName,
// [VRFS ARCHIVED]   getSection,
// [VRFS ARCHIVED]   isItemFree,
// [VRFS ARCHIVED]   getCreditsForItem,
// [VRFS ARCHIVED]   getMarketplaceActive,
// [VRFS ARCHIVED]   getMarketplaceOwners,
// [VRFS ARCHIVED]   getMarketplaceCreatorName,
// [VRFS ARCHIVED]   getMarketplaceCreatorId,
// [VRFS ARCHIVED]   type VRFSItem,
// [VRFS ARCHIVED]   type VRFSMarketplaceItem,
// [VRFS ARCHIVED]   type VRFSProfile,
// [VRFS ARCHIVED]   type VRFSOutfit,
// [VRFS ARCHIVED]   type OwnershipCheckResult,
// [VRFS ARCHIVED] } from "./middleware/api/vrfs.js";
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
  private activeWorkers = 0;
  private readonly maxWorkers = 4;
  private dedupMap = new Map<string, number>();
  private dedupSweepInterval: NodeJS.Timeout | null = null;
  private bot: Client;
  private metrics: MetricsCollector;

  constructor(bot: Client, metrics: MetricsCollector) {
    this.bot = bot;
    this.metrics = metrics;
    this.dedupSweepInterval = setInterval(() => this.sweepDedup(), DEDUP_SWEEP_INTERVAL_MS);
    this.dedupSweepInterval.unref?.();
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
      logger.debug("Notification dedup cache swept", {
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
    this.queue.length = 0;
  }

  enqueue(data: GiveawayData, inviteUrl: string): void {
    const existing = this.dedupMap.get(data.messageId);
    if (existing !== undefined && Date.now() - existing < DEDUP_TTL_MS) {
      logger.debug("Notification duplicate prevented", { messageId: data.messageId });
      return;
    }

    this.dedupMap.set(data.messageId, Date.now());
    (data as any).cachedInviteUrl = inviteUrl;
    this.queue.push({ data, attempt: 1, maxRetries: 3, messageId: data.messageId });
    this.drain();
  }

  private drain(): void {
    while (this.activeWorkers < this.maxWorkers && this.queue.length > 0) {
      const job = this.queue.shift()!;
      this.activeWorkers++;
      void this.runJob(job).finally(() => {
        this.activeWorkers--;
        this.drain();
      });
    }
  }

  private async runJob(job: NotificationJob): Promise<void> {
    try {
      await this.sendWithRetry(job);
    } catch (err) {
      logger.error("Notification failed after retries", {
        messageId: job.messageId,
        error: formatError(err),
      });
      try {
        await updateNotificationStatus?.(job.messageId, job.data.channelId, {
          notificationStatus: "failed",
          notificationError: formatError(err),
        });
      } catch {}
      this.metrics.recordNotification(false, 0);
    }
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
          await new Promise((resolve) => setTimeout(resolve, wait));
        }
      }
    }
    throw lastError;
  }

  private async sendOne(job: NotificationJob): Promise<void> {
    const channel = this.bot.channels.cache.get(CONFIG.trackerChannelId) as TextChannel | undefined;
    if (!channel) throw new Error("Tracker channel not found");

    const data = job.data;
    const guild = this.bot.guilds.cache.get(data.guildId);
    const guildName = guild?.name || data.guildName || "Unknown";
    const guildIcon = (data as any).guildIcon || guild?.iconURL({ size: 512 }) || null;
    const guildBanner = (data as any).guildBanner || guild?.bannerURL({ size: 1024 }) || null;
    const memberCount = ((data as any).memberCount || guild?.memberCount) ?? null;
    const inviteUrl = (data as any).cachedInviteUrl || data.inviteUrl || "No invite available";
    const endsAt = data.endsAt || Date.now() + 3600000;
    const endTimestamp = Math.floor(endsAt / 1000);
    const winnerCount = extractWinnerCount(data.prize);
    const pingMention = process.env.PING_ROLE_ID ? `<@&${process.env.PING_ROLE_ID}>` : "@everyone";

    const container = buildGiveawayNotificationContainer(
      {
        ...data,
        guildName,
        guildIcon,
        guildBanner,
        memberCount,
        inviteUrl,
      },
      "active"
    );
    if (pingMention) addV2Text(container, pingMention);

    const start = Date.now();
    const sentMessage = await channel.send({
      components: [container],
      flags: MessageFlags.IsComponentsV2,
      allowedMentions: process.env.PING_ROLE_ID
        ? { roles: [process.env.PING_ROLE_ID] }
        : { parse: ["everyone"] },
    });
    this.metrics.recordNotification(true, Date.now() - start);
    await setNotificationMessageId(data.messageId, data.channelId, sentMessage.id);
    try {
      await updateNotificationStatus?.(data.messageId, data.channelId, {
        notificationStatus: "sent",
        notificationSentAt: Date.now(),
        notificationMessageId: sentMessage.id,
      });
    } catch {}
  }
}
function buildGiveawayNotificationContainer(
  data: GiveawayData & Record<string, any>,
  status: "active" | "ended"
): ContainerBuilder {
  const guildName = data.guildName || "Unknown";
  const guildIcon = data.guildIcon || null;
  const guildBanner = data.guildBanner || null;
  const memberCount = data.memberCount ?? null;
  const inviteUrl = data.inviteUrl || data.cachedInviteUrl || "No invite available";
  const endsAt = data.endsAt || Date.now() + 3600000;
  const endTimestamp = Math.floor(endsAt / 1000);
  const winnerCount = extractWinnerCount(data.prize || "");
  const messageUrl = `https://discord.com/channels/${data.guildId}/${data.channelId}/${data.messageId}`;

  const container = new ContainerBuilder()
    .setAccentColor(status === "ended" ? 0xe74c3c : 0x5865f2);

  const heading = new TextDisplayBuilder().setContent(
    `-# ${status === "ended" ? "Giveaway Ended" : "New Giveaway"}`
  );
  const title = new TextDisplayBuilder().setContent(
    `# ${truncate(data.prize || "Unknown Prize", 256)}`
  );
  const details = new TextDisplayBuilder().setContent(
    [
      ...(status === "ended" ? ["### Status", "**Status:** Ended", ""] : []),
      "### Details",
      `**Server:** ${guildName}`,
      `**Winners:** ${winnerCount}`,
    ].join("\n")
  );

  // Components V2 has no embed thumbnail, so use a Section accessory.
  // This keeps the server icon small and beside the giveaway details instead
  // of rendering it as a huge full-width Media Gallery image.
  if (guildIcon) {
    const section = new SectionBuilder()
      .addTextDisplayComponents(heading, title, details)
      .setThumbnailAccessory(
        new ThumbnailBuilder()
          .setURL(guildIcon)
          .setDescription(`${guildName} icon`)
      );
    container.addSectionComponents(section);
  } else {
    container.addTextDisplayComponents(heading, title, details);
  }

  addV2Text(
    container,
    [
      "### Time",
      `**Ends:** <t:${endTimestamp}:F>`,
      `**Countdown:** <t:${endTimestamp}:R>`,
      "",
      "### Links",
      `**Invite:** ${inviteUrl}`,
      memberCount ? `**Members:** ${memberCount.toLocaleString()}` : "",
    ].filter(Boolean).join("\n")
  );

  // Keep the original giveaway banner as a full-width V2 media gallery.
  // Unlike the server icon, the old embed image was intentionally large.
  if (guildBanner) {
    container.addMediaGalleryComponents(
      new MediaGalleryBuilder().addItems({
        media: { url: guildBanner },
        description: `${guildName} giveaway banner`,
      })
    );
  }

  const row = new ActionRowBuilder<ButtonBuilder>();
  if (inviteUrl.startsWith("http") && status !== "ended") {
    row.addComponents(
      new ButtonBuilder()
        .setLabel("Join Server")
        .setStyle(ButtonStyle.Link)
        .setURL(inviteUrl)
    );
  }
  row.addComponents(
    new ButtonBuilder()
      .setLabel("Message")
      .setStyle(ButtonStyle.Link)
      .setURL(messageUrl)
  );
  container.addActionRowComponents(row);

  return container;
}

function extractWinnerCount(prize: string): string {
  const match = prize.match(/(\d+)\s*[xX×]/);
  if (match) return match[1];
  if (/\b(?:one|1)\s*(?:winner|win|giveaway)/i.test(prize)) return "1";
  const m = prize.match(/(\d+)\s*(?:winners?)/i);
  if (m) return m[1];
  return "1";
}
async function deferReply(
  interaction: ChatInputCommandInteraction<CacheType>,
  ephemeral = true
) {
  if (!interaction.deferred && !interaction.replied) {
    await interaction.deferReply({ flags: ephemeral
      ? MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral
      : MessageFlags.IsComponentsV2,
    });
  }
}

function addV2Text(container: ContainerBuilder, text: string): void {
  if (!text) return;
  const limit = 3800;
  for (let i = 0; i < text.length; i += limit) {
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(text.slice(i, i + limit))
    );
  }
}

function createV2Container(
  title?: string,
  body?: string,
  accentColor = 0x5865f2
): ContainerBuilder {
  const container = new ContainerBuilder().setAccentColor(accentColor);
  if (title) {
    addV2Text(container, `# ${truncate(title, 256)}`);
  }
  if (body) {
    addV2Text(container, body);
  }
  return container;
}

function embedToV2Container(embed: EmbedBuilder): ContainerBuilder {
  const data = embed.toJSON();
  const accent = typeof data.color === "number" ? data.color : 0x5865f2;
  const container = new ContainerBuilder().setAccentColor(accent);

  if (data.author?.name) {
    addV2Text(container, `-# ${truncate(data.author.name, 256)}`);
  }
  if (data.title) {
    addV2Text(container, `# ${truncate(data.title, 256)}`);
  }
  if (data.description) {
    addV2Text(container, data.description);
  }
  for (const field of data.fields ?? []) {
    addV2Text(container, `**${truncate(field.name, 256)}**\n${field.value}`);
  }
  if (data.thumbnail?.url || data.image?.url) {
    const url = data.image?.url ?? data.thumbnail?.url;
    if (url) {
      container.addMediaGalleryComponents(
        new MediaGalleryBuilder().addItems({
          media: { url },
          description: data.title ? truncate(data.title, 200) : "Image",
        })
      );
    }
  }
  if (data.footer?.text) {
    addV2Text(container, `-# ${truncate(data.footer.text, 2048)}`);
  }
  return container;
}

function v2ReplyPayload(
  container: ContainerBuilder,
  ephemeral = false
) {
  return {
    components: [container],
    flags: ephemeral
      ? MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral
      : MessageFlags.IsComponentsV2,
  };
}

type V2Payload = {
  components: ContainerBuilder[];
  flags: MessageFlags.IsComponentsV2;
};

function v2EditPayload(container: ContainerBuilder): V2Payload {
  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

async function replyV2Text(
  interaction: ChatInputCommandInteraction<CacheType>,
  title: string,
  body: string,
  ephemeral = true,
  accentColor = 0x5865f2
) {
  return interaction.reply(
    v2ReplyPayload(createV2Container(title, body, accentColor), ephemeral)
  );
}

async function editV2Text(
  interaction: ChatInputCommandInteraction<CacheType>,
  title: string,
  body: string,
  accentColor = 0x5865f2
) {
  return interaction.editReply(
    v2EditPayload(createV2Container(title, body, accentColor))
  );
}

async function editV2Embed(
  interaction: ChatInputCommandInteraction<CacheType>,
  embed: EmbedBuilder,
  buttons?: ActionRowBuilder<ButtonBuilder>
) {
  const container = embedToV2Container(embed);
  if (buttons) container.addActionRowComponents(buttons);
  return interaction.editReply(v2EditPayload(container));
}

interface GiveawayPageState {
  id: string;
  userId: string;
  mode: "active" | "recent";
  query: string;
  page: number;
  createdAt: number;
}

function giveawayPageId(): string {
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 9)}`;
}
function isAdmin(userId: string): boolean {
  return CONFIG.adminUserIds.includes(userId);
}
function isOwner(userId: string): boolean {
  return userId === process.env.OWNER_ID;
}
async function requireAdmin(
  interaction: ChatInputCommandInteraction<CacheType>
): Promise<boolean> {
  if (!isAdmin(interaction.user.id)) {
    await replyV2Text(interaction, "Access denied", "You do not have permission to use this command.");
    return false;
  }
  return true;
}
async function requireOwner(
  interaction: ChatInputCommandInteraction<CacheType>
): Promise<boolean> {
  if (!isOwner(interaction.user.id)) {
    await replyV2Text(interaction, "Access denied", "You do not have permission to use this command.");
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
async function getUserNotificationSettings(
  userId: string
): Promise<UserNotificationSettings> {
  const cached = notificationSettingsCache.get(userId);
  if (cached) return cached;
  const items = await getItems(userId);
  const settings = getDefaultSettings();
  if (items.includes("notif:giveaways:off")) settings.giveaways = false;
  if (items.includes("notif:scrims:off")) settings.scrims = false;
  if (items.includes("notif:events:off")) settings.events = false;
  notificationSettingsCache.set(userId, settings);
  return settings;
}
async function updateUserNotificationSetting(
  userId: string,
  type: "giveaways" | "scrims" | "events",
  enabled: boolean
): Promise<void> {
  const settings = await getUserNotificationSettings(userId);
  settings[type] = enabled;
  notificationSettingsCache.set(userId, settings);
  const keyOff = `notif:${type}:off`;
  if (enabled) {
    await removeItem(userId, keyOff);
  } else {
    await addItem(userId, keyOff);
  }
}
function safeUrl(value: unknown): string | null {
  if (typeof value !== "string" || !value.trim()) return null;
  try {
    const parsed = new URL(value.trim());
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") return null;
    return parsed.toString();
  } catch {
    return null;
  }
}
// [VRFS ARCHIVED] function formatNumber(value: unknown): string {
// [VRFS ARCHIVED]   const n = Number(value ?? 0);
// [VRFS ARCHIVED]   return Number.isFinite(n) ? n.toLocaleString() : "0";
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function formatCredits(value: unknown): string {
// [VRFS ARCHIVED]   const n = Number(value);
// [VRFS ARCHIVED]   if (!Number.isFinite(n)) return "Unknown";
// [VRFS ARCHIVED]   return `${n.toLocaleString()} Credits`;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function formatItemPrice(item: VRFSItem): string {
// [VRFS ARCHIVED]   if (isItemFree(item)) return "Free";
// [VRFS ARCHIVED]   const credits = getCreditsForItem(item);
// [VRFS ARCHIVED]   if (credits !== null) return formatCredits(credits);
// [VRFS ARCHIVED]   return "Paid";
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function formatMarketPrice(item: VRFSMarketplaceItem): string {
// [VRFS ARCHIVED]   const price = Number(item.coins_price);
// [VRFS ARCHIVED]   return Number.isFinite(price) ? formatCredits(price) : "Unknown";
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function marketType(item: VRFSMarketplaceItem): string {
// [VRFS ARCHIVED]   const category = String(item.category_id ?? item.category ?? "");
// [VRFS ARCHIVED]   const map: Record<string, string> = {
// [VRFS ARCHIVED]     "1": "Boots",
// [VRFS ARCHIVED]     "2": "Glasses",
// [VRFS ARCHIVED]     "3": "Gloves",
// [VRFS ARCHIVED]     "4": "Hat",
// [VRFS ARCHIVED]     "5": "Mask",
// [VRFS ARCHIVED]     "6": "Scarf",
// [VRFS ARCHIVED]     "7": "Other",
// [VRFS ARCHIVED]   };
// [VRFS ARCHIVED]   if (map[category]) return map[category];
// [VRFS ARCHIVED]   const sku = getSku(item).toLowerCase();
// [VRFS ARCHIVED]   if (/boot|shoe/.test(sku)) return "Boots";
// [VRFS ARCHIVED]   if (/glass|goggle/.test(sku)) return "Glasses";
// [VRFS ARCHIVED]   if (/glove|hand/.test(sku)) return "Gloves";
// [VRFS ARCHIVED]   if (/hat|cap|helmet/.test(sku)) return "Hat";
// [VRFS ARCHIVED]   if (/mask|face/.test(sku)) return "Mask";
// [VRFS ARCHIVED]   if (/scarf|neck/.test(sku)) return "Scarf";
// [VRFS ARCHIVED]   return "Other";
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function getCatalogImage(item: VRFSItem): string | null {
// [VRFS ARCHIVED]   const candidates = [
// [VRFS ARCHIVED]     item.image,
// [VRFS ARCHIVED]     item.image_url,
// [VRFS ARCHIVED]     item.thumbnail_url,
// [VRFS ARCHIVED]     item.texture_url,
// [VRFS ARCHIVED]     item.thumbnail,
// [VRFS ARCHIVED]     typeof item.thumb === "string"
// [VRFS ARCHIVED]       ? `https://vrfs.sebyplay.xyz/lockerchecker/assets/thumbs/${item.thumb}`
// [VRFS ARCHIVED]       : null,
// [VRFS ARCHIVED]   ];
// [VRFS ARCHIVED]   for (const candidate of candidates) {
// [VRFS ARCHIVED]     const url = safeUrl(candidate);
// [VRFS ARCHIVED]     if (url) return url;
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   return null;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function getMarketplaceImage(item: VRFSMarketplaceItem): string | null {
// [VRFS ARCHIVED]   const candidates = [
// [VRFS ARCHIVED]     item.thumbnail_url,
// [VRFS ARCHIVED]     item.texture_url,
// [VRFS ARCHIVED]     item.thumbnail,
// [VRFS ARCHIVED]     item.image_url,
// [VRFS ARCHIVED]   ];
// [VRFS ARCHIVED]   for (const candidate of candidates) {
// [VRFS ARCHIVED]     const url = safeUrl(candidate);
// [VRFS ARCHIVED]     if (url) return url;
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   return null;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function avatarUrl(id: number): string {
// [VRFS ARCHIVED]   return `https://userpic.vrfs.org/avatar/avatar-pics/${encodeURIComponent(
// [VRFS ARCHIVED]     String(id)
// [VRFS ARCHIVED]   )}.png`;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createEmbed(
// [VRFS ARCHIVED]   title?: string,
// [VRFS ARCHIVED]   color = 0x5865f2,
// [VRFS ARCHIVED]   description?: string
// [VRFS ARCHIVED] ): EmbedBuilder {
// [VRFS ARCHIVED]   const embed = new EmbedBuilder().setColor(color);
// [VRFS ARCHIVED]   if (title) embed.setTitle(truncate(title, 256));
// [VRFS ARCHIVED]   if (description) embed.setDescription(truncate(description, 4096));
// [VRFS ARCHIVED]   return embed;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createItemEmbed(item: VRFSItem): EmbedBuilder {
// [VRFS ARCHIVED]   const name = getItemName(item);
// [VRFS ARCHIVED]   const embed = createEmbed(name);
// [VRFS ARCHIVED]   const sku = getSku(item);
// [VRFS ARCHIVED]   if (sku) embed.setDescription(`\`${truncate(sku, 200)}\``);
// [VRFS ARCHIVED]   const image = getCatalogImage(item);
// [VRFS ARCHIVED]   if (image) embed.setImage(image);
// [VRFS ARCHIVED]   embed.addFields(
// [VRFS ARCHIVED]     { name: "Section", value: truncate(getSection(item), 1024), inline: true },
// [VRFS ARCHIVED]     { name: "Price", value: formatItemPrice(item), inline: true },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "ID",
// [VRFS ARCHIVED]       value: String(item.id ?? item.item_id ?? item.itemId ?? "Unknown"),
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   return embed;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createMarketplaceEmbed(item: VRFSMarketplaceItem): EmbedBuilder {
// [VRFS ARCHIVED]   const name = String((item.title ?? item.name ?? getSku(item)) || "Marketplace Item");
// [VRFS ARCHIVED]   const embed = createEmbed(name, getMarketplaceActive(item) ? 0x2ecc71 : 0xe74c3c);
// [VRFS ARCHIVED]   embed.setDescription(
// [VRFS ARCHIVED]     `ID \`#${item.id}\`\n${getSku(item) ? `\`${getSku(item)}\`` : "No SKU available"}`
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const image = getMarketplaceImage(item);
// [VRFS ARCHIVED]   if (image) embed.setImage(image);
// [VRFS ARCHIVED]   const creator = getMarketplaceCreatorName(item);
// [VRFS ARCHIVED]   const creatorId = getMarketplaceCreatorId(item);
// [VRFS ARCHIVED]   embed.addFields(
// [VRFS ARCHIVED]     { name: "Type", value: marketType(item), inline: true },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Price",
// [VRFS ARCHIVED]       value: formatMarketPrice(item),
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Owners",
// [VRFS ARCHIVED]       value: formatNumber(getMarketplaceOwners(item)),
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Creator",
// [VRFS ARCHIVED]       value: creatorId ? `${creator}\nID \`${creatorId}\`` : creator,
// [VRFS ARCHIVED]       inline: false,
// [VRFS ARCHIVED]     },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Availability",
// [VRFS ARCHIVED]       value: getMarketplaceActive(item) ? "Available" : "Unavailable",
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Gifts",
// [VRFS ARCHIVED]       value: formatNumber(item.gifts_left),
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   return embed;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createPlayerEmbed(
// [VRFS ARCHIVED]   id: number,
// [VRFS ARCHIVED]   username: string,
// [VRFS ARCHIVED]   profile?: VRFSProfile,
// [VRFS ARCHIVED]   outfits?: VRFSOutfit[]
// [VRFS ARCHIVED] ): EmbedBuilder {
// [VRFS ARCHIVED]   const embed = createEmbed(username);
// [VRFS ARCHIVED]   embed.setDescription(`ID \`${id}\``);
// [VRFS ARCHIVED]   embed.setThumbnail(avatarUrl(id));
// [VRFS ARCHIVED]   embed.addFields(
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Country",
// [VRFS ARCHIVED]       value: profile?.profileCountry || "Unknown",
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Followers",
// [VRFS ARCHIVED]       value: formatNumber(profile?.followersCount),
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     },
// [VRFS ARCHIVED]     {
// [VRFS ARCHIVED]       name: "Public Outfits",
// [VRFS ARCHIVED]       value: formatNumber(outfits?.length ?? 0),
// [VRFS ARCHIVED]       inline: true,
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const socials: Array<[string, string | undefined]> = [
// [VRFS ARCHIVED]     ["User Tag", profile?.userTag],
// [VRFS ARCHIVED]     ["TikTok", profile?.tiktokName],
// [VRFS ARCHIVED]     ["YouTube", profile?.youtubeName],
// [VRFS ARCHIVED]     ["Twitch", profile?.twitchName],
// [VRFS ARCHIVED]     ["Instagram", profile?.instagramName],
// [VRFS ARCHIVED]   ];
// [VRFS ARCHIVED]   const availableSocials = socials.filter(([, value]) => value);
// [VRFS ARCHIVED]   if (availableSocials.length > 0) {
// [VRFS ARCHIVED]     embed.addFields(
// [VRFS ARCHIVED]       availableSocials.map(([name, value]) => ({
// [VRFS ARCHIVED]         name,
// [VRFS ARCHIVED]         value: truncate(String(value), 1024),
// [VRFS ARCHIVED]         inline: true,
// [VRFS ARCHIVED]       }))
// [VRFS ARCHIVED]     );
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   if (outfits && outfits.length > 0) {
// [VRFS ARCHIVED]     const latest = outfits[0];
// [VRFS ARCHIVED]     const slots = latest?.slots ?? {};
// [VRFS ARCHIVED]     const slotLines = Object.entries(slots)
// [VRFS ARCHIVED]       .filter(([, value]) => value)
// [VRFS ARCHIVED]       .slice(0, 20)
// [VRFS ARCHIVED]       .map(([slot, sku]) => `**${slot}**\n\`${sku}\``);
// [VRFS ARCHIVED]     if (slotLines.length) {
// [VRFS ARCHIVED]       embed.addFields({
// [VRFS ARCHIVED]         name: "Latest Outfit",
// [VRFS ARCHIVED]         value: truncate(slotLines.join("\n"), 1024),
// [VRFS ARCHIVED]         inline: false,
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   return embed;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createLockerSummaryEmbed(
// [VRFS ARCHIVED]   id: number,
// [VRFS ARCHIVED]   username: string,
// [VRFS ARCHIVED]   catalog: VRFSItem[],
// [VRFS ARCHIVED]   ownership: OwnershipCheckResult
// [VRFS ARCHIVED] ): EmbedBuilder {
// [VRFS ARCHIVED]   const owned = ownership.owned.length;
// [VRFS ARCHIVED]   const notOwned = ownership.notOwned.length;
// [VRFS ARCHIVED]   const unknown = ownership.unknown.length;
// [VRFS ARCHIVED]   const completion =
// [VRFS ARCHIVED]     catalog.length > 0 ? ((owned / catalog.length) * 100).toFixed(1) : "0.0";
// [VRFS ARCHIVED]   const ownedItems = catalog.filter((item) =>
// [VRFS ARCHIVED]     ownership.owned.includes(getSku(item))
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const free = ownedItems.filter(isItemFree).length;
// [VRFS ARCHIVED]   const paid = Math.max(0, owned - free);
// [VRFS ARCHIVED]   const sections = new Map<string, number>();
// [VRFS ARCHIVED]   for (const item of ownedItems) {
// [VRFS ARCHIVED]     const section = getSection(item);
// [VRFS ARCHIVED]     sections.set(section, (sections.get(section) ?? 0) + 1);
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   const sectionLines = [...sections.entries()]
// [VRFS ARCHIVED]     .sort((a, b) => b[1] - a[1])
// [VRFS ARCHIVED]     .slice(0, 12)
// [VRFS ARCHIVED]     .map(([section, count]) => `**${section}** — ${count.toLocaleString()}`);
// [VRFS ARCHIVED]   const color =
// [VRFS ARCHIVED]     unknown > 0 ? 0xf1c40f : 0x2ecc71;
// [VRFS ARCHIVED]   const embed = createEmbed(
// [VRFS ARCHIVED]     "Locker",
// [VRFS ARCHIVED]     color,
// [VRFS ARCHIVED]     `**${username}**\nID \`${id}\`\n\n**${owned.toLocaleString()} / ${catalog.length.toLocaleString()}** items owned\n${completion}% collection`
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   embed.setThumbnail(avatarUrl(id));
// [VRFS ARCHIVED]   embed.addFields(
// [VRFS ARCHIVED]     { name: "Free", value: free.toLocaleString(), inline: true },
// [VRFS ARCHIVED]     { name: "Paid", value: paid.toLocaleString(), inline: true },
// [VRFS ARCHIVED]     { name: "Unconfirmed", value: unknown.toLocaleString(), inline: true },
// [VRFS ARCHIVED]     { name: "Not Owned", value: notOwned.toLocaleString(), inline: true }
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   if (sectionLines.length) {
// [VRFS ARCHIVED]     embed.addFields({
// [VRFS ARCHIVED]       name: "Collection by Section",
// [VRFS ARCHIVED]       value: truncate(sectionLines.join("\n"), 1024),
// [VRFS ARCHIVED]       inline: false,
// [VRFS ARCHIVED]     });
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   if (ownership.health !== "ok") {
// [VRFS ARCHIVED]     embed.addFields({
// [VRFS ARCHIVED]       name: "Status",
// [VRFS ARCHIVED]       value: "Some items could not be confirmed.",
// [VRFS ARCHIVED]       inline: false,
// [VRFS ARCHIVED]     });
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   return embed;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] interface VRFSPage {
// [VRFS ARCHIVED]   id: string;
// [VRFS ARCHIVED]   type: "catalog" | "marketplace" | "locker";
// [VRFS ARCHIVED]   userId: string;
// [VRFS ARCHIVED]   createdAt: number;
// [VRFS ARCHIVED]   page: number;
// [VRFS ARCHIVED]   pageSize: number;
// [VRFS ARCHIVED]   items: VRFSItem[] | VRFSMarketplaceItem[];
// [VRFS ARCHIVED]   title: string;
// [VRFS ARCHIVED]   description?: string;
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function paginate<T>(items: T[], page: number, size: number) {
// [VRFS ARCHIVED]   const totalPages = Math.max(1, Math.ceil(items.length / size));
// [VRFS ARCHIVED]   const safePage = Math.max(0, Math.min(page, totalPages - 1));
// [VRFS ARCHIVED]   return {
// [VRFS ARCHIVED]     page: safePage,
// [VRFS ARCHIVED]     totalPages,
// [VRFS ARCHIVED]     items: items.slice(safePage * size, safePage * size + size),
// [VRFS ARCHIVED]   };
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createPaginationRow(
// [VRFS ARCHIVED]   id: string,
// [VRFS ARCHIVED]   page: number,
// [VRFS ARCHIVED]   totalPages: number
// [VRFS ARCHIVED] ): ActionRowBuilder<ButtonBuilder> {
// [VRFS ARCHIVED]   return new ActionRowBuilder<ButtonBuilder>().addComponents(
// [VRFS ARCHIVED]     new ButtonBuilder()
// [VRFS ARCHIVED]       .setCustomId(`vrfs_page:${id}:prev`)
// [VRFS ARCHIVED]       .setLabel("⬅️")
// [VRFS ARCHIVED]       .setStyle(ButtonStyle.Secondary)
// [VRFS ARCHIVED]       .setDisabled(page <= 0),
// [VRFS ARCHIVED]     new ButtonBuilder()
// [VRFS ARCHIVED]       .setCustomId(`vrfs_page:${id}:current`)
// [VRFS ARCHIVED]       .setLabel(`${page + 1} / ${totalPages}`)
// [VRFS ARCHIVED]       .setStyle(ButtonStyle.Primary)
// [VRFS ARCHIVED]       .setDisabled(true),
// [VRFS ARCHIVED]     new ButtonBuilder()
// [VRFS ARCHIVED]       .setCustomId(`vrfs_page:${id}:next`)
// [VRFS ARCHIVED]       .setLabel("➡️")
// [VRFS ARCHIVED]       .setStyle(ButtonStyle.Secondary)
// [VRFS ARCHIVED]       .setDisabled(page >= totalPages - 1)
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createCatalogPage(
// [VRFS ARCHIVED]   session: VRFSPage
// [VRFS ARCHIVED] ): V2Payload {
// [VRFS ARCHIVED]   const result = paginate(
// [VRFS ARCHIVED]     session.items as VRFSItem[],
// [VRFS ARCHIVED]     session.page,
// [VRFS ARCHIVED]     session.pageSize
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const embed = createEmbed(
// [VRFS ARCHIVED]     session.title,
// [VRFS ARCHIVED]     0x5865f2,
// [VRFS ARCHIVED]     session.description
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const lines = result.items.map((item, index) => {
// [VRFS ARCHIVED]     const position = result.page * session.pageSize + index + 1;
// [VRFS ARCHIVED]     return `**${position}. ${truncate(getItemName(item), 80)}**\n\`${truncate(
// [VRFS ARCHIVED]       getSku(item),
// [VRFS ARCHIVED]       100
// [VRFS ARCHIVED]     )}\` · ${getSection(item)} · ${formatItemPrice(item)}`;
// [VRFS ARCHIVED]   });
// [VRFS ARCHIVED]   embed.addFields({
// [VRFS ARCHIVED]     name: "Items",
// [VRFS ARCHIVED]     value: truncate(lines.join("\n\n") || "No items found.", 4096),
// [VRFS ARCHIVED]     inline: false,
// [VRFS ARCHIVED]   });
// [VRFS ARCHIVED]   const container = embedToV2Container(embed);
// [VRFS ARCHIVED]   if (result.totalPages > 1) {
// [VRFS ARCHIVED]     container.addActionRowComponents(
// [VRFS ARCHIVED]       createPaginationRow(session.id, result.page, result.totalPages)
// [VRFS ARCHIVED]     );
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   return { components: [container], flags: MessageFlags.IsComponentsV2 };
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createMarketplacePage(
// [VRFS ARCHIVED]   session: VRFSPage
// [VRFS ARCHIVED] ): V2Payload {
// [VRFS ARCHIVED]   const result = paginate(
// [VRFS ARCHIVED]     session.items as VRFSMarketplaceItem[],
// [VRFS ARCHIVED]     session.page,
// [VRFS ARCHIVED]     session.pageSize
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const embed = createEmbed(
// [VRFS ARCHIVED]     session.title,
// [VRFS ARCHIVED]     0x5865f2,
// [VRFS ARCHIVED]     session.description
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const lines = result.items.map((item, index) => {
// [VRFS ARCHIVED]     const position = result.page * session.pageSize + index + 1;
// [VRFS ARCHIVED]     return `**${position}. ${truncate(
// [VRFS ARCHIVED]       String(item.title ?? item.name ?? getSku(item)),
// [VRFS ARCHIVED]       80
// [VRFS ARCHIVED]     )}**\n#${item.id} · ${marketType(item)} · ${formatNumber(
// [VRFS ARCHIVED]       getMarketplaceOwners(item)
// [VRFS ARCHIVED]     )} owners`;
// [VRFS ARCHIVED]   });
// [VRFS ARCHIVED]   embed.addFields({
// [VRFS ARCHIVED]     name: "Items",
// [VRFS ARCHIVED]     value: truncate(lines.join("\n\n") || "No items found.", 4096),
// [VRFS ARCHIVED]     inline: false,
// [VRFS ARCHIVED]   });
// [VRFS ARCHIVED]   const container = embedToV2Container(embed);
// [VRFS ARCHIVED]   if (result.totalPages > 1) {
// [VRFS ARCHIVED]     container.addActionRowComponents(
// [VRFS ARCHIVED]       createPaginationRow(session.id, result.page, result.totalPages)
// [VRFS ARCHIVED]     );
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   return { components: [container], flags: MessageFlags.IsComponentsV2 };
// [VRFS ARCHIVED] }
// [VRFS ARCHIVED] function createLockerPage(
// [VRFS ARCHIVED]   session: VRFSPage
// [VRFS ARCHIVED] ): V2Payload {
// [VRFS ARCHIVED]   const result = paginate(
// [VRFS ARCHIVED]     session.items as VRFSItem[],
// [VRFS ARCHIVED]     session.page,
// [VRFS ARCHIVED]     session.pageSize
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const embed = createEmbed(
// [VRFS ARCHIVED]     session.title,
// [VRFS ARCHIVED]     0x2ecc71,
// [VRFS ARCHIVED]     session.description
// [VRFS ARCHIVED]   );
// [VRFS ARCHIVED]   const lines = result.items.map((item, index) => {
// [VRFS ARCHIVED]     const position = result.page * session.pageSize + index + 1;
// [VRFS ARCHIVED]     return `**${position}. ${truncate(getItemName(item), 80)}**\n\`${truncate(
// [VRFS ARCHIVED]       getSku(item),
// [VRFS ARCHIVED]       100
// [VRFS ARCHIVED]     )}\` · ${getSection(item)} · ${formatItemPrice(item)}`;
// [VRFS ARCHIVED]   });
// [VRFS ARCHIVED]   embed.addFields({
// [VRFS ARCHIVED]     name: "Owned Items",
// [VRFS ARCHIVED]     value: truncate(lines.join("\n\n") || "No owned items.", 4096),
// [VRFS ARCHIVED]     inline: false,
// [VRFS ARCHIVED]   });
// [VRFS ARCHIVED]   const image = result.items.length
// [VRFS ARCHIVED]     ? getCatalogImage(result.items[0])
// [VRFS ARCHIVED]     : null;
// [VRFS ARCHIVED]   if (image) embed.setImage(image);
// [VRFS ARCHIVED]   const container = embedToV2Container(embed);
// [VRFS ARCHIVED]   if (result.totalPages > 1) {
// [VRFS ARCHIVED]     container.addActionRowComponents(
// [VRFS ARCHIVED]       createPaginationRow(session.id, result.page, result.totalPages)
// [VRFS ARCHIVED]     );
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   return { components: [container], flags: MessageFlags.IsComponentsV2 };
// [VRFS ARCHIVED] }
export class BotManager {
  private client: Client;
  private commandsRegistered = false;
  private presenceInterval: NodeJS.Timeout | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;
  private verificationInterval: NodeJS.Timeout | null = null;
  private lastPresenceUpdate = 0;
  private presenceUpdateInFlight: Promise<void> | null = null;
// [VRFS ARCHIVED]   private vrfsCleanupInterval: NodeJS.Timeout | null = null;
// [VRFS ARCHIVED]   private vrfsPages = new Map<string, VRFSPage>();
  private giveawayPages = new Map<string, GiveawayPageState>();
  public metrics = new MetricsCollector();
  public notifications: NotificationService;
  private commands = new Map<
    string,
    (
      interaction: ChatInputCommandInteraction<CacheType>
    ) => Promise<void>
  >();
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
    this.commands.set("stats", this.statsCommand.bind(this));
    this.commands.set("active", this.activeCommand.bind(this));
    this.commands.set("recent", this.recentCommand.bind(this));
    this.commands.set("setchannel", this.setchannelCommand.bind(this));
    this.commands.set("reset", this.resetCommand.bind(this));
    this.commands.set("status", this.statusCommand.bind(this));
    this.commands.set("metrics", this.metricsCommand.bind(this));
    this.commands.set("help", this.helpCommand.bind(this));
    this.commands.set("purge", this.purgeCommand.bind(this));
    this.commands.set("giveawaytrack", this.giveawayTrackCommand.bind(this));
    this.commands.set("eventtrack", this.eventTrackCommand.bind(this));
    this.commands.set("licenseadmin", this.licenseAdminCommand.bind(this));
    this.commands.set("revoke", this.revokeCommand.bind(this));
// [VRFS ARCHIVED]     this.commands.set("vrfs", this.vrfsCommand.bind(this));
    this.client.on(
      "guildMemberUpdate",
      this.handleGuildMemberUpdate.bind(this)
    );
    this.client.on(
      "guildMemberAdd",
      this.handleGuildMemberAdd.bind(this)
    );
    this.client.once("ready", async () => {
      logger.info(`Logged in as ${this.client.user?.tag}`, {
        component: "BotManager",
      });
      await this.updatePresence();
      this.presenceInterval = setInterval(
        () => void this.updatePresence(),
        30_000
      );
      this.presenceInterval.unref?.();
      await this.purgeAndUpdatePresence();
      this.cleanupInterval = setInterval(
        () => {
          void this.purgeAndUpdatePresence();
          this.cleanupPages();
        },
        60_000
      );
      this.cleanupInterval.unref?.();
      await this.registerCommands();
      await this.sendNotificationPanel();
      await this.sendLicensePanel();
      await this.sendPremiumPanel();
      await this.assignPremiumToExistingBoosters();
      this.verificationInterval = setInterval(
        () => void this.verifyAllPremiumRoles(),
        300000
      );
      this.verificationInterval.unref?.();
// [VRFS ARCHIVED]       this.vrfsCleanupInterval = setInterval(
// [VRFS ARCHIVED]         () => this.cleanupVRFSPages(),
// [VRFS ARCHIVED]         60_000
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       this.vrfsCleanupInterval.unref?.();
    });
    this.client.on(
      "interactionCreate",
      async (interaction: Interaction) => {
        if (interaction.isButton()) {
          if (interaction.customId === "toggle_giveaway") {
            await this.handleNotificationToggle(interaction, "giveaways");
            return;
          }
          if (interaction.customId === "toggle_scrim") {
            await this.handleNotificationToggle(interaction, "scrims");
            return;
          }
          if (interaction.customId === "toggle_event") {
            await this.handleNotificationToggle(interaction, "events");
            return;
          }
          if (interaction.customId === "license_activate") {
            const channel = interaction.channel as TextChannel;
            const panel = new KeyPanel(channel);
            await panel.handleInteraction(interaction);
            return;
          }
          if (interaction.customId === "premium_autojoiner") {
            const channel = interaction.channel as TextChannel;
            const panel = new PremiumPanel(channel);
            await panel.handleInteraction(interaction);
            return;
          }
          if (
            ["admin_generate_key", "admin_list_keys", "admin_refresh"].includes(
              interaction.customId
            )
          ) {
            if (!isOwner(interaction.user.id)) {
              await replyV2Text(interaction as any, "Access denied", "You do not have permission to use this.");
              return;
            }
            const panel = new AdminPanel();
            await panel.handleInteraction(interaction);
            return;
          }
          if (
            [
              "activate_premium",
              "check_premium",
              "generate_key",
              "list_keys",
              "refresh_stats",
            ].includes(interaction.customId)
          ) {
            const channel = interaction.channel as TextChannel;
            const panel = new KeyPanel(channel);
            await panel.handleInteraction(interaction);
            return;
          }
          if (interaction.customId.startsWith("giveaway_page:")) {
            await this.handleGiveawayPageButton(interaction);
            return;
          }
// [VRFS ARCHIVED]           if (interaction.customId.startsWith("vrfs_page:")) {
// [VRFS ARCHIVED]             await this.handleVRFSPageButton(interaction);
// [VRFS ARCHIVED]             return;
// [VRFS ARCHIVED]           }
          return;
        }
        if (interaction.isModalSubmit()) {
          if (interaction.customId.startsWith("giveaway_search_modal:")) {
            await this.handleGiveawaySearchModal(interaction);
            return;
          }
          if (interaction.customId === "license_activate_modal") {
            const channel = interaction.channel as TextChannel;
            const panel = new KeyPanel(channel);
            await panel.handleModalSubmit(interaction);
            return;
          }
          if (interaction.customId === "premium_autojoiner_modal") {
            const channel = interaction.channel as TextChannel;
            const panel = new PremiumPanel(channel);
            await panel.handleModalSubmit(interaction);
            return;
          }
          if (interaction.customId === "activate_premium_modal") {
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
          await replyV2Text(interaction, "Unknown command", "That command is not registered.");
          return;
        }
        try {
          await handler(interaction);
        } catch (err) {
          logger.error(`Command error: ${interaction.commandName}`, {
            error: formatError(err),
          });
          if (interaction.replied || interaction.deferred) {
            await editV2Text(interaction, "Request failed", "Something went wrong while processing that request.").catch(() => {});
          } else {
            await replyV2Text(interaction, "Request failed", "Something went wrong while processing that request.").catch(() => {});
          }
        }
      }
    );
    this.client.on("error", (err) =>
      logger.error("Client error", { error: err })
    );
  }
  public async start(): Promise<void> {
    const LOGIN_TIMEOUT_MS = 10000;
    logger.info("BotManager: attempting login...", {
      component: "BotManager",
    });
    try {
      await Promise.race([
        this.client.login(this.botToken),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("Login timed out after 10s")),
            LOGIN_TIMEOUT_MS
          )
        ),
      ]);
      await Promise.race([
        new Promise<void>((resolve) => {
          if (this.client.isReady()) {
            resolve();
          } else {
            this.client.once("ready", () => resolve());
          }
        }),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("Ready event timed out after 10s")),
            LOGIN_TIMEOUT_MS
          )
        ),
      ]);
      logger.info("BotManager started successfully", {
        component: "BotManager",
      });
    } catch (err) {
      logger.error(`BotManager start failed: ${formatError(err)}`, {
        component: "BotManager",
      });
      throw err;
    }
  }
  public async destroy(): Promise<void> {
    if (this.presenceInterval) clearInterval(this.presenceInterval);
    if (this.cleanupInterval) clearInterval(this.cleanupInterval);
    if (this.verificationInterval) clearInterval(this.verificationInterval);
// [VRFS ARCHIVED]     if (this.vrfsCleanupInterval) clearInterval(this.vrfsCleanupInterval);
    this.notifications.shutdown();
// [VRFS ARCHIVED]     this.vrfsPages.clear();
    await this.client.destroy();
  }
  public async sendGiveawayNotification(
    data: GiveawayData & { inviteUrl?: string }
  ): Promise<boolean> {
    this.notifications.enqueue(data, data.inviteUrl || "");
    this.metrics.recordDetection(Date.now() - data.detectedAt);
    void this.updatePresence();
    return true;
  }
  private async sendNotificationPanel(): Promise<void> {
    const panelChannelId =
      process.env.PANEL_CHANNEL_ID ||
      process.env.NOTIFICATION_PANEL_CHANNEL_ID ||
      CONFIG.trackerChannelId;
    const channel = this.client.channels.cache.get(
      panelChannelId
    ) as TextChannel | undefined;
    if (!channel) {
      logger.warn("Notification panel channel not found", {
        component: "BotManager",
        channelId: panelChannelId,
      });
      return;
    }

    try {
      const messages = await channel.messages.fetch({ limit: 100 });
      const isNotificationPanel = (message: any): boolean => {
        if (message.author?.id !== this.client.user?.id) return false;
        if (
          message.embeds?.some(
            (embed: any) => embed?.title === "Notifications"
          )
        ) {
          return true;
        }
        const hasButton = (component: any): boolean => {
          if (!component) return false;
          if (
            ["toggle_giveaway", "toggle_scrim", "toggle_event"].includes(
              component.customId
            )
          ) {
            return true;
          }
          return Array.isArray(component.components)
            ? component.components.some(hasButton)
            : false;
        };
        return Array.isArray(message.components)
          ? message.components.some(hasButton)
          : false;
      };
      for (const message of messages.values()) {
        if (isNotificationPanel(message)) {
          await message.delete().catch(() => {});
        }
      }
    } catch (error) {
      logger.warn("Failed to clean old notification panels", {
        error: formatError(error),
      });
    }

    const container = createV2Container(
      "Notifications",
      "Manage which notifications you receive.\n\n**Giveaways**\nReceive notifications for new giveaways.\n\n**Scrims**\nReceive notifications for scrim announcements.\n\n**Events**\nReceive notifications for event announcements.",
      0x5865f2
    );
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder()
        .setCustomId("toggle_giveaway")
        .setLabel("Giveaways")
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId("toggle_scrim")
        .setLabel("Scrims")
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId("toggle_event")
        .setLabel("Events")
        .setStyle(ButtonStyle.Primary)
    );
    container.addActionRowComponents(row);
    await channel.send({
      components: [container],
      flags: MessageFlags.IsComponentsV2,
    });
    logger.info("Notification panel sent", { channelId: panelChannelId });
  }

  private async handleNotificationToggle(
    interaction: ButtonInteraction,
    type: "giveaways" | "scrims" | "events"
  ): Promise<void> {
    await interaction.deferReply({ ephemeral: true });
    const userId = interaction.user.id;
    const settings = await getUserNotificationSettings(userId);
    const currentState = settings[type];
    const newState = !currentState;
    await updateUserNotificationSetting(userId, type, newState);
    let roleId: string | undefined;
    const typeLabel = {
      giveaways: "Giveaway",
      scrims: "Scrim",
      events: "Event",
    }[type];
    if (type === "giveaways") roleId = process.env.PING_ROLE_ID;
    else if (type === "scrims") roleId = process.env.SCRIM_ROLE_ID;
    else roleId = process.env.EVENT_ROLE_ID;
    if (roleId && interaction.guild) {
      try {
        const member = await interaction.guild.members.fetch(userId);
        const role = interaction.guild.roles.cache.get(roleId);
        if (role) {
          if (newState) await member.roles.add(role);
          else await member.roles.remove(role);
        }
      } catch (error) {
        logger.error(`Failed to update ${type} role`, {
          userId,
          error: String(error),
        });
      }
    }
    await interaction.editReply(
      v2EditPayload(createV2Container("Notification settings", `${typeLabel} notifications ${
        newState ? "enabled" : "disabled"
      }.`))
    );
  }
  public async sendScrimNotification(data: any): Promise<boolean> {
    let channelId: string;
    let channelName: string;
    if (data.type === "scrim") {
      channelId = CONFIG.scrimChannelId || CONFIG.trackerChannelId;
      channelName = "Scrim";
    } else {
      channelId = CONFIG.eventChannelId || CONFIG.trackerChannelId;
      channelName = "Event";
    }
    const channel = this.client.channels.cache.get(
      channelId
    ) as TextChannel | undefined;
    if (!channel) {
      logger.warn(`${channelName} channel not found`, {
        component: "BotManager",
        channelId,
      });
      const fallbackChannel = this.client.channels.cache.get(
        CONFIG.trackerChannelId
      ) as TextChannel | undefined;
      if (!fallbackChannel) return false;
      return this.sendScrimToChannel(data, fallbackChannel);
    }
    return this.sendScrimToChannel(data, channel);
  }
  private async sendScrimToChannel(
    data: any,
    channel: TextChannel
  ): Promise<boolean> {
    const typeLabel =
      {
        scrim: "Scrim",
        squid_game: "Squid Game",
        gagaball: "Gagaball",
      }[data.type] || "Event";
    const typeColor =
      {
        scrim: 0x5865f2,
        squid_game: 0xff6b6b,
        gagaball: 0x4ecdc4,
      }[data.type] || 0x5865f2;
    const guild = this.client.guilds.cache.get(data.guildId);
    const guildName = guild?.name || data.guildName || "Unknown";
    const guildIcon = data.guildIcon || guild?.iconURL({ size: 512 }) || null;
    const guildBanner =
      data.guildBanner || guild?.bannerURL({ size: 1024 }) || null;
    const memberCount = (data.memberCount || guild?.memberCount) ?? null;
    const inviteUrl = this.getFastInviteUrl(data.guildId, data.channelId, data.inviteUrl);
    let pingMention = "@everyone";
    if (data.type === "scrim") {
      const roleId = process.env.SCRIM_ROLE_ID;
      if (roleId) pingMention = `<@&${roleId}>`;
    } else {
      const roleId = process.env.EVENT_ROLE_ID;
      if (roleId) pingMention = `<@&${roleId}>`;
    }
    const description = [
      "### Details",
      `**Server:** ${guildName}`,
      `**Channel:** #${data.channelName}`,
      data.host ? `**Host:** ${data.host}` : "",
      data.coHost ? `**Co-Host:** ${data.coHost}` : "",
      data.time ? `**Time:** ${data.time}` : "",
      data.teams ? `**Teams:** ${data.teams}` : "",
      data.region ? `**Region:** ${data.region}` : "",
      data.reward ? `**Reward:** ${data.reward}` : "",
      data.ticks !== null ? `**Ticks:** ${data.ticks}+` : "",
      "",
      "### Time",
      `**Detected:** <t:${Math.floor(data.detectedAt / 1000)}:R>`,
      "",
      "### Links",
      `**Invite:** ${inviteUrl}`,
      memberCount
        ? `**Members:** ${memberCount.toLocaleString()}`
        : "",
    ]
      .filter(Boolean)
      .join("\n");
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
    if (inviteUrl.startsWith("http")) {
      row.addComponents(
        new ButtonBuilder()
          .setLabel("Join Server")
          .setStyle(ButtonStyle.Link)
          .setURL(inviteUrl)
      );
    }
    row.addComponents(
      new ButtonBuilder()
        .setLabel("Message")
        .setStyle(ButtonStyle.Link)
        .setURL(messageUrl)
    );
    try {
      const container = embedToV2Container(embed);
      if (pingMention) addV2Text(container, pingMention);
      container.addActionRowComponents(row);
      await channel.send({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
      return true;
    } catch (error) {
      logger.error("Failed to send scrim notification", {
        component: "BotManager",
        error: formatError(error),
      });
      return false;
    }
  }
  private async deleteAllPremiumData(
    userId: string,
    guildId: string
  ): Promise<void> {
    await removePremiumUser(userId, guildId);
    await removeBoosterPremium(userId, guildId);
    await updateUserToken(userId, guildId, "", "");
    await updateUserWebhook(userId, guildId, "");
    try {
      const autoJoinCol = await getAutoJoinEntriesCollection();
      await autoJoinCol.deleteMany({ userId });
    } catch (error) {
      logger.warn("Failed to delete auto-join entries", {
        userId,
        error: String(error),
      });
    }
    try {
      const { stopTokenSession } = await import("./premium/tokenManager.js");
      stopTokenSession(userId, guildId);
    } catch {}
    try {
      clearPremiumCache(userId);
    } catch {}
  }
  private async verifyAllPremiumRoles(): Promise<void> {
    const guildId = process.env.GUILD_ID;
    const premiumRoleId = process.env.PREMIUM_ROLE_ID;
    if (!guildId || !premiumRoleId) return;
    try {
      const guild = await this.client.guilds.fetch(guildId);
      const members = await guild.members.fetch();
      const allPremiumUsers = await getAllPremiumUsers(guildId);
      const validUserIds = new Set(allPremiumUsers.map((u) => u.userId));
      const boosterRoleId = process.env.BOOSTER_ROLE_ID;
      let fixed = 0;
      for (const [, member] of members) {
        const hasRole = member.roles.cache.has(premiumRoleId);
        const isBooster = boosterRoleId
          ? member.roles.cache.has(boosterRoleId)
          : false;
        const shouldHaveRole = validUserIds.has(member.id) || isBooster;
        if (hasRole && !shouldHaveRole) {
          await member.roles.remove(premiumRoleId);
          fixed++;
        } else if (!hasRole && shouldHaveRole) {
          await member.roles.add(premiumRoleId);
          fixed++;
        }
      }
      if (fixed > 0) {
        logger.info(`Premium role verification fixed ${fixed} members`, {
          component: "BotManager",
        });
      }
    } catch (error) {
      logger.error("Premium role verification failed", {
        component: "BotManager",
        error: formatError(error),
      });
    }
  }
  private getFastInviteUrl(
    guildId: string | undefined,
    channelId: string | undefined,
    fallbackInvite?: string | null
  ): string {
    if (fallbackInvite && fallbackInvite.startsWith("http")) return fallbackInvite;
    if (!guildId) return "No invite available";
    const guild = this.client.guilds.cache.get(guildId);
    if (!guild) return "No invite available";
    if (guild.vanityURLCode) return `https://discord.gg/${guild.vanityURLCode}`;
    const channel = channelId ? guild.channels.cache.get(channelId) : null;
    if (channel && "createInvite" in channel) {
      const cached = (channel as any).lastInviteUrl;
      if (typeof cached === "string" && cached.startsWith("http")) return cached;
    }
    return "No invite available";
  }

  private async resolveInviteUrl(
    guildId: string,
    channelId: string,
    fallbackInvite?: string | null
  ): Promise<string> {
    if (fallbackInvite && fallbackInvite.startsWith("http")) {
      return fallbackInvite;
    }
    let inviteUrl = "No invite available";
    try {
      const guild = this.client.guilds.cache.get(guildId);
      if (guild) {
        const invites = await guild.invites.fetch().catch(
          () => new Collection<string, Invite>()
        );
        const existingInvite = invites.find(
          (inv: Invite) => inv.channelId === channelId && inv.maxUses === 0
        );
        if (existingInvite) return existingInvite.url;
        const channel = guild.channels.cache.get(channelId);
        if (
          channel &&
          channel.isTextBased() &&
          "createInvite" in channel
        ) {
          const perms = channel.permissionsFor(this.client.user?.id || "");
          if (perms?.has("CreateInstantInvite")) {
            const newInvite = await channel.createInvite({
              maxAge: 86400,
              maxUses: 0,
              reason: "Tracker notification",
            });
            inviteUrl = newInvite.url;
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
    memberCount?: number | null
  ): Promise<boolean> {
    try {
      let user;
      try {
        user = await this.client.users.fetch(userId);
      } catch {
        user = this.client.users.cache.get(userId);
        if (!user) return false;
      }
      const dmChannel = await user.createDM().catch(() => null);
      if (!dmChannel) return false;
      const urlParts = messageUrl.split("/");
      const channelId = urlParts[5] || "";
      const resolvedInvite = this.getFastInviteUrl(guildId, channelId, inviteUrl);
      const endTimestamp = endsAt
        ? Math.floor(endsAt / 1000)
        : Math.floor((Date.now() + 3600000) / 1000);
      const winnerCount = extractWinnerCount(prize);
      const description = [
        "### Details",
        `**Server:** ${guildName}`,
        `**Channel:** #${channelName}`,
        `**Winners:** ${winnerCount}`,
        "",
        "### Time",
        `**Ends:** <t:${endTimestamp}:F>`,
        `**Countdown:** <t:${endTimestamp}:R>`,
        "",
        "### Links",
        `**Invite:** ${resolvedInvite}`,
        memberCount
          ? `**Members:** ${memberCount.toLocaleString()}`
          : "",
      ]
        .filter(Boolean)
        .join("\n");
      const embed = new EmbedBuilder()
        .setAuthor({
          name: "New Giveaway",
          iconURL: this.client.user?.displayAvatarURL(),
        })
        .setTitle(prize || "Unknown Prize")
        .setDescription(description)
        .setColor(0x5865f2);
      if (guildIcon) embed.setThumbnail(guildIcon);
      if (guildBanner) embed.setImage(guildBanner);
      const row = new ActionRowBuilder<ButtonBuilder>();
      if (resolvedInvite.startsWith("http")) {
        row.addComponents(
          new ButtonBuilder()
            .setLabel("Join Server")
            .setStyle(ButtonStyle.Link)
            .setURL(resolvedInvite)
        );
      }
      row.addComponents(
        new ButtonBuilder()
          .setLabel("Message")
          .setStyle(ButtonStyle.Link)
          .setURL(messageUrl)
      );
      const container = embedToV2Container(embed);
      container.addActionRowComponents(row);
      await dmChannel.send({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
      return true;
    } catch {
      return false;
    }
  }
  public async sendGiveawayEndedDM(
    userId: string,
    prize: string,
    guildName: string,
    channelName: string,
    messageUrl: string,
    guildIcon?: string | null
  ): Promise<boolean> {
    try {
      let user;
      try {
        user = await this.client.users.fetch(userId);
      } catch {
        user = this.client.users.cache.get(userId);
        if (!user) return false;
      }
      const dmChannel = await user.createDM().catch(() => null);
      if (!dmChannel) return false;
      const embed = new EmbedBuilder()
        .setAuthor({
          name: "Giveaway Ended",
          iconURL: this.client.user?.displayAvatarURL(),
        })
        .setTitle(prize || "Giveaway Ended")
        .setDescription(
          [
            `**Server:** ${guildName}`,
            `**Channel:** #${channelName}`,
            "",
            "This giveaway has ended.",
          ].join("\n")
        )
        .setColor(0xe74c3c);
      if (guildIcon) embed.setThumbnail(guildIcon);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder()
          .setLabel("View Giveaway")
          .setStyle(ButtonStyle.Link)
          .setURL(messageUrl)
      );
      const container = embedToV2Container(embed);
      container.addActionRowComponents(row);
      await dmChannel.send({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
      return true;
    } catch {
      return false;
    }
  }
  private async giveawayTrackCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const sub = interaction.options.getSubcommand();
    if (sub === "add") await this.giveawayAdd(interaction);
    else if (sub === "remove") await this.giveawayRemove(interaction);
    else if (sub === "list") await this.giveawayList(interaction);
    else if (sub === "clear") await this.giveawayClear(interaction);
  }
  private async giveawayAdd(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const item = interaction.options.getString("item", true).trim().toLowerCase();
    if (item.length < 2 || item.length > 50) {
      await replyV2Text(interaction, "Updated", "Item must be between 2 and 50 characters.");
      return;
    }
    await addItem(interaction.user.id, item);
    const items = await getItems(interaction.user.id);
    await replyV2Text(interaction, "Giveaway tracking", `Tracking **${item}**.\n\nYour items:\n${items
        .map((i) => `- ${i}`)
        .join("\n")}`);
  }
  private async giveawayRemove(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const item = interaction.options.getString("item", true).trim().toLowerCase();
    const removed = await removeItem(interaction.user.id, item);
    if (!removed) {
      await replyV2Text(interaction, "Updated", `**${item}** is not in your tracked items.`);
      return;
    }
    const items = await getItems(interaction.user.id);
    await replyV2Text(interaction, "Giveaway tracking", `Removed **${item}**.\n\nYour items:\n${items
        .map((i) => `- ${i}`)
        .join("\n")}`);
  }
  private async giveawayList(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const items = await getItems(interaction.user.id);
    if (items.length === 0) {
      await replyV2Text(interaction, "Updated", "You are not tracking any giveaway items. Use `/giveawaytrack add` to start.");
      return;
    }
    await replyV2Text(interaction, "Giveaway tracking", `**Tracked giveaway items (${items.length})**\n${items
        .map((i) => `- ${i}`)
        .join("\n")}`);
  }
  private async giveawayClear(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const items = await getItems(interaction.user.id);
    if (items.length === 0) {
      await replyV2Text(interaction, "Updated", "Your tracked item list is already empty.");
      return;
    }
    await clearItems(interaction.user.id);
    await replyV2Text(interaction, "Updated", `Cleared ${items.length} tracked item${
        items.length === 1 ? "" : "s"
      }.`);
  }
  private async eventTrackCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const sub = interaction.options.getSubcommand();
    if (sub === "add") await this.eventAdd(interaction);
    else if (sub === "remove") await this.eventRemove(interaction);
    else if (sub === "list") await this.eventList(interaction);
    else if (sub === "clear") await this.eventClear(interaction);
  }
  private async eventAdd(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const filter = interaction.options.getString("filter", true).trim().toLowerCase();
    const validFilters = [
      "scrim",
      "squid",
      "squid_game",
      "gagaball",
      "2v2",
      "3v3",
      "4v4",
      "5v5",
      "1v1",
      "vrll",
      "vrel",
      "vucl",
    ];
    const matchedFilter = validFilters.find((f) => filter.includes(f));
    if (!matchedFilter && filter.length < 2) {
      await replyV2Text(interaction, "Updated", "Invalid filter. Supported filters include scrim, squid, squid_game, gagaball, 1v1, 2v2, 3v3, 4v4, 5v5, vrll, vrel and vucl.");
      return;
    }
    const eventItem = `event:${filter}`;
    await addItem(interaction.user.id, eventItem);
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter((i) => i.startsWith("event:"));
    await replyV2Text(interaction, "Event tracking", `Tracking **${filter}**.\n\nYour event filters:\n${eventItems
        .map((i) => `- ${i.replace("event:", "")}`)
        .join("\n")}`);
  }
  private async eventRemove(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const filter = interaction.options.getString("filter", true).trim().toLowerCase();
    const eventItem = `event:${filter}`;
    const removed = await removeItem(interaction.user.id, eventItem);
    if (!removed) {
      await replyV2Text(interaction, "Updated", `**${filter}** is not in your event filters.`);
      return;
    }
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter((i) => i.startsWith("event:"));
    await replyV2Text(interaction, "Event tracking", `Removed **${filter}**.\n\nYour event filters:\n${eventItems
        .map((i) => `- ${i.replace("event:", "")}`)
        .join("\n")}`);
  }
  private async eventList(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter((i) => i.startsWith("event:"));
    if (eventItems.length === 0) {
      await replyV2Text(interaction, "Updated", "You are not tracking any event filters. Use `/eventtrack add` to start.");
      return;
    }
    await replyV2Text(interaction, "Event tracking", `**Event filters (${eventItems.length})**\n${eventItems
        .map((i) => `- ${i.replace("event:", "")}`)
        .join("\n")}`);
  }
  private async eventClear(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    const items = await getItems(interaction.user.id);
    const eventItems = items.filter((i) => i.startsWith("event:"));
    if (eventItems.length === 0) {
      await replyV2Text(interaction, "Updated", "Your event filter list is already empty.");
      return;
    }
    for (const item of eventItems) {
      await removeItem(interaction.user.id, item);
    }
    await replyV2Text(interaction, "Updated", `Cleared ${eventItems.length} event filter${
        eventItems.length === 1 ? "" : "s"
      }.`);
  }
  private async licenseAdminCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    if (!(await requireOwner(interaction))) return;
    await interaction.deferReply({ ephemeral: true });
    const panel = new AdminPanel();
    await panel.sendPanel(interaction);
  }
  private async revokeCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ): Promise<void> {
    if (!(await requireAdmin(interaction))) return;
    const user = interaction.options.getUser("user", true);
    const guildId = interaction.guildId;
    if (!guildId) {
      await replyV2Text(interaction, "Unavailable", "This command must be used in a server.");
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    try {
      const hasPremium = await isPremiumUser(user.id, guildId);
      if (!hasPremium) {
        await editV2Text(interaction, "Premium access", `<@${user.id}> does not have premium access.`);
        return;
      }
      const premiumUser = await getPremiumUser(user.id, guildId);
      const source = premiumUser?.source || "unknown";
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
        logger.warn("Could not remove premium role", {
          userId: user.id,
          error: String(roleError),
        });
      }
      logger.info("Premium revoked by admin", {
        adminId: interaction.user.id,
        userId: user.id,
        guildId,
        source,
      });
      await editV2Text(interaction, "Premium revoked", `Premium access has been revoked from <@${user.id}>.`);
    } catch (error) {
      logger.error("Revoke command failed", {
        adminId: interaction.user.id,
        userId: user.id,
        error: String(error),
      });
      await editV2Text(interaction, "Revoke failed", "Failed to revoke premium access.", 0xe74c3c);
    }
  }
  private async statsCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    await deferReply(interaction, false);
    const stats = await getStats();
    const totalEver = await getTotalDetected();
    let scrimStats: Awaited<ReturnType<typeof getScrimStats>> | null = null;
    try {
      scrimStats = await getScrimStats();
    } catch {}

    const sections = [
      `**Total Giveaways Tracked:** ${totalEver}`,
      `**Active Giveaways:** ${stats.activeGiveaways}`,
      `**Servers:** ${stats.serversWithGiveaways}`,
      `**Last Detection:** ${stats.lastDetected ? formatTimestamp(stats.lastDetected) : "Never"}`,
    ];
    if (scrimStats) {
      sections.push(
        `**Total Events:** ${scrimStats.total}`,
        `**Active Events:** ${scrimStats.active}`,
        `**Scrims:** ${scrimStats.byType.scrim}`,
        `**Squid Games:** ${scrimStats.byType.squid_game}`,
        `**Gagaballs:** ${scrimStats.byType.gagaball}`
      );
    }
    await interaction.editReply(
      v2EditPayload(createV2Container("Tracker Stats", sections.join("\n"), 0x00aaff))
    );
  }
  private async activeCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    await deferReply(interaction, false);
    const id = giveawayPageId();
    this.giveawayPages.set(id, {
      id,
      userId: interaction.user.id,
      mode: "active",
      query: "",
      page: 0,
      createdAt: Date.now(),
    });
    await this.renderGiveawayPage(interaction, id);
  }
  private async recentCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    await deferReply(interaction, false);
    const id = giveawayPageId();
    this.giveawayPages.set(id, {
      id,
      userId: interaction.user.id,
      mode: "recent",
      query: "",
      page: 0,
      createdAt: Date.now(),
    });
    await this.renderGiveawayPage(interaction, id);
  }
  private async setchannelCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    if (!(await requireAdmin(interaction))) return;
    const channel = interaction.options.getChannel("channel", true);
    if (
      ![ChannelType.GuildText, ChannelType.GuildAnnouncement].includes(
        channel.type
      )
    ) {
      await replyV2Text(interaction, "Invalid channel", "Choose a text channel.");
      return;
    }
    (CONFIG as any).trackerChannelId = channel.id;
    await replyV2Text(interaction, "Notification channel updated", `Notification channel set to ${channel}.`);
  }
  private async resetCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    if (!(await requireAdmin(interaction))) return;
    await deferReply(interaction, true);
    await resetDatabase();
    await editV2Text(interaction, "Database reset", "Database reset complete.");
  }
  private async statusCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    if (!(await requireAdmin(interaction))) return;
    await deferReply(interaction, false);
    const stats = await getStats();
    const totalEver = await getTotalDetected();
    const embed = new EmbedBuilder()
      .setColor(0x2ecc71)
      .setTitle("System Status")
      .addFields(
        {
          name: "Total Giveaways",
          value: String(totalEver),
          inline: true,
        },
        {
          name: "Active",
          value: String(stats.activeGiveaways),
          inline: true,
        },
        {
          name: "Servers",
          value: String(stats.serversWithGiveaways),
          inline: true,
        },
        {
          name: "Notification Channel",
          value: `<#${CONFIG.trackerChannelId}>`,
          inline: false,
        }
      );
    await editV2Embed(interaction, embed);
  }
  private async metricsCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    if (!(await requireAdmin(interaction))) return;
    await deferReply(interaction, false);
    const m = this.metrics.getSnapshot();
    const embed = new EmbedBuilder()
      .setColor(0x3498db)
      .setTitle("Performance Metrics")
      .addFields(
        {
          name: "Giveaways Detected",
          value: String(m.giveawaysDetected),
          inline: true,
        },
        {
          name: "Notifications Sent",
          value: String(m.notificationsSent),
          inline: true,
        },
        {
          name: "Failed Notifications",
          value: String(m.notificationsFailed),
          inline: true,
        },
        {
          name: "Retry Attempts",
          value: String(m.retryAttempts),
          inline: true,
        },
        {
          name: "Avg Detection to Notify",
          value: `${m.avgDetectionLatency}ms`,
          inline: true,
        },
        {
          name: "Avg Discord Latency",
          value: `${m.avgDiscordLatency}ms`,
          inline: true,
        }
      );
    await editV2Embed(interaction, embed);
  }
  private async helpCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    await deferReply(interaction, false);
    const embed = new EmbedBuilder()
      .setColor(0x9b59b6)
      .setTitle("Commands")
      .addFields(
        { name: "/stats", value: "Tracker statistics", inline: false },
        { name: "/active", value: "Active giveaways", inline: false },
        { name: "/recent", value: "Recently detected giveaways", inline: false },
        {
          name: "/status",
          value: "System status",
          inline: false,
        },
        {
          name: "/metrics",
          value: "Performance metrics",
          inline: false,
        },
        {
          name: "/setchannel",
          value: "Set the notification channel",
          inline: false,
        },
        {
          name: "/reset",
          value: "Reset the database",
          inline: false,
        },
        {
          name: "/revoke",
          value: "Revoke premium access",
          inline: false,
        },
        {
          name: "/giveawaytrack",
          value:
            "`add`, `remove`, `list`, `clear` — manage giveaway filters",
          inline: false,
        },
        {
          name: "/eventtrack",
          value:
            "`add`, `remove`, `list`, `clear` — manage event filters",
          inline: false,
        }
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "/vrfs",
// [VRFS ARCHIVED]           value:
// [VRFS ARCHIVED]             "`player`, `locker`, `item`, `market`, `creator`, `stats`, `status`",
// [VRFS ARCHIVED]           inline: false,
// [VRFS ARCHIVED]         }
      );
    await editV2Embed(interaction, embed);
  }
  private async purgeCommand(
    interaction: ChatInputCommandInteraction<CacheType>
  ) {
    if (!(await requireAdmin(interaction))) return;
    const amount = interaction.options.getInteger("amount") || 50;
    await deferReply(interaction, true);
    const channel = interaction.channel as TextChannel;
    if (!channel) return;
    try {
      const messages = await channel.messages.fetch({ limit: 100 });
      const botMessages = messages.filter(
        (m) => m.author.id === this.client.user?.id
      );
      const toDelete = botMessages.first(amount);
      if (toDelete.length === 0) {
        await editV2Text(interaction, "Purge", "Nothing to delete.");
        return;
      }
      await channel.bulkDelete(toDelete, true);
      await editV2Text(
        interaction,
        "Purge complete",
        `Deleted ${toDelete.length} message${toDelete.length === 1 ? "" : "s"}.`
      );
    } catch {
      await editV2Text(interaction, "Purge failed", "Failed to delete messages.");
    }
  }
  private async sendLicensePanel(): Promise<void> {
    const panelChannelId = process.env.LICENSE_PANEL_CHANNEL_ID;
    if (!panelChannelId) return;
    const channel = this.client.channels.cache.get(
      panelChannelId
    ) as TextChannel | undefined;
    if (!channel) return;

    try {
      const messages = await channel.messages.fetch({ limit: 100 });
      const oldIds = new Set([
        "license_activate",
        "activate_premium",
        "check_premium",
        "generate_key",
        "list_keys",
        "refresh_stats",
      ]);
      const hasPanelButton = (component: any): boolean => {
        if (!component) return false;
        if (oldIds.has(component.customId)) return true;
        return Array.isArray(component.components)
          ? component.components.some(hasPanelButton)
          : false;
      };
      for (const message of messages.values()) {
        if (message.author?.id !== this.client.user?.id) continue;
        const legacy = message.embeds?.some(
          (embed: any) =>
            embed?.title === "Premium Access" ||
            embed?.title === "License Panel"
        );
        const v2 = Array.isArray(message.components)
          ? message.components.some(hasPanelButton)
          : false;
        if (legacy || v2) await message.delete().catch(() => {});
      }

      const container = createV2Container(
        "Premium Access",
        "Activate your premium access with a license key or check your premium status!",
        0x5865f2
      );
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder()
          .setCustomId("activate_premium")
          .setLabel("Activate Premium")
          .setStyle(ButtonStyle.Primary),
        new ButtonBuilder()
          .setCustomId("check_premium")
          .setLabel("Check Premium")
          .setStyle(ButtonStyle.Secondary)
      );
      container.addActionRowComponents(row);
      await channel.send({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
      logger.info("License panel sent", { channelId: panelChannelId });
    } catch (error) {
      logger.error("Failed to send license panel", {
        error: formatError(error),
      });
    }
  }

  private async sendPremiumPanel(): Promise<void> {
    const panelChannelId = process.env.PREMIUM_PANEL_CHANNEL_ID;
    if (!panelChannelId) return;
    const channel = this.client.channels.cache.get(
      panelChannelId
    ) as TextChannel | undefined;
    if (!channel) return;

    try {
      const messages = await channel.messages.fetch({ limit: 100 });
      const hasPanelButton = (component: any): boolean => {
        if (!component) return false;
        if (component.customId === "premium_autojoiner") return true;
        return Array.isArray(component.components)
          ? component.components.some(hasPanelButton)
          : false;
      };
      for (const message of messages.values()) {
        if (message.author?.id !== this.client.user?.id) continue;
        const legacy = message.embeds?.some(
          (embed: any) => embed?.title === "Premium Panel"
        );
        const v2 = Array.isArray(message.components)
          ? message.components.some(hasPanelButton)
          : false;
        if (legacy || v2) await message.delete().catch(() => {});
      }

      const container = createV2Container(
        "Premium Panel",
        "Premium settings",
        0x5865f2
      );
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder()
          .setCustomId("premium_autojoiner")
          .setLabel("AutoJoiner")
          .setStyle(ButtonStyle.Primary)
      );
      container.addActionRowComponents(row);
      await channel.send({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
      logger.info("Premium panel sent", { channelId: panelChannelId });
    } catch (error) {
      logger.error("Failed to send premium panel", {
        error: formatError(error),
      });
    }
  }

  private async handleGuildMemberUpdate(
    oldMember: any,
    newMember: any
  ): Promise<void> {
    const guildId = process.env.GUILD_ID;
    if (!guildId || newMember.guild.id !== guildId) return;
    const boosterRoleId = process.env.BOOSTER_ROLE_ID;
    const premiumRoleId = process.env.PREMIUM_ROLE_ID;
    if (!boosterRoleId || !premiumRoleId) return;
    const hadBooster = oldMember.roles.cache.has(boosterRoleId);
    const hasBooster = newMember.roles.cache.has(boosterRoleId);
    if (!hadBooster && hasBooster) {
      try {
        await newMember.roles.add(premiumRoleId);
        await setPremiumUser(newMember.id, guildId, "booster");
        await setBoosterPremium(newMember.id, guildId, true);
      } catch (error) {
        logger.error("Failed to add premium role to booster", {
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
      } catch (error) {
        logger.error("Failed to remove premium data", {
          userId: newMember.id,
          error: String(error),
        });
      }
    }
  }
  private async handleGuildMemberAdd(member: any): Promise<void> {
    const guildId = process.env.GUILD_ID;
    if (!guildId || member.guild.id !== guildId) return;
    const boosterRoleId = process.env.BOOSTER_ROLE_ID;
    const premiumRoleId = process.env.PREMIUM_ROLE_ID;
    if (!boosterRoleId || !premiumRoleId) return;
    if (!member.roles.cache.has(boosterRoleId)) return;
    try {
      const existing = await getPremiumUser(member.id, guildId);
      if (!existing || !existing.isPremium) {
        await member.roles.add(premiumRoleId);
        await setPremiumUser(member.id, guildId, "booster");
        await setBoosterPremium(member.id, guildId, true);
      }
    } catch (error) {
      logger.error("Failed to add premium role to booster on join", {
        userId: member.id,
        error: String(error),
      });
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
        if (!member.roles.cache.has(boosterRoleId)) continue;
        const existing = await getPremiumUser(member.id, guildId);
        if (!existing || !existing.isPremium) {
          try {
            await member.roles.add(premiumRoleId);
            await setPremiumUser(member.id, guildId, "booster");
            await setBoosterPremium(member.id, guildId, true);
            count++;
          } catch (error) {
            logger.error("Failed to add premium role", {
              userId: member.id,
              error: String(error),
            });
          }
        }
      }
      if (count > 0) {
        logger.info(`Added premium role to ${count} existing boosters`);
      }
    } catch (error) {
      logger.error("Failed to assign premium to existing boosters", {
        error: String(error),
      });
    }
  }
  private async updatePresence(force = false): Promise<void> {
    const now = Date.now();
    if (!force && now - this.lastPresenceUpdate < 30_000) return;
    if (this.presenceUpdateInFlight) return this.presenceUpdateInFlight;
    this.presenceUpdateInFlight = (async () => {
      const totalEver = await getTotalDetected();
      this.client.user?.setPresence({
        activities: [{ name: `${totalEver.toLocaleString()} giveaways tracked`, type: ActivityType.Watching }],
        status: "online",
      });
      this.lastPresenceUpdate = Date.now();
    })().catch((error) => {
      logger.debug("Presence update failed", { error: formatError(error) });
    }).finally(() => {
      this.presenceUpdateInFlight = null;
    });
    return this.presenceUpdateInFlight;
  }
  private async purgeAndUpdatePresence() {
    const removed = await purgeEndedGiveaways();
    if (removed.length > 0) {
      const trackerChannel = this.client.channels.cache.get(
        CONFIG.trackerChannelId
      ) as TextChannel | undefined;
      for (const giveaway of removed) {
        const notifMsgId = giveaway.notificationMessageId;
        if (notifMsgId && trackerChannel) {
          const msg = await trackerChannel.messages
            .fetch(notifMsgId)
            .catch(() => null);
          if (msg) {
            const guild = this.client.guilds.cache.get(giveaway.guildId);
            const inviteUrl = this.getFastInviteUrl(
              giveaway.guildId,
              giveaway.channelId,
              giveaway.inviteUrl
            );
            const container = buildGiveawayNotificationContainer(
              {
                ...giveaway,
                guildName: giveaway.guildName || guild?.name || "Unknown",
                guildIcon: giveaway.guildIcon || guild?.iconURL({ size: 512 }) || null,
                guildBanner: giveaway.guildBanner || guild?.bannerURL({ size: 1024 }) || null,
                memberCount: giveaway.memberCount ?? guild?.memberCount ?? null,
                inviteUrl,
              } as GiveawayData & Record<string, any>,
              "ended"
            );
            await msg.edit({
              components: [container],
              flags: MessageFlags.IsComponentsV2,
            }).catch(() => {});
          }
        }
      }
      await this.updatePresence();
    }
  }
  private createGiveawayPageButtons(state: GiveawayPageState, totalPages: number): ActionRowBuilder<ButtonBuilder> {
    return new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder()
        .setCustomId(`giveaway_page:${state.id}:prev`)
        .setLabel("⬅️")
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(state.page <= 0),
      new ButtonBuilder()
        .setCustomId(`giveaway_page:${state.id}:current`)
        .setLabel(`${state.page + 1} / ${totalPages}`)
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(true),
      new ButtonBuilder()
        .setCustomId(`giveaway_page:${state.id}:next`)
        .setLabel("➡️")
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(state.page >= totalPages - 1),
      new ButtonBuilder()
        .setCustomId(`giveaway_page:${state.id}:search`)
        .setLabel("Search")
        .setStyle(ButtonStyle.Primary)
    );
  }

  private async renderGiveawayPage(
    interaction: ChatInputCommandInteraction<CacheType> | any,
    pageId: string
  ): Promise<void> {
    const state = this.giveawayPages.get(pageId);
    if (!state) {
      const container = createV2Container("Results expired", "Run the command again.", 0xe74c3c);
      if (interaction.isChatInputCommand?.() && interaction.deferred) {
        await interaction.editReply(v2EditPayload(container));
      } else if (interaction.deferred || interaction.replied) {
        await interaction.editReply(v2EditPayload(container));
      } else {
        await interaction.reply(v2ReplyPayload(container, true));
      }
      return;
    }

    const raw = state.mode === "active" ? await getActiveGiveaways(100) : await getAllGiveaways(100);
    const query = state.query.trim().toLowerCase();
    const filtered = query
      ? raw.filter((g: any) =>
          [g.prize, g.guildName, g.channelName]
            .filter(Boolean)
            .some((value) => String(value).toLowerCase().includes(query))
        )
      : raw;

    const pageSize = 5;
    const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
    state.page = Math.max(0, Math.min(state.page, totalPages - 1));
    const start = state.page * pageSize;
    const pageItems = filtered.slice(start, start + pageSize);

    const title = state.mode === "active" ? "Active Giveaways" : "Recent Giveaways";
    const header = query
      ? `${filtered.length.toLocaleString()} result${filtered.length === 1 ? "" : "s"} matching **${truncate(state.query, 80)}**`
      : `${filtered.length.toLocaleString()} result${filtered.length === 1 ? "" : "s"}`;

    const lines = pageItems.map((g: any, index: number) => {
      const position = start + index + 1;
      const ends = g.endsAt ? `<t:${Math.floor(g.endsAt / 1000)}:R>` : "Unknown";
      const jump = g.guildId && g.channelId && g.messageId
        ? `\n[Open Giveaway](https://discord.com/channels/${g.guildId}/${g.channelId}/${g.messageId})`
        : "";
      const status = g.status === "active" ? "Active" : "Ended";
      return `### ${position}. ${truncate(String(g.prize || "Unknown Prize"), 90)}\n**Server:** ${g.guildName || "Unknown"}\n**Channel:** #${g.channelName || "unknown"}\n**Status:** ${status}${state.mode === "active" ? `\n**Ends:** ${ends}` : `\n**Detected:** ${formatTimestamp(g.detectedAt)}`}${jump}`;
    });

    const body = `${header}\n\n${lines.join("\n\n") || "No giveaways matched your search."}`;
    const container = createV2Container(title, body, state.mode === "active" ? 0xffd700 : 0x5865f2);
    container.addActionRowComponents(this.createGiveawayPageButtons(state, totalPages));

    const payload = v2EditPayload(container);
    if (interaction.deferred || interaction.replied) {
      await interaction.editReply(payload);
    } else {
      await interaction.reply(v2ReplyPayload(container, true));
    }
  }

  private async handleGiveawayPageButton(interaction: ButtonInteraction): Promise<void> {
    const parts = interaction.customId.split(":");
    if (parts.length !== 3) return;
    const [, pageId, action] = parts;
    const state = this.giveawayPages.get(pageId);
    if (!state) {
      await replyV2Text(interaction as any, "Results expired", "Run the command again.");
      return;
    }
    if (state.userId !== interaction.user.id) {
      await replyV2Text(interaction as any, "Access denied", "Only the person who requested these results can navigate them.");
      return;
    }
    if (action === "search") {
      const modal = new ModalBuilder()
        .setCustomId(`giveaway_search_modal:${pageId}`)
        .setTitle(`Search ${state.mode === "active" ? "Active" : "Recent"} Giveaways`);
      const input = new TextInputBuilder()
        .setCustomId("query")
        .setLabel("Search prize, server, or channel")
        .setStyle(TextInputStyle.Short)
        .setPlaceholder("Example: Nitro, VRFS, giveaways")
        .setRequired(false)
        .setMaxLength(100);
      modal.addComponents(new ActionRowBuilder<TextInputBuilder>().addComponents(input));
      await interaction.showModal(modal);
      return;
    }
    if (action === "current") return;
    state.page += action === "next" ? 1 : -1;
    state.page = Math.max(0, state.page);
    await interaction.deferUpdate();
    await this.renderGiveawayPage(interaction, pageId);
  }

  private async handleGiveawaySearchModal(interaction: any): Promise<void> {
    const parts = String(interaction.customId).split(":");
    const pageId = parts[1];
    const state = this.giveawayPages.get(pageId);
    if (!state) {
      await replyV2Text(interaction as any, "Search expired", "Run the command again.");
      return;
    }
    if (state.userId !== interaction.user.id) {
      await replyV2Text(interaction as any, "Access denied", "Only the person who requested these results can search them.");
      return;
    }
    const query = interaction.fields.getTextInputValue("query").trim();
    const searchId = giveawayPageId();
    const nextState: GiveawayPageState = {
      ...state,
      id: searchId,
      query,
      page: 0,
      createdAt: Date.now(),
    };
    this.giveawayPages.set(searchId, nextState);
    await this.renderGiveawayPage(interaction, searchId);
  }

  // [VRFS ARCHIVED] VRFS page cleanup is disabled while the VRFS API is archived.
  private cleanupPages(): void {
    const cutoff = Date.now() - 10 * 60 * 1000;
    // [VRFS ARCHIVED] for (const [id, page] of this.vrfsPages) {
    // [VRFS ARCHIVED]   if (page.createdAt < cutoff) this.vrfsPages.delete(id);
    // [VRFS ARCHIVED] }
    for (const [id, page] of this.giveawayPages) {
      if (page.createdAt < cutoff) this.giveawayPages.delete(id);
    }
  }
// [VRFS ARCHIVED]   private createVRFSPage(
// [VRFS ARCHIVED]     userId: string,
// [VRFS ARCHIVED]     type: VRFSPage["type"],
// [VRFS ARCHIVED]     items: VRFSPage["items"],
// [VRFS ARCHIVED]     title: string,
// [VRFS ARCHIVED]     description?: string,
// [VRFS ARCHIVED]     pageSize = 10
// [VRFS ARCHIVED]   ): VRFSPage {
// [VRFS ARCHIVED]     const id = `${Date.now().toString(36)}${Math.random()
// [VRFS ARCHIVED]       .toString(36)
// [VRFS ARCHIVED]       .slice(2, 8)}`;
// [VRFS ARCHIVED]     const page: VRFSPage = {
// [VRFS ARCHIVED]       id,
// [VRFS ARCHIVED]       type,
// [VRFS ARCHIVED]       userId,
// [VRFS ARCHIVED]       createdAt: Date.now(),
// [VRFS ARCHIVED]       page: 0,
// [VRFS ARCHIVED]       pageSize,
// [VRFS ARCHIVED]       items,
// [VRFS ARCHIVED]       title,
// [VRFS ARCHIVED]       description,
// [VRFS ARCHIVED]     };
// [VRFS ARCHIVED]     this.vrfsPages.set(id, page);
// [VRFS ARCHIVED]     return page;
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async handleVRFSPageButton(
// [VRFS ARCHIVED]     interaction: ButtonInteraction
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     const parts = interaction.customId.split(":");
// [VRFS ARCHIVED]     if (parts.length !== 3) return;
// [VRFS ARCHIVED]     const [, pageId, direction] = parts;
// [VRFS ARCHIVED]     const session = this.vrfsPages.get(pageId);
// [VRFS ARCHIVED]     if (!session) {
// [VRFS ARCHIVED]       await interaction.reply({
// [VRFS ARCHIVED]         content: "This result has expired. Run the command again.",
// [VRFS ARCHIVED]         ephemeral: true,
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (session.userId !== interaction.user.id) {
// [VRFS ARCHIVED]       await interaction.reply({
// [VRFS ARCHIVED]         content: "Only the person who requested this result can navigate it.",
// [VRFS ARCHIVED]         ephemeral: true,
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (direction === "current") return;
// [VRFS ARCHIVED]     const next =
// [VRFS ARCHIVED]       direction === "next"
// [VRFS ARCHIVED]         ? session.page + 1
// [VRFS ARCHIVED]         : Math.max(0, session.page - 1);
// [VRFS ARCHIVED]     const totalPages = Math.max(
// [VRFS ARCHIVED]       1,
// [VRFS ARCHIVED]       Math.ceil(session.items.length / session.pageSize)
// [VRFS ARCHIVED]     );
// [VRFS ARCHIVED]     if (next < 0 || next >= totalPages) return;
// [VRFS ARCHIVED]     session.page = next;
// [VRFS ARCHIVED]     const page =
// [VRFS ARCHIVED]       session.type === "catalog"
// [VRFS ARCHIVED]         ? createCatalogPage(session)
// [VRFS ARCHIVED]         : session.type === "marketplace"
// [VRFS ARCHIVED]         ? createMarketplacePage(session)
// [VRFS ARCHIVED]         : createLockerPage(session);
// [VRFS ARCHIVED]     await interaction.update(page);
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     const sub = interaction.options.getSubcommand();
// [VRFS ARCHIVED]     if (sub === "player") {
// [VRFS ARCHIVED]       await this.vrfsPlayerCommand(interaction);
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (sub === "locker") {
// [VRFS ARCHIVED]       await this.vrfsLockerCommand(interaction);
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (sub === "item") {
// [VRFS ARCHIVED]       await this.vrfsItemCommand(interaction);
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (sub === "market") {
// [VRFS ARCHIVED]       await this.vrfsMarketCommand(interaction);
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (sub === "creator") {
// [VRFS ARCHIVED]       await this.vrfsCreatorCommand(interaction);
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (sub === "stats") {
// [VRFS ARCHIVED]       await this.vrfsStatsCommand(interaction);
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]     if (sub === "status") {
// [VRFS ARCHIVED]       await this.vrfsStatusCommand(interaction);
// [VRFS ARCHIVED]       return;
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsPlayerCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     const id = interaction.options.getInteger("id", true);
// [VRFS ARCHIVED]     await deferReply(interaction, false);
// [VRFS ARCHIVED]     try {
// [VRFS ARCHIVED]       const player = await getPlayer(id);
// [VRFS ARCHIVED]       const username = player.username;
// [VRFS ARCHIVED]       const profile = player.profile;
// [VRFS ARCHIVED]       const outfits = player.outfits ?? [];
// [VRFS ARCHIVED]       const embed = createPlayerEmbed(id, username, profile, outfits);
// [VRFS ARCHIVED]       await editV2Embed(interaction, embed);
// [VRFS ARCHIVED]     } catch (error) {
// [VRFS ARCHIVED]       logger.warn("Player lookup failed", {
// [VRFS ARCHIVED]         id,
// [VRFS ARCHIVED]         error: formatError(error),
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       await editV2Text(interaction, "Player lookup", "That player could not be found.", 0xe74c3c);
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsLockerCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     const id = interaction.options.getInteger("id", true);
// [VRFS ARCHIVED]     const query = interaction.options.getString("item")?.trim() || null;
// [VRFS ARCHIVED]     await deferReply(interaction, false);
// [VRFS ARCHIVED]     try {
// [VRFS ARCHIVED]       const catalog = await getCatalog();
// [VRFS ARCHIVED]       if (!query) {
// [VRFS ARCHIVED]         await interaction.editReply(
// [VRFS ARCHIVED]           v2EditPayload(
// [VRFS ARCHIVED]             createV2Container(
// [VRFS ARCHIVED]               "Locker",
// [VRFS ARCHIVED]               `Checking the collection for ID \`${id}\`.\n\nScanning ${catalog.length.toLocaleString()} items.`,
// [VRFS ARCHIVED]               0x5865f2
// [VRFS ARCHIVED]             )
// [VRFS ARCHIVED]           )
// [VRFS ARCHIVED]         );
// [VRFS ARCHIVED]         const ownership = await checkOwnership(id, catalog.map(getSku), {
// [VRFS ARCHIVED]           batchSize: 250,
// [VRFS ARCHIVED]           minBatchSize: 5,
// [VRFS ARCHIVED]           maxBatchSize: 500,
// [VRFS ARCHIVED]           delayMs: 150,
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         const player = await getUsername(id);
// [VRFS ARCHIVED]         const summary = createLockerSummaryEmbed(
// [VRFS ARCHIVED]           id,
// [VRFS ARCHIVED]           player.username,
// [VRFS ARCHIVED]           catalog,
// [VRFS ARCHIVED]           ownership
// [VRFS ARCHIVED]         );
// [VRFS ARCHIVED]         const ownedItems = catalog.filter((item) =>
// [VRFS ARCHIVED]           ownership.owned.includes(getSku(item))
// [VRFS ARCHIVED]         );
// [VRFS ARCHIVED]         const session = this.createVRFSPage(
// [VRFS ARCHIVED]           interaction.user.id,
// [VRFS ARCHIVED]           "locker",
// [VRFS ARCHIVED]           ownedItems,
// [VRFS ARCHIVED]           "Owned Items",
// [VRFS ARCHIVED]           `ID \`${id}\` · ${ownedItems.length.toLocaleString()} owned items`,
// [VRFS ARCHIVED]           10
// [VRFS ARCHIVED]         );
// [VRFS ARCHIVED]         const summaryContainer = embedToV2Container(summary);
// [VRFS ARCHIVED]         const lockerResult = paginate(ownedItems, session.page, session.pageSize);
// [VRFS ARCHIVED]         const lockerLines = lockerResult.items.map((item: VRFSItem, index: number) => {
// [VRFS ARCHIVED]           const position = lockerResult.page * session.pageSize + index + 1;
// [VRFS ARCHIVED]           return `**${position}. ${truncate(getItemName(item), 80)}**\n\`${truncate(
// [VRFS ARCHIVED]             getSku(item),
// [VRFS ARCHIVED]             100
// [VRFS ARCHIVED]           )}\` · ${getSection(item)} · ${formatItemPrice(item)}`;
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         addV2Text(
// [VRFS ARCHIVED]           summaryContainer,
// [VRFS ARCHIVED]           `### Owned Items\n${truncate(
// [VRFS ARCHIVED]             lockerLines.join("\n\n") || "No owned items.",
// [VRFS ARCHIVED]             4096
// [VRFS ARCHIVED]           )}`
// [VRFS ARCHIVED]         );
// [VRFS ARCHIVED]         if (lockerResult.totalPages > 1) {
// [VRFS ARCHIVED]           summaryContainer.addActionRowComponents(
// [VRFS ARCHIVED]             createPaginationRow(session.id, lockerResult.page, lockerResult.totalPages)
// [VRFS ARCHIVED]           );
// [VRFS ARCHIVED]         }
// [VRFS ARCHIVED]         await interaction.editReply(v2EditPayload(summaryContainer));
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const matches = await searchCatalog(query, 25);
// [VRFS ARCHIVED]       if (!matches.length) {
// [VRFS ARCHIVED]         await editV2Text(interaction, "Item search", `No items matched **${truncate(query, 100)}**.`, 0xe74c3c);
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       if (matches.length === 1) {
// [VRFS ARCHIVED]         const sku = getSku(matches[0]);
// [VRFS ARCHIVED]         const ownership = await checkOwnership(id, [sku], {
// [VRFS ARCHIVED]           batchSize: 1,
// [VRFS ARCHIVED]           minBatchSize: 1,
// [VRFS ARCHIVED]           maxBatchSize: 1,
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         const embed = createItemEmbed(matches[0]);
// [VRFS ARCHIVED]         embed.addFields({
// [VRFS ARCHIVED]           name: "Ownership",
// [VRFS ARCHIVED]           value:
// [VRFS ARCHIVED]             ownership.results[sku] === true
// [VRFS ARCHIVED]               ? `Owned by ID \`${id}\``
// [VRFS ARCHIVED]               : ownership.results[sku] === false
// [VRFS ARCHIVED]               ? `Not owned by ID \`${id}\``
// [VRFS ARCHIVED]               : "Could not be confirmed",
// [VRFS ARCHIVED]           inline: false,
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         await editV2Embed(interaction, embed);
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const session = this.createVRFSPage(
// [VRFS ARCHIVED]         interaction.user.id,
// [VRFS ARCHIVED]         "catalog",
// [VRFS ARCHIVED]         matches,
// [VRFS ARCHIVED]         "Locker Search",
// [VRFS ARCHIVED]         `Results for **${truncate(query, 100)}**`,
// [VRFS ARCHIVED]         10
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       const page = createCatalogPage(session);
// [VRFS ARCHIVED]       await interaction.editReply(page);
// [VRFS ARCHIVED]     } catch (error) {
// [VRFS ARCHIVED]       logger.warn("Locker request failed", {
// [VRFS ARCHIVED]         id,
// [VRFS ARCHIVED]         error: formatError(error),
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       await editV2Text(interaction, "Locker lookup", "The locker could not be loaded right now.", 0xe74c3c);
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsItemCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     const query = interaction.options.getString("query", true).trim();
// [VRFS ARCHIVED]     await deferReply(interaction, false);
// [VRFS ARCHIVED]     try {
// [VRFS ARCHIVED]       const item = await getCatalogItem(query);
// [VRFS ARCHIVED]       if (item) {
// [VRFS ARCHIVED]         await interaction.editReply({
// [VRFS ARCHIVED]           components: [embedToV2Container(createItemEmbed(item))],
// [VRFS ARCHIVED]           flags: MessageFlags.IsComponentsV2,
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const results = await searchCatalog(query, 50);
// [VRFS ARCHIVED]       if (!results.length) {
// [VRFS ARCHIVED]         await editV2Text(interaction, "Item search", `No items matched **${truncate(query, 100)}**.`, 0xe74c3c);
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       if (results.length === 1) {
// [VRFS ARCHIVED]         await interaction.editReply({
// [VRFS ARCHIVED]           components: [embedToV2Container(createItemEmbed(results[0]))],
// [VRFS ARCHIVED]           flags: MessageFlags.IsComponentsV2,
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const session = this.createVRFSPage(
// [VRFS ARCHIVED]         interaction.user.id,
// [VRFS ARCHIVED]         "catalog",
// [VRFS ARCHIVED]         results,
// [VRFS ARCHIVED]         "Item Search",
// [VRFS ARCHIVED]         `Results for **${truncate(query, 100)}**`,
// [VRFS ARCHIVED]         10
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       await interaction.editReply(createCatalogPage(session));
// [VRFS ARCHIVED]     } catch (error) {
// [VRFS ARCHIVED]       logger.warn("Item lookup failed", {
// [VRFS ARCHIVED]         query,
// [VRFS ARCHIVED]         error: formatError(error),
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       await editV2Text(interaction, "Item catalogue", "The item catalogue could not be loaded right now.", 0xe74c3c);
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsMarketCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     const query = interaction.options.getString("query", true).trim();
// [VRFS ARCHIVED]     await deferReply(interaction, false);
// [VRFS ARCHIVED]     try {
// [VRFS ARCHIVED]       const item = await getMarketplaceItem(query);
// [VRFS ARCHIVED]       if (item) {
// [VRFS ARCHIVED]         await interaction.editReply({
// [VRFS ARCHIVED]           components: [embedToV2Container(createMarketplaceEmbed(item))],
// [VRFS ARCHIVED]           flags: MessageFlags.IsComponentsV2,
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const results = await searchMarketplace(query, 50);
// [VRFS ARCHIVED]       if (!results.length) {
// [VRFS ARCHIVED]         await editV2Text(interaction, "Marketplace search", `No marketplace items matched **${truncate(query, 100)}**.`, 0xe74c3c);
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       if (results.length === 1) {
// [VRFS ARCHIVED]         await interaction.editReply({
// [VRFS ARCHIVED]           components: [embedToV2Container(createMarketplaceEmbed(results[0]))],
// [VRFS ARCHIVED]           flags: MessageFlags.IsComponentsV2,
// [VRFS ARCHIVED]         });
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const session = this.createVRFSPage(
// [VRFS ARCHIVED]         interaction.user.id,
// [VRFS ARCHIVED]         "marketplace",
// [VRFS ARCHIVED]         results,
// [VRFS ARCHIVED]         "Marketplace Search",
// [VRFS ARCHIVED]         `Results for **${truncate(query, 100)}**`,
// [VRFS ARCHIVED]         10
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       await interaction.editReply(createMarketplacePage(session));
// [VRFS ARCHIVED]     } catch (error) {
// [VRFS ARCHIVED]       logger.warn("Marketplace lookup failed", {
// [VRFS ARCHIVED]         query,
// [VRFS ARCHIVED]         error: formatError(error),
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       await editV2Text(interaction, "Marketplace", "The marketplace could not be loaded right now.", 0xe74c3c);
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsCreatorCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     const name = interaction.options.getString("name", true).trim().toLowerCase();
// [VRFS ARCHIVED]     await deferReply(interaction, false);
// [VRFS ARCHIVED]     try {
// [VRFS ARCHIVED]       const marketplace = await getMarketplace();
// [VRFS ARCHIVED]       const matches = marketplace.filter((item) =>
// [VRFS ARCHIVED]         getMarketplaceCreatorName(item).toLowerCase().includes(name)
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       if (!matches.length) {
// [VRFS ARCHIVED]         await editV2Text(interaction, "Creator search", `No creator matched **${truncate(name, 100)}**.`, 0xe74c3c);
// [VRFS ARCHIVED]         return;
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const creator = getMarketplaceCreatorName(matches[0]);
// [VRFS ARCHIVED]       const creatorId = getMarketplaceCreatorId(matches[0]);
// [VRFS ARCHIVED]       const active = matches.filter(getMarketplaceActive).length;
// [VRFS ARCHIVED]       const totalOwners = matches.reduce(
// [VRFS ARCHIVED]         (sum, item) => sum + getMarketplaceOwners(item),
// [VRFS ARCHIVED]         0
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       const typeCounts = new Map<string, number>();
// [VRFS ARCHIVED]       for (const item of matches) {
// [VRFS ARCHIVED]         const type = marketType(item);
// [VRFS ARCHIVED]         typeCounts.set(type, (typeCounts.get(type) ?? 0) + 1);
// [VRFS ARCHIVED]       }
// [VRFS ARCHIVED]       const embed = createEmbed(
// [VRFS ARCHIVED]         creator,
// [VRFS ARCHIVED]         0x5865f2,
// [VRFS ARCHIVED]         `Marketplace creator${creatorId ? ` · ID \`${creatorId}\`` : ""}`
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       embed.addFields(
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Items",
// [VRFS ARCHIVED]           value: matches.length.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Available",
// [VRFS ARCHIVED]           value: active.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Total Owners",
// [VRFS ARCHIVED]           value: totalOwners.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Categories",
// [VRFS ARCHIVED]           value:
// [VRFS ARCHIVED]             [...typeCounts.entries()]
// [VRFS ARCHIVED]               .sort((a, b) => b[1] - a[1])
// [VRFS ARCHIVED]               .map(([type, count]) => `**${type}** — ${count}`)
// [VRFS ARCHIVED]               .join("\n") || "None",
// [VRFS ARCHIVED]           inline: false,
// [VRFS ARCHIVED]         }
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       const preview = matches.slice(0, 10).map((item, index) => {
// [VRFS ARCHIVED]         return `**${index + 1}. ${truncate(
// [VRFS ARCHIVED]           String(item.title ?? item.name ?? getSku(item)),
// [VRFS ARCHIVED]           70
// [VRFS ARCHIVED]         )}**\n#${item.id} · ${marketType(item)} · ${formatNumber(
// [VRFS ARCHIVED]           getMarketplaceOwners(item)
// [VRFS ARCHIVED]         )} owners`;
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       embed.addFields({
// [VRFS ARCHIVED]         name: "Items",
// [VRFS ARCHIVED]         value: truncate(preview.join("\n\n") || "None", 4096),
// [VRFS ARCHIVED]         inline: false,
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       const image = getMarketplaceImage(matches[0]);
// [VRFS ARCHIVED]       if (image) embed.setImage(image);
// [VRFS ARCHIVED]       await editV2Embed(interaction, embed);
// [VRFS ARCHIVED]     } catch (error) {
// [VRFS ARCHIVED]       logger.warn("Creator lookup failed", {
// [VRFS ARCHIVED]         name,
// [VRFS ARCHIVED]         error: formatError(error),
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       await editV2Text(interaction, "Creator lookup", "The creator information could not be loaded right now.", 0xe74c3c);
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsStatsCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     await deferReply(interaction, false);
// [VRFS ARCHIVED]     try {
// [VRFS ARCHIVED]       const [catalog, marketplace] = await Promise.all([
// [VRFS ARCHIVED]         getCatalog(),
// [VRFS ARCHIVED]         getMarketplace(),
// [VRFS ARCHIVED]       ]);
// [VRFS ARCHIVED]       const free = catalog.filter(isItemFree).length;
// [VRFS ARCHIVED]       const paid = Math.max(0, catalog.length - free);
// [VRFS ARCHIVED]       const active = marketplace.filter(getMarketplaceActive).length;
// [VRFS ARCHIVED]       const creators = new Set(
// [VRFS ARCHIVED]         marketplace
// [VRFS ARCHIVED]           .map(getMarketplaceCreatorId)
// [VRFS ARCHIVED]           .filter((value) => value.length > 0)
// [VRFS ARCHIVED]       ).size;
// [VRFS ARCHIVED]       const owners = marketplace.reduce(
// [VRFS ARCHIVED]         (sum, item) => sum + getMarketplaceOwners(item),
// [VRFS ARCHIVED]         0
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       const sections = new Set(catalog.map(getSection)).size;
// [VRFS ARCHIVED]       const embed = createEmbed(
// [VRFS ARCHIVED]         "VRFS",
// [VRFS ARCHIVED]         0x5865f2,
// [VRFS ARCHIVED]         "Current game data overview."
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       embed.addFields(
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Catalogue Items",
// [VRFS ARCHIVED]           value: catalog.length.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Free",
// [VRFS ARCHIVED]           value: free.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Paid",
// [VRFS ARCHIVED]           value: paid.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Sections",
// [VRFS ARCHIVED]           value: sections.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Marketplace Items",
// [VRFS ARCHIVED]           value: marketplace.length.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Available",
// [VRFS ARCHIVED]           value: active.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Creators",
// [VRFS ARCHIVED]           value: creators.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Total Owners",
// [VRFS ARCHIVED]           value: owners.toLocaleString(),
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         }
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       await editV2Embed(interaction, embed);
// [VRFS ARCHIVED]     } catch (error) {
// [VRFS ARCHIVED]       logger.warn("VRFS stats failed", {
// [VRFS ARCHIVED]         error: formatError(error),
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       await editV2Text(interaction, "Statistics", "The statistics could not be loaded right now.", 0xe74c3c);
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
// [VRFS ARCHIVED]   private async vrfsStatusCommand(
// [VRFS ARCHIVED]     interaction: ChatInputCommandInteraction<CacheType>
// [VRFS ARCHIVED]   ): Promise<void> {
// [VRFS ARCHIVED]     await deferReply(interaction, false);
// [VRFS ARCHIVED]     try {
// [VRFS ARCHIVED]       const [catalog, marketplace] = await Promise.all([
// [VRFS ARCHIVED]         getCatalog(),
// [VRFS ARCHIVED]         getMarketplace(),
// [VRFS ARCHIVED]       ]);
// [VRFS ARCHIVED]       const embed = createEmbed(
// [VRFS ARCHIVED]         "VRFS Status",
// [VRFS ARCHIVED]         0x2ecc71,
// [VRFS ARCHIVED]         "Game data services are responding normally."
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       embed.addFields(
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Catalogue",
// [VRFS ARCHIVED]           value: `${catalog.length.toLocaleString()} items`,
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Marketplace",
// [VRFS ARCHIVED]           value: `${marketplace.length.toLocaleString()} items`,
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         },
// [VRFS ARCHIVED]         {
// [VRFS ARCHIVED]           name: "Availability",
// [VRFS ARCHIVED]           value: "Online",
// [VRFS ARCHIVED]           inline: true,
// [VRFS ARCHIVED]         }
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       await editV2Embed(interaction, embed);
// [VRFS ARCHIVED]     } catch (error) {
// [VRFS ARCHIVED]       logger.warn("VRFS status failed", {
// [VRFS ARCHIVED]         error: formatError(error),
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       const embed = createEmbed(
// [VRFS ARCHIVED]         "VRFS Status",
// [VRFS ARCHIVED]         0xe74c3c,
// [VRFS ARCHIVED]         "Game data services are currently unavailable."
// [VRFS ARCHIVED]       );
// [VRFS ARCHIVED]       embed.addFields({
// [VRFS ARCHIVED]         name: "Availability",
// [VRFS ARCHIVED]         value: "Unavailable",
// [VRFS ARCHIVED]         inline: true,
// [VRFS ARCHIVED]       });
// [VRFS ARCHIVED]       await editV2Embed(interaction, embed);
// [VRFS ARCHIVED]     }
// [VRFS ARCHIVED]   }
  private async registerCommands(): Promise<void> {
    if (this.commandsRegistered) return;
// [VRFS ARCHIVED]     const vrfsCommand = new SlashCommandBuilder()
// [VRFS ARCHIVED]       .setName("vrfs")
// [VRFS ARCHIVED]       .setDescription("Player, locker, item and marketplace tools")
// [VRFS ARCHIVED]       .addSubcommand((sub) =>
// [VRFS ARCHIVED]         sub
// [VRFS ARCHIVED]           .setName("player")
// [VRFS ARCHIVED]           .setDescription("View a player")
// [VRFS ARCHIVED]           .addIntegerOption((opt) =>
// [VRFS ARCHIVED]             opt
// [VRFS ARCHIVED]               .setName("id")
// [VRFS ARCHIVED]               .setDescription("Player ID")
// [VRFS ARCHIVED]               .setRequired(true)
// [VRFS ARCHIVED]               .setMinValue(1)
// [VRFS ARCHIVED]           )
// [VRFS ARCHIVED]       )
// [VRFS ARCHIVED]       .addSubcommand((sub) =>
// [VRFS ARCHIVED]         sub
// [VRFS ARCHIVED]           .setName("locker")
// [VRFS ARCHIVED]           .setDescription("View a player's locker")
// [VRFS ARCHIVED]           .addIntegerOption((opt) =>
// [VRFS ARCHIVED]             opt
// [VRFS ARCHIVED]               .setName("id")
// [VRFS ARCHIVED]               .setDescription("Player ID")
// [VRFS ARCHIVED]               .setRequired(true)
// [VRFS ARCHIVED]               .setMinValue(1)
// [VRFS ARCHIVED]           )
// [VRFS ARCHIVED]           .addStringOption((opt) =>
// [VRFS ARCHIVED]             opt
// [VRFS ARCHIVED]               .setName("item")
// [VRFS ARCHIVED]               .setDescription("Optional item name or SKU")
// [VRFS ARCHIVED]               .setRequired(false)
// [VRFS ARCHIVED]           )
// [VRFS ARCHIVED]       )
// [VRFS ARCHIVED]       .addSubcommand((sub) =>
// [VRFS ARCHIVED]         sub
// [VRFS ARCHIVED]           .setName("item")
// [VRFS ARCHIVED]           .setDescription("Look up an item")
// [VRFS ARCHIVED]           .addStringOption((opt) =>
// [VRFS ARCHIVED]             opt
// [VRFS ARCHIVED]               .setName("query")
// [VRFS ARCHIVED]               .setDescription("Item name, SKU or ID")
// [VRFS ARCHIVED]               .setRequired(true)
// [VRFS ARCHIVED]           )
// [VRFS ARCHIVED]       )
// [VRFS ARCHIVED]       .addSubcommand((sub) =>
// [VRFS ARCHIVED]         sub
// [VRFS ARCHIVED]           .setName("market")
// [VRFS ARCHIVED]           .setDescription("Look up a marketplace item")
// [VRFS ARCHIVED]           .addStringOption((opt) =>
// [VRFS ARCHIVED]             opt
// [VRFS ARCHIVED]               .setName("query")
// [VRFS ARCHIVED]               .setDescription("Marketplace ID, name or SKU")
// [VRFS ARCHIVED]               .setRequired(true)
// [VRFS ARCHIVED]           )
// [VRFS ARCHIVED]       )
// [VRFS ARCHIVED]       .addSubcommand((sub) =>
// [VRFS ARCHIVED]         sub
// [VRFS ARCHIVED]           .setName("creator")
// [VRFS ARCHIVED]           .setDescription("View a marketplace creator")
// [VRFS ARCHIVED]           .addStringOption((opt) =>
// [VRFS ARCHIVED]             opt
// [VRFS ARCHIVED]               .setName("name")
// [VRFS ARCHIVED]               .setDescription("Creator name")
// [VRFS ARCHIVED]               .setRequired(true)
// [VRFS ARCHIVED]           )
// [VRFS ARCHIVED]       )
// [VRFS ARCHIVED]       .addSubcommand((sub) =>
// [VRFS ARCHIVED]         sub.setName("stats").setDescription("View game item statistics")
// [VRFS ARCHIVED]       )
// [VRFS ARCHIVED]       .addSubcommand((sub) =>
// [VRFS ARCHIVED]         sub.setName("status").setDescription("View game data status")
// [VRFS ARCHIVED]       );
    const commandData = [
      new SlashCommandBuilder()
        .setName("stats")
        .setDescription("Tracker statistics"),
      new SlashCommandBuilder()
        .setName("active")
        .setDescription("Active giveaways"),
      new SlashCommandBuilder()
        .setName("recent")
        .setDescription("Recently detected giveaways"),
      new SlashCommandBuilder()
        .setName("setchannel")
        .setDescription("Set notification channel")
        .addChannelOption((opt) =>
          opt
            .setName("channel")
            .setDescription("Target channel")
            .setRequired(true)
        )
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName("reset")
        .setDescription("Wipe database")
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName("status")
        .setDescription("Check system status")
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName("metrics")
        .setDescription("Performance metrics")
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName("purge")
        .setDescription("Delete bot messages")
        .addIntegerOption((opt) =>
          opt.setName("amount").setDescription("How many")
        )
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName("help")
        .setDescription("List commands"),
      new SlashCommandBuilder()
        .setName("revoke")
        .setDescription("Revoke premium access")
        .addUserOption((opt) =>
          opt
            .setName("user")
            .setDescription("User to revoke premium from")
            .setRequired(true)
        )
        .setDefaultMemberPermissions(0),
      new SlashCommandBuilder()
        .setName("giveawaytrack")
        .setDescription("Manage giveaway tracking")
        .addSubcommand((sub) =>
          sub
            .setName("add")
            .setDescription("Add an item to track")
            .addStringOption((opt) =>
              opt
                .setName("item")
                .setDescription("Item to track")
                .setRequired(true)
                .setMinLength(2)
                .setMaxLength(50)
            )
        )
        .addSubcommand((sub) =>
          sub
            .setName("remove")
            .setDescription("Remove an item from tracking")
            .addStringOption((opt) =>
              opt
                .setName("item")
                .setDescription("Item to remove")
                .setRequired(true)
            )
        )
        .addSubcommand((sub) =>
          sub.setName("list").setDescription("Show your tracked items")
        )
        .addSubcommand((sub) =>
          sub.setName("clear").setDescription("Clear all tracked items")
        ),
      new SlashCommandBuilder()
        .setName("eventtrack")
        .setDescription("Manage event tracking")
        .addSubcommand((sub) =>
          sub
            .setName("add")
            .setDescription("Add an event filter")
            .addStringOption((opt) =>
              opt
                .setName("filter")
                .setDescription("Event filter")
                .setRequired(true)
                .setMinLength(2)
                .setMaxLength(30)
            )
        )
        .addSubcommand((sub) =>
          sub
            .setName("remove")
            .setDescription("Remove an event filter")
            .addStringOption((opt) =>
              opt
                .setName("filter")
                .setDescription("Filter to remove")
                .setRequired(true)
            )
        )
        .addSubcommand((sub) =>
          sub.setName("list").setDescription("Show your event filters")
        )
        .addSubcommand((sub) =>
          sub.setName("clear").setDescription("Clear all event filters")
        ),
      new SlashCommandBuilder()
        .setName("licenseadmin")
        .setDescription("Send the admin license panel")
        .setDefaultMemberPermissions(0),
// [VRFS ARCHIVED]       vrfsCommand,
    ];
    const rest = new REST({ version: "10" }).setToken(this.botToken);
    try {
      await rest.put(Routes.applicationCommands(this.client.user!.id), {
        body: commandData.map((cmd) => cmd.toJSON()),
      });
      this.commandsRegistered = true;
      logger.info("Commands registered", {
        component: "BotManager",
        count: commandData.length,
      });
    } catch (err) {
      logger.error("Command registration failed", {
        error: formatError(err),
      });
    }
  }
}
