import { Client, Guild, GuildMember, PartialGuildMember, Role } from 'discord.js';
import { logger } from '../logger.js';
import {
  setPremiumUser,
  removePremiumUser as removePremiumUserDb,
  getPremiumUser,
  getAllPremiumUsers,
  getPremiumStats as getPremiumStatsDb,
} from '../database.js';

export type PremiumSource = 'key' | 'booster' | 'manual';

interface CacheEntry {
  isPremium: boolean;
  expiresAt: number;
  guildId: string;
  roleId: string | null;
  source?: PremiumSource;
  validatedAt: number;
}

interface PremiumRecord {
  isPremium?: boolean;
  source?: string;
  licenseKey?: string;
}

interface PremiumCheckResult {
  isPremium: boolean;
  guildId?: string;
  roleId?: string;
  source?: PremiumSource;
  error?: string;
}

let clientRef: Client | null = null;
let roleId: string | null = null;
let eventsRegistered = false;

const premiumCache = new Map<string, CacheEntry>();

const CACHE_TTL_MS = 5 * 60 * 1000;
const NEGATIVE_CACHE_TTL_MS = 30 * 1000;
const MEMBERSHIP_CACHE_TTL_MS = 60 * 1000;

function cacheKey(userId: string, guildId: string): string {
  return `${userId}:${guildId}`;
}

function normalizeSource(source: unknown): PremiumSource | undefined {
  if (source === 'key') return 'key';
  if (source === 'booster') return 'booster';
  if (source === 'manual') return 'manual';
  return undefined;
}

function getGuild(guildId: string): Guild | null {
  return clientRef?.guilds.cache.get(guildId) ?? null;
}

function getPremiumRole(guild: Guild): Role | null {
  if (!roleId) return null;
  return guild.roles.cache.get(roleId) ?? null;
}

async function fetchMember(
  guild: Guild,
  userId: string,
): Promise<GuildMember | null> {
  const cached = guild.members.cache.get(userId);
  if (cached) return cached;

  try {
    return await guild.members.fetch(userId);
  } catch {
    return null;
  }
}

function cacheResult(
  userId: string,
  guildId: string,
  result: PremiumCheckResult,
  ttlMs: number,
): void {
  premiumCache.set(cacheKey(userId, guildId), {
    isPremium: result.isPremium,
    expiresAt: Date.now() + ttlMs,
    guildId,
    roleId,
    source: result.source,
    validatedAt: Date.now(),
  });
}

function getCachedResult(
  userId: string,
  guildId: string,
): CacheEntry | null {
  const key = cacheKey(userId, guildId);
  const cached = premiumCache.get(key);

  if (!cached) return null;

  if (cached.expiresAt <= Date.now()) {
    premiumCache.delete(key);
    return null;
  }

  return cached;
}

/**
 * DB is always revoked first. Role removal is best-effort when a current
 * member object exists. A missing member is expected during guildMemberRemove.
 */
async function revokePremiumEntitlement(
  userId: string,
  guildId: string,
  reason: string,
  removeRole = true,
): Promise<void> {
  try {
    await removePremiumUserDb(userId, guildId);
  } catch (error) {
    logger.error('Premium DB revocation failed', {
      component: 'LicenseMiddleware',
      userId,
      guildId,
      reason,
      error: String(error),
    });
  }

  clearPremiumCache(userId, guildId);

  if (!removeRole || !clientRef || !roleId) return;

  const guild = getGuild(guildId);
  if (!guild) return;

  const member = guild.members.cache.get(userId);
  if (!member) return;

  const role = getPremiumRole(guild);
  if (!role || !member.roles.cache.has(role.id)) return;

  try {
    await member.roles.remove(role);
  } catch (error) {
    logger.warn('Premium role removal failed during revocation', {
      component: 'LicenseMiddleware',
      userId,
      guildId,
      reason,
      error: String(error),
    });
  }
}

export function setClient(client: Client): void {
  clientRef = client;
  roleId = process.env.PREMIUM_ROLE_ID?.trim() || null;

  if (!roleId) {
    logger.warn(
      'PREMIUM_ROLE_ID not set. Premium checks will return false.',
      { component: 'LicenseMiddleware' },
    );
  }

  registerPremiumEvents(client);
}

export function getPremiumRoleId(): string | null {
  return roleId;
}

export function clearPremiumCache(
  userId: string,
  guildId?: string,
): void {
  if (guildId) {
    premiumCache.delete(cacheKey(userId, guildId));
    return;
  }

  const prefix = `${userId}:`;

  for (const key of premiumCache.keys()) {
    if (key.startsWith(prefix)) {
      premiumCache.delete(key);
    }
  }
}

export function clearAllPremiumCache(): void {
  premiumCache.clear();
}

/**
 * Defensive entitlement check.
 *
 * A premium record is NEVER enough by itself. The user must also still be a
 * member of the guild. If they are not, their entitlement is revoked.
 */
export async function checkPremium(
  userId: string,
  guildId?: string,
): Promise<PremiumCheckResult> {
  if (!guildId) {
    return {
      isPremium: false,
      error: 'Guild ID required.',
    };
  }

  if (!clientRef) {
    return {
      isPremium: false,
      error: 'Bot client not initialized.',
    };
  }

  if (!roleId) {
    return {
      isPremium: false,
      error: 'Premium role not configured.',
    };
  }

  const cached = getCachedResult(userId, guildId);

  if (cached) {
    return {
      isPremium: cached.isPremium,
      guildId: cached.guildId,
      roleId: cached.roleId ?? undefined,
      source: cached.source,
    };
  }

  try {
    const guild = getGuild(guildId);

    if (!guild) {
      cacheResult(
        userId,
        guildId,
        {
          isPremium: false,
          guildId,
          roleId,
        },
        NEGATIVE_CACHE_TTL_MS,
      );

      return {
        isPremium: false,
        guildId,
        roleId,
        error: 'Guild not found.',
      };
    }

    const dbUser = await getPremiumUser(
      userId,
      guildId,
    ) as PremiumRecord | null;

    if (!dbUser?.isPremium) {
      cacheResult(
        userId,
        guildId,
        {
          isPremium: false,
          guildId,
          roleId,
        },
        NEGATIVE_CACHE_TTL_MS,
      );

      return {
        isPremium: false,
        guildId,
        roleId,
      };
    }

    const member = await fetchMember(guild, userId);

    /*
     * CRITICAL FAIL-CLOSED CHECK:
     *
     * If the user left, the premium DB record is invalid. Remove it before
     * returning. This protects against missed gateway events and stale data.
     */
    if (!member) {
      await revokePremiumEntitlement(
        userId,
        guildId,
        'user_not_in_guild_during_check',
        false,
      );

      return {
        isPremium: false,
        guildId,
        roleId,
        error: 'User is not a member of this guild.',
      };
    }

    const source = normalizeSource(dbUser.source);

    /*
     * Never trust unknown legacy records. Fail closed instead of accidentally
     * turning an ambiguous historical record into premium.
     */
    if (!source) {
      await revokePremiumEntitlement(
        userId,
        guildId,
        'unknown_premium_source',
        true,
      );

      logger.warn('Unknown premium source revoked', {
        component: 'LicenseMiddleware',
        userId,
        guildId,
        source: dbUser.source,
      });

      return {
        isPremium: false,
        guildId,
        roleId,
        error: 'Unknown premium entitlement source.',
      };
    }

    /*
     * Membership is verified. Now synchronize the premium role if necessary.
     */
    const premiumRole = getPremiumRole(guild);

    if (
      premiumRole &&
      !member.roles.cache.has(premiumRole.id)
    ) {
      try {
        await member.roles.add(premiumRole);
      } catch (error) {
        logger.warn('Failed restoring premium role', {
          component: 'LicenseMiddleware',
          userId,
          guildId,
          source,
          error: String(error),
        });
      }
    }

    const result: PremiumCheckResult = {
      isPremium: true,
      guildId,
      roleId,
      source,
    };

    cacheResult(
      userId,
      guildId,
      result,
      source === 'booster'
        ? MEMBERSHIP_CACHE_TTL_MS
        : CACHE_TTL_MS,
    );

    return result;
  } catch (error) {
    logger.error('Premium check failed', {
      component: 'LicenseMiddleware',
      userId,
      guildId,
      error: String(error),
    });

    return {
      isPremium: false,
      guildId,
      roleId,
      error: String(error),
    };
  }
}

export async function isPremium(
  userId: string,
  guildId?: string,
): Promise<boolean> {
  if (guildId) {
    return (await checkPremium(userId, guildId)).isPremium;
  }

  if (!clientRef || !roleId) return false;

  for (const guild of clientRef.guilds.cache.values()) {
    if ((await checkPremium(userId, guild.id)).isPremium) {
      return true;
    }
  }

  return false;
}

export async function isPremiumFresh(
  userId: string,
  guildId?: string,
): Promise<boolean> {
  clearPremiumCache(userId, guildId);
  return isPremium(userId, guildId);
}

export async function requirePremium(
  userId: string,
  guildId?: string,
): Promise<{
  allowed: boolean;
  message?: string;
}> {
  if (!(await isPremium(userId, guildId))) {
    return {
      allowed: false,
      message:
        'Premium access required for this feature. Use /activate to unlock premium.',
    };
  }

  return {
    allowed: true,
  };
}

/**
 * Grants an entitlement to a user who is currently in the guild.
 *
 * If role assignment fails, the DB write is rolled back so the entitlement
 * cannot exist without its associated role synchronization.
 */
export async function addPremiumUser(
  userId: string,
  guildId: string,
  source: PremiumSource,
  licenseKey?: string,
): Promise<{
  success: boolean;
  error?: string;
}> {
  if (!clientRef) {
    return {
      success: false,
      error: 'Bot client not initialized.',
    };
  }

  if (!roleId) {
    return {
      success: false,
      error: 'PREMIUM_ROLE_ID not configured.',
    };
  }

  try {
    const guild = getGuild(guildId);

    if (!guild) {
      return {
        success: false,
        error: 'Guild not found.',
      };
    }

    const member = await fetchMember(guild, userId);

    if (!member) {
      return {
        success: false,
        error: 'User not found in this server.',
      };
    }

    const role = getPremiumRole(guild);

    if (!role) {
      return {
        success: false,
        error: 'Premium role not found.',
      };
    }

    await setPremiumUser(
      userId,
      guildId,
      source,
      licenseKey,
    );

    try {
      if (!member.roles.cache.has(role.id)) {
        await member.roles.add(role);
      }
    } catch (roleError) {
      try {
        await removePremiumUserDb(userId, guildId);
      } catch (rollbackError) {
        logger.error('Premium grant rollback failed', {
          component: 'LicenseMiddleware',
          userId,
          guildId,
          source,
          roleError: String(roleError),
          rollbackError: String(rollbackError),
        });
      }

      clearPremiumCache(userId, guildId);

      return {
        success: false,
        error: `Failed to assign premium role: ${String(roleError)}`,
      };
    }

    clearPremiumCache(userId, guildId);

    logger.info('Premium user added', {
      component: 'LicenseMiddleware',
      userId,
      guildId,
      source,
    });

    return {
      success: true,
    };
  } catch (error) {
    logger.error('Failed to add premium user', {
      component: 'LicenseMiddleware',
      userId,
      guildId,
      source,
      error: String(error),
    });

    return {
      success: false,
      error: String(error),
    };
  }
}

export async function removePremiumUser(
  userId: string,
  guildId: string,
): Promise<{
  success: boolean;
  error?: string;
}> {
  if (!clientRef) {
    return {
      success: false,
      error: 'Bot client not initialized.',
    };
  }

  if (!roleId) {
    return {
      success: false,
      error: 'PREMIUM_ROLE_ID not configured.',
    };
  }

  try {
    const guild = getGuild(guildId);

    if (guild) {
      const role = getPremiumRole(guild);
      const member = await fetchMember(guild, userId);

      if (
        role &&
        member?.roles.cache.has(role.id)
      ) {
        await member.roles.remove(role);
      }
    }

    await removePremiumUserDb(
      userId,
      guildId,
    );

    clearPremiumCache(
      userId,
      guildId,
    );

    logger.info('Premium user removed', {
      component: 'LicenseMiddleware',
      userId,
      guildId,
    });

    return {
      success: true,
    };
  } catch (error) {
    logger.error('Failed to remove premium user', {
      component: 'LicenseMiddleware',
      userId,
      guildId,
      error: String(error),
    });

    return {
      success: false,
      error: String(error),
    };
  }
}

/**
 * Primary automatic revocation.
 *
 * This intentionally ignores premium source. If a user leaves the guild,
 * key/booster/manual premium for that guild is revoked.
 *
 * discord.js can emit GuildMember | PartialGuildMember here, so we only
 * use properties guaranteed to exist on both types.
 */
async function handlePremiumMemberRemove(
  member: GuildMember | PartialGuildMember,
): Promise<void> {
  try {
    await revokePremiumEntitlement(
      member.id,
      member.guild.id,
      'guild_member_remove',
      false,
    );

    logger.info('Premium automatically revoked after guild leave', {
      component: 'LicenseMiddleware',
      userId: member.id,
      guildId: member.guild.id,
    });
  } catch (error) {
    logger.error(
      'Unhandled premium revoke error on member removal',
      {
        component: 'LicenseMiddleware',
        userId: member.id,
        guildId: member.guild.id,
        error: String(error),
      },
    );
  }
}

/**
 * Rejoining never automatically restores an entitlement.
 *
 * We only clear caches here. The DB entitlement should already have been
 * removed by guildMemberRemove. If Discord missed that event, the next
 * checkPremium() while they were absent would remove it defensively.
 */
async function handlePremiumMemberAdd(
  member: GuildMember,
): Promise<void> {
  try {
    clearPremiumCache(
      member.id,
      member.guild.id,
    );

    logger.debug(
      'Premium cache cleared after guild rejoin',
      {
        component: 'LicenseMiddleware',
        userId: member.id,
        guildId: member.guild.id,
      },
    );
  } catch (error) {
    logger.warn(
      'Failed clearing premium cache after guild rejoin',
      {
        component: 'LicenseMiddleware',
        userId: member.id,
        guildId: member.guild.id,
        error: String(error),
      },
    );
  }
}

export function registerPremiumEvents(
  client: Client,
): void {
  if (eventsRegistered) return;

  eventsRegistered = true;

  client.on(
    'guildMemberRemove',
    member => {
      void handlePremiumMemberRemove(member);
    },
  );

  client.on(
    'guildMemberAdd',
    member => {
      void handlePremiumMemberAdd(member);
    },
  );

  client.on(
    'error',
    error => {
      logger.error(
        'Discord client error in premium middleware',
        {
          component: 'LicenseMiddleware',
          error: String(error),
        },
      );
    },
  );

  logger.info(
    'Premium membership enforcement enabled',
    {
      component: 'LicenseMiddleware',
    },
  );
}

export async function getPremiumUsers(
  guildId: string,
): Promise<any[]> {
  return getAllPremiumUsers(guildId);
}

export async function getPremiumStats(
  guildId: string,
): Promise<any> {
  return getPremiumStatsDb(guildId);
}

/**
 * Legacy compatibility.
 *
 * Historically this grants premium through a license/key flow, so it keeps
 * the persistent "key" source.
 */
export async function assignPremiumRole(
  userId: string,
  guildId: string,
): Promise<{
  success: boolean;
  error?: string;
}> {
  return addPremiumUser(
    userId,
    guildId,
    'key',
  );
}
