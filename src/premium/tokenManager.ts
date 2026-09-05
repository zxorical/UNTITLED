/**
 * @module tokenManager
 * Token encryption, validation, and session management
 *
 * Session lifecycle goals:
 * - Never intentionally disconnect healthy sessions because of age/idle timers.
 * - Never destroy the existing session until a replacement successfully logs in.
 * - Fully destroy clients when sessions are explicitly stopped.
 * - Clean up failed login clients.
 * - Prevent concurrent restore operations.
 * - Keep memory usage bounded without randomly killing Discord sessions.
 */

import crypto from 'crypto';
import { Client } from 'discord.js-selfbot-v13';
import { logger } from '../logger.js';

// ============================================================================
// Encryption
// ============================================================================

const ENCRYPTION_KEY = process.env.TOKEN_ENCRYPTION_KEY;

if (!ENCRYPTION_KEY || ENCRYPTION_KEY.length !== 64) {
  throw new Error(
    'TOKEN_ENCRYPTION_KEY must be 64 hex characters (32 bytes)',
  );
}

const KEY = Buffer.from(ENCRYPTION_KEY, 'hex');
const IV_LENGTH = 16;

export function encryptToken(text: string): string {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', KEY, iv);

  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  return `${iv.toString('hex')}:${encrypted}`;
}

export function decryptToken(encrypted: string): string {
  const parts = encrypted.split(':');

  if (parts.length !== 2) {
    throw new Error('Invalid encrypted token format');
  }

  const iv = Buffer.from(parts[0], 'hex');
  const decipher = crypto.createDecipheriv('aes-256-cbc', KEY, iv);

  let decrypted = decipher.update(parts[1], 'hex', 'utf8');
  decrypted += decipher.final('utf8');

  return decrypted;
}

// ============================================================================
// Token Validation
// ============================================================================

/**
 * Validates a token using a temporary client.
 *
 * IMPORTANT:
 * This client is always destroyed after validation.
 * This should only be called when validation is actually needed.
 */
export async function validateDiscordToken(token: string): Promise<boolean> {
  const client = new Client();

  let success = false;

  try {
    await client.login(token);

    success = true;
    return true;
  } catch (error) {
    logger.debug('Token validation failed', {
      component: 'TokenManager',
      error: error instanceof Error ? error.message : String(error),
    });

    return false;
  } finally {
    try {
      await destroyClient(client);
    } catch {
      // Ignore cleanup failures.
    }

    logger.debug('Token validation completed', {
      component: 'TokenManager',
      success,
    });
  }
}

// ============================================================================
// Session Types
// ============================================================================

interface TokenSession {
  client: Client;
  userId: string;
  guildId: string;
  token: string;
  label: string;
  startedAt: number;
  lastActivityAt: number;
  isActive: boolean;
}

// ============================================================================
// Session State
// ============================================================================

const sessions = new Map<string, TokenSession>();

let sessionCleanupInterval: NodeJS.Timeout | null = null;
let isRestoring = false;
let isShuttingDown = false;

// ============================================================================
// Constants
// ============================================================================

/**
 * Cleanup is intentionally lightweight.
 *
 * We DO NOT use this to disconnect sessions based on age or inactivity.
 *
 * Discord sessions are long-lived and should remain alive until:
 * - explicitly stopped
 * - the application shuts down
 * - the session fails and another part of the application decides
 *   that it needs to reconnect
 */
const SESSION_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;

// ============================================================================
// Client Cleanup
// ============================================================================

/**
 * Safely destroy a Discord client.
 *
 * Discord/selfbot clients can have internal timers, websocket state,
 * listeners and other resources. Cleanup is kept in one place so every
 * code path behaves consistently.
 */
async function destroyClient(client: Client | null | undefined): Promise<void> {
  if (!client) {
    return;
  }

  try {
    client.removeAllListeners();
  } catch {
    // Ignore listener cleanup errors.
  }

  try {
    await client.destroy();
  } catch {
    // Ignore destroy errors.
  }
}

// ============================================================================
// Ready Waiting
// ============================================================================

/**
 * Wait for the client to become ready.
 *
 * login() normally handles the connection process, but we give the client
 * a short window to emit ready before registering it as a session.
 *
 * IMPORTANT:
 * A ready timeout does NOT destroy the client.
 *
 * The client may still be establishing its connection.
 */
async function waitForReady(
  client: Client,
  timeoutMs = 5000,
): Promise<boolean> {
  if (client.isReady()) {
    return true;
  }

  return new Promise<boolean>((resolve) => {
    let settled = false;

    const finish = (ready: boolean) => {
      if (settled) {
        return;
      }

      settled = true;

      clearTimeout(timeout);
      resolve(ready);
    };

    const timeout = setTimeout(() => {
      /*
       * Do not treat this as a fatal connection failure.
       *
       * The client may still become ready shortly afterwards.
       */
      finish(client.isReady());
    }, timeoutMs);

    try {
      // discord.js-selfbot-v13 types may not expose this correctly.
      // @ts-ignore
      client.once('ready', () => {
        finish(true);
      });
    } catch {
      clearTimeout(timeout);

      /*
       * If the event registration fails, simply check the state.
       * Do not destroy the client here.
       */
      finish(client.isReady());
    }
  });
}

// ============================================================================
// Session Management
// ============================================================================

/**
 * Starts a token session.
 *
 * IMPORTANT:
 * The existing session is NOT destroyed until the new client has
 * successfully logged in.
 *
 * This prevents:
 *
 * old session
 *    ↓
 * destroy
 *    ↓
 * new login fails
 *    ↓
 * no session
 *
 * Instead:
 *
 * old session
 *    ↓
 * new client login
 *    ↓
 * success
 *    ↓
 * replace old session
 *    ↓
 * destroy old client
 */
export async function startTokenSession(
  userId: string,
  guildId: string,
  token: string,
  label: string,
): Promise<boolean> {
  if (isShuttingDown) {
    logger.warn('Ignoring session start during shutdown', {
      component: 'TokenManager',
      userId,
      guildId,
    });

    return false;
  }

  const sessionKey = `${userId}:${guildId}`;
  const existingSession = sessions.get(sessionKey);

  /*
   * Create the replacement client first.
   */
  const client = new Client() as Client;

  /*
   * Always have an error listener on managed clients.
   */
  client.on('error', (error) => {
    logger.error('Token session client error', {
      component: 'TokenManager',
      userId,
      guildId,
      error: error instanceof Error ? error.message : String(error),
    });
  });

  try {
    logger.debug('Starting token session login', {
      component: 'TokenManager',
      userId,
      guildId,
      label,
      replacingExisting: Boolean(existingSession),
    });

    /*
     * LOGIN FIRST.
     *
     * We intentionally do NOT stop the existing session before this.
     */
    await client.login(token);

    /*
     * Give the client a short opportunity to emit ready.
     *
     * A timeout here does not automatically destroy the client.
     */
    await waitForReady(client, 5000);

    /*
     * If login succeeded, the client is now safe to register.
     */
    const newSession: TokenSession = {
      client,
      userId,
      guildId,
      token,
      label,
      startedAt: Date.now(),
      lastActivityAt: Date.now(),
      isActive: true,
    };

    /*
     * Atomically replace the map entry.
     */
    sessions.set(sessionKey, newSession);

    /*
     * ONLY NOW destroy the previous client.
     */
    if (existingSession && existingSession.client !== client) {
      logger.debug('Replacing existing token session', {
        component: 'TokenManager',
        userId,
        guildId,
      });

      await destroyClient(existingSession.client);
    }

    logger.info('Token session started', {
      component: 'TokenManager',
      userId,
      guildId,
      label,
      sessionCount: sessions.size,
      ready: client.isReady(),
    });

    startSessionCleanup();

    return true;
  } catch (error) {
    logger.error('Failed to start token session', {
      component: 'TokenManager',
      userId,
      guildId,
      error: error instanceof Error ? error.message : String(error),
    });

    /*
     * IMPORTANT:
     * Destroy only the NEW failed client.
     *
     * The existing session, if there was one, remains untouched.
     */
    await destroyClient(client);

    return false;
  }
}

// ============================================================================
// Stop Session
// ============================================================================

/**
 * Explicitly stops a token session.
 *
 * This is the ONLY normal TokenManager operation that intentionally
 * destroys a managed session.
 */
export function stopTokenSession(
  userId: string,
  guildId: string,
): void {
  const sessionKey = `${userId}:${guildId}`;
  const session = sessions.get(sessionKey);

  if (!session) {
    return;
  }

  /*
   * Remove the map reference FIRST.
   *
   * This makes the session unreachable from TokenManager immediately,
   * allowing garbage collection after client cleanup finishes.
   */
  sessions.delete(sessionKey);

  session.isActive = false;

  void destroyClient(session.client);

  logger.info('Token session stopped', {
    component: 'TokenManager',
    userId,
    guildId,
    remainingSessions: sessions.size,
  });
}

// ============================================================================
// Session Activity
// ============================================================================

export function updateSessionActivity(
  userId: string,
  guildId: string,
): void {
  const sessionKey = `${userId}:${guildId}`;
  const session = sessions.get(sessionKey);

  if (!session) {
    return;
  }

  session.lastActivityAt = Date.now();
  session.isActive = true;
}

// ============================================================================
// Session Getters
// ============================================================================

export function getTokenSession(
  userId: string,
  guildId: string,
): TokenSession | null {
  const sessionKey = `${userId}:${guildId}`;
  const session = sessions.get(sessionKey);

  if (!session || !session.isActive) {
    return null;
  }

  updateSessionActivity(userId, guildId);

  return session;
}

export function getTokenSessionRaw(
  userId: string,
  guildId: string,
): TokenSession | null {
  const sessionKey = `${userId}:${guildId}`;

  return sessions.get(sessionKey) ?? null;
}

export function isSessionActive(
  userId: string,
  guildId: string,
): boolean {
  const sessionKey = `${userId}:${guildId}`;
  const session = sessions.get(sessionKey);

  return session?.isActive ?? false;
}

export function getAllSessions(): TokenSession[] {
  return Array.from(sessions.values());
}

export function getActiveSessionsCount(): number {
  let count = 0;

  for (const session of sessions.values()) {
    if (session.isActive) {
      count++;
    }
  }

  return count;
}

export function getTotalSessionsCount(): number {
  return sessions.size;
}

// ============================================================================
// Session Cleanup
// ============================================================================

/**
 * Starts the lightweight session cleanup timer.
 *
 * This timer intentionally DOES NOT disconnect sessions because:
 *
 * - they are older than 24h
 * - they have been idle
 * - client.isReady() is temporarily false
 *
 * Those checks caused healthy/temporarily reconnecting sessions to be
 * destroyed unnecessarily.
 */
function startSessionCleanup(): void {
  if (sessionCleanupInterval) {
    return;
  }

  sessionCleanupInterval = setInterval(() => {
    performSessionCleanup();
  }, SESSION_CLEANUP_INTERVAL_MS);

  /*
   * The cleanup timer must never keep Node alive by itself.
   */
  if (typeof sessionCleanupInterval.unref === 'function') {
    sessionCleanupInterval.unref();
  }

  logger.debug('Session cleanup interval started', {
    component: 'TokenManager',
    interval: `${SESSION_CLEANUP_INTERVAL_MS / 60000} minutes`,
  });
}

/**
 * Lightweight cleanup.
 *
 * At the moment this mainly removes invalid map entries rather than
 * deciding that a Discord connection should be killed.
 *
 * Reconnection decisions belong to the session/AutoJoin manager.
 */
function performSessionCleanup(): void {
  let cleaned = 0;

  for (const [key, session] of sessions) {
    /*
     * Defensive check.
     *
     * We only remove entries that are explicitly inactive and somehow
     * remained in the map.
     */
    if (!session.isActive) {
      sessions.delete(key);

      void destroyClient(session.client);

      cleaned++;

      logger.debug('Inactive session cleaned up', {
        component: 'TokenManager',
        userId: session.userId,
        guildId: session.guildId,
      });
    }
  }

  if (cleaned > 0) {
    logger.info('Session cleanup completed', {
      component: 'TokenManager',
      cleaned,
      remaining: sessions.size,
    });
  }
}

// ============================================================================
// Restore Sessions From Database
// ============================================================================

export async function restoreTokenSessionsFromDatabase(): Promise<number> {
  if (isShuttingDown) {
    logger.debug('Skipping session restore during shutdown', {
      component: 'TokenManager',
    });

    return 0;
  }

  /*
   * Prevent two restore operations from running simultaneously.
   */
  if (isRestoring) {
    logger.debug('Session restore already in progress, skipping', {
      component: 'TokenManager',
    });

    return 0;
  }

  isRestoring = true;

  try {
    /*
     * Dynamic import prevents circular dependency problems.
     */
    const {
      getAllPremiumUsersAllGuilds,
      setTokenActive,
    } = await import('../database.js');

    const users = await getAllPremiumUsersAllGuilds();

    let restored = 0;
    let skipped = 0;
    let failed = 0;

    logger.info(
      `Starting session restore for ${users.length} premium users`,
      {
        component: 'TokenManager',
      },
    );

    for (const user of users) {
      if (isShuttingDown) {
        logger.warn('Stopping session restore because shutdown started', {
          component: 'TokenManager',
        });

        break;
      }

      /*
       * Skip users without usable tokens.
       */
      if (!user.token) {
        skipped++;
        continue;
      }

      if (user.tokenActive === false) {
        skipped++;
        continue;
      }

      const sessionKey = `${user.userId}:${user.guildId}`;

      /*
       * Never create a duplicate session.
       */
      if (sessions.has(sessionKey)) {
        skipped++;
        continue;
      }

      try {
        const decryptedToken = decryptToken(user.token);

        const success = await startTokenSession(
          user.userId,
          user.guildId,
          decryptedToken,
          user.tokenLabel || 'main',
        );

        if (success) {
          restored++;
        } else {
          failed++;

          /*
           * Only mark inactive when restoration actually failed.
           */
          try {
            await setTokenActive(
              user.userId,
              user.guildId,
              false,
            );
          } catch (dbError) {
            logger.error('Failed to mark token inactive', {
              component: 'TokenManager',
              userId: user.userId,
              guildId: user.guildId,
              error:
                dbError instanceof Error
                  ? dbError.message
                  : String(dbError),
            });
          }
        }
      } catch (error) {
        failed++;

        logger.error('Failed to restore token session', {
          component: 'TokenManager',
          userId: user.userId,
          guildId: user.guildId,
          error:
            error instanceof Error
              ? error.message
              : String(error),
        });

        /*
         * Database failures shouldn't cause the entire restore loop
         * to die.
         */
        try {
          await setTokenActive(
            user.userId,
            user.guildId,
            false,
          );
        } catch (dbError) {
          logger.error('Failed to mark token inactive', {
            component: 'TokenManager',
            userId: user.userId,
            guildId: user.guildId,
            error:
              dbError instanceof Error
                ? dbError.message
                : String(dbError),
          });
        }
      }
    }

    logger.info('Session restore complete', {
      component: 'TokenManager',
      restored,
      skipped,
      failed,
      totalUsers: users.length,
      activeSessions: sessions.size,
    });

    startSessionCleanup();

    return restored;
  } catch (error) {
    logger.error('Failed to restore token sessions', {
      component: 'TokenManager',
      error:
        error instanceof Error
          ? error.message
          : String(error),
    });

    return 0;
  } finally {
    isRestoring = false;
  }
}

// ============================================================================
// Shutdown
// ============================================================================

export function shutdownTokenManager(): void {
  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;

  logger.info('Shutting down token manager...', {
    component: 'TokenManager',
    activeSessions: sessions.size,
  });

  /*
   * Stop cleanup timer first.
   */
  if (sessionCleanupInterval) {
    clearInterval(sessionCleanupInterval);
    sessionCleanupInterval = null;
  }

  /*
   * Take all sessions out of the map immediately.
   */
  const currentSessions = Array.from(sessions.values());
  sessions.clear();

  let stopped = 0;

  /*
   * Destroy clients asynchronously without allowing one failure
   * to prevent the others from being cleaned up.
   */
  for (const session of currentSessions) {
    session.isActive = false;

    void destroyClient(session.client);

    stopped++;
  }

  logger.info('Token manager shutdown complete', {
    component: 'TokenManager',
    stopped,
  });
}

// ============================================================================
// Health Check
// ============================================================================

export function getSessionStats(): {
  totalSessions: number;
  activeSessions: number;
  idleSessions: number;
  oldestSessionAge: number | null;
  averageSessionAge: number | null;
} {
  const now = Date.now();

  let active = 0;
  let idle = 0;
  let totalAge = 0;
  let oldestAge = 0;

  for (const session of sessions.values()) {
    if (session.isActive) {
      active++;
    } else {
      idle++;
    }

    const age = now - session.startedAt;

    totalAge += age;

    if (age > oldestAge) {
      oldestAge = age;
    }
  }

  return {
    totalSessions: sessions.size,
    activeSessions: active,
    idleSessions: idle,
    oldestSessionAge:
      sessions.size > 0 ? oldestAge : null,
    averageSessionAge:
      sessions.size > 0
        ? Math.round(totalAge / sessions.size)
        : null,
  };
}

// ============================================================================
// Process Shutdown
// ============================================================================

process.on('SIGTERM', () => {
  logger.info('SIGTERM received, cleaning up token sessions...', {
    component: 'TokenManager',
  });

  shutdownTokenManager();
});

process.on('SIGINT', () => {
  logger.info('SIGINT received, cleaning up token sessions...', {
    component: 'TokenManager',
  });

  shutdownTokenManager();
});

// ============================================================================
// Export
// ============================================================================

export default {
  encryptToken,
  decryptToken,
  validateDiscordToken,
  startTokenSession,
  stopTokenSession,
  getTokenSession,
  getTokenSessionRaw,
  isSessionActive,
  getAllSessions,
  getActiveSessionsCount,
  getTotalSessionsCount,
  updateSessionActivity,
  restoreTokenSessionsFromDatabase,
  shutdownTokenManager,
  getSessionStats,
};
