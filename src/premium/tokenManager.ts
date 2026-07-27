/**
 * @module tokenManager
 * Token encryption, validation, and session management
 *
 * MEMORY FIX:
 * validateDiscordToken() and startTokenSession() previously created a
 * discord.js-selfbot-v13 Client and, on a failed login, never called
 * client.destroy() — the Client (open socket, internal caches, timers)
 * was simply dropped from scope and leaked. Both are called frequently
 * (every AutoJoinManager session start/reconnect/retry cycle, every
 * premium token submission, every boot-time restore), so any token that
 * was even occasionally flaky would leak a full Client on every failed
 * attempt. Fixed by always destroying the client in a `finally`/on the
 * failure path, and by not registering a session in the `sessions` map
 * unless login actually succeeded.
 */

import crypto from 'crypto';
import { Client } from 'discord.js-selfbot-v13';
import { logger } from '../logger.js';

// ============================================================================
// Encryption
// ============================================================================

const ENCRYPTION_KEY = process.env.TOKEN_ENCRYPTION_KEY;
if (!ENCRYPTION_KEY || ENCRYPTION_KEY.length !== 64) {
  throw new Error('TOKEN_ENCRYPTION_KEY must be 64 hex characters (32 bytes)');
}

const KEY = Buffer.from(ENCRYPTION_KEY, 'hex');
const IV_LENGTH = 16;

export function encryptToken(text: string): string {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', KEY, iv);
  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  return iv.toString('hex') + ':' + encrypted;
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

export async function validateDiscordToken(token: string): Promise<boolean> {
  const client = new Client();
  try {
    await client.login(token);
    return true;
  } catch {
    return false;
  } finally {
    // Always destroy, whether login succeeded or failed — previously
    // this only ran on the success path, leaking a Client (open socket,
    // caches, timers) on every failed validation.
    try {
      await client.destroy();
    } catch {
      // Ignore — client may not have gotten far enough to need cleanup
    }
  }
}

// ============================================================================
// Session Management
// ============================================================================

interface TokenSession {
  client: Client;
  userId: string;
  guildId: string;
  token: string;
  label: string;
  startedAt: number;
}

const sessions = new Map<string, TokenSession>();

/**
 * Starts a token session. Returns true if login succeeded and the
 * session was registered, false otherwise.
 *
 * IMPORTANT: this is now async and must be awaited by callers. The
 * previous fire-and-forget version registered the session in `sessions`
 * unconditionally, before login had even resolved — a failed login left
 * a permanently orphaned Client sitting in the map forever, since
 * nothing ever called stopSession() for it.
 */
export async function startTokenSession(
  userId: string,
  guildId: string,
  token: string,
  label: string
): Promise<boolean> {
  const sessionKey = `${userId}:${guildId}`;

  if (sessions.has(sessionKey)) {
    stopTokenSession(userId, guildId);
  }

  const client = new Client();

  try {
    await client.login(token);
  } catch (error) {
    logger.error('Failed to start token session', {
      userId,
      guildId,
      error: String(error),
    });
    try {
      await client.destroy();
    } catch {
      // Ignore
    }
    return false;
  }

  sessions.set(sessionKey, {
    client,
    userId,
    guildId,
    token,
    label,
    startedAt: Date.now(),
  });

  logger.info('Token session started', { userId, guildId, label });
  return true;
}

export function stopTokenSession(userId: string, guildId: string): void {
  const sessionKey = `${userId}:${guildId}`;
  const session = sessions.get(sessionKey);

  if (session) {
    try {
      session.client.destroy();
    } catch {
      // Ignore
    }
    sessions.delete(sessionKey);
    logger.info('Token session stopped', { userId, guildId });
  }
}

export function getTokenSession(userId: string, guildId: string): TokenSession | null {
  const sessionKey = `${userId}:${guildId}`;
  return sessions.get(sessionKey) || null;
}

export function isSessionActive(userId: string, guildId: string): boolean {
  return sessions.has(`${userId}:${guildId}`);
}

export function getAllSessions(): TokenSession[] {
  return Array.from(sessions.values());
}

// ============================================================================
// Restore Sessions from Database
// ============================================================================

export async function restoreTokenSessionsFromDatabase(): Promise<number> {
  try {
    // Import dynamically to avoid circular dependency
    const { getAllPremiumUsersAllGuilds } = await import('../database.js');
    
    const users = await getAllPremiumUsersAllGuilds();
    let restored = 0;

    for (const user of users) {
      if (!user.token || user.tokenActive === false) continue;

      const sessionKey = `${user.userId}:${user.guildId}`;
      if (sessions.has(sessionKey)) continue;

      try {
        const decryptedToken = decryptToken(user.token);
        const success = await startTokenSession(
          user.userId,
          user.guildId,
          decryptedToken,
          user.tokenLabel || 'main'
        );
        if (success) restored++;
      } catch (error) {
        logger.error('Failed to restore token session', {
          userId: user.userId,
          error: String(error),
        });
        // Mark token as inactive if decryption fails
        const { setTokenActive } = await import('../database.js');
        await setTokenActive(user.userId, user.guildId, false);
      }
    }

    logger.info(`Restored ${restored} token sessions from database`, {
      component: 'TokenManager',
    });

    return restored;
  } catch (error) {
    logger.error('Failed to restore token sessions', {
      component: 'TokenManager',
      error: String(error),
    });
    return 0;
  }
}
