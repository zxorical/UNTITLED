import crypto from 'crypto';
import { Client } from 'discord.js-selfbot-v13';
import { logger } from '../logger.js';
const ENCRYPTION_KEY = process.env.TOKEN_ENCRYPTION_KEY;
if (!ENCRYPTION_KEY || !/^[0-9a-fA-F]{64}$/.test(ENCRYPTION_KEY)) {
  throw new Error('TOKEN_ENCRYPTION_KEY must be 64 hexadecimal characters (32 bytes)');
}
const KEY = Buffer.from(ENCRYPTION_KEY, 'hex');
const IV_LENGTH = 16;
const LOGIN_TIMEOUT_MS = 30000;
const READY_TIMEOUT_MS = 10000;
const DESTROY_TIMEOUT_MS = 10000;
const SESSION_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;
export function encryptToken(text: string): string {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', KEY, iv);
  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  return `${iv.toString('hex')}:${encrypted}`;
}
export function decryptToken(encrypted: string): string {
  const parts = encrypted.split(':');
  if (parts.length !== 2) throw new Error('Invalid encrypted token format');
  if (!/^[0-9a-fA-F]{32}$/.test(parts[0]) || !/^[0-9a-fA-F]+$/.test(parts[1]) || parts[1].length % 2 !== 0) {
    throw new Error('Invalid encrypted token format');
  }
  const iv = Buffer.from(parts[0], 'hex');
  const decipher = crypto.createDecipheriv('aes-256-cbc', KEY, iv);
  let decrypted = decipher.update(parts[1], 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  return decrypted;
}
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
const sessions = new Map<string, TokenSession>();
const pendingStarts = new Map<string, Promise<boolean>>();
const sessionGenerations = new Map<string, number>();
let sessionCleanupInterval: NodeJS.Timeout | null = null;
let isRestoring = false;
let isShuttingDown = false;
function withTimeout<T>(promise: Promise<T>, timeoutMs: number, timeoutMessage: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(timeoutMessage));
    }, timeoutMs);
    timer.unref?.();
    promise.then(value => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(value);
    }).catch(error => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    });
  });
}
async function destroyClient(client: Client | null | undefined): Promise<void> {
  if (!client) return;
  try {
    await withTimeout(Promise.resolve().then(() => client.destroy()), DESTROY_TIMEOUT_MS, 'Discord client destroy timeout');
  } catch {
  } finally {
    try {
      client.removeAllListeners();
    } catch {
    }
  }
}
async function waitForReady(client: Client, timeoutMs = READY_TIMEOUT_MS): Promise<boolean> {
  if (client.isReady()) return true;
  return new Promise<boolean>(resolve => {
    let settled = false;
    let timeout: NodeJS.Timeout | null = null;
    const onReady = () => finish(true);
    const cleanup = () => {
      if (timeout) clearTimeout(timeout);
      timeout = null;
      try {
        client.removeListener('ready', onReady as any);
      } catch {
      }
    };
    const finish = (ready: boolean) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(ready);
    };
    client.once('ready', onReady);
    timeout = setTimeout(() => finish(client.isReady()), timeoutMs);
    timeout.unref?.();
  });
}
export async function validateDiscordToken(token: string): Promise<boolean> {
  const client = new Client();
  let success = false;
  try {
    await withTimeout(client.login(token), LOGIN_TIMEOUT_MS, 'Discord token validation login timeout');
    success = true;
    return true;
  } catch (error) {
    logger.debug('Token validation failed', { component: 'TokenManager', error: error instanceof Error ? error.message : String(error) });
    return false;
  } finally {
    await destroyClient(client);
    logger.debug('Token validation completed', { component: 'TokenManager', success });
  }
}
async function createAndLoginSession(userId: string, guildId: string, token: string, label: string, generation: number): Promise<boolean> {
  if (isShuttingDown) return false;
  const sessionKey = `${userId}:${guildId}`;
  if ((sessionGenerations.get(sessionKey) ?? 0) !== generation) return false;
  const existingSession = sessions.get(sessionKey);
  const client = new Client() as Client;
  client.on('error', error => {
    logger.error('Token session client error', { component: 'TokenManager', userId, guildId, error: error instanceof Error ? error.message : String(error) });
  });
  try {
    logger.debug('Starting token session login', { component: 'TokenManager', userId, guildId, label, replacingExisting: Boolean(existingSession) });
    await withTimeout(client.login(token), LOGIN_TIMEOUT_MS, 'Discord token session login timeout');
    const ready = await waitForReady(client, READY_TIMEOUT_MS);
    if (!ready || !client.isReady()) throw new Error('Discord client did not become ready');
    if (isShuttingDown || (sessionGenerations.get(sessionKey) ?? 0) !== generation) {
      await destroyClient(client);
      return false;
    }
    const newSession: TokenSession = { client, userId, guildId, token, label, startedAt: Date.now(), lastActivityAt: Date.now(), isActive: true };
    sessions.set(sessionKey, newSession);
    if (existingSession && existingSession.client !== client) await destroyClient(existingSession.client);
    logger.info('Token session started', { component: 'TokenManager', userId, guildId, label, sessionCount: sessions.size, ready: true });
    startSessionCleanup();
    return true;
  } catch (error) {
    logger.error('Failed to start token session', { component: 'TokenManager', userId, guildId, error: error instanceof Error ? error.message : String(error) });
    await destroyClient(client);
    return false;
  }
}
export async function startTokenSession(userId: string, guildId: string, token: string, label: string): Promise<boolean> {
  if (isShuttingDown) {
    logger.warn('Ignoring session start during shutdown', { component: 'TokenManager', userId, guildId });
    return false;
  }
  const sessionKey = `${userId}:${guildId}`;
  const existingStart = pendingStarts.get(sessionKey);
  if (existingStart) return existingStart;
  const generation = sessionGenerations.get(sessionKey) ?? 0;
  const startPromise = createAndLoginSession(userId, guildId, token, label, generation).finally(() => {
    if (pendingStarts.get(sessionKey) === startPromise) pendingStarts.delete(sessionKey);
  });
  pendingStarts.set(sessionKey, startPromise);
  return startPromise;
}
export function stopTokenSession(userId: string, guildId: string): void {
  const sessionKey = `${userId}:${guildId}`;
  const session = sessions.get(sessionKey);
  if (!session) return;
  sessions.delete(sessionKey);
  sessionGenerations.set(sessionKey, (sessionGenerations.get(sessionKey) ?? 0) + 1);
  session.isActive = false;
  void destroyClient(session.client);
  logger.info('Token session stopped', { component: 'TokenManager', userId, guildId, remainingSessions: sessions.size });
}
export function updateSessionActivity(userId: string, guildId: string): void {
  const session = sessions.get(`${userId}:${guildId}`);
  if (!session) return;
  session.lastActivityAt = Date.now();
  session.isActive = true;
}
export function getTokenSession(userId: string, guildId: string): TokenSession | null {
  const session = sessions.get(`${userId}:${guildId}`);
  if (!session || !session.isActive) return null;
  session.lastActivityAt = Date.now();
  return session;
}
export function getTokenSessionRaw(userId: string, guildId: string): TokenSession | null {
  return sessions.get(`${userId}:${guildId}`) ?? null;
}
export function isSessionActive(userId: string, guildId: string): boolean {
  return sessions.get(`${userId}:${guildId}`)?.isActive ?? false;
}
export function getAllSessions(): TokenSession[] {
  return Array.from(sessions.values());
}
export function getActiveSessionsCount(): number {
  let count = 0;
  for (const session of sessions.values()) if (session.isActive) count++;
  return count;
}
export function getTotalSessionsCount(): number {
  return sessions.size;
}
function startSessionCleanup(): void {
  if (sessionCleanupInterval || isShuttingDown) return;
  sessionCleanupInterval = setInterval(performSessionCleanup, SESSION_CLEANUP_INTERVAL_MS);
  sessionCleanupInterval.unref?.();
}
function performSessionCleanup(): void {
  let cleaned = 0;
  for (const [key, session] of sessions) {
    if (!session.isActive) {
      sessions.delete(key);
      void destroyClient(session.client);
      cleaned++;
    }
  }
  if (cleaned > 0) logger.info('Session cleanup completed', { component: 'TokenManager', cleaned, remaining: sessions.size });
}
export async function restoreTokenSessionsFromDatabase(): Promise<number> {
  if (isShuttingDown || isRestoring) return 0;
  isRestoring = true;
  try {
    const { getAllPremiumUsersAllGuilds, setTokenActive } = await import('../database.js');
    const users = await getAllPremiumUsersAllGuilds();
    let restored = 0;
    let skipped = 0;
    let failed = 0;
    logger.info(`Starting session restore for ${users.length} premium users`, { component: 'TokenManager' });
    for (const user of users) {
      if (isShuttingDown) break;
      if (!user.token || user.tokenActive === false) {
        skipped++;
        continue;
      }
      const sessionKey = `${user.userId}:${user.guildId}`;
      if (sessions.has(sessionKey) || pendingStarts.has(sessionKey)) {
        skipped++;
        continue;
      }
      try {
        const decryptedToken = decryptToken(user.token);
        const success = await startTokenSession(user.userId, user.guildId, decryptedToken, user.tokenLabel || 'main');
        if (success) {
          restored++;
        } else {
          failed++;
          try {
            await setTokenActive(user.userId, user.guildId, false);
          } catch (dbError) {
            logger.error('Failed to mark token inactive', { component: 'TokenManager', userId: user.userId, guildId: user.guildId, error: dbError instanceof Error ? dbError.message : String(dbError) });
          }
        }
      } catch (error) {
        failed++;
        logger.error('Failed to restore token session', { component: 'TokenManager', userId: user.userId, guildId: user.guildId, error: error instanceof Error ? error.message : String(error) });
        try {
          await setTokenActive(user.userId, user.guildId, false);
        } catch (dbError) {
          logger.error('Failed to mark token inactive', { component: 'TokenManager', userId: user.userId, guildId: user.guildId, error: dbError instanceof Error ? dbError.message : String(dbError) });
        }
      }
    }
    logger.info('Session restore complete', { component: 'TokenManager', restored, skipped, failed, totalUsers: users.length, activeSessions: sessions.size });
    startSessionCleanup();
    return restored;
  } catch (error) {
    logger.error('Failed to restore token sessions', { component: 'TokenManager', error: error instanceof Error ? error.message : String(error) });
    return 0;
  } finally {
    isRestoring = false;
  }
}
export function shutdownTokenManager(): void {
  if (isShuttingDown) return;
  isShuttingDown = true;
  if (sessionCleanupInterval) {
    clearInterval(sessionCleanupInterval);
    sessionCleanupInterval = null;
  }
  const currentSessions = Array.from(sessions.values());
  sessions.clear();
  pendingStarts.clear();
  sessionGenerations.clear();
  for (const session of currentSessions) {
    session.isActive = false;
    void destroyClient(session.client);
  }
  logger.info('Token manager shutdown complete', { component: 'TokenManager', stopped: currentSessions.length });
}
export function getSessionStats(): { totalSessions: number; activeSessions: number; idleSessions: number; oldestSessionAge: number | null; averageSessionAge: number | null } {
  const now = Date.now();
  let active = 0;
  let idle = 0;
  let totalAge = 0;
  let oldestAge = 0;
  for (const session of sessions.values()) {
    if (session.isActive) active++;
    else idle++;
    const age = now - session.startedAt;
    totalAge += age;
    if (age > oldestAge) oldestAge = age;
  }
  return { totalSessions: sessions.size, activeSessions: active, idleSessions: idle, oldestSessionAge: sessions.size > 0 ? oldestAge : null, averageSessionAge: sessions.size > 0 ? Math.round(totalAge / sessions.size) : null };
}
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, cleaning up token sessions...', { component: 'TokenManager' });
  shutdownTokenManager();
});
process.on('SIGINT', () => {
  logger.info('SIGINT received, cleaning up token sessions...', { component: 'TokenManager' });
  shutdownTokenManager();
});
export default { encryptToken, decryptToken, validateDiscordToken, startTokenSession, stopTokenSession, getTokenSession, getTokenSessionRaw, isSessionActive, getAllSessions, getActiveSessionsCount, getTotalSessionsCount, updateSessionActivity, restoreTokenSessionsFromDatabase, shutdownTokenManager, getSessionStats };
