/**
 * @module index
 *
 * Stable application bootstrap.
 *
 * Goals:
 * - Start the database, bot, tracker accounts and AutoJoiner once.
 * - Keep VRFS middleware available without allowing it to control process lifetime.
 * - Do not perform automatic boot loops.
 * - Do not aggressively clear GiveawayManager internals.
 * - Detect sustained process-wide failure and exit once so PM2 can restart us.
 * - Detect sustained memory pressure before the OS kills the process.
 * - Keep health endpoints read-only.
 */

import http from 'node:http';
import { Client } from 'discord.js-selfbot-v13';
import type { Message } from 'discord.js-selfbot-v13';
import 'dotenv/config';

import { CONFIG } from './config.js';
import { logger, reconfigureLogger } from './logger.js';
import GiveawayManager from './giveawayManager.js';
import { BotManager } from './bot.js';
import { delay, formatError } from './utils.js';
import {
  getDb,
  closeDb,
  cleanupOldGiveaways,
  getScrimStats,
} from './database.js';
import { AutoJoinManager } from './autoJoin/index.js';
import {
  vrfs,
  seby,
  getStatus as getVRFSStatus,
} from './middleware/api/vrfs.js';

// ============================================================================
// CONSTANTS
// ============================================================================

const PORT = Number.parseInt(process.env.PORT ?? '3000', 10) || 3000;

// Startup
const DATABASE_TIMEOUT_MS = 15_000;
const CLIENT_READY_TIMEOUT_MS = 60_000;
const BOT_START_TIMEOUT_MS = 30_000;

// Runtime monitoring
const WATCHDOG_INTERVAL_MS = 30_000;
const STATS_INTERVAL_MS = CONFIG.statsIntervalMs;

// A temporary spike is fine.
// We only react when the condition persists.
const MEMORY_WARNING_RSS_MB = 4_500;
const MEMORY_RESTART_RSS_MB = 5_500;

const MEMORY_RESTART_CONFIRMATIONS = 3;

// "Everything is dead" must remain dead for several checks.
// This prevents a temporary Discord outage/reconnect from restarting us.
const GLOBAL_FAILURE_CONFIRMATIONS = 4;

// Do not restart merely because there have been no giveaway messages.
// We only use connection/process health here.
const MAX_EVENT_LOOP_LAG_MS = 15_000;
const EVENT_LOOP_LAG_CONFIRMATIONS = 4;

// Shutdown protection
const SHUTDOWN_TIMEOUT_MS = 15_000;

// ============================================================================
// STATE
// ============================================================================

let activeManagers: GiveawayManager[] = [];
let botManager: BotManager | null = null;
let autoJoiner: AutoJoinManager | null = null;

let statsInterval: NodeJS.Timeout | null = null;
let watchdogInterval: NodeJS.Timeout | null = null;

let shuttingDown = false;
let shutdownStarted = false;
let restartRequested = false;

let memoryFailureCount = 0;
let globalFailureCount = 0;
let eventLoopFailureCount = 0;

let lastWatchdogAt = Date.now();
let lastVRFSStatus: Record<string, unknown> = {
  available: false,
};

let lastWatchdogState = {
  healthy: true,
  reason: 'starting',
  timestamp: new Date().toISOString(),
};

// ============================================================================
// MEMORY
// ============================================================================

function getMemoryUsage() {
  const memory = process.memoryUsage();

  return {
    rssMB: Math.round(memory.rss / 1024 / 1024),
    heapUsedMB: Math.round(memory.heapUsed / 1024 / 1024),
    heapTotalMB: Math.round(memory.heapTotal / 1024 / 1024),
    externalMB: Math.round(memory.external / 1024 / 1024),
    arrayBuffersMB: Math.round(memory.arrayBuffers / 1024 / 1024),
  };
}

// ============================================================================
// TIMEOUT HELPER
// ============================================================================

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  description: string,
): Promise<T> {
  let timeout: NodeJS.Timeout | null = null;

  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => {
          reject(new Error(`${description} timed out after ${timeoutMs}ms`));
        }, timeoutMs);

        timeout.unref?.();
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

// ============================================================================
// HEALTH SERVER
// ============================================================================

/**
 * Read-only local health endpoint.
 *
 * Deliberately does NOT expose:
 * - shutdown
 * - GC control
 * - tokens
 * - configuration secrets
 */
const healthServer = http.createServer((req, res) => {
  if (req.method !== 'GET' || (req.url !== '/' && req.url !== '/health')) {
    res.writeHead(404);
    res.end('Not found');
    return;
  }

  const memory = getMemoryUsage();

  let autoJoinStats = {
    activeSessions: 0,
    totalSessions: 0,
  };

  if (autoJoiner) {
    try {
      const stats = autoJoiner.getStats();

      autoJoinStats = {
        activeSessions: Number(stats.activeSessions) || 0,
        totalSessions: Number(stats.totalSessions) || 0,
      };
    } catch {
      // Health endpoint must never crash because AutoJoiner is unavailable.
    }
  }

  const response = {
    status: shuttingDown ? 'shutting_down' : 'ok',
    uptimeSeconds: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),

    memory,

    tracker: {
      activeManagers: activeManagers.length,
    },

    autoJoiner: autoJoinStats,

    bot: {
      running: botManager !== null,
    },

    vrfs: lastVRFSStatus,

    watchdog: lastWatchdogState,
  };

  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });

  res.end(JSON.stringify(response));
});

// Bind locally.
// This avoids exposing administrative endpoints publicly.
healthServer.listen(PORT, '127.0.0.1', () => {
  logger.info(`Health server listening on 127.0.0.1:${PORT}`, {
    component: 'Bootstrap',
  });
});

healthServer.on('error', (error) => {
  logger.error('Health server error', {
    component: 'Bootstrap',
    error: formatError(error),
  });
});

// ============================================================================
// GLOBAL ERROR HANDLERS
// ============================================================================

process.on('uncaughtException', (error) => {
  console.error('[Process] Uncaught exception:', error);

  try {
    logger.error('Uncaught exception', {
      component: 'Process',
      error: formatError(error),
    });
  } catch {
    // Logger must never prevent process recovery.
  }

  /**
   * Do NOT attempt to continue after an uncaught exception.
   *
   * Continuing can leave Discord sockets, timers or database state
   * partially broken.
   *
   * Exit once. PM2 should be responsible for restarting the process.
   */
  void requestRestart('uncaught_exception');
});

process.on('unhandledRejection', (reason) => {
  console.error('[Process] Unhandled rejection:', reason);

  try {
    logger.error('Unhandled promise rejection', {
      component: 'Process',
      error: formatError(reason),
    });
  } catch {
    // Ignore logger failures.
  }

  /**
   * An individual rejected promise does not mean the entire application
   * is broken, so we deliberately do NOT restart here.
   */
});

// ============================================================================
// DISCORD CLIENT HEALTH
// ============================================================================

function getManagerClient(manager: GiveawayManager): Client | null {
  try {
    const value = (manager as any).client;

    return value instanceof Client ? value : value ?? null;
  } catch {
    return null;
  }
}

function isClientHealthy(client: Client): boolean {
  try {
    if (!client.user) {
      return false;
    }

    const ws = (client as any).ws;

    /**
     * discord.js-selfbot-v13 exposes WebSocket state through the manager.
     *
     * We intentionally keep this defensive because the library's internals
     * can vary between releases.
     */
    if (ws) {
      const status = ws.status;

      // 0 = READY in discord.js WebSocketStatus.
      if (typeof status === 'number') {
        return status === 0;
      }

      const connection = ws.connection;

      if (connection?.readyState !== undefined) {
        // WebSocket.OPEN
        return connection.readyState === 1;
      }
    }

    /**
     * If we cannot inspect the websocket internals, a logged-in client
     * is still considered healthy rather than falsely triggering a restart.
     */
    return true;
  } catch {
    return false;
  }
}

function getTrackerHealth() {
  if (activeManagers.length === 0) {
    return {
      healthy: false,
      healthyManagers: 0,
      totalManagers: 0,
    };
  }

  let healthyManagers = 0;

  for (const manager of activeManagers) {
    const client = getManagerClient(manager);

    if (client && isClientHealthy(client)) {
      healthyManagers++;
    }
  }

  return {
    healthy: healthyManagers > 0,
    healthyManagers,
    totalManagers: activeManagers.length,
  };
}

// ============================================================================
// EVENT LOOP MONITOR
// ============================================================================

function checkEventLoopLag(): number {
  const now = Date.now();
  const expected = lastWatchdogAt + WATCHDOG_INTERVAL_MS;

  lastWatchdogAt = now;

  return Math.max(0, now - expected);
}

// ============================================================================
// WATCHDOG
// ============================================================================

async function watchdog(): Promise<void> {
  if (shuttingDown) {
    return;
  }

  const memory = getMemoryUsage();
  const tracker = getTrackerHealth();
  const eventLoopLag = checkEventLoopLag();

  // --------------------------------------------------------------------------
  // MEMORY
  // --------------------------------------------------------------------------

  if (memory.rssMB >= MEMORY_RESTART_RSS_MB) {
    memoryFailureCount++;

    logger.warn('High process RSS detected', {
      component: 'Watchdog',
      rssMB: memory.rssMB,
      heapUsedMB: memory.heapUsedMB,
      confirmation: `${memoryFailureCount}/${MEMORY_RESTART_CONFIRMATIONS}`,
    });

    if (memoryFailureCount >= MEMORY_RESTART_CONFIRMATIONS) {
      await requestRestart(
        `sustained_high_memory_rss_${memory.rssMB}MB`,
      );

      return;
    }
  } else {
    if (memory.rssMB >= MEMORY_WARNING_RSS_MB) {
      logger.warn('Memory usage elevated', {
        component: 'Watchdog',
        rssMB: memory.rssMB,
        heapUsedMB: memory.heapUsedMB,
      });
    }

    memoryFailureCount = 0;
  }

  // --------------------------------------------------------------------------
  // EVENT LOOP
  // --------------------------------------------------------------------------

  if (eventLoopLag >= MAX_EVENT_LOOP_LAG_MS) {
    eventLoopFailureCount++;

    logger.warn('Event loop lag detected', {
      component: 'Watchdog',
      lagMs: eventLoopLag,
      confirmation: `${eventLoopFailureCount}/${EVENT_LOOP_LAG_CONFIRMATIONS}`,
    });

    if (eventLoopFailureCount >= EVENT_LOOP_LAG_CONFIRMATIONS) {
      await requestRestart(
        `sustained_event_loop_lag_${eventLoopLag}ms`,
      );

      return;
    }
  } else {
    eventLoopFailureCount = 0;
  }

  // --------------------------------------------------------------------------
  // DISCORD
  // --------------------------------------------------------------------------

  if (!tracker.healthy) {
    globalFailureCount++;

    logger.warn('No healthy tracker clients detected', {
      component: 'Watchdog',
      healthyManagers: tracker.healthyManagers,
      totalManagers: tracker.totalManagers,
      confirmation: `${globalFailureCount}/${GLOBAL_FAILURE_CONFIRMATIONS}`,
    });

    /**
     * IMPORTANT:
     *
     * We do not restart immediately.
     *
     * Discord can disconnect/reconnect temporarily, especially during
     * periods of high activity.
     */
    if (globalFailureCount >= GLOBAL_FAILURE_CONFIRMATIONS) {
      await requestRestart('all_tracker_clients_unhealthy');

      return;
    }
  } else {
    globalFailureCount = 0;
  }

  // --------------------------------------------------------------------------
  // STATE
  // --------------------------------------------------------------------------

  lastWatchdogState = {
    healthy:
      tracker.healthy &&
      memory.rssMB < MEMORY_RESTART_RSS_MB &&
      eventLoopFailureCount === 0,

    reason: tracker.healthy
      ? 'healthy'
      : 'no_healthy_tracker_clients',

    timestamp: new Date().toISOString(),
  };
}

// ============================================================================
// RESTART REQUEST
// ============================================================================

async function requestRestart(reason: string): Promise<void> {
  if (restartRequested || shuttingDown) {
    return;
  }

  restartRequested = true;

  logger.error('Process restart requested', {
    component: 'Watchdog',
    reason,
    memory: getMemoryUsage(),
  });

  /**
   * Clean shutdown first.
   *
   * Then exit with code 1.
   *
   * PM2 should see the non-zero exit and restart the process.
   */
  try {
    await shutdown(`watchdog:${reason}`);
  } catch (error) {
    console.error('[Watchdog] Shutdown failed:', error);
  }

  process.exitCode = 1;
  process.exit(1);
}

// ============================================================================
// VRFS
// ============================================================================

function initializeVRFS(): void {
  try {
    lastVRFSStatus = {
      available: true,
      ...getVRFSStatus(),
    };

    logger.info('VRFS middleware loaded', {
      component: 'VRFS',
    });
  } catch (error) {
    /**
     * VRFS is optional infrastructure.
     *
     * It must never prevent the Discord tracker from starting.
     */
    lastVRFSStatus = {
      available: false,
      error: formatError(error),
    };

    logger.warn('VRFS middleware unavailable', {
      component: 'VRFS',
      error: formatError(error),
    });
  }
}

// ============================================================================
// START DATABASE
// ============================================================================

async function startDatabase(): Promise<void> {
  await withTimeout(
    getDb(),
    DATABASE_TIMEOUT_MS,
    'Database connection',
  );

  logger.info('Database connected', {
    component: 'Database',
  });

  void cleanupOldGiveaways(30).catch((error) => {
    logger.warn('Old giveaway cleanup failed', {
      component: 'Database',
      error: formatError(error),
    });
  });
}

// ============================================================================
// START BOT
// ============================================================================

async function startBot(): Promise<void> {
  if (!CONFIG.botToken) {
    logger.warn('No bot token configured; continuing without BotManager', {
      component: 'Bot',
    });

    return;
  }

  const manager = new BotManager(CONFIG.botToken);

  try {
    await withTimeout(
      manager.start(),
      BOT_START_TIMEOUT_MS,
      'BotManager.start()',
    );

    botManager = manager;

    logger.info('BotManager started', {
      component: 'Bot',
    });
  } catch (error) {
    /**
     * BotManager is not allowed to prevent tracker accounts from starting.
     */
    logger.warn('BotManager failed to start; continuing without it', {
      component: 'Bot',
      error: formatError(error),
    });

    try {
      await manager.destroy();
    } catch {
      // Ignore cleanup errors.
    }

    botManager = null;
  }
}

// ============================================================================
// TRACKER CLIENT
// ============================================================================

function registerDiscordEvents(
  client: Client,
  manager: GiveawayManager,
  label: string,
): void {
  client.setMaxListeners(30);

  const messageCreateHandler = (message: Message) => {
    if (shuttingDown || !message.guild) {
      return;
    }

    void manager.handleMessage(message).catch((error) => {
      logger.error('messageCreate handler failed', {
        component: 'Discord',
        account: label,
        messageId: message.id,
        error: formatError(error),
      });
    });
  };

  const messageUpdateHandler = (
    _oldMessage: unknown,
    updatedMessage: unknown,
  ) => {
    if (shuttingDown) {
      return;
    }

    const message = updatedMessage as Message;

    if (!message?.id || !message?.channel) {
      return;
    }

    void manager.handleMessage(message).catch((error) => {
      logger.error('messageUpdate handler failed', {
        component: 'Discord',
        account: label,
        messageId: message.id,
        error: formatError(error),
      });
    });
  };

  const guildCreateHandler = (guild: any) => {
    if (shuttingDown) {
      return;
    }

    logger.info('Joined server', {
      component: 'Discord',
      account: label,
      guildId: guild.id,
      guildName: guild.name,
    });
  };

  const guildDeleteHandler = (guild: any) => {
    if (shuttingDown) {
      return;
    }

    logger.info('Left server', {
      component: 'Discord',
      account: label,
      guildId: guild.id,
      guildName: guild.name,
    });

    try {
      manager.clearInviteCache(guild.id);
    } catch {
      // Cache cleanup must never break the client.
    }
  };

  const disconnectHandler = () => {
    if (shuttingDown) {
      return;
    }

    logger.warn('Discord client disconnected', {
      component: 'Discord',
      account: label,
    });
  };

  const reconnectingHandler = () => {
    if (shuttingDown) {
      return;
    }

    logger.info('Discord client reconnecting', {
      component: 'Discord',
      account: label,
    });
  };

  const errorHandler = (error: Error) => {
    if (shuttingDown) {
      return;
    }

    logger.error('Discord client error', {
      component: 'Discord',
      account: label,
      error: formatError(error),
    });
  };

  (manager as any)._handlers = {
    messageCreate: messageCreateHandler,
    messageUpdate: messageUpdateHandler,
    guildCreate: guildCreateHandler,
    guildDelete: guildDeleteHandler,
    disconnect: disconnectHandler,
    reconnecting: reconnectingHandler,
    error: errorHandler,
  };

  client.on('messageCreate', messageCreateHandler);
  client.on('messageUpdate', messageUpdateHandler);
  client.on('guildCreate', guildCreateHandler);
  client.on('guildDelete', guildDeleteHandler);
  client.on('disconnect', disconnectHandler);
  client.on('reconnecting', reconnectingHandler);
  client.on('error', errorHandler);
}

async function startTrackerClient(
  token: string,
  index: number,
): Promise<GiveawayManager | null> {
  const label = `acc${index + 1}`;

  if (!token?.trim()) {
    logger.warn('Empty tracker token skipped', {
      component: 'Bootstrap',
      account: label,
    });

    return null;
  }

  const client = new Client();

  client.setMaxListeners(30);

  client.once('ready', () => {
    logger.info('Tracker account ready', {
      component: 'Discord',
      account: label,
      userId: client.user?.id,
      username: client.user?.username,
      guilds: client.guilds.cache.size,
    });
  });

  client.on('error', (error) => {
    logger.error('Tracker account error', {
      component: 'Discord',
      account: label,
      error: formatError(error),
    });
  });

  if (CONFIG.logLevel === 'debug') {
    client.on('debug', (message) => {
      logger.debug(`[${label}] ${message}`, {
        component: 'Discord',
      });
    });
  }

  const manager = new GiveawayManager(
    client,
    logger,
    token,
    label,
    botManager,
  );

  registerDiscordEvents(client, manager, label);

  try {
    await withTimeout(
      client.login(token),
      CLIENT_READY_TIMEOUT_MS,
      `${label} login`,
    );

    /**
     * login() resolving is not always enough to guarantee the ready event
     * has populated the client state, so verify it here.
     */
    if (!client.user) {
      throw new Error(`${label} login completed without a ready user`);
    }

    activeManagers.push(manager);

    logger.info('Tracker account connected', {
      component: 'Bootstrap',
      account: label,
      userId: client.user.id,
      guilds: client.guilds.cache.size,
      memory: getMemoryUsage(),
    });

    return manager;
  } catch (error) {
    logger.error('Tracker account failed to start', {
      component: 'Bootstrap',
      account: label,
      error: formatError(error),
    });

    try {
      client.removeAllListeners();
      await client.destroy();
    } catch {
      // Ignore cleanup failure.
    }

    return null;
  }
}

// ============================================================================
// START ALL TRACKERS
// ============================================================================

async function startTrackers(): Promise<void> {
  const tokens = CONFIG.tokens.filter(
    (token) => typeof token === 'string' && token.trim().length > 0,
  );

  if (tokens.length === 0) {
    throw new Error('No tracker tokens configured');
  }

  let started = 0;

  /**
   * Keep startup controlled.
   *
   * Three accounts at once is enough to avoid creating a huge connection
   * burst while still making startup reasonably quick.
   */
  const BATCH_SIZE = 3;

  for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
    if (shuttingDown) {
      throw new Error('Shutdown requested during tracker startup');
    }

    const batch = tokens.slice(i, i + BATCH_SIZE);

    const results = await Promise.all(
      batch.map((token, offset) =>
        startTrackerClient(token, i + offset),
      ),
    );

    started += results.filter(Boolean).length;

    if (i + BATCH_SIZE < tokens.length) {
      await delay(1_000);
    }
  }

  if (started === 0) {
    throw new Error('No tracker accounts successfully started');
  }

  logger.info('Tracker accounts started', {
    component: 'Bootstrap',
    accounts: started,
    memory: getMemoryUsage(),
  });
}

// ============================================================================
// AUTOJOINER
// ============================================================================

async function startAutoJoiner(): Promise<void> {
  try {
    const manager = new AutoJoinManager();

    /**
     * AutoJoiner failure must not take down the tracker.
     */
    await manager.startAllSessions();

    try {
      await manager.restoreSessionsFromDatabase();
    } catch (error) {
      logger.warn('AutoJoiner session restore failed', {
        component: 'AutoJoiner',
        error: formatError(error),
      });
    }

    autoJoiner = manager;

    const stats = manager.getStats();

    logger.info('AutoJoiner started', {
      component: 'AutoJoiner',
      activeSessions: stats.activeSessions,
      totalSessions: stats.totalSessions,
    });
  } catch (error) {
    logger.warn('AutoJoiner unavailable; continuing without it', {
      component: 'AutoJoiner',
      error: formatError(error),
    });

    autoJoiner = null;
  }
}

// ============================================================================
// PERIODIC STATS
// ============================================================================

function startStatsLogging(): void {
  if (statsInterval) {
    return;
  }

  statsInterval = setInterval(() => {
    if (shuttingDown) {
      return;
    }

    for (const manager of activeManagers) {
      try {
        manager.logStats();
      } catch (error) {
        logger.warn('Manager stats failed', {
          component: 'Stats',
          error: formatError(error),
        });
      }
    }

    if (autoJoiner) {
      try {
        const stats = autoJoiner.getStats();

        logger.info('AutoJoiner status', {
          component: 'Stats',
          activeSessions: stats.activeSessions,
          totalSessions: stats.totalSessions,
          memory: getMemoryUsage(),
        });
      } catch {
        // Stats must never affect the application.
      }
    }

    logger.debug('Runtime stats', {
      component: 'Stats',
      memory: getMemoryUsage(),
      activeManagers: activeManagers.length,
    });
  }, STATS_INTERVAL_MS);

  statsInterval.unref?.();
}

// ============================================================================
// WATCHDOG
// ============================================================================

function startWatchdog(): void {
  if (watchdogInterval) {
    return;
  }

  lastWatchdogAt = Date.now();

  watchdogInterval = setInterval(() => {
    void watchdog().catch((error) => {
      logger.error('Watchdog error', {
        component: 'Watchdog',
        error: formatError(error),
      });
    });
  }, WATCHDOG_INTERVAL_MS);

  watchdogInterval.unref?.();

  logger.info('Watchdog started', {
    component: 'Watchdog',
    intervalMs: WATCHDOG_INTERVAL_MS,
  });
}

// ============================================================================
// SHUTDOWN
// ============================================================================

async function shutdown(reason: string): Promise<void> {
  if (shutdownStarted) {
    return;
  }

  shutdownStarted = true;
  shuttingDown = true;

  logger.warn('Beginning graceful shutdown', {
    component: 'Shutdown',
    reason,
  });

  // Stop timers first.
  if (statsInterval) {
    clearInterval(statsInterval);
    statsInterval = null;
  }

  if (watchdogInterval) {
    clearInterval(watchdogInterval);
    watchdogInterval = null;
  }

  // --------------------------------------------------------------------------
  // Tracker managers
  // --------------------------------------------------------------------------

  const managers = [...activeManagers];
  activeManagers = [];

  await Promise.race([
    Promise.all(
      managers.map(async (manager) => {
        try {
          await manager.shutdown();
        } catch (error) {
          logger.warn('Manager shutdown failed', {
            component: 'Shutdown',
            error: formatError(error),
          });
        }

        try {
          const client = getManagerClient(manager);

          if (client) {
            const handlers = (manager as any)._handlers;

            if (handlers) {
              for (const [event, handler] of Object.entries(handlers)) {
                try {
                  client.off(event, handler as any);
                } catch {
                  // Ignore listener cleanup errors.
                }
              }
            }

            client.removeAllListeners();

            try {
              await client.destroy();
            } catch {
              // Ignore Discord destroy errors.
            }
          }
        } catch {
          // Ignore cleanup errors.
        }
      }),
    ),
    delay(SHUTDOWN_TIMEOUT_MS),
  ]);

  // --------------------------------------------------------------------------
  // AutoJoiner
  // --------------------------------------------------------------------------

  if (autoJoiner) {
    const manager = autoJoiner;
    autoJoiner = null;

    try {
      await Promise.race([
        manager.shutdown(),
        delay(SHUTDOWN_TIMEOUT_MS / 2),
      ]);
    } catch (error) {
      logger.warn('AutoJoiner shutdown failed', {
        component: 'Shutdown',
        error: formatError(error),
      });
    }
  }

  // --------------------------------------------------------------------------
  // Bot
  // --------------------------------------------------------------------------

  if (botManager) {
    const manager = botManager;
    botManager = null;

    try {
      await Promise.race([
        manager.destroy(),
        delay(SHUTDOWN_TIMEOUT_MS / 2),
      ]);
    } catch (error) {
      logger.warn('BotManager shutdown failed', {
        component: 'Shutdown',
        error: formatError(error),
      });
    }
  }

  // --------------------------------------------------------------------------
  // VRFS
  // --------------------------------------------------------------------------

  try {
    vrfs.clearCaches();
    seby.clearFlights();
  } catch (error) {
    logger.warn('VRFS cache cleanup failed', {
      component: 'Shutdown',
      error: formatError(error),
    });
  }

  // --------------------------------------------------------------------------
  // Database
  // --------------------------------------------------------------------------

  try {
    await Promise.race([
      closeDb(),
      delay(SHUTDOWN_TIMEOUT_MS / 2),
    ]);
  } catch (error) {
    logger.warn('Database shutdown failed', {
      component: 'Shutdown',
      error: formatError(error),
    });
  }

  // --------------------------------------------------------------------------
  // Health server
  // --------------------------------------------------------------------------

  try {
    healthServer.close();
  } catch {
    // Ignore.
  }

  logger.warn('Graceful shutdown complete', {
    component: 'Shutdown',
    reason,
    memory: getMemoryUsage(),
  });
}

// ============================================================================
// SIGNALS
// ============================================================================

process.once('SIGINT', () => {
  void shutdown('SIGINT').finally(() => {
    process.exit(0);
  });
});

process.once('SIGTERM', () => {
  void shutdown('SIGTERM').finally(() => {
    process.exit(0);
  });
});

// ============================================================================
// MAIN
// ============================================================================

async function main(): Promise<void> {
  reconfigureLogger(CONFIG.logLevel, CONFIG.logDir);

  logger.info('Starting UNTITLED tracker', {
    component: 'Bootstrap',
    node: process.version,
    pid: process.pid,
    memory: getMemoryUsage(),
  });

  // --------------------------------------------------------------------------
  // VRFS
  // --------------------------------------------------------------------------

  initializeVRFS();

  // --------------------------------------------------------------------------
  // Database
  // --------------------------------------------------------------------------

  await startDatabase();

  // --------------------------------------------------------------------------
  // Real Discord bot
  // --------------------------------------------------------------------------

  await startBot();

  // --------------------------------------------------------------------------
  // Tracker accounts
  // --------------------------------------------------------------------------

  await startTrackers();

  // --------------------------------------------------------------------------
  // AutoJoiner
  // --------------------------------------------------------------------------

  await startAutoJoiner();

  // --------------------------------------------------------------------------
  // Stats
  // --------------------------------------------------------------------------

  try {
    const scrimStats = await getScrimStats();

    logger.info('Initial scrim statistics', {
      component: 'Stats',
      total: scrimStats.total,
      active: scrimStats.active,
      servers: scrimStats.servers,
      scrims: scrimStats.byType.scrim,
      squidGames: scrimStats.byType.squid_game,
      gagaballs: scrimStats.byType.gagaball,
    });
  } catch (error) {
    logger.debug('Initial scrim statistics unavailable', {
      component: 'Stats',
      error: formatError(error),
    });
  }

  startStatsLogging();
  startWatchdog();

  logger.info('Tracker is live', {
    component: 'Bootstrap',
    accounts: activeManagers.length,
    memory: getMemoryUsage(),
  });
}

// ============================================================================
// START
// ============================================================================

void main().catch(async (error) => {
  console.error('[Bootstrap] Fatal startup error:', error);

  try {
    logger.error('Fatal startup error', {
      component: 'Bootstrap',
      error: formatError(error),
    });
  } catch {
    // Ignore logging failure.
  }

  await shutdown('startup_failure');

  process.exit(1);
});
