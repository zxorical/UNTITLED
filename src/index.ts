/**
 * @module index
 * Main application entry point.
 *
 * Responsibilities:
 * - Start the database
 * - Start the Discord bot
 * - Start tracker accounts
 * - Start AutoJoiner
 * - Expose a read-only health endpoint
 * - Handle graceful shutdown
 *
 * VRFS middleware is kept, but index.ts does not poll the VRFS API.
 * VRFS request/caching logic remains inside middleware/api/vrfs.ts.
 */

import http from 'http';
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

// Keep VRFS middleware.
// index.ts only reads its status; actual API logic stays in vrfs.ts.
import { vrfs, seby, getStatus as getVRFSStatus } from './middleware/api/vrfs.js';

// -----------------------------------------------------------------------------
// STATE
// -----------------------------------------------------------------------------

let activeManagers: GiveawayManager[] = [];
let botManager: BotManager | null = null;
let autoJoiner: AutoJoinManager | null = null;

let statsInterval: NodeJS.Timeout | null = null;
let memoryInterval: NodeJS.Timeout | null = null;

let shuttingDown = false;
let shutdownStarted = false;

// -----------------------------------------------------------------------------
// CONFIG
// -----------------------------------------------------------------------------

const PORT = Number.parseInt(process.env.PORT ?? '3000', 10) || 3000;

const CLIENT_READY_TIMEOUT_MS = 60_000;
const BOT_START_TIMEOUT_MS = 15_000;
const AUTOJOIN_START_TIMEOUT_MS = 60_000;
const AUTOJOIN_RESTORE_TIMEOUT_MS = 30_000;

const SHUTDOWN_TIMEOUT_MS = 10_000;

const MEMORY_WARNING_MB = 3500;
const MEMORY_CRITICAL_MB = 5000;

// -----------------------------------------------------------------------------
// MEMORY
// -----------------------------------------------------------------------------

function getMemoryUsage() {
  const memory = process.memoryUsage();

  return {
    heapUsedMB: Math.round(memory.heapUsed / 1024 / 1024),
    heapTotalMB: Math.round(memory.heapTotal / 1024 / 1024),
    rssMB: Math.round(memory.rss / 1024 / 1024),
    externalMB: Math.round(memory.external / 1024 / 1024),
  };
}

function checkMemory(): void {
  if (shuttingDown) return;

  const memory = getMemoryUsage();

  if (memory.heapUsedMB >= MEMORY_CRITICAL_MB) {
    logger.warn('High memory usage detected', {
      component: 'Memory',
      memory,
    });

    /*
     * Do NOT terminate the entire application here.
     *
     * The old index.ts called process.exit() when memory became high.
     * That can make PM2 look like the bot randomly died.
     *
     * Garbage collection is only requested when Node exposes it.
     */
    if (global.gc) {
      global.gc();
    }

    return;
  }

  if (memory.heapUsedMB >= MEMORY_WARNING_MB) {
    logger.warn('Memory usage elevated', {
      component: 'Memory',
      memory,
    });
  }
}

// -----------------------------------------------------------------------------
// HEALTH SERVER
// -----------------------------------------------------------------------------

const healthServer = http.createServer((req, res) => {
  /*
   * Only expose a read-only health endpoint.
   *
   * There is intentionally NO:
   *   /shutdown
   *   /gc
   *
   * Neither operation should be remotely accessible.
   */

  if (req.method !== 'GET') {
    res.writeHead(405, {
      'Content-Type': 'text/plain; charset=utf-8',
    });

    res.end('Method Not Allowed');
    return;
  }

  if (req.url !== '/' && req.url !== '/health') {
    res.writeHead(404, {
      'Content-Type': 'text/plain; charset=utf-8',
    });

    res.end('Not Found');
    return;
  }

  let vrfsStatus: Record<string, unknown> = {
    available: false,
  };

  try {
    vrfsStatus = {
      available: true,
      ...getVRFSStatus(),
    };
  } catch (error) {
    vrfsStatus = {
      available: false,
      error: formatError(error),
    };
  }

  let autoJoinStats = {
    activeSessions: 0,
    totalSessions: 0,
  };

  if (autoJoiner) {
    try {
      const stats = autoJoiner.getStats();

      autoJoinStats = {
        activeSessions: stats.activeSessions ?? 0,
        totalSessions: stats.totalSessions ?? 0,
      };
    } catch {
      // Health endpoint should never crash because of stats.
    }
  }

  const response = {
    status: shuttingDown ? 'shutting_down' : 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),

    memory: getMemoryUsage(),

    tracker: {
      activeManagers: activeManagers.length,
    },

    autoJoiner: autoJoinStats,

    botManager: {
      running: botManager !== null,
    },

    vrfs: vrfsStatus,
  };

  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });

  res.end(JSON.stringify(response));
});

healthServer.on('error', (error) => {
  logger.error('Health server error', {
    component: 'Health',
    error: formatError(error),
  });
});

healthServer.listen(PORT, '0.0.0.0', () => {
  logger.info(`Health server listening on port ${PORT}`, {
    component: 'Health',
  });
});

// -----------------------------------------------------------------------------
// ERROR HANDLERS
// -----------------------------------------------------------------------------

process.on('uncaughtException', (error) => {
  console.error('UNCAUGHT EXCEPTION:', error);

  try {
    logger.error('Uncaught exception', {
      component: 'Process',
      error: formatError(error),
    });
  } catch {
    // Logging must never cause another exception.
  }

  /*
   * An uncaught exception means the process may no longer be trustworthy.
   * Let the process exit instead of pretending everything is healthy.
   *
   * PM2/systemd/etc. can restart it.
   */
  if (!shuttingDown) {
    void shutdown('uncaughtException').finally(() => {
      process.exit(1);
    });
  }
});

process.on('unhandledRejection', (reason) => {
  console.error('UNHANDLED REJECTION:', reason);

  try {
    logger.error('Unhandled rejection', {
      component: 'Process',
      error: formatError(reason),
    });
  } catch {
    // Ignore logging failure.
  }
});

process.setMaxListeners(100);

// -----------------------------------------------------------------------------
// DISCORD EVENTS
// -----------------------------------------------------------------------------

function registerDiscordEvents(
  client: Client,
  manager: GiveawayManager,
): void {
  const messageCreateHandler = (message: Message) => {
    if (shuttingDown || !message.guild) return;

    void manager.handleMessage(message).catch((error) => {
      logger.error('messageCreate handler failed', {
        component: 'Events',
        messageId: message.id,
        error: formatError(error),
      });
    });
  };

  const messageUpdateHandler = (
    _oldMessage: unknown,
    updatedMessage: Message,
  ) => {
    if (
      shuttingDown ||
      !updatedMessage?.id ||
      !updatedMessage?.channel
    ) {
      return;
    }

    void manager.handleMessage(updatedMessage).catch((error) => {
      logger.error('messageUpdate handler failed', {
        component: 'Events',
        messageId: updatedMessage.id,
        error: formatError(error),
      });
    });
  };

  const guildCreateHandler = (guild: any) => {
    if (shuttingDown) return;

    logger.info('Joined server', {
      component: 'Events',
      guildId: guild.id,
      guildName: guild.name,
    });
  };

  const guildDeleteHandler = (guild: any) => {
    if (shuttingDown) return;

    logger.info('Left server', {
      component: 'Events',
      guildId: guild.id,
      guildName: guild.name,
    });

    try {
      manager.clearInviteCache(guild.id);
    } catch (error) {
      logger.warn('Failed to clear invite cache', {
        component: 'Events',
        guildId: guild.id,
        error: formatError(error),
      });
    }
  };

  const disconnectHandler = () => {
    if (shuttingDown) return;

    logger.warn('Discord client disconnected', {
      component: 'Events',
    });
  };

  const reconnectingHandler = () => {
    if (shuttingDown) return;

    logger.info('Discord client reconnecting', {
      component: 'Events',
    });
  };

  const errorHandler = (error: Error) => {
    if (shuttingDown) return;

    logger.error('Discord client error', {
      component: 'Events',
      error: formatError(error),
    });
  };

  /*
   * Store handlers so shutdown can remove exactly the listeners
   * created by this file.
   */
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

// -----------------------------------------------------------------------------
// CLIENT LOGIN
// -----------------------------------------------------------------------------

function waitForReady(
  client: Client,
  token: string,
  label: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let finished = false;

    const cleanup = () => {
      client.off('ready', readyHandler);
      client.off('error', errorHandler);
    };

    const finishResolve = () => {
      if (finished) return;

      finished = true;
      cleanup();

      logger.info(`${label} ready`, {
        component: 'Client',
      });

      resolve();
    };

    const finishReject = (error: unknown) => {
      if (finished) return;

      finished = true;
      cleanup();

      reject(error);
    };

    const readyHandler = () => {
      finishResolve();
    };

    const errorHandler = (error: Error) => {
      finishReject(error);
    };

    client.once('ready', readyHandler);
    client.once('error', errorHandler);

    client.login(token).catch((error) => {
      finishReject(
        new Error(`Login failed: ${formatError(error)}`),
      );
    });
  });
}

// -----------------------------------------------------------------------------
// START BOT MANAGER
// -----------------------------------------------------------------------------

async function startBotManager(): Promise<void> {
  if (!CONFIG.botToken) {
    logger.warn('No bot token configured; skipping BotManager', {
      component: 'Bootstrap',
    });

    return;
  }

  logger.info('Starting BotManager...', {
    component: 'Bootstrap',
  });

  const manager = new BotManager(CONFIG.botToken);

  /*
   * Important:
   * We don't leave a timed-out BotManager running in the background.
   *
   * The old Promise.race() could time out while botManager.start()
   * continued running.
   */
  await Promise.race([
    manager.start(),
    delay(BOT_START_TIMEOUT_MS).then(() => {
      throw new Error('BotManager startup timed out');
    }),
  ]);

  botManager = manager;

  logger.info('BotManager started', {
    component: 'Bootstrap',
  });
}

// -----------------------------------------------------------------------------
// START TRACKER ACCOUNTS
// -----------------------------------------------------------------------------

async function startTrackerAccounts(): Promise<void> {
  activeManagers = [];

  const tokens = CONFIG.tokens.filter(
    (token) => typeof token === 'string' && token.trim().length > 0,
  );

  if (tokens.length === 0) {
    throw new Error('No tracker tokens configured');
  }

  let authenticationFailures = 0;

  /*
   * Start accounts sequentially.
   *
   * This is intentionally simpler than the old batch system.
   * A small delay between accounts also avoids creating a large
   * connection spike during boot.
   */
  for (let index = 0; index < tokens.length; index++) {
    if (shuttingDown) {
      throw new Error('Startup cancelled during shutdown');
    }

    const token = tokens[index];
    const label = `acc${index + 1}`;

    logger.info(
      `Starting tracker account ${index + 1}/${tokens.length}`,
      {
        component: 'Bootstrap',
        label,
      },
    );

    let client: Client | null = null;

    try {
      client = new Client();

      client.setMaxListeners(50);

      if (CONFIG.logLevel === 'debug') {
        client.on('debug', (info) => {
          logger.debug(`[${label}] ${info}`, {
            component: 'Client',
          });
        });
      }

      client.on('ready', () => {
        logger.info(`[${label}] ready`, {
          component: 'Client',
        });
      });

      client.on('error', (error) => {
        logger.error(`[${label}] client error`, {
          component: 'Client',
          error: formatError(error),
        });
      });

      const manager = new GiveawayManager(
        client,
        logger,
        token,
        label,
        botManager,
      );

      registerDiscordEvents(client, manager);

      await Promise.race([
        waitForReady(client, token, label),
        delay(CLIENT_READY_TIMEOUT_MS).then(() => {
          throw new Error(
            `${label} did not become ready within ${CLIENT_READY_TIMEOUT_MS}ms`,
          );
        }),
      ]);

      activeManagers.push(manager);

      logger.info(`${label} connected`, {
        component: 'Bootstrap',
        userId: client.user?.id,
        username: client.user?.username,
        guilds: client.guilds.cache.size,
        memory: getMemoryUsage(),
      });
    } catch (error) {
      const message = formatError(error);

      const authenticationError =
        /token|auth|login|invalid|unauthorized|401|403/i.test(message);

      if (authenticationError) {
        authenticationFailures++;

        logger.warn(`${label} authentication failed`, {
          component: 'Bootstrap',
          error: message,
        });
      } else {
        logger.error(`${label} failed to start`, {
          component: 'Bootstrap',
          error: message,
        });
      }

      /*
       * Most importantly, destroy the client created for this attempt.
       * Otherwise a failed login can leave sockets/listeners behind.
       */
      if (client) {
        try {
          client.removeAllListeners();
          await client.destroy();
        } catch {
          // Best effort cleanup.
        }
      }
    }

    await delay(1000);
  }

  if (activeManagers.length === 0) {
    if (authenticationFailures === tokens.length) {
      const error = new Error('All tracker tokens failed authentication');

      (error as any).code = 'AUTH_ALL_FAILED';

      throw error;
    }

    throw new Error('No tracker accounts could be started');
  }

  logger.info('Tracker accounts started', {
    component: 'Bootstrap',
    active: activeManagers.length,
    configured: tokens.length,
    authenticationFailures,
  });
}

// -----------------------------------------------------------------------------
// START AUTOJOINER
// -----------------------------------------------------------------------------

async function startAutoJoiner(): Promise<void> {
  try {
    logger.info('Starting AutoJoiner...', {
      component: 'Bootstrap',
    });

    const manager = new AutoJoinManager();

    await Promise.race([
      manager.startAllSessions(),
      delay(AUTOJOIN_START_TIMEOUT_MS).then(() => {
        throw new Error('AutoJoiner startup timed out');
      }),
    ]);

    await Promise.race([
      manager.restoreSessionsFromDatabase(),
      delay(AUTOJOIN_RESTORE_TIMEOUT_MS).then(() => {
        throw new Error('AutoJoiner session restore timed out');
      }),
    ]);

    autoJoiner = manager;

    const stats = manager.getStats();

    logger.info('AutoJoiner started', {
      component: 'Bootstrap',
      activeSessions: stats.activeSessions,
      totalSessions: stats.totalSessions,
    });
  } catch (error) {
    logger.warn('AutoJoiner unavailable; continuing without it', {
      component: 'Bootstrap',
      error: formatError(error),
    });

    autoJoiner = null;
  }
}

// -----------------------------------------------------------------------------
// PERIODIC STATS
// -----------------------------------------------------------------------------

function startStats(): void {
  if (statsInterval) return;

  statsInterval = setInterval(() => {
    if (shuttingDown) return;

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

        logger.info(
          `AutoJoiner: ${stats.activeSessions}/${stats.totalSessions} active`,
          {
            component: 'Stats',
            memory: getMemoryUsage(),
          },
        );
      } catch {
        // Stats must never bring down the application.
      }
    }

    void getScrimStats()
      .then((stats) => {
        logger.info('Scrim stats', {
          component: 'Stats',
          total: stats.total,
          active: stats.active,
          servers: stats.servers,
          scrims: stats.byType.scrim,
          squidGames: stats.byType.squid_game,
          gagaballs: stats.byType.gagaball,
        });
      })
      .catch((error) => {
        logger.warn('Failed to retrieve scrim stats', {
          component: 'Stats',
          error: formatError(error),
        });
      });
  }, CONFIG.statsIntervalMs);

  statsInterval.unref();
}

function startMemoryMonitoring(): void {
  if (memoryInterval) return;

  memoryInterval = setInterval(checkMemory, 30_000);
  memoryInterval.unref();
}

// -----------------------------------------------------------------------------
// SHUTDOWN
// -----------------------------------------------------------------------------

async function shutdown(reason: string): Promise<void> {
  if (shutdownStarted) {
    return;
  }

  shutdownStarted = true;
  shuttingDown = true;

  logger.info(`Shutting down (${reason})...`, {
    component: 'Shutdown',
  });

  if (statsInterval) {
    clearInterval(statsInterval);
    statsInterval = null;
  }

  if (memoryInterval) {
    clearInterval(memoryInterval);
    memoryInterval = null;
  }

  // Stop tracker managers.
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
          const managerAny = manager as any;
          const client: Client | undefined = managerAny.client;

          if (client) {
            const handlers = managerAny._handlers;

            if (handlers) {
              for (const [event, handler] of Object.entries(handlers)) {
                try {
                  client.off(event, handler as any);
                } catch {
                  // Best effort.
                }
              }
            }

            client.removeAllListeners();

            try {
              await client.destroy();
            } catch {
              // Already destroyed.
            }
          }
        } catch {
          // Best effort.
        }
      }),
    ),

    delay(SHUTDOWN_TIMEOUT_MS),
  ]);

  // Stop AutoJoiner.
  if (autoJoiner) {
    const manager = autoJoiner;
    autoJoiner = null;

    try {
      await Promise.race([
        manager.shutdown(),
        delay(SHUTDOWN_TIMEOUT_MS),
      ]);
    } catch (error) {
      logger.warn('AutoJoiner shutdown failed', {
        component: 'Shutdown',
        error: formatError(error),
      });
    }
  }

  // Stop real Discord bot.
  if (botManager) {
    const manager = botManager;
    botManager = null;

    try {
      await Promise.race([
        manager.destroy(),
        delay(SHUTDOWN_TIMEOUT_MS),
      ]);
    } catch (error) {
      logger.warn('BotManager shutdown failed', {
        component: 'Shutdown',
        error: formatError(error),
      });
    }
  }

  // Clear VRFS middleware state.
  try {
    vrfs.clearCaches();
    seby.clearFlights();
  } catch (error) {
    logger.warn('VRFS cleanup failed', {
      component: 'Shutdown',
      error: formatError(error),
    });
  }

  // Close database.
  try {
    await closeDb();
  } catch (error) {
    logger.warn('Database shutdown failed', {
      component: 'Shutdown',
      error: formatError(error),
    });
  }

  // Close health server.
  try {
    healthServer.close();
  } catch {
    // Already closed.
  }

  logger.info('Shutdown complete', {
    component: 'Shutdown',
    memory: getMemoryUsage(),
  });
}

// -----------------------------------------------------------------------------
// SIGNALS
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// MAIN
// -----------------------------------------------------------------------------

async function main(): Promise<void> {
  reconfigureLogger(CONFIG.logLevel, CONFIG.logDir);

  logger.info('Starting UNTITLED tracker...', {
    component: 'Bootstrap',
  });

  logger.info('Configuration loaded', {
    component: 'Bootstrap',
    trackerAccounts: CONFIG.tokens.length,
    monitoredChannels: CONFIG.monitoredChannels.length || 'all',
    trackerChannel: CONFIG.trackerChannelId,
    statsIntervalMs: CONFIG.statsIntervalMs,
  });

  // ---------------------------------------------------------------------------
  // VRFS
  // ---------------------------------------------------------------------------

  /*
   * VRFS remains available to GiveawayManager and other modules.
   *
   * We intentionally do NOT perform an API health request here.
   * A temporary VRFS outage must not affect application startup.
   */
  try {
    logger.info('VRFS middleware loaded', {
      component: 'VRFS',
      status: getVRFSStatus(),
    });
  } catch (error) {
    logger.warn('Unable to read VRFS middleware status', {
      component: 'VRFS',
      error: formatError(error),
    });
  }

  // ---------------------------------------------------------------------------
  // DATABASE
  // ---------------------------------------------------------------------------

  logger.info('Connecting to database...', {
    component: 'Database',
  });

  await Promise.race([
    getDb(),
    delay(10_000).then(() => {
      throw new Error('Database connection timed out');
    }),
  ]);

  logger.info('Database connected', {
    component: 'Database',
  });

  void cleanupOldGiveaways(30).catch((error) => {
    logger.warn('Old giveaway cleanup failed', {
      component: 'Database',
      error: formatError(error),
    });
  });

  // ---------------------------------------------------------------------------
  // BOT
  // ---------------------------------------------------------------------------

  try {
    await startBotManager();
  } catch (error) {
    /*
     * The tracker can still operate without the notification bot.
     */
    logger.warn('BotManager failed to start; continuing', {
      component: 'Bootstrap',
      error: formatError(error),
    });

    botManager = null;
  }

  // ---------------------------------------------------------------------------
  // TRACKER ACCOUNTS
  // ---------------------------------------------------------------------------

  await startTrackerAccounts();

  // ---------------------------------------------------------------------------
  // AUTOJOINER
  // ---------------------------------------------------------------------------

  await startAutoJoiner();

  // ---------------------------------------------------------------------------
  // MONITORING
  // ---------------------------------------------------------------------------

  startStats();
  startMemoryMonitoring();

  logger.info('🟢 Tracker is live', {
    component: 'Bootstrap',
    trackerAccounts: activeManagers.length,
    autoJoiner: autoJoiner !== null,
    botManager: botManager !== null,
    memory: getMemoryUsage(),
  });
}

// -----------------------------------------------------------------------------
// START
// -----------------------------------------------------------------------------

void main().catch((error) => {
  logger.error('Fatal startup error', {
    component: 'Bootstrap',
    error: formatError(error),
  });

  void shutdown('startup failure').finally(() => {
    process.exit(1);
  });
});
