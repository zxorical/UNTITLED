/**
 * @module index
 * Application entry point – with BotManager timeout and fallback.
 * Now includes AutoJoiner for premium users (monitors ALL servers).
 *
 * MEMORY FIXES v2:
 * 1. REMOVED duplicate token session restore (was causing double clients)
 * 2. AutoJoiner is now the ONLY system that starts self-bot clients
 * 3. Proper cleanup of all managers on shutdown
 * 4. Forced GC when memory exceeds thresholds
 * 5. Session cleanup on boot retry
 * 6. Health check with memory metrics
 * 7. Graceful shutdown with timeout
 * 8. Destroy clients that time out during startup (was leaking sockets/listeners)
 * 9. Gate noisy `debug` event listener behind log level
 * 10. Prune GiveawayManager invite cache when the bot leaves a guild
 * 11. Memory thresholds adjusted for 8GB RAM
 * 12. Unhandled rejection handler no longer exits process
 * 13. Force GC after every shutdown
 * 14. Max listeners warning prevention
 * 15. Session cleanup on boot retry
 * 16. Scrim/Event detection stats in logging
 * 17. FIXED: AutoJoiner starts AFTER BotManager, not before
 * 18. Added VRFS/Seby middleware initialization and monitoring
 * 19. Added VRFS middleware status to health endpoint
 * 20. Added periodic VRFS upstream health monitoring
 */
import http from 'http';
import { Client } from 'discord.js-selfbot-v13';
import type { Message } from 'discord.js-selfbot-v13';
import 'dotenv/config';
import { CONFIG } from './config.js';
import { logger, reconfigureLogger } from './logger.js';
import GiveawayManager from './giveawayManager.js';
import { BotManager } from './bot.js';
import { delay, formatError, formatDuration } from './utils.js';
import { getDb, closeDb, cleanupOldGiveaways, getScrimStats } from './database.js';
import { AutoJoinManager } from './autoJoin/index.js';
import { vrfs, seby, health as vrfsHealth, getStatus as getVRFSStatus } from './middleware/api/vrfs.js';
// ----------------------------------------------------------------------------
// MEMORY MANAGEMENT - 8GB RAM Optimized
// ----------------------------------------------------------------------------
const MEMORY_WARNING_MB = 3500;
const MEMORY_CRITICAL_MB = 4800;
const MEMORY_MAX_MB = 5800;
const MEMORY_FATAL_MB = 6800;
let memoryWarningLogged = false;
let memoryCriticalLogged = false;
let memoryCleanupInterval: NodeJS.Timeout | null = null;
let isInMemoryCleanup = false;
function getMemoryUsage() {
  const mem = process.memoryUsage();
  return {
    heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
    heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
    rssMB: Math.round(mem.rss / 1024 / 1024),
    externalMB: Math.round(mem.external / 1024 / 1024),
  };
}
function checkMemoryAndCleanup() {
  if (isInMemoryCleanup) return;
  const mem = getMemoryUsage();
  if (mem.heapUsedMB > MEMORY_FATAL_MB) {
    console.error(`🚨 FATAL: Memory at ${mem.heapUsedMB}MB, forcing shutdown...`);
    if (global.gc) global.gc();
    process.exit(1);
  }
  if (mem.heapUsedMB > MEMORY_MAX_MB) {
    console.warn(`⚠️ MAX: Memory at ${mem.heapUsedMB}MB, aggressive cleanup...`);
    isInMemoryCleanup = true;
    try {
      if (global.gc) global.gc();
      if (autoJoiner) {
        try {
          const stats = autoJoiner.getStats();
          if (stats.activeSessions > 10) {
            console.log(`[Memory] AutoJoiner: ${stats.activeSessions}/${stats.totalSessions} sessions`);
          }
        } catch {}
      }
      if (global.gc) global.gc();
    } finally {
      isInMemoryCleanup = false;
    }
    return;
  }
  if (mem.heapUsedMB > MEMORY_CRITICAL_MB) {
    if (!memoryCriticalLogged) {
      console.warn(`⚠️ CRITICAL: Memory at ${mem.heapUsedMB}MB, forcing cleanup...`);
      memoryCriticalLogged = true;
    }
    try {
      if (global.gc) global.gc();
      for (const m of activeManagers) {
        try {
          const mgr = m as any;
          if (mgr.giveawayTextCache) mgr.giveawayTextCache.clear();
          if (mgr.creationCache) mgr.creationCache.clear();
          if (mgr.processingMessages) mgr.processingMessages.clear();
        } catch {}
      }
      if (global.gc) global.gc();
    } catch {}
    return;
  }
  if (mem.heapUsedMB > MEMORY_WARNING_MB) {
    if (!memoryWarningLogged) {
      console.warn(`⚠️ Memory warning: ${mem.heapUsedMB}MB`);
      memoryWarningLogged = true;
    }
    if (global.gc && Math.random() < 0.1) {
      global.gc();
    }
  } else {
    memoryWarningLogged = false;
    memoryCriticalLogged = false;
  }
}
memoryCleanupInterval = setInterval(checkMemoryAndCleanup, 30000);
if (memoryCleanupInterval.unref) memoryCleanupInterval.unref();
// ----------------------------------------------------------------------------
// HEALTH SERVER
// ----------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || '3000', 10) || 3000;
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    const mem = getMemoryUsage();
    let activeSessions = 0;
    let totalSessions = 0;
    if (autoJoiner) {
      try {
        const stats = autoJoiner.getStats();
        activeSessions = stats.activeSessions || 0;
        totalSessions = stats.totalSessions || 0;
      } catch {}
    }
    let vrfsStatus: Record<string, unknown> = {
      available: false,
    };
    try {
      vrfsStatus = {
        available: true,
        ...getVRFSStatus(),
      };
    } catch (err) {
      vrfsStatus = {
        available: false,
        error: formatError(err),
      };
    }
    const stats = {
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
      memory: mem,
      sessions: { activeSessions, totalSessions },
      activeManagers: activeManagers.length,
      gcAvailable: !!global.gc,
      vrfs: vrfsStatus,
      vrfsUpstream: lastVRFSHealth,
    };
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
    });
    res.end(JSON.stringify(stats));
  } else if (req.url === '/gc') {
    if (global.gc) {
      global.gc();
      res.writeHead(200, {
        'Content-Type': 'text/plain; charset=utf-8',
      });
      res.end('GC forced');
    } else {
      res.writeHead(500, {
        'Content-Type': 'text/plain; charset=utf-8',
      });
      res.end('GC not available (run with --expose-gc)');
    }
  } else if (req.url === '/shutdown') {
    res.writeHead(200, {
      'Content-Type': 'text/plain; charset=utf-8',
    });
    res.end('Shutting down...');
    setTimeout(() => process.exit(0), 1000);
  } else {
    res.writeHead(404);
    res.end();
  }
});
healthServer.listen(PORT, '0.0.0.0', () => {
  console.log(`[Bootstrap] Health check server on port ${PORT}`);
  const mem = getMemoryUsage();
  console.log(`[Bootstrap] Memory: ${mem.heapUsedMB}MB / 8000MB (${Math.round((mem.heapUsedMB / 8000) * 100)}%)`);
});
healthServer.on('error', (err) => console.error('[Bootstrap] Health server error:', err));
// ----------------------------------------------------------------------------
// GLOBAL ERROR HANDLERS
// ----------------------------------------------------------------------------
process.on('uncaughtException', (err) => {
  console.error('🔥 UNCAUGHT EXCEPTION:', err);
  try {
    logger.error('Uncaught exception', {
      component: 'Process',
      error: err,
    });
  } catch {}
  if (err.message?.includes('ENOMEM') || err.message?.includes('out of memory')) {
    if (global.gc) global.gc();
    process.exit(1);
  }
});
process.on('unhandledRejection', (reason) => {
  console.error('🔥 UNHANDLED REJECTION:', reason);
  try {
    logger.warn('Unhandled rejection', {
      component: 'Process',
      reason: formatError(reason),
    });
  } catch {}
});
process.setMaxListeners(100);
// ----------------------------------------------------------------------------
// STATE
// ----------------------------------------------------------------------------
let activeManagers: GiveawayManager[] = [];
let botManager: BotManager | null = null;
let autoJoiner: AutoJoinManager | null = null;
let statsInterval: ReturnType<typeof setInterval> | null = null;
let vrfsHealthInterval: ReturnType<typeof setInterval> | null = null;
let shuttingDown = false;
let vrfsHealthRunning = false;
let lastVRFSHealth: Record<string, unknown> = {
  ok: false,
  status: 'not_checked',
  timestamp: null,
};
const CLIENT_READY_TIMEOUT_MS = 60000;
const MAX_BOOT_RETRIES = 5;
const BOOT_RETRY_DELAY_MS = 15000;
const BOT_MANAGER_START_TIMEOUT_MS = 10000;
const SHUTDOWN_TIMEOUT_MS = 10000;
const VRFS_HEALTH_INTERVAL_MS = 60000;
// ----------------------------------------------------------------------------
// VRFS / SEBY MONITORING
// ----------------------------------------------------------------------------
async function checkVRFSHealth(): Promise<void> {
  if (shuttingDown || vrfsHealthRunning) return;
  vrfsHealthRunning = true;
  const started = Date.now();
  try {
    const result = await vrfsHealth(1);
    lastVRFSHealth = {
      ...result,
      timestamp: new Date().toISOString(),
      durationMs: Date.now() - started,
    };
    if (result.ok) {
      logger.info('VRFS upstream health check passed', {
        component: 'VRFS',
        latencyMs: result.latencyMs,
        services: result.services,
      });
    } else {
      logger.warn('VRFS upstream health check degraded', {
        component: 'VRFS',
        latencyMs: result.latencyMs,
        services: result.services,
      });
    }
  } catch (err) {
    lastVRFSHealth = {
      ok: false,
      status: 'error',
      error: formatError(err),
      timestamp: new Date().toISOString(),
      durationMs: Date.now() - started,
    };
    logger.warn('VRFS upstream health check failed', {
      component: 'VRFS',
      error: formatError(err),
    });
  } finally {
    vrfsHealthRunning = false;
  }
}
function startVRFSMonitoring(): void {
  if (vrfsHealthInterval) return;
  void checkVRFSHealth();
  vrfsHealthInterval = setInterval(() => {
    void checkVRFSHealth();
  }, VRFS_HEALTH_INTERVAL_MS);
  if (vrfsHealthInterval.unref) vrfsHealthInterval.unref();
}
function stopVRFSMonitoring(): void {
  if (!vrfsHealthInterval) return;
  clearInterval(vrfsHealthInterval);
  vrfsHealthInterval = null;
  vrfsHealthRunning = false;
}
// ----------------------------------------------------------------------------
// MAIN
// ----------------------------------------------------------------------------
async function main(): Promise<void> {
  reconfigureLogger(CONFIG.logLevel, CONFIG.logDir);
  logger.info('╔═══════════════════════════════════════╗', { component: 'Bootstrap' });
  logger.info('║    Discord Giveaway Tracker v2        ║', { component: 'Bootstrap' });
  logger.info('╚═══════════════════════════════════════╝', { component: 'Bootstrap' });
  const mem = getMemoryUsage();
  logger.info(`Memory: ${mem.heapUsedMB}MB / 8000MB (${Math.round((mem.heapUsedMB / 8000) * 100)}%)`, {
    component: 'Bootstrap',
  });
  logger.info('Configuration', {
    component: 'Bootstrap',
    accounts: CONFIG.tokens.length,
    monitoredChannels: CONFIG.monitoredChannels.length || 'all',
    trackerChannel: CONFIG.trackerChannelId,
    cooldown: CONFIG.notificationCooldown,
    dbPath: CONFIG.dbPath,
  });
  // --------------------------------------------------------------------------
  // INITIALIZE VRFS MIDDLEWARE
  // --------------------------------------------------------------------------
  logger.info('Initializing VRFS/Seby middleware...', {
    component: 'VRFS',
  });
  try {
    const vrfsStatus = getVRFSStatus();
    logger.info('VRFS/Seby middleware initialized', {
      component: 'VRFS',
      status: vrfsStatus,
    });
  } catch (err) {
    logger.warn('VRFS middleware initialization warning', {
      component: 'VRFS',
      error: formatError(err),
    });
  }
  startVRFSMonitoring();
  // --------------------------------------------------------------------------
  // DATABASE
  // --------------------------------------------------------------------------
  try {
    await Promise.race([
      getDb(),
      delay(10000).then(() => {
        throw new Error('Database connection timeout');
      }),
    ]);
    logger.info('Database connection established', {
      component: 'Bootstrap',
    });
  } catch (err) {
    logger.error('Database connection failed', {
      component: 'Bootstrap',
      error: formatError(err),
    });
    throw err;
  }
  cleanupOldGiveaways(30).catch(err => logger.warn('cleanupOldGiveaways error', { error: err }));
  // --------------------------------------------------------------------------
  // START BOTMANAGER (REAL BOT) - MUST BE FIRST
  // --------------------------------------------------------------------------
  logger.info('Initializing BotManager...', {
    component: 'Bootstrap',
  });
  try {
    const startPromise = (async () => {
      botManager = new BotManager(CONFIG.botToken);
      await botManager.start();
    })();
    await Promise.race([
      startPromise,
      delay(BOT_MANAGER_START_TIMEOUT_MS).then(() => {
        throw new Error('BotManager.start() timed out');
      }),
    ]);
    logger.info('BotManager started successfully.', {
      component: 'Bootstrap',
    });
  } catch (err) {
    logger.warn('BotManager failed to start (will continue without it):', {
      component: 'Bootstrap',
      error: formatError(err),
    });
    botManager = null;
  }
  // --------------------------------------------------------------------------
  // START ACCOUNT CLIENTS (TRACKER SELF-BOTS)
  // --------------------------------------------------------------------------
  activeManagers = [];
  let authFailures = 0;
  let clientsStarted = 0;
  const BATCH_SIZE = 3;
  const tokenBatches: string[][] = [];
  for (let i = 0; i < CONFIG.tokens.length; i += BATCH_SIZE) {
    tokenBatches.push(CONFIG.tokens.slice(i, i + BATCH_SIZE));
  }
  for (const batch of tokenBatches) {
    const currentMem = getMemoryUsage();
    if (currentMem.heapUsedMB > MEMORY_CRITICAL_MB) {
      logger.warn(`Memory high (${currentMem.heapUsedMB}MB), stopping account creation`, {
        component: 'Bootstrap',
        started: clientsStarted,
      });
      break;
    }
    const batchPromises = batch.map(async (token, batchIndex) => {
      const globalIndex = clientsStarted + batchIndex;
      const label = `acc${globalIndex + 1}`;
      if (!token || token.trim() === '') {
        logger.warn(`Token ${globalIndex + 1} is empty – skipping`, {
          component: 'Bootstrap',
        });
        return null;
      }
      let client: Client | null = null;
      try {
        logger.info(`Starting account ${globalIndex + 1}/${CONFIG.tokens.length} (${label})...`, {
          component: 'Bootstrap',
        });
        client = new Client();
        client.setMaxListeners(50);
        if (CONFIG.logLevel === 'debug') {
          client.on('debug', (info) => {
            logger.debug(`[${label}] Debug: ${info}`, {
              component: 'Client',
            });
          });
        }
        client.on('ready', () => {
          logger.info(`[${label}] Client ready event fired`, {
            component: 'Client',
          });
        });
        client.on('error', (err) => {
          logger.error(`[${label}] Client error event: ${formatError(err)}`, {
            component: 'Client',
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
        logger.info(`[${label}] Calling waitForReady...`, {
          component: 'Bootstrap',
        });
        try {
          await Promise.race([
            waitForReady(client, token, label),
            delay(CLIENT_READY_TIMEOUT_MS).then(() => {
              throw new Error(`Client ${label} did not become ready`);
            }),
          ]);
        } catch (raceErr) {
          try {
            client.removeAllListeners();
            await client.destroy();
          } catch {}
          throw raceErr;
        }
        logger.info(`[${label}] waitForReady resolved successfully`, {
          component: 'Bootstrap',
        });
        activeManagers.push(manager);
        logger.info(`Account ${label} connected`, {
          component: 'Bootstrap',
          userId: client.user?.id,
          username: client.user?.username,
          guilds: client.guilds.cache.size,
          memory: getMemoryUsage(),
        });
        return manager;
      } catch (err) {
        const message = formatError(err);
        const isAuth = /token|auth|login|invalid|unauthorized|401|403/i.test(message);
        if (isAuth) {
          authFailures++;
          logger.warn(`Account ${label} skipped (auth error)`, {
            component: 'Bootstrap',
            error: message,
          });
          return null;
        }
        logger.error(`Account ${label} failed`, {
          component: 'Bootstrap',
          error: message,
        });
        return null;
      }
    });
    const results = await Promise.all(batchPromises);
    for (const result of results) {
      if (result) clientsStarted++;
    }
    await delay(1000);
  }
  if (activeManagers.length === 0 && authFailures > 0 && authFailures === CONFIG.tokens.length) {
    throw Object.assign(
      new Error('All tokens failed authentication'),
      { code: 'AUTH_ALL_FAILED' },
    );
  }
  if (activeManagers.length === 0) {
    throw new Error('No accounts could be started');
  }
  logger.info(`✅ ${activeManagers.length} account(s) running`, {
    component: 'Bootstrap',
    active: activeManagers.length,
    failures: authFailures,
    memory: getMemoryUsage(),
  });
  // --------------------------------------------------------------------------
  // START AUTOJOINER (PREMIUM SELF-BOTS) - MUST BE AFTER TRACKER SELF-BOTS
  // --------------------------------------------------------------------------
  try {
    logger.info('Starting AutoJoiner (premium self-bots)...', {
      component: 'Bootstrap',
    });
    autoJoiner = new AutoJoinManager();
    await Promise.race([
      autoJoiner.startAllSessions(),
      delay(60000).then(() => {
        throw new Error('AutoJoiner start timed out');
      }),
    ]);
    await Promise.race([
      autoJoiner.restoreSessionsFromDatabase(),
      delay(30000).then(() => {
        throw new Error('AutoJoiner restore timed out');
      }),
    ]);
    const stats = autoJoiner.getStats();
    logger.info(`✅ AutoJoiner running with ${stats.activeSessions}/${stats.totalSessions} active sessions`, {
      component: 'Bootstrap',
    });
  } catch (err) {
    logger.warn('AutoJoiner failed to start:', {
      component: 'Bootstrap',
      error: formatError(err),
    });
    autoJoiner = null;
  }
  // --------------------------------------------------------------------------
  // INITIAL SCRIM STATS
  // --------------------------------------------------------------------------
  try {
    const scrimStats = await getScrimStats();
    logger.info(`📊 Scrim Stats: ${scrimStats.total} total, ${scrimStats.active} active, ${scrimStats.servers} servers`, {
      component: 'Bootstrap',
      scrims: scrimStats.byType.scrim,
      squidGames: scrimStats.byType.squid_game,
      gagaballs: scrimStats.byType.gagaball,
    });
  } catch {}
  // --------------------------------------------------------------------------
  // PERIODIC STATS
  // --------------------------------------------------------------------------
  statsInterval = setInterval(() => {
    if (shuttingDown) return;
    for (const m of activeManagers) {
      try {
        m.logStats();
      } catch {}
    }
    if (!shuttingDown) {
      try {
        getScrimStats().then(scrimStats => {
          logger.info(`📊 Scrim Stats: ${scrimStats.total} total, ${scrimStats.active} active`, {
            component: 'Bootstrap',
            scrims: scrimStats.byType.scrim,
            squidGames: scrimStats.byType.squid_game,
            gagaballs: scrimStats.byType.gagaball,
            servers: scrimStats.servers,
          });
        }).catch(() => {});
      } catch {}
    }
    if (autoJoiner && !shuttingDown) {
      try {
        const stats = autoJoiner.getStats();
        logger.info(`AutoJoiner: ${stats.activeSessions}/${stats.totalSessions} sessions active`, {
          component: 'Bootstrap',
          memory: getMemoryUsage(),
        });
      } catch {}
    }
    try {
      const vrfsStatus = getVRFSStatus();
      logger.debug?.('[VRFS] Middleware status', {
        component: 'VRFS',
        status: vrfsStatus,
      });
    } catch {}
  }, CONFIG.statsIntervalMs);
  statsInterval.unref();
  registerShutdown();
  logger.info('🟢 Tracker is live', {
    component: 'Bootstrap',
    accounts: activeManagers.length,
    statsEvery: `${CONFIG.statsIntervalMs / 1000}s`,
    memory: getMemoryUsage(),
  });
}
// ----------------------------------------------------------------------------
// DISCORD EVENT HANDLERS
// ----------------------------------------------------------------------------
function registerDiscordEvents(client: Client, manager: GiveawayManager): void {
  const maxListeners = 50;
  client.setMaxListeners(maxListeners);
  const messageCreateHandler = (msg: Message) => {
    if (!msg.guild || shuttingDown) return;
    manager.handleMessage(msg).catch((err) => {
      logger.error('messageCreate handler error', {
        component: 'Events',
        error: formatError(err),
        messageId: msg.id,
      });
    });
  };
  const messageUpdateHandler = (_old: any, updated: any) => {
    if (!updated.id || !updated.channel || shuttingDown) return;
    manager.handleMessage(updated as Message).catch((err) => {
      logger.error('messageUpdate handler error', {
        component: 'Events',
        error: formatError(err),
        messageId: updated.id,
      });
    });
  };
  const guildCreateHandler = (guild: any) => {
    if (shuttingDown) return;
    logger.info('Joined server', {
      component: 'Events',
      guildId: guild.id,
      guildName: guild.name,
      memberCount: guild.memberCount,
    });
  };
  const guildDeleteHandler = (guild: any) => {
    if (shuttingDown) return;
    logger.info('Left server', {
      component: 'Events',
      guildId: guild.id,
      guildName: guild.name,
    });
    manager.clearInviteCache(guild.id);
  };
  const disconnectHandler = () => {
    if (shuttingDown) return;
    logger.warn('Disconnected', {
      component: 'Events',
    });
  };
  const reconnectingHandler = () => {
    if (shuttingDown) return;
    logger.info('Reconnecting...', {
      component: 'Events',
    });
  };
  const errorHandler = (err: Error) => {
    if (shuttingDown) return;
    logger.error('Client error', {
      component: 'Events',
      error: err,
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
// ----------------------------------------------------------------------------
// HELPERS
// ----------------------------------------------------------------------------
function waitForReady(client: Client, token: string, label: string): Promise<void> {
  return new Promise((resolve, reject) => {
    console.log(`[${label}] waitForReady: setting up listeners and calling login...`);
    let resolved = false;
    const readyHandler = () => {
      if (resolved) return;
      resolved = true;
      console.log(`[${label}] waitForReady: ready event received`);
      cleanup();
      resolve();
    };
    const errorHandler = (err: Error) => {
      if (resolved) return;
      resolved = true;
      console.error(`[${label}] waitForReady: error event received`, err);
      cleanup();
      reject(err);
    };
    const cleanup = () => {
      client.off('ready', readyHandler);
      client.off('error', errorHandler);
    };
    client.once('ready', readyHandler);
    client.once('error', errorHandler);
    client.login(token)
      .then(() => {
        console.log(`[${label}] waitForReady: client.login() resolved`);
      })
      .catch((err) => {
        if (resolved) return;
        resolved = true;
        console.error(`[${label}] waitForReady: client.login() rejected`, err);
        cleanup();
        reject(new Error(`Login failed: ${formatError(err)}`));
      });
  });
}
// ----------------------------------------------------------------------------
// SHUTDOWN
// ----------------------------------------------------------------------------
function registerShutdown(): void {
  const handle = async (signal: string): Promise<void> => {
    if (shuttingDown) {
      console.log('[Shutdown] Already shutting down, forcing exit...');
      process.exit(1);
    }
    shuttingDown = true;
    console.log(`[Shutdown] ${signal} received – shutting down cleanly...`);
    stopVRFSMonitoring();
    if (statsInterval) {
      clearInterval(statsInterval);
      statsInterval = null;
    }
    if (memoryCleanupInterval) {
      clearInterval(memoryCleanupInterval);
      memoryCleanupInterval = null;
    }
    console.log(`[Shutdown] Stopping ${activeManagers.length} account managers...`);
    const managerPromises = activeManagers.map(async (m) => {
      try {
        await m.shutdown();
        const managerAny = m as any;
        const client = managerAny.client;
        if (client) {
          try {
            const handlers = managerAny._handlers;
            if (handlers) {
              for (const [event, handler] of Object.entries(handlers)) {
                try {
                  client.off(event, handler as any);
                } catch {}
              }
            }
            client.removeAllListeners();
            await client.destroy();
          } catch {}
        }
      } catch (err) {
        console.error('[Shutdown] Error stopping manager:', err);
      }
    });
    await Promise.race([
      Promise.allSettled(managerPromises),
      delay(SHUTDOWN_TIMEOUT_MS),
    ]);
    activeManagers = [];
    if (autoJoiner) {
      console.log('[Shutdown] Shutting down AutoJoiner...');
      try {
        await Promise.race([
          autoJoiner.shutdown(),
          delay(SHUTDOWN_TIMEOUT_MS / 2),
        ]);
      } catch (err) {
        console.error('[Shutdown] Error stopping AutoJoiner:', err);
      }
      autoJoiner = null;
    }
    if (botManager) {
      console.log('[Shutdown] Shutting down BotManager...');
      try {
        await Promise.race([
          botManager.destroy(),
          delay(SHUTDOWN_TIMEOUT_MS / 2),
        ]);
      } catch (err) {
        console.error('[Shutdown] Error stopping BotManager:', err);
      }
      botManager = null;
    }
    try {
      console.log('[Shutdown] Closing VRFS middleware...');
      vrfs.clearCaches();
      seby.clearFlights();
      logger.info('VRFS middleware caches cleared', {
        component: 'VRFS',
      });
    } catch (err) {
      console.error('[Shutdown] Error clearing VRFS middleware:', err);
    }
    try {
      console.log('[Shutdown] Closing database...');
      await closeDb();
    } catch (err) {
      console.error('[Shutdown] Error closing database:', err);
    }
    try {
      healthServer.close(() => {});
    } catch {}
    if (global.gc) {
      console.log('[Shutdown] Forcing garbage collection...');
      global.gc();
      await delay(100);
      global.gc();
      await delay(100);
      global.gc();
    }
    const mem = getMemoryUsage();
    console.log(`[Shutdown] Final memory: ${mem.heapUsedMB}MB / 8000MB`);
    console.log('[Shutdown] Goodbye.');
    setTimeout(() => process.exit(0), 500);
  };
  process.on('SIGINT', () => handle('SIGINT').catch(() => process.exit(1)));
  process.on('SIGTERM', () => handle('SIGTERM').catch(() => process.exit(1)));
}
// ----------------------------------------------------------------------------
// BOOT LOOP
// ----------------------------------------------------------------------------
async function boot(): Promise<void> {
  let attempt = 0;
  while (attempt < MAX_BOOT_RETRIES) {
    try {
      attempt++;
      if (attempt > 1) {
        logger.info(`Boot attempt ${attempt}/${MAX_BOOT_RETRIES}`, {
          component: 'Bootstrap',
        });
      }
      await main();
      return;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const code = (err as any)?.code;
      logger.error('Startup error', {
        component: 'Bootstrap',
        error: message,
        attempt,
        maxRetries: MAX_BOOT_RETRIES,
      });
      if (code === 'AUTH_ALL_FAILED') {
        logger.error('All tokens invalid – exiting', {
          component: 'Bootstrap',
        });
        process.exit(1);
      }
      if (/token|auth|login|invalid|unauthorized|401|403/i.test(message)) {
        logger.error('Fatal auth error – exiting', {
          component: 'Bootstrap',
        });
        process.exit(1);
      }
      if (attempt >= MAX_BOOT_RETRIES) {
        logger.error('Max retries exceeded', {
          component: 'Bootstrap',
        });
        process.exit(1);
      }
      console.log('[Bootstrap] Cleaning up before retry...');
      stopVRFSMonitoring();
      for (const m of activeManagers) {
        try {
          await m.shutdown();
          const managerAny = m as any;
          const client = managerAny.client;
          if (client) {
            client.removeAllListeners();
            await client.destroy();
          }
        } catch {}
      }
      activeManagers = [];
      if (autoJoiner) {
        try {
          await autoJoiner.shutdown();
        } catch {}
        autoJoiner = null;
      }
      if (botManager) {
        try {
          await botManager.destroy();
        } catch {}
        botManager = null;
      }
      try {
        vrfs.clearCaches();
        seby.clearFlights();
      } catch {}
      if (global.gc) {
        global.gc();
      }
      shuttingDown = false;
      logger.info(`Retrying in ${BOOT_RETRY_DELAY_MS / 1000}s...`, {
        component: 'Bootstrap',
      });
      await delay(BOOT_RETRY_DELAY_MS);
    }
  }
}
// ----------------------------------------------------------------------------
// START
// ----------------------------------------------------------------------------
console.log('[Bootstrap] Starting with 8GB RAM...');
const initialMem = getMemoryUsage();
console.log(`[Bootstrap] Initial memory: ${initialMem.heapUsedMB}MB / 8000MB`);
console.log(`[Bootstrap] GC available: ${!!global.gc}`);
if (global.gc) {
  global.gc();
  console.log('[Bootstrap] Initial GC complete');
}
boot();
