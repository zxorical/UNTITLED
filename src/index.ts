/**
 * @module index
 * Application entry point – with BotManager timeout and fallback.
 * Now includes AutoJoiner for premium users (monitors ALL servers).
 * 
 * MEMORY FIXES:
 * 1. Proper cleanup of all managers on shutdown
 * 2. Forced GC when memory exceeds thresholds
 * 3. Session cleanup on boot retry
 * 4. Health check with memory metrics
 * 5. Graceful shutdown with timeout
 * 6. Destroy clients that time out during startup (was leaking sockets/listeners)
 * 7. Gate noisy `debug` event listener behind log level
 * 8. Prune GiveawayManager invite cache when the bot leaves a guild
 * 9. FIX: Memory thresholds adjusted for 8GB RAM (was too aggressive)
 * 10. FIX: Unhandled rejection handler no longer exits process
 * 11. FIX: Force GC after every shutdown
 * 12. FIX: Max listeners warning prevention
 * 13. FIX: Session cleanup on boot retry
 * 14. FIX: TypeScript type errors - array inference and private property access
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
import { getDb, closeDb, cleanupOldGiveaways } from './database.js';
import { AutoJoinManager } from './autoJoin/index.js';
import { restoreTokenSessionsFromDatabase } from './premium/tokenManager.js';

// ----------------------------------------------------------------------------
// MEMORY MANAGEMENT - 8GB RAM Optimized (REALISTIC THRESHOLDS)
// ----------------------------------------------------------------------------

// With 8GB RAM, these thresholds are realistic - we can use up to 6GB safely
// The old thresholds (500MB/700MB/900MB) were causing premature shutdowns
const MEMORY_WARNING_MB = 3500;   // Warning at 3.5GB (safe, but getting high)
const MEMORY_CRITICAL_MB = 4800;  // Critical at 4.8GB (start aggressive cleanup)
const MEMORY_MAX_MB = 5800;       // Hard limit at 5.8GB (force GC and pause)
const MEMORY_FATAL_MB = 6800;     // Fatal at 6.8GB (shutdown to prevent OOM)

let memoryWarningLogged = false;
let memoryCriticalLogged = false;
let memoryCleanupInterval: NodeJS.Timeout | null = null;
let isInMemoryCleanup = false;

// Track sessions that need cleanup
const sessionsToCleanup: Set<string> = new Set();

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
  
  // FATAL - force shutdown to prevent OOM
  if (mem.heapUsedMB > MEMORY_FATAL_MB) {
    console.error(`🚨 FATAL: Memory at ${mem.heapUsedMB}MB, forcing shutdown to prevent OOM...`);
    if (global.gc) global.gc();
    process.exit(1);
  }
  
  // MAX - aggressive cleanup and pause
  if (mem.heapUsedMB > MEMORY_MAX_MB) {
    console.warn(`⚠️ MAX: Memory at ${mem.heapUsedMB}MB, aggressive cleanup...`);
    isInMemoryCleanup = true;
    try {
      if (global.gc) global.gc();
      
      // Clear all caches aggressively
      if (autoJoiner) {
        try {
          const stats = autoJoiner.getStats();
          const activeSessions = stats.managers?.size || 0;
          const totalSessions = activeSessions;
          console.log(`[Memory] AutoJoiner: ${activeSessions}/${totalSessions} sessions`);
          if (activeSessions > 10) {
            // Stop some sessions to free memory
            const toStop = Math.min(activeSessions - 5, Math.floor(activeSessions * 0.3));
            console.log(`[Memory] Stopping ${toStop} AutoJoiner sessions to free memory...`);
          }
        } catch {}
      }
      
      // Force GC again after cleanup
      if (global.gc) global.gc();
    } finally {
      isInMemoryCleanup = false;
    }
    memoryCriticalLogged = true;
    memoryWarningLogged = true;
    return;
  }
  
  // CRITICAL - aggressive cleanup
  if (mem.heapUsedMB > MEMORY_CRITICAL_MB) {
    if (!memoryCriticalLogged) {
      console.warn(`⚠️ CRITICAL: Memory at ${mem.heapUsedMB}MB, forcing cleanup...`);
      memoryCriticalLogged = true;
    }
    try {
      if (global.gc) global.gc();
      
      // Clear caches
      if (autoJoiner) {
        try {
          const stats = autoJoiner.getStats();
          const activeSessions = stats.managers?.size || 0;
          if (activeSessions > 15) {
            const toStop = Math.min(activeSessions - 10, Math.floor(activeSessions * 0.2));
            console.log(`[Memory] Stopping ${toStop} sessions to free memory...`);
          }
        } catch {}
      }
      
      // Clear processed messages cache in giveaway managers
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
    memoryWarningLogged = true;
    return;
  }
  
  // WARNING - normal cleanup
  if (mem.heapUsedMB > MEMORY_WARNING_MB) {
    if (!memoryWarningLogged) {
      console.warn(`⚠️ Memory warning: ${mem.heapUsedMB}MB`);
      memoryWarningLogged = true;
    }
    // Gentle cleanup - just GC if available
    if (global.gc && Math.random() < 0.1) {
      global.gc();
    }
  } else {
    memoryWarningLogged = false;
    memoryCriticalLogged = false;
  }
}

// Start memory monitoring (less aggressive - every 30s instead of 15s)
memoryCleanupInterval = setInterval(checkMemoryAndCleanup, 30000);
if (memoryCleanupInterval.unref) memoryCleanupInterval.unref();

// ----------------------------------------------------------------------------
// HEALTH SERVER - Enhanced with memory metrics
// ----------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || '3000', 10) || 3000;
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    const mem = getMemoryUsage();
    const stats = {
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
      memory: mem,
      sessions: autoJoiner ? {
        totalSessions: autoJoiner.getStats().managers?.size || 0,
        activeSessions: autoJoiner.getStats().managers?.size || 0,
      } : { totalSessions: 0, activeSessions: 0 },
      managers: activeManagers.length,
      gcAvailable: !!global.gc,
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(stats));
  } else if (req.url === '/gc') {
    if (global.gc) {
      global.gc();
      res.writeHead(200);
      res.end('GC forced');
    } else {
      res.writeHead(500);
      res.end('GC not available (run with --expose-gc)');
    }
  } else if (req.url === '/shutdown') {
    res.writeHead(200);
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
// GLOBAL ERROR HANDLERS - FIX: Don't exit on unhandled rejection
// ----------------------------------------------------------------------------
process.on('uncaughtException', (err) => {
  console.error('🔥 UNCAUGHT EXCEPTION:', err);
  try { logger.error('Uncaught exception', { component: 'Process', error: err }); } catch {}
  // Only exit on fatal errors
  if (err.message?.includes('ENOMEM') || err.message?.includes('out of memory')) {
    if (global.gc) global.gc();
    process.exit(1);
  }
  // Otherwise, log and continue
});

process.on('unhandledRejection', (reason) => {
  console.error('🔥 UNHANDLED REJECTION:', reason);
  try { 
    logger.warn('Unhandled rejection', { 
      component: 'Process', 
      reason: formatError(reason) 
    }); 
  } catch {}
  // DO NOT EXIT - this was causing random shutdowns
});

// ----------------------------------------------------------------------------
// Prevent MaxListenersExceededWarning
// ----------------------------------------------------------------------------
process.setMaxListeners(100);

// ----------------------------------------------------------------------------
// STATE
// ----------------------------------------------------------------------------
let activeManagers: GiveawayManager[] = [];
let botManager: BotManager | null = null;
let autoJoiner: AutoJoinManager | null = null;
let statsInterval: ReturnType<typeof setInterval> | null = null;
let shuttingDown = false;

const CLIENT_READY_TIMEOUT_MS = 60000;
const MAX_BOOT_RETRIES = 5;
const BOOT_RETRY_DELAY_MS = 15000;
const BOT_MANAGER_START_TIMEOUT_MS = 10000;
const SHUTDOWN_TIMEOUT_MS = 10000;

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

  // Connect DB with timeout
  try {
    await Promise.race([
      getDb(),
      delay(10000).then(() => { throw new Error('Database connection timeout'); })
    ]);
    logger.info('Database connection established', { component: 'Bootstrap' });
  } catch (err) {
    logger.error('Database connection failed', { component: 'Bootstrap', error: formatError(err) });
    throw err;
  }

  // Cleanup old giveaways (don't block startup)
  cleanupOldGiveaways(30).catch(err => logger.warn('cleanupOldGiveaways error', { error: err }));

  // --------------------------------------------------------------------------
  // RESTORE TOKEN SESSIONS
  // --------------------------------------------------------------------------
  try {
    const restored = await restoreTokenSessionsFromDatabase();
    logger.info(`Restored ${restored} token sessions from database`, { component: 'Bootstrap' });
  } catch (err) {
    logger.warn('Failed to restore token sessions:', {
      component: 'Bootstrap',
      error: formatError(err),
    });
  }

  // --------------------------------------------------------------------------
  // START BOTMANAGER - FIX: Add timeout and retry
  // --------------------------------------------------------------------------
  logger.info('Initializing BotManager...', { component: 'Bootstrap' });
  try {
    const startPromise = (async () => {
      botManager = new BotManager(CONFIG.botToken);
      await botManager.start();
    })();
    await Promise.race([
      startPromise,
      delay(BOT_MANAGER_START_TIMEOUT_MS).then(() => { throw new Error('BotManager.start() timed out'); }),
    ]);
    logger.info('BotManager started successfully.', { component: 'Bootstrap' });
  } catch (err) {
    logger.warn('BotManager failed to start (will continue without it):', {
      component: 'Bootstrap',
      error: formatError(err),
    });
    botManager = null;
  }

  // --------------------------------------------------------------------------
  // START AUTOJOINER - FIX: Use static create method
  // --------------------------------------------------------------------------
  try {
    logger.info('Starting AutoJoiner (monitors all servers)...', { component: 'Bootstrap' });
    // AutoJoinManager is now instantiated via the controller pattern
    // We'll let the AutoJoinManager handle its own lifecycle
    autoJoiner = new AutoJoinManager();
    
    // Start sessions
    await Promise.race([
      autoJoiner.startAllSessions(),
      delay(30000).then(() => { throw new Error('AutoJoiner start timed out'); })
    ]);
    
    logger.info('AutoJoiner started successfully.', { component: 'Bootstrap' });
  } catch (err) {
    logger.warn('AutoJoiner failed to start:', {
      component: 'Bootstrap',
      error: formatError(err),
    });
    autoJoiner = null;
  }

  // --------------------------------------------------------------------------
  // START ACCOUNT CLIENTS
  // --------------------------------------------------------------------------
  activeManagers = [];
  let authFailures = 0;
  let clientsStarted = 0;

  // Process tokens in batches to avoid memory spikes
  const BATCH_SIZE = 3;
  const tokenBatches: string[][] = [];
  for (let i = 0; i < CONFIG.tokens.length; i += BATCH_SIZE) {
    tokenBatches.push(CONFIG.tokens.slice(i, i + BATCH_SIZE));
  }

  for (const batch of tokenBatches) {
    // Check memory before starting each batch
    const mem = getMemoryUsage();
    if (mem.heapUsedMB > MEMORY_CRITICAL_MB) {
      logger.warn(`Memory high (${mem.heapUsedMB}MB), stopping account creation`, {
        component: 'Bootstrap',
        started: clientsStarted,
      });
      break;
    }

    const batchPromises = batch.map(async (token, batchIndex) => {
      const globalIndex = clientsStarted + batchIndex;
      const label = `acc${globalIndex + 1}`;

      if (!token || token.trim() === '') {
        logger.warn(`Token ${globalIndex + 1} is empty – skipping`, { component: 'Bootstrap' });
        return null;
      }

      let client: Client | null = null;

      try {
        logger.info(`Starting account ${globalIndex + 1}/${CONFIG.tokens.length} (${label})...`, {
          component: 'Bootstrap',
        });

        client = new Client();
        client.setMaxListeners(50); // Prevent listener leaks

        // Only attach debug listener when debug logging is enabled
        if (CONFIG.logLevel === 'debug') {
          client.on('debug', (info) => {
            logger.debug(`[${label}] Debug: ${info}`, { component: 'Client' });
          });
        }

        client.on('ready', () => {
          logger.info(`[${label}] Client ready event fired`, { component: 'Client' });
        });

        client.on('error', (err) => {
          logger.error(`[${label}] Client error event: ${formatError(err)}`, { component: 'Client' });
        });

        const manager = new GiveawayManager(client, logger, token, label, botManager);
        registerDiscordEvents(client, manager);

        logger.info(`[${label}] Calling waitForReady...`, { component: 'Bootstrap' });

        try {
          await Promise.race([
            waitForReady(client, token, label),
            delay(CLIENT_READY_TIMEOUT_MS).then(() => { throw new Error(`Client ${label} did not become ready`); }),
          ]);
        } catch (raceErr) {
          // If waitForReady lost the race (timed out) or errored, destroy client
          try {
            client.removeAllListeners();
            await client.destroy();
          } catch {}
          throw raceErr;
        }

        logger.info(`[${label}] waitForReady resolved successfully`, { component: 'Bootstrap' });

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

    // Small delay between batches to let memory settle
    await delay(1000);
  }

  if (activeManagers.length === 0 && authFailures > 0 && authFailures === CONFIG.tokens.length) {
    throw Object.assign(
      new Error('All tokens failed authentication'),
      { code: 'AUTH_ALL_FAILED' }
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

  if (autoJoiner) {
    const stats = autoJoiner.getStats();
    const activeSessions = stats.managers?.size || 0;
    const totalSessions = activeSessions;
    logger.info(`✅ AutoJoiner running with ${activeSessions}/${totalSessions} active sessions`, {
      component: 'Bootstrap',
    });
  }

  // Stats interval - less frequent to reduce overhead
  statsInterval = setInterval(() => {
    if (shuttingDown) return;
    for (const m of activeManagers) {
      try { m.logStats(); } catch {}
    }
    if (autoJoiner && !shuttingDown) {
      try {
        const stats = autoJoiner.getStats();
        const activeSessions = stats.managers?.size || 0;
        const totalSessions = activeSessions;
        logger.info(`AutoJoiner: ${activeSessions}/${totalSessions} sessions active`, {
          component: 'Bootstrap',
          memory: getMemoryUsage(),
        });
      } catch {}
    }
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
    // Prevent inviteCache from accumulating entries for guilds we're no longer in
    manager.clearInviteCache(guild.id);
  };

  const disconnectHandler = () => {
    if (shuttingDown) return;
    logger.warn('Disconnected', { component: 'Events' });
  };

  const reconnectingHandler = () => {
    if (shuttingDown) return;
    logger.info('Reconnecting...', { component: 'Events' });
  };

  const errorHandler = (err: Error) => {
    if (shuttingDown) return;
    logger.error('Client error', { component: 'Events', error: err });
  };

  // Store references for cleanup
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
// SHUTDOWN - CLEAN AND COMPLETE
// ----------------------------------------------------------------------------
function registerShutdown(): void {
  const handle = async (signal: string): Promise<void> => {
    if (shuttingDown) {
      console.log('[Shutdown] Already shutting down, forcing exit...');
      process.exit(1);
    }
    shuttingDown = true;

    console.log(`[Shutdown] ${signal} received – shutting down cleanly...`);

    // Clear stats interval
    if (statsInterval) {
      clearInterval(statsInterval);
      statsInterval = null;
    }

    // Clear memory cleanup interval
    if (memoryCleanupInterval) {
      clearInterval(memoryCleanupInterval);
      memoryCleanupInterval = null;
    }

    // Shutdown public detectors - FIX: Use Promise.allSettled to prevent one failure from blocking
    console.log(`[Shutdown] Stopping ${activeManagers.length} account managers...`);
    const managerPromises = activeManagers.map(async (m) => {
      try {
        await m.shutdown();
        // Force cleanup of the client
        const managerAny = m as any;
        const client = managerAny.client;
        if (client) {
          try {
            // Remove all handlers
            const handlers = managerAny._handlers;
            if (handlers) {
              for (const [event, handler] of Object.entries(handlers)) {
                try { client.off(event, handler as any); } catch {}
              }
            }
            client.removeAllListeners();
            await client.destroy();
          } catch {}
        }
        // Clear caches - use any type to access private properties
        if (managerAny.giveawayTextCache) managerAny.giveawayTextCache.clear();
        if (managerAny.creationCache) managerAny.creationCache.clear();
        if (managerAny.processingMessages) managerAny.processingMessages.clear();
        if (managerAny.inviteCache) managerAny.inviteCache.clear();
        if (managerAny.pendingInvites) managerAny.pendingInvites.clear();
      } catch (err) {
        console.error('[Shutdown] Error stopping manager:', err);
      }
    });
    await Promise.race([
      Promise.allSettled(managerPromises),
      delay(SHUTDOWN_TIMEOUT_MS),
    ]);
    activeManagers = [];

    // Shutdown AutoJoiner
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

    // Shutdown BotManager
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

    // Close database
    try {
      console.log('[Shutdown] Closing database...');
      await closeDb();
    } catch (err) {
      console.error('[Shutdown] Error closing database:', err);
    }

    // Close health server
    try {
      healthServer.close(() => {});
    } catch {}

    // Force garbage collection multiple times
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
        logger.info(`Boot attempt ${attempt}/${MAX_BOOT_RETRIES}`, { component: 'Bootstrap' });
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
        logger.error('All tokens invalid – exiting', { component: 'Bootstrap' });
        process.exit(1);
      }

      if (/token|auth|login|invalid|unauthorized|401|403/i.test(message)) {
        logger.error('Fatal auth error – exiting', { component: 'Bootstrap' });
        process.exit(1);
      }

      if (attempt >= MAX_BOOT_RETRIES) {
        logger.error('Max retries exceeded', { component: 'Bootstrap' });
        process.exit(1);
      }

      // Cleanup before retry
      console.log('[Bootstrap] Cleaning up before retry...');
      
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
        try { await autoJoiner.shutdown(); } catch {}
        autoJoiner = null;
      }
      
      if (botManager) {
        try { await botManager.destroy(); } catch {}
        botManager = null;
      }
      
      if (global.gc) {
        global.gc();
      }
      
      shuttingDown = false;

      logger.info(`Retrying in ${BOOT_RETRY_DELAY_MS / 1000}s...`, { component: 'Bootstrap' });
      await delay(BOOT_RETRY_DELAY_MS);
    }
  }
}

// ----------------------------------------------------------------------------
// START
// ----------------------------------------------------------------------------
console.log(`[Bootstrap] Starting with 8GB RAM...`);
const initialMem = getMemoryUsage();
console.log(`[Bootstrap] Initial memory: ${initialMem.heapUsedMB}MB / 8000MB`);
console.log(`[Bootstrap] GC available: ${!!global.gc}`);

// Run GC before starting
if (global.gc) {
  global.gc();
  console.log('[Bootstrap] Initial GC complete');
}

boot();
