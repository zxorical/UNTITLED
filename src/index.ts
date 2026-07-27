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
// MEMORY MANAGEMENT - 8GB RAM Optimized
// ----------------------------------------------------------------------------

const MEMORY_WARNING_MB = 500;   // Warning at 500MB (8GB has more room)
const MEMORY_CRITICAL_MB = 700;  // Critical at 700MB
const MEMORY_MAX_MB = 900;       // Hard limit at 900MB

let memoryWarningLogged = false;
let memoryCleanupInterval: NodeJS.Timeout | null = null;

function getMemoryUsage() {
  const mem = process.memoryUsage();
  return {
    heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
    heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
    rssMB: Math.round(mem.rss / 1024 / 1024),
  };
}

function checkMemoryAndCleanup() {
  const mem = getMemoryUsage();
  
  if (mem.heapUsedMB > MEMORY_MAX_MB) {
    console.error(`🚨 CRITICAL: Memory at ${mem.heapUsedMB}MB, forcing shutdown...`);
    if (global.gc) global.gc();
    process.exit(1);
  }
  
  if (mem.heapUsedMB > MEMORY_CRITICAL_MB) {
    console.warn(`⚠️ CRITICAL: Memory at ${mem.heapUsedMB}MB, forcing cleanup...`);
    if (global.gc) global.gc();
    // Clear all caches
    if (autoJoiner) {
      try {
        const stats = autoJoiner.getStats();
        console.log(`[Memory] AutoJoiner: ${stats.activeSessions}/${stats.totalSessions} sessions`);
        if (stats.activeSessions > 3) {
          console.log(`[Memory] Stopping extra sessions to free memory...`);
        }
      } catch {}
    }
    memoryWarningLogged = true;
  } else if (mem.heapUsedMB > MEMORY_WARNING_MB) {
    if (!memoryWarningLogged) {
      console.warn(`⚠️ Memory warning: ${mem.heapUsedMB}MB`);
      memoryWarningLogged = true;
    }
    if (global.gc) global.gc();
  } else {
    memoryWarningLogged = false;
  }
}

// Start memory monitoring
memoryCleanupInterval = setInterval(checkMemoryAndCleanup, 15000);
if (memoryCleanupInterval.unref) memoryCleanupInterval.unref();

// ----------------------------------------------------------------------------
// HEALTH SERVER - Enhanced with memory metrics
// ----------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || '3000', 10) || 3000;
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    const mem = getMemoryUsage();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
      memory: mem,
      sessions: autoJoiner ? autoJoiner.getStats() : { totalSessions: 0, activeSessions: 0 },
    }));
  } else if (req.url === '/gc') {
    if (global.gc) {
      global.gc();
      res.writeHead(200);
      res.end('GC forced');
    } else {
      res.writeHead(500);
      res.end('GC not available (run with --expose-gc)');
    }
  } else {
    res.writeHead(404);
    res.end();
  }
});

healthServer.listen(PORT, '0.0.0.0', () => {
  console.log(`[Bootstrap] Health check server on port ${PORT}`);
  console.log(`[Bootstrap] Memory: ${getMemoryUsage().heapUsedMB}MB / 8000MB`);
});
healthServer.on('error', (err) => console.error('[Bootstrap] Health server error:', err));

// ----------------------------------------------------------------------------
// GLOBAL ERROR HANDLERS
// ----------------------------------------------------------------------------
process.on('uncaughtException', (err) => {
  console.error('🔥 UNCAUGHT EXCEPTION:', err);
  try { logger.error('Uncaught exception', { component: 'Process', error: err }); } catch {}
  if (global.gc) global.gc();
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  console.error('🔥 UNHANDLED REJECTION:', reason);
  try { logger.warn('Unhandled rejection', { component: 'Process', reason: formatError(reason) }); } catch {}
  // Don't exit on unhandled rejection - just log and continue
});

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

  // Connect DB
  await getDb();
  logger.info('Database connection established', { component: 'Bootstrap' });

  // Cleanup old giveaways
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
  // START BOTMANAGER
  // --------------------------------------------------------------------------
  logger.info('Initializing BotManager...', { component: 'Bootstrap' });
  try {
    const startPromise = (async () => {
      botManager = new BotManager(CONFIG.botToken);
      await botManager.start();
    })();
    await Promise.race([
      startPromise,
      timeout(BOT_MANAGER_START_TIMEOUT_MS, 'BotManager.start() timed out'),
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
  // START AUTOJOINER
  // --------------------------------------------------------------------------
  try {
    logger.info('Starting AutoJoiner (monitors all servers)...', { component: 'Bootstrap' });
    autoJoiner = new AutoJoinManager();
    await autoJoiner.startAllSessions();
    await autoJoiner.restoreSessionsFromDatabase();
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

  for (let i = 0; i < CONFIG.tokens.length; i++) {
    // Check memory before starting each account
    const mem = getMemoryUsage();
    if (mem.heapUsedMB > MEMORY_CRITICAL_MB) {
      logger.warn(`Memory high (${mem.heapUsedMB}MB), stopping account creation`, {
        component: 'Bootstrap',
        index: i,
      });
      break;
    }

    const token = CONFIG.tokens[i]!.trim();
    const label = `acc${i + 1}`;

    if (!token) {
      logger.warn(`Token ${i + 1} is empty – skipping`, { component: 'Bootstrap' });
      continue;
    }

    try {
      logger.info(`Starting account ${i + 1}/${CONFIG.tokens.length} (${label})...`, {
        component: 'Bootstrap',
      });

      const client = new Client();

      client.on('debug', (info) => {
        logger.debug(`[${label}] Debug: ${info}`, { component: 'Client' });
      });

      client.on('ready', () => {
        logger.info(`[${label}] Client ready event fired`, { component: 'Client' });
      });

      client.on('error', (err) => {
        logger.error(`[${label}] Client error event: ${formatError(err)}`, { component: 'Client' });
      });

      const manager = new GiveawayManager(client, logger, token, label, botManager);
      registerDiscordEvents(client, manager);

      logger.info(`[${label}] Calling waitForReady...`, { component: 'Bootstrap' });

      await Promise.race([
        waitForReady(client, token, label),
        timeout(CLIENT_READY_TIMEOUT_MS, `Client ${label} did not become ready`),
      ]);

      logger.info(`[${label}] waitForReady resolved successfully`, { component: 'Bootstrap' });

      activeManagers.push(manager);

      logger.info(`Account ${label} connected`, {
        component: 'Bootstrap',
        userId: client.user?.id,
        username: client.user?.username,
        guilds: client.guilds.cache.size,
        memory: getMemoryUsage(),
      });
    } catch (err) {
      const message = formatError(err);
      const isAuth = /token|auth|login|invalid|unauthorized|401|403/i.test(message);

      if (isAuth) {
        authFailures++;
        logger.warn(`Account ${label} skipped (auth error)`, {
          component: 'Bootstrap',
          error: message,
        });
        continue;
      }

      logger.error(`Account ${label} failed`, {
        component: 'Bootstrap',
        error: message,
      });
    }
  }

  if (activeManagers.length === 0 && authFailures > 0) {
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
    logger.info(`✅ AutoJoiner running with ${stats.activeSessions}/${stats.totalSessions} active sessions`, {
      component: 'Bootstrap',
    });
  }

  statsInterval = setInterval(() => {
    for (const m of activeManagers) {
      m.logStats();
    }
    if (autoJoiner) {
      const stats = autoJoiner.getStats();
      logger.info(`AutoJoiner: ${stats.activeSessions}/${stats.totalSessions} sessions active`, {
        component: 'Bootstrap',
        memory: getMemoryUsage(),
      });
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
  client.on('messageCreate', (msg: Message) => {
    if (!msg.guild) return;
    manager.handleMessage(msg).catch((err) => {
      logger.error('messageCreate handler error', {
        component: 'Events',
        error: formatError(err),
        messageId: msg.id,
      });
    });
  });

  client.on('messageUpdate', (_old: any, updated: any) => {
    if (!updated.id || !updated.channel) return;
    manager.handleMessage(updated as Message).catch((err) => {
      logger.error('messageUpdate handler error', {
        component: 'Events',
        error: formatError(err),
        messageId: updated.id,
      });
    });
  });

  client.on('guildCreate', (guild) => {
    logger.info('Joined server', {
      component: 'Events',
      guildId: guild.id,
      guildName: guild.name,
      memberCount: guild.memberCount,
    });
  });

  client.on('guildDelete', (guild) => {
    logger.info('Left server', {
      component: 'Events',
      guildId: guild.id,
      guildName: guild.name,
    });
  });

  client.on('disconnect', () => logger.warn('Disconnected', { component: 'Events' }));
  client.on('reconnecting', () => logger.info('Reconnecting...', { component: 'Events' }));
  client.on('error', (err) => logger.error('Client error', { component: 'Events', error: err }));
}

// ----------------------------------------------------------------------------
// HELPERS
// ----------------------------------------------------------------------------
function waitForReady(client: Client, token: string, label: string): Promise<void> {
  return new Promise((resolve, reject) => {
    console.log(`[${label}] waitForReady: setting up listeners and calling login...`);
    client.once('ready', () => {
      console.log(`[${label}] waitForReady: ready event received`);
      resolve();
    });
    client.once('error', (err) => {
      console.error(`[${label}] waitForReady: error event received`, err);
      reject(err);
    });
    client.login(token)
      .then(() => {
        console.log(`[${label}] waitForReady: client.login() resolved`);
      })
      .catch((err) => {
        console.error(`[${label}] waitForReady: client.login() rejected`, err);
        reject(new Error(`Login failed: ${formatError(err)}`));
      });
  });
}

function timeout(ms: number, message: string): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => {
      console.error(`Timeout triggered: ${message}`);
      reject(new Error(message));
    }, ms);
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

    // Shutdown public detectors
    console.log(`[Shutdown] Stopping ${activeManagers.length} account managers...`);
    const managerPromises = activeManagers.map(async (m) => {
      try {
        await m.shutdown();
        // Force cleanup of the client
        const client = (m as any).client;
        if (client) {
          try {
            client.removeAllListeners();
            await client.destroy();
          } catch {}
        }
      } catch (err) {
        console.error('[Shutdown] Error stopping manager:', err);
      }
    });
    await Promise.race([
      Promise.all(managerPromises),
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

    // Force garbage collection
    if (global.gc) {
      console.log('[Shutdown] Forcing garbage collection...');
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
          const client = (m as any).client;
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
console.log(`[Bootstrap] Initial memory: ${getMemoryUsage().heapUsedMB}MB / 8000MB`);
console.log(`[Bootstrap] GC available: ${!!global.gc}`);

boot();
