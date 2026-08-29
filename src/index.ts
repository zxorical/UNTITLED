```ts
/**
 * @module index
 * Application entry point – supervised / self-healing version.
 *
 * IMPORTANT:
 * - PM2 is the OUTER supervisor.
 * - This supervisor monitors the application from INSIDE the process.
 * - If the application becomes unrecoverably unhealthy, this process exits
 *   with code 1 so PM2 can restart it.
 *
 * Monitoring:
 *  1. V8 heap usage
 *  2. RSS memory
 *  3. Event-loop lag
 *  4. Unhandled rejection rate
 *  5. Discord tracker client health
 *  6. BotManager health where inspectable
 *  7. AutoJoiner health where inspectable
 *  8. VRFS upstream health
 *  9. Application heartbeat
 * 10. Recent incidents / diagnostics
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
import {
  vrfs,
  seby,
  health as vrfsHealth,
  getStatus as getVRFSStatus,
} from './middleware/api/vrfs.js';

// ============================================================================
// CONFIGURATION
// ============================================================================

const PORT = parseInt(process.env.PORT || '3000', 10) || 3000;

const CLIENT_READY_TIMEOUT_MS = 60_000;
const MAX_BOOT_RETRIES = 5;
const BOOT_RETRY_DELAY_MS = 15_000;
const BOT_MANAGER_START_TIMEOUT_MS = 10_000;
const SHUTDOWN_TIMEOUT_MS = 10_000;
const VRFS_HEALTH_INTERVAL_MS = 60_000;

// Supervisor
const SUPERVISOR_INTERVAL_MS = 15_000;

// Memory limits.
// V8's heap limit can be reached long before the VPS's total RAM is full,
// so we monitor both heap and RSS.
const MEMORY_HEAP_WARNING_MB = 400;
const MEMORY_HEAP_CRITICAL_MB = 460;
const MEMORY_HEAP_FATAL_MB = 500;

const MEMORY_RSS_WARNING_MB = 2500;
const MEMORY_RSS_CRITICAL_MB = 4500;
const MEMORY_RSS_FATAL_MB = 6000;

// Event loop
const EVENT_LOOP_WARNING_MS = 1000;
const EVENT_LOOP_CRITICAL_MS = 3000;

// Error rate
const ERROR_WINDOW_MS = 60_000;
const ERROR_WARNING_COUNT = 50;
const ERROR_CRITICAL_COUNT = 150;
const ERROR_FATAL_COUNT = 300;

// How long an unhealthy condition must persist before killing the process.
const UNHEALTHY_CONFIRMATION_MS = 30_000;

// Recent incident history.
const MAX_INCIDENTS = 100;

// ============================================================================
// STATE
// ============================================================================

let activeManagers: GiveawayManager[] = [];
let botManager: BotManager | null = null;
let autoJoiner: AutoJoinManager | null = null;

let statsInterval: ReturnType<typeof setInterval> | null = null;
let vrfsHealthInterval: ReturnType<typeof setInterval> | null = null;
let memoryCleanupInterval: ReturnType<typeof setInterval> | null = null;
let supervisorInterval: ReturnType<typeof setInterval> | null = null;
let eventLoopMonitorInterval: ReturnType<typeof setInterval> | null = null;

let shuttingDown = false;
let supervisorRunning = false;
let vrfsHealthRunning = false;
let restartRequested = false;

let lastVRFSHealth: Record<string, unknown> = {
  ok: false,
  status: 'not_checked',
  timestamp: null,
};

interface SupervisorState {
  startedAt: number;

  lastHeartbeatAt: number;
  lastMessageEventAt: number;
  lastMessageUpdateAt: number;

  lastEventLoopCheckAt: number;
  eventLoopLagMs: number;
  eventLoopMaxLagMs: number;

  lastManagerCheckAt: number;
  lastBotCheckAt: number;
  lastAutoJoinerCheckAt: number;
  lastVRFSCheckAt: number;

  totalUnhandledRejections: number;
  recentUnhandledRejections: number;

  totalClientErrors: number;
  recentClientErrors: number;

  consecutiveUnhealthyChecks: number;
  unhealthySince: number | null;

  lastIncident: {
    type: string;
    message: string;
    timestamp: string;
  } | null;

  incidents: Array<{
    type: string;
    message: string;
    timestamp: string;
  }>;
}

const supervisorState: SupervisorState = {
  startedAt: Date.now(),

  lastHeartbeatAt: Date.now(),
  lastMessageEventAt: Date.now(),
  lastMessageUpdateAt: Date.now(),

  lastEventLoopCheckAt: Date.now(),
  eventLoopLagMs: 0,
  eventLoopMaxLagMs: 0,

  lastManagerCheckAt: Date.now(),
  lastBotCheckAt: Date.now(),
  lastAutoJoinerCheckAt: Date.now(),
  lastVRFSCheckAt: Date.now(),

  totalUnhandledRejections: 0,
  recentUnhandledRejections: 0,

  totalClientErrors: 0,
  recentClientErrors: 0,

  consecutiveUnhealthyChecks: 0,
  unhealthySince: null,

  lastIncident: null,
  incidents: [],
};

// ============================================================================
// UTILITIES
// ============================================================================

function getMemoryUsage() {
  const mem = process.memoryUsage();

  return {
    heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
    heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
    rssMB: Math.round(mem.rss / 1024 / 1024),
    externalMB: Math.round(mem.external / 1024 / 1024),
    arrayBuffersMB: Math.round(mem.arrayBuffers / 1024 / 1024),
  };
}

function addIncident(type: string, message: string): void {
  const incident = {
    type,
    message,
    timestamp: new Date().toISOString(),
  };

  supervisorState.lastIncident = incident;

  supervisorState.incidents.push(incident);

  if (supervisorState.incidents.length > MAX_INCIDENTS) {
    supervisorState.incidents.shift();
  }

  logger.warn(`[Supervisor] ${type}: ${message}`, {
    component: 'Supervisor',
  });
}

function countRecentIncidents(
  type?: string,
  windowMs = ERROR_WINDOW_MS,
): number {
  const cutoff = Date.now() - windowMs;

  return supervisorState.incidents.filter((incident) => {
    if (new Date(incident.timestamp).getTime() < cutoff) {
      return false;
    }

    if (type && incident.type !== type) {
      return false;
    }

    return true;
  }).length;
}

// ============================================================================
// EVENT LOOP MONITOR
// ============================================================================

function startEventLoopMonitor(): void {
  if (eventLoopMonitorInterval) return;

  eventLoopMonitorInterval = setInterval(() => {
    const expected = 1000;
    const started = Date.now();

    setTimeout(() => {
      const elapsed = Date.now() - started;
      const lag = Math.max(0, elapsed - expected);

      supervisorState.lastEventLoopCheckAt = Date.now();
      supervisorState.eventLoopLagMs = lag;

      if (lag > supervisorState.eventLoopMaxLagMs) {
        supervisorState.eventLoopMaxLagMs = lag;
      }

      if (lag >= EVENT_LOOP_CRITICAL_MS) {
        addIncident(
          'event_loop_critical',
          `Event loop lag reached ${lag}ms`,
        );
      } else if (lag >= EVENT_LOOP_WARNING_MS) {
        logger.warn(
          `[Supervisor] Event loop lag: ${lag}ms`,
          {
            component: 'Supervisor',
          },
        );
      }
    }, expected);
  }, 1_000);

  eventLoopMonitorInterval.unref?.();
}

function stopEventLoopMonitor(): void {
  if (!eventLoopMonitorInterval) return;

  clearInterval(eventLoopMonitorInterval);
  eventLoopMonitorInterval = null;
}

// ============================================================================
// MEMORY MONITOR
// ============================================================================

function checkMemoryAndCleanup(): void {
  const mem = getMemoryUsage();

  // --------------------------------------------------------------------------
  // HEAP
  // --------------------------------------------------------------------------

  if (mem.heapUsedMB >= MEMORY_HEAP_FATAL_MB) {
    addIncident(
      'heap_fatal',
      `V8 heap reached ${mem.heapUsedMB}MB`,
    );

    if (global.gc) {
      try {
        global.gc();
      } catch {}
    }

    void requestSupervisedRestart(
      `V8 heap exceeded ${MEMORY_HEAP_FATAL_MB}MB`,
    );

    return;
  }

  if (mem.heapUsedMB >= MEMORY_HEAP_CRITICAL_MB) {
    logger.error(
      `[Supervisor] CRITICAL heap usage: ${mem.heapUsedMB}MB`,
      {
        component: 'Memory',
        memory: mem,
      },
    );

    try {
      for (const manager of activeManagers) {
        const managerAny = manager as any;

        managerAny.giveawayTextCache?.clear?.();
        managerAny.creationCache?.clear?.();
        managerAny.processingMessages?.clear?.();
      }
    } catch {}

    if (global.gc) {
      try {
        global.gc();
      } catch {}
    }
  } else if (mem.heapUsedMB >= MEMORY_HEAP_WARNING_MB) {
    logger.warn(
      `[Supervisor] High heap usage: ${mem.heapUsedMB}MB`,
      {
        component: 'Memory',
        memory: mem,
      },
    );
  }

  // --------------------------------------------------------------------------
  // RSS
  // --------------------------------------------------------------------------

  if (mem.rssMB >= MEMORY_RSS_FATAL_MB) {
    addIncident(
      'rss_fatal',
      `RSS reached ${mem.rssMB}MB`,
    );

    if (global.gc) {
      try {
        global.gc();
      } catch {}
    }

    void requestSupervisedRestart(
      `RSS exceeded ${MEMORY_RSS_FATAL_MB}MB`,
    );

    return;
  }

  if (mem.rssMB >= MEMORY_RSS_CRITICAL_MB) {
    logger.error(
      `[Supervisor] CRITICAL RSS: ${mem.rssMB}MB`,
      {
        component: 'Memory',
        memory: mem,
      },
    );

    if (global.gc) {
      try {
        global.gc();
      } catch {}
    }
  } else if (mem.rssMB >= MEMORY_RSS_WARNING_MB) {
    logger.warn(
      `[Supervisor] High RSS: ${mem.rssMB}MB`,
      {
        component: 'Memory',
        memory: mem,
      },
    );
  }
}

function startMemoryMonitoring(): void {
  if (memoryCleanupInterval) return;

  memoryCleanupInterval = setInterval(() => {
    if (!shuttingDown) {
      checkMemoryAndCleanup();
    }
  }, 15_000);

  memoryCleanupInterval.unref?.();
}

function stopMemoryMonitoring(): void {
  if (!memoryCleanupInterval) return;

  clearInterval(memoryCleanupInterval);
  memoryCleanupInterval = null;
}

// ============================================================================
// DISCORD HEALTH
// ============================================================================

function inspectDiscordClients(): {
  healthy: number;
  unhealthy: number;
  details: Array<Record<string, unknown>>;
} {
  let healthy = 0;
  let unhealthy = 0;

  const details: Array<Record<string, unknown>> = [];

  for (const manager of activeManagers) {
    try {
      const managerAny = manager as any;
      const client = managerAny.client as Client | undefined;

      if (!client) {
        unhealthy++;

        details.push({
          healthy: false,
          reason: 'missing_client',
        });

        continue;
      }

      const user = client.user;

      if (!user) {
        unhealthy++;

        details.push({
          healthy: false,
          reason: 'client_has_no_user',
        });

        continue;
      }

      healthy++;

      details.push({
        healthy: true,
        userId: user.id,
        username: user.username,
        guilds: client.guilds?.cache?.size ?? 0,
      });
    } catch (err) {
      unhealthy++;

      details.push({
        healthy: false,
        reason: formatError(err),
      });
    }
  }

  return {
    healthy,
    unhealthy,
    details,
  };
}

function inspectBotManager(): Record<string, unknown> {
  if (!botManager) {
    return {
      available: false,
      healthy: false,
      reason: 'not_initialized',
    };
  }

  try {
    const managerAny = botManager as any;

    const client =
      managerAny.client ??
      managerAny.discordClient ??
      managerAny._client;

    if (client) {
      return {
        available: true,
        healthy: !!client.user,
        userId: client.user?.id ?? null,
        username: client.user?.username ?? null,
      };
    }

    // BotManager may not expose its client publicly.
    // In that case, the existence of the manager alone isn't enough
    // to declare it dead.
    return {
      available: true,
      healthy: true,
      inspectable: false,
    };
  } catch (err) {
    return {
      available: true,
      healthy: false,
      error: formatError(err),
    };
  }
}

function inspectAutoJoiner(): Record<string, unknown> {
  if (!autoJoiner) {
    return {
      available: false,
      healthy: false,
      reason: 'not_initialized',
    };
  }

  try {
    const stats = autoJoiner.getStats();

    return {
      available: true,
      healthy: true,
      ...stats,
    };
  } catch (err) {
    return {
      available: true,
      healthy: false,
      error: formatError(err),
    };
  }
}

// ============================================================================
// SUPERVISOR
// ============================================================================

async function runSupervisorCheck(): Promise<void> {
  if (shuttingDown || supervisorRunning || restartRequested) {
    return;
  }

  supervisorRunning = true;

  try {
    const now = Date.now();
    const mem = getMemoryUsage();

    supervisorState.lastHeartbeatAt = now;

    // ------------------------------------------------------------------------
    // MEMORY
    // ------------------------------------------------------------------------

    if (
      mem.heapUsedMB >= MEMORY_HEAP_FATAL_MB ||
      mem.rssMB >= MEMORY_RSS_FATAL_MB
    ) {
      await requestSupervisedRestart(
        `Fatal memory condition: heap=${mem.heapUsedMB}MB rss=${mem.rssMB}MB`,
      );

      return;
    }

    // ------------------------------------------------------------------------
    // TRACKER CLIENTS
    // ------------------------------------------------------------------------

    const discordHealth = inspectDiscordClients();

    supervisorState.lastManagerCheckAt = now;

    if (
      activeManagers.length > 0 &&
      discordHealth.healthy === 0
    ) {
      addIncident(
        'discord_all_clients_unhealthy',
        `All ${activeManagers.length} tracker clients are unhealthy`,
      );
    }

    // ------------------------------------------------------------------------
    // BOT
    // ------------------------------------------------------------------------

    const botHealth = inspectBotManager();

    supervisorState.lastBotCheckAt = now;

    if (botManager && botHealth.healthy === false) {
      addIncident(
        'bot_unhealthy',
        `BotManager appears unhealthy: ${JSON.stringify(botHealth)}`,
      );
    }

    // ------------------------------------------------------------------------
    // AUTOJOINER
    // ------------------------------------------------------------------------

    const autoJoinerHealth = inspectAutoJoiner();

    supervisorState.lastAutoJoinerCheckAt = now;

    // We intentionally do NOT restart just because activeSessions is 0.
    // There may simply be no premium sessions.
    if (
      autoJoiner &&
      autoJoinerHealth.healthy === false
    ) {
      addIncident(
        'autojoiner_unhealthy',
        `AutoJoiner inspection failed`,
      );
    }

    // ------------------------------------------------------------------------
    // ERROR RATE
    // ------------------------------------------------------------------------

    const recentRejections = countRecentIncidents(
      'unhandled_rejection',
    );

    const recentClientErrors = countRecentIncidents(
      'client_error',
    );

    supervisorState.recentUnhandledRejections = recentRejections;
    supervisorState.recentClientErrors = recentClientErrors;

    const recentErrors =
      recentRejections +
      recentClientErrors;

    if (recentErrors >= ERROR_FATAL_COUNT) {
      addIncident(
        'error_rate_fatal',
        `${recentErrors} application errors detected in the last minute`,
      );

      await requestSupervisedRestart(
        `Error rate exceeded ${ERROR_FATAL_COUNT}/minute`,
      );

      return;
    }

    if (recentErrors >= ERROR_CRITICAL_COUNT) {
      logger.error(
        `[Supervisor] Critical error rate: ${recentErrors}/minute`,
        {
          component: 'Supervisor',
          recentRejections,
          recentClientErrors,
        },
      );
    } else if (recentErrors >= ERROR_WARNING_COUNT) {
      logger.warn(
        `[Supervisor] High error rate: ${recentErrors}/minute`,
        {
          component: 'Supervisor',
          recentRejections,
          recentClientErrors,
        },
      );
    }

    // ------------------------------------------------------------------------
    // EVENT LOOP
    // ------------------------------------------------------------------------

    if (
      supervisorState.eventLoopLagMs >= EVENT_LOOP_CRITICAL_MS
    ) {
      addIncident(
        'event_loop_critical',
        `Event loop lag: ${supervisorState.eventLoopLagMs}ms`,
      );
    }

    // ------------------------------------------------------------------------
    // STALE MESSAGE ACTIVITY
    // ------------------------------------------------------------------------
    //
    // IMPORTANT:
    // We only WARN here.
    //
    // A quiet Discord server is perfectly normal, so lack of messages alone
    // must never restart the entire tracker.
    //

    const messageAgeMs =
      now - supervisorState.lastMessageEventAt;

    if (messageAgeMs >= 15 * 60_000) {
      logger.warn(
        `[Supervisor] No messageCreate event for ${Math.round(
          messageAgeMs / 1000,
        )}s`,
        {
          component: 'Supervisor',
          managers: activeManagers.length,
        },
      );
    }

    // ------------------------------------------------------------------------
    // OVERALL STATUS
    // ------------------------------------------------------------------------

    const definitelyUnhealthy =
      activeManagers.length === 0 ||
      discordHealth.healthy === 0 ||
      mem.heapUsedMB >= MEMORY_HEAP_CRITICAL_MB ||
      mem.rssMB >= MEMORY_RSS_CRITICAL_MB;

    if (definitelyUnhealthy) {
      if (!supervisorState.unhealthySince) {
        supervisorState.unhealthySince = now;
      }

      supervisorState.consecutiveUnhealthyChecks++;

      const unhealthyFor =
        now - supervisorState.unhealthySince;

      logger.warn(
        `[Supervisor] Application unhealthy for ${Math.round(
          unhealthyFor / 1000,
        )}s`,
        {
          component: 'Supervisor',
          managers: discordHealth,
          bot: botHealth,
          autoJoiner: autoJoinerHealth,
          memory: mem,
          eventLoopLagMs: supervisorState.eventLoopLagMs,
        },
      );

      if (
        unhealthyFor >= UNHEALTHY_CONFIRMATION_MS &&
        activeManagers.length === 0
      ) {
        await requestSupervisedRestart(
          'No active tracker managers',
        );

        return;
      }

      if (
        unhealthyFor >= UNHEALTHY_CONFIRMATION_MS &&
        discordHealth.healthy === 0 &&
        activeManagers.length > 0
      ) {
        await requestSupervisedRestart(
          'All tracker Discord clients became unhealthy',
        );

        return;
      }
    } else {
      supervisorState.unhealthySince = null;
      supervisorState.consecutiveUnhealthyChecks = 0;
    }

    // ------------------------------------------------------------------------
    // HEARTBEAT
    // ------------------------------------------------------------------------

    logger.info('[Supervisor] 💓 heartbeat', {
      component: 'Supervisor',
      uptimeSeconds: Math.round(process.uptime()),
      memory: mem,
      eventLoopLagMs: supervisorState.eventLoopLagMs,
      eventLoopMaxLagMs: supervisorState.eventLoopMaxLagMs,

      trackerClients: {
        activeManagers: activeManagers.length,
        healthy: discordHealth.healthy,
        unhealthy: discordHealth.unhealthy,
      },

      bot: botHealth,

      autoJoiner: autoJoinerHealth,

      errors: {
        recentUnhandledRejections: recentRejections,
        recentClientErrors,
        totalUnhandledRejections:
          supervisorState.totalUnhandledRejections,
        totalClientErrors:
          supervisorState.totalClientErrors,
      },

      lastMessageSecondsAgo: Math.round(
        messageAgeMs / 1000,
      ),

      vrfs: lastVRFSHealth,

      lastIncident: supervisorState.lastIncident,
    });
  } catch (err) {
    addIncident(
      'supervisor_error',
      formatError(err),
    );
  } finally {
    supervisorRunning = false;
  }
}

function startSupervisor(): void {
  if (supervisorInterval) return;

  logger.info(
    '[Supervisor] 🛡️ Application supervisor started',
    {
      component: 'Supervisor',
      intervalMs: SUPERVISOR_INTERVAL_MS,
    },
  );

  supervisorInterval = setInterval(() => {
    void runSupervisorCheck();
  }, SUPERVISOR_INTERVAL_MS);

  supervisorInterval.unref?.();

  void runSupervisorCheck();
}

function stopSupervisor(): void {
  if (!supervisorInterval) return;

  clearInterval(supervisorInterval);
  supervisorInterval = null;

  logger.info(
    '[Supervisor] Supervisor stopped',
    {
      component: 'Supervisor',
    },
  );
}

// ============================================================================
// CONTROLLED RESTART
// ============================================================================

async function requestSupervisedRestart(
  reason: string,
): Promise<void> {
  if (restartRequested || shuttingDown) {
    return;
  }

  restartRequested = true;

  addIncident(
    'supervised_restart',
    reason,
  );

  logger.error(
    '🚨 [Supervisor] CONTROLLED RESTART REQUESTED',
    {
      component: 'Supervisor',
      reason,
      memory: getMemoryUsage(),
      uptimeSeconds: process.uptime(),
      managers: activeManagers.length,
      eventLoopLagMs: supervisorState.eventLoopLagMs,
      recentErrors:
        supervisorState.recentUnhandledRejections +
        supervisorState.recentClientErrors,
      lastIncident: supervisorState.lastIncident,
    },
  );

  // Let the normal shutdown machinery perform cleanup.
  try {
    await performShutdown(
      `SUPERVISOR: ${reason}`,
      1,
    );
  } catch (err) {
    console.error(
      '[Supervisor] Controlled shutdown failed:',
      err,
    );

    process.exit(1);
  }
}

// ============================================================================
// VRFS MONITORING
// ============================================================================

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

    supervisorState.lastVRFSCheckAt = Date.now();

    if (result.ok) {
      logger.info(
        'VRFS upstream health check passed',
        {
          component: 'VRFS',
          latencyMs: result.latencyMs,
          services: result.services,
        },
      );
    } else {
      logger.warn(
        'VRFS upstream health check degraded',
        {
          component: 'VRFS',
          latencyMs: result.latencyMs,
          services: result.services,
        },
      );
    }
  } catch (err) {
    lastVRFSHealth = {
      ok: false,
      status: 'error',
      error: formatError(err),
      timestamp: new Date().toISOString(),
      durationMs: Date.now() - started,
    };

    logger.warn(
      'VRFS upstream health check failed',
      {
        component: 'VRFS',
        error: formatError(err),
      },
    );
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

  vrfsHealthInterval.unref?.();
}

function stopVRFSMonitoring(): void {
  if (!vrfsHealthInterval) return;

  clearInterval(vrfsHealthInterval);
  vrfsHealthInterval = null;
  vrfsHealthRunning = false;
}

// ============================================================================
// HEALTH SERVER
// ============================================================================

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

    const discordHealth = inspectDiscordClients();
    const botHealth = inspectBotManager();

    const stats = {
      status:
        restartRequested
          ? 'restarting'
          : 'ok',

      uptime: process.uptime(),
      timestamp: new Date().toISOString(),

      memory: mem,

      eventLoop: {
        currentLagMs:
          supervisorState.eventLoopLagMs,
        maxLagMs:
          supervisorState.eventLoopMaxLagMs,
      },

      sessions: {
        activeSessions,
        totalSessions,
      },

      trackers: {
        activeManagers: activeManagers.length,
        healthy: discordHealth.healthy,
        unhealthy: discordHealth.unhealthy,
      },

      bot: botHealth,

      errors: {
        totalUnhandledRejections:
          supervisorState.totalUnhandledRejections,
        recentUnhandledRejections:
          supervisorState.recentUnhandledRejections,

        totalClientErrors:
          supervisorState.totalClientErrors,
        recentClientErrors:
          supervisorState.recentClientErrors,
      },

      supervisor: {
        running: !!supervisorInterval,
        lastHeartbeatAt:
          new Date(
            supervisorState.lastHeartbeatAt,
          ).toISOString(),

        unhealthySince:
          supervisorState.unhealthySince
            ? new Date(
                supervisorState.unhealthySince,
              ).toISOString()
            : null,

        consecutiveUnhealthyChecks:
          supervisorState.consecutiveUnhealthyChecks,

        lastIncident:
          supervisorState.lastIncident,

        recentIncidents:
          supervisorState.incidents.slice(-20),
      },

      activity: {
        lastMessageEventAt:
          new Date(
            supervisorState.lastMessageEventAt,
          ).toISOString(),

        lastMessageSecondsAgo:
          Math.round(
            (
              Date.now() -
              supervisorState.lastMessageEventAt
            ) / 1000,
          ),
      },

      gcAvailable: !!global.gc,

      vrfs: vrfsStatus,
      vrfsUpstream: lastVRFSHealth,
    };

    res.writeHead(200, {
      'Content-Type':
        'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
    });

    res.end(
      JSON.stringify(stats, null, 2),
    );

    return;
  }

  // --------------------------------------------------------------------------
  // IMPORTANT:
  // There is intentionally NO public /shutdown endpoint.
  // --------------------------------------------------------------------------

  if (req.url === '/gc') {
    if (global.gc) {
      global.gc();

      res.writeHead(200, {
        'Content-Type':
          'text/plain; charset=utf-8',
      });

      res.end('GC forced');

      return;
    }

    res.writeHead(500, {
      'Content-Type':
        'text/plain; charset=utf-8',
    });

    res.end(
      'GC not available (run with --expose-gc)',
    );

    return;
  }

  res.writeHead(404);
  res.end();
});

healthServer.listen(
  PORT,
  '0.0.0.0',
  () => {
    console.log(
      `[Bootstrap] Health server listening on port ${PORT}`,
    );

    console.log(
      `[Bootstrap] Initial memory:`,
      getMemoryUsage(),
    );
  },
);

healthServer.on('error', (err) => {
  console.error(
    '[Bootstrap] Health server error:',
    err,
  );

  addIncident(
    'health_server_error',
    formatError(err),
  );
});

// ============================================================================
// GLOBAL ERROR HANDLERS
// ============================================================================

process.on(
  'uncaughtException',
  (err) => {
    const message = formatError(err);

    console.error(
      '🔥 UNCAUGHT EXCEPTION:',
      err,
    );

    addIncident(
      'uncaught_exception',
      message,
    );

    try {
      logger.error(
        'Uncaught exception',
        {
          component: 'Process',
          error: message,
          memory: getMemoryUsage(),
        },
      );
    } catch {}

    // An uncaught exception leaves Node in an unknown state.
    // Let PM2 restart us rather than pretending everything is fine.
    if (!shuttingDown) {
      void requestSupervisedRestart(
        `Uncaught exception: ${message}`,
      );
    }
  },
);

process.on(
  'unhandledRejection',
  (reason) => {
    const message = formatError(reason);

    supervisorState.totalUnhandledRejections++;

    // Special handling for the very noisy Discord component error.
    const isComponentValidation =
      message.includes(
        'COMPONENT_VALIDATION_FAILED',
      );

    addIncident(
      'unhandled_rejection',
      isComponentValidation
        ? 'Discord component validation failed'
        : message,
    );

    console.error(
      '🔥 UNHANDLED REJECTION:',
      reason,
    );

    try {
      logger.warn(
        'Unhandled rejection',
        {
          component: 'Process',
          reason: message,
          componentValidation:
            isComponentValidation,
        },
      );
    } catch {}
  },
);

process.on(
  'beforeExit',
  (code) => {
    logger.warn(
      '⚠️ Node beforeExit',
      {
        component: 'Process',
        code,
        uptime: process.uptime(),
        memory: getMemoryUsage(),
      },
    );

    addIncident(
      'before_exit',
      `Node beforeExit with code ${code}`,
    );
  },
);

process.on(
  'exit',
  (code) => {
    console.error(
      '💀 PROCESS EXIT',
      {
        code,
        uptime: process.uptime(),
        memory: getMemoryUsage(),
      },
    );
  },
);

process.setMaxListeners(100);

// ============================================================================
// MAIN
// ============================================================================

async function main(): Promise<void> {
  reconfigureLogger(
    CONFIG.logLevel,
    CONFIG.logDir,
  );

  logger.info(
    '╔═══════════════════════════════════════╗',
    { component: 'Bootstrap' },
  );

  logger.info(
    '║    Discord Giveaway Tracker v2        ║',
    { component: 'Bootstrap' },
  );

  logger.info(
    '╚═══════════════════════════════════════╝',
    { component: 'Bootstrap' },
  );

  logger.info(
    'Initial memory',
    {
      component: 'Bootstrap',
      memory: getMemoryUsage(),
      gcAvailable: !!global.gc,
    },
  );

  logger.info(
    'Configuration',
    {
      component: 'Bootstrap',
      accounts: CONFIG.tokens.length,
      monitoredChannels:
        CONFIG.monitoredChannels.length ||
        'all',
      trackerChannel:
        CONFIG.trackerChannelId,
      cooldown:
        CONFIG.notificationCooldown,
      dbPath:
        CONFIG.dbPath,
    },
  );

  // ==========================================================================
  // VRFS
  // ==========================================================================

  logger.info(
    'Initializing VRFS/Seby middleware...',
    {
      component: 'VRFS',
    },
  );

  try {
    const vrfsStatus =
      getVRFSStatus();

    logger.info(
      'VRFS/Seby middleware initialized',
      {
        component: 'VRFS',
        status: vrfsStatus,
      },
    );
  } catch (err) {
    logger.warn(
      'VRFS middleware initialization warning',
      {
        component: 'VRFS',
        error: formatError(err),
      },
    );
  }

  startVRFSMonitoring();

  // ==========================================================================
  // DATABASE
  // ==========================================================================

  try {
    await Promise.race([
      getDb(),

      delay(10_000).then(() => {
        throw new Error(
          'Database connection timeout',
        );
      }),
    ]);

    logger.info(
      'Database connection established',
      {
        component: 'Bootstrap',
      },
    );
  } catch (err) {
    logger.error(
      'Database connection failed',
      {
        component: 'Bootstrap',
        error: formatError(err),
      },
    );

    throw err;
  }

  cleanupOldGiveaways(30).catch(
    (err) => {
      logger.warn(
        'cleanupOldGiveaways error',
        {
          error: formatError(err),
        },
      );
    },
  );

  // ==========================================================================
  // BOTMANAGER
  // ==========================================================================

  logger.info(
    'Initializing BotManager...',
    {
      component: 'Bootstrap',
    },
  );

  try {
    const startPromise =
      (async () => {
        botManager =
          new BotManager(
            CONFIG.botToken,
          );

        await botManager.start();
      })();

    await Promise.race([
      startPromise,

      delay(
        BOT_MANAGER_START_TIMEOUT_MS,
      ).then(() => {
        throw new Error(
          'BotManager.start() timed out',
        );
      }),
    ]);

    logger.info(
      'BotManager started successfully.',
      {
        component: 'Bootstrap',
      },
    );
  } catch (err) {
    logger.warn(
      'BotManager failed to start (continuing without it)',
      {
        component: 'Bootstrap',
        error: formatError(err),
      },
    );

    botManager = null;
  }

  // ==========================================================================
  // TRACKER SELF-BOT CLIENTS
  // ==========================================================================

  activeManagers = [];

  let authFailures = 0;
  let clientsStarted = 0;

  const BATCH_SIZE = 3;
  const tokenBatches: string[][] = [];

  for (
    let i = 0;
    i < CONFIG.tokens.length;
    i += BATCH_SIZE
  ) {
    tokenBatches.push(
      CONFIG.tokens.slice(
        i,
        i + BATCH_SIZE,
      ),
    );
  }

  for (const batch of tokenBatches) {
    const currentMem =
      getMemoryUsage();

    if (
      currentMem.heapUsedMB >=
      MEMORY_HEAP_CRITICAL_MB
    ) {
      logger.warn(
        `Memory high (${currentMem.heapUsedMB}MB), stopping account creation`,
        {
          component: 'Bootstrap',
          started: clientsStarted,
        },
      );

      break;
    }

    const batchPromises =
      batch.map(
        async (
          token,
          batchIndex,
        ) => {
          const globalIndex =
            clientsStarted +
            batchIndex;

          const label =
            `acc${globalIndex + 1}`;

          if (
            !token ||
            token.trim() === ''
          ) {
            logger.warn(
              `Token ${globalIndex + 1} is empty – skipping`,
              {
                component: 'Bootstrap',
              },
            );

            return null;
          }

          let client:
            | Client
            | null = null;

          try {
            logger.info(
              `Starting account ${globalIndex + 1}/${CONFIG.tokens.length} (${label})...`,
              {
                component: 'Bootstrap',
              },
            );

            client =
              new Client();

            client.setMaxListeners(
              50,
            );

            if (
              CONFIG.logLevel ===
              'debug'
            ) {
              client.on(
                'debug',
                (info) => {
                  logger.debug(
                    `[${label}] Debug: ${info}`,
                    {
                      component:
                        'Client',
                    },
                  );
                },
              );
            }

            client.on(
              'ready',
              () => {
                logger.info(
                  `[${label}] Client ready`,
                  {
                    component:
                      'Client',
                  },
                );
              },
            );

            client.on(
              'error',
              (err) => {
                supervisorState.totalClientErrors++;

                addIncident(
                  'client_error',
                  `[${label}] ${formatError(err)}`,
                );

                logger.error(
                  `[${label}] Client error`,
                  {
                    component:
                      'Client',
                    error:
                      formatError(
                        err,
                      ),
                  },
                );
              },
            );

            const manager =
              new GiveawayManager(
                client,
                logger,
                token,
                label,
                botManager,
              );

            registerDiscordEvents(
              client,
              manager,
              label,
            );

            logger.info(
              `[${label}] Calling waitForReady...`,
              {
                component:
                  'Bootstrap',
              },
            );

            try {
              await Promise.race([
                waitForReady(
                  client,
                  token,
                  label,
                ),

                delay(
                  CLIENT_READY_TIMEOUT_MS,
                ).then(() => {
                  throw new Error(
                    `Client ${label} did not become ready`,
                  );
                }),
              ]);
            } catch (raceErr) {
              try {
                client.removeAllListeners();
                await client.destroy();
              } catch {}

              throw raceErr;
            }

            activeManagers.push(
              manager,
            );

            logger.info(
              `Account ${label} connected`,
              {
                component:
                  'Bootstrap',

                userId:
                  client.user?.id,

                username:
                  client.user
                    ?.username,

                guilds:
                  client.guilds
                    .cache.size,

                memory:
                  getMemoryUsage(),
              },
            );

            return manager;
          } catch (err) {
            const message =
              formatError(err);

            const isAuth =
              /token|auth|login|invalid|unauthorized|401|403/i.test(
                message,
              );

            if (isAuth) {
              authFailures++;

              logger.warn(
                `Account ${label} skipped (auth error)`,
                {
                  component:
                    'Bootstrap',
                  error:
                    message,
                },
              );

              return null;
            }

            logger.error(
              `Account ${label} failed`,
              {
                component:
                  'Bootstrap',
                error:
                  message,
              },
            );

            return null;
          }
        },
      );

    const results =
      await Promise.all(
        batchPromises,
      );

    for (const result of results) {
      if (result) {
        clientsStarted++;
      }
    }

    await delay(1000);
  }

  if (
    activeManagers.length === 0 &&
    authFailures > 0 &&
    authFailures ===
      CONFIG.tokens.length
  ) {
    throw Object.assign(
      new Error(
        'All tokens failed authentication',
      ),
      {
        code:
          'AUTH_ALL_FAILED',
      },
    );
  }

  if (
    activeManagers.length === 0
  ) {
    throw new Error(
      'No accounts could be started',
    );
  }

  logger.info(
    `✅ ${activeManagers.length} account(s) running`,
    {
      component:
        'Bootstrap',

      active:
        activeManagers.length,

      failures:
        authFailures,

      memory:
        getMemoryUsage(),
    },
  );

  // ==========================================================================
  // AUTOJOINER
  // ==========================================================================

  try {
    logger.info(
      'Starting AutoJoiner...',
      {
        component:
          'Bootstrap',
      },
    );

    autoJoiner =
      new AutoJoinManager();

    await Promise.race([
      autoJoiner.startAllSessions(),

      delay(
        60_000,
      ).then(() => {
        throw new Error(
          'AutoJoiner start timed out',
        );
      }),
    ]);

    await Promise.race([
      autoJoiner.restoreSessionsFromDatabase(),

      delay(
        30_000,
      ).then(() => {
        throw new Error(
          'AutoJoiner restore timed out',
        );
      }),
    ]);

    const stats =
      autoJoiner.getStats();

    logger.info(
      `✅ AutoJoiner running with ${stats.activeSessions}/${stats.totalSessions} active sessions`,
      {
        component:
          'Bootstrap',
      },
    );
  } catch (err) {
    logger.warn(
      'AutoJoiner failed to start',
      {
        component:
          'Bootstrap',
        error:
          formatError(err),
      },
    );

    autoJoiner = null;
  }

  // ==========================================================================
  // INITIAL SCRIM STATS
  // ==========================================================================

  try {
    const scrimStats =
      await getScrimStats();

    logger.info(
      `📊 Scrim Stats: ${scrimStats.total} total, ${scrimStats.active} active, ${scrimStats.servers} servers`,
      {
        component:
          'Bootstrap',

        scrims:
          scrimStats
            .byType.scrim,

        squidGames:
          scrimStats
            .byType.squid_game,

        gagaballs:
          scrimStats
            .byType.gagaball,
      },
    );
  } catch {}

  // ==========================================================================
  // PERIODIC STATS
  // ==========================================================================

  statsInterval =
    setInterval(() => {
      if (shuttingDown) {
        return;
      }

      for (
        const manager of
        activeManagers
      ) {
        try {
          manager.logStats();
        } catch {}
      }

      if (!shuttingDown) {
        getScrimStats()
          .then(
            (scrimStats) => {
              logger.info(
                `📊 Scrim Stats: ${scrimStats.total} total, ${scrimStats.active} active`,
                {
                  component:
                    'Bootstrap',

                  scrims:
                    scrimStats
                      .byType.scrim,

                  squidGames:
                    scrimStats
                      .byType
                      .squid_game,

                  gagaballs:
                    scrimStats
                      .byType
                      .gagaball,

                  servers:
                    scrimStats.servers,
                },
              );
            },
          )
          .catch(() => {});
      }

      if (
        autoJoiner &&
        !shuttingDown
      ) {
        try {
          const stats =
            autoJoiner.getStats();

          logger.info(
            `AutoJoiner: ${stats.activeSessions}/${stats.totalSessions} sessions active`,
            {
              component:
                'Bootstrap',

              memory:
                getMemoryUsage(),
            },
          );
        } catch {}
      }
    }, CONFIG.statsIntervalMs);

  statsInterval.unref?.();

  // ==========================================================================
  // SUPERVISION
  // ==========================================================================

  startEventLoopMonitor();
  startMemoryMonitoring();
  startSupervisor();

  logger.info(
    '🟢 Giveaway tracker is LIVE',
    {
      component:
        'Bootstrap',

      accounts:
        activeManagers.length,

      statsEvery:
        `${CONFIG.statsIntervalMs / 1000}s`,

      supervisorEvery:
        `${SUPERVISOR_INTERVAL_MS / 1000}s`,

      memory:
        getMemoryUsage(),
    },
  );
}

// ============================================================================
// DISCORD EVENT HANDLERS
// ============================================================================

function registerDiscordEvents(
  client: Client,
  manager: GiveawayManager,
  label: string,
): void {
  const maxListeners = 50;

  client.setMaxListeners(
    maxListeners,
  );

  const messageCreateHandler =
    (msg: Message) => {
      if (
        !msg.guild ||
        shuttingDown
      ) {
        return;
      }

      supervisorState.lastMessageEventAt =
        Date.now();

      manager
        .handleMessage(msg)
        .catch((err) => {
          const message =
            formatError(err);

          addIncident(
            'message_handler_error',
            `[${label}] ${message}`,
          );

          logger.error(
            'messageCreate handler error',
            {
              component:
                'Events',

              error:
                message,

              messageId:
                msg.id,
            },
          );
        });
    };

  const messageUpdateHandler =
    (
      _old: any,
      updated: any,
    ) => {
      if (
        !updated.id ||
        !updated.channel ||
        shuttingDown
      ) {
        return;
      }

      supervisorState.lastMessageUpdateAt =
        Date.now();

      manager
        .handleMessage(
          updated as Message,
        )
        .catch((err) => {
          const message =
            formatError(err);

          addIncident(
            'message_update_error',
            `[${label}] ${message}`,
          );

          logger.error(
            'messageUpdate handler error',
            {
              component:
                'Events',

              error:
                message,

              messageId:
                updated.id,
            },
          );
        });
    };

  const guildCreateHandler =
    (guild: any) => {
      if (shuttingDown) {
        return;
      }

      logger.info(
        'Joined server',
        {
          component:
            'Events',

          guildId:
            guild.id,

          guildName:
            guild.name,

          memberCount:
            guild.memberCount,
        },
      );
    };

  const guildDeleteHandler =
    (guild: any) => {
      if (shuttingDown) {
        return;
      }

      logger.info(
        'Left server',
        {
          component:
            'Events',

          guildId:
            guild.id,

          guildName:
            guild.name,
        },
      );

      try {
        manager.clearInviteCache(
          guild.id,
        );
      } catch {}
    };

  const disconnectHandler =
    () => {
      if (shuttingDown) {
        return;
      }

      addIncident(
        'discord_disconnect',
        `[${label}] Discord client disconnected`,
      );

      logger.warn(
        `[${label}] Disconnected`,
        {
          component:
            'Events',
        },
      );
    };

  const reconnectingHandler =
    () => {
      if (shuttingDown) {
        return;
      }

      logger.info(
        `[${label}] Reconnecting...`,
        {
          component:
            'Events',
        },
      );
    };

  const errorHandler =
    (err: Error) => {
      if (shuttingDown) {
        return;
      }

      supervisorState.totalClientErrors++;

      addIncident(
        'client_error',
        `[${label}] ${formatError(err)}`,
      );

      logger.error(
        `[${label}] Client error`,
        {
          component:
            'Events',

          error:
            formatError(err),
        },
      );
    };

  (
    manager as any
  )._handlers = {
    messageCreate:
      messageCreateHandler,

    messageUpdate:
      messageUpdateHandler,

    guildCreate:
      guildCreateHandler,

    guildDelete:
      guildDeleteHandler,

    disconnect:
      disconnectHandler,

    reconnecting:
      reconnectingHandler,

    error:
      errorHandler,
  };

  client.on(
    'messageCreate',
    messageCreateHandler,
  );

  client.on(
    'messageUpdate',
    messageUpdateHandler,
  );

  client.on(
    'guildCreate',
    guildCreateHandler,
  );

  client.on(
    'guildDelete',
    guildDeleteHandler,
  );

  client.on(
    'disconnect',
    disconnectHandler,
  );

  client.on(
    'reconnecting',
    reconnectingHandler,
  );

  client.on(
    'error',
    errorHandler,
  );
}

// ============================================================================
// LOGIN
// ============================================================================

function waitForReady(
  client: Client,
  token: string,
  label: string,
): Promise<void> {
  return new Promise(
    (resolve, reject) => {
      console.log(
        `[${label}] waitForReady: setting up listeners and calling login...`,
      );

      let resolved = false;

      const readyHandler =
        () => {
          if (resolved) {
            return;
          }

          resolved = true;

          console.log(
            `[${label}] waitForReady: ready event received`,
          );

          cleanup();
          resolve();
        };

      const errorHandler =
        (err: Error) => {
          if (resolved) {
            return;
          }

          resolved = true;

          console.error(
            `[${label}] waitForReady: error event received`,
            err,
          );

          cleanup();
          reject(err);
        };

      const cleanup =
        () => {
          client.off(
            'ready',
            readyHandler,
          );

          client.off(
            'error',
            errorHandler,
          );
        };

      client.once(
        'ready',
        readyHandler,
      );

      client.once(
        'error',
        errorHandler,
      );

      client
        .login(token)
        .then(() => {
          console.log(
            `[${label}] waitForReady: client.login() resolved`,
          );
        })
        .catch((err) => {
          if (resolved) {
            return;
          }

          resolved = true;

          console.error(
            `[${label}] waitForReady: client.login() rejected`,
            err,
          );

          cleanup();

          reject(
            new Error(
              `Login failed: ${formatError(err)}`,
            ),
          );
        });
    },
  );
}

// ============================================================================
// SHUTDOWN
// ============================================================================

async function performShutdown(
  reason: string,
  exitCode: number,
): Promise<void> {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  console.log(
    `[Shutdown] ${reason}`,
  );

  stopSupervisor();
  stopEventLoopMonitor();
  stopMemoryMonitoring();
  stopVRFSMonitoring();

  if (statsInterval) {
    clearInterval(statsInterval);
    statsInterval = null;
  }

  // --------------------------------------------------------------------------
  // TRACKER MANAGERS
  // --------------------------------------------------------------------------

  console.log(
    `[Shutdown] Stopping ${activeManagers.length} account managers...`,
  );

  const managerPromises =
    activeManagers.map(
      async (manager) => {
        try {
          await manager.shutdown();

          const managerAny =
            manager as any;

          const client =
            managerAny.client;

          if (client) {
            try {
              const handlers =
                managerAny._handlers;

              if (handlers) {
                for (
                  const [
                    event,
                    handler,
                  ] of Object.entries(
                    handlers,
                  )
                ) {
                  try {
                    client.off(
                      event,
                      handler as any,
                    );
                  } catch {}
                }
              }

              client.removeAllListeners();

              await client.destroy();
            } catch {}
          }
        } catch (err) {
          console.error(
            '[Shutdown] Error stopping manager:',
            err,
          );
        }
      },
    );

  await Promise.race([
    Promise.allSettled(
      managerPromises,
    ),

    delay(
      SHUTDOWN_TIMEOUT_MS,
    ),
  ]);

  activeManagers = [];

  // --------------------------------------------------------------------------
  // AUTOJOINER
  // --------------------------------------------------------------------------

  if (autoJoiner) {
    console.log(
      '[Shutdown] Shutting down AutoJoiner...',
    );

    try {
      await Promise.race([
        autoJoiner.shutdown(),

        delay(
          SHUTDOWN_TIMEOUT_MS / 2,
        ),
      ]);
    } catch (err) {
      console.error(
        '[Shutdown] AutoJoiner shutdown error:',
        err,
      );
    }

    autoJoiner = null;
  }

  // --------------------------------------------------------------------------
  // BOT
  // --------------------------------------------------------------------------

  if (botManager) {
    console.log(
      '[Shutdown] Shutting down BotManager...',
    );

    try {
      await Promise.race([
        botManager.destroy(),

        delay(
          SHUTDOWN_TIMEOUT_MS / 2,
        ),
      ]);
    } catch (err) {
      console.error(
        '[Shutdown] BotManager shutdown error:',
        err,
      );
    }

    botManager = null;
  }

  // --------------------------------------------------------------------------
  // VRFS
  // --------------------------------------------------------------------------

  try {
    console.log(
      '[Shutdown] Clearing VRFS middleware...',
    );

    vrfs.clearCaches();
    seby.clearFlights();

    logger.info(
      'VRFS middleware caches cleared',
      {
        component:
          'VRFS',
      },
    );
  } catch (err) {
    console.error(
      '[Shutdown] VRFS cleanup error:',
      err,
    );
  }

  // --------------------------------------------------------------------------
  // DATABASE
  // --------------------------------------------------------------------------

  try {
    console.log(
      '[Shutdown] Closing database...',
    );

    await closeDb();
  } catch (err) {
    console.error(
      '[Shutdown] Database close error:',
      err,
    );
  }

  // --------------------------------------------------------------------------
  // HTTP
  // --------------------------------------------------------------------------

  try {
    await new Promise<void>(
      (resolve) => {
        healthServer.close(
          () => resolve(),
        );
      },
    );
  } catch {}

  // --------------------------------------------------------------------------
  // GC
  // --------------------------------------------------------------------------

  if (global.gc) {
    console.log(
      '[Shutdown] Forcing garbage collection...',
    );

    try {
      global.gc();
      await delay(100);

      global.gc();
      await delay(100);

      global.gc();
    } catch {}
  }

  const mem =
    getMemoryUsage();

  console.log(
    `[Shutdown] Final memory: ${mem.heapUsedMB}MB heap / ${mem.rssMB}MB RSS`,
  );

  console.log(
    `[Shutdown] Exiting with code ${exitCode}`,
  );

  setTimeout(
    () => process.exit(exitCode),
    250,
  );
}

function registerShutdown(): void {
  const handle =
    async (
      signal: string,
    ) => {
      if (shuttingDown) {
        console.log(
          '[Shutdown] Already shutting down, forcing exit...',
        );

        process.exit(1);
      }

      await performShutdown(
        `${signal} received`,
        0,
      );
    };

  process.on(
    'SIGINT',
    () => {
      void handle('SIGINT');
    },
  );

  process.on(
    'SIGTERM',
    () => {
      void handle('SIGTERM');
    },
  );
}

// ============================================================================
// BOOT LOOP
// ============================================================================

async function boot(): Promise<void> {
  let attempt = 0;

  while (
    attempt <
    MAX_BOOT_RETRIES
  ) {
    try {
      attempt++;

      if (attempt > 1) {
        logger.info(
          `Boot attempt ${attempt}/${MAX_BOOT_RETRIES}`,
          {
            component:
              'Bootstrap',
          },
        );
      }

      await main();

      registerShutdown();

      return;
    } catch (err) {
      const message =
        err instanceof Error
          ? err.message
          : String(err);

      const code =
        (err as any)?.code;

      logger.error(
        'Startup error',
        {
          component:
            'Bootstrap',

          error:
            message,

          attempt,

          maxRetries:
            MAX_BOOT_RETRIES,
        },
      );

      if (
        code ===
        'AUTH_ALL_FAILED'
      ) {
        logger.error(
          'All tokens invalid – exiting',
          {
            component:
              'Bootstrap',
          },
        );

        process.exit(1);
      }

      if (
        /token|auth|login|invalid|unauthorized|401|403/i.test(
          message,
        )
      ) {
        logger.error(
          'Fatal auth error – exiting',
          {
            component:
              'Bootstrap',
          },
        );

        process.exit(1);
      }

      if (
        attempt >=
        MAX_BOOT_RETRIES
      ) {
        logger.error(
          'Max retries exceeded',
          {
            component:
              'Bootstrap',
          },
        );

        process.exit(1);
      }

      console.log(
        '[Bootstrap] Cleaning up before retry...',
      );

      stopSupervisor();
      stopEventLoopMonitor();
      stopMemoryMonitoring();
      stopVRFSMonitoring();

      for (
        const manager of
        activeManagers
      ) {
        try {
          await manager.shutdown();

          const managerAny =
            manager as any;

          const client =
            managerAny.client;

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
        try {
          global.gc();
        } catch {}
      }

      shuttingDown = false;
      restartRequested = false;

      logger.info(
        `Retrying in ${BOOT_RETRY_DELAY_MS / 1000}s...`,
        {
          component:
            'Bootstrap',
        },
      );

      await delay(
        BOOT_RETRY_DELAY_MS,
      );
    }
  }
}

// ============================================================================
// START
// ============================================================================

console.log(
  '[Bootstrap] Starting giveaway-tracker with supervisor...',
);

const initialMem =
  getMemoryUsage();

console.log(
  '[Bootstrap] Initial memory:',
  initialMem,
);

console.log(
  `[Bootstrap] GC available: ${!!global.gc}`,
);

if (global.gc) {
  try {
    global.gc();

    console.log(
      '[Bootstrap] Initial GC complete',
    );
  } catch {}
}

void boot();
```
