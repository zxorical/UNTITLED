/**
 * @module logger
 * Memory-safe Winston logger
 * FIX: No circular references, no storing large objects, auto-cleanup
 */

import { createLogger, format, transports, Logger } from 'winston';
import chalk from 'chalk';
import fs from 'fs';
import path from 'path';

const LEVEL_COLORS: Record<string, (text: string) => string> = {
  error: chalk.bold.red,
  warn: chalk.bold.yellow,
  info: chalk.bold.cyan,
  debug: chalk.bold.gray,
};

const LEVEL_BADGE: Record<string, string> = {
  error: '✖ ERROR',
  warn: '⚠ WARN ',
  info: '● INFO ',
  debug: '◌ DEBUG',
};

const consoleFormat = format.printf((info) => {
  const { timestamp, level, message, stack, component, ...meta } = info as any;
  const colorFn = LEVEL_COLORS[level] ?? chalk.white;
  const badge = LEVEL_BADGE[level] ?? level.toUpperCase().padEnd(7);
  const ts = chalk.dim(timestamp ?? '');
  const lvl = colorFn(`[${badge}]`);
  const comp = component ? chalk.magenta(`[${component}] `) : '';

  const known = new Set(['timestamp', 'level', 'message', 'stack', 'component', 'splat']);
  
  // ============================================================
  // 🔥 MEMORY FIX: Sanitize meta before displaying
  // ============================================================
  const sanitizedMeta = sanitizeForLog(meta);
  const extras = Object.entries(sanitizedMeta)
    .filter(([k]) => !known.has(k))
    .map(([k, v]) => `${chalk.dim(k)}=${chalk.yellow(typeof v === 'string' ? v : JSON.stringify(v))}`)
    .join(' ');

  const extraStr = extras ? `  ${extras}` : '';
  const stackStr = stack ? `\n${chalk.dim.red(String(stack))}` : '';

  return `${ts} ${lvl} ${comp}${String(message)}${extraStr}${stackStr}`;
});

// ============================================================
// 🔥 CRITICAL: Sanitize ALL objects before logging
// ============================================================
function sanitizeForLog(obj: any, depth = 0): any {
  if (depth > 2) return '[Depth Limit]';
  if (!obj) return obj;
  if (typeof obj !== 'object') return obj;
  
  // Handle circular references
  try {
    JSON.stringify(obj);
  } catch {
    return '[Circular]';
  }
  
  // Don't log large arrays
  if (Array.isArray(obj)) {
    if (obj.length > 10) {
      return `[Array(${obj.length})]`;
    }
    return obj.map(item => sanitizeForLog(item, depth + 1));
  }
  
  // Check if it's a Discord object
  if (obj.constructor?.name?.includes('Client') || 
      obj.constructor?.name?.includes('Message') ||
      obj.constructor?.name?.includes('Channel') ||
      obj.constructor?.name?.includes('Guild') ||
      obj.constructor?.name?.includes('User') ||
      obj.constructor?.name?.includes('Member')) {
    return `[${obj.constructor.name}]`;
  }
  
  // Check for Error objects
  if (obj instanceof Error) {
    return {
      message: obj.message,
      name: obj.name,
      // ❌ DON'T store full stack trace unless needed
      // stack: obj.stack?.split('\n').slice(0, 3).join('\n')
    };
  }
  
  // Sanitize each property
  const result: any = {};
  for (const [key, value] of Object.entries(obj)) {
    // Skip large/binary fields
    if (key === 'buffer' || key === 'data' || key === 'raw') {
      result[key] = '[Binary]';
      continue;
    }
    
    // Skip internal Discord fields
    if (key.startsWith('_') || key === 'client' || key === 'session') {
      continue;
    }
    
    result[key] = sanitizeForLog(value, depth + 1);
  }
  
  return result;
}

function buildLogger(logLevel: string, logDir: string): Logger {
  const resolved = path.resolve(logDir);
  if (!fs.existsSync(resolved)) fs.mkdirSync(resolved, { recursive: true });

  // ============================================================
  // 🔥 MEMORY FIX: Reduce file sizes and increase rotation
  // ============================================================
  return createLogger({
    level: logLevel,
    format: format.combine(
      format.errors({ stack: false }), // ❌ DON'T store full stacks
      format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
      format.splat()
    ),
    transports: [
      new transports.Console({
        format: format.combine(format.colorize({ level: false }), consoleFormat),
      }),
      new transports.File({
        filename: path.join(resolved, 'combined.log'),
        format: format.combine(format.json()),
        maxsize: 2 * 1024 * 1024,  // 🔥 2MB (down from 10MB)
        maxFiles: 3,               // 🔥 3 files (down from 5)
        tailable: true,
        // 🔥 CRITICAL: Don't keep logs in memory
        options: { flags: 'a' },
      }),
      new transports.File({
        filename: path.join(resolved, 'error.log'),
        level: 'error',
        format: format.combine(format.json()),
        maxsize: 1 * 1024 * 1024,  // 🔥 1MB (down from 5MB)
        maxFiles: 3,               // 🔥 3 files (down from 5)
        tailable: true,
        options: { flags: 'a' },
      }),
    ],
    exitOnError: false,
  });
}

let winstonLogger = buildLogger('info', './logs');

export function reconfigureLogger(level: string, dir: string): void {
  // ============================================================
  // 🔥 MEMORY FIX: Close old logger before creating new one
  // ============================================================
  try {
    winstonLogger.close();
  } catch {}
  winstonLogger = buildLogger(level, dir);
}

// ============================================================
// 🔥 MEMORY FIX: Auto-cleanup old logs every hour
// ============================================================
setInterval(() => {
  try {
    const logDir = path.resolve('./logs');
    if (!fs.existsSync(logDir)) return;
    
    const files = fs.readdirSync(logDir);
    const now = Date.now();
    const ONE_DAY = 24 * 60 * 60 * 1000;
    
    for (const file of files) {
      if (!file.endsWith('.log')) continue;
      const filePath = path.join(logDir, file);
      const stats = fs.statSync(filePath);
      
      // Delete logs older than 7 days
      if (now - stats.mtimeMs > 7 * ONE_DAY) {
        fs.unlinkSync(filePath);
      }
    }
  } catch {}
}, 3600000); // Every hour

// ============================================================
// 🔥 MEMORY FIX: Limit log queue size
// ============================================================
let logQueue: Array<{ level: string; msg: string; meta?: any }> = [];
const MAX_LOG_QUEUE = 100;

function flushLogQueue(): void {
  if (logQueue.length === 0) return;
  const batch = logQueue.splice(0, logQueue.length);
  
  for (const entry of batch) {
    try {
      switch (entry.level) {
        case 'info': winstonLogger.info(entry.msg, entry.meta ?? {}); break;
        case 'warn': winstonLogger.warn(entry.msg, entry.meta ?? {}); break;
        case 'error': winstonLogger.error(entry.msg, entry.meta ?? {}); break;
        case 'debug': winstonLogger.debug(entry.msg, entry.meta ?? {}); break;
      }
    } catch {}
  }
}

// Flush queue every 5 seconds
setInterval(flushLogQueue, 5000);

export const logger = {
  info(msg: string, meta?: Record<string, unknown>) {
    // ============================================================
    // 🔥 CRITICAL: Sanitize BEFORE logging
    // ============================================================
    const safe = meta ? sanitizeForLog(meta) : {};
    logQueue.push({ level: 'info', msg, meta: safe });
    if (logQueue.length > MAX_LOG_QUEUE) {
      flushLogQueue();
    }
  },
  
  warn(msg: string, meta?: Record<string, unknown>) {
    const safe = meta ? sanitizeForLog(meta) : {};
    logQueue.push({ level: 'warn', msg, meta: safe });
    if (logQueue.length > MAX_LOG_QUEUE) {
      flushLogQueue();
    }
  },
  
  error(msg: string, meta?: Record<string, unknown>) {
    // ============================================================
    // 🔥 CRITICAL: Extract ONLY what we need from errors
    // ============================================================
    const safe: any = {};
    if (meta) {
      for (const [key, value] of Object.entries(meta)) {
        if (value instanceof Error) {
          // ✅ GOOD: Store minimal error info
          safe[key] = {
            message: value.message,
            name: value.name,
            // 🔥 Only store first 2 lines of stack
            stack: value.stack?.split('\n').slice(0, 2).join('\n') || 'No stack',
          };
        } else if (typeof value === 'object' && value !== null) {
          safe[key] = sanitizeForLog(value);
        } else {
          safe[key] = value;
        }
      }
    }
    
    logQueue.push({ level: 'error', msg, meta: safe });
    if (logQueue.length > MAX_LOG_QUEUE) {
      flushLogQueue();
    }
  },
  
  debug(msg: string, meta?: Record<string, unknown>) {
    const safe = meta ? sanitizeForLog(meta) : {};
    logQueue.push({ level: 'debug', msg, meta: safe });
    if (logQueue.length > MAX_LOG_QUEUE) {
      flushLogQueue();
    }
  },
  
  // ============================================================
  // 🔥 Add method to force flush and cleanup
  // ============================================================
  flush(): void {
    flushLogQueue();
  },
  
  close(): void {
    try {
      flushLogQueue();
      winstonLogger.close();
    } catch {}
  }
};

export type AppLogger = typeof logger;
