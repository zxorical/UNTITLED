/**
 * @module config
 * Environment configuration loader with validation
 */

import 'dotenv/config';
import { AppConfig } from './types.js';

function requireEnv(key: string): string {
  const val = process.env[key];
  if (!val || val.trim() === '') {
    throw new Error(`Missing required env var: ${key}`);
  }
  return val.trim();
}

function optionalEnv(key: string, fallback: string): string {
  const val = process.env[key];
  return val && val.trim() !== '' ? val.trim() : fallback;
}

function csvEnv(key: string): string[] {
  const val = process.env[key];
  if (!val || val.trim() === '') return [];
  return val.split(',').map(s => s.trim()).filter(Boolean);
}

function assertSnowflake(id: string, label: string): void {
  if (!/^\d{17,19}$/.test(id)) {
    throw new Error(`${label} "${id}" is not a valid Discord Snowflake`);
  }
}

function assertInt(raw: string, label: string, min: number, max: number): number {
  const n = parseInt(raw, 10);
  if (isNaN(n) || n < min || n > max) {
    throw new Error(`${label} must be between ${min} and ${max}, got "${raw}"`);
  }
  return n;
}

function tokensEnv(): string[] {
  const multi = process.env.DISCORD_TOKENS;
  if (multi && multi.trim() !== '') {
    const tokens = multi.split(',').map(s => s.trim()).filter(Boolean);
    if (tokens.length === 0) throw new Error('DISCORD_TOKENS is empty');
    return tokens;
  }
  const single = process.env.DISCORD_TOKEN;
  if (single && single.trim() !== '') {
    return [single.trim()];
  }
  throw new Error('Missing DISCORD_TOKENS or DISCORD_TOKEN');
}

export const CONFIG: AppConfig = {
  // Account tokens for public detectors
  tokens: tokensEnv(),
  
  // Main bot token for notifications
  botToken: requireEnv('DISCORD_BOT_TOKEN'),
  
  // Channel where giveaway notifications are sent
  trackerChannelId: requireEnv('TRACKER_CHANNEL_ID'),
  
  // Optional: only monitor specific channels
  monitoredChannels: csvEnv('MONITORED_CHANNELS'),
  
  // Database path (fallback if MongoDB not used)
  dbPath: optionalEnv('DB_PATH', './data/giveaways.json'),
  
  // Logging
  logLevel: optionalEnv('LOG_LEVEL', 'info'),
  logDir: optionalEnv('LOG_DIR', './logs'),
  
  // Notification cooldown to prevent spam
  notificationCooldown: assertInt(
    optionalEnv('NOTIFICATION_COOLDOWN', '30'),
    'NOTIFICATION_COOLDOWN', 10, 3600
  ),
  
  // Stats logging interval
  statsIntervalMs: assertInt(
    optionalEnv('STATS_INTERVAL_MS', '60000'),
    'STATS_INTERVAL_MS', 10000, 3600000
  ),
  
  // Admin user IDs
  adminUserIds: csvEnv('ADMIN_USER_IDS'),
  
  // ============================================================
  // AutoJoin Settings
  // ============================================================
  
  // Maximum retry attempts for entering a giveaway
  maxRetries: assertInt(
    optionalEnv('MAX_RETRIES', '3'),
    'MAX_RETRIES', 1, 10
  ),
  
  // Base delay between retry attempts (exponential backoff)
  retryDelayMs: assertInt(
    optionalEnv('RETRY_DELAY_MS', '2000'),
    'RETRY_DELAY_MS', 500, 30000
  ),
  
  // Delay before clicking a button (to ensure component loads)
  buttonDelayMs: assertInt(
    optionalEnv('BUTTON_DELAY_MS', '500'),
    'BUTTON_DELAY_MS', 0, 5000
  ),
  
  // Delay before adding a reaction (deprecated - kept for backwards compat)
  reactionDelayMs: assertInt(
    optionalEnv('REACTION_DELAY_MS', '300'),
    'REACTION_DELAY_MS', 0, 5000
  ),
  
  // Global webhook URLs (fallback if user doesn't set their own)
  webhookUrl: optionalEnv('WEBHOOK_URL', ''),
  winWebhookUrl: optionalEnv('WIN_WEBHOOK_URL', ''),
};

// ============================================================
// Validation
// ============================================================

// Validate channel snowflakes
CONFIG.monitoredChannels.forEach((id, i) => assertSnowflake(id, `MONITORED_CHANNELS[${i}]`));
assertSnowflake(CONFIG.trackerChannelId, 'TRACKER_CHANNEL_ID');

// Validate admin user IDs
CONFIG.adminUserIds.forEach((id, i) => assertSnowflake(id, `ADMIN_USER_IDS[${i}]`));

// Ensure at least one token is provided
if (CONFIG.tokens.length === 0) {
  throw new Error('At least one token is required in DISCORD_TOKENS');
}

// ============================================================
// Optional Config Checks (warnings only)
// ============================================================

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  console.warn('[Config] Warning: MONGO_URI not set — database will not connect');
}

const ownerId = process.env.OWNER_ID;
if (!ownerId) {
  console.warn('[Config] Warning: OWNER_ID not set — license panel will not work');
} else if (!/^\d{17,19}$/.test(ownerId)) {
  console.warn('[Config] Warning: OWNER_ID is not a valid Discord Snowflake');
}

const premiumRoleId = process.env.PREMIUM_ROLE_ID;
if (!premiumRoleId) {
  console.warn('[Config] Warning: PREMIUM_ROLE_ID not set — premium checks will not work');
} else if (!/^\d{17,19}$/.test(premiumRoleId)) {
  console.warn('[Config] Warning: PREMIUM_ROLE_ID is not a valid Discord Snowflake');
}

const guildId = process.env.GUILD_ID;
if (!guildId) {
  console.warn('[Config] Warning: GUILD_ID not set — AutoJoiner will not start');
} else if (!/^\d{17,19}$/.test(guildId)) {
  console.warn('[Config] Warning: GUILD_ID is not a valid Discord Snowflake');
}

// ============================================================
// Config Log
// ============================================================

console.log('[Config] Loaded successfully');
console.log(`  - Accounts: ${CONFIG.tokens.length}`);
console.log(`  - Bot Token: ${CONFIG.botToken ? 'Set' : 'Missing'}`);
console.log(`  - Tracker Channel: ${CONFIG.trackerChannelId ? 'Set' : 'Missing'}`);
console.log(`  - Monitored Channels: ${CONFIG.monitoredChannels.length || 'All'}`);
console.log(`  - MongoDB: ${mongoUri ? 'Set' : 'Missing'}`);
console.log(`  - Log Level: ${CONFIG.logLevel}`);
console.log(`  - Notification Cooldown: ${CONFIG.notificationCooldown}s`);
console.log(`  - Admins: ${CONFIG.adminUserIds.length || 'None'}`);
console.log(`  - Owner ID: ${ownerId ? 'Set' : 'Missing'}`);
console.log(`  - Premium Role ID: ${premiumRoleId ? 'Set' : 'Missing'}`);
console.log(`  - Guild ID: ${guildId ? 'Set' : 'Missing'}`);
console.log(`  - AutoJoin Settings:`);
console.log(`    - Max Retries: ${CONFIG.maxRetries}`);
console.log(`    - Retry Delay: ${CONFIG.retryDelayMs}ms`);
console.log(`    - Button Delay: ${CONFIG.buttonDelayMs}ms`);
console.log(`  - Webhooks: ${CONFIG.webhookUrl ? 'Set' : 'Not set'} | ${CONFIG.winWebhookUrl ? 'Win webhook set' : 'Win webhook not set'}`);
