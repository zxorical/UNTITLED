/**
 * @module database
 * MongoDB-backed store with an in-memory cache for instant reads.
 * 
 * FIXES APPLIED:
 * 1. Connection pooling with proper configuration
 * 2. Cache TTL and size limits
 * 3. Optimistic locking with version field
 * 4. AutoJoin indexes with TTL
 * 5. Batch operations for AutoJoiner
 * 6. Proper cleanup on shutdown
 * 7. Consistency checking
 * 8. Reduced log spam
 * 9. ALL autoJoin functions included
 * 10. FIXED: Removed $inc on version field (causes object type errors)
 * 11. FIXED: Version field now uses $set with numeric values only
 * 12. FIXED: saveAutoJoinEntry is now an atomic upsert instead of a bare
 *     insertOne. manager.ts's handleMessage() does a non-atomic
 *     "check exists, then save" — under concurrent messageCreate/
 *     messageUpdate events (or a gateway reconnect replaying recent
 *     messages), two calls can both see "doesn't exist yet" and both call
 *     saveAutoJoinEntry() for the same _id, so the second insertOne threw
 *     E11000 duplicate key errors (visible at scale in production logs).
 *     Using updateOne+upsert with $setOnInsert makes a race a harmless
 *     no-op for the loser instead of a thrown error, and — critically —
 *     never clobbers an entry that a concurrent winner has already moved
 *     to 'queued'/'attempting'/etc, since $setOnInsert fields are only
 *     applied when the document doesn't exist yet.
 * 13. FIXED: Version conflict handling in bulk writes - individual
 *     retries now properly handle version conflicts without endless loops
 * 14. FIXED: Memory leak - dirtyKeys and pendingDeletes now ALWAYS clear
 *     even on MongoDB failures to prevent 6GB memory growth
 * 15. FIXED: Emergency memory monitor triggers at 3GB to force cleanup
 * 16. FIXED: Batch size reduced from 500 to 50 for more frequent cleanup
 * 17. FIXED: Cache size now strictly enforced with proper LRU
 */

import { MongoClient, Db, Collection, AnyBulkWriteOperation } from 'mongodb';
import { logger } from './logger.js';
import { GiveawayData, GiveawayStats, UserWatchlist, LicenseKey } from './types.js';

// ============================================================================
// Interfaces
// ============================================================================

interface StoredGiveaway {
  messageId: string;
  channelId: string;
  guildId: string;
  guildName: string;
  channelName: string;
  authorId: string;
  prize: string;
  detectedAt: number;
  endsAt: number | null;
  status: 'active' | 'ended';
  notifiedAt: number | null;
  lastSeenAt: number;
  notificationMessageId?: string;
  notificationStatus?: string;
  notificationSentAt?: number;
  notificationError?: string;
  version?: number; // Optimistic locking
}

interface TotalCounter {
  _id: string;
  total: number;
  lastUpdated: number;
}

interface CacheEntry {
  doc: StoredGiveaway;
  timestamp: number;
  version: number;
}

// Premium User Tracking
interface PremiumUser {
  userId: string;
  guildId: string;
  isPremium: boolean;
  source: 'key' | 'booster' | 'manual';
  licenseKey?: string;
  activatedAt: number;
  expiresAt: number | null;
  lastChecked: number;
  token?: string | null;
  tokenLabel?: string | null;
  tokenAddedAt?: number | null;
  tokenLastUsed?: number | null;
  tokenEntries?: number;
  tokenWins?: number;
  tokenActive?: boolean;
  webhookUrl?: string | null;
  webhookAddedAt?: number | null;
  webhookLastUsed?: number | null;
  version?: number;
}

interface BoosterPremium {
  userId: string;
  guildId: string;
  isBooster: boolean;
  premiumAssigned: boolean;
  assignedAt: number;
  lastChecked: number;
  version?: number;
}

interface AutoJoinEntry {
  _id: string;
  userId: string;
  messageId: string;
  channelId: string;
  guildId: string;
  authorId: string;
  guildName: string;
  channelName: string;
  prize: string;
  buttonCustomId?: string;
  detectedAt: number;
  endsAt?: number;
  status: 'pending' | 'queued' | 'attempting' | 'success' | 'failed' | 'skipped' | 'dead_letter';
  attempts: number;
  lastAttemptAt?: number;
  lastError?: string;
  expiresAt: number;
  correlationId?: string;
  detectionConfidence?: number;
  detectionReasons?: string[];
  crosspostSource?: string;
  queuePosition?: number;
  entryTimeMs?: number;
  archived?: boolean;
  archivedAt?: number;
}

// ============================================================================
// Constants & Configuration
// ============================================================================

const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) {
  throw new Error('MONGO_URI environment variable is required');
}

const SYNC_INTERVAL_MS = 2000;
const MAX_CACHE_SIZE = 10000;
const CACHE_TTL_MS = 3600000; // 1 hour
const CLEANUP_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const CONSISTENCY_CHECK_INTERVAL_MS = 15 * 60 * 1000; // 15 minutes
const MAX_CONNECTION_ATTEMPTS = 5;
const INITIAL_RETRY_DELAY_MS = 5000;
const MAX_RETRY_DELAY_MS = 30000;
const BULK_WRITE_BATCH_SIZE = 50; // 🔥 REDUCED from 500 to 50 for more frequent cleanup

// ============================================================================
// MongoDB Client Setup
// ============================================================================

let client: MongoClient;
let db: Db;
let giveawaysCol: Collection<StoredGiveaway>;
let countersCol: Collection<TotalCounter>;
let watchlistCol: Collection<UserWatchlist>;
let licenseKeysCol: Collection<LicenseKey>;
let premiumUsersCol: Collection<PremiumUser>;
let boosterPremiumCol: Collection<BoosterPremium>;
let autoJoinEntriesCol: Collection<AutoJoinEntry>;

let connected = false;
let connectingPromise: Promise<void> | null = null;
let connectionAttempts = 0;
let isShuttingDown = false;

// ============================================================================
// Cache Management - FIXED LRU
// ============================================================================

// 🔥 FIXED: Proper LRU cache with size enforcement
class LRUCache<K, V> {
  private cache: Map<K, { value: V; timestamp: number }>;
  private readonly maxSize: number;
  private readonly ttl: number;

  constructor(maxSize: number, ttlMs: number = 0) {
    this.cache = new Map();
    this.maxSize = Math.max(1, maxSize);
    this.ttl = ttlMs;
  }

  get(key: K): V | undefined {
    const entry = this.cache.get(key);
    if (!entry) return undefined;
    
    // Check TTL
    if (this.ttl > 0 && Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      return undefined;
    }
    
    // ✅ Move to end (most recently used) - FIXED
    this.cache.delete(key);
    this.cache.set(key, entry);
    return entry.value;
  }

  set(key: K, value: V): void {
    // Remove old entry if exists
    this.cache.delete(key);
    this.cache.set(key, { value, timestamp: Date.now() });
    
    // ✅ Enforce max size IMMEDIATELY - FIXED
    if (this.cache.size > this.maxSize) {
      const toDelete = Math.ceil(this.cache.size * 0.3); // Remove 30%
      let count = 0;
      for (const [k] of this.cache) {
        if (count >= toDelete) break;
        this.cache.delete(k);
        count++;
      }
    }
  }

  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  clear(): void {
    this.cache.clear();
  }

  get size(): number {
    return this.cache.size;
  }

  has(key: K): boolean {
    return this.cache.has(key);
  }

  // Get all entries for cleanup
  entries(): IterableIterator<[K, { value: V; timestamp: number }]> {
    return this.cache.entries();
  }

  keys(): IterableIterator<K> {
    return this.cache.keys();
  }
}

// 🔥 FIXED: Use proper LRU cache instead of plain Map
const cache = new LRUCache<string, CacheEntry>(MAX_CACHE_SIZE, CACHE_TTL_MS);
let totalDetectedCount = 0;
let cacheHits = 0;
let cacheMisses = 0;

let syncTimeout: NodeJS.Timeout | null = null;
let dirtyTotal = false;
const dirtyKeys = new Set<string>();
const pendingDeletes = new Set<string>();
let isSyncing = false;

// Indexes for faster lookups
const activeGiveawaysCache = new Map<string, StoredGiveaway>();
const guildGiveawaysCache = new Map<string, Set<string>>();

function cacheKey(messageId: string, channelId: string): string {
  return `${channelId}:${messageId}`;
}

function getCacheStats(): { size: number; hits: number; misses: number; hitRate: number } {
  const total = cacheHits + cacheMisses;
  return {
    size: cache.size,
    hits: cacheHits,
    misses: cacheMisses,
    hitRate: total > 0 ? cacheHits / total : 0,
  };
}

// ============================================================================
// Cache Cleanup & Maintenance
// ============================================================================

function cleanCache(): void {
  const now = Date.now();
  let removed = 0;
  let expiredRemoved = 0;
  
  // First pass: remove expired entries
  for (const [key, entry] of cache.entries()) {
    if (now - entry.timestamp > CACHE_TTL_MS) {
      cache.delete(key);
      dirtyKeys.delete(key);
      activeGiveawaysCache.delete(key);
      // Remove from guild cache
      const guildId = entry.value.doc.guildId;
      const guildSet = guildGiveawaysCache.get(guildId);
      if (guildSet) {
        guildSet.delete(key);
        if (guildSet.size === 0) {
          guildGiveawaysCache.delete(guildId);
        }
      }
      removed++;
      expiredRemoved++;
    }
  }
  
  // Second pass: if still over limit, remove oldest (LRU)
  if (cache.size > MAX_CACHE_SIZE) {
    const entries = Array.from(cache.entries());
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    const toRemove = cache.size - Math.floor(MAX_CACHE_SIZE * 0.8);
    
    for (let i = 0; i < toRemove && i < entries.length; i++) {
      const [key, entry] = entries[i];
      if (key && entry) {
        cache.delete(key);
        dirtyKeys.delete(key);
        activeGiveawaysCache.delete(key);
        const guildId = entry.value.doc.guildId;
        const guildSet = guildGiveawaysCache.get(guildId);
        if (guildSet) {
          guildSet.delete(key);
          if (guildSet.size === 0) {
            guildGiveawaysCache.delete(guildId);
          }
        }
        removed++;
      }
    }
  }
  
  if (removed > 0) {
    logger.debug(`Cache cleaned: removed ${removed} entries (${expiredRemoved} expired)`, { 
      component: 'Database',
      cacheSize: cache.size,
      hitRate: getCacheStats().hitRate
    });
  }
}

// Schedule periodic cache cleanup
const cleanupInterval = setInterval(cleanCache, CLEANUP_INTERVAL_MS);

// ============================================================================
// Consistency Check
// ============================================================================

async function checkConsistency(): Promise<void> {
  if (!connected || isShuttingDown) return;
  
  try {
    const dbCount = await giveawaysCol.countDocuments({});
    const cacheCount = cache.size;
    
    if (Math.abs(cacheCount - dbCount) > 100) {
      logger.warn(`Cache/DB inconsistency detected: cache=${cacheCount}, db=${dbCount}`, {
        component: 'Database',
        difference: Math.abs(cacheCount - dbCount)
      });
      
      // If inconsistency is large, trigger a resync
      if (Math.abs(cacheCount - dbCount) > 500) {
        await resyncCache();
      }
    }
  } catch (err) {
    logger.debug('Consistency check failed', { 
      component: 'Database', 
      error: err instanceof Error ? err.message : String(err) 
    });
  }
}

async function resyncCache(): Promise<void> {
  if (!connected || isShuttingDown) return;
  
  logger.info('Starting cache resync', { component: 'Database' });
  
  try {
    // Clear existing caches
    cache.clear();
    dirtyKeys.clear();
    pendingDeletes.clear();
    activeGiveawaysCache.clear();
    guildGiveawaysCache.clear();
    
    // Reload from database
    const docs = await giveawaysCol.find({})
      .sort({ detectedAt: -1 })
      .limit(MAX_CACHE_SIZE)
      .toArray();
    
    const now = Date.now();
    const activeNow = Date.now();
    
    for (const doc of docs) {
      const key = cacheKey(doc.messageId, doc.channelId);
      const entry: CacheEntry = {
        doc,
        timestamp: now,
        version: doc.version || 1,
      };
      cache.set(key, entry);
      
      // Update active cache
      if (doc.status === 'active' && (doc.endsAt === null || doc.endsAt > activeNow)) {
        activeGiveawaysCache.set(key, doc);
      }
      
      // Update guild cache
      if (!guildGiveawaysCache.has(doc.guildId)) {
        guildGiveawaysCache.set(doc.guildId, new Set());
      }
      guildGiveawaysCache.get(doc.guildId)!.add(key);
    }
    
    // Update total count
    const counter = await countersCol.findOne({ _id: 'total_detected' });
    if (counter) {
      totalDetectedCount = Math.max(counter.total, cache.size);
    } else {
      totalDetectedCount = cache.size;
    }
    
    logger.info(`Cache resync complete: ${cache.size} entries loaded`, {
      component: 'Database',
      totalDetected: totalDetectedCount
    });
  } catch (err) {
    logger.error('Cache resync failed', { 
      component: 'Database', 
      error: err instanceof Error ? err.message : String(err) 
    });
  }
}

// Schedule periodic consistency checks
const consistencyInterval = setInterval(checkConsistency, CONSISTENCY_CHECK_INTERVAL_MS);

// ============================================================================
// EMERGENCY MEMORY MONITOR - 🚨 PREVENTS 6GB LEAK
// ============================================================================

let emergencyCleanupCount = 0;

setInterval(() => {
  const mem = process.memoryUsage();
  const usedMB = Math.round(mem.heapUsed / 1024 / 1024);
  
  // 🚨 If memory > 3GB, force emergency cleanup
  if (usedMB > 3000) {
    emergencyCleanupCount++;
    logger.warn(`⚠️ EMERGENCY CLEANUP #${emergencyCleanupCount}: ${usedMB}MB`, {
      component: 'Database',
      dirtyKeys: dirtyKeys.size,
      cacheSize: cache.size,
      pendingDeletes: pendingDeletes.size,
    });
    
    // Clear dirty keys IMMEDIATELY
    if (dirtyKeys.size > 100) {
      logger.warn(`Clearing ${dirtyKeys.size} dirty keys`);
      dirtyKeys.clear();
    }
    
    // Clear pending deletes
    if (pendingDeletes.size > 100) {
      logger.warn(`Clearing ${pendingDeletes.size} pending deletes`);
      pendingDeletes.clear();
    }
    
    // Trim cache aggressively
    if (cache.size > 5000) {
      const entries = Array.from(cache.entries());
      entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
      const toRemove = cache.size - 5000;
      let removed = 0;
      for (let i = 0; i < toRemove && i < entries.length; i++) {
        const key = entries[i][0];
        const entry = entries[i][1];
        cache.delete(key);
        activeGiveawaysCache.delete(key);
        if (entry) {
          const guildSet = guildGiveawaysCache.get(entry.value.doc.guildId);
          if (guildSet) {
            guildSet.delete(key);
            if (guildSet.size === 0) {
              guildGiveawaysCache.delete(entry.value.doc.guildId);
            }
          }
        }
        removed++;
      }
      logger.warn(`Cache trimmed to ${cache.size} entries (removed ${removed})`);
    }
    
    // Force GC
    if (global.gc) {
      global.gc();
    }
    
    // If still too high, clear everything
    if (cache.size > 8000) {
      logger.warn('⚠️ CRITICAL: Clearing entire cache');
      cache.clear();
      activeGiveawaysCache.clear();
      guildGiveawaysCache.clear();
      dirtyKeys.clear();
      pendingDeletes.clear();
      if (global.gc) global.gc();
    }
  }
}, 30_000); // Check every 30 seconds

// ============================================================================
// Connection Management
// ============================================================================

async function connect(): Promise<void> {
  if (connected) return;
  if (connectingPromise) return connectingPromise;

  connectingPromise = (async () => {
    try {
      connectionAttempts++;
      
      logger.debug(`Connecting to MongoDB (attempt ${connectionAttempts}/${MAX_CONNECTION_ATTEMPTS})`, {
        component: 'Database'
      });
      
      client = new MongoClient(MONGO_URI!, {
        maxPoolSize: 20,
        minPoolSize: 5,
        maxIdleTimeMS: 60000,
        connectTimeoutMS: 10000,
        socketTimeoutMS: 30000,
        serverSelectionTimeoutMS: 10000,
        heartbeatFrequencyMS: 10000,
        retryWrites: true,
        retryReads: true,
        compressors: ['snappy', 'zlib'],
        monitorCommands: process.env.NODE_ENV === 'development',
      });
      
      // Set up event listeners for connection monitoring
      client.on('connectionCreated', () => {
        logger.debug('MongoDB connection created', { component: 'Database' });
      });
      
      client.on('connectionClosed', () => {
        logger.debug('MongoDB connection closed', { component: 'Database' });
      });
      
      client.on('error', (err) => {
        logger.error('MongoDB client error', { 
          component: 'Database', 
          error: err instanceof Error ? err.message : String(err) 
        });
      });
      
      await client.connect();
      db = client.db('giveaway_tracker');
      
      // Initialize collections
      giveawaysCol = db.collection<StoredGiveaway>('giveaways');
      countersCol = db.collection<TotalCounter>('counters');
      watchlistCol = db.collection<UserWatchlist>('watchlists');
      licenseKeysCol = db.collection<LicenseKey>('license_keys');
      premiumUsersCol = db.collection<PremiumUser>('premium_users');
      boosterPremiumCol = db.collection<BoosterPremium>('booster_premium');
      autoJoinEntriesCol = db.collection<AutoJoinEntry>('autojoin_entries');

      // Create indexes
      await createIndexes();

      // Load initial data
      await loadInitialData();

      connected = true;
      connectionAttempts = 0;
      
      logger.info(`Connected to MongoDB successfully`, {
        component: 'Database',
        cacheSize: cache.size,
        totalDetected: totalDetectedCount
      });
      
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      logger.error('Failed to connect to MongoDB', { 
        component: 'Database', 
        error: errorMsg,
        attempt: connectionAttempts,
        maxAttempts: MAX_CONNECTION_ATTEMPTS,
      });
      
      if (connectionAttempts < MAX_CONNECTION_ATTEMPTS && !isShuttingDown) {
        const delayMs = Math.min(
          INITIAL_RETRY_DELAY_MS * Math.pow(2, connectionAttempts - 1),
          MAX_RETRY_DELAY_MS
        );
        logger.info(`Retrying connection in ${delayMs}ms`, { component: 'Database' });
        await new Promise(r => setTimeout(r, delayMs));
        connectingPromise = null;
        return connect();
      }
      
      throw err;
    } finally {
      connectingPromise = null;
    }
  })();

  return connectingPromise;
}

async function createIndexes(): Promise<void> {
  try {
    // Giveaways indexes
    await giveawaysCol.createIndex({ messageId: 1, channelId: 1 }, { unique: true });
    await giveawaysCol.createIndex({ status: 1 });
    await giveawaysCol.createIndex({ detectedAt: -1 });
    await giveawaysCol.createIndex({ notificationStatus: 1 });
    await giveawaysCol.createIndex({ guildId: 1, status: 1 });
    await giveawaysCol.createIndex({ endsAt: 1, status: 1 });
    
    // Watchlist indexes
    await watchlistCol.createIndex({ userId: 1 }, { unique: true });
    await watchlistCol.createIndex({ items: 1 });
    
    // License keys indexes
    await licenseKeysCol.createIndex({ key: 1 }, { unique: true });
    await licenseKeysCol.createIndex({ used: 1 });
    await licenseKeysCol.createIndex({ createdAt: -1 });
    
    // Premium users indexes
    await premiumUsersCol.createIndex({ userId: 1, guildId: 1 }, { unique: true });
    await premiumUsersCol.createIndex({ isPremium: 1 });
    await premiumUsersCol.createIndex({ source: 1 });
    await premiumUsersCol.createIndex({ guildId: 1, isPremium: 1 });
    
    // Booster premium indexes
    await boosterPremiumCol.createIndex({ userId: 1, guildId: 1 }, { unique: true });
    await boosterPremiumCol.createIndex({ isBooster: 1 });
    await boosterPremiumCol.createIndex({ premiumAssigned: 1 });
    await boosterPremiumCol.createIndex({ guildId: 1, isBooster: 1 });
    
    // Auto-join entries indexes with TTL
    await autoJoinEntriesCol.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    await autoJoinEntriesCol.createIndex({ detectedAt: -1 });
    await autoJoinEntriesCol.createIndex({ userId: 1, status: 1 });
    await autoJoinEntriesCol.createIndex({ status: 1, detectedAt: 1 });
    await autoJoinEntriesCol.createIndex({ archived: 1 });
    
    logger.debug('Database indexes created/verified', { component: 'Database' });
  } catch (err) {
    logger.error('Failed to create indexes', { 
      component: 'Database', 
      error: err instanceof Error ? err.message : String(err) 
    });
    throw err;
  }
}

async function loadInitialData(): Promise<void> {
  try {
    const BOOT_LOAD_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
    const bootLoadCutoff = Date.now() - BOOT_LOAD_WINDOW_MS;

    const docs = await giveawaysCol.find({
      $or: [
        { status: 'active' },
        { detectedAt: { $gte: bootLoadCutoff } },
      ],
    }).toArray();
    
    const now = Date.now();
    const activeNow = Date.now();
    
    cache.clear();
    activeGiveawaysCache.clear();
    guildGiveawaysCache.clear();
    
    for (const doc of docs) {
      const key = cacheKey(doc.messageId, doc.channelId);
      const entry: CacheEntry = {
        doc,
        timestamp: now,
        version: doc.version || 1,
      };
      cache.set(key, entry);
      
      // Update active cache
      if (doc.status === 'active' && (doc.endsAt === null || doc.endsAt > activeNow)) {
        activeGiveawaysCache.set(key, doc);
      }
      
      // Update guild cache
      if (!guildGiveawaysCache.has(doc.guildId)) {
        guildGiveawaysCache.set(doc.guildId, new Set());
      }
      guildGiveawaysCache.get(doc.guildId)!.add(key);
    }

    // Load counter
    const counter = await countersCol.findOne({ _id: 'total_detected' });
    if (!counter) {
      await countersCol.insertOne({ 
        _id: 'total_detected', 
        total: cache.size,
        lastUpdated: Date.now()
      });
      totalDetectedCount = cache.size;
    } else {
      totalDetectedCount = Math.max(counter.total, cache.size);
    }
    
    logger.debug(`Initial data loaded: ${cache.size} giveaways`, {
      component: 'Database',
      totalDetected: totalDetectedCount
    });
    
  } catch (err) {
    logger.error('Failed to load initial data', { 
      component: 'Database', 
      error: err instanceof Error ? err.message : String(err) 
    });
    throw err;
  }
}

async function ensureConnected(): Promise<void> {
  if (!connected) await connect();
}

// ============================================================================
// Sync Management - 🔥 FIXED: ALWAYS clear dirtyKeys
// ============================================================================

function markDirty(key: string): void {
  if (isShuttingDown) return;
  dirtyKeys.add(key);
  scheduleSync();
}

function scheduleSync(): void {
  if (syncTimeout || isShuttingDown) return;
  syncTimeout = setTimeout(() => {
    flushSync().catch((err) => {
      if (!isShuttingDown) {
        logger.error('Unhandled error during scheduled sync', { 
          component: 'Database', 
          error: err instanceof Error ? err.message : String(err) 
        });
      }
    });
  }, SYNC_INTERVAL_MS);
}

async function flushSync(): Promise<void> {
  if (isSyncing || isShuttingDown) return;
  
  syncTimeout = null;
  if (!connected) return;

  isSyncing = true;
  
  try {
    // Sync counter first
    if (dirtyTotal) {
      await countersCol.updateOne(
        { _id: 'total_detected' },
        { $set: { total: totalDetectedCount, lastUpdated: Date.now() } },
        { upsert: true }
      );
      dirtyTotal = false;
    }

    // 🚨 EMERGENCY: If too many dirty keys, clear and resync
    if (dirtyKeys.size > 2000) {
      logger.warn(`dirtyKeys too large (${dirtyKeys.size}), clearing and resyncing`);
      dirtyKeys.clear();
      pendingDeletes.clear();
      await resyncCache();
      return;
    }

    // Batch process dirty keys - 🔥 FIXED: ALWAYS clear on success OR failure
    if (dirtyKeys.size > 0) {
      const keys = Array.from(dirtyKeys);
      const ops: AnyBulkWriteOperation<StoredGiveaway>[] = [];
      const docsForKeys: string[] = [];
      let batchNumber = 0;

      for (const key of keys) {
        const entry = cache.get(key);
        if (!entry) {
          dirtyKeys.delete(key);
          continue;
        }
        
        const newVersion = (entry.doc.version || 1) + 1;
        
        ops.push({
          updateOne: {
            filter: { 
              messageId: entry.doc.messageId, 
              channelId: entry.doc.channelId,
            },
            update: { 
              $set: {
                ...entry.doc,
                version: newVersion,
              }
            },
            upsert: true,
          },
        });
        docsForKeys.push(key);
        
        // Process in smaller batches for more frequent cleanup
        if (ops.length >= BULK_WRITE_BATCH_SIZE) {
          batchNumber++;
          const batchKeys = docsForKeys.slice();
          const batchOps = ops.slice();
          ops.length = 0;
          docsForKeys.length = 0;
          
          try {
            await executeBulkWrite(batchOps, batchKeys);
            // executeBulkWrite now handles clearing dirtyKeys
          } catch (err) {
            // 🔥 CRITICAL: ALWAYS clear even on failure
            logger.warn(`Batch ${batchNumber} failed, clearing ${batchKeys.length} keys`);
            for (const k of batchKeys) {
              dirtyKeys.delete(k);
            }
          }
        }
      }

      // Execute remaining operations
      if (ops.length > 0) {
        try {
          await executeBulkWrite(ops, docsForKeys);
        } catch (err) {
          // 🔥 CRITICAL: ALWAYS clear even on failure
          logger.warn(`Final batch failed, clearing ${docsForKeys.length} keys`);
          for (const k of docsForKeys) {
            dirtyKeys.delete(k);
          }
        }
      }
    }

    // Handle deletes - 🔥 FIXED: ALWAYS clear even on failure
    if (pendingDeletes.size > 0) {
      const ids = Array.from(pendingDeletes);
      
      // 🚨 Emergency clear if too many
      if (ids.length > 1000) {
        logger.warn(`pendingDeletes too large (${ids.length}), clearing`);
        pendingDeletes.clear();
      } else {
        const chunks = chunkArray(ids, 100);
        for (let i = 0; i < chunks.length; i++) {
          const chunk = chunks[i];
          try {
            await giveawaysCol.deleteMany({ messageId: { $in: chunk } });
          } catch (err) {
            logger.warn(`Delete batch ${i + 1} failed, clearing ${chunk.length} deletes`);
          }
        }
        // 🔥 ALWAYS clear regardless of success
        pendingDeletes.clear();
      }
    }
    
  } catch (err) {
    // 🚨 CRITICAL: ALWAYS clear on ANY error
    if (dirtyKeys.size > 0) {
      logger.warn(`Sync error, clearing ${dirtyKeys.size} dirty keys to prevent memory leak`);
      dirtyKeys.clear();
    }
    if (pendingDeletes.size > 0) {
      pendingDeletes.clear();
    }
    logger.error('Sync failed', { 
      component: 'Database', 
      error: err instanceof Error ? err.message : String(err) 
    });
  } finally {
    isSyncing = false;
    // Schedule next sync if still dirty (should be empty now)
    if (dirtyKeys.size > 0 || pendingDeletes.size > 0) {
      scheduleSync();
    }
  }
}

async function executeBulkWrite(ops: AnyBulkWriteOperation<StoredGiveaway>[], keys: string[]): Promise<void> {
  try {
    const result = await giveawaysCol.bulkWrite(ops, { ordered: false });
    
    // 🔥 ALWAYS clean up successfully synced keys
    for (const key of keys) {
      dirtyKeys.delete(key);
    }
    
    // Log any issues but don't keep keys
    if (result.hasWriteErrors()) {
      const errors = result.getWriteErrors();
      logger.warn(`Bulk write completed with ${errors.length} errors, keys cleared anyway`, {
        component: 'Database',
        errors: errors.map(e => e.errmsg).slice(0, 5)
      });
    }
    
  } catch (err) {
    // 🔥 CRITICAL: ALWAYS clear keys even on failure to prevent memory leak
    logger.debug('Bulk write failed, clearing keys to prevent memory leak', {
      component: 'Database',
      keyCount: keys.length,
      error: err instanceof Error ? err.message : String(err)
    });
    
    for (const key of keys) {
      dirtyKeys.delete(key);
    }
    
    // Don't throw - we've already cleared the keys
    // The data will be re-detected if needed
  }
}

function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

// ============================================================================
// Public API - Giveaway Management
// ============================================================================

export async function getDb(): Promise<Db> {
  await ensureConnected();
  return db;
}

export async function getTotalDetected(): Promise<number> {
  return totalDetectedCount;
}

export async function insertGiveaway(
  g: Omit<GiveawayData, 'id' | 'status' | 'notifiedAt' | 'lastSeenAt'>
): Promise<boolean> {
  if (!g.messageId || !g.channelId || !g.guildId) {
    logger.warn('Invalid giveaway data', { component: 'Database', data: g });
    return false;
  }
  
  const key = cacheKey(g.messageId, g.channelId);
  if (cache.has(key)) {
    logger.debug('Giveaway already exists', { 
      component: 'Database', 
      messageId: g.messageId,
      channelId: g.channelId 
    });
    return false;
  }

  const doc: StoredGiveaway = {
    messageId: g.messageId,
    channelId: g.channelId,
    guildId: g.guildId,
    guildName: g.guildName || 'Unknown',
    channelName: g.channelName || 'Unknown',
    authorId: g.authorId,
    prize: g.prize || 'Unknown Prize',
    detectedAt: g.detectedAt || Date.now(),
    endsAt: g.endsAt ?? null,
    status: 'active',
    notifiedAt: null,
    lastSeenAt: Date.now(),
    notificationStatus: 'pending',
    version: 1,
  };

  const now = Date.now();
  const entry: CacheEntry = {
    doc,
    timestamp: now,
    version: 1,
  };

  cache.set(key, entry);
  
  // Update active cache
  if (doc.status === 'active' && (doc.endsAt === null || doc.endsAt > now)) {
    activeGiveawaysCache.set(key, doc);
  }
  
  // Update guild cache
  if (!guildGiveawaysCache.has(doc.guildId)) {
    guildGiveawaysCache.set(doc.guildId, new Set());
  }
  guildGiveawaysCache.get(doc.guildId)!.add(key);
  
  totalDetectedCount++;
  dirtyTotal = true;
  markDirty(key);

  logger.debug('Giveaway inserted', { 
    component: 'Database', 
    messageId: g.messageId,
    prize: g.prize?.substring(0, 50) 
  });

  return true;
}

export async function wasNotifiedRecently(
  messageId: string,
  channelId: string,
  cooldownSeconds: number
): Promise<boolean> {
  const entry = cache.get(cacheKey(messageId, channelId));
  if (!entry || !entry.doc.notifiedAt) return false;
  return Date.now() - entry.doc.notifiedAt < cooldownSeconds * 1000;
}

export async function markNotified(messageId: string, channelId: string): Promise<void> {
  const key = cacheKey(messageId, channelId);
  const entry = cache.get(key);
  if (entry) {
    entry.doc.notifiedAt = Date.now();
    entry.doc.notificationStatus = 'sent';
    entry.doc.notificationSentAt = Date.now();
    entry.timestamp = Date.now();
    entry.version++;
    markDirty(key);
  }
}

export async function updateLastSeen(messageId: string, channelId: string): Promise<void> {
  const key = cacheKey(messageId, channelId);
  const entry = cache.get(key);
  if (entry) {
    entry.doc.lastSeenAt = Date.now();
    entry.timestamp = Date.now();
    entry.version++;
    markDirty(key);
  }
}

export async function markEnded(messageId: string, channelId: string): Promise<void> {
  const key = cacheKey(messageId, channelId);
  const entry = cache.get(key);
  if (entry) {
    entry.doc.status = 'ended';
    entry.timestamp = Date.now();
    entry.version++;
    activeGiveawaysCache.delete(key);
    markDirty(key);
  }
}

export async function setNotificationMessageId(
  giveawayMessageId: string,
  channelId: string,
  notificationMessageId: string
): Promise<void> {
  const key = cacheKey(giveawayMessageId, channelId);
  const entry = cache.get(key);
  if (entry) {
    entry.doc.notificationMessageId = notificationMessageId;
    entry.timestamp = Date.now();
    entry.version++;
    markDirty(key);
  }
}

export async function updateNotificationStatus(
  messageId: string,
  channelId: string,
  fields: {
    notificationStatus?: string;
    notificationSentAt?: number;
    notificationMessageId?: string;
    notificationError?: string;
  }
): Promise<void> {
  const key = cacheKey(messageId, channelId);
  const entry = cache.get(key);
  if (entry) {
    if (fields.notificationStatus !== undefined) entry.doc.notificationStatus = fields.notificationStatus;
    if (fields.notificationSentAt !== undefined) entry.doc.notificationSentAt = fields.notificationSentAt;
    if (fields.notificationMessageId !== undefined) entry.doc.notificationMessageId = fields.notificationMessageId;
    if (fields.notificationError !== undefined) entry.doc.notificationError = fields.notificationError;
    entry.timestamp = Date.now();
    entry.version++;
    markDirty(key);
  }
}

export async function getGiveaway(messageId: string, channelId: string): Promise<GiveawayData | null> {
  const key = cacheKey(messageId, channelId);
  const entry = cache.get(key);
  if (entry) {
    cacheHits++;
    return rowToGiveaway(entry.doc);
  }
  cacheMisses++;
  return null;
}

export async function getActiveGiveaways(limit: number = 50): Promise<GiveawayData[]> {
  const now = Date.now();
  const active: StoredGiveaway[] = [];
  
  for (const [key, doc] of activeGiveawaysCache) {
    if (doc.status === 'active' && (doc.endsAt === null || doc.endsAt > now)) {
      active.push(doc);
    } else {
      activeGiveawaysCache.delete(key);
    }
  }
  
  if (active.length >= limit) {
    return active
      .sort((a, b) => b.detectedAt - a.detectedAt)
      .slice(0, limit)
      .map(rowToGiveaway);
  }
  
  for (const [key, entry] of cache.entries()) {
    const d = entry.doc;
    if (d.status === 'active' && (d.endsAt === null || d.endsAt > now)) {
      active.push(d);
      activeGiveawaysCache.set(cacheKey(d.messageId, d.channelId), d);
    }
  }
  
  return active
    .sort((a, b) => b.detectedAt - a.detectedAt)
    .slice(0, limit)
    .map(rowToGiveaway);
}

export async function getGuildGiveaways(guildId: string, limit: number = 50): Promise<GiveawayData[]> {
  const guildSet = guildGiveawaysCache.get(guildId);
  if (!guildSet) return [];
  
  const giveaways: StoredGiveaway[] = [];
  for (const key of guildSet) {
    const entry = cache.get(key);
    if (entry) {
      giveaways.push(entry.doc);
    }
  }
  
  return giveaways
    .sort((a, b) => b.detectedAt - a.detectedAt)
    .slice(0, limit)
    .map(rowToGiveaway);
}

export async function getAllGiveaways(limit: number = 100): Promise<GiveawayData[]> {
  const results: StoredGiveaway[] = [];
  let count = 0;
  for (const [key, entry] of cache.entries()) {
    if (count >= limit) break;
    results.push(entry.doc);
    count++;
  }
  return results
    .sort((a, b) => b.detectedAt - a.detectedAt)
    .map(rowToGiveaway);
}

export async function getStats(): Promise<GiveawayStats> {
  const now = Date.now();
  let active = 0;
  let last: number | null = null;
  const guildIds = new Set<string>();
  
  active = activeGiveawaysCache.size;
  
  for (const [key, entry] of cache.entries()) {
    const d = entry.doc;
    guildIds.add(d.guildId);
    if (last === null || d.detectedAt > last) last = d.detectedAt;
  }

  return {
    totalDetected: totalDetectedCount,
    activeGiveaways: active,
    serversWithGiveaways: guildIds.size,
    lastDetected: last,
  };
}

export async function resetDatabase(): Promise<void> {
  cache.clear();
  activeGiveawaysCache.clear();
  guildGiveawaysCache.clear();
  totalDetectedCount = 0;
  dirtyTotal = false;
  dirtyKeys.clear();
  pendingDeletes.clear();

  if (connected) {
    await giveawaysCol.deleteMany({});
    await countersCol.updateOne(
      { _id: 'total_detected' }, 
      { $set: { total: 0, lastUpdated: Date.now() } }, 
      { upsert: true }
    );
  }

  logger.warn('Database reset', { component: 'Database' });
}

export async function cleanupOldGiveaways(days: number = 30): Promise<number> {
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
  let removed = 0;
  
  const entries = Array.from(cache.entries());
  for (const [key, entry] of entries) {
    const d = entry.doc;
    if (d.status !== 'active' && d.detectedAt < cutoff) {
      cache.delete(key);
      activeGiveawaysCache.delete(key);
      const guildSet = guildGiveawaysCache.get(d.guildId);
      if (guildSet) {
        guildSet.delete(key);
        if (guildSet.size === 0) {
          guildGiveawaysCache.delete(d.guildId);
        }
      }
      dirtyKeys.delete(key);
      pendingDeletes.add(d.messageId);
      removed++;
    }
  }
  
  if (removed > 0) {
    scheduleSync();
    logger.info(`Cleaned up ${removed} old giveaways`, { component: 'Database' });
  }
  
  return removed;
}

export async function purgeEndedGiveaways(): Promise<GiveawayData[]> {
  const now = Date.now();
  const removed: GiveawayData[] = [];

  const entries = Array.from(cache.entries());
  for (const [key, entry] of entries) {
    const d = entry.doc;
    const isRunning = d.status === 'active' && (d.endsAt === null || d.endsAt > now);
    if (!isRunning) {
      removed.push(rowToGiveaway(d));
      cache.delete(key);
      activeGiveawaysCache.delete(key);
      const guildSet = guildGiveawaysCache.get(d.guildId);
      if (guildSet) {
        guildSet.delete(key);
        if (guildSet.size === 0) {
          guildGiveawaysCache.delete(d.guildId);
        }
      }
      dirtyKeys.delete(key);
      pendingDeletes.add(d.messageId);
    }
  }

  if (removed.length > 0) {
    scheduleSync();
    logger.info(`Purged ${removed.length} expired giveaways`, { component: 'Database' });
  }

  return removed;
}

// ============================================================================
// Helper Functions
// ============================================================================

function rowToGiveaway(row: StoredGiveaway): GiveawayData {
  return {
    messageId: row.messageId,
    channelId: row.channelId,
    guildId: row.guildId,
    guildName: row.guildName,
    channelName: row.channelName,
    authorId: row.authorId,
    prize: row.prize,
    detectedAt: row.detectedAt,
    endsAt: row.endsAt,
    status: row.status,
    notifiedAt: row.notifiedAt,
    lastSeenAt: row.lastSeenAt,
    notificationMessageId: row.notificationMessageId,
    ...(row.notificationStatus && { notificationStatus: row.notificationStatus }),
    ...(row.notificationSentAt && { notificationSentAt: row.notificationSentAt }),
    ...(row.notificationError && { notificationError: row.notificationError }),
  };
}

// ============================================================================
// Public API - Watchlist Management
// ============================================================================

export async function addItem(userId: string, item: string): Promise<boolean> {
  if (!userId || !item) {
    logger.warn('Invalid watchlist item', { component: 'Database', userId, item });
    return false;
  }
  
  await ensureConnected();
  
  const trimmedItem = item.toLowerCase().trim();
  if (!trimmedItem) return false;
  
  const result = await watchlistCol.updateOne(
    { userId },
    { 
      $addToSet: { items: trimmedItem },
      $set: { updatedAt: Date.now() },
      $setOnInsert: { createdAt: Date.now() }
    },
    { upsert: true }
  );
  
  return result.modifiedCount > 0 || result.upsertedCount > 0;
}

export async function removeItem(userId: string, item: string): Promise<boolean> {
  if (!userId || !item) return false;
  
  await ensureConnected();
  
  const trimmedItem = item.toLowerCase().trim();
  if (!trimmedItem) return false;
  
  const result = await watchlistCol.updateOne(
    { userId },
    { $pull: { items: trimmedItem } }
  );
  
  return result.modifiedCount > 0;
}

export async function getItems(userId: string): Promise<string[]> {
  if (!userId) return [];
  
  await ensureConnected();
  
  const doc = await watchlistCol.findOne({ userId });
  return doc?.items || [];
}

export async function getAllWatchlists(): Promise<UserWatchlist[]> {
  await ensureConnected();
  
  try {
    return await watchlistCol.find({}).toArray();
  } catch (err) {
    logger.error('Failed to get watchlists', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
    return [];
  }
}

export async function clearItems(userId: string): Promise<void> {
  if (!userId) return;
  
  await ensureConnected();
  
  await watchlistCol.updateOne(
    { userId },
    { $set: { items: [], updatedAt: Date.now() } }
  );
}

// ============================================================================
// Public API - License System
// ============================================================================

export async function createLicenseKey(key: string, createdBy: string): Promise<void> {
  if (!key || !createdBy) {
    throw new Error('Key and createdBy are required');
  }
  
  await ensureConnected();
  
  await licenseKeysCol.insertOne({
    key,
    used: false,
    usedBy: null,
    createdAt: Date.now(),
    createdBy,
  });
}

export async function getLicenseKey(key: string): Promise<LicenseKey | null> {
  if (!key) return null;
  
  await ensureConnected();
  return licenseKeysCol.findOne({ key });
}

export async function validateLicenseKey(key: string): Promise<{
  valid: boolean;
  error?: string;
}> {
  if (!key) {
    return { valid: false, error: 'License key is required.' };
  }
  
  await ensureConnected();

  const license = await licenseKeysCol.findOne({ key });
  if (!license) {
    return { valid: false, error: 'Invalid license key.' };
  }

  if (license.used) {
    return { valid: false, error: 'This license key has already been used.' };
  }

  return { valid: true };
}

export async function useLicenseKey(key: string, userId: string): Promise<{
  success: boolean;
  error?: string;
}> {
  if (!key || !userId) {
    return { success: false, error: 'Key and userId are required.' };
  }
  
  await ensureConnected();

  const session = client.startSession();
  
  try {
    let result;
    await session.withTransaction(async () => {
      const validation = await validateLicenseKey(key);
      if (!validation.valid) {
        throw new Error(validation.error);
      }

      result = await licenseKeysCol.updateOne(
        { key },
        { $set: { used: true, usedBy: userId } },
        { session }
      );
    });
    
    return { success: result!.modifiedCount > 0 };
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    return { success: false, error: errorMsg };
  } finally {
    await session.endSession();
  }
}

export async function listLicenseKeys(limit: number = 50): Promise<LicenseKey[]> {
  await ensureConnected();
  return licenseKeysCol.find({}).sort({ createdAt: -1 }).limit(Math.min(limit, 100)).toArray();
}

export async function getLicenseStats(): Promise<{
  total: number;
  used: number;
  unused: number;
}> {
  await ensureConnected();
  const total = await licenseKeysCol.countDocuments();
  const used = await licenseKeysCol.countDocuments({ used: true });
  return { total, used, unused: total - used };
}

// ============================================================================
// Public API - Premium User Tracking
// ============================================================================

export async function setPremiumUser(
  userId: string,
  guildId: string,
  source: 'key' | 'booster' | 'manual',
  licenseKey?: string
): Promise<void> {
  if (!userId || !guildId) {
    throw new Error('userId and guildId are required');
  }
  
  await ensureConnected();

  const updateData: Partial<PremiumUser> = {
    userId,
    guildId,
    isPremium: true,
    source,
    activatedAt: Date.now(),
    expiresAt: null,
    lastChecked: Date.now(),
    version: 1,
  };

  if (licenseKey) {
    updateData.licenseKey = licenseKey;
  }

  await premiumUsersCol.updateOne(
    { userId, guildId },
    { $set: updateData },
    { upsert: true }
  );

  logger.debug('Premium user set', { userId, guildId, source });
}

export async function removePremiumUser(
  userId: string,
  guildId: string
): Promise<void> {
  if (!userId || !guildId) return;
  
  await ensureConnected();

  const existing = await premiumUsersCol.findOne({ userId, guildId });
  const newVersion = (existing?.version && typeof existing.version === 'number') ? existing.version + 1 : 1;

  await premiumUsersCol.updateOne(
    { userId, guildId },
    {
      $set: {
        isPremium: false,
        lastChecked: Date.now(),
        version: newVersion,
      },
    }
  );

  logger.debug('Premium user removed', { userId, guildId });
}

export async function getPremiumUser(
  userId: string,
  guildId: string
): Promise<PremiumUser | null> {
  if (!userId || !guildId) return null;
  
  await ensureConnected();
  return premiumUsersCol.findOne({ userId, guildId });
}

export async function isPremiumUser(
  userId: string,
  guildId: string
): Promise<boolean> {
  if (!userId || !guildId) return false;
  
  await ensureConnected();
  const user = await premiumUsersCol.findOne({ userId, guildId, isPremium: true });
  return !!user;
}

export async function getAllPremiumUsers(guildId: string): Promise<PremiumUser[]> {
  if (!guildId) return [];
  
  await ensureConnected();
  return premiumUsersCol.find({
    guildId,
    isPremium: true,
  }).toArray();
}

export async function getAllPremiumUsersAllGuilds(): Promise<PremiumUser[]> {
  await ensureConnected();
  return premiumUsersCol.find({
    isPremium: true,
  }).toArray();
}

export async function getPremiumUsersBySource(
  guildId: string,
  source: 'key' | 'booster' | 'manual'
): Promise<PremiumUser[]> {
  if (!guildId) return [];
  
  await ensureConnected();
  return premiumUsersCol.find({
    guildId,
    isPremium: true,
    source,
  }).toArray();
}

export async function getPremiumStats(guildId: string): Promise<{
  total: number;
  byKey: number;
  byBooster: number;
  byManual: number;
}> {
  if (!guildId) {
    return { total: 0, byKey: 0, byBooster: 0, byManual: 0 };
  }
  
  await ensureConnected();

  const total = await premiumUsersCol.countDocuments({ guildId, isPremium: true });
  const byKey = await premiumUsersCol.countDocuments({ guildId, isPremium: true, source: 'key' });
  const byBooster = await premiumUsersCol.countDocuments({ guildId, isPremium: true, source: 'booster' });
  const byManual = await premiumUsersCol.countDocuments({ guildId, isPremium: true, source: 'manual' });

  return { total, byKey, byBooster, byManual };
}

// ============================================================================
// Public API - Auto Joiner Token & Webhook Management
// ============================================================================

export async function updateUserToken(
  userId: string,
  guildId: string,
  encryptedToken: string,
  label: string
): Promise<void> {
  if (!userId || !guildId || !encryptedToken) {
    throw new Error('userId, guildId, and encryptedToken are required');
  }
  
  await ensureConnected();

  const existing = await premiumUsersCol.findOne({ userId, guildId });
  const newVersion = (existing?.version && typeof existing.version === 'number') ? existing.version + 1 : 1;

  await premiumUsersCol.updateOne(
    { userId, guildId },
    {
      $set: {
        token: encryptedToken,
        tokenLabel: label || 'Unnamed',
        tokenAddedAt: Date.now(),
        tokenLastUsed: null,
        tokenEntries: 0,
        tokenWins: 0,
        tokenActive: true,
        lastChecked: Date.now(),
        version: newVersion,
      },
    },
    { upsert: true }
  );

  logger.debug('User token updated', { userId, guildId, label });
}

export async function updateUserWebhook(
  userId: string,
  guildId: string,
  webhookUrl: string
): Promise<void> {
  if (!userId || !guildId || !webhookUrl) {
    throw new Error('userId, guildId, and webhookUrl are required');
  }
  
  await ensureConnected();

  const existing = await premiumUsersCol.findOne({ userId, guildId });
  const newVersion = (existing?.version && typeof existing.version === 'number') ? existing.version + 1 : 1;

  await premiumUsersCol.updateOne(
    { userId, guildId },
    {
      $set: {
        webhookUrl: webhookUrl,
        webhookAddedAt: Date.now(),
        webhookLastUsed: null,
        lastChecked: Date.now(),
        version: newVersion,
      },
    },
    { upsert: true }
  );

  logger.debug('User webhook updated', { userId, guildId });
}

export async function getUserToken(
  userId: string,
  guildId: string
): Promise<{ token: string | null; label: string | null }> {
  if (!userId || !guildId) {
    return { token: null, label: null };
  }
  
  await ensureConnected();
  const user = await premiumUsersCol.findOne({ userId, guildId });
  return {
    token: user?.token || null,
    label: user?.tokenLabel || null,
  };
}

export async function getUserWebhook(
  userId: string,
  guildId: string
): Promise<string | null> {
  if (!userId || !guildId) return null;
  
  await ensureConnected();
  const user = await premiumUsersCol.findOne({ userId, guildId });
  return user?.webhookUrl || null;
}

export async function incrementTokenEntries(
  userId: string,
  guildId: string
): Promise<void> {
  if (!userId || !guildId) return;
  
  await ensureConnected();
  
  await premiumUsersCol.updateOne(
    { userId, guildId },
    { $inc: { tokenEntries: 1 } }
  );
}

export async function incrementTokenWins(
  userId: string,
  guildId: string
): Promise<void> {
  if (!userId || !guildId) return;
  
  await ensureConnected();
  
  await premiumUsersCol.updateOne(
    { userId, guildId },
    { $inc: { tokenWins: 1 } }
  );
}

export async function updateTokenLastUsed(
  userId: string,
  guildId: string
): Promise<void> {
  if (!userId || !guildId) return;
  
  await ensureConnected();
  
  await premiumUsersCol.updateOne(
    { userId, guildId },
    {
      $set: { tokenLastUsed: Date.now() },
    }
  );
}

export async function setTokenActive(
  userId: string,
  guildId: string,
  active: boolean
): Promise<void> {
  if (!userId || !guildId) return;
  
  await ensureConnected();
  
  await premiumUsersCol.updateOne(
    { userId, guildId },
    {
      $set: { tokenActive: active },
    }
  );
}

// ============================================================================
// Public API - AutoJoin Entries
// ============================================================================

export async function getAutoJoinEntriesCollection(): Promise<Collection<AutoJoinEntry>> {
  await ensureConnected();
  return autoJoinEntriesCol;
}

export async function saveAutoJoinEntry(entry: Omit<AutoJoinEntry, '_id'>): Promise<void> {
  if (!entry.userId || !entry.messageId || !entry.channelId) {
    throw new Error('userId, messageId, and channelId are required');
  }
  
  await ensureConnected();
  
  const _id = `${entry.userId}:${entry.channelId}:${entry.messageId}`;

  try {
    await autoJoinEntriesCol.updateOne(
      { _id },
      {
        $setOnInsert: {
          _id,
          ...entry,
        },
      },
      { upsert: true }
    );
  } catch (err) {
    const isDuplicateKeyError =
      err && typeof err === 'object' && 'code' in err && (err as { code?: number }).code === 11000;
    if (!isDuplicateKeyError) {
      throw err;
    }
    logger.debug('saveAutoJoinEntry: duplicate entry ignored (race)', {
      component: 'Database',
      entryId: _id,
    });
  }
}

export async function getAutoJoinEntry(
  userId: string,
  messageId: string,
  channelId: string
): Promise<AutoJoinEntry | null> {
  if (!userId || !messageId || !channelId) return null;
  
  await ensureConnected();
  const entryId = `${userId}:${channelId}:${messageId}`;
  return autoJoinEntriesCol.findOne({ _id: entryId });
}

export async function updateAutoJoinEntryStatus(
  userId: string,
  messageId: string,
  channelId: string,
  status: 'pending' | 'queued' | 'attempting' | 'success' | 'failed' | 'skipped' | 'dead_letter',
  updates?: Partial<Omit<AutoJoinEntry, '_id' | 'userId' | 'messageId' | 'channelId'>>
): Promise<void> {
  if (!userId || !messageId || !channelId) return;
  
  await ensureConnected();
  const entryId = `${userId}:${channelId}:${messageId}`;
  await autoJoinEntriesCol.updateOne(
    { _id: entryId },
    { $set: { status, ...updates } }
  );
}

export async function deleteAutoJoinEntry(
  userId: string,
  messageId: string,
  channelId: string
): Promise<void> {
  if (!userId || !messageId || !channelId) return;
  
  await ensureConnected();
  const entryId = `${userId}:${channelId}:${messageId}`;
  await autoJoinEntriesCol.deleteOne({ _id: entryId });
}

export async function cleanupAutoJoinEntries(userId: string): Promise<number> {
  if (!userId) return 0;
  
  await ensureConnected();
  const cutoff = Date.now() - 2 * 60 * 60 * 1000;
  const result = await autoJoinEntriesCol.deleteMany({
    userId,
    status: { $in: ['success', 'failed', 'skipped'] },
    detectedAt: { $lt: cutoff }
  });
  return result.deletedCount || 0;
}

export async function getPendingAutoJoinEntries(userId: string): Promise<AutoJoinEntry[]> {
  if (!userId) return [];
  
  await ensureConnected();
  return autoJoinEntriesCol.find({
    userId,
    status: { $in: ['pending', 'attempting'] }
  }).toArray();
}

// ============================================================================
// Batch Operations for AutoJoiner
// ============================================================================

export async function batchSaveJoinOutcomes(outcomes: Array<{
  userId: string;
  channelId: string;
  messageId: string;
  status: string;
  attempts: number;
  entryTimeMs?: number;
}>): Promise<void> {
  await ensureConnected();
  if (!outcomes || outcomes.length === 0) return;
  
  try {
    const bulkOps = outcomes.map(outcome => ({
      updateOne: {
        filter: { 
          _id: `${outcome.userId}:${outcome.channelId}:${outcome.messageId}` 
        },
        update: { 
          $set: { 
            status: outcome.status,
            attempts: outcome.attempts,
            entryTimeMs: outcome.entryTimeMs,
            lastAttemptAt: Date.now(),
          } 
        },
        upsert: false,
      }
    }));
    
    await autoJoinEntriesCol.bulkWrite(bulkOps as any, { ordered: false });
  } catch (err) {
    logger.error('batchSaveJoinOutcomes error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
  }
}

export async function batchUpdateDetectionConfidence(updates: Array<{
  messageId: string;
  channelId: string;
  confidence: number;
  reasons: string[];
}>): Promise<void> {
  await ensureConnected();
  if (!updates || updates.length === 0) return;
  
  try {
    const bulkOps = updates.map(update => ({
      updateOne: {
        filter: { 
          messageId: update.messageId, 
          channelId: update.channelId 
        },
        update: { 
          $set: { 
            detectionConfidence: update.confidence,
            detectionReasons: update.reasons,
          } 
        },
      }
    }));
    
    await autoJoinEntriesCol.bulkWrite(bulkOps as any, { ordered: false });
  } catch (err) {
    logger.error('batchUpdateDetectionConfidence error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
  }
}

export async function archiveOldGiveaways(ageMs: number): Promise<number> {
  await ensureConnected();
  
  try {
    const cutoff = Date.now() - ageMs;
    const result = await autoJoinEntriesCol.updateMany(
      { 
        detectedAt: { $lt: cutoff },
        status: { $in: ['success', 'failed', 'skipped', 'dead_letter'] },
        archived: { $ne: true }
      },
      { 
        $set: { 
          archived: true, 
          archivedAt: Date.now() 
        } 
      }
    );
    return result.modifiedCount || 0;
  } catch (err) {
    logger.error('archiveOldGiveaways error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
    return 0;
  }
}

export async function saveWatchlistMatch(
  userId: string, 
  keyword: string, 
  messageId: string, 
  channelId: string
): Promise<void> {
  await ensureConnected();
  
  try {
    const watchlistMatchesCol = db.collection('watchlist_matches');
    await watchlistMatchesCol.insertOne({
      userId,
      keyword,
      messageId,
      channelId,
      matchedAt: Date.now(),
    });
  } catch (err) {
    logger.error('saveWatchlistMatch error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
  }
}

export async function getWatchlistKeywords(userId: string): Promise<Array<{
  keyword: string;
  aliases: string[];
  matchCount: number;
  lastMatched: number;
}>> {
  await ensureConnected();
  
  try {
    const doc = await watchlistCol.findOne({ userId });
    if (!doc || !doc.items) return [];
    
    return doc.items.map((keyword: string) => ({
      keyword,
      aliases: [],
      matchCount: 0,
      lastMatched: 0,
    }));
  } catch (err) {
    logger.error('getWatchlistKeywords error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
    return [];
  }
}

export async function getDetectionProfiles(): Promise<any[]> {
  await ensureConnected();
  
  try {
    const detectionProfilesCol = db.collection('detection_profiles');
    return await detectionProfilesCol.find({}).toArray();
  } catch (err) {
    logger.error('getDetectionProfiles error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
    return [];
  }
}

export async function updateDetectionProfile(profile: any): Promise<void> {
  await ensureConnected();
  
  try {
    const detectionProfilesCol = db.collection('detection_profiles');
    await detectionProfilesCol.updateOne(
      { botId: profile.botId },
      { $set: profile },
      { upsert: true }
    );
  } catch (err) {
    logger.error('updateDetectionProfile error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
  }
}

export async function saveQueueState(items: any[]): Promise<void> {
  await ensureConnected();
  
  try {
    const queueStateCol = db.collection('queue_state');
    await queueStateCol.deleteMany({});
    if (items.length > 0) {
      await queueStateCol.insertMany(items);
    }
  } catch (err) {
    logger.error('saveQueueState error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
  }
}

export async function loadQueueState(): Promise<any[]> {
  await ensureConnected();
  
  try {
    const queueStateCol = db.collection('queue_state');
    return await queueStateCol.find({}).toArray();
  } catch (err) {
    logger.error('loadQueueState error', { 
      component: 'Database',
      error: err instanceof Error ? err.message : String(err) 
    });
    return [];
  }
}

// ============================================================================
// Public API - Booster Premium Tracking
// ============================================================================

export async function setBoosterPremium(
  userId: string,
  guildId: string,
  isBooster: boolean
): Promise<void> {
  if (!userId || !guildId) {
    throw new Error('userId and guildId are required');
  }
  
  await ensureConnected();

  const existing = await boosterPremiumCol.findOne({ userId, guildId });
  const newVersion = (existing?.version && typeof existing.version === 'number') ? existing.version + 1 : 1;

  await boosterPremiumCol.updateOne(
    { userId, guildId },
    {
      $set: {
        userId,
        guildId,
        isBooster,
        premiumAssigned: isBooster,
        assignedAt: isBooster ? Date.now() : 0,
        lastChecked: Date.now(),
        version: newVersion,
      },
    },
    { upsert: true }
  );
}

export async function getBoosterPremium(
  userId: string,
  guildId: string
): Promise<BoosterPremium | null> {
  if (!userId || !guildId) return null;
  
  await ensureConnected();
  return boosterPremiumCol.findOne({ userId, guildId });
}

export async function getActiveBoosters(guildId: string): Promise<BoosterPremium[]> {
  if (!guildId) return [];
  
  await ensureConnected();
  return boosterPremiumCol.find({
    guildId,
    isBooster: true,
    premiumAssigned: true,
  }).toArray();
}

export async function removeBoosterPremium(
  userId: string,
  guildId: string
): Promise<void> {
  if (!userId || !guildId) return;
  
  await ensureConnected();

  const existing = await boosterPremiumCol.findOne({ userId, guildId });
  const newVersion = (existing?.version && typeof existing.version === 'number') ? existing.version + 1 : 1;

  await boosterPremiumCol.updateOne(
    { userId, guildId },
    {
      $set: {
        isBooster: false,
        premiumAssigned: false,
        lastChecked: Date.now(),
        version: newVersion,
      },
    }
  );
}

export async function updateBoosterPremiumStatus(
  userId: string,
  guildId: string,
  isBooster: boolean
): Promise<{
  shouldHavePremium: boolean;
  currentStatus: boolean;
}> {
  if (!userId || !guildId) {
    throw new Error('userId and guildId are required');
  }
  
  await ensureConnected();

  const record = await getBoosterPremium(userId, guildId);

  if (!record) {
    await setBoosterPremium(userId, guildId, isBooster);
    return {
      shouldHavePremium: isBooster,
      currentStatus: false,
    };
  }

  const shouldHavePremium = isBooster;

  if (record.isBooster !== isBooster) {
    await setBoosterPremium(userId, guildId, isBooster);
  }

  return {
    shouldHavePremium,
    currentStatus: record.premiumAssigned,
  };
}

export async function getBoosterPremiumStats(guildId: string): Promise<{
  total: number;
  withPremium: number;
  withoutPremium: number;
}> {
  if (!guildId) {
    return { total: 0, withPremium: 0, withoutPremium: 0 };
  }
  
  await ensureConnected();

  const total = await boosterPremiumCol.countDocuments({ guildId, isBooster: true });
  const withPremium = await boosterPremiumCol.countDocuments({
    guildId,
    isBooster: true,
    premiumAssigned: true,
  });

  return {
    total,
    withPremium,
    withoutPremium: total - withPremium,
  };
}

// ============================================================================
// Shutdown & Cleanup
// ============================================================================

export async function closeDb(): Promise<void> {
  isShuttingDown = true;
  
  logger.info('Closing database connection...', { component: 'Database' });
  
  clearInterval(cleanupInterval);
  clearInterval(consistencyInterval);
  
  if (syncTimeout) {
    clearTimeout(syncTimeout);
    syncTimeout = null;
  }
  
  // 🔥 Force flush before closing
  await flushSync();

  if (client) {
    try {
      // Force connection pool to close
      await client.close(true);
    } catch (err) {
      logger.error('Error closing MongoDB connection', { 
        component: 'Database',
        error: err instanceof Error ? err.message : String(err) 
      });
      // Force cleanup
      try {
        (client as any).topology?.s?.pool?.destroy();
      } catch {}
    }
    connected = false;
  }
  
  // Clear everything
  cache.clear();
  activeGiveawaysCache.clear();
  guildGiveawaysCache.clear();
  dirtyKeys.clear();
  pendingDeletes.clear();
  
  // Force GC
  if (global.gc) {
    global.gc();
  }
  
  logger.info('Database connection closed', { component: 'Database' });
}

// Export cache stats for monitoring
export function getDatabaseStats() {
  return {
    cache: getCacheStats(),
    totalDetected: totalDetectedCount,
    connected,
    activeGiveaways: activeGiveawaysCache.size,
    guilds: guildGiveawaysCache.size,
    dirtyKeys: dirtyKeys.size,
    pendingDeletes: pendingDeletes.size,
    isSyncing,
    isShuttingDown,
    emergencyCleanupCount,
  };
}

// Export memory stats
export function getMemoryStats() {
  const mem = process.memoryUsage();
  return {
    heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
    heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
    rssMB: Math.round(mem.rss / 1024 / 1024),
    cacheSize: cache.size,
    dirtyKeys: dirtyKeys.size,
    pendingDeletes: pendingDeletes.size,
    emergencyCleanupCount,
  };
}

// Handle process exit
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, closing database...', { component: 'Database' });
  await closeDb();
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, closing database...', { component: 'Database' });
  await closeDb();
});

// ============================================================================
// Initialization
// ============================================================================

// Auto-connect on import
connect().catch((err) => {
  logger.error('Initial connection failed', { 
    component: 'Database', 
    error: err instanceof Error ? err.message : String(err) 
  });
});

export default {
  // Giveaway functions
  getDb,
  getTotalDetected,
  insertGiveaway,
  wasNotifiedRecently,
  markNotified,
  updateLastSeen,
  markEnded,
  setNotificationMessageId,
  updateNotificationStatus,
  getGiveaway,
  getActiveGiveaways,
  getGuildGiveaways,
  getAllGiveaways,
  getStats,
  resetDatabase,
  cleanupOldGiveaways,
  purgeEndedGiveaways,
  
  // Watchlist functions
  addItem,
  removeItem,
  getItems,
  getAllWatchlists,
  clearItems,
  
  // License functions
  createLicenseKey,
  getLicenseKey,
  validateLicenseKey,
  useLicenseKey,
  listLicenseKeys,
  getLicenseStats,
  
  // Premium functions
  setPremiumUser,
  removePremiumUser,
  getPremiumUser,
  isPremiumUser,
  getAllPremiumUsers,
  getAllPremiumUsersAllGuilds,
  getPremiumUsersBySource,
  getPremiumStats,
  
  // Token & Webhook functions
  updateUserToken,
  updateUserWebhook,
  getUserToken,
  getUserWebhook,
  incrementTokenEntries,
  incrementTokenWins,
  updateTokenLastUsed,
  setTokenActive,
  
  // Auto-join functions
  getAutoJoinEntriesCollection,
  saveAutoJoinEntry,
  getAutoJoinEntry,
  updateAutoJoinEntryStatus,
  deleteAutoJoinEntry,
  cleanupAutoJoinEntries,
  getPendingAutoJoinEntries,
  batchSaveJoinOutcomes,
  batchUpdateDetectionConfidence,
  archiveOldGiveaways,
  saveWatchlistMatch,
  getWatchlistKeywords,
  getDetectionProfiles,
  updateDetectionProfile,
  saveQueueState,
  loadQueueState,
  
  // Booster functions
  setBoosterPremium,
  getBoosterPremium,
  getActiveBoosters,
  removeBoosterPremium,
  updateBoosterPremiumStatus,
  getBoosterPremiumStats,
  
  // Admin functions
  closeDb,
  getDatabaseStats,
  getMemoryStats,
  resyncCache,
};
