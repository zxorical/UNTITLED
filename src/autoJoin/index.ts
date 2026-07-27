/**
 * @module autoJoin
 * 
 * AutoJoin module exports
 */

// Main class
export { AutoJoinManager } from './manager.js';

// Types
export type { 
  GiveawayEntry, 
  GiveawayButton,
  GiveawayStats,
  EntryStatus 
} from './manager.js';

// If you need globalQueue, uncomment this AFTER adding export to manager.ts
// export { globalQueue } from './manager.js';
