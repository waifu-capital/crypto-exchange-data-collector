use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Data structure for snapshot messages sent through the channel
pub struct SnapshotData {
    pub exchange: String,              // "binance", "coinbase", "bybit", etc.
    pub symbol: String,                // "btcusdt", "BTC-USD", etc.
    pub data_type: String,             // "orderbook", "trade"
    pub exchange_sequence_id: String,  // Exchange-specific ID for deduplication
    pub timestamp_collector: i64,      // Microseconds since Unix epoch (our receipt time)
    pub timestamp_exchange: i64,       // Microseconds since Unix epoch (exchange event time)
    pub data: String,                  // JSON payload
}

/// Default maximum entries for SequenceTracker (prevents unbounded memory)
const DEFAULT_MAX_TRACKER_ENTRIES: usize = 10_000;

/// Tracks sequence IDs for detecting gaps in exchange data streams.
/// Uses LRU-style eviction to bound memory usage.
pub struct SequenceTracker {
    /// Last seen sequence ID per key ("exchange:symbol:data_type")
    last_seen: HashMap<String, i64>,
    /// Insertion order for LRU eviction
    insertion_order: VecDeque<String>,
    /// Maximum number of entries before eviction
    max_entries: usize,
}

/// Represents a detected gap in sequence IDs
#[derive(Debug, Clone)]
pub struct SequenceGap {
    pub key: String,
    pub expected: i64,
    pub received: i64,
    pub gap_size: i64,
    pub detected_at: i64,
}

/// Result of sequence check - distinguishes between different sequence anomalies
#[derive(Debug, Clone)]
pub enum SequenceCheckResult {
    /// Normal progression (seq == last + 1 or first message)
    Ok,
    /// Gap detected (seq > last + 1, missed messages)
    Gap(SequenceGap),
    /// Out-of-order message (seq < last)
    OutOfOrder { expected: i64, received: i64 },
    /// Duplicate message (seq == last)
    Duplicate { seq: i64 },
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self {
            last_seen: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_entries: DEFAULT_MAX_TRACKER_ENTRIES,
        }
    }

    /// Create a tracker with custom max entries
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            last_seen: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_entries,
        }
    }

    /// Check for sequence anomalies (gaps, out-of-order, duplicates).
    /// Only works for numeric sequence IDs; returns Ok for non-numeric.
    pub fn check(
        &mut self,
        exchange: &str,
        symbol: &str,
        data_type: &str,
        sequence_id: &str,
        current_time: i64,
    ) -> SequenceCheckResult {
        let key = format!("{}:{}:{}", exchange, symbol, data_type);

        // Try to parse as i64 for numeric comparison
        let seq = match sequence_id.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return SequenceCheckResult::Ok, // Can't track non-numeric sequences
        };

        if let Some(&last) = self.last_seen.get(&key) {
            if seq > last + 1 {
                // Gap detected - update last_seen
                self.last_seen.insert(key.clone(), seq);
                return SequenceCheckResult::Gap(SequenceGap {
                    key,
                    expected: last + 1,
                    received: seq,
                    gap_size: seq - last - 1,
                    detected_at: current_time,
                });
            } else if seq == last {
                // Duplicate - don't update last_seen
                return SequenceCheckResult::Duplicate { seq };
            } else if seq < last {
                // Out of order - don't update last_seen
                return SequenceCheckResult::OutOfOrder {
                    expected: last + 1,
                    received: seq,
                };
            }
            // seq == last + 1: Normal progression, update below
        }

        // Before inserting new key, evict if at capacity
        if !self.last_seen.contains_key(&key) {
            if self.last_seen.len() >= self.max_entries {
                if let Some(oldest_key) = self.insertion_order.pop_front() {
                    self.last_seen.remove(&oldest_key);
                }
            }
            self.insertion_order.push_back(key.clone());
        }

        self.last_seen.insert(key, seq);
        SequenceCheckResult::Ok
    }

    /// Returns the current number of tracked keys
    pub fn len(&self) -> usize {
        self.last_seen.len()
    }

    /// Returns true if no keys are being tracked
    pub fn is_empty(&self) -> bool {
        self.last_seen.is_empty()
    }
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared state for WebSocket connection status
/// Key: "exchange:symbol" (e.g., "binance:btcusdt"), Value: connected (true/false)
pub type ConnectionState = Arc<RwLock<HashMap<String, bool>>>;

/// Create a new connection state instance
pub fn new_connection_state() -> ConnectionState {
    Arc::new(RwLock::new(HashMap::new()))
}
