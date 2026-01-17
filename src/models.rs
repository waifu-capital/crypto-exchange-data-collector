use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Data structure for snapshot messages sent through the channel
pub struct SnapshotData {
    pub exchange: String,              // "binance", "coinbase", "bybit", etc.
    pub symbol: String,                // "btcusdt", "BTC-USD", etc.
    pub data_type: String,             // "orderbook", "trade"
    pub exchange_sequence_id: String,  // Exchange-specific ID for deduplication
    pub timestamp_collector: i64,      // Microseconds since Unix epoch (our receipt time)
    pub timestamp_exchange: i64,       // Milliseconds since Unix epoch (exchange event time)
    pub data: String,                  // JSON payload
}

/// Tracks sequence IDs for detecting gaps in exchange data streams
pub struct SequenceTracker {
    /// Last seen sequence ID per key ("exchange:symbol:data_type")
    last_seen: HashMap<String, i64>,
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

impl SequenceTracker {
    pub fn new() -> Self {
        Self {
            last_seen: HashMap::new(),
        }
    }

    /// Check for sequence gaps. Returns Some(gap) if a gap is detected.
    /// Only works for numeric sequence IDs; returns None for non-numeric.
    pub fn check(
        &mut self,
        exchange: &str,
        symbol: &str,
        data_type: &str,
        sequence_id: &str,
        current_time: i64,
    ) -> Option<SequenceGap> {
        let key = format!("{}:{}:{}", exchange, symbol, data_type);

        // Try to parse as i64 for numeric comparison
        let seq = match sequence_id.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return None, // Can't track non-numeric sequences
        };

        if let Some(&last) = self.last_seen.get(&key) {
            let expected = last + 1;
            if seq > expected {
                let gap = SequenceGap {
                    key: key.clone(),
                    expected,
                    received: seq,
                    gap_size: seq - expected,
                    detected_at: current_time,
                };
                self.last_seen.insert(key, seq);
                return Some(gap);
            }
        }

        self.last_seen.insert(key, seq);
        None
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
