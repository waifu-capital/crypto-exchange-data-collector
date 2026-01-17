use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Data structure for snapshot messages sent through the channel
pub struct SnapshotData {
    pub exchange: String,              // "binance", "coinbase", "bybit", etc.
    pub symbol: String,                // "btcusdt", "BTC-USD", etc.
    pub data_type: String,             // "orderbook", "trade"
    pub exchange_sequence_id: String,  // Exchange-specific ID for deduplication
    pub timestamp: i64,                // Microseconds since Unix epoch (our receipt time)
    pub data: String,                  // JSON payload
}

/// Shared state for WebSocket connection status
/// Key: "exchange:symbol" (e.g., "binance:btcusdt"), Value: connected (true/false)
pub type ConnectionState = Arc<RwLock<HashMap<String, bool>>>;

/// Create a new connection state instance
pub fn new_connection_state() -> ConnectionState {
    Arc::new(RwLock::new(HashMap::new()))
}
