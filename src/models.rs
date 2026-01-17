use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type-safe enum for market data types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Orderbook,
    Trade,
}

impl DataType {
    /// String representation for database/metrics
    pub fn as_str(&self) -> &'static str {
        match self {
            DataType::Orderbook => "orderbook",
            DataType::Trade => "trade",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Market event data sent through the channel to the database worker.
/// Replaces the old SnapshotData name which was misleading for trades.
pub struct MarketEvent {
    pub exchange: String,              // "binance", "coinbase", "bybit", etc.
    pub symbol: String,                // "btcusdt", "BTC-USD", etc.
    pub data_type: DataType,           // Orderbook or Trade (was String)
    pub exchange_sequence_id: String,  // Exchange-specific ID for deduplication
    pub timestamp_collector: i64,      // Microseconds since Unix epoch (our receipt time)
    pub timestamp_exchange: i64,       // Microseconds since Unix epoch (exchange event time)
    pub data: String,                  // JSON payload
}

/// Shared state for WebSocket connection status
/// Key: "exchange:symbol" (e.g., "binance:btcusdt"), Value: connected (true/false)
pub type ConnectionState = Arc<RwLock<HashMap<String, bool>>>;

/// Create a new connection state instance
pub fn new_connection_state() -> ConnectionState {
    Arc::new(RwLock::new(HashMap::new()))
}
