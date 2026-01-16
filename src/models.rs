/// Data structure for snapshot messages sent through the channel
pub struct SnapshotData {
    pub exchange: String,              // "binance", "coinbase", "bybit", etc.
    pub symbol: String,                // "btcusdt", "BTC-USD", etc.
    pub data_type: String,             // "orderbook", "trade"
    pub exchange_sequence_id: String,  // Exchange-specific ID for deduplication
    pub timestamp: i64,                // Microseconds since Unix epoch (our receipt time)
    pub data: String,                  // JSON payload
}
