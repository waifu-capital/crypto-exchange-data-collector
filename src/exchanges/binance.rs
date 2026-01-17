//! Binance exchange implementation.
//!
//! WebSocket documentation: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

use serde_json::Value;

use super::{Exchange, ExchangeError, ExchangeMessage, FeedType};

/// Binance exchange connector.
pub struct Binance {
    /// Depth levels for orderbook (5, 10, or 20)
    depth_levels: u8,
    /// Update speed in milliseconds (100 or 1000)
    update_speed_ms: u16,
}

impl Binance {
    /// Creates a new Binance exchange with default settings.
    ///
    /// Defaults: 20 depth levels, 100ms updates
    pub fn new() -> Self {
        Self {
            depth_levels: 20,
            update_speed_ms: 100,
        }
    }

    /// Creates a Binance exchange with custom depth and speed.
    pub fn with_config(depth_levels: u8, update_speed_ms: u16) -> Self {
        Self {
            depth_levels,
            update_speed_ms,
        }
    }
}

impl Default for Binance {
    fn default() -> Self {
        Self::new()
    }
}

impl Exchange for Binance {
    fn name(&self) -> &'static str {
        "binance"
    }

    fn websocket_url(&self, symbol: &str) -> String {
        // Binance uses URL-based subscriptions for simple cases
        // For multiple streams, use the combined stream endpoint
        format!(
            "wss://stream.binance.com:9443/ws/{}@depth{}@{}ms",
            symbol.to_lowercase(),
            self.depth_levels,
            self.update_speed_ms
        )
    }

    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String> {
        // Build stream names for each feed type
        let streams: Vec<String> = feeds
            .iter()
            .map(|feed| match feed {
                FeedType::Orderbook => format!(
                    "{}@depth{}@{}ms",
                    symbol.to_lowercase(),
                    self.depth_levels,
                    self.update_speed_ms
                ),
                FeedType::Trades => format!("{}@trade", symbol.to_lowercase()),
            })
            .collect();

        if streams.is_empty() {
            return vec![];
        }

        // Single subscription message for all streams
        vec![serde_json::json!({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        })
        .to_string()]
    }

    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError> {
        let json: Value =
            serde_json::from_str(msg).map_err(|e| ExchangeError::Parse(e.to_string()))?;

        // Check for subscription confirmation
        if json.get("result").is_some() || json.get("id").is_some() {
            return Ok(ExchangeMessage::Other(msg.to_string()));
        }

        // Check for error response
        if let Some(error) = json.get("error") {
            return Err(ExchangeError::Parse(format!("Binance error: {}", error)));
        }

        // Orderbook depth update (partial book)
        if json.get("lastUpdateId").is_some() {
            let symbol = json
                .get("s")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let sequence_id = json["lastUpdateId"].to_string();
            // E = event time in milliseconds
            let timestamp_exchange = json
                .get("E")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            return Ok(ExchangeMessage::Orderbook {
                symbol,
                sequence_id,
                timestamp_exchange,
                data: msg.to_string(),
            });
        }

        // Trade event
        if json.get("e").and_then(|v| v.as_str()) == Some("trade") {
            let symbol = json
                .get("s")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let sequence_id = json
                .get("t")
                .map(|v| v.to_string())
                .unwrap_or_else(|| "0".to_string());
            // E = event time, T = trade time (use E for consistency)
            let timestamp_exchange = json
                .get("E")
                .and_then(|v| v.as_i64())
                .or_else(|| json.get("T").and_then(|v| v.as_i64()))
                .unwrap_or(0);

            return Ok(ExchangeMessage::Trade {
                symbol,
                sequence_id,
                timestamp_exchange,
                data: msg.to_string(),
            });
        }

        // Aggregate trade event
        if json.get("e").and_then(|v| v.as_str()) == Some("aggTrade") {
            let symbol = json
                .get("s")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let sequence_id = json
                .get("a")
                .map(|v| v.to_string())
                .unwrap_or_else(|| "0".to_string());
            // E = event time, T = trade time
            let timestamp_exchange = json
                .get("E")
                .and_then(|v| v.as_i64())
                .or_else(|| json.get("T").and_then(|v| v.as_i64()))
                .unwrap_or(0);

            return Ok(ExchangeMessage::Trade {
                symbol,
                sequence_id,
                timestamp_exchange,
                data: msg.to_string(),
            });
        }

        Ok(ExchangeMessage::Other(msg.to_string()))
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // Binance uses lowercase without separators: btcusdt
        symbol.to_lowercase().replace(['-', '_', '/'], "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_url() {
        let binance = Binance::new();
        let url = binance.websocket_url("btcusdt");
        assert!(url.contains("btcusdt@depth20@100ms"));
    }

    #[test]
    fn test_normalize_symbol() {
        let binance = Binance::new();
        assert_eq!(binance.normalize_symbol("BTC-USDT"), "btcusdt");
        assert_eq!(binance.normalize_symbol("ETH/USD"), "ethusd");
    }

    #[test]
    fn test_parse_orderbook() {
        let binance = Binance::new();
        let msg = r#"{"lastUpdateId":160,"E":1672515782136,"bids":[["0.0024","10"]],"asks":[["0.0026","100"]]}"#;
        let result = binance.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook { sequence_id, timestamp_exchange, .. } => {
                assert_eq!(sequence_id, "160");
                assert_eq!(timestamp_exchange, 1672515782136);
            }
            _ => panic!("Expected Orderbook message"),
        }
    }

    #[test]
    fn test_parse_trade() {
        let binance = Binance::new();
        let msg = r#"{"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,"p":"0.001","q":"100","T":1672515782136,"m":true}"#;
        let result = binance.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Trade {
                symbol,
                sequence_id,
                timestamp_exchange,
                ..
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(sequence_id, "12345");
                assert_eq!(timestamp_exchange, 1672515782136);
            }
            _ => panic!("Expected Trade message"),
        }
    }
}
