//! Upbit exchange implementation.
//!
//! WebSocket documentation: https://docs.upbit.com/reference/websocket-orderbook

use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use super::{Exchange, ExchangeError, ExchangeMessage, FeedType};

/// Upbit exchange connector.
pub struct Upbit;

impl Upbit {
    /// Creates a new Upbit exchange.
    pub fn new() -> Self {
        Self
    }
}

impl Default for Upbit {
    fn default() -> Self {
        Self::new()
    }
}

impl Exchange for Upbit {
    fn name(&self) -> &'static str {
        "upbit"
    }

    fn websocket_url(&self, _symbol: &str) -> String {
        // Upbit uses a single endpoint; symbols are specified in subscription
        "wss://api.upbit.com/websocket/v1".to_string()
    }

    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String> {
        // Upbit expects uppercase with dashes: "KRW-BTC"
        // Note: normalize_symbol() is for storage/logging only, not API calls
        let api_symbol = symbol.to_uppercase();
        let ticket = Uuid::new_v4().to_string();
        let mut messages = Vec::new();

        for feed in feeds {
            let type_name = match feed {
                FeedType::Orderbook => "orderbook",
                FeedType::Trades => "trade",
            };

            // Upbit uses array format for subscription
            // Note: "SIMPLE" format sends JSON text; "DEFAULT" sends binary
            let msg = serde_json::json!([
                {"ticket": ticket},
                {"type": type_name, "codes": [api_symbol]},
                {"format": "SIMPLE"}
            ]);
            messages.push(msg.to_string());
        }

        messages
    }

    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError> {
        let json: Value =
            serde_json::from_str(msg).map_err(|e| ExchangeError::Parse(e.to_string()))?;

        // Upbit uses abbreviated field names
        let msg_type = json
            .get("ty")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match msg_type {
            "orderbook" => {
                let symbol = json
                    .get("cd")
                    .or_else(|| json.get("code"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let sequence_id = json
                    .get("tms")
                    .or_else(|| json.get("timestamp"))
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "0".to_string());
                // tms = timestamp in milliseconds, convert to μs
                let timestamp_exchange_us = json
                    .get("tms")
                    .or_else(|| json.get("timestamp"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
                    * 1000;

                Ok(ExchangeMessage::Orderbook {
                    symbol,
                    sequence_id,
                    timestamp_exchange_us,
                    data: msg.to_string(),
                })
            }

            "trade" => {
                let symbol = json
                    .get("cd")
                    .or_else(|| json.get("code"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let sequence_id = json
                    .get("sid")
                    .or_else(|| json.get("sequential_id"))
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| {
                        json.get("ttms")
                            .or_else(|| json.get("trade_timestamp"))
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "0".to_string())
                    });
                // ttms = trade timestamp in milliseconds, convert to μs
                let timestamp_exchange_us = json
                    .get("ttms")
                    .or_else(|| json.get("trade_timestamp"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
                    * 1000;

                Ok(ExchangeMessage::Trade {
                    symbol,
                    sequence_id,
                    timestamp_exchange_us,
                    data: msg.to_string(),
                })
            }

            _ => Ok(ExchangeMessage::Other(msg.to_string())),
        }
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // Normalize to lowercase without separators for consistent storage/logging
        symbol.to_lowercase().replace(['-', '_', '/'], "")
    }

    fn build_ping_message(&self) -> Option<Message> {
        // Upbit ping/pong requirements not documented
        // Currently disabled in config, return None for now
        None
    }

    fn ping_interval(&self) -> Option<std::time::Duration> {
        // Upbit ping/pong requirements not documented
        // Server likely initiates pings - no client ping timer needed
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_symbol() {
        let upbit = Upbit::new();
        assert_eq!(upbit.normalize_symbol("KRW-BTC"), "krwbtc");
        assert_eq!(upbit.normalize_symbol("krw-btc"), "krwbtc");
        assert_eq!(upbit.normalize_symbol("BTC-KRW"), "btckrw");
    }

    #[test]
    fn test_parse_orderbook() {
        let upbit = Upbit::new();
        let msg = r#"{"ty":"orderbook","cd":"KRW-BTC","tms":1672515782136,"tas":12.345,"tbs":23.456,"obu":[{"ap":50000000,"as":0.1,"bp":49999000,"bs":0.2}]}"#;
        let result = upbit.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook { symbol, timestamp_exchange_us, .. } => {
                assert_eq!(symbol, "KRW-BTC");
                assert_eq!(timestamp_exchange_us, 1672515782136000); // microseconds
            }
            _ => panic!("Expected Orderbook message"),
        }
    }

    #[test]
    fn test_parse_trade() {
        let upbit = Upbit::new();
        let msg = r#"{"ty":"trade","cd":"KRW-BTC","tp":50000000,"tv":0.001,"ab":"BID","ttms":1672515782136,"sid":12345}"#;
        let result = upbit.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Trade {
                symbol,
                sequence_id,
                timestamp_exchange_us,
                ..
            } => {
                assert_eq!(symbol, "KRW-BTC");
                assert_eq!(sequence_id, "12345");
                assert_eq!(timestamp_exchange_us, 1672515782136000); // microseconds
            }
            _ => panic!("Expected Trade message"),
        }
    }
}
