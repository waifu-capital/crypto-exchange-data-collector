//! Upbit exchange implementation.
//!
//! WebSocket documentation: https://docs.upbit.com/reference/websocket-orderbook

use serde_json::Value;
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
        let normalized = self.normalize_symbol(symbol);
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
                {"type": type_name, "codes": [normalized]},
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

                Ok(ExchangeMessage::Orderbook {
                    symbol,
                    sequence_id,
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

                Ok(ExchangeMessage::Trade {
                    symbol,
                    sequence_id,
                    data: msg.to_string(),
                })
            }

            _ => Ok(ExchangeMessage::Other(msg.to_string())),
        }
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // Upbit uses QUOTE-BASE format (e.g., KRW-BTC), uppercase with hyphen
        // Quote priority: KRW > USDT > BTC > ETH (KRW is always quote if present)
        let upper = symbol.to_uppercase();

        // Helper to get quote priority (lower = higher priority as quote)
        let quote_priority = |s: &str| -> Option<usize> {
            ["KRW", "USDT", "BTC", "ETH"]
                .iter()
                .position(|&q| q == s)
        };

        // If already has hyphen, check if it's in correct order
        if upper.contains('-') {
            let parts: Vec<&str> = upper.split('-').collect();
            if parts.len() == 2 {
                let p0 = quote_priority(parts[0]);
                let p1 = quote_priority(parts[1]);

                // If second part has higher quote priority, swap
                if let (Some(pri0), Some(pri1)) = (p0, p1) {
                    if pri1 < pri0 {
                        return format!("{}-{}", parts[1], parts[0]);
                    }
                } else if p1.is_some() && p0.is_none() {
                    // Second is a quote, first is not - swap
                    return format!("{}-{}", parts[1], parts[0]);
                }
            }
            return upper;
        }

        // Try to detect and add hyphen
        // Common patterns: KRWBTC -> KRW-BTC
        let prefixes = ["KRW", "USDT", "BTC", "ETH"];
        for prefix in prefixes {
            if upper.starts_with(prefix) && upper.len() > prefix.len() {
                return format!("{}-{}", prefix, &upper[prefix.len()..]);
            }
        }

        upper
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_symbol() {
        let upbit = Upbit::new();
        assert_eq!(upbit.normalize_symbol("KRW-BTC"), "KRW-BTC");
        assert_eq!(upbit.normalize_symbol("krw-btc"), "KRW-BTC");
        assert_eq!(upbit.normalize_symbol("BTC-KRW"), "KRW-BTC");
    }

    #[test]
    fn test_parse_orderbook() {
        let upbit = Upbit::new();
        let msg = r#"{"ty":"orderbook","cd":"KRW-BTC","tms":1672515782136,"tas":12.345,"tbs":23.456,"obu":[{"ap":50000000,"as":0.1,"bp":49999000,"bs":0.2}]}"#;
        let result = upbit.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook { symbol, .. } => {
                assert_eq!(symbol, "KRW-BTC");
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
                ..
            } => {
                assert_eq!(symbol, "KRW-BTC");
                assert_eq!(sequence_id, "12345");
            }
            _ => panic!("Expected Trade message"),
        }
    }
}
