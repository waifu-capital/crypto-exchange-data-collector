//! Coinbase exchange implementation.
//!
//! WebSocket documentation: https://docs.cdp.coinbase.com/exchange/websocket-feed/overview

use serde_json::Value;

use super::{Exchange, ExchangeError, ExchangeMessage, FeedType};

/// Coinbase exchange connector.
pub struct Coinbase {
    /// Use batched level2 updates (50ms) instead of real-time
    use_batched: bool,
}

impl Coinbase {
    /// Creates a new Coinbase exchange with default settings.
    pub fn new() -> Self {
        Self { use_batched: false }
    }

    /// Creates a Coinbase exchange with batched updates.
    pub fn with_batched(use_batched: bool) -> Self {
        Self { use_batched }
    }
}

impl Default for Coinbase {
    fn default() -> Self {
        Self::new()
    }
}

impl Exchange for Coinbase {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    fn websocket_url(&self, _symbol: &str) -> String {
        // Coinbase uses a single endpoint; symbols are specified in subscription
        "wss://ws-feed.exchange.coinbase.com".to_string()
    }

    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String> {
        let mut channels: Vec<Value> = Vec::new();

        for feed in feeds {
            match feed {
                FeedType::Orderbook => {
                    let channel = if self.use_batched {
                        "level2_batch"
                    } else {
                        "level2"
                    };
                    channels.push(Value::String(channel.to_string()));
                }
                FeedType::Trades => {
                    channels.push(Value::String("matches".to_string()));
                }
            }
        }

        if channels.is_empty() {
            return vec![];
        }

        let normalized = self.normalize_symbol(symbol);
        vec![serde_json::json!({
            "type": "subscribe",
            "product_ids": [normalized],
            "channels": channels
        })
        .to_string()]
    }

    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError> {
        let json: Value =
            serde_json::from_str(msg).map_err(|e| ExchangeError::Parse(e.to_string()))?;

        let msg_type = json
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match msg_type {
            // Orderbook snapshot
            "snapshot" => {
                let symbol = json
                    .get("product_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                // Snapshot doesn't have sequence, use timestamp
                let sequence_id = json
                    .get("time")
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .to_string();

                Ok(ExchangeMessage::Orderbook {
                    symbol,
                    sequence_id,
                    data: msg.to_string(),
                })
            }

            // Orderbook update
            "l2update" => {
                let symbol = json
                    .get("product_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let sequence_id = json
                    .get("time")
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .to_string();

                Ok(ExchangeMessage::Orderbook {
                    symbol,
                    sequence_id,
                    data: msg.to_string(),
                })
            }

            // Trade match
            "match" | "last_match" => {
                let symbol = json
                    .get("product_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let sequence_id = json
                    .get("sequence")
                    .map(|v| v.to_string())
                    .or_else(|| json.get("trade_id").map(|v| v.to_string()))
                    .unwrap_or_else(|| "0".to_string());

                Ok(ExchangeMessage::Trade {
                    symbol,
                    sequence_id,
                    data: msg.to_string(),
                })
            }

            // Subscription confirmations and other messages
            "subscriptions" | "error" | "heartbeat" => {
                Ok(ExchangeMessage::Other(msg.to_string()))
            }

            _ => Ok(ExchangeMessage::Other(msg.to_string())),
        }
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // Coinbase uses uppercase with hyphen: BTC-USD
        let cleaned = symbol.to_uppercase().replace(['_', '/'], "-");
        // Ensure there's a hyphen between base and quote
        if !cleaned.contains('-') && cleaned.len() >= 6 {
            // Try to split at common positions (3 or 4 char base)
            if cleaned.ends_with("USD") || cleaned.ends_with("EUR") || cleaned.ends_with("GBP") {
                let (base, quote) = cleaned.split_at(cleaned.len() - 3);
                return format!("{}-{}", base, quote);
            }
            if cleaned.ends_with("USDT") || cleaned.ends_with("USDC") {
                let (base, quote) = cleaned.split_at(cleaned.len() - 4);
                return format!("{}-{}", base, quote);
            }
        }
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_symbol() {
        let coinbase = Coinbase::new();
        assert_eq!(coinbase.normalize_symbol("btcusd"), "BTC-USD");
        assert_eq!(coinbase.normalize_symbol("BTC-USD"), "BTC-USD");
        assert_eq!(coinbase.normalize_symbol("eth_eur"), "ETH-EUR");
    }

    #[test]
    fn test_parse_snapshot() {
        let coinbase = Coinbase::new();
        let msg = r#"{"type":"snapshot","product_id":"BTC-USD","bids":[["10101.10","0.45054140"]],"asks":[["10102.55","0.57753524"]]}"#;
        let result = coinbase.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook { symbol, .. } => {
                assert_eq!(symbol, "BTC-USD");
            }
            _ => panic!("Expected Orderbook message"),
        }
    }

    #[test]
    fn test_parse_match() {
        let coinbase = Coinbase::new();
        let msg = r#"{"type":"match","trade_id":10,"sequence":50,"product_id":"BTC-USD","size":"5.23512","price":"400.23","side":"sell"}"#;
        let result = coinbase.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Trade {
                symbol,
                sequence_id,
                ..
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(sequence_id, "50");
            }
            _ => panic!("Expected Trade message"),
        }
    }
}
