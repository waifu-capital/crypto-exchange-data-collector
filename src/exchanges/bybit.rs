//! Bybit exchange implementation.
//!
//! WebSocket documentation: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook

use serde_json::Value;

use super::{Exchange, ExchangeError, ExchangeMessage, FeedType};

/// Bybit market type.
#[derive(Debug, Clone, Copy)]
pub enum BybitMarket {
    /// Spot market
    Spot,
    /// Linear perpetual (USDT-margined)
    Linear,
    /// Inverse perpetual (coin-margined)
    Inverse,
}

impl BybitMarket {
    fn endpoint(&self) -> &'static str {
        match self {
            Self::Spot => "wss://stream.bybit.com/v5/public/spot",
            Self::Linear => "wss://stream.bybit.com/v5/public/linear",
            Self::Inverse => "wss://stream.bybit.com/v5/public/inverse",
        }
    }
}

/// Bybit orderbook depth levels.
#[derive(Debug, Clone, Copy)]
pub enum BybitDepth {
    /// 1 level, 10ms updates
    L1 = 1,
    /// 50 levels, 20ms updates
    L50 = 50,
    /// 200 levels, 100-200ms updates
    L200 = 200,
    /// 1000 levels, 200ms updates (not available for spot)
    L1000 = 1000,
}

/// Bybit exchange connector.
pub struct Bybit {
    /// Market type
    market: BybitMarket,
    /// Orderbook depth
    depth: BybitDepth,
}

impl Bybit {
    /// Creates a new Bybit exchange with default settings (spot, 50 levels).
    pub fn new() -> Self {
        Self {
            market: BybitMarket::Spot,
            depth: BybitDepth::L50,
        }
    }

    /// Creates a Bybit exchange with custom settings.
    pub fn with_config(market: BybitMarket, depth: BybitDepth) -> Self {
        Self { market, depth }
    }
}

impl Default for Bybit {
    fn default() -> Self {
        Self::new()
    }
}

impl Exchange for Bybit {
    fn name(&self) -> &'static str {
        "bybit"
    }

    fn websocket_url(&self, _symbol: &str) -> String {
        self.market.endpoint().to_string()
    }

    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String> {
        let normalized = self.normalize_symbol(symbol);
        let mut args: Vec<String> = Vec::new();

        for feed in feeds {
            match feed {
                FeedType::Orderbook => {
                    args.push(format!("orderbook.{}.{}", self.depth as u16, normalized));
                }
                FeedType::Trades => {
                    args.push(format!("publicTrade.{}", normalized));
                }
            }
        }

        if args.is_empty() {
            return vec![];
        }

        vec![serde_json::json!({
            "op": "subscribe",
            "args": args
        })
        .to_string()]
    }

    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError> {
        let json: Value =
            serde_json::from_str(msg).map_err(|e| ExchangeError::Parse(e.to_string()))?;

        // Check for subscription confirmation or error
        if json.get("success").is_some() || json.get("ret_msg").is_some() {
            return Ok(ExchangeMessage::Other(msg.to_string()));
        }

        // Get topic to determine message type
        let topic = json
            .get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Orderbook messages
        if topic.starts_with("orderbook.") {
            let data = json.get("data");
            let symbol = data
                .and_then(|d| d.get("s"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let sequence_id = data
                .and_then(|d| d.get("u"))
                .map(|v| v.to_string())
                .or_else(|| {
                    data.and_then(|d| d.get("seq"))
                        .map(|v| v.to_string())
                })
                .unwrap_or_else(|| {
                    json.get("ts")
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "0".to_string())
                });

            return Ok(ExchangeMessage::Orderbook {
                symbol,
                sequence_id,
                data: msg.to_string(),
            });
        }

        // Trade messages
        if topic.starts_with("publicTrade.") {
            let data = json.get("data").and_then(|d| d.get(0));
            let symbol = data
                .and_then(|d| d.get("s"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let sequence_id = data
                .and_then(|d| d.get("i"))
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string();

            return Ok(ExchangeMessage::Trade {
                symbol,
                sequence_id,
                data: msg.to_string(),
            });
        }

        // Pong response
        if json.get("op").and_then(|v| v.as_str()) == Some("pong") {
            return Ok(ExchangeMessage::Pong);
        }

        Ok(ExchangeMessage::Other(msg.to_string()))
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // Bybit uses uppercase without separator: BTCUSDT
        symbol
            .to_uppercase()
            .replace(['-', '_', '/'], "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_symbol() {
        let bybit = Bybit::new();
        assert_eq!(bybit.normalize_symbol("btc-usdt"), "BTCUSDT");
        assert_eq!(bybit.normalize_symbol("BTC_USDT"), "BTCUSDT");
        assert_eq!(bybit.normalize_symbol("BTCUSDT"), "BTCUSDT");
    }

    #[test]
    fn test_parse_orderbook() {
        let bybit = Bybit::new();
        let msg = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1672515782136,"data":{"s":"BTCUSDT","b":[["41006.3","0.30178218"]],"a":[["41006.8","0.60038921"]],"u":123456789,"seq":987654321}}"#;
        let result = bybit.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook {
                symbol,
                sequence_id,
                ..
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(sequence_id, "123456789");
            }
            _ => panic!("Expected Orderbook message"),
        }
    }

    #[test]
    fn test_parse_trade() {
        let bybit = Bybit::new();
        let msg = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672515782136,"data":[{"i":"2100000000007764175","T":1672515782136,"p":"16578.50","v":"0.001","S":"Buy","s":"BTCUSDT","BT":false}]}"#;
        let result = bybit.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Trade {
                symbol,
                sequence_id,
                ..
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(sequence_id, "2100000000007764175");
            }
            _ => panic!("Expected Trade message"),
        }
    }
}
