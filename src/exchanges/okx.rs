//! OKX exchange implementation.
//!
//! WebSocket documentation: https://www.okx.com/docs-v5/en/#order-book-trading-ws

use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use super::{Exchange, ExchangeError, ExchangeMessage, FeedType};

/// OKX orderbook channel types.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum OkxBookChannel {
    /// 400 levels, 100ms updates
    Books,
    /// 5 levels, snapshot only
    Books5,
    /// 50 levels, tick-by-tick (10ms)
    Books50L2Tbt,
    /// 400 levels, tick-by-tick (10ms)
    BooksL2Tbt,
}

impl OkxBookChannel {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Books => "books",
            Self::Books5 => "books5",
            Self::Books50L2Tbt => "books50-l2-tbt",
            Self::BooksL2Tbt => "books-l2-tbt",
        }
    }
}

/// OKX exchange connector.
pub struct Okx {
    /// Orderbook channel to use
    book_channel: OkxBookChannel,
}

impl Okx {
    /// Creates a new OKX exchange with default settings.
    pub fn new() -> Self {
        Self {
            book_channel: OkxBookChannel::Books,
        }
    }

    /// Creates an OKX exchange with a specific book channel.
    #[allow(dead_code)]
    pub fn with_book_channel(book_channel: OkxBookChannel) -> Self {
        Self { book_channel }
    }
}

impl Default for Okx {
    fn default() -> Self {
        Self::new()
    }
}

impl Exchange for Okx {
    fn name(&self) -> &'static str {
        "okx"
    }

    fn websocket_url(&self, _symbol: &str) -> String {
        // OKX public WebSocket endpoint
        "wss://ws.okx.com:8443/ws/v5/public".to_string()
    }

    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String> {
        // OKX expects uppercase with dashes: "BTC-USDT"
        // Note: normalize_symbol() is for storage/logging only, not API calls
        let api_symbol = symbol.to_uppercase();
        let mut args: Vec<Value> = Vec::new();

        for feed in feeds {
            match feed {
                FeedType::Orderbook => {
                    args.push(serde_json::json!({
                        "channel": self.book_channel.as_str(),
                        "instId": api_symbol
                    }));
                }
                FeedType::Trades => {
                    args.push(serde_json::json!({
                        "channel": "trades",
                        "instId": api_symbol
                    }));
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
        // OKX responds to "ping" with literal text "pong"
        if msg == "pong" {
            return Ok(ExchangeMessage::Pong);
        }

        let json: Value =
            serde_json::from_str(msg).map_err(|e| ExchangeError::Parse(e.to_string()))?;

        // Check for event messages (subscription confirmations, notices, errors)
        if let Some(event) = json.get("event").and_then(|v| v.as_str()) {
            // Handle notice events (e.g., upcoming disconnection for service upgrade)
            // OKX sends these 30-60 seconds before disconnecting
            if event == "notice" {
                let code = json.get("code").and_then(|v| v.as_str()).unwrap_or("unknown");
                let msg_text = json.get("msg").and_then(|v| v.as_str()).unwrap_or("");
                tracing::warn!(
                    code = code,
                    message = msg_text,
                    "OKX notice: connection may be disconnected soon"
                );
            }
            return Ok(ExchangeMessage::Other(msg.to_string()));
        }

        // Get channel from arg
        let channel = json
            .get("arg")
            .and_then(|arg| arg.get("channel"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let inst_id = json
            .get("arg")
            .and_then(|arg| arg.get("instId"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        // Orderbook channels
        if channel.starts_with("books") {
            let data = json.get("data").and_then(|d| d.get(0));
            let sequence_id = data
                .and_then(|d| d.get("seqId"))
                .map(|v| v.to_string())
                .or_else(|| {
                    data.and_then(|d| d.get("ts"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| "0".to_string());
            // ts = timestamp in milliseconds (as string), convert to μs
            let timestamp_exchange_us = data
                .and_then(|d| d.get("ts"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0)
                * 1000;

            return Ok(ExchangeMessage::Orderbook {
                symbol: inst_id,
                sequence_id,
                timestamp_exchange_us,
                data: msg.to_string(),
            });
        }

        // Trades channel
        if channel == "trades" {
            let data = json.get("data").and_then(|d| d.get(0));
            let sequence_id = data
                .and_then(|d| d.get("tradeId"))
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string();
            // ts = timestamp in milliseconds (as string), convert to μs
            let timestamp_exchange_us = data
                .and_then(|d| d.get("ts"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0)
                * 1000;

            return Ok(ExchangeMessage::Trade {
                symbol: inst_id,
                sequence_id,
                timestamp_exchange_us,
                data: msg.to_string(),
            });
        }

        Ok(ExchangeMessage::Other(msg.to_string()))
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // Normalize to lowercase without separators for consistent storage/logging
        symbol.to_lowercase().replace(['-', '_', '/'], "")
    }

    fn build_ping_message(&self) -> Option<Message> {
        // OKX requires client-initiated text "ping" messages
        // Server responds with text "pong"
        // Connection times out after 30 seconds without client pings
        Some(Message::Text("ping".into()))
    }

    fn ping_interval(&self) -> Option<std::time::Duration> {
        // OKX connection times out after 30 seconds without client pings
        // Send ping every 20 seconds for safety margin
        Some(std::time::Duration::from_secs(20))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_symbol() {
        let okx = Okx::new();
        assert_eq!(okx.normalize_symbol("btcusdt"), "btcusdt");
        assert_eq!(okx.normalize_symbol("BTC-USDT"), "btcusdt");
        assert_eq!(okx.normalize_symbol("ETHBTC"), "ethbtc");
    }

    #[test]
    fn test_parse_orderbook() {
        let okx = Okx::new();
        let msg = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["41006.8","0.60038921","0","1"]],"bids":[["41006.3","0.30178218","0","2"]],"ts":"1672515782136","seqId":123456789}]}"#;
        let result = okx.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook {
                symbol,
                sequence_id,
                timestamp_exchange_us,
                ..
            } => {
                assert_eq!(symbol, "BTC-USDT");
                assert_eq!(sequence_id, "123456789");
                assert_eq!(timestamp_exchange_us, 1672515782136000); // microseconds
            }
            _ => panic!("Expected Orderbook message"),
        }
    }

    #[test]
    fn test_parse_trade() {
        let okx = Okx::new();
        let msg = r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"130639474","px":"42219.9","sz":"0.12060306","side":"buy","ts":"1672515782136"}]}"#;
        let result = okx.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Trade {
                symbol,
                sequence_id,
                timestamp_exchange_us,
                ..
            } => {
                assert_eq!(symbol, "BTC-USDT");
                assert_eq!(sequence_id, "130639474");
                assert_eq!(timestamp_exchange_us, 1672515782136000); // microseconds
            }
            _ => panic!("Expected Trade message"),
        }
    }
}
