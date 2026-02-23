//! Chainlink RTDS (Real-Time Data Socket) price feed via Polymarket.
//!
//! Streams Chainlink Data Streams oracle prices for crypto assets via
//! Polymarket's unauthenticated WebSocket relay.
//!
//! Protocol details:
//! - Endpoint: wss://ws-live-data.polymarket.com (unauthenticated)
//! - Keepalive: application-level text "ping" every 5 seconds (NOT WebSocket control frames)
//! - Subscribe with topic "crypto_prices_chainlink" and symbol filter (double-encoded JSON)
//! - Only process messages where type == "update" (ignore "historical", "pong", etc.)
//!
//! Known issue: the RTDS stream freezes after ~20 minutes while the WebSocket
//! connection stays alive (ping/pong still works). Use a short data_timeout_secs
//! (e.g., 10s) in config to force reconnection on staleness.
//!
//! Reference implementation: poly-trade/crates/poly-collect/src/rtds.rs

use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use super::{Exchange, ExchangeError, ExchangeMessage, FeedType};

const DEFAULT_WS_URL: &str = "wss://ws-live-data.polymarket.com";

/// Chainlink RTDS price feed connector via Polymarket.
pub struct Chainlink {
    /// WebSocket endpoint URL
    base_url: String,
}

impl Chainlink {
    /// Creates a new Chainlink connector with default RTDS endpoint.
    pub fn new() -> Self {
        Self {
            base_url: DEFAULT_WS_URL.to_string(),
        }
    }

    /// Creates a Chainlink connector with a custom WebSocket URL.
    #[allow(dead_code)]
    pub fn with_base_url(url: String) -> Self {
        Self { base_url: url }
    }
}

impl Default for Chainlink {
    fn default() -> Self {
        Self::new()
    }
}

impl Exchange for Chainlink {
    fn name(&self) -> &'static str {
        "chainlink"
    }

    fn websocket_url(&self, _symbol: &str) -> String {
        // RTDS uses a single endpoint for all symbols; subscription is via messages.
        self.base_url.clone()
    }

    fn build_subscribe_messages(&self, symbol: &str, _feeds: &[FeedType]) -> Vec<String> {
        // RTDS subscription format uses double-encoded JSON in the filters field.
        // The filters value is a JSON string nested inside the outer JSON object.
        //
        // Example for btc/usd:
        // {"action":"subscribe","subscriptions":[{
        //   "topic":"crypto_prices_chainlink",
        //   "type":"*",
        //   "filters":"{\"symbol\":\"btc/usd\"}"
        // }]}
        let filters = format!("{{\"symbol\":\"{}\"}}", symbol.to_lowercase());

        let msg = serde_json::json!({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": filters,
            }]
        });

        vec![msg.to_string()]
    }

    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError> {
        // RTDS responds to "ping" with literal text "pong"
        if msg == "pong" {
            return Ok(ExchangeMessage::Pong);
        }

        // Quick-reject messages without "payload" to avoid unnecessary JSON parsing
        if !msg.contains("payload") {
            return Ok(ExchangeMessage::Other(msg.to_string()));
        }

        let json: Value =
            serde_json::from_str(msg).map_err(|e| ExchangeError::Parse(e.to_string()))?;

        // Only process "update" messages; ignore "historical", subscription confirmations, etc.
        let msg_type = json.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if msg_type != "update" {
            return Ok(ExchangeMessage::Other(msg.to_string()));
        }

        let payload = json
            .get("payload")
            .ok_or_else(|| ExchangeError::Parse("missing payload field".to_string()))?;

        // Extract symbol from payload (e.g., "btc/usd")
        let symbol = payload
            .get("symbol")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        // payload.timestamp = Chainlink oracle observation time in milliseconds
        // Convert to microseconds for consistency with other exchanges
        let payload_ts_ms = payload
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let timestamp_exchange_us = (payload_ts_ms as i64) * 1000;

        // Use the envelope timestamp as sequence_id for ordering
        let sequence_id = json
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .map(|t| t.to_string())
            .unwrap_or_else(|| "0".to_string());

        Ok(ExchangeMessage::Price {
            symbol,
            sequence_id,
            timestamp_exchange_us,
            data: msg.to_string(),
        })
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // "btc/usd" -> "btcusd", "ETH/USD" -> "ethusd"
        // Default implementation already handles '/' removal
        symbol.to_lowercase().replace(['-', '_', '/'], "")
    }

    fn build_ping_message(&self) -> Option<Message> {
        // RTDS requires application-level text "ping" messages (same pattern as OKX).
        // Server responds with text "pong".
        Some(Message::Text("ping".into()))
    }

    fn ping_interval(&self) -> Option<std::time::Duration> {
        // RTDS expects pings every 5 seconds to keep the connection alive.
        // This is more aggressive than OKX (15s) but matches the poly-trade reference.
        Some(std::time::Duration::from_secs(5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        let cl = Chainlink::new();
        assert_eq!(cl.name(), "chainlink");
    }

    #[test]
    fn test_websocket_url_ignores_symbol() {
        let cl = Chainlink::new();
        assert_eq!(cl.websocket_url("btc/usd"), DEFAULT_WS_URL);
        assert_eq!(cl.websocket_url("eth/usd"), DEFAULT_WS_URL);
    }

    #[test]
    fn test_websocket_url_custom() {
        let cl = Chainlink::with_base_url("wss://custom.example.com".to_string());
        assert_eq!(cl.websocket_url("btc/usd"), "wss://custom.example.com");
    }

    #[test]
    fn test_subscribe_message_format() {
        let cl = Chainlink::new();
        let msgs = cl.build_subscribe_messages("btc/usd", &[FeedType::Price]);

        assert_eq!(msgs.len(), 1);
        let json: Value = serde_json::from_str(&msgs[0]).unwrap();

        assert_eq!(json["action"], "subscribe");

        let sub = &json["subscriptions"][0];
        assert_eq!(sub["topic"], "crypto_prices_chainlink");
        assert_eq!(sub["type"], "*");

        // Verify the double-encoded filters field
        let filters_str = sub["filters"].as_str().unwrap();
        let filters: Value = serde_json::from_str(filters_str).unwrap();
        assert_eq!(filters["symbol"], "btc/usd");
    }

    #[test]
    fn test_subscribe_message_lowercase() {
        let cl = Chainlink::new();
        let msgs = cl.build_subscribe_messages("ETH/USD", &[FeedType::Price]);

        let json: Value = serde_json::from_str(&msgs[0]).unwrap();
        let filters_str = json["subscriptions"][0]["filters"].as_str().unwrap();
        let filters: Value = serde_json::from_str(filters_str).unwrap();
        assert_eq!(filters["symbol"], "eth/usd");
    }

    #[test]
    fn test_parse_price_update() {
        let cl = Chainlink::new();
        let msg = r#"{
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": 1753314064237,
            "connection_id": "abc",
            "payload": {
                "symbol": "btc/usd",
                "timestamp": 1753314064213,
                "value": 67234.50
            }
        }"#;

        let result = cl.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Price {
                symbol,
                sequence_id,
                timestamp_exchange_us,
                data,
            } => {
                assert_eq!(symbol, "btc/usd");
                // Envelope timestamp as sequence_id
                assert_eq!(sequence_id, "1753314064237");
                // payload.timestamp (ms) * 1000 = microseconds
                assert_eq!(timestamp_exchange_us, 1753314064213 * 1000);
                // Raw JSON is preserved
                assert!(data.contains("67234.50") || data.contains("67234.5"));
            }
            _ => panic!("Expected Price message"),
        }
    }

    #[test]
    fn test_parse_pong() {
        let cl = Chainlink::new();
        let result = cl.parse_message("pong").unwrap();
        assert!(matches!(result, ExchangeMessage::Pong));
    }

    #[test]
    fn test_parse_historical_ignored() {
        let cl = Chainlink::new();
        let msg = r#"{
            "topic": "crypto_prices_chainlink",
            "type": "historical",
            "timestamp": 1753314064237,
            "payload": {
                "symbol": "btc/usd",
                "timestamp": 1753314064213,
                "value": 67234.50
            }
        }"#;

        let result = cl.parse_message(msg).unwrap();
        assert!(matches!(result, ExchangeMessage::Other(_)));
    }

    #[test]
    fn test_parse_subscription_confirmation_ignored() {
        let cl = Chainlink::new();
        let msg = r#"{"type": "subscribed", "channel": "crypto_prices_chainlink"}"#;

        let result = cl.parse_message(msg).unwrap();
        assert!(matches!(result, ExchangeMessage::Other(_)));
    }

    #[test]
    fn test_parse_no_payload_ignored() {
        let cl = Chainlink::new();
        let msg = r#"{"type": "heartbeat", "timestamp": 123}"#;

        let result = cl.parse_message(msg).unwrap();
        assert!(matches!(result, ExchangeMessage::Other(_)));
    }

    #[test]
    fn test_normalize_symbol() {
        let cl = Chainlink::new();
        assert_eq!(cl.normalize_symbol("btc/usd"), "btcusd");
        assert_eq!(cl.normalize_symbol("ETH/USD"), "ethusd");
        assert_eq!(cl.normalize_symbol("sol/usd"), "solusd");
        assert_eq!(cl.normalize_symbol("XRP/USD"), "xrpusd");
    }

    #[test]
    fn test_ping_message() {
        let cl = Chainlink::new();
        let ping = cl.build_ping_message().unwrap();
        assert!(matches!(ping, Message::Text(_)));
    }

    #[test]
    fn test_ping_interval() {
        let cl = Chainlink::new();
        let interval = cl.ping_interval().unwrap();
        assert_eq!(interval, std::time::Duration::from_secs(5));
    }
}
