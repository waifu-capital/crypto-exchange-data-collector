//! Coinbase exchange implementation.
//!
//! WebSocket documentation: https://docs.cdp.coinbase.com/advanced-trade/docs/ws-overview
//!
//! Authentication is required for level2 (orderbook) channel since August 2023.
//! The matches (trades) channel works without authentication.

use chrono::DateTime;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{Exchange, ExchangeError, ExchangeMessage, FeedType};

/// Parse ISO8601 timestamp string to microseconds since epoch
fn parse_iso8601_to_micros(time_str: Option<&str>) -> i64 {
    time_str
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_micros())
        .unwrap_or(0)
}

/// JWT claims for Coinbase authentication
#[derive(Debug, Serialize)]
struct CoinbaseClaims {
    iss: String,
    nbf: u64,
    exp: u64,
    sub: String,
}

/// Coinbase exchange connector.
pub struct Coinbase {
    /// API key for authenticated channels (format: organizations/{org_id}/apiKeys/{key_id})
    api_key: Option<String>,
    /// API secret (EC private key in PEM format)
    api_secret: Option<String>,
}

impl Coinbase {
    /// Creates a new Coinbase exchange without authentication.
    /// Only trades (matches) channel will work; orderbook (level2) requires auth.
    pub fn new() -> Self {
        Self {
            api_key: None,
            api_secret: None,
        }
    }

    /// Creates a Coinbase exchange with API credentials for authenticated channels.
    pub fn with_credentials(api_key: String, api_secret: String) -> Self {
        Self {
            api_key: Some(api_key),
            api_secret: Some(api_secret),
        }
    }

    /// Generate a JWT for Coinbase authentication
    fn generate_jwt(&self) -> Option<String> {
        let api_key = match self.api_key.as_ref() {
            Some(k) => k,
            None => {
                // No API key - this is expected if user hasn't configured auth
                return None;
            }
        };
        let api_secret = match self.api_secret.as_ref() {
            Some(s) => s,
            None => {
                tracing::warn!("COINBASE_API_KEY is set but COINBASE_API_SECRET is missing");
                return None;
            }
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_secs();

        let claims = CoinbaseClaims {
            iss: "cdp".to_string(),
            nbf: now,
            exp: now + 120, // JWT valid for 120 seconds
            sub: api_key.clone(),
        };

        // Parse the secret key - it should be in PEM format
        // Log the key structure for debugging (without revealing the actual secret)
        let line_count = api_secret.lines().count();
        let has_begin = api_secret.contains("-----BEGIN");
        let has_end = api_secret.contains("-----END");
        tracing::debug!(
            line_count = line_count,
            has_begin = has_begin,
            has_end = has_end,
            total_len = api_secret.len(),
            "Attempting to parse EC PEM key"
        );

        let encoding_key = match EncodingKey::from_ec_pem(api_secret.as_bytes()) {
            Ok(key) => key,
            Err(e) => {
                // Try PKCS#8 format as fallback (some keys use BEGIN PRIVATE KEY)
                tracing::warn!(
                    error = %e,
                    "EC PEM parse failed, this may be a key format issue"
                );
                tracing::error!(
                    error = %e,
                    secret_len = api_secret.len(),
                    line_count = line_count,
                    secret_starts_with = %api_secret.chars().take(31).collect::<String>(),
                    secret_ends_with = %api_secret.chars().rev().take(29).collect::<String>().chars().rev().collect::<String>(),
                    "Failed to parse Coinbase API secret as EC PEM key. Ensure the key is in SEC1 format (BEGIN EC PRIVATE KEY)"
                );
                return None;
            }
        };

        // Create header with kid (API key)
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(api_key.clone());

        // Encode the JWT
        match encode(&header, &claims, &encoding_key) {
            Ok(token) => Some(token),
            Err(e) => {
                tracing::error!(error = %e, "Failed to encode JWT");
                None
            }
        }
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
        // Use Advanced Trade WebSocket endpoint
        "wss://advanced-trade-ws.coinbase.com".to_string()
    }

    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String> {
        let mut messages = Vec::new();
        let product_id = symbol.to_uppercase();

        for feed in feeds {
            match feed {
                FeedType::Orderbook => {
                    // level2 requires authentication
                    if let Some(jwt) = self.generate_jwt() {
                        let msg = serde_json::json!({
                            "type": "subscribe",
                            "product_ids": [product_id],
                            "channel": "level2",
                            "jwt": jwt
                        });
                        messages.push(msg.to_string());
                    } else {
                        tracing::warn!(
                            "Coinbase level2 (orderbook) requires authentication. \
                             Set COINBASE_API_KEY and COINBASE_API_SECRET env vars. \
                             Skipping orderbook subscription."
                        );
                    }
                }
                FeedType::Trades => {
                    // matches channel works without authentication
                    // but we can still use JWT if available for more reliable connection
                    let msg = if let Some(jwt) = self.generate_jwt() {
                        serde_json::json!({
                            "type": "subscribe",
                            "product_ids": [product_id],
                            "channel": "market_trades",
                            "jwt": jwt
                        })
                    } else {
                        serde_json::json!({
                            "type": "subscribe",
                            "product_ids": [product_id],
                            "channel": "market_trades"
                        })
                    };
                    messages.push(msg.to_string());
                }
            }
        }

        messages
    }

    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError> {
        let json: Value =
            serde_json::from_str(msg).map_err(|e| ExchangeError::Parse(e.to_string()))?;

        // Check for channel field (Advanced Trade API format)
        let channel = json.get("channel").and_then(|v| v.as_str()).unwrap_or("");
        let msg_type = json.get("type").and_then(|v| v.as_str()).unwrap_or("unknown");

        // Handle Advanced Trade API format
        match channel {
            "l2_data" => {
                // Level 2 orderbook data
                let events = json.get("events").and_then(|v| v.as_array());
                let product_id = events
                    .and_then(|e| e.first())
                    .and_then(|e| e.get("product_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                let timestamp_str = json.get("timestamp").and_then(|v| v.as_str());
                let timestamp_exchange_us = parse_iso8601_to_micros(timestamp_str);

                let sequence_num = json
                    .get("sequence_num")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                Ok(ExchangeMessage::Orderbook {
                    symbol: product_id,
                    sequence_id: sequence_num.to_string(),
                    timestamp_exchange_us,
                    data: msg.to_string(),
                })
            }

            "market_trades" => {
                // Trade data
                let trades = json.get("events").and_then(|v| v.as_array());
                let first_trade = trades
                    .and_then(|t| t.first())
                    .and_then(|t| t.get("trades"))
                    .and_then(|t| t.as_array())
                    .and_then(|t| t.first());

                let product_id = first_trade
                    .and_then(|t| t.get("product_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                let trade_id = first_trade
                    .and_then(|t| t.get("trade_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .to_string();

                let timestamp_str = first_trade.and_then(|t| t.get("time")).and_then(|v| v.as_str());
                let timestamp_exchange_us = parse_iso8601_to_micros(timestamp_str);

                Ok(ExchangeMessage::Trade {
                    symbol: product_id,
                    sequence_id: trade_id,
                    timestamp_exchange_us,
                    data: msg.to_string(),
                })
            }

            "subscriptions" => Ok(ExchangeMessage::Other(msg.to_string())),

            "" => {
                // Might be old Exchange API format or error/heartbeat
                match msg_type {
                    // Old Exchange API formats (for backwards compatibility)
                    "snapshot" => {
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
                        let timestamp_exchange_us =
                            parse_iso8601_to_micros(json.get("time").and_then(|v| v.as_str()));

                        Ok(ExchangeMessage::Orderbook {
                            symbol,
                            sequence_id,
                            timestamp_exchange_us,
                            data: msg.to_string(),
                        })
                    }

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
                        let timestamp_exchange_us =
                            parse_iso8601_to_micros(json.get("time").and_then(|v| v.as_str()));

                        Ok(ExchangeMessage::Orderbook {
                            symbol,
                            sequence_id,
                            timestamp_exchange_us,
                            data: msg.to_string(),
                        })
                    }

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
                        let timestamp_exchange_us =
                            parse_iso8601_to_micros(json.get("time").and_then(|v| v.as_str()));

                        Ok(ExchangeMessage::Trade {
                            symbol,
                            sequence_id,
                            timestamp_exchange_us,
                            data: msg.to_string(),
                        })
                    }

                    "subscriptions" | "error" | "heartbeat" => {
                        Ok(ExchangeMessage::Other(msg.to_string()))
                    }

                    _ => Ok(ExchangeMessage::Other(msg.to_string())),
                }
            }

            _ => Ok(ExchangeMessage::Other(msg.to_string())),
        }
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // Normalize to lowercase without separators for consistent storage/logging
        symbol.to_lowercase().replace(['-', '_', '/'], "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_symbol() {
        let coinbase = Coinbase::new();
        assert_eq!(coinbase.normalize_symbol("btcusd"), "btcusd");
        assert_eq!(coinbase.normalize_symbol("BTC-USD"), "btcusd");
        assert_eq!(coinbase.normalize_symbol("eth_eur"), "etheur");
    }

    #[test]
    fn test_parse_advanced_trade_l2() {
        let coinbase = Coinbase::new();
        let msg = r#"{"channel":"l2_data","client_id":"","timestamp":"2023-01-01T00:03:02.136000Z","sequence_num":123456,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100.00","new_quantity":"1.0"}]}]}"#;
        let result = coinbase.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook {
                symbol,
                sequence_id,
                timestamp_exchange_us,
                ..
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(sequence_id, "123456");
                assert_eq!(timestamp_exchange_us, 1672531382136000);
            }
            _ => panic!("Expected Orderbook message"),
        }
    }

    #[test]
    fn test_parse_advanced_trade_market_trades() {
        let coinbase = Coinbase::new();
        let msg = r#"{"channel":"market_trades","client_id":"","timestamp":"2023-01-01T00:03:02.136000Z","sequence_num":0,"events":[{"type":"snapshot","trades":[{"trade_id":"12345","product_id":"BTC-USD","price":"100.00","size":"1.0","side":"BUY","time":"2023-01-01T00:03:02.136000Z"}]}]}"#;
        let result = coinbase.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Trade {
                symbol,
                sequence_id,
                timestamp_exchange_us,
                ..
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(sequence_id, "12345");
                assert_eq!(timestamp_exchange_us, 1672531382136000);
            }
            _ => panic!("Expected Trade message"),
        }
    }

    #[test]
    fn test_parse_old_snapshot() {
        let coinbase = Coinbase::new();
        let msg = r#"{"type":"snapshot","product_id":"BTC-USD","time":"2023-01-01T00:03:02.136000Z","bids":[["10101.10","0.45054140"]],"asks":[["10102.55","0.57753524"]]}"#;
        let result = coinbase.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Orderbook {
                symbol,
                timestamp_exchange_us,
                ..
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(timestamp_exchange_us, 1672531382136000);
            }
            _ => panic!("Expected Orderbook message"),
        }
    }

    #[test]
    fn test_parse_old_match() {
        let coinbase = Coinbase::new();
        let msg = r#"{"type":"match","trade_id":10,"sequence":50,"product_id":"BTC-USD","time":"2023-01-01T00:03:02.136000Z","size":"5.23512","price":"400.23","side":"sell"}"#;
        let result = coinbase.parse_message(msg).unwrap();
        match result {
            ExchangeMessage::Trade {
                symbol,
                sequence_id,
                timestamp_exchange_us,
                ..
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(sequence_id, "50");
                assert_eq!(timestamp_exchange_us, 1672531382136000);
            }
            _ => panic!("Expected Trade message"),
        }
    }

    #[test]
    fn test_unauthenticated_subscribe() {
        let coinbase = Coinbase::new();
        let messages = coinbase.build_subscribe_messages("BTC-USD", &[FeedType::Trades]);
        assert_eq!(messages.len(), 1);
        let msg: Value = serde_json::from_str(&messages[0]).unwrap();
        assert_eq!(msg["type"], "subscribe");
        assert_eq!(msg["channel"], "market_trades");
        assert!(msg.get("jwt").is_none());
    }

    #[test]
    fn test_orderbook_requires_auth() {
        let coinbase = Coinbase::new();
        // Without auth, orderbook should not produce a message (just a warning)
        let messages = coinbase.build_subscribe_messages("BTC-USD", &[FeedType::Orderbook]);
        assert_eq!(messages.len(), 0);
    }
}
