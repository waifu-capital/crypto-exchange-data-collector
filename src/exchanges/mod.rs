//! Exchange abstraction layer for multi-exchange WebSocket support.
//!
//! This module provides a unified interface for connecting to different
//! cryptocurrency exchanges and receiving orderbook and trade data.

pub mod binance;
pub mod bybit;
pub mod coinbase;
pub mod okx;
pub mod upbit;

use std::fmt;

use futures_util::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Type alias for WebSocket write half
pub type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tokio_tungstenite::tungstenite::Message>;

/// Type alias for WebSocket read half
pub type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Feed types supported by exchanges
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedType {
    /// Order book depth snapshots
    Orderbook,
    /// Trade executions
    Trades,
}

impl FeedType {
    /// Returns the string representation of the feed type
    pub fn as_str(&self) -> &'static str {
        match self {
            FeedType::Orderbook => "orderbook",
            FeedType::Trades => "trades",
        }
    }
}

/// Parsed message from an exchange WebSocket
#[derive(Debug, Clone)]
pub enum ExchangeMessage {
    /// Order book update
    Orderbook {
        symbol: String,
        sequence_id: String,
        timestamp_exchange_us: i64, // Exchange event time in microseconds
        data: String,
    },
    /// Trade execution
    Trade {
        symbol: String,
        sequence_id: String,
        timestamp_exchange_us: i64, // Exchange event time in microseconds
        data: String,
    },
    /// Ping frame that needs a pong response
    Ping(Vec<u8>),
    /// Pong frame (response to our ping)
    Pong,
    /// Other message types (subscriptions confirmations, etc.)
    Other(String),
}

/// Error type for exchange operations
#[derive(Debug)]
pub enum ExchangeError {
    /// WebSocket connection error
    Connection(String),
    /// Message parsing error
    Parse(String),
    /// Subscription error
    Subscribe(String),
    /// Generic error
    Other(String),
}

impl fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(msg) => write!(f, "Connection error: {}", msg),
            Self::Parse(msg) => write!(f, "Parse error: {}", msg),
            Self::Subscribe(msg) => write!(f, "Subscribe error: {}", msg),
            Self::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for ExchangeError {}

/// Trait defining the interface for cryptocurrency exchanges.
///
/// Each exchange implementation provides methods for:
/// - Constructing WebSocket URLs
/// - Building subscription messages
/// - Parsing incoming messages into a unified format
pub trait Exchange: Send + Sync {
    /// Returns the exchange name (e.g., "binance", "coinbase")
    fn name(&self) -> &'static str;

    /// Returns the WebSocket URL for connecting to this exchange.
    ///
    /// Some exchanges encode the symbol in the URL (Binance),
    /// others use a single endpoint and subscribe via message (Coinbase).
    fn websocket_url(&self, symbol: &str) -> String;

    /// Builds the subscription message(s) for the given symbol and feed types.
    ///
    /// Returns a vector of messages to send after connection.
    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String>;

    /// Parses a raw WebSocket message into an ExchangeMessage.
    ///
    /// Returns `Ok(ExchangeMessage)` on success, or `Err(ExchangeError)` if
    /// the message cannot be parsed.
    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError>;

    /// Normalizes the symbol to a consistent format for storage and logging.
    ///
    /// All symbols are normalized to lowercase without separators (e.g., "btcusdt").
    /// This ensures consistent storage across all exchanges regardless of their
    /// native symbol format (BTC-USD, BTC_USDT, etc.).
    fn normalize_symbol(&self, symbol: &str) -> String {
        symbol.to_lowercase().replace(['-', '_', '/'], "")
    }
}

/// Coinbase API credentials for authenticated channels.
#[derive(Clone, Default)]
pub struct CoinbaseCredentials {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

impl CoinbaseCredentials {
    pub fn new(api_key: Option<String>, api_secret: Option<String>) -> Self {
        Self { api_key, api_secret }
    }

    pub fn has_credentials(&self) -> bool {
        self.api_key.is_some() && self.api_secret.is_some()
    }
}

/// Creates an exchange instance by name.
///
/// # Arguments
/// * `name` - Exchange name: "binance", "coinbase", "upbit", "okx", "bybit"
/// * `coinbase_creds` - Optional Coinbase API credentials for authenticated channels
///
/// # Returns
/// `Some(Box<dyn Exchange>)` if the name is recognized, `None` otherwise.
pub fn create_exchange(name: &str, coinbase_creds: &CoinbaseCredentials) -> Option<Box<dyn Exchange>> {
    match name.to_lowercase().as_str() {
        "binance" => Some(Box::new(binance::Binance::new())),
        "coinbase" => {
            if let (Some(key), Some(secret)) = (&coinbase_creds.api_key, &coinbase_creds.api_secret) {
                Some(Box::new(coinbase::Coinbase::with_credentials(key.clone(), secret.clone())))
            } else {
                Some(Box::new(coinbase::Coinbase::new()))
            }
        }
        "upbit" => Some(Box::new(upbit::Upbit::new())),
        "okx" => Some(Box::new(okx::Okx::new())),
        "bybit" => Some(Box::new(bybit::Bybit::new())),
        _ => None,
    }
}

/// Returns a list of all supported exchange names.
pub fn supported_exchanges() -> &'static [&'static str] {
    &["binance", "coinbase", "upbit", "okx", "bybit"]
}

#[cfg(test)]
mod smoke_tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::time::{Duration, Instant};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    /// Connect to an exchange, receive messages, and verify parsing works.
    ///
    /// This helper connects to the exchange WebSocket, subscribes to the given
    /// feeds, receives `message_count` data messages, and verifies all parse correctly.
    async fn run_smoke_test(
        exchange: &dyn Exchange,
        symbol: &str,
        message_count: usize,
    ) -> Result<(), String> {
        let url = exchange.websocket_url(symbol);
        println!("Connecting to {} at {}", exchange.name(), url);

        let (ws_stream, _) = connect_async(&url)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;

        let (mut write, mut read) = ws_stream.split();

        // Send subscription messages
        let feeds = vec![FeedType::Orderbook, FeedType::Trades];
        for msg in exchange.build_subscribe_messages(symbol, &feeds) {
            println!("Sending subscription: {}", &msg[..msg.len().min(100)]);
            write
                .send(Message::Text(msg.into()))
                .await
                .map_err(|e| format!("Send failed: {}", e))?;
        }

        // Receive and parse messages
        let timeout = Duration::from_secs(30);
        let mut received = 0;
        let mut data_messages = 0;
        let deadline = Instant::now() + timeout;

        println!("Waiting for {} data messages...", message_count);

        while data_messages < message_count && Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(5), read.next()).await {
                Ok(Some(Ok(msg))) => {
                    // Handle both text and binary messages
                    let text = if msg.is_text() {
                        msg.into_text().unwrap().to_string()
                    } else if msg.is_binary() {
                        // Try to decode binary as UTF-8 (some exchanges send JSON as binary)
                        match String::from_utf8(msg.into_data().to_vec()) {
                            Ok(s) => s,
                            Err(_) => continue, // Skip non-UTF8 binary
                        }
                    } else {
                        continue; // Skip ping/pong/close frames
                    };

                    received += 1;

                    match exchange.parse_message(&text) {
                        Ok(ExchangeMessage::Orderbook { symbol, .. }) => {
                            data_messages += 1;
                            if data_messages <= 3 {
                                println!("  [{}] Orderbook for {}", data_messages, symbol);
                            }
                        }
                        Ok(ExchangeMessage::Trade { symbol, .. }) => {
                            data_messages += 1;
                            if data_messages <= 3 {
                                println!("  [{}] Trade for {}", data_messages, symbol);
                            }
                        }
                        Ok(ExchangeMessage::Other(_)) => {
                            // Subscription confirmations, heartbeats, etc.
                        }
                        Ok(ExchangeMessage::Pong) => {}
                        Ok(ExchangeMessage::Ping(_)) => {}
                        Err(e) => {
                            return Err(format!(
                                "Parse failed on message {}: {}\nRaw: {}",
                                received,
                                e,
                                &text[..text.len().min(200)]
                            ));
                        }
                    }
                }
                Ok(Some(Err(e))) => return Err(format!("WebSocket error: {}", e)),
                Ok(None) => return Err("Connection closed unexpectedly".to_string()),
                Err(_) => {
                    // Timeout on single message read, continue waiting
                }
            }
        }

        if data_messages < message_count {
            return Err(format!(
                "Only received {}/{} data messages (total messages: {})",
                data_messages, message_count, received
            ));
        }

        println!(
            "Success: received {} data messages ({} total)",
            data_messages, received
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn live_binance_smoke_test() {
        let exchange = binance::Binance::new();
        let result = run_smoke_test(&exchange, "btcusdt", 10).await;
        assert!(
            result.is_ok(),
            "Binance smoke test failed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn live_coinbase_smoke_test() {
        let exchange = coinbase::Coinbase::new();
        let result = run_smoke_test(&exchange, "BTC-USD", 10).await;
        assert!(
            result.is_ok(),
            "Coinbase smoke test failed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn live_upbit_smoke_test() {
        let exchange = upbit::Upbit::new();
        let result = run_smoke_test(&exchange, "KRW-BTC", 10).await;
        assert!(
            result.is_ok(),
            "Upbit smoke test failed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn live_okx_smoke_test() {
        let exchange = okx::Okx::new();
        let result = run_smoke_test(&exchange, "BTC-USDT", 10).await;
        assert!(result.is_ok(), "OKX smoke test failed: {:?}", result.err());
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn live_bybit_smoke_test() {
        let exchange = bybit::Bybit::new();
        let result = run_smoke_test(&exchange, "BTCUSDT", 10).await;
        assert!(
            result.is_ok(),
            "Bybit smoke test failed: {:?}",
            result.err()
        );
    }
}
