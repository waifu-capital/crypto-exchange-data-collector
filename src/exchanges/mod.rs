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

/// Parsed message from an exchange WebSocket
#[derive(Debug, Clone)]
pub enum ExchangeMessage {
    /// Order book update
    Orderbook {
        symbol: String,
        sequence_id: String,
        data: String,
    },
    /// Trade execution
    Trade {
        symbol: String,
        sequence_id: String,
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

    /// Normalizes the symbol format for this exchange.
    ///
    /// Different exchanges use different conventions:
    /// - Binance: "btcusdt" (lowercase, no separator)
    /// - Coinbase: "BTC-USD" (uppercase, hyphen)
    /// - Upbit: "KRW-BTC" (quote-base order)
    fn normalize_symbol(&self, symbol: &str) -> String {
        symbol.to_string()
    }
}

/// Creates an exchange instance by name.
///
/// # Arguments
/// * `name` - Exchange name: "binance", "coinbase", "upbit", "okx", "bybit"
///
/// # Returns
/// `Some(Box<dyn Exchange>)` if the name is recognized, `None` otherwise.
pub fn create_exchange(name: &str) -> Option<Box<dyn Exchange>> {
    match name.to_lowercase().as_str() {
        "binance" => Some(Box::new(binance::Binance::new())),
        "coinbase" => Some(Box::new(coinbase::Coinbase::new())),
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
