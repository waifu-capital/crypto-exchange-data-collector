use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tracing::{debug, error, info, warn};

/// WebSocket connection configuration
pub struct WsConfig {
    /// Timeout for receiving WebSocket messages (seconds)
    pub message_timeout_secs: u64,
    /// Initial delay before retrying a failed connection (seconds)
    pub initial_retry_delay_secs: u64,
    /// Maximum delay between retry attempts (seconds)
    pub max_retry_delay_secs: u64,
}

/// Exponential backoff helper for connection retries.
struct ExponentialBackoff {
    current_delay: Duration,
    max_delay: Duration,
    initial_delay: Duration,
}

impl ExponentialBackoff {
    fn new(initial_delay_secs: u64, max_delay_secs: u64) -> Self {
        Self {
            current_delay: Duration::from_secs(initial_delay_secs),
            max_delay: Duration::from_secs(max_delay_secs),
            initial_delay: Duration::from_secs(initial_delay_secs),
        }
    }

    /// Returns the next delay and doubles it for the next call (capped at max_delay).
    fn next_delay(&mut self) -> Duration {
        let delay = self.current_delay;
        self.current_delay = (self.current_delay * 2).min(self.max_delay);
        delay
    }

    /// Resets the backoff to the initial delay (call after successful connection).
    fn reset(&mut self) {
        self.current_delay = self.initial_delay;
    }
}

use crate::metrics::{
    LAST_MESSAGE_TIMESTAMP, MESSAGES_DROPPED, MESSAGES_RECEIVED, MESSAGE_TIMEOUTS,
    WEBSOCKET_CONNECTED, WEBSOCKET_RECONNECTS,
};
use crate::models::{ConnectionState, SnapshotData};

/// Helper to update connection state
async fn set_connection_status(conn_state: &ConnectionState, exchange: &str, symbol: &str, connected: bool) {
    let key = format!("{}:{}", exchange, symbol);
    conn_state.write().await.insert(key, connected);
}

/// WebSocket worker that connects to Binance and streams orderbook data
pub async fn websocket_worker(
    db_tx: Sender<SnapshotData>,
    market_symbol: String,
    ws_config: WsConfig,
    conn_state: ConnectionState,
) {
    let url = format!(
        "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
        market_symbol.to_lowercase()
    );
    let message_timeout = Duration::from_secs(ws_config.message_timeout_secs);
    let mut backoff = ExponentialBackoff::new(
        ws_config.initial_retry_delay_secs,
        ws_config.max_retry_delay_secs,
    );

    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!(symbol = %market_symbol, "Connected to Binance WebSocket");
                WEBSOCKET_CONNECTED
                    .with_label_values(&["binance", &market_symbol])
                    .set(1.0);
                set_connection_status(&conn_state, "binance", &market_symbol, true).await;
                backoff.reset(); // Reset backoff on successful connection
                let (mut write, mut read) = ws_stream.split();

                loop {
                    match timeout(message_timeout, read.next()).await {
                        Ok(Some(Ok(msg))) => {
                            if msg.is_text() {
                                // Sample log ~0.1% of messages at debug level
                                if rand::random::<u32>() % 1000 == 0 {
                                    if let Ok(preview) = msg.to_text() {
                                        let preview_len = preview.len().min(50);
                                        debug!(
                                            preview = &preview[..preview_len],
                                            "Received orderbook message"
                                        );
                                    }
                                }

                                let text = match msg.into_text() {
                                    Ok(t) => t,
                                    Err(e) => {
                                        error!(error = %e, "Failed to extract text from message");
                                        continue;
                                    }
                                };

                                let snapshot: Value = match serde_json::from_str(&text) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        error!(error = %e, "Failed to parse JSON");
                                        continue;
                                    }
                                };

                                let exchange_sequence_id = snapshot["lastUpdateId"].to_string();

                                // Update metrics
                                MESSAGES_RECEIVED
                                    .with_label_values(&["binance", &market_symbol, "orderbook"])
                                    .inc();
                                LAST_MESSAGE_TIMESTAMP
                                    .with_label_values(&["binance", &market_symbol])
                                    .set(
                                        SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_secs_f64(),
                                    );

                                save_snapshot(
                                    &db_tx,
                                    "binance",
                                    &market_symbol,
                                    "orderbook",
                                    &exchange_sequence_id,
                                    &text,
                                );
                            } else if msg.is_ping() {
                                debug!("Received ping frame");
                                let pong =
                                    tokio_tungstenite::tungstenite::Message::Pong(msg.into_data());
                                if let Err(e) = write.send(pong).await {
                                    error!(error = %e, "Failed to send pong frame");
                                    break;
                                }
                                debug!("Sent pong frame in response to ping");
                            } else if msg.is_pong() {
                                debug!("Received pong frame");
                            } else {
                                debug!(?msg, "Received other message");
                            }
                        }
                        Ok(Some(Err(e))) => {
                            error!(error = %e, "WebSocket error");
                            WEBSOCKET_CONNECTED
                                .with_label_values(&["binance", &market_symbol])
                                .set(0.0);
                            break;
                        }
                        Ok(None) => {
                            info!(symbol = %market_symbol, "WebSocket stream ended");
                            WEBSOCKET_CONNECTED
                                .with_label_values(&["binance", &market_symbol])
                                .set(0.0);
                            break;
                        }
                        Err(_) => {
                            error!(
                                symbol = %market_symbol,
                                timeout_secs = message_timeout.as_secs(),
                                "Message timeout - connection may be stale, reconnecting"
                            );
                            MESSAGE_TIMEOUTS
                                .with_label_values(&["binance", &market_symbol])
                                .inc();
                            WEBSOCKET_CONNECTED
                                .with_label_values(&["binance", &market_symbol])
                                .set(0.0);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to WebSocket");
            }
        }

        // Mark connection as down before attempting reconnect
        set_connection_status(&conn_state, "binance", &market_symbol, false).await;

        let delay = backoff.next_delay();
        WEBSOCKET_RECONNECTS
            .with_label_values(&["binance", &market_symbol])
            .inc();
        info!(
            symbol = %market_symbol,
            delay_secs = delay.as_secs(),
            "Reconnecting to WebSocket with exponential backoff"
        );
        sleep(delay).await;
    }
}

/// Save a snapshot to the channel for database processing
fn save_snapshot(
    db_tx: &Sender<SnapshotData>,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    exchange_sequence_id: &str,
    raw_data: &str,
) {
    let data = SnapshotData {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        data_type: data_type.to_string(),
        exchange_sequence_id: exchange_sequence_id.to_string(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64,
        data: raw_data.to_string(),
    };

    // Non-blocking send - never stall WebSocket even under backpressure
    match db_tx.try_send(data) {
        Ok(_) => {}
        Err(_) => {
            MESSAGES_DROPPED.inc();
            warn!(
                total_dropped = MESSAGES_DROPPED.get() as u64,
                "Channel full, dropped snapshot"
            );
        }
    }
}
