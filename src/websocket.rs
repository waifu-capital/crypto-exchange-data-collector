use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tracing::{debug, error, info, warn};

/// Timeout for receiving WebSocket messages.
/// With 100ms message frequency from Binance, 30s without a message indicates a stale connection.
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Initial delay before retrying a failed connection.
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Maximum delay between retry attempts.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Exponential backoff helper for connection retries.
struct ExponentialBackoff {
    current_delay: Duration,
}

impl ExponentialBackoff {
    fn new() -> Self {
        Self {
            current_delay: INITIAL_RETRY_DELAY,
        }
    }

    /// Returns the next delay and doubles it for the next call (capped at MAX_RETRY_DELAY).
    fn next_delay(&mut self) -> Duration {
        let delay = self.current_delay;
        self.current_delay = (self.current_delay * 2).min(MAX_RETRY_DELAY);
        delay
    }

    /// Resets the backoff to the initial delay (call after successful connection).
    fn reset(&mut self) {
        self.current_delay = INITIAL_RETRY_DELAY;
    }
}

use crate::models::SnapshotData;
use crate::utils::increment_dropped_snapshots;

/// WebSocket worker that connects to Binance and streams orderbook data
pub async fn websocket_worker(db_tx: Sender<SnapshotData>, market_symbol: String) {
    let url = format!(
        "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
        market_symbol.to_lowercase()
    );
    let mut backoff = ExponentialBackoff::new();

    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!(symbol = %market_symbol, "Connected to Binance WebSocket");
                backoff.reset(); // Reset backoff on successful connection
                let (mut write, mut read) = ws_stream.split();

                loop {
                    match timeout(MESSAGE_TIMEOUT, read.next()).await {
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
                            break;
                        }
                        Ok(None) => {
                            info!(symbol = %market_symbol, "WebSocket stream ended");
                            break;
                        }
                        Err(_) => {
                            error!(
                                symbol = %market_symbol,
                                timeout_secs = MESSAGE_TIMEOUT.as_secs(),
                                "Message timeout - connection may be stale, reconnecting"
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to WebSocket");
            }
        }

        let delay = backoff.next_delay();
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
            let count = increment_dropped_snapshots();
            warn!(
                total_dropped = count,
                "Channel full, dropped snapshot"
            );
        }
    }
}
