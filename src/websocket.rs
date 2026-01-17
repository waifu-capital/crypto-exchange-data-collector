use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::exchanges::{Exchange, ExchangeMessage, FeedType};
use crate::metrics::{
    LAST_MESSAGE_TIMESTAMP, LATENCY_EXCHANGE_TO_COLLECTOR, MESSAGES_DROPPED, MESSAGES_RECEIVED,
    MESSAGE_TIMEOUTS, SEQUENCE_GAPS, SEQUENCE_GAP_SIZE, WEBSOCKET_CONNECTED, WEBSOCKET_RECONNECTS,
};
use crate::models::{ConnectionState, SequenceTracker, SnapshotData};

/// WebSocket connection configuration
#[derive(Clone)]
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

/// Helper to update connection state
async fn set_connection_status(
    conn_state: &ConnectionState,
    exchange: &str,
    symbol: &str,
    connected: bool,
) {
    let key = format!("{}:{}", exchange, symbol);
    conn_state.write().await.insert(key, connected);
}

/// WebSocket worker that connects to an exchange and streams market data.
///
/// This is a generic worker that works with any exchange implementing the `Exchange` trait.
/// It handles connection management, message parsing, and forwards data to the database channel.
pub async fn websocket_worker(
    exchange: Box<dyn Exchange>,
    db_tx: Sender<SnapshotData>,
    symbol: String,
    feeds: Vec<FeedType>,
    ws_config: WsConfig,
    conn_state: ConnectionState,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let exchange_name = exchange.name();
    let normalized_symbol = exchange.normalize_symbol(&symbol);
    let url = exchange.websocket_url(&symbol);
    let message_timeout = Duration::from_secs(ws_config.message_timeout_secs);
    let mut backoff = ExponentialBackoff::new(
        ws_config.initial_retry_delay_secs,
        ws_config.max_retry_delay_secs,
    );

    // Sequence tracker for gap detection
    let mut seq_tracker = SequenceTracker::new();

    info!(
        exchange = exchange_name,
        symbol = %normalized_symbol,
        url = %url,
        feeds = ?feeds,
        "Starting WebSocket worker"
    );

    loop {
        // Check for shutdown signal before attempting to connect
        if shutdown_rx.try_recv().is_ok() {
            info!(
                exchange = exchange_name,
                symbol = %normalized_symbol,
                "WebSocket worker received shutdown signal"
            );
            break;
        }

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!(
                    exchange = exchange_name,
                    symbol = %normalized_symbol,
                    "Connected to WebSocket"
                );
                WEBSOCKET_CONNECTED
                    .with_label_values(&[exchange_name, &normalized_symbol])
                    .set(1.0);
                set_connection_status(&conn_state, exchange_name, &normalized_symbol, true).await;
                backoff.reset();

                let (mut write, mut read) = ws_stream.split();

                // Send subscription messages
                let subscribe_msgs = exchange.build_subscribe_messages(&symbol, &feeds);
                for msg in subscribe_msgs {
                    debug!(
                        exchange = exchange_name,
                        message = %msg,
                        "Sending subscription message"
                    );
                    if let Err(e) = write.send(Message::Text(msg.into())).await {
                        error!(
                            exchange = exchange_name,
                            error = %e,
                            "Failed to send subscription message"
                        );
                        break;
                    }
                }

                loop {
                    // Check for shutdown signal
                    if shutdown_rx.try_recv().is_ok() {
                        info!(
                            exchange = exchange_name,
                            symbol = %normalized_symbol,
                            "WebSocket worker received shutdown signal, closing connection"
                        );
                        WEBSOCKET_CONNECTED
                            .with_label_values(&[exchange_name, &normalized_symbol])
                            .set(0.0);
                        set_connection_status(&conn_state, exchange_name, &normalized_symbol, false).await;
                        // Close the websocket cleanly
                        if let Err(e) = write.close().await {
                            debug!(error = %e, "Error closing WebSocket connection");
                        }
                        info!(
                            exchange = exchange_name,
                            symbol = %normalized_symbol,
                            "WebSocket worker stopped"
                        );
                        return;
                    }

                    match timeout(message_timeout, read.next()).await {
                        Ok(Some(Ok(msg))) => {
                            if msg.is_text() {
                                let text = match msg.into_text() {
                                    Ok(t) => t,
                                    Err(e) => {
                                        error!(error = %e, "Failed to extract text from message");
                                        continue;
                                    }
                                };

                                // Sample log ~0.1% of messages at debug level
                                if rand::random::<u32>() % 1000 == 0 {
                                    let preview_len = text.len().min(80);
                                    debug!(
                                        exchange = exchange_name,
                                        preview = &text[..preview_len],
                                        "Received message"
                                    );
                                }

                                // Parse message using exchange-specific logic
                                match exchange.parse_message(&text) {
                                    Ok(ExchangeMessage::Orderbook {
                                        symbol: sym,
                                        sequence_id,
                                        timestamp_exchange,
                                        data,
                                    }) => {
                                        let collector_time_ms = now_millis();
                                        MESSAGES_RECEIVED
                                            .with_label_values(&[
                                                exchange_name,
                                                &normalized_symbol,
                                                "orderbook",
                                            ])
                                            .inc();
                                        update_last_message_timestamp(
                                            exchange_name,
                                            &normalized_symbol,
                                        );

                                        // Record latency (only positive values, clock skew can cause negative)
                                        if timestamp_exchange > 0 {
                                            let latency = collector_time_ms - timestamp_exchange;
                                            if latency > 0 {
                                                LATENCY_EXCHANGE_TO_COLLECTOR
                                                    .with_label_values(&[exchange_name, &normalized_symbol, "orderbook"])
                                                    .observe(latency as f64);
                                            }
                                        }

                                        // Check for sequence gaps
                                        if let Some(gap) = seq_tracker.check(
                                            exchange_name,
                                            &sym,
                                            "orderbook",
                                            &sequence_id,
                                            collector_time_ms,
                                        ) {
                                            SEQUENCE_GAPS
                                                .with_label_values(&[exchange_name, &sym, "orderbook"])
                                                .inc();
                                            SEQUENCE_GAP_SIZE
                                                .with_label_values(&[exchange_name, &sym, "orderbook"])
                                                .observe(gap.gap_size as f64);
                                            warn!(
                                                exchange = exchange_name,
                                                symbol = %sym,
                                                data_type = "orderbook",
                                                expected = gap.expected,
                                                received = gap.received,
                                                gap_size = gap.gap_size,
                                                "Sequence gap detected"
                                            );
                                        }

                                        save_snapshot(
                                            &db_tx,
                                            exchange_name,
                                            &sym,
                                            "orderbook",
                                            &sequence_id,
                                            timestamp_exchange,
                                            &data,
                                        );
                                    }
                                    Ok(ExchangeMessage::Trade {
                                        symbol: sym,
                                        sequence_id,
                                        timestamp_exchange,
                                        data,
                                    }) => {
                                        let collector_time_ms = now_millis();
                                        MESSAGES_RECEIVED
                                            .with_label_values(&[
                                                exchange_name,
                                                &normalized_symbol,
                                                "trade",
                                            ])
                                            .inc();
                                        update_last_message_timestamp(
                                            exchange_name,
                                            &normalized_symbol,
                                        );

                                        // Record latency (only positive values, clock skew can cause negative)
                                        if timestamp_exchange > 0 {
                                            let latency = collector_time_ms - timestamp_exchange;
                                            if latency > 0 {
                                                LATENCY_EXCHANGE_TO_COLLECTOR
                                                    .with_label_values(&[exchange_name, &normalized_symbol, "trade"])
                                                    .observe(latency as f64);
                                            }
                                        }

                                        // Check for sequence gaps
                                        if let Some(gap) = seq_tracker.check(
                                            exchange_name,
                                            &sym,
                                            "trade",
                                            &sequence_id,
                                            collector_time_ms,
                                        ) {
                                            SEQUENCE_GAPS
                                                .with_label_values(&[exchange_name, &sym, "trade"])
                                                .inc();
                                            SEQUENCE_GAP_SIZE
                                                .with_label_values(&[exchange_name, &sym, "trade"])
                                                .observe(gap.gap_size as f64);
                                            warn!(
                                                exchange = exchange_name,
                                                symbol = %sym,
                                                data_type = "trade",
                                                expected = gap.expected,
                                                received = gap.received,
                                                gap_size = gap.gap_size,
                                                "Sequence gap detected"
                                            );
                                        }

                                        save_snapshot(
                                            &db_tx,
                                            exchange_name,
                                            &sym,
                                            "trade",
                                            &sequence_id,
                                            timestamp_exchange,
                                            &data,
                                        );
                                    }
                                    Ok(ExchangeMessage::Ping(data)) => {
                                        debug!(
                                            exchange = exchange_name,
                                            "Received ping, sending pong"
                                        );
                                        if let Err(e) = write.send(Message::Pong(data.into())).await {
                                            error!(error = %e, "Failed to send pong");
                                            break;
                                        }
                                    }
                                    Ok(ExchangeMessage::Pong) => {
                                        debug!(exchange = exchange_name, "Received pong");
                                    }
                                    Ok(ExchangeMessage::Other(other)) => {
                                        // Subscription confirmations, heartbeats, etc.
                                        debug!(
                                            exchange = exchange_name,
                                            message = %other.chars().take(100).collect::<String>(),
                                            "Received other message"
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            exchange = exchange_name,
                                            error = %e,
                                            "Failed to parse message"
                                        );
                                    }
                                }
                            } else if msg.is_ping() {
                                debug!(exchange = exchange_name, "Received ping frame");
                                let pong = Message::Pong(msg.into_data());
                                if let Err(e) = write.send(pong).await {
                                    error!(error = %e, "Failed to send pong frame");
                                    break;
                                }
                                debug!(exchange = exchange_name, "Sent pong frame");
                            } else if msg.is_pong() {
                                debug!(exchange = exchange_name, "Received pong frame");
                            } else if msg.is_binary() {
                                // Some exchanges send binary messages (e.g., OKX compressed)
                                debug!(
                                    exchange = exchange_name,
                                    len = msg.len(),
                                    "Received binary message"
                                );
                            } else {
                                debug!(exchange = exchange_name, ?msg, "Received other frame");
                            }
                        }
                        Ok(Some(Err(e))) => {
                            error!(
                                exchange = exchange_name,
                                error = %e,
                                "WebSocket error"
                            );
                            WEBSOCKET_CONNECTED
                                .with_label_values(&[exchange_name, &normalized_symbol])
                                .set(0.0);
                            break;
                        }
                        Ok(None) => {
                            info!(
                                exchange = exchange_name,
                                symbol = %normalized_symbol,
                                "WebSocket stream ended"
                            );
                            WEBSOCKET_CONNECTED
                                .with_label_values(&[exchange_name, &normalized_symbol])
                                .set(0.0);
                            break;
                        }
                        Err(_) => {
                            error!(
                                exchange = exchange_name,
                                symbol = %normalized_symbol,
                                timeout_secs = message_timeout.as_secs(),
                                "Message timeout - connection may be stale, reconnecting"
                            );
                            MESSAGE_TIMEOUTS
                                .with_label_values(&[exchange_name, &normalized_symbol])
                                .inc();
                            WEBSOCKET_CONNECTED
                                .with_label_values(&[exchange_name, &normalized_symbol])
                                .set(0.0);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    exchange = exchange_name,
                    url = %url,
                    error = %e,
                    "Failed to connect to WebSocket"
                );
            }
        }

        // Mark connection as down before attempting reconnect
        set_connection_status(&conn_state, exchange_name, &normalized_symbol, false).await;

        let delay = backoff.next_delay();
        WEBSOCKET_RECONNECTS
            .with_label_values(&[exchange_name, &normalized_symbol])
            .inc();
        info!(
            exchange = exchange_name,
            symbol = %normalized_symbol,
            delay_secs = delay.as_secs(),
            "Reconnecting to WebSocket with exponential backoff"
        );
        sleep(delay).await;
    }
}

/// Get current time in milliseconds since epoch
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Update the last message timestamp metric
fn update_last_message_timestamp(exchange: &str, symbol: &str) {
    LAST_MESSAGE_TIMESTAMP
        .with_label_values(&[exchange, symbol])
        .set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        );
}

/// Save a snapshot to the channel for database processing
fn save_snapshot(
    db_tx: &Sender<SnapshotData>,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    exchange_sequence_id: &str,
    timestamp_exchange: i64,
    raw_data: &str,
) {
    let data = SnapshotData {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        data_type: data_type.to_string(),
        exchange_sequence_id: exchange_sequence_id.to_string(),
        timestamp_collector: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64,
        timestamp_exchange,
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
