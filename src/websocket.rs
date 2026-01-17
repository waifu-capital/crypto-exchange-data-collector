use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::exchanges::{Exchange, ExchangeMessage, FeedType};
use crate::metrics::{
    LAST_MESSAGE_TIMESTAMP, LATENCY_EXCHANGE_TO_COLLECTOR, MESSAGES_DROPPED, MESSAGES_RECEIVED,
    MESSAGE_TIMEOUTS, PARSE_CIRCUIT_BREAKS, SEQUENCE_DUPLICATES, SEQUENCE_GAPS, SEQUENCE_GAP_SIZE,
    SEQUENCE_OUT_OF_ORDER, WEBSOCKET_CONNECTED, WEBSOCKET_RECONNECTS,
};
use crate::models::{ConnectionState, SequenceCheckResult, SequenceTracker, SnapshotData};

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

/// Exponential backoff helper for connection retries with jitter.
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

    /// Returns the next delay with jitter (±25%) and doubles it for the next call.
    fn next_delay(&mut self) -> Duration {
        let base_delay = self.current_delay;
        self.current_delay = (self.current_delay * 2).min(self.max_delay);

        // Add jitter: 75% to 125% of base delay to prevent thundering herd
        let jitter_factor = rand::rng().random_range(0.75..1.25);
        Duration::from_secs_f64(base_delay.as_secs_f64() * jitter_factor)
    }

    /// Resets the backoff to the initial delay (call after successful connection).
    fn reset(&mut self) {
        self.current_delay = self.initial_delay;
    }
}

/// Tracks parse error rate for circuit breaker functionality.
/// If error rate exceeds threshold, triggers a reconnect.
struct ParseErrorTracker {
    /// Errors in current window
    window_errors: u32,
    /// Total messages in current window
    window_total: u32,
    /// When current window started
    window_start: Instant,
    /// Window duration before reset
    window_duration: Duration,
    /// Error ratio threshold to trip circuit (e.g., 0.5 = 50%)
    threshold_ratio: f64,
    /// Minimum sample size before checking ratio
    min_samples: u32,
}

impl ParseErrorTracker {
    fn new() -> Self {
        Self {
            window_errors: 0,
            window_total: 0,
            window_start: Instant::now(),
            window_duration: Duration::from_secs(60),
            threshold_ratio: 0.5,
            min_samples: 100,
        }
    }

    /// Reset window if expired
    fn maybe_reset_window(&mut self) {
        if self.window_start.elapsed() > self.window_duration {
            self.window_errors = 0;
            self.window_total = 0;
            self.window_start = Instant::now();
        }
    }

    /// Record a successful parse
    fn record_success(&mut self) {
        self.maybe_reset_window();
        self.window_total += 1;
    }

    /// Record a parse error. Returns true if circuit should trip.
    fn record_error(&mut self) -> bool {
        self.maybe_reset_window();
        self.window_total += 1;
        self.window_errors += 1;

        // Only check ratio after minimum samples
        if self.window_total >= self.min_samples {
            let error_rate = self.window_errors as f64 / self.window_total as f64;
            error_rate >= self.threshold_ratio
        } else {
            false
        }
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
    // Parse error tracker for circuit breaker
    let mut parse_tracker = ParseErrorTracker::new();

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
                                        timestamp_exchange_us,
                                        data,
                                    }) => {
                                        // Use same timestamp for collector and latency calculation
                                        let collector_time_us = now_micros();
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

                                        // Record latency in ms (only positive values, clock skew can cause negative)
                                        if timestamp_exchange_us > 0 {
                                            let latency_us = collector_time_us - timestamp_exchange_us;
                                            if latency_us > 0 {
                                                // Report latency in milliseconds for readability
                                                LATENCY_EXCHANGE_TO_COLLECTOR
                                                    .with_label_values(&[exchange_name, &normalized_symbol, "orderbook"])
                                                    .observe((latency_us / 1000) as f64);
                                            }
                                        }

                                        // Check for sequence anomalies
                                        match seq_tracker.check(
                                            exchange_name,
                                            &sym,
                                            "orderbook",
                                            &sequence_id,
                                            collector_time_us,
                                        ) {
                                            SequenceCheckResult::Gap(gap) => {
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
                                            SequenceCheckResult::OutOfOrder { expected, received } => {
                                                SEQUENCE_OUT_OF_ORDER
                                                    .with_label_values(&[exchange_name, &sym, "orderbook"])
                                                    .inc();
                                                warn!(
                                                    exchange = exchange_name,
                                                    symbol = %sym,
                                                    data_type = "orderbook",
                                                    expected,
                                                    received,
                                                    "Out-of-order sequence detected"
                                                );
                                            }
                                            SequenceCheckResult::Duplicate { seq } => {
                                                SEQUENCE_DUPLICATES
                                                    .with_label_values(&[exchange_name, &sym, "orderbook"])
                                                    .inc();
                                                debug!(
                                                    exchange = exchange_name,
                                                    symbol = %sym,
                                                    seq,
                                                    "Duplicate sequence detected"
                                                );
                                            }
                                            SequenceCheckResult::Ok => {}
                                        }

                                        save_snapshot(
                                            &db_tx,
                                            exchange_name,
                                            &sym,
                                            "orderbook",
                                            &sequence_id,
                                            collector_time_us,
                                            timestamp_exchange_us,
                                            &data,
                                        );
                                        parse_tracker.record_success();
                                    }
                                    Ok(ExchangeMessage::Trade {
                                        symbol: sym,
                                        sequence_id,
                                        timestamp_exchange_us,
                                        data,
                                    }) => {
                                        // Use same timestamp for collector and latency calculation
                                        let collector_time_us = now_micros();
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

                                        // Record latency in ms (only positive values, clock skew can cause negative)
                                        if timestamp_exchange_us > 0 {
                                            let latency_us = collector_time_us - timestamp_exchange_us;
                                            if latency_us > 0 {
                                                // Report latency in milliseconds for readability
                                                LATENCY_EXCHANGE_TO_COLLECTOR
                                                    .with_label_values(&[exchange_name, &normalized_symbol, "trade"])
                                                    .observe((latency_us / 1000) as f64);
                                            }
                                        }

                                        // Check for sequence anomalies
                                        match seq_tracker.check(
                                            exchange_name,
                                            &sym,
                                            "trade",
                                            &sequence_id,
                                            collector_time_us,
                                        ) {
                                            SequenceCheckResult::Gap(gap) => {
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
                                            SequenceCheckResult::OutOfOrder { expected, received } => {
                                                SEQUENCE_OUT_OF_ORDER
                                                    .with_label_values(&[exchange_name, &sym, "trade"])
                                                    .inc();
                                                warn!(
                                                    exchange = exchange_name,
                                                    symbol = %sym,
                                                    data_type = "trade",
                                                    expected,
                                                    received,
                                                    "Out-of-order sequence detected"
                                                );
                                            }
                                            SequenceCheckResult::Duplicate { seq } => {
                                                SEQUENCE_DUPLICATES
                                                    .with_label_values(&[exchange_name, &sym, "trade"])
                                                    .inc();
                                                debug!(
                                                    exchange = exchange_name,
                                                    symbol = %sym,
                                                    seq,
                                                    "Duplicate sequence detected"
                                                );
                                            }
                                            SequenceCheckResult::Ok => {}
                                        }

                                        save_snapshot(
                                            &db_tx,
                                            exchange_name,
                                            &sym,
                                            "trade",
                                            &sequence_id,
                                            collector_time_us,
                                            timestamp_exchange_us,
                                            &data,
                                        );
                                        parse_tracker.record_success();
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
                                        if parse_tracker.record_error() {
                                            error!(
                                                exchange = exchange_name,
                                                error_rate = "50%+",
                                                "Circuit breaker tripped - too many parse errors, reconnecting"
                                            );
                                            PARSE_CIRCUIT_BREAKS.with_label_values(&[exchange_name]).inc();
                                            break;  // Exit message loop to trigger reconnect
                                        }
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

/// Get current time in microseconds since epoch
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// Update the last message timestamp metric (integer seconds for precision)
fn update_last_message_timestamp(exchange: &str, symbol: &str) {
    // Use integer seconds to avoid floating-point precision loss
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as f64;

    LAST_MESSAGE_TIMESTAMP
        .with_label_values(&[exchange, symbol])
        .set(now_secs);
}

/// Save a snapshot to the channel for database processing
fn save_snapshot(
    db_tx: &Sender<SnapshotData>,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    exchange_sequence_id: &str,
    timestamp_collector_us: i64,
    timestamp_exchange_us: i64,
    raw_data: &str,
) {
    let data = SnapshotData {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        data_type: data_type.to_string(),
        exchange_sequence_id: exchange_sequence_id.to_string(),
        timestamp_collector: timestamp_collector_us,
        timestamp_exchange: timestamp_exchange_us,
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
