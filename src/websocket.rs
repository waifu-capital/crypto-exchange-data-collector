use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

/// Grace period after subscription before checking for data timeout.
/// This allows time for subscription confirmation and first data message.
const SUBSCRIPTION_GRACE_PERIOD: Duration = Duration::from_secs(30);

use crate::exchanges::{Exchange, ExchangeMessage, FeedType};
use crate::metrics::{
    LAST_MESSAGE_TIMESTAMP, LATENCY_EXCHANGE_TO_COLLECTOR, MESSAGES_DROPPED, MESSAGES_RECEIVED,
    MESSAGE_TIMEOUTS, PARSE_CIRCUIT_BREAKS, WEBSOCKET_CONNECTED, WEBSOCKET_PINGS_SENT,
    WEBSOCKET_PONGS_RECEIVED, WEBSOCKET_RECONNECTS,
};
use crate::models::{ConnectionState, DataType, MarketEvent};

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
    feed: &str,
    connected: bool,
) {
    let key = format!("{}:{}:{}", exchange, symbol, feed);
    conn_state.write().await.insert(key, connected);
}

/// WebSocket worker that connects to an exchange and streams market data.
///
/// This is a generic worker that works with any exchange implementing the `Exchange` trait.
/// It handles connection management, message parsing, and forwards data to the database channel.
#[allow(clippy::too_many_arguments)]
pub async fn websocket_worker(
    exchange: Box<dyn Exchange>,
    db_tx: Sender<MarketEvent>,
    symbol: String,
    feeds: Vec<FeedType>,
    ws_config: WsConfig,
    conn_state: ConnectionState,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    data_timeout_override_secs: Option<u64>,
) {
    let exchange_name = exchange.name();
    let normalized_symbol = exchange.normalize_symbol(&symbol);
    let url = exchange.websocket_url(&symbol);
    // Use per-market data timeout if provided, otherwise fall back to global config
    let data_timeout_secs = data_timeout_override_secs.unwrap_or(ws_config.message_timeout_secs);
    let message_timeout = Duration::from_secs(data_timeout_secs);
    let mut backoff = ExponentialBackoff::new(
        ws_config.initial_retry_delay_secs,
        ws_config.max_retry_delay_secs,
    );

    // Each worker now handles exactly one feed type (isolated failure domains)
    let feed_type = feeds.first().expect("Worker must have at least one feed");
    let feed_str = feed_type.as_str();

    // Parse error tracker for circuit breaker
    let mut parse_tracker = ParseErrorTracker::new();

    info!(
        exchange = exchange_name,
        symbol = %normalized_symbol,
        feed = feed_str,
        url = %url,
        "Starting WebSocket worker"
    );

    loop {
        // Check for shutdown signal before attempting to connect
        if shutdown_rx.try_recv().is_ok() {
            info!(
                exchange = exchange_name,
                symbol = %normalized_symbol,
                feed = feed_str,
                "WebSocket worker received shutdown signal"
            );
            break;
        }

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!(
                    exchange = exchange_name,
                    symbol = %normalized_symbol,
                    feed = feed_str,
                    "Connected to WebSocket"
                );
                WEBSOCKET_CONNECTED
                    .with_label_values(&[exchange_name, &normalized_symbol])
                    .set(1.0);
                set_connection_status(&conn_state, exchange_name, &normalized_symbol, feed_str, true).await;
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

                // Track when we last received actual data (orderbook/trade), not control messages
                // None until first data arrives - this allows a grace period for subscription
                let mut last_data_received: Option<Instant> = None;
                let mut data_timeout_warned = false;
                let subscription_sent = Instant::now();

                // Get exchange-specific ping interval (None = server initiates pings)
                let ping_interval = exchange.ping_interval();

                // Track when we last sent a ping (for exchanges requiring client pings)
                // This is used as a backup check after each message in case select! doesn't
                // pick the timer often enough during sustained high message volume
                let mut last_ping_sent = Instant::now();

                // Create ping timer only if this exchange requires client-initiated pings
                let mut ping_timer = ping_interval.map(|d| {
                    let mut timer = tokio::time::interval(d);
                    // Reset the timer to avoid immediate first tick
                    timer.reset();
                    timer
                });

                loop {
                    // Check for shutdown signal
                    if shutdown_rx.try_recv().is_ok() {
                        info!(
                            exchange = exchange_name,
                            symbol = %normalized_symbol,
                            feed = feed_str,
                            "WebSocket worker received shutdown signal, closing connection"
                        );
                        WEBSOCKET_CONNECTED
                            .with_label_values(&[exchange_name, &normalized_symbol])
                            .set(0.0);
                        set_connection_status(&conn_state, exchange_name, &normalized_symbol, feed_str, false).await;
                        // Close the websocket cleanly
                        if let Err(e) = write.close().await {
                            debug!(error = %e, "Error closing WebSocket connection");
                        }
                        info!(
                            exchange = exchange_name,
                            symbol = %normalized_symbol,
                            feed = feed_str,
                            "WebSocket worker stopped"
                        );
                        return;
                    }

                    // Check for data timeout
                    // Two cases: never received data (subscription failed) vs data stopped flowing
                    let should_reconnect = match last_data_received {
                        Some(last) => {
                            // Have received data before - check if it stopped
                            let elapsed = last.elapsed();
                            if elapsed > message_timeout && !data_timeout_warned {
                                warn!(
                                    exchange = exchange_name,
                                    symbol = %normalized_symbol,
                                    feed = feed_str,
                                    elapsed_secs = elapsed.as_secs(),
                                    "No data received - connection may be stale"
                                );
                                data_timeout_warned = true;
                            }
                            // Reconnect if no data for 3x timeout
                            elapsed > message_timeout * 3
                        }
                        None => {
                            // Never received data - check grace period
                            let since_subscription = subscription_sent.elapsed();
                            if since_subscription > SUBSCRIPTION_GRACE_PERIOD && !data_timeout_warned {
                                warn!(
                                    exchange = exchange_name,
                                    symbol = %normalized_symbol,
                                    feed = feed_str,
                                    elapsed_secs = since_subscription.as_secs(),
                                    "No data received after subscription - subscription may have failed"
                                );
                                data_timeout_warned = true;
                            }
                            // Reconnect if no data after extended grace period (3x normal timeout from subscription)
                            since_subscription > message_timeout * 3
                        }
                    };

                    if should_reconnect {
                        error!(
                            exchange = exchange_name,
                            symbol = %normalized_symbol,
                            feed = feed_str,
                            "Data timeout - reconnecting"
                        );
                        MESSAGE_TIMEOUTS
                            .with_label_values(&[exchange_name, &normalized_symbol])
                            .inc();
                        break;
                    }

                    // Use different strategies based on whether exchange needs client pings
                    // NOTE: We deliberately do NOT use `biased` in select! because it caused
                    // the ping timer to be starved under high message volume, leading to
                    // "Connection reset without closing handshake" errors on OKX/Coinbase.
                    let msg_result = if let Some(ref mut timer) = ping_timer {
                        // Exchange requires client-initiated pings - use select! to handle both
                        // Without `biased`, tokio fairly schedules between read and timer
                        tokio::select! {
                            result = read.next() => result,
                            _ = timer.tick() => {
                                // Send exchange-specific ping for keepalive
                                if let Some(ping_msg) = exchange.build_ping_message() {
                                    if let Err(e) = write.send(ping_msg).await {
                                        error!(
                                            exchange = exchange_name,
                                            symbol = %normalized_symbol,
                                            error = %e,
                                            "Failed to send ping, reconnecting"
                                        );
                                        break;
                                    }
                                    WEBSOCKET_PINGS_SENT
                                        .with_label_values(&[exchange_name, &normalized_symbol])
                                        .inc();
                                    debug!(
                                        exchange = exchange_name,
                                        symbol = %normalized_symbol,
                                        "Sent keepalive ping"
                                    );
                                    last_ping_sent = Instant::now();
                                }
                                continue;
                            }
                        }
                    } else {
                        // Server initiates pings - simple blocking read (no timeout wrapper needed)
                        read.next().await
                    };

                    // Process the message
                    match msg_result {
                        Some(Ok(msg)) => {
                            if msg.is_text() {
                                let text = match msg.into_text() {
                                    Ok(t) => t,
                                    Err(e) => {
                                        error!(error = %e, "Failed to extract text from message");
                                        continue;
                                    }
                                };

                                // Sample log ~0.1% of messages at debug level
                                if rand::random::<u32>().is_multiple_of(1000) {
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
                                        symbol: _sym,
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

                                        save_event(
                                            &db_tx,
                                            exchange_name,
                                            &normalized_symbol,
                                            DataType::Orderbook,
                                            &sequence_id,
                                            collector_time_us,
                                            timestamp_exchange_us,
                                            &data,
                                        );
                                        parse_tracker.record_success();
                                        // Reset data timeout - we received actual data
                                        last_data_received = Some(Instant::now());
                                        data_timeout_warned = false;
                                    }
                                    Ok(ExchangeMessage::Trade {
                                        symbol: _sym,
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

                                        save_event(
                                            &db_tx,
                                            exchange_name,
                                            &normalized_symbol,
                                            DataType::Trade,
                                            &sequence_id,
                                            collector_time_us,
                                            timestamp_exchange_us,
                                            &data,
                                        );
                                        parse_tracker.record_success();
                                        // Reset data timeout - we received actual data
                                        last_data_received = Some(Instant::now());
                                        data_timeout_warned = false;
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
                                        debug!(exchange = exchange_name, symbol = %normalized_symbol, "Received app-level pong");
                                        WEBSOCKET_PONGS_RECEIVED
                                            .with_label_values(&[exchange_name, &normalized_symbol])
                                            .inc();
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
                                debug!(exchange = exchange_name, symbol = %normalized_symbol, "Received protocol pong frame");
                                WEBSOCKET_PONGS_RECEIVED
                                    .with_label_values(&[exchange_name, &normalized_symbol])
                                    .inc();
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
                        Some(Err(e)) => {
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
                        None => {
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
                    }

                    // Backup ping check after processing each message
                    // This handles edge cases where select! doesn't pick the timer often enough
                    // during sustained high message volume (belt-and-suspenders approach)
                    if let Some(interval) = ping_interval
                        && last_ping_sent.elapsed() >= interval
                            && let Some(ping_msg) = exchange.build_ping_message() {
                                if let Err(e) = write.send(ping_msg).await {
                                    error!(
                                        exchange = exchange_name,
                                        symbol = %normalized_symbol,
                                        error = %e,
                                        "Failed to send ping, reconnecting"
                                    );
                                    break;
                                }
                                WEBSOCKET_PINGS_SENT
                                    .with_label_values(&[exchange_name, &normalized_symbol])
                                    .inc();
                                debug!(
                                    exchange = exchange_name,
                                    symbol = %normalized_symbol,
                                    "Sent backup keepalive ping"
                                );
                                last_ping_sent = Instant::now();
                            }
                }
            }
            Err(e) => {
                error!(
                    exchange = exchange_name,
                    url = %url,
                    feed = feed_str,
                    error = %e,
                    "Failed to connect to WebSocket"
                );
            }
        }

        // Mark connection as down before attempting reconnect
        set_connection_status(&conn_state, exchange_name, &normalized_symbol, feed_str, false).await;

        let delay = backoff.next_delay();
        WEBSOCKET_RECONNECTS
            .with_label_values(&[exchange_name, &normalized_symbol])
            .inc();
        info!(
            exchange = exchange_name,
            symbol = %normalized_symbol,
            feed = feed_str,
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

/// Save a market event to the channel for database processing
#[allow(clippy::too_many_arguments)]
fn save_event(
    db_tx: &Sender<MarketEvent>,
    exchange: &str,
    symbol: &str,
    data_type: DataType,
    exchange_sequence_id: &str,
    timestamp_collector_us: i64,
    timestamp_exchange_us: i64,
    raw_data: &str,
) {
    let event = MarketEvent {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        data_type,
        exchange_sequence_id: exchange_sequence_id.to_string(),
        timestamp_collector: timestamp_collector_us,
        timestamp_exchange: timestamp_exchange_us,
        data: raw_data.to_string(),
    };

    // Non-blocking send - never stall WebSocket even under backpressure
    match db_tx.try_send(event) {
        Ok(_) => {}
        Err(_) => {
            MESSAGES_DROPPED.inc();
            warn!(
                total_dropped = MESSAGES_DROPPED.get() as u64,
                "Channel full, dropped event"
            );
        }
    }
}
