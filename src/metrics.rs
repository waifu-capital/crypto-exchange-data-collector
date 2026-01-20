use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec,
    register_histogram_vec, Counter, CounterVec, Gauge, GaugeVec, HistogramVec,
};

lazy_static! {
    // ===================
    // WebSocket Metrics
    // ===================

    /// WebSocket connection status (1=connected, 0=disconnected)
    pub static ref WEBSOCKET_CONNECTED: GaugeVec = register_gauge_vec!(
        "collector_websocket_connected",
        "WebSocket connection status (1=connected, 0=disconnected)",
        &["exchange", "symbol"]
    ).expect("Failed to register WEBSOCKET_CONNECTED");

    /// Total WebSocket reconnection attempts
    pub static ref WEBSOCKET_RECONNECTS: CounterVec = register_counter_vec!(
        "collector_websocket_reconnects_total",
        "Total WebSocket reconnection attempts",
        &["exchange", "symbol"]
    ).expect("Failed to register WEBSOCKET_RECONNECTS");

    /// Unix timestamp of last message received per exchange/symbol
    pub static ref LAST_MESSAGE_TIMESTAMP: GaugeVec = register_gauge_vec!(
        "collector_last_message_timestamp_seconds",
        "Unix timestamp of last message received",
        &["exchange", "symbol"]
    ).expect("Failed to register LAST_MESSAGE_TIMESTAMP");

    /// Total messages received from WebSocket
    pub static ref MESSAGES_RECEIVED: CounterVec = register_counter_vec!(
        "collector_messages_received_total",
        "Total messages received from WebSocket",
        &["exchange", "symbol", "data_type"]
    ).expect("Failed to register MESSAGES_RECEIVED");

    /// Total messages dropped due to backpressure
    pub static ref MESSAGES_DROPPED: Counter = register_counter!(
        "collector_messages_dropped_total",
        "Total messages dropped due to channel backpressure"
    ).expect("Failed to register MESSAGES_DROPPED");

    /// Message timeout events (stale connection detected)
    pub static ref MESSAGE_TIMEOUTS: CounterVec = register_counter_vec!(
        "collector_message_timeouts_total",
        "Message timeout events indicating stale connections",
        &["exchange", "symbol"]
    ).expect("Failed to register MESSAGE_TIMEOUTS");

    /// Total pings sent by collector for connection keepalive
    pub static ref WEBSOCKET_PINGS_SENT: CounterVec = register_counter_vec!(
        "collector_websocket_pings_sent_total",
        "Total ping messages sent by collector for keepalive",
        &["exchange", "symbol"]
    ).expect("Failed to register WEBSOCKET_PINGS_SENT");

    /// Total pongs received (responses to our pings)
    pub static ref WEBSOCKET_PONGS_RECEIVED: CounterVec = register_counter_vec!(
        "collector_websocket_pongs_received_total",
        "Total pong responses received from exchanges",
        &["exchange", "symbol"]
    ).expect("Failed to register WEBSOCKET_PONGS_RECEIVED");

    // ===================
    // Parquet Writer Metrics
    // ===================

    /// Total rows written to Parquet files
    pub static ref PARQUET_ROWS_WRITTEN: Counter = register_counter!(
        "collector_parquet_rows_written_total",
        "Total rows written to Parquet files"
    ).expect("Failed to register PARQUET_ROWS_WRITTEN");

    /// Current rows buffered (not yet flushed to Parquet)
    pub static ref PARQUET_ROWS_BUFFERED: Gauge = register_gauge!(
        "collector_parquet_rows_buffered",
        "Current rows buffered waiting to be written"
    ).expect("Failed to register PARQUET_ROWS_BUFFERED");

    /// Total Parquet files rotated (completed)
    pub static ref PARQUET_FILES_ROTATED: CounterVec = register_counter_vec!(
        "collector_parquet_files_rotated_total",
        "Total Parquet files rotated",
        &["exchange", "symbol"]
    ).expect("Failed to register PARQUET_FILES_ROTATED");

    /// Parquet write errors
    pub static ref PARQUET_WRITE_ERRORS: Counter = register_counter!(
        "collector_parquet_write_errors_total",
        "Total Parquet write errors"
    ).expect("Failed to register PARQUET_WRITE_ERRORS");

    /// Current channel queue depth (messages waiting for parquet worker)
    pub static ref CHANNEL_QUEUE_DEPTH: Gauge = register_gauge!(
        "collector_channel_queue_depth",
        "Current number of messages waiting in channel buffer"
    ).expect("Failed to register CHANNEL_QUEUE_DEPTH");

    // ===================
    // Upload Metrics
    // ===================

    /// S3 upload duration in seconds
    pub static ref S3_UPLOAD_DURATION: HistogramVec = register_histogram_vec!(
        "collector_s3_upload_seconds",
        "Time spent uploading to S3",
        &["status"],
        vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
    ).expect("Failed to register S3_UPLOAD_DURATION");

    /// S3 upload failures
    pub static ref S3_UPLOAD_FAILURES: Counter = register_counter!(
        "collector_s3_upload_failures_total",
        "Total S3 upload failures"
    ).expect("Failed to register S3_UPLOAD_FAILURES");

    /// S3 upload retries
    pub static ref S3_UPLOAD_RETRIES: Counter = register_counter!(
        "collector_s3_upload_retries_total",
        "Total S3 upload retry attempts"
    ).expect("Failed to register S3_UPLOAD_RETRIES");

    /// Current upload queue depth (files waiting for upload)
    pub static ref UPLOAD_QUEUE_DEPTH: Gauge = register_gauge!(
        "collector_upload_queue_depth",
        "Current number of files waiting to be uploaded"
    ).expect("Failed to register UPLOAD_QUEUE_DEPTH");

    // ===================
    // Parse Metrics
    // ===================

    /// Parse error circuit breaker activations
    pub static ref PARSE_CIRCUIT_BREAKS: CounterVec = register_counter_vec!(
        "collector_parse_circuit_breaks_total",
        "Parse error circuit breaker activations",
        &["exchange"]
    ).expect("Failed to register PARSE_CIRCUIT_BREAKS");

    // ===================
    // Latency Metrics
    // ===================

    /// Latency from exchange timestamp to collector receipt (milliseconds)
    pub static ref LATENCY_EXCHANGE_TO_COLLECTOR: HistogramVec = register_histogram_vec!(
        "collector_latency_exchange_to_collector_ms",
        "Latency from exchange timestamp to collector receipt",
        &["exchange", "symbol", "data_type"],
        vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0]
    ).expect("Failed to register LATENCY_EXCHANGE_TO_COLLECTOR");

    // ===================
    // Application Metrics
    // ===================

    /// Application start timestamp
    pub static ref APP_START_TIMESTAMP: Gauge = register_gauge!(
        "collector_start_timestamp_seconds",
        "Unix timestamp when the collector started"
    ).expect("Failed to register APP_START_TIMESTAMP");
}

/// Initialize metrics that need startup values
pub fn init_metrics() {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Use integer seconds to avoid floating-point precision loss
    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_secs() as f64;

    APP_START_TIMESTAMP.set(start_time);
}
