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

    // ===================
    // Database Metrics
    // ===================

    /// Current number of messages in the channel buffer
    pub static ref CHANNEL_QUEUE_DEPTH: Gauge = register_gauge!(
        "collector_channel_queue_depth",
        "Current number of messages waiting in channel buffer"
    ).expect("Failed to register CHANNEL_QUEUE_DEPTH");

    /// Database batch write duration in seconds
    pub static ref DB_WRITE_DURATION: HistogramVec = register_histogram_vec!(
        "collector_db_write_seconds",
        "Time spent writing batches to SQLite",
        &["operation"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
    ).expect("Failed to register DB_WRITE_DURATION");

    /// Total snapshots written to database
    pub static ref DB_SNAPSHOTS_WRITTEN: Counter = register_counter!(
        "collector_db_snapshots_written_total",
        "Total snapshots successfully written to database"
    ).expect("Failed to register DB_SNAPSHOTS_WRITTEN");

    /// Database insert errors by type
    pub static ref DB_INSERT_ERRORS: CounterVec = register_counter_vec!(
        "collector_db_insert_errors_total",
        "Database insert errors by type",
        &["error_type"]  // "duplicate", "constraint", "io", "other"
    ).expect("Failed to register DB_INSERT_ERRORS");

    // ===================
    // Archive Metrics
    // ===================

    /// Total archive cycles completed
    pub static ref ARCHIVES_COMPLETED: Counter = register_counter!(
        "collector_archives_completed_total",
        "Total archive cycles completed successfully"
    ).expect("Failed to register ARCHIVES_COMPLETED");

    /// Total snapshots archived
    pub static ref SNAPSHOTS_ARCHIVED: Counter = register_counter!(
        "collector_snapshots_archived_total",
        "Total snapshots archived to Parquet/S3"
    ).expect("Failed to register SNAPSHOTS_ARCHIVED");

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

    // ===================
    // Sequence Metrics
    // ===================

    /// Total sequence gaps detected
    pub static ref SEQUENCE_GAPS: CounterVec = register_counter_vec!(
        "collector_sequence_gaps_total",
        "Total sequence gaps detected",
        &["exchange", "symbol", "data_type"]
    ).expect("Failed to register SEQUENCE_GAPS");

    /// Size distribution of detected gaps
    pub static ref SEQUENCE_GAP_SIZE: HistogramVec = register_histogram_vec!(
        "collector_sequence_gap_size",
        "Size of detected sequence gaps",
        &["exchange", "symbol", "data_type"],
        vec![1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]
    ).expect("Failed to register SEQUENCE_GAP_SIZE");

    /// Out-of-order messages detected
    pub static ref SEQUENCE_OUT_OF_ORDER: CounterVec = register_counter_vec!(
        "collector_sequence_out_of_order_total",
        "Out-of-order messages detected",
        &["exchange", "symbol", "data_type"]
    ).expect("Failed to register SEQUENCE_OUT_OF_ORDER");

    /// Duplicate sequence IDs detected
    pub static ref SEQUENCE_DUPLICATES: CounterVec = register_counter_vec!(
        "collector_sequence_duplicates_total",
        "Duplicate sequence IDs detected",
        &["exchange", "symbol", "data_type"]
    ).expect("Failed to register SEQUENCE_DUPLICATES");

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
    // Archive Failure Metrics
    // ===================

    /// Archive failures by stage
    pub static ref ARCHIVE_FAILURES: CounterVec = register_counter_vec!(
        "collector_archive_failures_total",
        "Archive failures by stage",
        &["stage"]  // "fetch", "parquet", "upload", "verify_size", "verify_head", "timeout"
    ).expect("Failed to register ARCHIVE_FAILURES");

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
