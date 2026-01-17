mod archive;
mod config;
mod db;
mod exchanges;
mod http;
mod metrics;
mod models;
mod utils;
mod websocket;

use std::time::Duration;

use tokio::sync::mpsc::channel;
use tracing::info;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::archive::{create_bucket_if_not_exists, create_s3_client, run_archive_scheduler};
use crate::config::Config;
use crate::db::{create_pool, db_worker, init_database};
use crate::exchanges::{create_exchange, FeedType};
use crate::http::{run_http_server, run_liveness_probe};
use crate::metrics::init_metrics;
use crate::models::{new_connection_state, SnapshotData};
use crate::utils::cleanup_old_logs;
use crate::websocket::{websocket_worker, WsConfig};

/// Initialize tracing with multiple outputs:
/// - Pretty format to stdout (for humans)
/// - JSON format to rolling log files (for aggregation)
fn init_tracing() {
    // Create logs directory
    std::fs::create_dir_all("logs").expect("Failed to create logs directory");

    // Rolling file appender - rotates daily
    let file_appender = RollingFileAppender::new(Rotation::DAILY, "logs", "collector.log");

    // Stdout layer - pretty format for humans
    let stdout_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false);

    // File layer - JSON format for log aggregation
    let file_layer = fmt::layer().json().with_writer(file_appender);

    // Environment filter - default to info, configurable via RUST_LOG
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();
}

/// Parse feed type strings into FeedType enum
fn parse_feeds(feeds: &[String]) -> Vec<FeedType> {
    feeds
        .iter()
        .filter_map(|f| match f.as_str() {
            "orderbook" => Some(FeedType::Orderbook),
            "trades" | "trade" => Some(FeedType::Trades),
            _ => {
                tracing::warn!(feed = %f, "Unknown feed type, skipping");
                None
            }
        })
        .collect()
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    // Initialize structured logging
    init_tracing();

    // Initialize metrics
    init_metrics();

    // Load configuration
    let config = Config::from_env();

    // Clean up old log files on startup
    cleanup_old_logs("logs", config.log_retention_days);

    // Create exchange instance
    let exchange = create_exchange(&config.exchange).unwrap_or_else(|| {
        panic!(
            "Unknown exchange: {}. Supported: binance, coinbase, upbit, okx, bybit",
            config.exchange
        )
    });

    // Parse feed types
    let feeds = parse_feeds(&config.feeds);
    if feeds.is_empty() {
        panic!("No valid feeds configured. Use FEEDS=orderbook,trades");
    }

    info!(
        exchange = %config.exchange,
        market_symbol = %config.market_symbol,
        feeds = ?feeds,
        aws_region = %config.aws_region,
        log_retention_days = config.log_retention_days,
        "Starting crypto exchange data collector"
    );

    // Initialize AWS client
    let client = create_s3_client(&config).await;

    // Ensure S3 bucket exists
    create_bucket_if_not_exists(&client, &config.bucket_name, &config.aws_region).await;

    // Initialize database
    let db_pool = create_pool(&config.database_path).await;
    init_database(&db_pool).await;

    // Create channel for snapshot data
    let (db_tx, mut db_rx) = channel::<SnapshotData>(1000);

    // Create shared connection state for health checks
    let conn_state = new_connection_state();

    // Spawn database worker
    {
        let db_pool = db_pool.clone();
        let batch_interval = config.batch_interval_secs;
        tokio::spawn(async move {
            db_worker(db_pool, &mut db_rx, batch_interval).await;
        });
    }

    // Spawn WebSocket worker
    {
        let websocket_tx = db_tx.clone();
        let market_symbol = config.market_symbol.clone();
        let ws_config = WsConfig {
            message_timeout_secs: config.ws_message_timeout_secs,
            initial_retry_delay_secs: config.ws_initial_retry_delay_secs,
            max_retry_delay_secs: config.ws_max_retry_delay_secs,
        };
        let conn_state = conn_state.clone();
        tokio::spawn(async move {
            websocket_worker(exchange, websocket_tx, market_symbol, feeds, ws_config, conn_state)
                .await;
        });
    }

    // Spawn liveness probe
    tokio::spawn(async move {
        run_liveness_probe().await;
    });

    // Spawn metrics HTTP server (port 9090 by default)
    let metrics_port = config.metrics_port;
    tokio::spawn(async move {
        run_http_server(Some(metrics_port), conn_state).await;
    });

    // Spawn periodic log cleanup task (runs once per day)
    {
        let retention_days = config.log_retention_days;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
            loop {
                interval.tick().await;
                cleanup_old_logs("logs", retention_days);
            }
        });
    }

    // Run archive scheduler (blocks main task)
    run_archive_scheduler(
        db_pool,
        config.archive_dir,
        config.bucket_name,
        client,
        config.exchange,
        config.market_symbol,
        config.home_server_name,
        config.archive_interval_secs,
    )
    .await;
}
