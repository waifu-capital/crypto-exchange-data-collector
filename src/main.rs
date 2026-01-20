mod config;
mod exchanges;
mod http;
mod metrics;
mod models;
mod parquet;
mod s3;
mod upload;
mod utils;
mod websocket;

use std::time::Duration;

use tokio::sync::{broadcast, mpsc::channel};
use tracing::{info, warn};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::{Config, MarketPair};
use crate::exchanges::{create_exchange, CoinbaseCredentials, ExchangeConfig, FeedType};
use crate::http::{run_http_server, run_liveness_probe};
use crate::metrics::init_metrics;
use crate::models::{new_connection_state, MarketEvent};
use crate::parquet::{parquet_worker, CompletedFile};
use crate::s3::{create_bucket_if_not_exists, create_s3_client};
use crate::upload::upload_worker;
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
    // Load .env file if present
    match dotenv::dotenv() {
        Ok(path) => eprintln!("Loaded .env from: {:?}", path),
        Err(e) => eprintln!("No .env file loaded: {}", e),
    }

    // Debug: check if Coinbase env vars are set
    eprintln!("COINBASE_API_KEY set: {}", std::env::var("COINBASE_API_KEY").is_ok());
    eprintln!("COINBASE_API_SECRET set: {}", std::env::var("COINBASE_API_SECRET").is_ok());

    // Initialize structured logging
    init_tracing();

    // Initialize metrics
    init_metrics();

    // Load configuration from TOML
    let config = Config::from_toml();

    // Clean up old log files on startup
    cleanup_old_logs("logs", config.log_retention_days);

    info!(
        market_pairs = config.market_pairs.len(),
        storage_mode = ?config.storage_mode,
        data_dir = %config.data_dir.display(),
        log_retention_days = config.log_retention_days,
        "Starting crypto exchange data collector (direct Parquet streaming)"
    );

    // Log all configured market pairs with their feeds
    for pair in &config.market_pairs {
        info!(
            exchange = %pair.exchange,
            symbol = %pair.symbol,
            feeds = ?pair.feeds,
            "Configured market pair"
        );
    }

    // Initialize AWS client (optional for local-only storage)
    let s3_client = create_s3_client(&config).await;

    // Ensure S3 bucket exists (only if S3 client is available)
    if let Some(ref client) = s3_client {
        create_bucket_if_not_exists(client, &config.bucket_name, &config.aws_region).await;
    }

    // Create channel for market events (sized for all market pairs * feeds)
    // Each (pair, feed) combination gets its own worker
    let total_workers: usize = config.market_pairs.iter().map(|p| p.feeds.len()).sum();
    let channel_size = 1000 * total_workers.max(1);
    let (event_tx, event_rx) = channel::<MarketEvent>(channel_size);

    // Create channel for completed files to upload
    let (upload_tx, upload_rx) = channel::<CompletedFile>(100);

    // Create shutdown broadcast channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Create shared connection state for health checks
    let conn_state = new_connection_state();

    // Spawn parquet worker
    let parquet_handle = {
        let data_dir = config.data_dir.clone();
        let home_server_name = config.home_server_name.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            parquet_worker(event_rx, data_dir, home_server_name, upload_tx, shutdown_rx).await;
        })
    };

    // Spawn upload worker
    let upload_handle = {
        let storage_mode = config.storage_mode;
        let local_storage_path = Some(config.local_storage_path.clone());
        let bucket_name = config.bucket_name.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            upload_worker(upload_rx, storage_mode, local_storage_path, bucket_name, s3_client, shutdown_rx).await;
        })
    };

    // WebSocket config shared by all workers
    let ws_config = WsConfig {
        message_timeout_secs: config.ws_message_timeout_secs,
        initial_retry_delay_secs: config.ws_initial_retry_delay_secs,
        max_retry_delay_secs: config.ws_max_retry_delay_secs,
    };

    // Create Coinbase credentials from config (for authenticated channels)
    let coinbase_creds = CoinbaseCredentials::new(
        config.coinbase_api_key.clone(),
        config.coinbase_api_secret.clone(),
    );
    if coinbase_creds.has_credentials() {
        info!("Coinbase API credentials detected - authenticated channels (level2) available");
    } else {
        info!("No Coinbase API credentials - only unauthenticated channels (trades) available");
    }

    // Spawn WebSocket worker for each (market pair, feed) combination
    // This isolates failure domains - if orderbook feed fails, trades continue (and vice versa)
    let mut ws_handles: Vec<(MarketPair, FeedType, tokio::task::JoinHandle<()>)> = Vec::new();
    for pair in &config.market_pairs {
        // Parse feeds for this specific market
        let market_feeds = parse_feeds(&pair.feeds);
        if market_feeds.is_empty() {
            warn!(
                exchange = %pair.exchange,
                symbol = %pair.symbol,
                configured_feeds = ?pair.feeds,
                "No valid feeds for market, skipping"
            );
            continue;
        }

        for feed in market_feeds {
            // Build exchange config with per-market base_url override
            let exchange_config = ExchangeConfig {
                coinbase_creds: coinbase_creds.clone(),
                binance_base_url: pair.base_url.clone(),
            };
            let exchange = create_exchange(&pair.exchange, &exchange_config).unwrap_or_else(|| {
                panic!(
                    "Unknown exchange: {}. Supported: binance, coinbase, upbit, okx, bybit",
                    pair.exchange
                )
            });

            let websocket_tx = event_tx.clone();
            let symbol = pair.symbol.clone();
            let feed_vec = vec![feed]; // Single feed per worker
            let ws_config = ws_config.clone();
            let conn_state = conn_state.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            let pair_clone = pair.clone();
            let data_timeout_secs = pair.data_timeout_secs;

            info!(
                exchange = %pair.exchange,
                symbol = %pair.symbol,
                feed = feed.as_str(),
                data_timeout_override = ?data_timeout_secs,
                "Spawning WebSocket worker"
            );

            let handle = tokio::spawn(async move {
                websocket_worker(exchange, websocket_tx, symbol, feed_vec, ws_config, conn_state, shutdown_rx, data_timeout_secs)
                    .await;
            });

            ws_handles.push((pair_clone, feed, handle));
        }
    }

    // Drop the original sender so parquet worker knows when all WebSocket workers are done
    drop(event_tx);

    // Spawn liveness probe
    tokio::spawn(async move {
        run_liveness_probe().await;
    });

    // Spawn metrics HTTP server
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

    // Wait for shutdown signal (Ctrl+C / SIGTERM)
    tokio::signal::ctrl_c().await.ok();
    info!("Shutdown signal received, stopping workers...");

    // Signal all workers to stop
    shutdown_tx.send(()).ok();

    // Per-worker timeouts
    let ws_timeout = Duration::from_secs(10);
    let parquet_timeout = Duration::from_secs(30); // More time to flush pending data
    let upload_timeout = Duration::from_secs(60);  // Upload may need time to finish

    // Wait for all WebSocket workers
    for (pair, feed, handle) in ws_handles {
        match tokio::time::timeout(ws_timeout, handle).await {
            Ok(Ok(_)) => info!(
                exchange = %pair.exchange,
                symbol = %pair.symbol,
                feed = feed.as_str(),
                "WebSocket worker stopped cleanly"
            ),
            Ok(Err(e)) => warn!(
                exchange = %pair.exchange,
                symbol = %pair.symbol,
                feed = feed.as_str(),
                error = %e,
                "WebSocket worker panicked"
            ),
            Err(_) => warn!(
                exchange = %pair.exchange,
                symbol = %pair.symbol,
                feed = feed.as_str(),
                timeout_secs = ws_timeout.as_secs(),
                "WebSocket worker shutdown timed out"
            ),
        }
    }

    // Wait for parquet and upload workers
    let (_, _) = tokio::join!(
        async {
            match tokio::time::timeout(parquet_timeout, parquet_handle).await {
                Ok(Ok(_)) => info!("Parquet worker stopped cleanly"),
                Ok(Err(e)) => warn!(error = %e, "Parquet worker panicked"),
                Err(_) => warn!(timeout_secs = parquet_timeout.as_secs(), "Parquet worker shutdown timed out"),
            }
        },
        async {
            match tokio::time::timeout(upload_timeout, upload_handle).await {
                Ok(Ok(_)) => info!("Upload worker stopped cleanly"),
                Ok(Err(e)) => warn!(error = %e, "Upload worker panicked"),
                Err(_) => warn!(timeout_secs = upload_timeout.as_secs(), "Upload worker shutdown timed out"),
            }
        }
    );

    info!("Collector shutdown complete");
}
