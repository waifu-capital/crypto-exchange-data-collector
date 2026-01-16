mod archive;
mod config;
mod db;
mod models;
mod utils;
mod websocket;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use tokio::sync::mpsc::channel;
use tokio::time::sleep;
use tracing::info;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::archive::{create_bucket_if_not_exists, run_archive_scheduler};
use crate::config::Config;
use crate::db::{create_pool, db_worker, init_database};
use crate::models::SnapshotData;
use crate::websocket::websocket_worker;

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
    let file_layer = fmt::layer()
        .json()
        .with_writer(file_appender);

    // Environment filter - default to info, configurable via RUST_LOG
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    // Initialize structured logging
    init_tracing();

    // Load configuration
    let config = Config::from_env();

    info!(
        market_symbol = %config.market_symbol,
        aws_region = %config.aws_region,
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
        tokio::spawn(async move {
            websocket_worker(websocket_tx, market_symbol).await;
        });
    }

    // Spawn liveness probe
    tokio::spawn(async move {
        run_liveness_probe().await;
    });

    // Run archive scheduler (blocks main task)
    run_archive_scheduler(
        db_pool,
        config.archive_dir,
        config.bucket_name,
        client,
        config.home_server_name,
    )
    .await;
}

/// Create and configure the S3 client
async fn create_s3_client(config: &Config) -> Client {
    let region_provider = RegionProviderChain::first_try(Region::new(config.aws_region.clone()))
        .or_else(Region::new("us-west-2"));

    let credentials_provider = aws_sdk_s3::config::Credentials::new(
        &config.aws_access_key,
        &config.aws_secret_key,
        None,
        None,
        "custom",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;

    Client::new(&shared_config)
}

/// Log liveness probe at regular intervals
async fn run_liveness_probe() {
    loop {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        info!(timestamp, "Liveness probe");
        sleep(Duration::from_secs(60)).await;
    }
}
