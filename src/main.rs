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

use crate::archive::{create_bucket_if_not_exists, run_archive_scheduler};
use crate::config::Config;
use crate::db::{create_pool, db_worker, init_database};
use crate::models::SnapshotData;
use crate::websocket::websocket_worker;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    // Load configuration
    let config = Config::from_env();

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
        print_liveness_probe().await;
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

/// Print liveness probe at regular intervals
async fn print_liveness_probe() {
    loop {
        println_with_timestamp!(
            "Liveness probe: {}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        sleep(Duration::from_secs(60)).await;
    }
}
