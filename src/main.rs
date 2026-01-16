use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::SinkExt;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use aws_sdk_s3::Client;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::config::Region;
use serde_json::Value;
use tokio::sync::mpsc::{channel, Sender};
use tokio_tungstenite::connect_async;
use futures_util::StreamExt;
use tokio::time::{sleep, interval};
use aws_sdk_s3::config::BehaviorVersion;
use rand;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;

macro_rules! println_with_timestamp {
    ($($arg:tt)*) => {{
        let now = chrono::Utc::now();
        print!("[{}] ", now.to_rfc3339());
        println!($($arg)*);
    }}
}

// Counter for dropped snapshots due to channel backpressure
static DROPPED_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);

// Data structure for snapshot messages sent through the channel
struct SnapshotData {
    exchange: String,              // "binance", "coinbase", "bybit", etc.
    symbol: String,                // "btcusdt", "BTC-USD", etc.
    data_type: String,             // "orderbook", "trade"
    exchange_sequence_id: String,  // Exchange-specific ID for deduplication
    timestamp: i64,                // Microseconds since Unix epoch (our receipt time)
    data: String,                  // JSON payload
}

async fn init_database(db_pool: &SqlitePool) {
    // Enable WAL mode for better crash recovery and write performance
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(db_pool)
        .await
        .expect("Failed to enable WAL mode");

    // Ensure the snapshots table exists
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            data_type TEXT NOT NULL,
            exchange_sequence_id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            data TEXT NOT NULL,
            UNIQUE(exchange, symbol, data_type, exchange_sequence_id)
        )"
    )
    .execute(db_pool)
    .await
    .expect("Failed to create snapshots table");

    println_with_timestamp!("Database initialized with WAL mode");
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let aws_access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID must be set");
    let aws_secret_key = env::var("AWS_SECRET_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
    let aws_region = env::var("AWS_REGION").unwrap_or("us-west-2".to_string());

    let curr_dir = std::env::current_dir().expect("Failed to get current directory");
    let base_path = curr_dir.join("orderbookdata");
    let market_symbol = env::var("MARKET_SYMBOL").unwrap_or("bnbusdt".to_string());
    let bucket_name = format!("binance-spot-{}", market_symbol.to_lowercase());
    let database_path = base_path.join(format!("snapshots-binance-spot-{}.db", market_symbol.to_lowercase()));
    let archive_dir = base_path.join(format!("archive-{}", market_symbol.to_lowercase()));

    std::fs::create_dir_all(&base_path).expect("Failed to create base data directory");
    std::fs::create_dir_all(&archive_dir).expect("Failed to create archive directory");

    let region_provider = RegionProviderChain::first_try(Region::new(aws_region.clone())).or_else(Region::new("us-west-2"));
    let credentials_provider = aws_sdk_s3::config::Credentials::new(aws_access_key, aws_secret_key, None, None, "custom");
    let shared_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;
    let client = Client::new(&shared_config);

    create_bucket_if_not_exists(&client, &bucket_name).await;

    // Create SQLite connection pool
    let db_url = format!("sqlite:{}?mode=rwc", database_path.display());
    let db_pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to create SQLite pool");

    // Initialize database schema
    init_database(&db_pool).await;

    let (db_tx, mut db_rx) = channel::<SnapshotData>(1000);  // Large buffer for burst absorption

    {
        let db_pool = db_pool.clone();
        let batch_interval = env::var("BATCH_INTERVAL")
            .unwrap_or("5".to_string())
            .parse::<u64>()
            .unwrap_or(5);

        tokio::spawn(async move {
            db_worker(db_pool, &mut db_rx, batch_interval).await;
        });
    }

    let websocket_tx = db_tx.clone();

    tokio::spawn(async move {
        websocket_worker(websocket_tx, market_symbol).await;
    });

    tokio::spawn(async move {
        print_liveness_probe().await;
    });

    schedule_daily_task(db_pool, archive_dir, bucket_name, client).await;
}

async fn create_bucket_if_not_exists(client: &Client, bucket_name: &str) {
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => println_with_timestamp!("Bucket {} already exists.", bucket_name),
        Err(_) => {
            let create_bucket_config = aws_sdk_s3::types::CreateBucketConfiguration::builder()
                // TODO: this needs to be env AWS Region
                .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::UsWest2)
                .build();

            match client.create_bucket()
                .bucket(bucket_name)
                .create_bucket_configuration(create_bucket_config)
                .send()
                .await
            {
                Ok(_) => println_with_timestamp!("Bucket {} created.", bucket_name),
                Err(e) => println_with_timestamp!("WARNING: Failed to create bucket {}: {}. Will retry on next upload.", bucket_name, e),
            }
        }
    }
}

async fn db_worker(db_pool: SqlitePool, db_rx: &mut tokio::sync::mpsc::Receiver<SnapshotData>, batch_interval: u64) {
    let mut batch: Vec<SnapshotData> = Vec::new();
    let mut interval = interval(Duration::from_secs(batch_interval));

    loop {
        tokio::select! {
            Some(snapshot) = db_rx.recv() => {
                batch.push(snapshot);
            },
            _ = interval.tick() => {
                if !batch.is_empty() {
                    let batch_size = batch.len();

                    let tx = match db_pool.begin().await {
                        Ok(tx) => tx,
                        Err(e) => {
                            println_with_timestamp!("ERROR: Failed to begin transaction: {}. Retrying next interval.", e);
                            continue;
                        }
                    };
                    let mut tx = tx;

                    let mut insert_errors = 0;
                    for snapshot in batch.drain(..) {
                        // INSERT OR IGNORE skips duplicates based on UNIQUE constraint
                        if let Err(e) = sqlx::query(
                            "INSERT OR IGNORE INTO snapshots (exchange, symbol, data_type, exchange_sequence_id, timestamp, data) VALUES (?, ?, ?, ?, ?, ?)"
                        )
                        .bind(&snapshot.exchange)
                        .bind(&snapshot.symbol)
                        .bind(&snapshot.data_type)
                        .bind(&snapshot.exchange_sequence_id)
                        .bind(snapshot.timestamp)
                        .bind(&snapshot.data)
                        .execute(&mut *tx)
                        .await {
                            insert_errors += 1;
                            println_with_timestamp!("ERROR: Failed to insert snapshot: {}", e);
                        }
                    }

                    if let Err(e) = tx.commit().await {
                        println_with_timestamp!("ERROR: Failed to commit transaction: {}. {} snapshots may be lost.", e, batch_size);
                    } else if insert_errors > 0 {
                        println_with_timestamp!("WARNING: Committed batch with {} insert errors out of {} snapshots", insert_errors, batch_size);
                    }
                }
            }
        }
    }
}

async fn print_liveness_probe() {
    loop {
        println_with_timestamp!("Liveness probe: {}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
        sleep(Duration::from_secs(60)).await;
    }
}

async fn websocket_worker(db_tx: Sender<SnapshotData>, market_symbol: String) {
    let url = format!("wss://stream.binance.com:9443/ws/{}@depth20@100ms", market_symbol.to_lowercase());
    let retry_delay = Duration::from_secs(5);

    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();

                while let Some(message) = read.next().await {
                    match message {
                        Ok(msg) => {
                            if msg.is_text() {
                                if rand::random::<u32>() % 1000 == 0 {
                                    if let Ok(preview) = msg.to_text() {
                                        let preview_len = preview.len().min(50);
                                        println_with_timestamp!("Received orderbook message: {:?}...", &preview[..preview_len]);
                                    }
                                }
                                let text = match msg.into_text() {
                                    Ok(t) => t,
                                    Err(e) => {
                                        println_with_timestamp!("ERROR: Failed to extract text from message: {}", e);
                                        continue;
                                    }
                                };
                                let snapshot: Value = match serde_json::from_str(&text) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        println_with_timestamp!("ERROR: Failed to parse JSON: {}", e);
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
                                println_with_timestamp!("Received ping frame: {:?}", msg);
                                let pong = tokio_tungstenite::tungstenite::Message::Pong(msg.into_data());
                                if let Err(e) = write.send(pong).await {
                                    println_with_timestamp!("Failed to send pong frame: {}", e);
                                    break;
                                }
                                println_with_timestamp!("Sent pong frame in response to ping.");
                            } else if msg.is_pong() {
                                println_with_timestamp!("Received pong frame: {:?}", msg);
                            } else {
                                println_with_timestamp!("Received other message: {:?}", msg);
                            }
                        },
                        Err(e) => {
                            println_with_timestamp!("WebSocket error: {}", e);
                            break;
                        }
                    }
                }
            },
            Err(e) => {
                println_with_timestamp!("Failed to connect to WebSocket: {}", e);
            }
        }

        println_with_timestamp!("Reconnecting in {} seconds...", retry_delay.as_secs());
        sleep(retry_delay).await;
    }
}

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
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64,
        data: raw_data.to_string(),
    };

    // Non-blocking send - never stall WebSocket even under backpressure
    match db_tx.try_send(data) {
        Ok(_) => {},
        Err(_) => {
            let count = DROPPED_SNAPSHOTS.fetch_add(1, Ordering::Relaxed) + 1;
            println_with_timestamp!("WARNING: Channel full, dropped snapshot (total dropped: {})", count);
        }
    }
}

async fn schedule_daily_task(db_pool: SqlitePool, archive_dir: std::path::PathBuf, bucket_name: String, client: Client) {
    loop {
        let sleep_hour = 3600;
        println_with_timestamp!("Sleeping for {} seconds (hour).", sleep_hour);
        sleep(Duration::from_secs(sleep_hour)).await;

        archive_snapshots(db_pool.clone(), archive_dir.clone(), bucket_name.clone(), client.clone()).await;
    }
}

async fn archive_snapshots(db_pool: SqlitePool, archive_dir: std::path::PathBuf, bucket_name: String, client: Client) {
    use polars::prelude::*;
    let home_server_name = match env::var("HOME_SERVER_NAME") {
        Ok(name) => name,
        Err(_) => {
            println_with_timestamp!("ERROR: HOME_SERVER_NAME not set, skipping archive");
            return;
        }
    };

    // Fetch all snapshots using sqlx
    let rows = match sqlx::query("SELECT exchange, symbol, data_type, exchange_sequence_id, timestamp, data FROM snapshots")
        .fetch_all(&db_pool)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            println_with_timestamp!("ERROR: Failed to fetch snapshots for archive: {}", e);
            return;
        }
    };

    let snapshots: Vec<(String, String, String, String, i64, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("exchange"),
                row.get::<String, _>("symbol"),
                row.get::<String, _>("data_type"),
                row.get::<String, _>("exchange_sequence_id"),
                row.get::<i64, _>("timestamp"),
                row.get::<String, _>("data"),
            )
        })
        .collect();

    if !snapshots.is_empty() {
        let mut df = match DataFrame::new(vec![
            Series::new("exchange".into(), snapshots.iter().map(|s| s.0.clone()).collect::<Vec<String>>()).into(),
            Series::new("symbol".into(), snapshots.iter().map(|s| s.1.clone()).collect::<Vec<String>>()).into(),
            Series::new("data_type".into(), snapshots.iter().map(|s| s.2.clone()).collect::<Vec<String>>()).into(),
            Series::new("exchange_sequence_id".into(), snapshots.iter().map(|s| s.3.clone()).collect::<Vec<String>>()).into(),
            Series::new("timestamp".into(), snapshots.iter().map(|s| s.4).collect::<Vec<i64>>()).into(),
            Series::new("data".into(), snapshots.iter().map(|s| s.5.clone()).collect::<Vec<String>>()).into(),
        ]) {
            Ok(df) => df,
            Err(e) => {
                println_with_timestamp!("ERROR: Failed to create DataFrame: {}", e);
                return;
            }
        };

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("System time before Unix epoch").as_millis();
        let archive_file_name = format!("snapshots_{}_{}.parquet", home_server_name, timestamp);
        let archive_file_path = archive_dir.join(&archive_file_name);

        let mut file = match std::fs::File::create(&archive_file_path) {
            Ok(f) => f,
            Err(e) => {
                println_with_timestamp!("ERROR: Failed to create archive file {}: {}", archive_file_path.display(), e);
                return;
            }
        };

        if let Err(e) = ParquetWriter::new(&mut file).finish(&mut df) {
            println_with_timestamp!("ERROR: Failed to write Parquet file {}: {}", archive_file_path.display(), e);
            // Try to clean up the partial file
            let _ = std::fs::remove_file(&archive_file_path);
            return;
        }
        println_with_timestamp!("Written {} snapshots to {}", snapshots.len(), archive_file_path.display());

        if let Err(e) = upload_to_s3(&archive_file_path, &bucket_name, &archive_file_name, &client).await {
            println_with_timestamp!("ERROR: Failed to upload to S3: {}. Will retry next cycle.", e);
            return;
        }

        // Verify upload succeeded before deleting local data
        match client.head_object().bucket(&bucket_name).key(&archive_file_name).send().await {
            Ok(_) => {
                println_with_timestamp!("Verified S3 upload: s3://{}/{}", bucket_name, archive_file_name);
                if let Err(e) = sqlx::query("DELETE FROM snapshots")
                    .execute(&db_pool)
                    .await
                {
                    println_with_timestamp!("ERROR: Failed to delete snapshots from database: {}", e);
                    // Data is safe in S3, but duplicates may occur next cycle
                }
                if let Err(e) = std::fs::remove_file(&archive_file_path) {
                    println_with_timestamp!("WARNING: Failed to remove local archive file: {}", e);
                    // Not critical - file can be cleaned up manually
                }
                println_with_timestamp!("Deleted local data after verified upload");
            },
            Err(e) => {
                println_with_timestamp!("ERROR: Failed to verify S3 upload, keeping local data: {}", e);
                // Don't delete SQLite data or local file - will retry next cycle
            }
        }
    }
}

async fn upload_to_s3(file_path: &std::path::Path, bucket: &str, s3_key: &str, client: &Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Exponential backoff: 1s, 2s, 4s, 8s, 16s (capped at 30s), 5 attempts total
    let retry_strategy = ExponentialBackoff::from_millis(1000)
        .factor(2)
        .max_delay(Duration::from_secs(30))
        .take(5);

    let file_path_owned = file_path.to_path_buf();
    let bucket_owned = bucket.to_string();
    let s3_key_owned = s3_key.to_string();

    let mut attempt = 0u32;
    let result = Retry::spawn(retry_strategy, || {
        let file_path = file_path_owned.clone();
        let bucket = bucket_owned.clone();
        let s3_key = s3_key_owned.clone();
        let client = client.clone();
        attempt += 1;

        async move {
            if attempt > 1 {
                println_with_timestamp!("S3 upload attempt {} for {}", attempt, s3_key);
            }

            let body = ByteStream::from_path(&file_path).await
                .map_err(|e| format!("Failed to read file: {}", e))?;

            client.put_object()
                .bucket(&bucket)
                .key(&s3_key)
                .body(body)
                .send()
                .await
                .map_err(|e| format!("S3 put_object failed: {}", e))?;

            Ok::<(), String>(())
        }
    }).await;

    match result {
        Ok(_) => {
            if attempt > 1 {
                println_with_timestamp!("Successfully uploaded {} to s3://{}/{} after {} attempts", file_path.display(), bucket, s3_key, attempt);
            } else {
                println_with_timestamp!("Successfully uploaded {} to s3://{}/{}", file_path.display(), bucket, s3_key);
            }
            Ok(())
        }
        Err(e) => Err(format!("S3 upload failed after {} attempts: {}", attempt, e).into())
    }
}
