use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::SinkExt;
use rusqlite::Connection;
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

macro_rules! println_with_timestamp {
    ($($arg:tt)*) => {{
        let now = chrono::Utc::now();
        print!("[{}] ", now.to_rfc3339());
        println!($($arg)*);
    }}
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let aws_access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID must be set");
    let aws_secret_key = env::var("AWS_SECRET_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
    let aws_region = env::var("AWS_REGION").unwrap_or("us-west-2".to_string());

    let curr_dir = std::env::current_dir().unwrap();
    let base_path = curr_dir.join("orderbookdata");
    let market_symbol = env::var("MARKET_SYMBOL").unwrap_or("bnbusdt".to_string());
    let bucket_name = format!("binance-spot-{}", market_symbol.to_lowercase());
    let database_path = base_path.join(format!("snapshots-binance-spot-{}.db", market_symbol.to_lowercase()));
    let archive_dir = base_path.join(format!("archive-{}", market_symbol.to_lowercase()));

    std::fs::create_dir_all(&base_path).unwrap();
    std::fs::create_dir_all(&archive_dir).unwrap();

    let region_provider = RegionProviderChain::first_try(Region::new(aws_region.clone())).or_else(Region::new("us-west-2"));
    let credentials_provider = aws_sdk_s3::config::Credentials::new(aws_access_key, aws_secret_key, None, None, "custom");
    let shared_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;
    let client = Client::new(&shared_config);

    create_bucket_if_not_exists(&client, &bucket_name).await;

    let (db_tx, mut db_rx) = channel(100);
    let db_conn = Arc::new(Mutex::new(Connection::open(&database_path).unwrap()));

    {
        let db_conn = db_conn.clone();
        let batch_interval = env::var("BATCH_INTERVAL")
            .unwrap_or("60".to_string())
            .parse::<u64>()
            .unwrap_or(60);

        tokio::spawn(async move {
            db_worker(db_conn, &mut db_rx, batch_interval).await;
        });
    }

    let websocket_tx = db_tx.clone();

    tokio::spawn(async move {
        websocket_worker(websocket_tx, market_symbol).await;
    });

    tokio::spawn(async move {
        print_liveness_probe().await;
    });

    schedule_daily_task(db_conn, archive_dir, bucket_name, client).await;
}

async fn create_bucket_if_not_exists(client: &Client, bucket_name: &str) {
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => println_with_timestamp!("Bucket {} already exists.", bucket_name),
        Err(_) => {
            let create_bucket_config = aws_sdk_s3::types::CreateBucketConfiguration::builder()
                // TODO: this needs to be env AWS Region
                .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::UsWest2)
                .build();

            client.create_bucket()
                .bucket(bucket_name)
                .create_bucket_configuration(create_bucket_config)
                .send()
                .await
                .unwrap();

            println_with_timestamp!("Bucket {} created.", bucket_name);
        }
    }
}

async fn db_worker(db_conn: Arc<Mutex<Connection>>, db_rx: &mut tokio::sync::mpsc::Receiver<(String, Vec<String>)>, batch_interval: u64) {
    {
        // Ensure the snapshots table exists
        let conn = db_conn.lock().unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                lastUpdateId INTEGER NOT NULL,
                bids TEXT NOT NULL,
                asks TEXT NOT NULL
            )",
            [],
        ).unwrap();
    }

    let mut batch: Vec<(String, Vec<String>)> = Vec::new();
    let mut interval = interval(Duration::from_secs(batch_interval));

    loop {
        tokio::select! {
            Some((query, params)) = db_rx.recv() => {
                batch.push((query, params));
            },
            _ = interval.tick() => {
                if !batch.is_empty() {
                    let mut conn = db_conn.lock().unwrap();
                    let tx = conn.transaction().unwrap();

                    for (query, params) in batch.drain(..) {
                        let params_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
                        tx.execute(&query, params_refs.as_slice()).unwrap();
                    }

                    tx.commit().unwrap();
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

async fn websocket_worker(db_tx: Sender<(String, Vec<String>)>, market_symbol: String) {
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
                                    println_with_timestamp!("Received orderbook message: {:?}...", &msg.to_text().unwrap()[..50]);
                                }
                                let text = msg.into_text().unwrap();
                                let snapshot: Value = serde_json::from_str(&text).unwrap();
                                save_snapshot(db_tx.clone(), snapshot).await;
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

async fn save_snapshot(db_tx: Sender<(String, Vec<String>)>, snapshot: Value) {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
    let last_update_id = snapshot["lastUpdateId"].to_string();
    let bids = serde_json::to_string(&snapshot["bids"]).unwrap();
    let asks = serde_json::to_string(&snapshot["asks"]).unwrap();

    let query = "INSERT INTO snapshots (timestamp, lastUpdateId, bids, asks) VALUES (?, ?, ?, ?)".to_string();
    let params = vec![timestamp, last_update_id, bids, asks];

    db_tx.send((query, params)).await.unwrap();
}

async fn schedule_daily_task(db_conn: Arc<Mutex<Connection>>, archive_dir: std::path::PathBuf, bucket_name: String, client: Client) {
    loop {
        let sleep_hour = 3600;
        println_with_timestamp!("Sleeping for {} seconds (hour).", sleep_hour);
        sleep(Duration::from_secs(sleep_hour)).await;

        archive_snapshots(db_conn.clone(), archive_dir.clone(), bucket_name.clone(), client.clone()).await;
    }
}

async fn archive_snapshots(db_conn: Arc<Mutex<Connection>>, archive_dir: std::path::PathBuf, bucket_name: String, client: Client) {
    use polars::prelude::*;
    let home_server_name = env::var("HOME_SERVER_NAME").expect("HOME_SERVER_NAME must be set");

    let snapshots = {
        let conn = db_conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT timestamp, lastUpdateId, bids, asks FROM snapshots").unwrap();
        let snapshot_iter = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        }).unwrap();

        let mut snapshots = Vec::new();
        for snapshot in snapshot_iter {
            let (timestamp, last_update_id, bids, asks) = snapshot.unwrap();
            snapshots.push((
                timestamp,
                last_update_id,
                bids,
                asks,
            ));
        }
        snapshots
    };

    if !snapshots.is_empty() {
        let mut df = DataFrame::new(vec![
            Series::new("timestamp".into(), snapshots.iter().map(|s| s.0.clone()).collect::<Vec<String>>()).into(),
            Series::new("lastUpdateId".into(), snapshots.iter().map(|s| s.1).collect::<Vec<i64>>()).into(),
            Series::new("bids".into(), snapshots.iter().map(|s| s.2.clone()).collect::<Vec<String>>()).into(),
            Series::new("asks".into(), snapshots.iter().map(|s| s.3.clone()).collect::<Vec<String>>()).into(),
        ]).unwrap();

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let archive_file_name = format!("snapshots_{}_{}.parquet", home_server_name, timestamp);
        let archive_file_path = archive_dir.join(&archive_file_name);
        let mut file = std::fs::File::create(&archive_file_path).unwrap();
        ParquetWriter::new(&mut file).finish(&mut df).unwrap();
        println_with_timestamp!("Written {} snapshots to {}", snapshots.len(), archive_file_path.display());

        upload_to_s3(&archive_file_path, &bucket_name, &archive_file_name, &client).await;

        let conn = db_conn.lock().unwrap();
        conn.execute("DELETE FROM snapshots", []).unwrap();
        std::fs::remove_file(&archive_file_path).unwrap();
    }
}

async fn upload_to_s3(file_path: &std::path::Path, bucket: &str, s3_key: &str, client: &Client) {
    let body = ByteStream::from_path(file_path).await.unwrap();
    client.put_object().bucket(bucket).key(s3_key).body(body).send().await.unwrap();
    println_with_timestamp!("Successfully uploaded {} to s3://{}/{}", file_path.display(), bucket, s3_key);
}
