use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

/// Track last successfully archived max timestamp to prevent duplicates
static LAST_ARCHIVE_MAX_TIMESTAMP: AtomicI64 = AtomicI64::new(0);
use chrono::Utc;
use polars::prelude::*;
use sqlx::sqlite::SqlitePool;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::db::{
    delete_all_orderbooks, delete_all_trades, fetch_all_orderbooks, fetch_all_trades, DbRow,
};
use crate::metrics::{
    ARCHIVE_FAILURES, ARCHIVES_COMPLETED, S3_UPLOAD_DURATION, S3_UPLOAD_FAILURES,
    S3_UPLOAD_RETRIES, SNAPSHOTS_ARCHIVED,
};
use crate::models::DataType;

/// Create and configure the S3 client
pub async fn create_s3_client(config: &Config) -> Client {
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

/// Create S3 bucket if it doesn't exist
pub async fn create_bucket_if_not_exists(client: &Client, bucket_name: &str, region: &str) {
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => info!(bucket = bucket_name, "Bucket already exists"),
        Err(_) => {
            // us-east-1 is the default region and doesn't use location constraint
            let create_bucket_config = if region == "us-east-1" {
                None
            } else {
                Some(
                    aws_sdk_s3::types::CreateBucketConfiguration::builder()
                        .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                            region,
                        ))
                        .build(),
                )
            };

            let mut request = client.create_bucket().bucket(bucket_name);
            if let Some(config) = create_bucket_config {
                request = request.create_bucket_configuration(config);
            }

            match request.send().await {
                Ok(_) => info!(bucket = bucket_name, region, "Bucket created"),
                Err(e) => warn!(
                    bucket = bucket_name,
                    error = %e,
                    "Failed to create bucket (may already exist or require manual creation)"
                ),
            }
        }
    }
}

/// Schedule and run archive task at regular intervals
/// Archives ALL data from all exchanges and symbols
pub async fn run_archive_scheduler(
    db_pool: SqlitePool,
    archive_dir: PathBuf,
    bucket_name: String,
    client: Client,
    home_server_name: Option<String>,
    archive_interval_secs: u64,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    // Archive timeout (5 minutes max per cycle)
    let archive_timeout = Duration::from_secs(300);

    loop {
        // Check for shutdown before starting archive
        if shutdown_rx.try_recv().is_ok() {
            info!("Archive scheduler received shutdown signal");
            break;
        }

        // Run archive with timeout
        match tokio::time::timeout(
            archive_timeout,
            archive_all_data(&db_pool, &archive_dir, &bucket_name, &client, home_server_name.as_deref()),
        )
        .await
        {
            Ok(_) => { /* success */ }
            Err(_) => {
                ARCHIVE_FAILURES.with_label_values(&["timeout"]).inc();
                error!("Archive operation timed out after 5 minutes");
            }
        }

        // Sleep with shutdown check
        let sleep_duration = Duration::from_secs(archive_interval_secs);
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Archive scheduler received shutdown signal during sleep");
                break;
            }
            _ = sleep(sleep_duration) => {}
        }
    }

    info!("Archive scheduler stopped");
}

/// Archive all data from both orderbooks and trades tables
/// Groups data by (exchange, symbol, data_type) and creates separate Parquet files
pub async fn archive_all_data(
    db_pool: &SqlitePool,
    archive_dir: &Path,
    bucket_name: &str,
    client: &Client,
    home_server_name: Option<&str>,
) {
    // Fetch from both tables
    let orderbooks = match fetch_all_orderbooks(db_pool).await {
        Ok(rows) => rows,
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["fetch"]).inc();
            error!(error = %e, "Failed to fetch orderbooks for archive");
            return;
        }
    };

    let trades = match fetch_all_trades(db_pool).await {
        Ok(rows) => rows,
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["fetch"]).inc();
            error!(error = %e, "Failed to fetch trades for archive");
            return;
        }
    };

    if orderbooks.is_empty() && trades.is_empty() {
        info!("No data to archive");
        return;
    }

    // Combine all rows
    let all_rows: Vec<DbRow> = orderbooks.into_iter().chain(trades.into_iter()).collect();
    let total_count = all_rows.len();

    // Get max timestamp for duplicate prevention
    let max_collector_ts = all_rows
        .iter()
        .map(|r| r.timestamp_collector)
        .max()
        .unwrap_or(0);
    let last_archived = LAST_ARCHIVE_MAX_TIMESTAMP.load(Ordering::SeqCst);

    // Check for duplicate archive scenario
    if max_collector_ts > 0 && max_collector_ts <= last_archived {
        warn!(
            last_archived,
            current_max = max_collector_ts,
            "Skipping archive - data appears to already be archived (retry scenario)"
        );
        // Delete from DB since data is already in S3
        if let Err(e) = delete_all_orderbooks(db_pool).await {
            error!(error = %e, "Failed to clean up already-archived orderbooks");
        }
        if let Err(e) = delete_all_trades(db_pool).await {
            error!(error = %e, "Failed to clean up already-archived trades");
        }
        return;
    }

    // Group rows by (exchange, symbol, data_type)
    let mut groups: HashMap<(String, String, DataType), Vec<DbRow>> = HashMap::new();
    for row in all_rows {
        let key = (row.exchange.clone(), row.symbol.clone(), row.data_type);
        groups.entry(key).or_default().push(row);
    }

    info!(
        total_count,
        groups = groups.len(),
        "Archiving data in groups"
    );

    // Process each group
    let mut all_uploads_succeeded = true;
    for ((exchange, symbol, data_type), rows) in groups {
        let success = archive_group(
            &rows,
            &exchange,
            &symbol,
            data_type,
            archive_dir,
            bucket_name,
            client,
            home_server_name,
        )
        .await;

        if !success {
            all_uploads_succeeded = false;
        }
    }

    // Only delete from DB if ALL uploads succeeded
    if all_uploads_succeeded {
        if let Err(e) = delete_all_orderbooks(db_pool).await {
            error!(error = %e, "Failed to delete orderbooks from database");
        }
        if let Err(e) = delete_all_trades(db_pool).await {
            error!(error = %e, "Failed to delete trades from database");
        }
        // Update last archived timestamp after successful delete
        LAST_ARCHIVE_MAX_TIMESTAMP.store(max_collector_ts, Ordering::SeqCst);
        info!(total_count, "Successfully archived and cleaned up all data");
    } else {
        warn!("Some uploads failed, keeping data in database for retry");
    }
}

/// Archive a single group of rows (same exchange/symbol/data_type)
async fn archive_group(
    rows: &[DbRow],
    exchange: &str,
    symbol: &str,
    data_type: DataType,
    archive_dir: &Path,
    bucket_name: &str,
    client: &Client,
    home_server_name: Option<&str>,
) -> bool {
    let row_count = rows.len();
    let data_type_str = data_type.as_str();

    // Extract columns
    let exchanges: Vec<String> = rows.iter().map(|r| r.exchange.clone()).collect();
    let symbols: Vec<String> = rows.iter().map(|r| r.symbol.clone()).collect();
    let seq_ids: Vec<String> = rows.iter().map(|r| r.exchange_sequence_id.clone()).collect();
    let timestamps_collector: Vec<i64> = rows.iter().map(|r| r.timestamp_collector).collect();
    let timestamps_exchange: Vec<i64> = rows.iter().map(|r| r.timestamp_exchange).collect();
    let data_col: Vec<String> = rows.iter().map(|r| r.data.clone()).collect();

    // Create DataFrame
    let mut df = match DataFrame::new(vec![
        Column::new("exchange".into(), exchanges),
        Column::new("symbol".into(), symbols),
        Column::new("exchange_sequence_id".into(), seq_ids),
        Column::new("timestamp_collector".into(), timestamps_collector),
        Column::new("timestamp_exchange".into(), timestamps_exchange),
        Column::new("data".into(), data_col),
    ]) {
        Ok(df) => df,
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["parquet"]).inc();
            error!(
                error = %e,
                exchange,
                symbol,
                data_type = data_type_str,
                "Failed to create DataFrame"
            );
            return false;
        }
    };

    // Build S3 key with hierarchical structure
    // Format: {exchange}/{symbol}/{data_type}/[{server}/]{date}/{timestamp}.parquet
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_millis();
    let date = Utc::now().format("%Y-%m-%d");

    let s3_key = match home_server_name {
        Some(server) => format!(
            "{}/{}/{}/{}/{}/{}.parquet",
            exchange, symbol, data_type_str, server, date, timestamp
        ),
        None => format!(
            "{}/{}/{}/{}/{}.parquet",
            exchange, symbol, data_type_str, date, timestamp
        ),
    };

    // Local file name (flat, for temporary storage)
    let local_file_name = format!("{}_{}_{}.parquet", exchange, symbol, timestamp);
    let archive_file_path = archive_dir.join(&local_file_name);

    let mut file = match std::fs::File::create(&archive_file_path) {
        Ok(f) => f,
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["parquet"]).inc();
            error!(
                error = %e,
                path = %archive_file_path.display(),
                "Failed to create archive file"
            );
            return false;
        }
    };

    if let Err(e) = ParquetWriter::new(&mut file).finish(&mut df) {
        ARCHIVE_FAILURES.with_label_values(&["parquet"]).inc();
        error!(
            error = %e,
            path = %archive_file_path.display(),
            "Failed to write Parquet file"
        );
        let _ = std::fs::remove_file(&archive_file_path);
        return false;
    }

    // Get local file size for verification
    let local_file_size = match std::fs::metadata(&archive_file_path) {
        Ok(m) => m.len(),
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["parquet"]).inc();
            error!(error = %e, "Failed to get local file size");
            return false;
        }
    };

    info!(
        row_count,
        exchange,
        symbol,
        data_type = data_type_str,
        s3_key = %s3_key,
        "Written group to Parquet file"
    );

    // Upload to S3
    let upload_start = Instant::now();
    if let Err(e) = upload_to_s3(&archive_file_path, bucket_name, &s3_key, client).await {
        ARCHIVE_FAILURES.with_label_values(&["upload"]).inc();
        error!(error = %e, "Failed to upload to S3, will retry next cycle");
        S3_UPLOAD_FAILURES.inc();
        S3_UPLOAD_DURATION
            .with_label_values(&["failure"])
            .observe(upload_start.elapsed().as_secs_f64());
        return false;
    }
    S3_UPLOAD_DURATION
        .with_label_values(&["success"])
        .observe(upload_start.elapsed().as_secs_f64());

    // Verify upload succeeded with size check
    match client
        .head_object()
        .bucket(bucket_name)
        .key(&s3_key)
        .send()
        .await
    {
        Ok(head_resp) => {
            let remote_size = head_resp.content_length().unwrap_or(0) as u64;

            if remote_size != local_file_size {
                ARCHIVE_FAILURES.with_label_values(&["verify_size"]).inc();
                error!(
                    local_size = local_file_size,
                    remote_size,
                    s3_key = %s3_key,
                    "Size mismatch after S3 upload"
                );
                return false;
            }

            info!(
                bucket = bucket_name,
                key = %s3_key,
                size = remote_size,
                "Verified S3 upload"
            );

            // Update metrics
            ARCHIVES_COMPLETED.inc();
            SNAPSHOTS_ARCHIVED.inc_by(row_count as f64);

            // Clean up local file
            if let Err(e) = std::fs::remove_file(&archive_file_path) {
                warn!(
                    error = %e,
                    path = %archive_file_path.display(),
                    "Failed to remove local archive file"
                );
            }

            true
        }
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["verify_head"]).inc();
            error!(
                error = %e,
                s3_key = %s3_key,
                "Failed to verify S3 upload"
            );
            false
        }
    }
}

/// Upload a file to S3 with exponential backoff retry
pub async fn upload_to_s3(
    file_path: &Path,
    bucket: &str,
    s3_key: &str,
    client: &Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                S3_UPLOAD_RETRIES.inc();
                info!(attempt, key = %s3_key, "Retrying S3 upload");
            }

            let body = ByteStream::from_path(&file_path)
                .await
                .map_err(|e| format!("Failed to read file: {}", e))?;

            client
                .put_object()
                .bucket(&bucket)
                .key(&s3_key)
                .body(body)
                .send()
                .await
                .map_err(|e| format!("S3 put_object failed: {}", e))?;

            Ok::<(), String>(())
        }
    })
    .await;

    match result {
        Ok(_) => {
            info!(
                path = %file_path.display(),
                bucket,
                key = s3_key,
                attempts = attempt,
                "Successfully uploaded to S3"
            );
            Ok(())
        }
        Err(e) => Err(format!("S3 upload failed after {} attempts: {}", attempt, e).into()),
    }
}
