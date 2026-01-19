use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use chrono::Utc;
use polars::prelude::*;
use sqlx::sqlite::SqlitePool;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use tracing::{error, info, warn};

use crate::config::{Config, StorageMode};
use crate::db::{
    count_orderbooks, count_trades, delete_orderbooks_by_id_range, delete_trades_by_id_range,
    fetch_orderbooks_batch, fetch_trades_batch, get_orderbook_groups, get_trade_groups, DbRow,
};
use crate::metrics::{
    ARCHIVE_FAILURES, ARCHIVES_COMPLETED, S3_UPLOAD_DURATION, S3_UPLOAD_FAILURES,
    S3_UPLOAD_RETRIES, SNAPSHOTS_ARCHIVED,
};
use crate::models::DataType;

/// Create and configure the S3 client (returns None if credentials are not available)
pub async fn create_s3_client(config: &Config) -> Option<Client> {
    let (access_key, secret_key) = match (&config.aws_access_key, &config.aws_secret_key) {
        (Some(ak), Some(sk)) => (ak.clone(), sk.clone()),
        _ => {
            info!("AWS credentials not configured, S3 client not created");
            return None;
        }
    };

    let region_provider = RegionProviderChain::first_try(Region::new(config.aws_region.clone()))
        .or_else(Region::new("us-west-2"));

    let credentials_provider = aws_sdk_s3::config::Credentials::new(
        &access_key,
        &secret_key,
        None,
        None,
        "custom",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;

    Some(Client::new(&shared_config))
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
    storage_mode: StorageMode,
    local_storage_path: PathBuf,
    bucket_name: String,
    client: Option<Client>,
    home_server_name: Option<String>,
    archive_interval_secs: u64,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    // Validate that S3 client is available when needed
    if matches!(storage_mode, StorageMode::S3 | StorageMode::Both) && client.is_none() {
        error!("Storage mode requires S3 but no S3 client configured");
        return;
    }

    info!(
        storage_mode = ?storage_mode,
        local_storage_path = %local_storage_path.display(),
        "Archive scheduler starting"
    );

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
            archive_all_data(
                &db_pool,
                &archive_dir,
                storage_mode,
                &local_storage_path,
                &bucket_name,
                client.as_ref(),
                home_server_name.as_deref(),
            ),
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

/// Batch size for archive operations - limits memory usage
const ARCHIVE_BATCH_SIZE: i64 = 10_000;

/// Archive all data from both orderbooks and trades tables
/// Processes data in batches to limit memory usage
/// Groups data by (exchange, symbol, data_type) and creates separate Parquet files
pub async fn archive_all_data(
    db_pool: &SqlitePool,
    archive_dir: &Path,
    storage_mode: StorageMode,
    local_storage_path: &Path,
    bucket_name: &str,
    client: Option<&Client>,
    home_server_name: Option<&str>,
) {
    // Get counts for logging
    let orderbook_count = count_orderbooks(db_pool).await.unwrap_or(0);
    let trade_count = count_trades(db_pool).await.unwrap_or(0);

    if orderbook_count == 0 && trade_count == 0 {
        info!("No data to archive");
        return;
    }

    info!(
        orderbook_count,
        trade_count,
        batch_size = ARCHIVE_BATCH_SIZE,
        "Starting batch archive"
    );

    let mut total_archived = 0u64;
    let mut total_failed = 0u64;

    // Process orderbooks by (exchange, symbol) groups
    let orderbook_groups = match get_orderbook_groups(db_pool).await {
        Ok(groups) => groups,
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["fetch"]).inc();
            error!(error = %e, "Failed to get orderbook groups");
            return;
        }
    };

    for group in orderbook_groups {
        let (archived, failed) = archive_table_group(
            db_pool,
            &group.exchange,
            &group.symbol,
            DataType::Orderbook,
            archive_dir,
            storage_mode,
            local_storage_path,
            bucket_name,
            client,
            home_server_name,
        )
        .await;
        total_archived += archived;
        total_failed += failed;
    }

    // Process trades by (exchange, symbol) groups
    let trade_groups = match get_trade_groups(db_pool).await {
        Ok(groups) => groups,
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["fetch"]).inc();
            error!(error = %e, "Failed to get trade groups");
            return;
        }
    };

    for group in trade_groups {
        let (archived, failed) = archive_table_group(
            db_pool,
            &group.exchange,
            &group.symbol,
            DataType::Trade,
            archive_dir,
            storage_mode,
            local_storage_path,
            bucket_name,
            client,
            home_server_name,
        )
        .await;
        total_archived += archived;
        total_failed += failed;
    }

    if total_failed > 0 {
        warn!(
            total_archived,
            total_failed,
            "Archive completed with some failures"
        );
    } else {
        info!(total_archived, "Archive completed successfully");
    }
}

/// Archive all data for a specific (exchange, symbol, data_type) in batches
/// Returns (archived_count, failed_count)
async fn archive_table_group(
    db_pool: &SqlitePool,
    exchange: &str,
    symbol: &str,
    data_type: DataType,
    archive_dir: &Path,
    storage_mode: StorageMode,
    local_storage_path: &Path,
    bucket_name: &str,
    client: Option<&Client>,
    home_server_name: Option<&str>,
) -> (u64, u64) {
    let mut total_archived = 0u64;
    let mut total_failed = 0u64;

    loop {
        // Fetch a batch
        let rows = match data_type {
            DataType::Orderbook => {
                fetch_orderbooks_batch(db_pool, exchange, symbol, ARCHIVE_BATCH_SIZE).await
            }
            DataType::Trade => {
                fetch_trades_batch(db_pool, exchange, symbol, ARCHIVE_BATCH_SIZE).await
            }
        };

        let rows = match rows {
            Ok(r) => r,
            Err(e) => {
                ARCHIVE_FAILURES.with_label_values(&["fetch"]).inc();
                error!(
                    error = %e,
                    exchange,
                    symbol,
                    data_type = data_type.as_str(),
                    "Failed to fetch batch"
                );
                total_failed += 1;
                break;
            }
        };

        if rows.is_empty() {
            break; // No more data for this group
        }

        let batch_count = rows.len() as u64;

        // Get ID range for deletion (rows are ordered by id)
        let min_id = rows.first().map(|r| r.id).unwrap_or(0);
        let max_id = rows.last().map(|r| r.id).unwrap_or(0);

        // Archive this batch
        let success = archive_group(
            rows, // Take ownership to avoid cloning
            exchange,
            symbol,
            data_type,
            archive_dir,
            storage_mode,
            local_storage_path,
            bucket_name,
            client,
            home_server_name,
        )
        .await;

        if success {
            // Delete archived rows from database by ID range
            let delete_result = match data_type {
                DataType::Orderbook => {
                    delete_orderbooks_by_id_range(db_pool, min_id, max_id).await
                }
                DataType::Trade => {
                    delete_trades_by_id_range(db_pool, min_id, max_id).await
                }
            };

            match delete_result {
                Ok(deleted) => {
                    total_archived += batch_count;
                    info!(
                        exchange,
                        symbol,
                        data_type = data_type.as_str(),
                        batch_count,
                        deleted,
                        "Archived and deleted batch"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        exchange,
                        symbol,
                        data_type = data_type.as_str(),
                        "Failed to delete archived rows"
                    );
                    total_failed += batch_count;
                }
            }
        } else {
            total_failed += batch_count;
            // Don't delete on failure - will retry next cycle
            warn!(
                exchange,
                symbol,
                data_type = data_type.as_str(),
                batch_count,
                "Batch archive failed, keeping data for retry"
            );
        }
    }

    (total_archived, total_failed)
}

/// Archive a single group of rows (same exchange/symbol/data_type)
/// Takes ownership of rows to avoid cloning large JSON strings
async fn archive_group(
    rows: Vec<DbRow>,
    exchange: &str,
    symbol: &str,
    data_type: DataType,
    archive_dir: &Path,
    storage_mode: StorageMode,
    local_storage_path: &Path,
    bucket_name: &str,
    client: Option<&Client>,
    home_server_name: Option<&str>,
) -> bool {
    let row_count = rows.len();
    let data_type_str = data_type.as_str();

    // Extract columns by taking ownership (no cloning needed)
    let mut exchanges: Vec<String> = Vec::with_capacity(row_count);
    let mut symbols: Vec<String> = Vec::with_capacity(row_count);
    let mut seq_ids: Vec<String> = Vec::with_capacity(row_count);
    let mut timestamps_collector: Vec<i64> = Vec::with_capacity(row_count);
    let mut timestamps_exchange: Vec<i64> = Vec::with_capacity(row_count);
    let mut data_col: Vec<String> = Vec::with_capacity(row_count);

    for row in rows {
        exchanges.push(row.exchange);
        symbols.push(row.symbol);
        seq_ids.push(row.exchange_sequence_id);
        timestamps_collector.push(row.timestamp_collector);
        timestamps_exchange.push(row.timestamp_exchange);
        data_col.push(row.data);
    }

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

    // Build hierarchical path structure
    // Format: {exchange}/{symbol}/{data_type}/[{server}/]{date}/{timestamp}.parquet
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_millis();
    let date = Utc::now().format("%Y-%m-%d");

    let relative_path = match home_server_name {
        Some(server) => format!(
            "{}/{}/{}/{}/{}/{}.parquet",
            exchange, symbol, data_type_str, server, date, timestamp
        ),
        None => format!(
            "{}/{}/{}/{}/{}.parquet",
            exchange, symbol, data_type_str, date, timestamp
        ),
    };

    // Temporary file for Parquet creation
    let temp_file_name = format!("{}_{}_{}.parquet", exchange, symbol, timestamp);
    let temp_file_path = archive_dir.join(&temp_file_name);

    // Create temporary Parquet file
    let mut file = match std::fs::File::create(&temp_file_path) {
        Ok(f) => f,
        Err(e) => {
            ARCHIVE_FAILURES.with_label_values(&["parquet"]).inc();
            error!(
                error = %e,
                path = %temp_file_path.display(),
                "Failed to create archive file"
            );
            return false;
        }
    };

    if let Err(e) = ParquetWriter::new(&mut file).finish(&mut df) {
        ARCHIVE_FAILURES.with_label_values(&["parquet"]).inc();
        error!(
            error = %e,
            path = %temp_file_path.display(),
            "Failed to write Parquet file"
        );
        let _ = std::fs::remove_file(&temp_file_path);
        return false;
    }

    // Get file size for verification
    let file_size = match std::fs::metadata(&temp_file_path) {
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
        path = %relative_path,
        size = file_size,
        "Written group to Parquet file"
    );

    // Handle storage based on mode
    let mut s3_success = true;
    let mut local_success = true;

    // Upload to S3 if needed
    if matches!(storage_mode, StorageMode::S3 | StorageMode::Both) {
        if let Some(s3_client) = client {
            let upload_start = Instant::now();
            if let Err(e) = upload_to_s3(&temp_file_path, bucket_name, &relative_path, s3_client).await {
                ARCHIVE_FAILURES.with_label_values(&["upload"]).inc();
                error!(error = %e, "Failed to upload to S3, will retry next cycle");
                S3_UPLOAD_FAILURES.inc();
                S3_UPLOAD_DURATION
                    .with_label_values(&["failure"])
                    .observe(upload_start.elapsed().as_secs_f64());
                s3_success = false;
            } else {
                S3_UPLOAD_DURATION
                    .with_label_values(&["success"])
                    .observe(upload_start.elapsed().as_secs_f64());

                // Verify upload succeeded with size check
                match s3_client
                    .head_object()
                    .bucket(bucket_name)
                    .key(&relative_path)
                    .send()
                    .await
                {
                    Ok(head_resp) => {
                        let remote_size = head_resp.content_length().unwrap_or(0) as u64;
                        if remote_size != file_size {
                            ARCHIVE_FAILURES.with_label_values(&["verify_size"]).inc();
                            error!(
                                local_size = file_size,
                                remote_size,
                                s3_key = %relative_path,
                                "Size mismatch after S3 upload"
                            );
                            s3_success = false;
                        } else {
                            info!(
                                bucket = bucket_name,
                                key = %relative_path,
                                size = remote_size,
                                "Verified S3 upload"
                            );
                        }
                    }
                    Err(e) => {
                        ARCHIVE_FAILURES.with_label_values(&["verify_head"]).inc();
                        error!(
                            error = %e,
                            s3_key = %relative_path,
                            "Failed to verify S3 upload"
                        );
                        s3_success = false;
                    }
                }
            }
        } else {
            error!("S3 client not available but storage mode requires S3");
            s3_success = false;
        }
    }

    // Copy to local storage if needed
    if matches!(storage_mode, StorageMode::Local | StorageMode::Both) {
        let local_dest_path = local_storage_path.join(&relative_path);

        // Create parent directories
        if let Some(parent) = local_dest_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                ARCHIVE_FAILURES.with_label_values(&["local_mkdir"]).inc();
                error!(
                    error = %e,
                    path = %parent.display(),
                    "Failed to create local storage directory"
                );
                local_success = false;
            }
        }

        if local_success {
            // Copy temp file to final local destination
            if let Err(e) = std::fs::copy(&temp_file_path, &local_dest_path) {
                ARCHIVE_FAILURES.with_label_values(&["local_copy"]).inc();
                error!(
                    error = %e,
                    src = %temp_file_path.display(),
                    dest = %local_dest_path.display(),
                    "Failed to copy to local storage"
                );
                local_success = false;
            } else {
                info!(
                    path = %local_dest_path.display(),
                    size = file_size,
                    "Persisted to local storage"
                );
            }
        }
    }

    // Determine overall success based on storage mode
    let success = match storage_mode {
        StorageMode::S3 => s3_success,
        StorageMode::Local => local_success,
        StorageMode::Both => s3_success && local_success,
    };

    if success {
        // Update metrics
        ARCHIVES_COMPLETED.inc();
        SNAPSHOTS_ARCHIVED.inc_by(row_count as f64);
    }

    // Clean up temporary file
    if let Err(e) = std::fs::remove_file(&temp_file_path) {
        warn!(
            error = %e,
            path = %temp_file_path.display(),
            "Failed to remove temporary archive file"
        );
    }

    success
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
