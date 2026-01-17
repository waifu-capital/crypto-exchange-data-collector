use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use polars::prelude::*;
use sqlx::sqlite::SqlitePool;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use tracing::{error, info, warn};

use crate::db::{delete_all_snapshots, fetch_all_snapshots};
use crate::metrics::{
    ARCHIVES_COMPLETED, S3_UPLOAD_DURATION, S3_UPLOAD_FAILURES, S3_UPLOAD_RETRIES,
    SNAPSHOTS_ARCHIVED,
};

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
                    "Failed to create bucket, will retry on next upload"
                ),
            }
        }
    }
}

/// Schedule and run archive task at regular intervals
pub async fn run_archive_scheduler(
    db_pool: SqlitePool,
    archive_dir: PathBuf,
    bucket_name: String,
    client: Client,
    home_server_name: Option<String>,
) {
    loop {
        let sleep_duration = 3600;
        info!(sleep_secs = sleep_duration, "Sleeping until next archive");
        sleep(Duration::from_secs(sleep_duration)).await;

        archive_snapshots(
            &db_pool,
            &archive_dir,
            &bucket_name,
            &client,
            home_server_name.as_deref(),
        )
        .await;
    }
}

/// Archive snapshots to Parquet and upload to S3
pub async fn archive_snapshots(
    db_pool: &SqlitePool,
    archive_dir: &Path,
    bucket_name: &str,
    client: &Client,
    home_server_name: Option<&str>,
) {
    let home_server_name = match home_server_name {
        Some(name) => name,
        None => {
            error!("HOME_SERVER_NAME not set, skipping archive");
            return;
        }
    };

    // Fetch all snapshots
    let snapshots = match fetch_all_snapshots(db_pool).await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "Failed to fetch snapshots for archive");
            return;
        }
    };

    if snapshots.is_empty() {
        info!("No snapshots to archive");
        return;
    }

    // Create DataFrame
    let mut df = match DataFrame::new(vec![
        Series::new(
            "exchange".into(),
            snapshots.iter().map(|s| s.0.clone()).collect::<Vec<String>>(),
        )
        .into(),
        Series::new(
            "symbol".into(),
            snapshots.iter().map(|s| s.1.clone()).collect::<Vec<String>>(),
        )
        .into(),
        Series::new(
            "data_type".into(),
            snapshots.iter().map(|s| s.2.clone()).collect::<Vec<String>>(),
        )
        .into(),
        Series::new(
            "exchange_sequence_id".into(),
            snapshots.iter().map(|s| s.3.clone()).collect::<Vec<String>>(),
        )
        .into(),
        Series::new(
            "timestamp".into(),
            snapshots.iter().map(|s| s.4).collect::<Vec<i64>>(),
        )
        .into(),
        Series::new(
            "data".into(),
            snapshots.iter().map(|s| s.5.clone()).collect::<Vec<String>>(),
        )
        .into(),
    ]) {
        Ok(df) => df,
        Err(e) => {
            error!(error = %e, "Failed to create DataFrame");
            return;
        }
    };

    // Create archive file
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_millis();
    let archive_file_name = format!("snapshots_{}_{}.parquet", home_server_name, timestamp);
    let archive_file_path = archive_dir.join(&archive_file_name);

    let mut file = match std::fs::File::create(&archive_file_path) {
        Ok(f) => f,
        Err(e) => {
            error!(
                error = %e,
                path = %archive_file_path.display(),
                "Failed to create archive file"
            );
            return;
        }
    };

    if let Err(e) = ParquetWriter::new(&mut file).finish(&mut df) {
        error!(
            error = %e,
            path = %archive_file_path.display(),
            "Failed to write Parquet file"
        );
        // Try to clean up the partial file
        let _ = std::fs::remove_file(&archive_file_path);
        return;
    }
    info!(
        snapshot_count = snapshots.len(),
        path = %archive_file_path.display(),
        "Written snapshots to Parquet file"
    );

    // Upload to S3
    let upload_start = Instant::now();
    if let Err(e) = upload_to_s3(&archive_file_path, bucket_name, &archive_file_name, client).await
    {
        error!(error = %e, "Failed to upload to S3, will retry next cycle");
        S3_UPLOAD_FAILURES.inc();
        S3_UPLOAD_DURATION
            .with_label_values(&["failure"])
            .observe(upload_start.elapsed().as_secs_f64());
        return;
    }
    S3_UPLOAD_DURATION
        .with_label_values(&["success"])
        .observe(upload_start.elapsed().as_secs_f64());

    // Verify upload succeeded before deleting local data
    match client
        .head_object()
        .bucket(bucket_name)
        .key(&archive_file_name)
        .send()
        .await
    {
        Ok(_) => {
            info!(
                bucket = bucket_name,
                key = archive_file_name,
                "Verified S3 upload"
            );

            // Update metrics for successful archive
            ARCHIVES_COMPLETED.inc();
            SNAPSHOTS_ARCHIVED.inc_by(snapshots.len() as f64);

            if let Err(e) = delete_all_snapshots(db_pool).await {
                error!(error = %e, "Failed to delete snapshots from database");
                // Data is safe in S3, but duplicates may occur next cycle
            }
            if let Err(e) = std::fs::remove_file(&archive_file_path) {
                warn!(
                    error = %e,
                    path = %archive_file_path.display(),
                    "Failed to remove local archive file"
                );
                // Not critical - file can be cleaned up manually
            }
            info!("Deleted local data after verified upload");
        }
        Err(e) => {
            error!(
                error = %e,
                "Failed to verify S3 upload, keeping local data"
            );
            // Don't delete SQLite data or local file - will retry next cycle
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
