use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use polars::prelude::*;
use sqlx::sqlite::SqlitePool;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;

use crate::db::{delete_all_snapshots, fetch_all_snapshots};
use crate::println_with_timestamp;

/// Create S3 bucket if it doesn't exist
pub async fn create_bucket_if_not_exists(client: &Client, bucket_name: &str, region: &str) {
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => println_with_timestamp!("Bucket {} already exists.", bucket_name),
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
                Ok(_) => {
                    println_with_timestamp!("Bucket {} created in region {}.", bucket_name, region)
                }
                Err(e) => println_with_timestamp!(
                    "WARNING: Failed to create bucket {}: {}. Will retry on next upload.",
                    bucket_name,
                    e
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
        println_with_timestamp!("Sleeping for {} seconds (1 hour).", sleep_duration);
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
            println_with_timestamp!("ERROR: HOME_SERVER_NAME not set, skipping archive");
            return;
        }
    };

    // Fetch all snapshots
    let snapshots = match fetch_all_snapshots(db_pool).await {
        Ok(s) => s,
        Err(e) => {
            println_with_timestamp!("ERROR: Failed to fetch snapshots for archive: {}", e);
            return;
        }
    };

    if snapshots.is_empty() {
        println_with_timestamp!("No snapshots to archive");
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
            println_with_timestamp!("ERROR: Failed to create DataFrame: {}", e);
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
            println_with_timestamp!(
                "ERROR: Failed to create archive file {}: {}",
                archive_file_path.display(),
                e
            );
            return;
        }
    };

    if let Err(e) = ParquetWriter::new(&mut file).finish(&mut df) {
        println_with_timestamp!(
            "ERROR: Failed to write Parquet file {}: {}",
            archive_file_path.display(),
            e
        );
        // Try to clean up the partial file
        let _ = std::fs::remove_file(&archive_file_path);
        return;
    }
    println_with_timestamp!(
        "Written {} snapshots to {}",
        snapshots.len(),
        archive_file_path.display()
    );

    // Upload to S3
    if let Err(e) = upload_to_s3(&archive_file_path, bucket_name, &archive_file_name, client).await
    {
        println_with_timestamp!("ERROR: Failed to upload to S3: {}. Will retry next cycle.", e);
        return;
    }

    // Verify upload succeeded before deleting local data
    match client
        .head_object()
        .bucket(bucket_name)
        .key(&archive_file_name)
        .send()
        .await
    {
        Ok(_) => {
            println_with_timestamp!("Verified S3 upload: s3://{}/{}", bucket_name, archive_file_name);
            if let Err(e) = delete_all_snapshots(db_pool).await {
                println_with_timestamp!("ERROR: Failed to delete snapshots from database: {}", e);
                // Data is safe in S3, but duplicates may occur next cycle
            }
            if let Err(e) = std::fs::remove_file(&archive_file_path) {
                println_with_timestamp!("WARNING: Failed to remove local archive file: {}", e);
                // Not critical - file can be cleaned up manually
            }
            println_with_timestamp!("Deleted local data after verified upload");
        }
        Err(e) => {
            println_with_timestamp!(
                "ERROR: Failed to verify S3 upload, keeping local data: {}",
                e
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
                println_with_timestamp!("S3 upload attempt {} for {}", attempt, s3_key);
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
            if attempt > 1 {
                println_with_timestamp!(
                    "Successfully uploaded {} to s3://{}/{} after {} attempts",
                    file_path.display(),
                    bucket,
                    s3_key,
                    attempt
                );
            } else {
                println_with_timestamp!(
                    "Successfully uploaded {} to s3://{}/{}",
                    file_path.display(),
                    bucket,
                    s3_key
                );
            }
            Ok(())
        }
        Err(e) => Err(format!("S3 upload failed after {} attempts: {}", attempt, e).into()),
    }
}
