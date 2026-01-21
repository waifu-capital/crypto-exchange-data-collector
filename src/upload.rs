//! Background S3 upload worker.
//!
//! Receives completed Parquet files and uploads them to S3 with retry logic.
//! Files are deleted locally only after successful upload (unless local storage mode).

use std::path::Path;
use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::sync::mpsc::Receiver;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use tracing::{error, info, warn};

use crate::config::StorageMode;
use crate::metrics::{
    S3_UPLOAD_DURATION, S3_UPLOAD_FAILURES, S3_UPLOAD_RETRIES, UPLOAD_QUEUE_DEPTH,
};
use crate::parquet::CompletedFile;

/// Upload a file to S3 with exponential backoff retry
async fn upload_to_s3(
    file_path: &Path,
    bucket: &str,
    s3_key: &str,
    client: &Client,
) -> Result<(), String> {
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
        Err(e) => Err(format!("S3 upload failed after {} attempts: {}", attempt, e)),
    }
}

/// Verify an S3 upload by checking the object exists and has correct size
async fn verify_s3_upload(
    client: &Client,
    bucket: &str,
    s3_key: &str,
    expected_size: u64,
) -> Result<(), String> {
    let head_resp = client
        .head_object()
        .bucket(bucket)
        .key(s3_key)
        .send()
        .await
        .map_err(|e| format!("Failed to verify S3 upload: {}", e))?;

    let remote_size = head_resp.content_length().unwrap_or(0) as u64;
    if remote_size != expected_size {
        return Err(format!(
            "Size mismatch: local={}, remote={}",
            expected_size, remote_size
        ));
    }

    Ok(())
}

/// Background worker that uploads completed Parquet files to S3
pub async fn upload_worker(
    mut file_rx: Receiver<CompletedFile>,
    storage_mode: StorageMode,
    local_storage_path: Option<std::path::PathBuf>,
    bucket_name: String,
    client: Option<Client>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!(
        storage_mode = ?storage_mode,
        "Upload worker started"
    );

    // Track pending uploads for queue depth metric
    let mut pending_count = 0u64;

    loop {
        tokio::select! {
            // Shutdown signal
            _ = shutdown_rx.recv() => {
                info!("Upload worker received shutdown signal");
                // Drain remaining files before shutdown
                while let Ok(file) = file_rx.try_recv() {
                    pending_count += 1;
                    UPLOAD_QUEUE_DEPTH.set(pending_count as f64);
                    process_file(&file, storage_mode, local_storage_path.as_deref(), &bucket_name, client.as_ref()).await;
                    pending_count = pending_count.saturating_sub(1);
                    UPLOAD_QUEUE_DEPTH.set(pending_count as f64);
                }
                break;
            }

            // Incoming file
            Some(file) = file_rx.recv() => {
                pending_count += 1;
                UPLOAD_QUEUE_DEPTH.set(pending_count as f64);
                process_file(&file, storage_mode, local_storage_path.as_deref(), &bucket_name, client.as_ref()).await;
                pending_count = pending_count.saturating_sub(1);
                UPLOAD_QUEUE_DEPTH.set(pending_count as f64);
            }
        }
    }

    info!("Upload worker stopped");
}

/// Process a completed file (upload to S3 and/or keep locally)
async fn process_file(
    file: &CompletedFile,
    storage_mode: StorageMode,
    local_storage_path: Option<&Path>,
    bucket_name: &str,
    client: Option<&Client>,
) {
    let mut s3_success = true;
    let mut local_success = true;

    // Upload to S3 if needed
    if matches!(storage_mode, StorageMode::S3 | StorageMode::Both) {
        if let Some(s3_client) = client {
            let upload_start = Instant::now();

            match upload_to_s3(&file.path, bucket_name, &file.relative_path, s3_client).await {
                Ok(_) => {
                    S3_UPLOAD_DURATION
                        .with_label_values(&["success"])
                        .observe(upload_start.elapsed().as_secs_f64());

                    // Verify upload
                    if let Err(e) = verify_s3_upload(s3_client, bucket_name, &file.relative_path, file.size).await {
                        error!(
                            error = %e,
                            key = %file.relative_path,
                            "S3 upload verification failed"
                        );
                        s3_success = false;
                    } else {
                        info!(
                            exchange = %file.exchange,
                            symbol = %file.symbol,
                            data_type = %file.data_type,
                            rows = file.rows,
                            size = file.size,
                            key = %file.relative_path,
                            "Verified S3 upload"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        exchange = %file.exchange,
                        symbol = %file.symbol,
                        "S3 upload failed"
                    );
                    S3_UPLOAD_FAILURES.inc();
                    S3_UPLOAD_DURATION
                        .with_label_values(&["failure"])
                        .observe(upload_start.elapsed().as_secs_f64());
                    s3_success = false;
                }
            }
        } else {
            error!("S3 client not available but storage mode requires S3");
            s3_success = false;
        }
    }

    // Copy to local storage if needed (for Both mode, file is already in data_dir)
    if matches!(storage_mode, StorageMode::Local | StorageMode::Both)
        && let Some(local_path) = local_storage_path {
            let dest_path = local_path.join(&file.relative_path);

            // Create parent directories
            if let Some(parent) = dest_path.parent()
                && let Err(e) = std::fs::create_dir_all(parent) {
                    error!(error = %e, path = %parent.display(), "Failed to create local directory");
                    local_success = false;
                }

            // Copy file if not already in the right location
            if local_success && file.path != dest_path {
                if let Err(e) = std::fs::copy(&file.path, &dest_path) {
                    error!(error = %e, "Failed to copy to local storage");
                    local_success = false;
                } else {
                    info!(
                        path = %dest_path.display(),
                        size = file.size,
                        "Persisted to local storage"
                    );
                }
            }
        }

    // Clean up source file based on storage mode and success
    let should_delete_source = match storage_mode {
        StorageMode::S3 => s3_success,
        StorageMode::Local => {
            // For local mode, the file is already where we want it
            // Only delete if we copied it elsewhere
            false
        }
        StorageMode::Both => s3_success && local_success,
    };

    if should_delete_source {
        if let Err(e) = std::fs::remove_file(&file.path) {
            warn!(
                error = %e,
                path = %file.path.display(),
                "Failed to remove source file after upload"
            );
        }
    } else if !s3_success && matches!(storage_mode, StorageMode::S3 | StorageMode::Both) {
        // Keep file for retry on next startup
        warn!(
            path = %file.path.display(),
            "Keeping file for later retry due to upload failure"
        );
    }
}
