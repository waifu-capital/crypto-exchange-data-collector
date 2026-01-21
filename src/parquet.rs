//! Streaming Parquet writer with automatic file rotation.
//!
//! Each (exchange, symbol, data_type) combination gets its own writer that:
//! - Streams rows directly to Parquet (bounded memory via row group flushing)
//! - Rotates files after 1 hour OR 500MB (whichever comes first)
//! - Sends completed files to upload worker for S3 upload

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info};

use crate::metrics::{
    PARQUET_FILES_ROTATED, PARQUET_ROWS_BUFFERED, PARQUET_ROWS_WRITTEN, PARQUET_WRITE_ERRORS,
};
use crate::models::{DataType, MarketEvent, WriterKey};

/// Maximum file age before rotation (1 hour)
const MAX_FILE_AGE: Duration = Duration::from_secs(3600);

/// Maximum file size before rotation (500 MB)
const MAX_FILE_SIZE: u64 = 500 * 1024 * 1024;

/// Batch size for writing to Parquet (number of rows to buffer before writing a row group)
const WRITE_BATCH_SIZE: usize = 1000;

/// Message sent to upload worker when a file is ready
#[derive(Debug, Clone)]
pub struct CompletedFile {
    pub path: PathBuf,
    pub relative_path: String,
    pub exchange: String,
    pub symbol: String,
    pub data_type: DataType,
    pub rows: u64,
    pub size: u64,
}

/// A single Parquet writer instance for one (exchange, symbol, data_type) combination
struct ParquetWriterInstance {
    writer: ArrowWriter<File>,
    file_path: PathBuf,
    relative_path: String,
    schema: Arc<Schema>,
    created_at: Instant,
    rows_written: u64,
    exchange: String,
    symbol: String,
    data_type: DataType,
    /// Buffered rows waiting to be written as a row group
    buffer: Vec<MarketEvent>,
}

impl ParquetWriterInstance {
    /// Create a new writer instance
    fn new(
        data_dir: &Path,
        exchange: &str,
        symbol: &str,
        data_type: DataType,
        home_server_name: Option<&str>,
    ) -> Result<Self, String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("exchange", ArrowDataType::Utf8, false),
            Field::new("symbol", ArrowDataType::Utf8, false),
            Field::new("exchange_sequence_id", ArrowDataType::Utf8, false),
            Field::new("timestamp_collector", ArrowDataType::Int64, false),
            Field::new("timestamp_exchange", ArrowDataType::Int64, false),
            Field::new("data", ArrowDataType::Utf8, false),
        ]));

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before Unix epoch")
            .as_millis();
        let date = Utc::now().format("%Y-%m-%d");
        let data_type_str = data_type.as_str();

        // Build relative path for S3
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

        // Local file path
        let file_path = data_dir.join(&relative_path);

        // Create parent directories
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory {}: {}", parent.display(), e))?;
        }

        // Create file
        let file = File::create(&file_path)
            .map_err(|e| format!("Failed to create file {}: {}", file_path.display(), e))?;

        // Create writer with ZSTD compression
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create Parquet writer: {}", e))?;

        Ok(Self {
            writer,
            file_path,
            relative_path,
            schema,
            created_at: Instant::now(),
            rows_written: 0,
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            data_type,
            buffer: Vec::with_capacity(WRITE_BATCH_SIZE),
        })
    }

    /// Check if the file should be rotated
    fn should_rotate(&self) -> bool {
        // Check age
        if self.created_at.elapsed() > MAX_FILE_AGE {
            return true;
        }

        // Check size (approximate based on rows written)
        // We check actual file size periodically via metadata
        if self.rows_written > 0 && self.rows_written.is_multiple_of(10000)
            && let Ok(metadata) = std::fs::metadata(&self.file_path)
                && metadata.len() > MAX_FILE_SIZE {
                    return true;
                }

        false
    }

    /// Buffer a row for writing
    fn buffer_row(&mut self, event: MarketEvent) {
        self.buffer.push(event);
        PARQUET_ROWS_BUFFERED.inc();
    }

    /// Flush buffered rows to Parquet
    fn flush_buffer(&mut self) -> Result<(), String> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch_size = self.buffer.len();

        // Convert to Arrow arrays
        let exchanges: Vec<&str> = self.buffer.iter().map(|r| r.exchange.as_str()).collect();
        let symbols: Vec<&str> = self.buffer.iter().map(|r| r.symbol.as_str()).collect();
        let seq_ids: Vec<&str> = self
            .buffer
            .iter()
            .map(|r| r.exchange_sequence_id.as_str())
            .collect();
        let ts_collectors: Vec<i64> = self.buffer.iter().map(|r| r.timestamp_collector).collect();
        let ts_exchanges: Vec<i64> = self.buffer.iter().map(|r| r.timestamp_exchange).collect();
        let data: Vec<&str> = self.buffer.iter().map(|r| r.data.as_str()).collect();

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(exchanges)) as ArrayRef,
                Arc::new(StringArray::from(symbols)) as ArrayRef,
                Arc::new(StringArray::from(seq_ids)) as ArrayRef,
                Arc::new(Int64Array::from(ts_collectors)) as ArrayRef,
                Arc::new(Int64Array::from(ts_exchanges)) as ArrayRef,
                Arc::new(StringArray::from(data)) as ArrayRef,
            ],
        )
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

        self.writer
            .write(&batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;

        self.rows_written += batch_size as u64;
        PARQUET_ROWS_WRITTEN.inc_by(batch_size as f64);
        PARQUET_ROWS_BUFFERED.sub(batch_size as f64);

        self.buffer.clear();
        Ok(())
    }

    /// Close the writer and return file info
    fn close(mut self) -> Result<CompletedFile, String> {
        // Flush any remaining buffered rows
        self.flush_buffer()?;

        // Close the writer
        self.writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        // Get final file size
        let size = std::fs::metadata(&self.file_path)
            .map(|m| m.len())
            .unwrap_or(0);

        Ok(CompletedFile {
            path: self.file_path,
            relative_path: self.relative_path,
            exchange: self.exchange,
            symbol: self.symbol,
            data_type: self.data_type,
            rows: self.rows_written,
            size,
        })
    }
}

/// Manages all Parquet writers and handles rotation
pub struct ParquetWriterManager {
    writers: HashMap<WriterKey, ParquetWriterInstance>,
    data_dir: PathBuf,
    home_server_name: Option<String>,
    upload_tx: Sender<CompletedFile>,
}

impl ParquetWriterManager {
    /// Create a new writer manager
    pub fn new(
        data_dir: PathBuf,
        home_server_name: Option<String>,
        upload_tx: Sender<CompletedFile>,
    ) -> Self {
        Self {
            writers: HashMap::new(),
            data_dir,
            home_server_name,
            upload_tx,
        }
    }

    /// Write an event to the appropriate Parquet file
    pub async fn write(&mut self, event: MarketEvent) -> Result<(), String> {
        let key = WriterKey {
            exchange: event.exchange.clone(),
            symbol: event.symbol.clone(),
            data_type: event.data_type,
        };

        // Check if we need to rotate the existing writer
        if let Some(writer) = self.writers.get(&key)
            && writer.should_rotate() {
                // Remove and close the writer
                let writer = self.writers.remove(&key).unwrap();
                self.finalize_writer(writer).await;
            }

        // Get or create writer
        let writer = if let Some(w) = self.writers.get_mut(&key) {
            w
        } else {
            let new_writer = ParquetWriterInstance::new(
                &self.data_dir,
                &key.exchange,
                &key.symbol,
                key.data_type,
                self.home_server_name.as_deref(),
            )?;

            info!(
                exchange = %key.exchange,
                symbol = %key.symbol,
                data_type = %key.data_type,
                path = %new_writer.file_path.display(),
                "Created new Parquet writer"
            );

            self.writers.insert(key.clone(), new_writer);
            self.writers.get_mut(&key).unwrap()
        };

        // Buffer the row
        writer.buffer_row(event);

        // Flush if buffer is full
        if writer.buffer.len() >= WRITE_BATCH_SIZE {
            writer.flush_buffer().inspect_err(|_| {
                PARQUET_WRITE_ERRORS.inc();
            })?;
        }

        Ok(())
    }

    /// Finalize a writer and send to upload queue
    async fn finalize_writer(&mut self, writer: ParquetWriterInstance) {
        let exchange = writer.exchange.clone();
        let symbol = writer.symbol.clone();
        let data_type = writer.data_type;

        match writer.close() {
            Ok(completed) => {
                if completed.rows > 0 {
                    info!(
                        exchange = %completed.exchange,
                        symbol = %completed.symbol,
                        data_type = %completed.data_type,
                        rows = completed.rows,
                        size = completed.size,
                        path = %completed.relative_path,
                        "Rotated Parquet file"
                    );

                    PARQUET_FILES_ROTATED
                        .with_label_values(&[&completed.exchange, &completed.symbol])
                        .inc();

                    // Send to upload worker
                    if let Err(e) = self.upload_tx.send(completed).await {
                        error!(error = %e, "Failed to send completed file to upload worker");
                    }
                } else {
                    // Empty file - delete it
                    let _ = std::fs::remove_file(&completed.path);
                }
            }
            Err(e) => {
                PARQUET_WRITE_ERRORS.inc();
                error!(
                    exchange = %exchange,
                    symbol = %symbol,
                    data_type = %data_type,
                    error = %e,
                    "Failed to close Parquet writer"
                );
            }
        }
    }

    /// Flush all writers (call on shutdown)
    pub async fn flush_all(&mut self) {
        info!(
            writers = self.writers.len(),
            "Flushing all Parquet writers"
        );

        let keys: Vec<_> = self.writers.keys().cloned().collect();
        for key in keys {
            if let Some(writer) = self.writers.remove(&key) {
                self.finalize_writer(writer).await;
            }
        }
    }

    /// Check and rotate any files that have exceeded limits
    pub async fn check_rotations(&mut self) {
        let keys_to_rotate: Vec<_> = self
            .writers
            .iter()
            .filter(|(_, w)| w.should_rotate())
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_rotate {
            if let Some(writer) = self.writers.remove(&key) {
                self.finalize_writer(writer).await;
            }
        }
    }
}

/// Background worker that receives market events and writes to Parquet
pub async fn parquet_worker(
    mut event_rx: Receiver<MarketEvent>,
    data_dir: PathBuf,
    home_server_name: Option<String>,
    upload_tx: Sender<CompletedFile>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let mut manager = ParquetWriterManager::new(data_dir, home_server_name, upload_tx);

    // Timer for periodic rotation checks (every 60 seconds)
    let mut rotation_check = tokio::time::interval(Duration::from_secs(60));
    rotation_check.tick().await; // Skip immediate first tick

    info!("Parquet worker started");

    loop {
        tokio::select! {
            // Shutdown signal
            _ = shutdown_rx.recv() => {
                info!("Parquet worker received shutdown signal");
                break;
            }

            // Periodic rotation check
            _ = rotation_check.tick() => {
                manager.check_rotations().await;
            }

            // Incoming event
            Some(event) = event_rx.recv() => {
                if let Err(e) = manager.write(event).await {
                    error!(error = %e, "Failed to write event to Parquet");
                }
            }
        }
    }

    // Flush all writers on shutdown
    manager.flush_all().await;

    info!("Parquet worker stopped");
}
