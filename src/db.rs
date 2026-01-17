use std::path::Path;
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::metrics::{CHANNEL_QUEUE_DEPTH, DB_INSERT_ERRORS, DB_SNAPSHOTS_WRITTEN, DB_WRITE_DURATION};
use crate::models::SnapshotData;

/// Create a SQLite connection pool
pub async fn create_pool(database_path: &Path) -> SqlitePool {
    let db_url = format!("sqlite:{}?mode=rwc", database_path.display());
    SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to create SQLite pool")
}

/// Initialize database schema with WAL mode
pub async fn init_database(db_pool: &SqlitePool) {
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

    info!("Database initialized with WAL mode");
}

/// Background worker that batches and writes snapshots to the database
pub async fn db_worker(
    db_pool: SqlitePool,
    db_rx: &mut tokio::sync::mpsc::Receiver<SnapshotData>,
    batch_interval_secs: u64,
) {
    let mut batch: Vec<SnapshotData> = Vec::new();
    let mut interval = interval(Duration::from_secs(batch_interval_secs));

    loop {
        tokio::select! {
            Some(snapshot) = db_rx.recv() => {
                batch.push(snapshot);
                // Update queue depth metric (approximate - batch size)
                CHANNEL_QUEUE_DEPTH.set(batch.len() as f64);
            },
            _ = interval.tick() => {
                if !batch.is_empty() {
                    flush_batch(&db_pool, &mut batch).await;
                    CHANNEL_QUEUE_DEPTH.set(0.0);
                }
            }
        }
    }
}

/// Flush a batch of snapshots to the database
async fn flush_batch(db_pool: &SqlitePool, batch: &mut Vec<SnapshotData>) {
    let batch_size = batch.len();
    let start = Instant::now();

    let tx = match db_pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            error!(error = %e, "Failed to begin transaction, retrying next interval");
            DB_WRITE_DURATION
                .with_label_values(&["transaction_begin_error"])
                .observe(start.elapsed().as_secs_f64());
            return;
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
            DB_INSERT_ERRORS.inc();
            error!(error = %e, "Failed to insert snapshot");
        }
    }

    if let Err(e) = tx.commit().await {
        error!(
            error = %e,
            batch_size,
            "Failed to commit transaction, snapshots may be lost"
        );
        DB_WRITE_DURATION
            .with_label_values(&["commit_error"])
            .observe(start.elapsed().as_secs_f64());
    } else {
        let duration = start.elapsed().as_secs_f64();
        DB_WRITE_DURATION
            .with_label_values(&["success"])
            .observe(duration);
        DB_SNAPSHOTS_WRITTEN.inc_by((batch_size - insert_errors) as f64);

        if insert_errors > 0 {
            warn!(
                insert_errors,
                batch_size,
                duration_ms = duration * 1000.0,
                "Committed batch with insert errors"
            );
        }
    }
}

/// Fetch all snapshots from the database for archiving
pub async fn fetch_all_snapshots(db_pool: &SqlitePool) -> Result<Vec<(String, String, String, String, i64, String)>, sqlx::Error> {
    let rows = sqlx::query("SELECT exchange, symbol, data_type, exchange_sequence_id, timestamp, data FROM snapshots")
        .fetch_all(db_pool)
        .await?;

    let snapshots = rows
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

    Ok(snapshots)
}

/// Delete all snapshots from the database after successful archive
pub async fn delete_all_snapshots(db_pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM snapshots")
        .execute(db_pool)
        .await?;
    Ok(())
}
