use std::path::Path;
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::metrics::{CHANNEL_QUEUE_DEPTH, DB_INSERT_ERRORS, DB_SNAPSHOTS_WRITTEN, DB_WRITE_DURATION};
use crate::models::{DataType, MarketEvent};

/// Create a SQLite connection pool
pub async fn create_pool(database_path: &Path) -> SqlitePool {
    let db_url = format!("sqlite:{}?mode=rwc", database_path.display());
    SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to create SQLite pool")
}

/// Initialize database schema with WAL mode and separate tables
pub async fn init_database(db_pool: &SqlitePool) {
    // Enable WAL mode for better crash recovery and write performance
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(db_pool)
        .await
        .expect("Failed to enable WAL mode");

    // Orderbooks table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orderbooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            exchange_sequence_id TEXT NOT NULL,
            timestamp_collector INTEGER NOT NULL,
            timestamp_exchange INTEGER NOT NULL,
            data TEXT NOT NULL,
            UNIQUE(exchange, symbol, exchange_sequence_id)
        )",
    )
    .execute(db_pool)
    .await
    .expect("Failed to create orderbooks table");

    // Index for time-based queries
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_orderbooks_time ON orderbooks(timestamp_collector)")
        .execute(db_pool)
        .await
        .ok();

    // Trades table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            exchange_sequence_id TEXT NOT NULL,
            timestamp_collector INTEGER NOT NULL,
            timestamp_exchange INTEGER NOT NULL,
            data TEXT NOT NULL,
            UNIQUE(exchange, symbol, exchange_sequence_id)
        )",
    )
    .execute(db_pool)
    .await
    .expect("Failed to create trades table");

    // Index for time-based queries
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp_collector)")
        .execute(db_pool)
        .await
        .ok();

    info!("Database initialized with WAL mode and separate orderbooks/trades tables");
}

/// Background worker that batches and writes market events to the database
pub async fn db_worker(
    db_pool: SqlitePool,
    mut db_rx: tokio::sync::mpsc::Receiver<MarketEvent>,
    batch_interval_secs: u64,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let mut batch: Vec<MarketEvent> = Vec::new();
    let mut interval = interval(Duration::from_secs(batch_interval_secs));

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("DB worker received shutdown signal");
                // Flush any remaining data before shutting down
                if !batch.is_empty() {
                    info!(batch_size = batch.len(), "Flushing final batch before shutdown");
                    flush_batch(&db_pool, &mut batch).await;
                }
                break;
            }
            Some(event) = db_rx.recv() => {
                batch.push(event);
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

    info!("DB worker stopped");
}

/// Flush a batch of market events to the appropriate tables
async fn flush_batch(db_pool: &SqlitePool, batch: &mut Vec<MarketEvent>) {
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
    for event in batch.drain(..) {
        // Route to correct table based on data type
        let table = match event.data_type {
            DataType::Orderbook => "orderbooks",
            DataType::Trade => "trades",
        };

        // INSERT OR IGNORE skips duplicates based on UNIQUE constraint
        let query = format!(
            "INSERT OR IGNORE INTO {} (exchange, symbol, exchange_sequence_id, timestamp_collector, timestamp_exchange, data) VALUES (?, ?, ?, ?, ?, ?)",
            table
        );

        if let Err(e) = sqlx::query(&query)
            .bind(&event.exchange)
            .bind(&event.symbol)
            .bind(&event.exchange_sequence_id)
            .bind(event.timestamp_collector)
            .bind(event.timestamp_exchange)
            .bind(&event.data)
            .execute(&mut *tx)
            .await
        {
            insert_errors += 1;
            let error_type = categorize_sqlx_error(&e);
            DB_INSERT_ERRORS.with_label_values(&[error_type]).inc();
            // Only log non-duplicate errors (duplicates are expected with INSERT OR IGNORE)
            if error_type != "duplicate" {
                error!(error = %e, error_type, table, "Failed to insert event");
            }
        }
    }

    if let Err(e) = tx.commit().await {
        error!(
            error = %e,
            batch_size,
            "Failed to commit transaction, events may be lost"
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

/// Row data returned from database queries
pub struct DbRow {
    pub exchange: String,
    pub symbol: String,
    pub data_type: DataType,
    pub exchange_sequence_id: String,
    pub timestamp_collector: i64,
    pub timestamp_exchange: i64,
    pub data: String,
}

/// Represents a unique (exchange, symbol) combination in the database
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExchangeSymbol {
    pub exchange: String,
    pub symbol: String,
}

/// Get distinct (exchange, symbol) pairs from orderbooks table
pub async fn get_orderbook_groups(db_pool: &SqlitePool) -> Result<Vec<ExchangeSymbol>, sqlx::Error> {
    let rows = sqlx::query("SELECT DISTINCT exchange, symbol FROM orderbooks")
        .fetch_all(db_pool)
        .await?;

    Ok(rows
        .iter()
        .map(|row| ExchangeSymbol {
            exchange: row.get("exchange"),
            symbol: row.get("symbol"),
        })
        .collect())
}

/// Get distinct (exchange, symbol) pairs from trades table
pub async fn get_trade_groups(db_pool: &SqlitePool) -> Result<Vec<ExchangeSymbol>, sqlx::Error> {
    let rows = sqlx::query("SELECT DISTINCT exchange, symbol FROM trades")
        .fetch_all(db_pool)
        .await?;

    Ok(rows
        .iter()
        .map(|row| ExchangeSymbol {
            exchange: row.get("exchange"),
            symbol: row.get("symbol"),
        })
        .collect())
}

/// Fetch a batch of orderbooks for a specific exchange/symbol
/// Returns rows ordered by id, limited to batch_size
pub async fn fetch_orderbooks_batch(
    db_pool: &SqlitePool,
    exchange: &str,
    symbol: &str,
    batch_size: i64,
) -> Result<Vec<DbRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT exchange, symbol, exchange_sequence_id, timestamp_collector, timestamp_exchange, data
         FROM orderbooks
         WHERE exchange = ? AND symbol = ?
         ORDER BY id
         LIMIT ?",
    )
    .bind(exchange)
    .bind(symbol)
    .bind(batch_size)
    .fetch_all(db_pool)
    .await?;

    Ok(rows
        .iter()
        .map(|row| DbRow {
            exchange: row.get("exchange"),
            symbol: row.get("symbol"),
            data_type: DataType::Orderbook,
            exchange_sequence_id: row.get("exchange_sequence_id"),
            timestamp_collector: row.get("timestamp_collector"),
            timestamp_exchange: row.get("timestamp_exchange"),
            data: row.get("data"),
        })
        .collect())
}

/// Fetch a batch of trades for a specific exchange/symbol
/// Returns rows ordered by id, limited to batch_size
pub async fn fetch_trades_batch(
    db_pool: &SqlitePool,
    exchange: &str,
    symbol: &str,
    batch_size: i64,
) -> Result<Vec<DbRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT exchange, symbol, exchange_sequence_id, timestamp_collector, timestamp_exchange, data
         FROM trades
         WHERE exchange = ? AND symbol = ?
         ORDER BY id
         LIMIT ?",
    )
    .bind(exchange)
    .bind(symbol)
    .bind(batch_size)
    .fetch_all(db_pool)
    .await?;

    Ok(rows
        .iter()
        .map(|row| DbRow {
            exchange: row.get("exchange"),
            symbol: row.get("symbol"),
            data_type: DataType::Trade,
            exchange_sequence_id: row.get("exchange_sequence_id"),
            timestamp_collector: row.get("timestamp_collector"),
            timestamp_exchange: row.get("timestamp_exchange"),
            data: row.get("data"),
        })
        .collect())
}

/// Delete orderbooks for a specific exchange/symbol by their sequence IDs
pub async fn delete_orderbooks_by_seq_ids(
    db_pool: &SqlitePool,
    exchange: &str,
    symbol: &str,
    seq_ids: &[String],
) -> Result<u64, sqlx::Error> {
    if seq_ids.is_empty() {
        return Ok(0);
    }

    // Build placeholders for IN clause
    let placeholders: Vec<&str> = seq_ids.iter().map(|_| "?").collect();
    let query = format!(
        "DELETE FROM orderbooks WHERE exchange = ? AND symbol = ? AND exchange_sequence_id IN ({})",
        placeholders.join(", ")
    );

    let mut q = sqlx::query(&query).bind(exchange).bind(symbol);
    for seq_id in seq_ids {
        q = q.bind(seq_id);
    }

    let result = q.execute(db_pool).await?;
    Ok(result.rows_affected())
}

/// Delete trades for a specific exchange/symbol by their sequence IDs
pub async fn delete_trades_by_seq_ids(
    db_pool: &SqlitePool,
    exchange: &str,
    symbol: &str,
    seq_ids: &[String],
) -> Result<u64, sqlx::Error> {
    if seq_ids.is_empty() {
        return Ok(0);
    }

    // Build placeholders for IN clause
    let placeholders: Vec<&str> = seq_ids.iter().map(|_| "?").collect();
    let query = format!(
        "DELETE FROM trades WHERE exchange = ? AND symbol = ? AND exchange_sequence_id IN ({})",
        placeholders.join(", ")
    );

    let mut q = sqlx::query(&query).bind(exchange).bind(symbol);
    for seq_id in seq_ids {
        q = q.bind(seq_id);
    }

    let result = q.execute(db_pool).await?;
    Ok(result.rows_affected())
}

/// Get total count of rows in orderbooks table (for logging)
pub async fn count_orderbooks(db_pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM orderbooks")
        .fetch_one(db_pool)
        .await?;
    Ok(row.get("count"))
}

/// Get total count of rows in trades table (for logging)
pub async fn count_trades(db_pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM trades")
        .fetch_one(db_pool)
        .await?;
    Ok(row.get("count"))
}

/// Categorize a SQLx error for metrics tracking
fn categorize_sqlx_error(e: &sqlx::Error) -> &'static str {
    let msg = e.to_string().to_lowercase();
    if msg.contains("unique constraint") || msg.contains("duplicate") {
        "duplicate"
    } else if msg.contains("constraint") {
        "constraint"
    } else if msg.contains("i/o") || msg.contains("disk") || msg.contains("readonly") {
        "io"
    } else {
        "other"
    }
}
