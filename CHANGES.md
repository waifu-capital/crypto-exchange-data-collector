# Changelog

## 2026-01-17

### Added: Log File Retention Management

**Files changed:** `src/config.rs`, `src/utils.rs`, `src/main.rs`

**Problem:** The logging setup uses `tracing-appender` with `Rotation::DAILY` but had no cleanup of old log files. Daily files would accumulate indefinitely, eventually filling up disk space.

**Solution:** Implemented age-based log cleanup:

1. **New configuration option:**
   ```bash
   LOG_RETENTION_DAYS=1  # Default: 1 day
   ```

2. **Cleanup function in `src/utils.rs`:**
   ```rust
   pub fn cleanup_old_logs(logs_dir: &str, retention_days: u64) {
       // Deletes files older than retention_days based on modification time
   }
   ```

3. **Cleanup triggers:**
   - On application startup (immediate cleanup)
   - Periodically every 24 hours via background task

**Behavior:**
- Scans the `logs/` directory for files
- Deletes any file whose modification time exceeds the retention period
- Logs deleted files at INFO level
- Logs errors at WARN level but continues processing

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_RETENTION_DAYS` | 1 | Number of days to keep log files |

**Trade-offs:**
- Doesn't limit individual file size (a single day could produce a large file)
- Cleanup only happens when the app is running
- Simple implementation without new dependencies

**Impact:** Prevents unbounded disk usage from log accumulation. Default 1-day retention keeps only recent logs while ensuring disk space is reclaimed.

---

## 2026-01-16

### Fixed: Polars API Compatibility

**Files changed:** `src/main.rs` (lines 256-259)

**Problem:** Code failed to compile due to Polars API changes. `Series::new` now requires `PlSmallStr` instead of `&str`, and `DataFrame::new` expects `Column` instead of `Series`.

**Solution:** Added `.into()` calls to convert types:
```rust
// Before
Series::new("timestamp", data)

// After
Series::new("timestamp".into(), data).into()
```

---

### Fixed: Deprecated AWS SDK BehaviorVersion

**Files changed:** `src/main.rs` (line 47)

**Problem:** `BehaviorVersion::v2024_03_28()` was deprecated.

**Solution:** Updated to current version `BehaviorVersion::v2026_01_12()`.

---

### Improved: Data Loss Window Reduction (Problem #1 from PLAN.md)

**Files changed:** `src/main.rs` (lines 61-64, 109-110)

**Problem:** If the process crashes between WebSocket receipt and batch commit, all buffered data in the channel is lost. The channel buffer (100 messages) is entirely in-memory, and the batch interval was 60 seconds by default.

**Rationale:** This is a critical issue for a data collection system. While we accept that some data loss is possible on crash, minimizing the window is important for data integrity.

**Solution:**

1. **Enabled SQLite WAL (Write-Ahead Logging) mode:**
   ```rust
   conn.execute("PRAGMA journal_mode=WAL", []).unwrap();
   ```

   Benefits:
   - Writes go to a separate WAL file before being checkpointed to main database
   - Better crash recovery - uncommitted transactions can be recovered from WAL
   - Improved concurrent read/write performance
   - Readers don't block writers and vice versa

2. **Reduced default batch interval from 60 seconds to 5 seconds:**
   ```rust
   let batch_interval = env::var("BATCH_INTERVAL")
       .unwrap_or("5".to_string())
       .parse::<u64>()
       .unwrap_or(5);
   ```

   Benefits:
   - Maximum data loss window reduced from ~60 seconds to ~5 seconds
   - At 10 messages/second, worst case loss reduced from ~600 messages to ~50 messages
   - Still configurable via `BATCH_INTERVAL` environment variable for tuning

**Trade-offs:**
- Slightly more frequent disk I/O (every 5s instead of 60s)
- WAL mode uses slightly more disk space (WAL file + main DB)
- These trade-offs are negligible for the throughput level of this application

**Impact:** Worst-case data loss on crash reduced from ~70 seconds worth of data to ~15 seconds worth of data (batch interval + in-flight channel messages).

---

### Improved: Safe Archive-Then-Delete Sequence (Problem #2 from PLAN.md)

**Files changed:** `src/main.rs` (lines 275-288)

**Problem:** The archive flow uploaded to S3, then immediately deleted from SQLite. This created two failure scenarios:
- Upload succeeds, crash before delete → duplicates (recoverable)
- Upload fails silently, delete proceeds → **data loss** (catastrophic)

**Rationale:** Before deleting the only copy of data from SQLite, we must have confirmation that the data successfully reached S3. A HEAD request is a lightweight way to verify object existence.

**Solution:** Added S3 HEAD verification before deletion:

```rust
// After upload, verify the object exists in S3
match client.head_object().bucket(&bucket_name).key(&archive_file_name).send().await {
    Ok(_) => {
        // Only delete if verified
        conn.execute("DELETE FROM snapshots", []).unwrap();
        std::fs::remove_file(&archive_file_path).unwrap();
    },
    Err(e) => {
        // Keep local data - will retry next cycle
        println_with_timestamp!("ERROR: Failed to verify S3 upload, keeping local data: {}", e);
    }
}
```

**Behavior:**
- On successful verification: deletes SQLite records and local Parquet file
- On verification failure: keeps both SQLite data and local Parquet file intact
- Next archive cycle will re-attempt (may create duplicate S3 object, but no data loss)

**Trade-offs:**
- One additional S3 API call per archive cycle (HEAD request)
- Negligible cost (~$0.0004 per 10,000 requests)
- Potential for duplicate S3 objects if upload succeeded but HEAD failed (rare, harmless)

**Impact:** Eliminates the catastrophic data loss scenario where deletion proceeds despite failed upload.

---

### Improved: Migrated from rusqlite to sqlx (Problem #3 from PLAN.md)

**Files changed:** `Cargo.toml`, `src/main.rs` (extensive changes)

**Problem:** `rusqlite` is a synchronous SQLite library. Using `std::sync::Mutex::lock()` in an async context can cause thread starvation and potential deadlocks in the Tokio runtime. The previous architecture required holding a mutex guard across database operations.

**Rationale:** `sqlx` is an async-native SQL toolkit that integrates naturally with Tokio. It provides connection pooling, eliminates the need for manual mutex management, and allows the runtime to efficiently schedule I/O operations.

**Solution:** Complete migration from `rusqlite` to `sqlx`:

1. **Dependency change:**
   ```toml
   # Before
   rusqlite = "0.38.0"

   # After
   sqlx = { version = "0.8", features = ["runtime-tokio", "sqlite"] }
   ```

2. **Connection management:**
   ```rust
   // Before: Manual mutex-wrapped connection
   let db_conn = Arc::new(Mutex::new(Connection::open(&database_path).unwrap()));

   // After: Connection pool (Clone + internally pooled)
   let db_pool = SqlitePoolOptions::new()
       .max_connections(5)
       .connect(&db_url)
       .await?;
   ```

3. **Database initialization (new function):**
   ```rust
   async fn init_database(db_pool: &SqlitePool) {
       sqlx::query("PRAGMA journal_mode=WAL").execute(db_pool).await.unwrap();
       sqlx::query("CREATE TABLE IF NOT EXISTS ...").execute(db_pool).await.unwrap();
   }
   ```

4. **Batched inserts with async transactions:**
   ```rust
   let mut tx = db_pool.begin().await.unwrap();
   for snapshot in batch.drain(..) {
       sqlx::query("INSERT INTO snapshots ...")
           .bind(&snapshot.timestamp)
           .bind(&snapshot.last_update_id)
           .bind(&snapshot.bids)
           .bind(&snapshot.asks)
           .execute(&mut *tx)
           .await
           .unwrap();
   }
   tx.commit().await.unwrap();
   ```

5. **Simplified data structure:**
   ```rust
   // New typed struct instead of (String, Vec<String>) tuple
   struct SnapshotData {
       timestamp: String,
       last_update_id: String,
       bids: String,
       asks: String,
   }
   ```

**Architecture changes:**
- Removed `Arc<Mutex<Connection>>` pattern entirely
- `SqlitePool` is `Clone` and handles pooling internally
- All database operations are now truly async
- Channel now carries typed `SnapshotData` instead of raw SQL strings
- Batching pattern preserved for write throughput

**Trade-offs:**
- Slightly larger dependency (sqlx vs rusqlite)
- Connection pool overhead (negligible for this use case)
- Database URL format change (`sqlite:path?mode=rwc`)

**Impact:** Eliminates potential thread starvation and deadlock issues. The Tokio runtime can now efficiently manage database I/O alongside WebSocket and S3 operations without blocking worker threads.

---

### Improved: Backpressure Handling (Problem #4 from PLAN.md)

**Files changed:** `src/main.rs` (lines 2, 28-29, 103, 241-257)

**Problem:** If WebSocket data arrives faster than it can be processed, the channel fills up, the sender blocks, and WebSocket reads stall. This could cause the WebSocket connection to be dropped by Binance.

**Rationale:** A data collection system should prioritize connection stability over perfect data capture. Orderbook snapshots arrive every 100ms - missing one is recoverable since the next snapshot provides a complete state. Blocking the WebSocket is worse than dropping a message.

**Solution:** Defense-in-depth approach combining three strategies:

1. **Increased channel buffer (100 → 1000):**
   ```rust
   let (db_tx, db_rx) = channel::<SnapshotData>(1000);  // Large buffer for burst absorption
   ```
   - Handles temporary bursts without dropping
   - At 10 msg/sec, buffer holds ~100 seconds of data

2. **Non-blocking sends with `try_send`:**
   ```rust
   match db_tx.try_send(data) {
       Ok(_) => {},
       Err(_) => {
           // Log and continue - never block WebSocket
       }
   }
   ```
   - WebSocket loop never stalls waiting for channel space
   - Connection remains healthy under all conditions

3. **Atomic counter for dropped messages:**
   ```rust
   static DROPPED_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);

   // On drop:
   let count = DROPPED_SNAPSHOTS.fetch_add(1, Ordering::Relaxed) + 1;
   println_with_timestamp!("WARNING: Channel full, dropped snapshot (total dropped: {})", count);
   ```
   - Visibility into backpressure events
   - Logged warnings for monitoring/alerting
   - Running total for debugging

**Additional change:** `save_snapshot` converted from `async fn` to `fn` since `try_send` is synchronous.

**Trade-offs:**
- Accepts possibility of data loss under extreme load
- For continuous streaming data (orderbook snapshots), this is acceptable
- If drops occur frequently, it signals a need to investigate performance

**Impact:** System remains stable under load. WebSocket connection never stalls due to internal backpressure. Operators have visibility into any dropped data through logs and counters.

---

### Improved: Microsecond Timestamp Resolution (Problem #5 from PLAN.md)

**Files changed:** `src/main.rs` (lines 33, 50, 168, 243, 279-293)

**Problem:** Timestamps used second-level granularity for data arriving at 100ms intervals. This meant ~10 records shared the same timestamp, making it impossible to reconstruct exact event ordering or measure latency accurately.

**Rationale:** Microsecond resolution provides 1,000,000x finer granularity than seconds, enabling:
- Precise event ordering within batches
- Accurate latency measurement between receipt and processing
- Future-proofing for higher-frequency data collection
- Better analysis capabilities in downstream tools

**Solution:** Changed timestamp from TEXT (seconds) to INTEGER (microseconds):

1. **Updated SnapshotData struct:**
   ```rust
   struct SnapshotData {
       timestamp: i64,  // Microseconds since Unix epoch
       // ...
   }
   ```

2. **Updated schema to INTEGER:**
   ```sql
   timestamp INTEGER NOT NULL,  -- was TEXT
   ```

3. **Changed timestamp capture to microseconds:**
   ```rust
   // Before
   .as_secs().to_string()

   // After
   .as_micros() as i64
   ```

4. **Updated archive query to read i64:**
   ```rust
   let snapshots: Vec<(i64, i64, String, String)> = rows
       .iter()
       .map(|row| (
           row.get::<i64, _>("timestamp"),
           // ...
       ))
       .collect();
   ```

**Benefits:**
- INTEGER storage is more efficient than TEXT (8 bytes vs ~10-13 bytes)
- Faster comparisons and sorting in SQLite
- Parquet stores i64 timestamps natively (efficient columnar storage)
- Tools like Polars/Pandas handle microsecond timestamps directly

**Note:** The `as i64` cast is safe - microseconds since Unix epoch won't overflow i64 until year 294,247.

**Impact:** Each record now has a unique, high-resolution timestamp. Enables precise ordering and latency analysis for downstream data processing.

---

### Improved: Multi-Exchange Schema with Deduplication (Problem #7 from PLAN.md)

**Files changed:** `src/main.rs` (extensive changes to schema, structs, and functions)

**Problem:** The original schema was Binance-specific (`lastUpdateId`, `bids`, `asks`). To support multiple exchanges (Coinbase, Bybit, OKX, Upbit) and multiple data types (orderbook, trades), a generalized schema was needed. Additionally, there was no deduplication mechanism to prevent duplicate records from WebSocket reconnections.

**Rationale:**
- Different exchanges have different ID fields (`lastUpdateId`, `sequence`, `seqId`, etc.)
- Different data types have different structures
- A multi-column unique constraint is the most robust deduplication approach
- Storing raw JSON payload provides flexibility for any data structure

**Solution:** Generalized schema with multi-column unique constraint:

1. **New SnapshotData struct:**
   ```rust
   struct SnapshotData {
       exchange: String,              // "binance", "coinbase", etc.
       symbol: String,                // "btcusdt", "BTC-USD", etc.
       data_type: String,             // "orderbook", "trade"
       exchange_sequence_id: String,  // Exchange-specific ID
       timestamp: i64,                // Our receipt time (microseconds)
       data: String,                  // Raw JSON payload
   }
   ```

2. **New database schema:**
   ```sql
   CREATE TABLE IF NOT EXISTS snapshots (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       exchange TEXT NOT NULL,
       symbol TEXT NOT NULL,
       data_type TEXT NOT NULL,
       exchange_sequence_id TEXT NOT NULL,
       timestamp INTEGER NOT NULL,
       data TEXT NOT NULL,
       UNIQUE(exchange, symbol, data_type, exchange_sequence_id)
   )
   ```

3. **Deduplication via INSERT OR IGNORE:**
   ```rust
   sqlx::query(
       "INSERT OR IGNORE INTO snapshots (exchange, symbol, data_type, exchange_sequence_id, timestamp, data) VALUES (?, ?, ?, ?, ?, ?)"
   )
   ```

4. **Updated save_snapshot signature:**
   ```rust
   fn save_snapshot(
       db_tx: &Sender<SnapshotData>,
       exchange: &str,
       symbol: &str,
       data_type: &str,
       exchange_sequence_id: &str,
       raw_data: &str,
   )
   ```

5. **Updated Parquet output columns:**
   - `exchange`, `symbol`, `data_type`, `exchange_sequence_id`, `timestamp`, `data`

**Why multi-column unique constraint over composite string key:**
- No delimiter collision issues (e.g., symbols containing underscores)
- SQLite handles uniqueness natively and efficiently
- More readable schema
- Better query flexibility

**Benefits:**
- Supports any exchange with any ID scheme
- Supports any data type (orderbook, trades, funding rates, etc.)
- Automatic deduplication on WebSocket reconnection
- Raw JSON preserves all exchange-specific fields
- Future-proof for adding new exchanges/data types

**Breaking change:** Existing databases with the old schema are incompatible. Delete the old `.db` file before running the updated code.

**Impact:** System is now ready for multi-exchange, multi-data-type collection with built-in deduplication.

---

### Improved: Hybrid Error Handling (Problem #8 from PLAN.md)

**Files changed:** `src/main.rs` (throughout)

**Problem:** Extensive use of `.unwrap()` throughout the codebase meant any error would cause a panic, potentially losing buffered data and crashing the entire process.

**Rationale:** Different error scenarios require different responses:
- Startup failures (missing env vars, DB connection) → should panic with clear message (unrecoverable)
- Per-message processing errors → log and skip, continue processing (don't let one bad message crash everything)
- Batch/transaction errors → log, data stays in buffer for next attempt
- Archive errors → log and return, will retry next cycle (data safe in SQLite)
- S3 upload errors → return Result to caller for handling

**Solution:** Hybrid approach matching error handling to context:

1. **Startup errors** - `.expect()` with descriptive messages:
   ```rust
   let aws_access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY must be set");
   let db_pool = SqlitePoolOptions::new()
       .connect(&db_url)
       .await
       .expect("Failed to create SQLite pool");
   ```

2. **Per-message processing** - log and skip:
   ```rust
   let text = match msg.into_text() {
       Ok(t) => t,
       Err(e) => {
           println_with_timestamp!("ERROR: Failed to extract text: {}", e);
           continue;  // Skip this message, process next
       }
   };
   ```

3. **Database worker** - graceful transaction handling:
   ```rust
   let tx = match db_pool.begin().await {
       Ok(tx) => tx,
       Err(e) => {
           println_with_timestamp!("ERROR: Failed to begin transaction: {}", e);
           continue;  // Data stays in batch, retry next interval
       }
   };
   // Track insert errors but continue batch
   if let Err(e) = sqlx::query(...).execute(&mut *tx).await {
       insert_errors += 1;
       println_with_timestamp!("ERROR: Failed to insert: {}", e);
   }
   ```

4. **Archive process** - log and return early:
   ```rust
   let mut file = match std::fs::File::create(&path) {
       Ok(f) => f,
       Err(e) => {
           println_with_timestamp!("ERROR: Failed to create archive: {}", e);
           return;  // Data safe in SQLite, retry next cycle
       }
   };
   ```

5. **S3 upload** - return Result for caller handling:
   ```rust
   async fn upload_to_s3(...) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
       let body = ByteStream::from_path(file_path).await?;
       client.put_object()...send().await?;
       Ok(())
   }
   ```

**Error categories and their handling:**

| Context | Strategy | Rationale |
|---------|----------|-----------|
| Startup | Panic with message | Unrecoverable, better to fail fast |
| WebSocket message | Log, skip | One bad message shouldn't crash collector |
| Channel send | Log, drop | Backpressure - already handled |
| DB transaction | Log, retry next interval | Data stays buffered |
| DB insert | Log, count errors | Continue batch, commit what we can |
| Archive file ops | Log, return | Data safe in SQLite |
| S3 upload | Return Result | Let caller decide (retry logic) |
| Post-upload cleanup | Log warning | Not critical, can clean manually |

**Trade-offs:**
- Slightly more verbose code
- Error messages need to be maintained
- Some edge cases might silently fail (logged but not acted upon)

**Impact:** System is now resilient to transient failures. Process stays running through recoverable errors, only panicking on truly unrecoverable startup failures. All errors are logged for monitoring and debugging.

---

### Improved: S3 Upload Retry with Exponential Backoff (Problem #9 from PLAN.md)

**Files changed:** `Cargo.toml`, `src/main.rs` (upload_to_s3 function)

**Problem:** S3 upload had no retry logic. Transient network failures (DNS issues, temporary S3 unavailability, network hiccups) would cause the upload to fail immediately, even though retrying would likely succeed.

**Rationale:** S3 is highly reliable but network operations can fail transiently. AWS best practices recommend exponential backoff for retrying failed requests. The `tokio-retry` crate provides a battle-tested implementation.

**Solution:** Added `tokio-retry` with exponential backoff strategy:

1. **New dependency:**
   ```toml
   tokio-retry = "0.3"
   ```

2. **Retry strategy configuration:**
   ```rust
   let retry_strategy = ExponentialBackoff::from_millis(1000)
       .factor(2)
       .max_delay(Duration::from_secs(30))
       .take(5);
   ```

   This produces delays: 1s → 2s → 4s → 8s → 16s (5 attempts total, ~31s max wait)

3. **Retry wrapper around S3 operations:**
   ```rust
   let result = Retry::spawn(retry_strategy, || {
       async move {
           let body = ByteStream::from_path(&file_path).await?;
           client.put_object().bucket(&bucket).key(&s3_key).body(body).send().await?;
           Ok(())
       }
   }).await;
   ```

4. **Logging for visibility:**
   - Logs retry attempts (attempt 2+)
   - Logs success with attempt count if retries occurred
   - Logs final failure with total attempts

**Retry behavior:**

| Attempt | Delay Before | Cumulative Time |
|---------|--------------|-----------------|
| 1 | 0s | 0s |
| 2 | 1s | 1s |
| 3 | 2s | 3s |
| 4 | 4s | 7s |
| 5 | 8s | 15s |

**What gets retried:**
- File read errors (ByteStream creation)
- S3 put_object errors (network, throttling, 5xx)

**Trade-offs:**
- New dependency (`tokio-retry`)
- Maximum 31 seconds delay before final failure (acceptable for hourly archive)
- Clones file path, bucket, and key strings on each retry (negligible overhead)

**Impact:** S3 uploads are now resilient to transient failures. Most network hiccups will be automatically recovered without operator intervention.

---

### Fixed: Hardcoded Region in Bucket Creation (Problem #10 from PLAN.md)

**Files changed:** `src/main.rs` (create_bucket_if_not_exists function)

**Problem:** Bucket creation used hardcoded `UsWest2` region despite `AWS_REGION` environment variable being available.

**Solution:** Updated `create_bucket_if_not_exists` to accept region as parameter and handle the us-east-1 special case:

```rust
async fn create_bucket_if_not_exists(client: &Client, bucket_name: &str, region: &str) {
    // us-east-1 is the default region and doesn't use location constraint
    let create_bucket_config = if region == "us-east-1" {
        None
    } else {
        Some(CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(region))
            .build())
    };
    // ...
}
```

**Note:** `us-east-1` is AWS's default region and requires no location constraint. Specifying one for us-east-1 actually causes an error.

**Impact:** Buckets are now created in the correct region as specified by `AWS_REGION` environment variable.

---

### Refactored: Module-Per-Concern Code Structure

**Files changed:** `src/main.rs` split into multiple modules

**Problem:** All code (~500 lines) was in a single `main.rs` file, mixing concerns:
- Configuration loading
- Database operations
- WebSocket handling
- S3/Archive operations
- Data models
- Utility functions

This made the code harder to navigate, test, and maintain.

**Solution:** Split into flat module structure following the Module-Per-Concern pattern:

```
src/
├── main.rs       # Entry point, orchestration only (~110 lines)
├── config.rs     # Configuration loading from environment
├── db.rs         # SQLite operations (init, worker, queries)
├── websocket.rs  # WebSocket connection and message handling
├── archive.rs    # Parquet creation, S3 upload, scheduling
├── models.rs     # SnapshotData struct
└── utils.rs      # println_with_timestamp macro, counters
```

**Module responsibilities:**

| Module | Responsibility |
|--------|----------------|
| `main.rs` | Application entry point, spawns workers, wires dependencies |
| `config.rs` | `Config` struct, loads all env vars, creates directories |
| `db.rs` | `create_pool`, `init_database`, `db_worker`, `fetch_all_snapshots`, `delete_all_snapshots` |
| `websocket.rs` | `websocket_worker`, `save_snapshot`, Binance-specific handling |
| `archive.rs` | `create_bucket_if_not_exists`, `run_archive_scheduler`, `archive_snapshots`, `upload_to_s3` |
| `models.rs` | `SnapshotData` struct definition |
| `utils.rs` | `DROPPED_SNAPSHOTS` counter (macro removed after tracing migration) |

**Benefits:**
- Clear separation of concerns
- Easier to locate and modify specific functionality
- Each module can be understood in isolation
- Prepares codebase for adding more exchanges (new files in same structure)
- Enables future unit testing per module

**Breaking changes:** None - same functionality, just reorganized.

**Impact:** Codebase is now organized for maintainability and future growth. Main.rs reduced from ~485 lines to ~110 lines.

---

### Improved: Structured Logging with Tracing

**Files changed:** `Cargo.toml`, `src/main.rs`, `src/db.rs`, `src/websocket.rs`, `src/archive.rs`, `src/utils.rs`

**Problem:** The codebase used a custom `println_with_timestamp!` macro for logging. While functional, this approach had limitations:
- Plain text output not suitable for log aggregation tools
- No log levels (info, warn, error, debug)
- No structured fields for filtering/searching
- Output only to stdout
- No way to control verbosity at runtime

**Rationale:** Production systems need structured logging for:
- Log aggregation (ELK, Loki, Datadog)
- Filtering by severity and fields
- Performance analysis via structured spans
- Multiple output destinations (stdout for humans, files for aggregation)

**Solution:** Implemented `tracing` with multiple outputs:

1. **New dependencies:**
   ```toml
   tracing = "0.1"
   tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
   tracing-appender = "0.2"
   ```

2. **Dual-output tracing initialization in `main.rs`:**
   ```rust
   fn init_tracing() {
       std::fs::create_dir_all("logs").expect("Failed to create logs directory");

       // Rolling file appender - rotates daily
       let file_appender = RollingFileAppender::new(Rotation::DAILY, "logs", "collector.log");

       // Stdout layer - pretty format for humans
       let stdout_layer = fmt::layer()
           .with_target(true)
           .with_thread_ids(false)
           .with_file(false);

       // File layer - JSON format for log aggregation
       let file_layer = fmt::layer()
           .json()
           .with_writer(file_appender);

       // Environment filter - default to info, configurable via RUST_LOG
       let env_filter = EnvFilter::try_from_default_env()
           .unwrap_or_else(|_| EnvFilter::new("info"));

       tracing_subscriber::registry()
           .with(env_filter)
           .with(stdout_layer)
           .with(file_layer)
           .init();
   }
   ```

3. **Replaced all `println_with_timestamp!` calls with tracing macros:**
   ```rust
   // Before
   println_with_timestamp!("Connected to Binance WebSocket for {}", market_symbol);

   // After
   info!(symbol = %market_symbol, "Connected to Binance WebSocket");
   ```

4. **Structured fields throughout:**
   ```rust
   // Structured key-value pairs
   info!(
       snapshot_count = snapshots.len(),
       path = %archive_file_path.display(),
       "Written snapshots to Parquet file"
   );

   warn!(
       total_dropped = count,
       "Channel full, dropped snapshot"
   );

   error!(
       error = %e,
       bucket = bucket_name,
       "Failed to create bucket"
   );
   ```

5. **Log levels used:**
   - `error!` - Failures requiring attention
   - `warn!` - Degraded behavior (dropped messages, retries)
   - `info!` - Normal operations (connections, archives, startup)
   - `debug!` - Verbose details (sampled message previews, pings/pongs)

6. **Removed old macro from `utils.rs`** - Only counters remain.

**Output formats:**

Stdout (human-readable):
```
2026-01-16T10:30:00.123Z  INFO crypto_collector::websocket: Connected to Binance WebSocket symbol=BTCUSDT
```

File (JSON for aggregation):
```json
{"timestamp":"2026-01-16T10:30:00.123Z","level":"INFO","target":"crypto_collector::websocket","fields":{"symbol":"BTCUSDT","message":"Connected to Binance WebSocket"}}
```

**Runtime configuration via RUST_LOG:**
```bash
RUST_LOG=debug ./collector          # All debug logs
RUST_LOG=warn ./collector           # Warnings and errors only
RUST_LOG=crypto_collector=debug     # Debug for this crate only
```

**Benefits:**
- Structured fields enable filtering (`symbol=BTCUSDT`)
- JSON output integrates with log aggregation pipelines
- Daily log rotation prevents disk exhaustion
- Runtime-configurable verbosity via RUST_LOG
- Pretty stdout format for local development
- Industry-standard tracing ecosystem

**Trade-offs:**
- Three new dependencies (~200KB compiled)
- Creates `logs/` directory and writes files
- Slight overhead for JSON serialization (negligible)

**Impact:** Production-ready logging infrastructure. Operators can now aggregate logs, filter by fields, and adjust verbosity without code changes.
