# Changelog

## 2026-01-21

### Docs: Added Important Rules to CLAUDE.md

**Files modified:**
- `CLAUDE.md` - Added rules 5, 6, and 7

Added three new important rules:
- **Rule 5: Apply expert domain knowledge** - Use deep expertise relevant to the domain (networking, cryptography, concurrency, etc.)
- **Rule 6: Present multiple implementation options** - For non-trivial changes, present 2-3 approaches with pros/cons and recommendation
- **Rule 7: Ensure code quality and linting** - Verify clippy, tests, and build pass before committing

---

### Chore: Fix All Clippy Warnings

**Files modified:**
- `src/parquet.rs` - Collapsed if statements, use `.is_multiple_of()`, use `inspect_err`
- `src/upload.rs` - Collapsed if statements
- `src/utils.rs` - Collapsed if statements
- `src/websocket.rs` - Collapsed if statements, use `.is_multiple_of()`, added `#[allow(clippy::too_many_arguments)]`

Applied clippy auto-fixes and manual fixes to resolve all warnings. Code now passes `cargo clippy` with zero warnings.

---

### Feature: Add Coinbase Heartbeats Channel Subscription

**Files modified:**
- `src/exchanges/coinbase.rs` - Added heartbeats channel subscription

**Problem:** Coinbase WebSocket channels close within 60-90 seconds if no updates are sent. This is separate from WebSocket protocol-level ping/pong and particularly affects illiquid trading pairs.

**Solution:** Subscribe to the `heartbeats` channel alongside other channel subscriptions. Coinbase sends a heartbeat message every second per subscribed product, keeping the connection alive even when no market data is flowing.

**Reference:** [Coinbase WebSocket Channels Documentation](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels)

---

### Docs: Added Commit Message Format Rule

**Files modified:**
- `CLAUDE.md` - Added rule #4 for detailed commit messages

Added requirement for conventional commit format with descriptive body explaining what changed and why, plus Co-Authored-By attribution. This ensures consistent commit history that's easy to understand.

---

### Fixed: WebSocket Ping Starvation Under High Message Volume

**Files modified:**
- `src/websocket.rs` - Fixed ping timer starvation in select! loop

---

**Problem:** OKX and Coinbase connections were frequently dropping with "Connection reset without closing handshake" errors. The servers were closing connections because keepalive pings weren't being sent.

**Root cause:** The `biased` keyword in `tokio::select!` caused the ping timer branch to never be selected when messages were continuously arriving. For high-volume streams (orderbook updates every 100ms), `read.next()` was always ready first, so the ping timer was starved indefinitely.

```rust
// BROKEN: biased causes ping timer to never fire under load
tokio::select! {
    biased;  // Always polls read.next() first
    result = read.next() => result,
    _ = timer.tick() => { send_ping(); }  // Never reached!
}
```

**Solution:**

1. **Removed `biased` keyword** - Allows fair scheduling so ping timer can occasionally "win"
2. **Added post-message ping check** - Backup for sustained high message volume

```rust
// FIXED: Fair scheduling + backup check
tokio::select! {
    // NO biased - fair scheduling
    result = read.next() => result,
    _ = timer.tick() => { send_ping(); continue; }
}

// After processing each message, backup check
if last_ping_sent.elapsed() >= ping_interval {
    send_ping();
}
```

**Why not use timeout() wrapper?**

A previous fix (2026-01-20) removed `timeout()` from `read.next()` because it caused false disconnects. This fix does NOT reintroduce timeout - it uses fair scheduling and backup checks instead.

**Impact:**
- OKX/Coinbase connections should remain stable
- Pings sent reliably even under high message load
- No false disconnects from spurious timeouts

---

## 2026-01-20

### Added: System Resource Monitoring Dashboard

**Files modified/created:**
- `docker-compose.yml` - Added Node Exporter service
- `prometheus.yml` - Added Node Exporter scrape target
- `grafana/provisioning/dashboards/system.json` - New system resources dashboard

---

**Purpose:** Add visibility into host system resources (CPU, memory, disk, network) alongside application metrics.

**Changes:**

1. **Node Exporter service in docker-compose.yml:**
   ```yaml
   node-exporter:
     image: prom/node-exporter:latest
     container_name: node-exporter
     restart: unless-stopped
     volumes:
       - /proc:/host/proc:ro
       - /sys:/host/sys:ro
       - /:/rootfs:ro
     command:
       - '--path.procfs=/host/proc'
       - '--path.sysfs=/host/sys'
       - '--path.rootfs=/rootfs'
       - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
     ports:
       - "9100:9100"
   ```

2. **Prometheus scrape config:**
   ```yaml
   - job_name: 'node-exporter'
     static_configs:
       - targets: ['node-exporter:9100']
   ```

3. **New Grafana dashboard panels:**

   | Panel | Metric | Type |
   |-------|--------|------|
   | CPU Usage | `node_cpu_seconds_total` | Gauge + Timeseries by mode |
   | Memory Usage | `node_memory_*` | Gauge + Stacked area |
   | Disk Free | `node_filesystem_*` | Gauge + Bar gauge by mount |
   | Network I/O | `node_network_*_bytes_total` | Bytes/sec + Packets/sec |
   | Load Average | `node_load1`, `node_load5`, `node_load15` | Timeseries |
   | System Uptime | `node_time_seconds - node_boot_time_seconds` | Stat |

**Note:** The existing "Channel Queue Depth" panel was kept as `collector_channel_queue_depth` metric is still actively used.

---

### WebSocket Hardening: Eliminated Connection Flakiness

**Files modified:**
- `src/websocket.rs` - Major rewrite of message loop
- `src/exchanges/mod.rs` - Added `ping_interval()` to Exchange trait
- `src/exchanges/binance.rs` - Added `ping_interval()` returning `None`
- `src/exchanges/coinbase.rs` - Added `ping_interval()` returning 30s
- `src/exchanges/okx.rs` - Added `ping_interval()` returning 20s
- `src/exchanges/bybit.rs` - Added `ping_interval()` returning 20s
- `src/exchanges/upbit.rs` - Added `ping_interval()` returning `None`

---

**Problem:** WebSocket connections were flaky across all exchanges, with spurious disconnects and reconnects. Comparison with the stable `bayes-rust` codebase revealed several architectural issues.

**Root cause analysis:**

1. **`timeout()` wrapper on `read.next()` caused false disconnects** - The 180s timeout could fire spuriously due to `tokio::select!` scheduling, even when the connection was healthy.

2. **Complex `select!` structure with competing branches** - The ping timer (20s) competed with message processing, causing unnecessary iterations and potential message delays for exchanges that don't need client pings.

3. **Race condition between subscription and data timeout** - `last_data_received` was set at connection time, but data doesn't arrive until after subscription confirmation, creating a window where timeout could fire prematurely.

4. **Unfair `select!` polling** - Without `biased`, tokio polls futures in random order, allowing the ping timer to starve message processing.

---

**Solution:**

1. **Removed `timeout()` wrapper from `read.next()`**
   - Rely on data timeout mechanism only (which already handles "alive but no data" case)
   - Trust TCP keepalive and WebSocket ping/pong for connection health
   - Matches the proven simple loop pattern from `bayes-rust`

2. **Added `biased` to `tokio::select!`**
   - Prefer message processing over ping timer
   - Reduces latency and prevents ping timer from starving message reads

3. **Made ping timer conditional based on exchange requirements**
   - Added `ping_interval()` method to Exchange trait
   - Exchanges that need client pings (OKX, Bybit, Coinbase) return `Some(Duration)`
   - Exchanges where server initiates pings (Binance, Upbit) return `None`
   - When `None`, use simple blocking `read.next().await` without `select!`

4. **Fixed data timeout race condition**
   - Changed `last_data_received` from `Instant` to `Option<Instant>`
   - Set to `None` initially, only becomes `Some` when first data arrives
   - Added 30-second grace period after subscription before checking timeout
   - Separate logic for "never received data" vs "data stopped flowing"

---

**New Exchange trait method:**

```rust
/// Returns the ping interval if this exchange requires client-initiated pings.
fn ping_interval(&self) -> Option<Duration> {
    None  // Default: server initiates pings
}
```

**Ping requirements by exchange:**

| Exchange | `ping_interval()` | Notes |
|----------|-------------------|-------|
| Binance | `None` | Server sends pings every ~3 min |
| Coinbase | `Some(30s)` | Client must ping within ~100s |
| OKX | `Some(20s)` | Client must ping within 30s |
| Bybit | `Some(20s)` | Client must ping within ~10 min |
| Upbit | `None` | Server likely initiates pings |

---

**Message loop strategy:**

```rust
// For exchanges with client pings (OKX, Bybit, Coinbase):
tokio::select! {
    biased;  // Prefer messages over pings
    result = read.next() => { process(result) }
    _ = ping_timer.tick() => { send_ping() }
}

// For exchanges with server pings (Binance, Upbit):
// Simple blocking read - no select needed
let result = read.next().await;
process(result);
```

---

**Data timeout logic:**

```rust
match last_data_received {
    Some(last) => {
        // Have received data before - check if it stopped
        if last.elapsed() > timeout * 3 { reconnect() }
    }
    None => {
        // Never received data - check grace period
        if subscription_sent.elapsed() > timeout * 3 { reconnect() }
    }
}
```

---

**Impact:**
- Eliminates spurious reconnects from timeout races
- Reduces latency for message processing
- Simplifies code path for exchanges that don't need client pings
- Properly handles subscription grace period
- Matches proven architecture from stable `bayes-rust` codebase

---

### Major Architecture Change: Direct Parquet Streaming (Eliminated SQLite)

**Files added:**
- `src/parquet.rs` - Streaming Parquet writer with automatic file rotation
- `src/upload.rs` - Background S3 upload worker with retry logic
- `src/s3.rs` - S3 client creation (extracted from old archive.rs)

**Files removed:**
- `src/db.rs` - SQLite database operations (no longer needed)
- `src/archive.rs` - Archive scheduler and S3 upload (replaced by parquet.rs + upload.rs)

**Files modified:**
- `src/main.rs` - Complete rewrite of worker orchestration
- `src/models.rs` - Added `WriterKey` struct for Parquet writer HashMap keys
- `src/metrics.rs` - Replaced DB metrics with Parquet/upload metrics
- `src/config.rs` - Removed database/archive config, simplified storage config
- `config.toml` - Removed `[database]` and `[archive]` sections
- `Cargo.toml` - Removed `sqlx` and `polars` dependencies
- `grafana/provisioning/dashboards/collector.json` - Updated dashboard for new metrics

---

**Problem:** Critical production failure during market spike. When Binance was sending ~1000 msgs/sec:
- 40,000+ messages dropped
- RAM spiked to 4GB (server limit)
- Archive operation timed out
- System became unresponsive

**Root cause analysis:** The SQLite-based architecture had a fundamental design flaw:

```
OLD ARCHITECTURE:
WebSocket → Channel → db_worker → SQLite → archive_scheduler → Parquet → S3
                          ↑                        ↓
                     [CONTENTION]              [RAM SPIKE]
```

1. **Write/Archive contention:** SQLite was both the write buffer AND the archive source. During high message volume, INSERT operations blocked waiting for archive's SELECT/DELETE to complete.

2. **Channel backup:** When db_worker blocked on SQLite, the channel filled up. With `try_send()`, excess messages were dropped.

3. **Memory explosion:** Archive loaded entire result sets into memory to convert to Parquet. 100K rows × 5KB average = 500MB just for data, plus Polars DataFrame overhead.

4. **Data loss risk:** The old architecture deleted rows from SQLite BEFORE verifying S3 upload success. If upload failed after delete, data was permanently lost.

---

**Solution: Direct Parquet Streaming Architecture**

```
NEW ARCHITECTURE:
WebSocket → Channel → parquet_worker → Parquet Files → upload_worker → S3
                           ↓                                ↓
                    [BOUNDED MEMORY]              [VERIFIED DELETE]
```

Key changes:

1. **Eliminated SQLite entirely** - WebSocket events stream directly to Parquet files
2. **Bounded memory via row group flushing** - Only 1000 rows buffered at a time per writer
3. **Decoupled write and upload** - Writes never block on S3 operations
4. **Safe deletion** - Local files only deleted after S3 upload is verified

---

**New data flow:**

1. **WebSocket workers** receive market data and send `MarketEvent` to channel
2. **parquet_worker** receives events, routes to appropriate `ParquetWriterInstance` based on (exchange, symbol, data_type)
3. **ParquetWriterInstance** buffers rows, flushes to disk as row groups every 1000 rows
4. **File rotation** triggers when file age > 1 hour OR file size > 500MB
5. **Completed files** sent to upload_worker via channel
6. **upload_worker** uploads to S3 with exponential backoff (5 attempts, 1s-30s delays)
7. **Verification** via HEAD request confirms upload size matches local file
8. **Local file deleted** only after successful verification

---

**Memory profile comparison:**

| Scenario | Old Architecture | New Architecture |
|----------|-----------------|------------------|
| Normal operation (100 msg/s) | ~200MB (SQLite + buffers) | ~50MB (row buffers only) |
| High volume (1000 msg/s) | 4GB+ spike, OOM risk | ~100MB steady |
| Archive cycle | +500MB-2GB spike | No spike (streaming) |
| Per writer memory | N/A | ~70-100KB (1000 row buffer) |

---

**New modules:**

### `src/parquet.rs`

```rust
/// Constants
const MAX_FILE_AGE: Duration = Duration::from_secs(3600);  // 1 hour
const MAX_FILE_SIZE: u64 = 500 * 1024 * 1024;              // 500 MB
const WRITE_BATCH_SIZE: usize = 1000;                       // rows per row group

/// ParquetWriterInstance - One writer per (exchange, symbol, data_type)
struct ParquetWriterInstance {
    writer: ArrowWriter<File>,
    buffer: Vec<MarketEvent>,  // Buffered rows
    // ... rotation tracking fields
}

/// ParquetWriterManager - Manages all writers, handles rotation
pub struct ParquetWriterManager {
    writers: HashMap<WriterKey, ParquetWriterInstance>,
    // ...
}

/// parquet_worker - Background task
pub async fn parquet_worker(
    event_rx: Receiver<MarketEvent>,
    data_dir: PathBuf,
    home_server_name: Option<String>,
    upload_tx: Sender<CompletedFile>,
    shutdown_rx: broadcast::Receiver<()>,
)
```

### `src/upload.rs`

```rust
/// upload_worker - Background S3 upload with retry
pub async fn upload_worker(
    file_rx: Receiver<CompletedFile>,
    storage_mode: StorageMode,
    local_storage_path: Option<PathBuf>,
    bucket_name: String,
    s3_client: Option<Client>,
    shutdown_rx: broadcast::Receiver<()>,
)

/// Exponential backoff: 1s, 2s, 4s, 8s, 16s (max 30s), 5 attempts
async fn upload_to_s3(client: &Client, bucket: &str, file: &CompletedFile) -> Result<(), String>

/// Verify upload via HEAD request
async fn verify_s3_upload(client: &Client, bucket: &str, key: &str, expected_size: u64) -> bool
```

---

**Configuration changes:**

**Removed from config.toml:**
```toml
# These sections no longer exist:
[database]
path = "data/collector.db"
batch_interval_secs = 1

[archive]
interval_secs = 300
archive_dir = "data/archive"
```

**New storage configuration:**
```toml
[storage]
# Storage mode: "s3" (default), "local", or "both"
mode = "s3"
# Directory for Parquet files (streaming writes)
data_dir = "data/parquet"
# Directory for permanent local storage (only used when mode is "local" or "both")
local_path = "data/archive"
```

---

**Metrics changes:**

**Removed metrics:**
- `db_write_duration_seconds` - No more database writes
- `db_snapshots_written_total` - No more database
- `db_insert_errors_total` - No more database
- `archive_failures_total` - Replaced by upload metrics
- `archives_completed_total` - Replaced by file rotation metrics
- `snapshots_archived_total` - Replaced by rows written metrics

**New metrics:**
- `parquet_rows_written_total` - Total rows written to Parquet files
- `parquet_rows_buffered` - Current rows in buffer (gauge)
- `parquet_files_rotated_total` - Files completed and sent for upload (by exchange, symbol)
- `parquet_write_errors_total` - Write failures
- `upload_queue_depth` - Files waiting for upload (gauge)
- `s3_upload_duration_seconds` - Upload latency histogram
- `s3_upload_errors_total` - Upload failures (by exchange, symbol)
- `s3_uploads_completed_total` - Successful uploads (by exchange, symbol)

---

**Shutdown behavior:**

The new architecture has graceful shutdown with per-component timeouts:

```rust
let ws_timeout = Duration::from_secs(10);      // WebSocket workers
let parquet_timeout = Duration::from_secs(30); // Flush pending data
let upload_timeout = Duration::from_secs(60);  // Finish in-progress uploads
```

On shutdown:
1. WebSocket workers stop receiving new data
2. parquet_worker flushes all buffered rows and rotates all files
3. upload_worker completes pending uploads

**Data loss on crash:** Only unflushed buffer data (up to 1000 rows per writer) may be lost on unexpected crash. This is acceptable given the bounded loss and the alternative of complex WAL/recovery logic.

---

**File path structure:**

Parquet files are written to:
```
{data_dir}/{exchange}/{symbol}/{data_type}/{server}/{date}/{timestamp}.parquet

Example:
data/parquet/binance/btcusdt/orderbook/cherry-wondrous-mongrel/2026-01-20/1737331200000.parquet
```

S3 key uses the same relative path structure.

---

**Dependencies removed from Cargo.toml:**

```toml
# Removed:
sqlx = { version = "0.8", features = ["runtime-tokio", "sqlite"] }
polars = { version = "0.46", features = ["lazy", "parquet", "dtype-struct"] }
```

SQLite and Polars are no longer needed. The `arrow` and `parquet` crates (already present from previous changes) handle all Parquet operations.

---

**Why this architecture is more stable:**

1. **No shared mutable state** - Each writer is independent, no database contention
2. **Bounded memory** - Row group flushing caps memory regardless of throughput
3. **Backpressure via try_send** - Channel overflow drops oldest data, not crash
4. **Crash-safe uploads** - Files persist on disk until S3 upload verified
5. **Simple failure domains** - If one exchange's writer fails, others continue
6. **Observable** - Prometheus metrics show buffer depth, rotation rate, upload health

---

**Grafana dashboard changes:**

The "Database & Archive" section was renamed to "Parquet & Upload" with updated panels:

| Old Panel | New Panel | New Metric |
|-----------|-----------|------------|
| DB Write Latency (p50, p95) | Buffer & Queue Depth | `collector_parquet_rows_buffered`, `collector_upload_queue_depth` |
| DB Writes/sec | Parquet Rows Written/sec | `collector_parquet_rows_written_total` |
| DB Insert Errors (1h) | (merged into Buffer & Queue Depth) | - |
| Archives Completed | Files Rotated | `collector_parquet_files_rotated_total` |
| Records Archived | Total Rows Written | `collector_parquet_rows_written_total` |
| Archive Failures | S3 Upload Failures | `collector_s3_upload_failures_total` |
| - | S3 Upload Retries (new) | `collector_s3_upload_retries_total` |

---

## 2026-01-19

### Fixed: Multiple Parquet Files Per Archive Cycle (REGRESSION)

**Files changed:** `Cargo.toml`, `src/archive.rs`

**Problem:** The OOM fix (batch-based archive processing) inadvertently changed behavior to create one Parquet file per 2,000-row batch instead of one file per exchange/symbol per archive cycle. With 100K rows, this resulted in 50+ small files instead of one consolidated file.

**Root cause:** The batch loop called `archive_group()` which created and uploaded a new file for each batch:
```
OLD: loop { fetch 2000 → create file → upload → delete }
     Result: 50 files for 100K rows
```

**Solution:** Replaced Polars-based `archive_group` with Arrow's streaming `ArrowWriter`:
```
NEW: create file → loop { fetch 2000 → write row group } → close → upload
     Result: 1 file with 50 row groups for 100K rows
```

**Changes:**
1. Added `arrow` and `parquet` crates to Cargo.toml (replacing Polars for archive)
2. Rewrote `archive_table_group` to use `ArrowWriter::try_new()` for streaming writes
3. Each batch becomes a row group within the same file
4. Removed old `archive_group` function entirely

**Arrow schema:**
```rust
let schema = Arc::new(Schema::new(vec![
    Field::new("exchange", DataType::Utf8, false),
    Field::new("symbol", DataType::Utf8, false),
    Field::new("exchange_sequence_id", DataType::Utf8, false),
    Field::new("timestamp_collector", DataType::Int64, false),
    Field::new("timestamp_exchange", DataType::Int64, false),
    Field::new("data", DataType::Utf8, false),
]));
```

**Benefits:**
- ONE file per exchange/symbol per archive cycle (original behavior restored)
- Memory stays bounded (only one 2000-row batch in memory at a time)
- ZSTD compression applied to each row group
- Streaming writes mean no OOM regardless of total data volume

**Verification:** After archive, check S3/local storage - should see exactly ONE `.parquet` file per exchange/symbol combination, not multiple files.

---

### Fixed: Critical Data Loss Bug in Archive DELETE (REGRESSION)

**Files changed:** `src/db.rs`, `src/archive.rs`

**Problem:** The previous ID-range deletion fix caused **catastrophic data loss**. When archiving, DELETE affected 20x more rows than were actually archived:

```
SELECT ... WHERE exchange = 'binance' AND symbol = 'btcusdt' LIMIT 10000  → 10,000 rows
DELETE FROM orderbooks WHERE id BETWEEN 1 AND 40000                       → 208,318 rows deleted!
```

**Root cause:** Rows from different exchanges/symbols are interleaved by auto-increment ID:
- ID 1: binance/btcusdt
- ID 2: coinbase/btcusd
- ID 3: okx/btcusdt
- ID 4: binance/btcusdt
- ...

When fetching 10,000 rows for one exchange/symbol, the IDs are sparse (1, 4, 8, 12...). The `DELETE WHERE id BETWEEN min AND max` had no exchange/symbol filter, so it deleted ALL rows in that ID range including rows from other exchanges that were never archived.

**Solution:**
1. Changed DELETE to include exchange/symbol filter:
   ```sql
   DELETE FROM orderbooks WHERE exchange = ? AND symbol = ? AND id <= ?
   ```
2. Added indexes on `(exchange, symbol)` for both tables to speed up queries
3. Reduced `ARCHIVE_BATCH_SIZE` from 10,000 to 2,000 to minimize lock contention

**Why this works:**
- `SELECT ... WHERE exchange=? AND symbol=? ORDER BY id LIMIT 2000` gets the oldest 2,000 rows for that specific exchange/symbol
- The max_id from these rows is the cutoff point
- `DELETE ... WHERE exchange=? AND symbol=? AND id <= max_id` deletes exactly those rows
- Rows from other exchanges are never touched

**Verification:** After this fix, the `rows_affected` in DELETE logs should equal the `rows_returned` from SELECT (both ~2,000).

---

### Fixed: Archive Timeout and Excessive "?" Logging

**Files changed:** `src/db.rs`, `src/archive.rs`

**Problem:** Archive operations timed out after 5 minutes, and logs were swamped with thousands of `?` characters from SQLx query logging.

**Root cause:** The DELETE queries used IN clauses with 10,000 placeholders:
```sql
DELETE FROM orderbooks WHERE exchange = ? AND symbol = ? AND exchange_sequence_id IN (?, ?, ?, ... x10000)
```

This caused:
- SQLx to log the massive query with 10,000 `?` characters
- SQLx warnings about slow query performance
- Queries taking so long they exceeded the 5-minute archive timeout

**Solution:** Replaced IN-clause deletion with ID-range deletion. Since rows are fetched with `ORDER BY id LIMIT batch_size`, we track the min/max row IDs and delete with a simple range query:

```sql
DELETE FROM orderbooks WHERE id BETWEEN ? AND ?
```

**Changes:**
1. Added `id: i64` field to `DbRow` struct
2. Updated `fetch_orderbooks_batch` and `fetch_trades_batch` to SELECT the `id` column
3. Replaced `delete_orderbooks_by_seq_ids` and `delete_trades_by_seq_ids` with `delete_orderbooks_by_id_range` and `delete_trades_by_id_range`
4. Updated `archive.rs` to track min/max IDs from fetched batches and use range deletion

**Benefits:**
- DELETE query has only 2 placeholders instead of 10,000
- No more excessive `?` logging from SQLx
- Much faster deletion using the primary key index
- Archive operations complete well within the 5-minute timeout

---

### Fixed: WebSocket Connection Drops for Coinbase and OKX (PING/PONG Keepalive)

**Files changed:**
- `src/exchanges/mod.rs` - Added `build_ping_message()` to Exchange trait
- `src/exchanges/binance.rs` - Returns `None` (server initiates pings)
- `src/exchanges/coinbase.rs` - Returns `Message::Ping(vec![])` (protocol-level)
- `src/exchanges/okx.rs` - Returns `Message::Text("ping")` + parses "pong" response
- `src/exchanges/bybit.rs` - Returns `Message::Text(r#"{"op":"ping"}"#)`
- `src/exchanges/upbit.rs` - Returns `None` (not configured)
- `src/websocket.rs` - Added 20-second ping timer with `tokio::select!`
- `src/metrics.rs` - Added `WEBSOCKET_PINGS_SENT` and `WEBSOCKET_PONGS_RECEIVED` counters
- `grafana/provisioning/dashboards/collector.json` - Added ping/pong rate panels

**Problem:** Coinbase and OKX WebSocket connections were being abruptly closed with "connection reset without closing handshake" errors. Investigation revealed different exchanges have different keepalive requirements:

| Exchange | Keepalive Mechanism | Timeout |
|----------|--------------------|---------|
| Binance | Server sends protocol PING → We respond with PONG | 60s |
| Coinbase | We send **protocol-level PING frames** | ~100s |
| OKX | We send text `"ping"` → Receive `"pong"` | 30s |
| Bybit | We send JSON `{"op":"ping"}` → Receive pong JSON | 10 min |

**Root cause:** Our code only responded to server-initiated PINGs (Binance model). Coinbase, OKX, and Bybit require **client-initiated** pings. Without sending pings, OKX connections died after 30 seconds and Coinbase after ~100 seconds.

**Solution:**
1. Added `build_ping_message()` method to Exchange trait - each exchange returns its specific ping format (or None if server initiates)
2. Added 20-second ping interval timer in the WebSocket message loop using `tokio::select!`
3. Added OKX "pong" response parsing (literal text "pong")
4. Added new metrics: `collector_websocket_pings_sent_total` and `collector_websocket_pongs_received_total`
5. Added Grafana panels for ping/pong rates by exchange

**Impact:** Connections should now remain stable without unexpected disconnections. Ping/pong metrics provide visibility into keepalive health.

---

### Fixed: Grafana Reconnects Panel Not Showing Data for Some Exchanges

**Files changed:** `grafana/provisioning/dashboards/collector.json`

**Problem:** The Grafana reconnects panel showed 0 for Coinbase despite the raw Prometheus query returning correct values. OKX showed correct reconnect counts.

**Root cause:** The dashboard used `increase(collector_websocket_reconnects_total[1h])` which has limitations:
- `increase()` requires at least 2 data points within the window
- Counter resets (pod restarts) cause `increase()` to return 0 or negative values
- Sparse reconnects may not have enough samples in the 1h window

**Solution:** Changed the query to use the dashboard's time range variable:
```json
"expr": "increase(collector_websocket_reconnects_total[$__range])"
```

Also updated panel title from "Reconnects (1h)" to "Reconnects" since it now adapts to the selected time range.

---

### Removed: Unused SEQUENCE_* Metrics

**Files changed:** `src/metrics.rs`

**Problem:** Four SEQUENCE_* metrics were defined but never used anywhere in the codebase:
- `collector_sequence_gaps_total`
- `collector_sequence_gap_size`
- `collector_sequence_out_of_order_total`
- `collector_sequence_duplicates_total`

These were originally planned for sequence tracking but the implementation was never completed.

**Solution:** Removed all four metric definitions from `src/metrics.rs`.

**Impact:** Reduces binary size slightly and eliminates confusion about metrics that appear in `/metrics` output but are always 0.

---

### Fixed: Critical Memory Leak in Archive System (4GB+ RAM Usage)

**Files changed:** `src/db.rs`, `src/archive.rs`

**Problem:** The archive system loaded the **entire database into memory** before processing, causing 4GB+ RAM usage and database issues on long-running servers. The memory growth was unbounded - the longer the collector ran between archives, the more memory it consumed.

**Root cause:** `archive_all_data()` called `fetch_all_orderbooks()` and `fetch_all_trades()` which used `fetch_all()` to load every row into a `Vec<DbRow>`. With hours of data from multiple exchanges, this could be millions of rows × ~1KB each = gigabytes of RAM. Additionally, the data was duplicated multiple times:
1. Loaded into `all_rows` vector
2. Grouped into a HashMap by (exchange, symbol, data_type)
3. Cloned again when building Parquet columns

**Solution:** Implemented batch-based archive processing:

1. **New paginated fetch functions in `db.rs`:**
   - `get_orderbook_groups()` / `get_trade_groups()` - Get distinct (exchange, symbol) pairs
   - `fetch_orderbooks_batch()` / `fetch_trades_batch()` - Fetch limited batches (10K rows max)
   - `delete_orderbooks_by_seq_ids()` / `delete_trades_by_seq_ids()` - Delete specific archived rows
   - `count_orderbooks()` / `count_trades()` - For logging

2. **Rewritten `archive_all_data()` in `archive.rs`:**
   - Process each (exchange, symbol) group independently
   - Fetch batch of 10,000 rows → Archive to Parquet → Delete from DB → Repeat
   - Memory stays bounded regardless of total data volume

3. **Optimized `archive_group()` to avoid cloning:**
   - Takes ownership of `Vec<DbRow>` instead of borrowing
   - Single pass through rows to extract columns (no `.clone()` calls)

**Memory impact:**

| Scenario | Before | After |
|----------|--------|-------|
| 1M rows in DB | ~2-4 GB RAM | ~50-100 MB RAM |
| 10M rows in DB | ~20-40 GB (OOM) | ~50-100 MB RAM |

**Batch size:** Configurable via `ARCHIVE_BATCH_SIZE` constant (default: 10,000 rows)

**Behavior change:** Archives now delete rows incrementally as each batch is archived, rather than deleting all at once at the end. This is safer - if the process crashes mid-archive, only the successfully archived data is deleted.

---

### Added: Per-Market Data Timeout Configuration

**Files changed:** `src/config.rs`, `src/websocket.rs`, `src/main.rs`, `config.toml`

**Problem:** Low-volume trading pairs like XRP-USDT on OKX trigger unnecessary reconnects. The code reconnects if no actual market data (trades/orderbook updates) is received for 3x the `message_timeout_secs` (default: 6 minutes). Heartbeats and subscription confirmations don't reset this timer.

**Solution:** Added optional `data_timeout_secs` per market in `config.toml`:

```toml
[[markets]]
exchange = "okx"
symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
# Uses global websocket.message_timeout_secs (120s, reconnect after 6 min)

[[markets]]
exchange = "okx"
symbols = ["XRP-USDT"]
data_timeout_secs = 600  # 10 min timeout, reconnect after 30 min no data
```

**Changes:**
- `config.rs`: Added `data_timeout_secs` field to `MarketConfig` and `MarketPair`
- `websocket.rs`: Accept per-market timeout override, fall back to global config
- `main.rs`: Pass `data_timeout_secs` from market pair to websocket worker
- `config.toml`: Split OKX XRP-USDT into separate section with longer timeout

**Behavior:**
- If `data_timeout_secs` is set: uses that value for data timeout
- If not set: uses global `websocket.message_timeout_secs`
- Reconnection happens after 3x the timeout with no data

---

### Added: Configurable Binance WebSocket Endpoint for Geo-Restrictions

**Files changed:** `src/config.rs`, `src/exchanges/binance.rs`, `src/exchanges/mod.rs`, `src/main.rs`, `config.toml`

**Problem:** Binance blocks US IP addresses from `stream.binance.com` with HTTP 451 "Unavailable For Legal Reasons". AWS EC2 instances in US regions cannot connect to the default Binance WebSocket endpoint.

**Solution:** Added configurable `base_url` option per market in `config.toml`:

```toml
[[markets]]
exchange = "binance"
symbols = ["btcusdt", "ethusdt"]
# Override WebSocket endpoint for geo-restricted servers:
base_url = "wss://data-stream.binance.vision/ws"
```

**Available endpoints:**
| Endpoint | Use Case |
|----------|----------|
| `wss://stream.binance.com:9443/ws` | International (default, blocked from US) |
| `wss://data-stream.binance.vision/ws` | Market data only, may bypass geo-restrictions |
| `wss://stream.binance.us:9443/ws` | US endpoint (different trading pairs: USD not USDT) |

**Changes:**
- `config.rs`: Added `base_url` field to `MarketConfig` and `MarketPair`
- `binance.rs`: Added `base_url` field and `with_base_url()` constructor
- `mod.rs`: Added `ExchangeConfig` struct, updated `create_exchange()` signature
- `main.rs`: Pass per-market `base_url` to exchange factory
- `config.toml`: Added documented `base_url` option with explanation

**Usage for US servers:**
```toml
[[markets]]
exchange = "binance"
symbols = ["btcusdt", "ethusdt"]
base_url = "wss://data-stream.binance.vision/ws"
```

---

### Fixed: Docker Compose logs not visible on host

**Files changed:** `docker-compose.yml`

**Problem:** Logs written to `/app/logs/` inside the container were not accessible from the host machine. The `docker-compose.yml` mounted `./data` for database persistence but not the logs directory.

**Solution:** Added volume mount for logs:
```yaml
volumes:
  - ./logs:/app/logs
```

**Result:**
- JSON logs now appear in `./logs/` on the host
- Logs persist across container restarts
- `docker logs <container>` still shows pretty-printed stdout logs

---

### Fixed: Stale .env.example with Unused Variables

**Files changed:** `.env.example`

**Problem:** `.env.example` contained several variables that were either unused or misleading:
- `BINANCE_API_KEY` / `BINANCE_SECRET_KEY` - never read by the code
- `HOME_SERVER_NAME` - documented as env var but actually read from `config.toml`
- `MARKET_SYMBOL` - legacy variable, markets are now configured in `config.toml`

**Solution:** Cleaned up `.env.example` to only show variables that are actually read from environment:
- `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`, `AWS_REGION` - required for S3 storage
- `COINBASE_API_KEY`, `COINBASE_API_SECRET_FILE` - optional for Coinbase orderbook auth

**Note:** `home_server_name` is already documented in `config.toml` under `[aws]` section:
```toml
[aws]
region = "us-west-2"
bucket = "crypto-exchange-data-collector"
# Optional: identifier for this server (appears in S3 path)
# home_server_name = "prod-1"
```

When set, it adds a server identifier to the S3 key path:
- Without: `{exchange}/{symbol}/{data_type}/{date}/{timestamp}.parquet`
- With: `{exchange}/{symbol}/{data_type}/{server}/{date}/{timestamp}.parquet`

---

### Fixed: Binance Double Subscription Causing Duplicate Messages

**Files changed:** `src/exchanges/binance.rs`

**Problem:** Binance orderbook messages showed a constant 40 msg/sec in Grafana instead of the expected 20 msg/sec (2 symbols × 10 updates/sec at @100ms interval). The rate was exactly 2× expected.

**Root cause:** Double subscription to the same stream:

1. `websocket_url()` returned `wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms` which **auto-subscribes** via the URL path
2. `build_subscribe_messages()` then sent `{"method":"SUBSCRIBE","params":["btcusdt@depth20@100ms"]}` which subscribed **again**

Binance sent each orderbook update twice due to this redundant subscription.

**Solution:** Changed `websocket_url()` to return the base WebSocket endpoint:

```rust
// Before
fn websocket_url(&self, symbol: &str) -> String {
    format!(
        "wss://stream.binance.com:9443/ws/{}@depth{}@{}ms",
        symbol.to_lowercase(),
        self.depth_levels,
        self.update_speed_ms
    )
}

// After
fn websocket_url(&self, _symbol: &str) -> String {
    // Use base endpoint; subscriptions handled via build_subscribe_messages()
    "wss://stream.binance.com:9443/ws".to_string()
}
```

Now all subscriptions are handled exclusively via `build_subscribe_messages()`, eliminating the duplicate.

**Impact:** Binance orderbook msg/sec drops from ~40 to ~20 (correct rate). No more duplicate messages being stored.

---

## 2026-01-17

### Added: COINBASE_API_SECRET_FILE Environment Variable

**Files changed:** `src/config.rs`

**Problem:** The `dotenv` crate cannot parse multi-line PEM keys in `.env` files. The Coinbase API secret is an EC private key in PEM format which spans multiple lines, making it impossible to set via standard `.env` file syntax.

**Solution:** Support loading the Coinbase API secret from a file path via `COINBASE_API_SECRET_FILE` environment variable.

**Usage:**
```bash
# In .env file:
COINBASE_API_KEY=organizations/{org_id}/apiKeys/{key_id}
COINBASE_API_SECRET_FILE=./coinbase_key.pem
```

**Important:** The key must be converted from SEC1 to PKCS#8 format for compatibility with the `jsonwebtoken` crate (which uses `ring` internally):
```bash
openssl pkcs8 -topk8 -nocrypt -in cdp_api_key.pem -out coinbase_key.pem
```

**Changes:**
- `src/config.rs`: Added logic to check for `COINBASE_API_SECRET_FILE` first, read the file contents, then fall back to `COINBASE_API_SECRET` env var
- File contents are trimmed and line endings normalized
- Improved logging to show which loading method was used

**Behavior:**
1. If `COINBASE_API_SECRET_FILE` is set → read secret from file at that path
2. Else if `COINBASE_API_SECRET` is set → use that value directly (with `\n` escape replacement)
3. Else → no Coinbase authentication (only trades channel will work)

---

### Added: Coinbase Authenticated WebSocket Support

**Files changed:** `src/exchanges/coinbase.rs`, `src/exchanges/mod.rs`, `src/config.rs`, `src/main.rs`, `Cargo.toml`, `config.toml`

**Problem:** Coinbase's `level2` (orderbook) channel requires authentication since August 2023. The previous implementation could only collect trades.

**Solution:** Implemented JWT-based authentication for Coinbase Advanced Trade WebSocket API.

**Environment Variables:**
- `COINBASE_API_KEY` - API key in format `organizations/{org_id}/apiKeys/{key_id}`
- `COINBASE_API_SECRET` - EC private key in PEM format

**Changes:**

1. **`src/exchanges/coinbase.rs`:**
   - Switched to Advanced Trade WebSocket endpoint (`wss://advanced-trade-ws.coinbase.com`)
   - Added JWT generation for authenticated channels using ES256 algorithm
   - `level2` channel uses JWT authentication when credentials available
   - `market_trades` channel works with or without authentication
   - Added parsing for Advanced Trade API message format (`l2_data`, `market_trades` channels)
   - Maintained backwards compatibility with old Exchange API formats

2. **`src/exchanges/mod.rs`:**
   - Added `CoinbaseCredentials` struct to pass API credentials
   - Updated `create_exchange()` to accept Coinbase credentials

3. **`src/config.rs`:**
   - Added `coinbase_api_key` and `coinbase_api_secret` fields (loaded from env vars)

4. **`src/main.rs`:**
   - Pass Coinbase credentials to exchange creation
   - Log whether Coinbase authentication is available at startup

5. **`Cargo.toml`:**
   - Added `jsonwebtoken = "9"` for JWT generation

**Behavior:**
- With credentials: Both `level2` (orderbook) and `market_trades` work
- Without credentials: Only `market_trades` works; `level2` subscription is skipped with a warning

---

### Added: Per-Market Feed Configuration

**Files changed:** `src/config.rs`, `src/main.rs`, `config.toml`

**Problem:** Coinbase's `level2` (orderbook) channel requires authentication since August 2023. The alternative `level2_batch` channel only provides incremental updates, not periodic snapshots like Binance's `@depth20`. This made Coinbase orderbook collection non-functional without API credentials.

**Decision:** Skip Coinbase orderbook entirely. Only collect Coinbase trades. Implement per-market feed selection in config.

**Solution:** Added optional `feeds` field to market config in `config.toml`:

```toml
[[markets]]
exchange = "coinbase"
symbols = ["BTC-USD", "ETH-USD"]
feeds = ["trades"]  # Only trades, no orderbook

[[markets]]
exchange = "binance"
symbols = ["btcusdt", "ethusdt"]
# feeds defaults to ["orderbook", "trades"] if not specified
```

**Changes:**

1. **`src/config.rs`:**
   - Added `feeds` field to `MarketConfig` with default `["orderbook", "trades"]`
   - Added `feeds` field to `MarketPair` struct
   - Flattening logic now includes per-market feeds

2. **`src/main.rs`:**
   - Removed global feeds parsing
   - Worker spawning now uses per-market feeds
   - Logs configured feeds for each market pair

3. **`config.toml`:**
   - Coinbase configured with `feeds = ["trades"]` only
   - Binance uses default (both orderbook and trades)

**Impact:** Coinbase now works correctly (trades only). Each exchange can have its own feed configuration without affecting others.

---

### Fixed: Exchange Subscription Silent Failure (Coinbase, OKX, Upbit, Bybit)

**Files changed:** `src/exchanges/coinbase.rs`, `src/exchanges/okx.rs`, `src/exchanges/upbit.rs`, `src/exchanges/bybit.rs`, `src/websocket.rs`

**Problem:** Multiple exchanges silently failed to receive data - connections succeeded, no errors in logs, health endpoint showed healthy, but no data was written to the database. Affected exchanges: Coinbase, OKX, Upbit, Bybit.

**Root causes:**

1. **Wrong symbol format in subscriptions:** The `build_subscribe_messages()` function used `normalize_symbol()` which converts symbols to lowercase without separators (e.g., "BTC-USD" → "btcusd"). However, each exchange API expects symbols in their native format:

   | Exchange | Expected Format | Was Sending |
   |----------|----------------|-------------|
   | Coinbase | `BTC-USD` (uppercase, dash) | `btcusd` |
   | OKX | `BTC-USDT` (uppercase, dash) | `btcusdt` |
   | Upbit | `KRW-BTC` (uppercase, dash) | `krwbtc` |
   | Bybit | `BTCUSDT` (uppercase, no separator) | `btcusdt` |

   Exchanges silently ignored invalid symbol subscriptions.

2. **No warning for failed subscriptions:** The message timeout only triggered when NO messages were received. But exchanges like Coinbase send heartbeats regardless of subscription success, so the connection appeared healthy while no data flowed.

**Solution:**

1. **Use exchange-native symbol format in subscriptions:**
   ```rust
   // Coinbase, OKX, Upbit - uppercase with original format
   let api_symbol = symbol.to_uppercase();

   // Bybit - uppercase without separators
   let api_symbol = symbol.to_uppercase().replace(['-', '_', '/'], "");
   ```
   Note: `normalize_symbol()` is now only used for storage/logging, not API calls.

2. **Added data timeout detection:**
   - Track `last_data_received` separately from message timeout
   - Only reset when receiving actual data (Orderbook/Trade), not control messages (heartbeats)
   - Warn if no data for `message_timeout_secs`
   - Reconnect if no data for 3x timeout

**New log messages:**
```
WARN No data received - subscription may have failed  exchange=coinbase symbol=btcusd feed=orderbook elapsed_secs=30
ERROR Extended data timeout - reconnecting  exchange=coinbase symbol=btcusd feed=orderbook elapsed_secs=90
```

**Impact:** Fixes silent data loss on Coinbase, OKX, Upbit, and Bybit. Provides early warning when subscriptions fail.

---

### Fixed: Binance Orderbook Symbol Shows "unknown" in Database

**Files changed:** `src/websocket.rs`

**Problem:** For Binance orderbook data, the `symbol` field in the database was stored as "unknown", while trade data had the correct symbol. This was specific to Binance but could affect other exchanges with similar message formats.

**Root cause:** Binance's partial depth stream (`@depth20@100ms`) doesn't include the symbol in the message payload - it only contains `lastUpdateId`, `E` (event time), `bids`, and `asks`. The code tried to extract the symbol from the `"s"` field which doesn't exist for orderbook updates:

```rust
let symbol = json
    .get("s")
    .and_then(|v| v.as_str())
    .unwrap_or("unknown")  // <- always "unknown" for orderbooks
    .to_string();
```

Trade messages include `"s": "BTCUSDT"` in the payload, so they parsed correctly.

**Solution:** In `websocket.rs`, changed `save_event` calls to use `&normalized_symbol` (the symbol we subscribed to) instead of the symbol extracted from the message (`&sym`). This is reliable because each WebSocket worker handles a single symbol configured at startup.

```rust
// Before
save_event(&db_tx, exchange_name, &sym, DataType::Orderbook, ...);

// After
save_event(&db_tx, exchange_name, &normalized_symbol, DataType::Orderbook, ...);
```

**Impact:** Both orderbook and trade data now correctly store the symbol in the database.

---

### Improved: Consistent Symbol Normalization Across All Exchanges

**Files changed:** `src/exchanges/mod.rs`, `src/exchanges/binance.rs`, `src/exchanges/coinbase.rs`, `src/exchanges/okx.rs`, `src/exchanges/bybit.rs`, `src/exchanges/upbit.rs`

**Problem:** Each exchange's `normalize_symbol` function returned symbols in different formats:
- Binance: `btcusdt` (lowercase, no separator)
- Coinbase: `BTC-USD` (uppercase, hyphen)
- OKX: `BTC-USDT` (uppercase, hyphen)
- Bybit: `BTCUSDT` (uppercase, no separator)
- Upbit: `KRW-BTC` (uppercase, hyphen, quote-base order)

This inconsistency made it harder to query data across exchanges and created confusing logs/metrics.

**Solution:** Standardized all `normalize_symbol` implementations to return lowercase without separators:

```rust
fn normalize_symbol(&self, symbol: &str) -> String {
    symbol.to_lowercase().replace(['-', '_', '/'], "")
}
```

**Examples:**
| Input | Before (varied) | After (consistent) |
|-------|-----------------|-------------------|
| `BTC-USD` | `BTC-USD` (Coinbase) | `btcusd` |
| `BTC-USDT` | `BTC-USDT` (OKX) | `btcusdt` |
| `BTCUSDT` | `BTCUSDT` (Bybit) | `btcusdt` |
| `KRW-BTC` | `KRW-BTC` (Upbit) | `krwbtc` |

**Note:** This only affects storage, logging, and metrics. Each exchange's API calls (`websocket_url`, `build_subscribe_messages`) still use the exchange's native format internally.

**Impact:** Consistent symbol format across all exchanges for easier cross-exchange queries and cleaner logs/metrics.

---

### Improved: Separate WebSocket Connections Per Feed Type

**Files changed:** `src/main.rs`, `src/websocket.rs`, `src/exchanges/mod.rs`

**Problem:** Previously, each WebSocket worker handled multiple feed types (orderbook + trades) on a single connection. This created a silent failure scenario:

```
┌─────────────────────────────────────────────────┐
│  Single WebSocket Connection (ETH-USD)          │
│                                                 │
│  ┌─────────────┐    ┌─────────────┐            │
│  │ Orderbook   │    │  Trades     │            │
│  │ (100ms)     │    │  (stopped)  │  ← silent  │
│  │ ✓ ✓ ✓ ✓ ✓   │    │  ✗          │    failure │
│  └─────────────┘    └─────────────┘            │
│                                                 │
│  Timeout: Never triggers (orderbook flowing)    │
└─────────────────────────────────────────────────┘
```

If orderbook messages kept flowing but trades silently stopped, the 30-second timeout would never fire (connection appears healthy), resulting in silent data loss.

**Solution:** Spawn one WebSocket worker per (exchange, symbol, feed) combination instead of per (exchange, symbol):

1. **Isolated failure domains:** Each feed has its own connection and timeout monitoring
2. **Independent reconnection:** If trades feed fails, only trades reconnect; orderbook continues uninterrupted
3. **Better observability:** Connection state now tracked per feed (`binance:btcusdt:orderbook`, `binance:btcusdt:trades`)

**Changes:**
- `main.rs`: Worker spawning loop now iterates over both market pairs AND feeds
- `websocket.rs`: Connection state key includes feed type; logging includes feed in all messages
- `exchanges/mod.rs`: Added `FeedType::as_str()` helper method
- Channel size calculation: Now `1000 * market_pairs * feeds` (was `1000 * market_pairs`)

**Example log output (before):**
```
Connected to WebSocket exchange="coinbase" symbol=ETH-USD
```

**Example log output (after):**
```
Connected to WebSocket exchange="coinbase" symbol=ETH-USD feed="orderbook"
Connected to WebSocket exchange="coinbase" symbol=ETH-USD feed="trades"
```

**Trade-offs:**
- 2x WebSocket connections per symbol (one per feed type)
- Slightly higher memory usage
- Some exchanges may rate-limit connections (not an issue for current supported exchanges)

**Impact:** Eliminates silent partial feed failures. Each feed is independently monitored and will reconnect if it goes stale, ensuring no silent data loss.

---

### Refactored: Multi-Exchange Architecture + Data Model Improvements

**Files changed:** `Cargo.toml`, `src/config.rs`, `src/models.rs`, `src/db.rs`, `src/websocket.rs`, `src/archive.rs`, `src/main.rs`, `config.toml` (new)

**Purpose:** Two major architectural improvements:
1. Multi-exchange/multi-symbol support in a single process
2. Better data models with type-safe enums and separate database tables

#### Part 1: Multi-Exchange/Multi-Symbol Support

**Problem:** Previously required running 1 process per exchange+symbol combination. Managing 10+ instances was operationally complex.

**Solution:** TOML configuration file with multi-worker spawning:

```toml
[[markets]]
exchange = "binance"
symbols = ["btcusdt", "ethusdt", "solusdt"]

[[markets]]
exchange = "coinbase"
symbols = ["BTC-USD", "ETH-USD"]

[[markets]]
exchange = "bybit"
symbols = ["BTCUSDT"]
```

**Changes:**
- Added `toml = "0.8"` dependency
- Rewrote `src/config.rs` for TOML parsing with `MarketPair`, `MarketConfig` structs
- `main.rs` spawns one WebSocket worker per market pair, all sharing a single DB channel
- Archive scheduler now archives ALL data (no exchange/symbol parameters)
- Channel sized for all market pairs: `1000 * market_pairs.len()`

**Configuration:**
- Config file path: `config.toml` (or set `CONFIG_PATH` env var)
- AWS credentials still from env vars: `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`

#### Part 2: Data Model Refactor

**Problem:** `SnapshotData` was misleadingly named (originally for Binance orderbook snapshots). String-based `data_type` field was error-prone. Single `snapshots` table mixed orderbook and trade data.

**Solution:**

1. **Renamed `SnapshotData` → `MarketEvent`** - Accurate name for all market data types

2. **Added type-safe `DataType` enum:**
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
   pub enum DataType {
       Orderbook,
       Trade,
   }
   ```

3. **Separate database tables:**
   ```sql
   CREATE TABLE orderbooks (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       exchange TEXT NOT NULL,
       symbol TEXT NOT NULL,
       exchange_sequence_id TEXT NOT NULL,
       timestamp_collector INTEGER NOT NULL,
       timestamp_exchange INTEGER NOT NULL,
       data TEXT NOT NULL,
       UNIQUE(exchange, symbol, exchange_sequence_id)
   );

   CREATE TABLE trades (
       -- Same schema as orderbooks
   );
   ```

4. **Updated S3 structure:**
   ```
   {exchange}/{symbol}/{orderbook|trade}/[{server}/]{date}/{timestamp}.parquet
   ```

**Files updated:**
- `src/models.rs` - Added `DataType` enum, renamed struct
- `src/db.rs` - Split into two tables, separate fetch/delete functions
- `src/websocket.rs` - Uses `DataType` enum, renamed `save_snapshot` → `save_event`
- `src/archive.rs` - Groups data by (exchange, symbol, data_type), creates separate Parquet files

#### Breaking Changes

1. **Config format:** Requires `config.toml` file (env vars no longer supported for market config)
2. **Database schema:** Delete existing `.db` file before running (new table structure)
3. **S3 structure:** New path includes data type: `{exchange}/{symbol}/{orderbook|trade}/...`

#### Verification

```bash
cargo build   # Compiles with warnings (unused code only)
```

---

### Added: Quant Expert Review Round 2 Fixes

**Files changed:** `src/models.rs`, `src/metrics.rs`, `src/websocket.rs`, `src/db.rs`, `src/archive.rs`, `src/http.rs`, `src/config.rs`, `src/main.rs`, `src/exchanges/mod.rs`, `src/exchanges/binance.rs`, `src/exchanges/coinbase.rs`, `src/exchanges/upbit.rs`, `src/exchanges/okx.rs`, `src/exchanges/bybit.rs`

**Purpose:** Implement 11 fixes from second quant expert review (3 HIGH, 7 MEDIUM, 1 LOW priority).

#### HIGH Priority Fixes

**1. Backwards Sequence Detection**

Problem: `SequenceTracker::check()` only detected forward gaps. Out-of-order (seq < last) and duplicate (seq == last) messages went undetected.

Solution:
- Added `SequenceCheckResult` enum with `Ok`, `Gap`, `OutOfOrder`, `Duplicate` variants
- Out-of-order messages don't update `last_seen` (preserves gap detection)
- New metrics: `collector_sequence_out_of_order_total`, `collector_sequence_duplicates_total`

**2. Timestamp Precision Mismatch**

Problem: `timestamp_collector` stored in microseconds, `timestamp_exchange` in milliseconds. Latency calculated with separate `now_millis()` call.

Solution:
- All timestamps now in **microseconds** (μs)
- Exchange parsers convert ms → μs at parse time (`* 1000`)
- Same `collector_time_us` used for storage AND latency calculation
- Renamed field to `timestamp_exchange_us` in `ExchangeMessage` enum

**3. Unbounded Memory in SequenceTracker**

Problem: `HashMap<String, i64>` grows forever. Watching many symbols = memory leak.

Solution:
- Added LRU-style eviction with `VecDeque` for insertion order
- Default max 10,000 entries (configurable via `with_max_entries()`)
- Oldest keys evicted when capacity reached

#### MEDIUM Priority Fixes

**4. Channel Backpressure in Health Check**

Problem: `/health` didn't reflect dropped messages.

Solution:
- Added `messages_dropped` field to health response
- Status changes to `"degraded"` with `degraded_reason: "backpressure"` when drops > 0
- Also shows `"degraded"` with `degraded_reason: "partial_connections"` for partial WS failures

**5. Archive Duplicate Prevention**

Problem: If S3 verify failed, next cycle uploaded same data with different timestamp = duplicates.

Solution:
- Track `LAST_ARCHIVE_MAX_TIMESTAMP` (AtomicI64)
- Skip archive if current batch max timestamp <= last archived
- Only update after successful verification AND DB delete

**6. Config Parameter Validation**

Problem: Conflicting config values caused mysterious failures.

Solution: Added `validate()` method that panics with helpful messages:
- `WS_MESSAGE_TIMEOUT_SECS >= BATCH_INTERVAL` (avoid false timeouts)
- `ARCHIVE_INTERVAL_SECS` between 60-86400 seconds
- `WS_INITIAL_RETRY_DELAY_SECS < WS_MESSAGE_TIMEOUT_SECS`
- `WS_MAX_RETRY_DELAY_SECS >= WS_INITIAL_RETRY_DELAY_SECS`

**7. Floating-Point Timestamp Precision**

Problem: `as_secs_f64()` loses precision for large timestamps.

Solution: Use integer seconds (`as_secs() as f64`) for metrics where sub-second precision isn't needed:
- `LAST_MESSAGE_TIMESTAMP` gauge
- `APP_START_TIMESTAMP` gauge
- Health endpoint uptime calculation

**8. Parse Error Circuit Breaker**

Problem: Format changes caused millions of warnings but collector kept running with garbage data.

Solution:
- Added `ParseErrorTracker` with sliding 60-second window
- Trips at 50% error rate (min 100 samples)
- On trip: logs error, increments `collector_parse_circuit_breaks_total`, triggers reconnect
- Window resets after duration expires

**9. DB Error Categorization**

Problem: Couldn't distinguish normal duplicate rejections from actual DB errors.

Solution:
- `DB_INSERT_ERRORS` changed from Counter to CounterVec with `error_type` label
- Categories: `"duplicate"`, `"constraint"`, `"io"`, `"other"`
- Added `categorize_sqlx_error()` function
- Only logs non-duplicate errors (duplicates are expected on reconnect)

**10. Per-Worker Shutdown Timeouts**

Problem: One stuck worker blocked all others from clean shutdown.

Solution: Individual timeouts with `tokio::time::timeout`:
- WebSocket: 10 seconds
- DB worker: 15 seconds (more time to flush)
- Archive: 5 seconds (lowest priority)

#### LOW Priority Fix

**11. Jitter in Exponential Backoff**

Problem: All reconnects at exact intervals (1s, 2s, 4s) = thundering herd.

Solution:
- Added ±25% random jitter using `rand::rng().random_range(0.75..1.25)`
- Applied to `ExponentialBackoff::next_delay()`

#### New Prometheus Metrics

```
collector_sequence_out_of_order_total{exchange,symbol,data_type}
collector_sequence_duplicates_total{exchange,symbol,data_type}
collector_parse_circuit_breaks_total{exchange}
collector_db_insert_errors_total{error_type}  # was Counter, now CounterVec
```

#### Verification

```bash
cargo build   # Compiles with warnings (unused code only)
cargo test    # 16 tests pass, 5 ignored (live smoke tests)
```

**Breaking changes:**
- `timestamp_exchange` column now stores microseconds (was milliseconds)
- Delete existing `.db` file before running

---

### Added: Production Hardening for Quant Systems

**Files changed:** `src/models.rs`, `src/db.rs`, `src/websocket.rs`, `src/archive.rs`, `src/main.rs`, `src/metrics.rs`, `src/exchanges/mod.rs`, `src/exchanges/binance.rs`, `src/exchanges/coinbase.rs`, `src/exchanges/upbit.rs`, `src/exchanges/okx.rs`, `src/exchanges/bybit.rs`

**Purpose:** Implement 6 critical production improvements identified in a quant expert review for time-series data integrity and operational reliability.

#### 1. Exchange Timestamps Added

**Problem:** Only stored collector receipt time. Couldn't measure network latency or do proper time-series analysis.

**Solution:**
- Added `timestamp_exchange` field (milliseconds) to `SnapshotData` and `ExchangeMessage`
- Renamed `timestamp` to `timestamp_collector` (microseconds, our receipt time)
- Updated database schema with both timestamp columns
- Each exchange now extracts its native timestamp:
  - **Binance:** `E` field (event time)
  - **Coinbase:** `time` field (ISO8601 parsed)
  - **Upbit:** `tms` (orderbook) / `ttms` (trades)
  - **OKX:** `ts` field in data array
  - **Bybit:** `ts` (message level) / `T` (trade level)

#### 2. Sequence Gap Detection

**Problem:** No tracking of missing messages in data streams.

**Solution:**
- Added `SequenceTracker` struct that tracks last seen sequence ID per `exchange:symbol:data_type`
- Detects gaps when sequence jumps (e.g., received 105 after 100 = gap of 4)
- Only works for numeric sequences; non-numeric IDs are skipped
- Logs warnings with gap details (expected, received, gap_size)

**New Prometheus metrics:**
- `collector_sequence_gaps_total{exchange,symbol,data_type}` - count of gaps
- `collector_sequence_gap_size{exchange,symbol,data_type}` - histogram of gap sizes

#### 3. Latency Metrics

**Problem:** No visibility into exchange-to-collector latency.

**Solution:**
- Records `collector_time_ms - timestamp_exchange` for each message
- Only records positive latencies (clock skew can cause negative)

**New Prometheus metric:**
- `collector_latency_exchange_to_collector_ms{exchange,symbol,data_type}` - histogram with buckets 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000ms

#### 4. Graceful Shutdown

**Problem:** No cleanup on SIGTERM/SIGINT. In-flight messages lost, connections not closed cleanly.

**Solution:**
- Added `tokio::sync::broadcast` channel for shutdown coordination
- All workers (WebSocket, DB, Archive) receive shutdown signal
- Workers flush data and close connections cleanly
- 30-second timeout for worker shutdown
- Main function waits for `tokio::signal::ctrl_c()`

**Shutdown behavior:**
- DB worker flushes any buffered batch before stopping
- WebSocket worker closes connection cleanly with `write.close()`
- Archive scheduler completes current cycle if running, then stops

#### 5. Non-Blocking Archive with Timeout

**Problem:** Archive scheduler blocked the main task during S3 uploads.

**Solution:**
- Archive scheduler now runs as a spawned task (non-blocking)
- Each archive cycle has a 5-minute timeout
- Timeout failure increments `collector_archive_failures_total{stage="timeout"}`
- Sleep between cycles respects shutdown signal

#### 6. Atomic Archive Verification

**Problem:** Data could be deleted after S3 upload even if verification failed.

**Solution:**
- After upload, issues HEAD request to verify object exists
- **NEW:** Compares local file size with remote `content_length`
- Only deletes local data if sizes match exactly
- Size mismatch increments `collector_archive_failures_total{stage="verify_size"}`

**New Prometheus metric:**
- `collector_archive_failures_total{stage}` - failures by stage (fetch, parquet, upload, verify_size, verify_head, timeout)

#### Verification

```bash
cargo build                   # Compiles with 13 warnings (all unused code)
cargo test                    # 16 tests pass
cargo test -- --ignored       # Live smoke tests pass
```

**Parquet schema change:** Column renamed from `timestamp` to `timestamp_collector`, new column `timestamp_exchange` added.

**Database schema change:** Migration required - delete existing `.db` file before running.

---

### Added: Live WebSocket Smoke Tests

**Files changed:** `src/exchanges/mod.rs`, `src/exchanges/upbit.rs`

**Purpose:** Validate that exchange WebSocket connections and message parsing work with real API data.

**Implementation:**
- Added `run_smoke_test()` helper in `src/exchanges/mod.rs`
- 5 smoke tests (one per exchange) marked with `#[ignore]`
- Tests connect, subscribe to orderbook+trades, receive 10 messages, verify parsing

**Usage:**
```bash
cargo test                            # 16 unit tests (smoke tests skipped)
cargo test -- --ignored               # Run all 5 smoke tests
cargo test live_binance -- --ignored  # Run single exchange test
```

**Fixes included:**
- Upbit: Changed subscription format from "DEFAULT" to "SIMPLE" (binary → JSON text)
- Smoke test handles both text and binary WebSocket frames (Upbit sends binary)

**Verified working:** Binance, Coinbase, Upbit, OKX, Bybit

---

### Improved: S3 Bucket Organization with Hierarchical Prefixes

**Files changed:** `src/config.rs`, `src/archive.rs`, `src/main.rs`

**Problem:** With multiple exchanges and symbols, the previous approach created one S3 bucket per exchange-symbol combination (e.g., `binance-spot-btcusdt`, `coinbase-spot-btc-usd`). This becomes unmanageable with many symbols and complicates IAM policies, lifecycle rules, and cross-symbol analysis.

**Solution:** Single bucket with hierarchical S3 key prefixes:

1. **New `BUCKET_NAME` env var:**
   ```bash
   BUCKET_NAME=crypto-exchange-data-collector  # Single bucket for all data
   ```

2. **Hierarchical S3 key structure:**
   ```
   # With HOME_SERVER_NAME (for redundant server setups):
   {exchange}/{symbol}/{server}/{YYYY-MM-DD}/{timestamp}.parquet

   # Without HOME_SERVER_NAME (default for single-server users):
   {exchange}/{symbol}/{YYYY-MM-DD}/{timestamp}.parquet
   ```

3. **Example S3 structure:**
   ```
   crypto-exchange-data-collector/
   ├── binance/
   │   └── btcusdt/
   │       └── server1/
   │           └── 2026-01-17/
   │               └── 1737100800000.parquet
   └── coinbase/
       └── btc-usd/
           └── 2026-01-17/
               └── 1737100800000.parquet
   ```

4. **Server level is optional:**
   - If `HOME_SERVER_NAME` is set: includes server in path (for multi-server redundancy)
   - If not set: omits server level (simpler for single-server deployments)

**Benefits:**
- Single bucket to manage with one IAM policy
- One lifecycle rule applies to all data
- Easy cross-exchange queries with S3 Select or Athena
- Navigable folder structure in AWS Console
- Backward compatible (just set `BUCKET_NAME`)

**Migration:**
1. Create bucket: `aws s3 mb s3://crypto-exchange-data-collector`
2. Set env var: `BUCKET_NAME=crypto-exchange-data-collector`
3. Deploy new code

---

### Refactored: WebSocket Worker to Use Exchange Trait

**Files changed:** `src/websocket.rs`, `src/config.rs`, `src/main.rs`

**Problem:** The websocket worker was hardcoded for Binance, making it impossible to use the Exchange trait implementations created earlier.

**Solution:** Refactored the websocket worker to accept `Box<dyn Exchange>` and use the trait methods:

1. **Updated `websocket_worker` signature:**
   ```rust
   pub async fn websocket_worker(
       exchange: Box<dyn Exchange>,
       db_tx: Sender<SnapshotData>,
       symbol: String,
       feeds: Vec<FeedType>,
       ws_config: WsConfig,
       conn_state: ConnectionState,
   )
   ```

2. **Generic message handling:**
   ```rust
   let url = exchange.websocket_url(&symbol);
   let subscribe_msgs = exchange.build_subscribe_messages(&symbol, &feeds);

   match exchange.parse_message(&text) {
       Ok(ExchangeMessage::Orderbook { symbol, sequence_id, data }) => { ... }
       Ok(ExchangeMessage::Trade { symbol, sequence_id, data }) => { ... }
       Ok(ExchangeMessage::Ping(data)) => { write.send(Message::Pong(data.into())).await; }
       // ...
   }
   ```

3. **New configuration options in `config.rs`:**
   ```bash
   EXCHANGE=binance        # Exchange to connect to (default: binance)
   FEEDS=orderbook,trades  # Comma-separated feed types (default: orderbook)
   ```

4. **Dynamic path naming:**
   - Database: `snapshots-{exchange}-spot-{symbol}.db`
   - Archive: `archive-{exchange}-{symbol}/`
   - S3 bucket: `{exchange}-spot-{symbol}`

5. **Exchange factory in `main.rs`:**
   ```rust
   let exchange = create_exchange(&config.exchange)
       .expect("Unknown exchange");
   let feeds = parse_feeds(&config.feeds);
   ```

**Supported exchanges:** binance, coinbase, upbit, okx, bybit

**Impact:** The system can now connect to any of the 5 implemented exchanges by setting `EXCHANGE=<name>`. Each exchange uses its own symbol normalization, subscription format, and message parsing.

---

### Added: Multi-Exchange Architecture with Exchange Trait

**Files created:**
- `src/exchanges/mod.rs` - Exchange trait and common types
- `src/exchanges/README.md` - Comprehensive API documentation
- `src/exchanges/binance.rs` - Binance implementation
- `src/exchanges/coinbase.rs` - Coinbase implementation
- `src/exchanges/upbit.rs` - Upbit implementation
- `src/exchanges/okx.rs` - OKX implementation
- `src/exchanges/bybit.rs` - Bybit implementation

**Architecture:**

Implemented a trait-based abstraction for multi-exchange support:

```rust
pub trait Exchange: Send + Sync {
    fn name(&self) -> &'static str;
    fn websocket_url(&self, symbol: &str) -> String;
    fn build_subscribe_messages(&self, symbol: &str, feeds: &[FeedType]) -> Vec<String>;
    fn parse_message(&self, msg: &str) -> Result<ExchangeMessage, ExchangeError>;
    fn normalize_symbol(&self, symbol: &str) -> String;
}
```

**Supported Exchanges:**

| Exchange | Orderbook | Trades | Symbol Format |
|----------|-----------|--------|---------------|
| Binance | `@depth20@100ms` | `@trade` | `btcusdt` |
| Coinbase | `level2` / `level2_batch` | `matches` | `BTC-USD` |
| Upbit | `orderbook` | `trade` | `KRW-BTC` |
| OKX | `books` / `books5` | `trades` | `BTC-USDT` |
| Bybit | `orderbook.50` | `publicTrade` | `BTCUSDT` |

**Key Features:**
- Unified `ExchangeMessage` enum for all exchanges (Orderbook, Trade, Ping, Pong, Other)
- Symbol normalization per exchange
- Configurable depth levels and update speeds
- Factory function `create_exchange(name)` for runtime selection
- Comprehensive unit tests for message parsing

**Documentation:**
`src/exchanges/README.md` contains complete WebSocket API reference for all exchanges including:
- Connection URLs
- Subscription formats
- Message schemas
- Sequence ID fields
- Rate limits

**Dependencies added:** `uuid = "1.11"` (for Upbit ticket generation)

**Impact:** Foundation for multi-exchange data collection. The websocket worker can now be generalized to use `Box<dyn Exchange>` instead of Binance-specific code.

---

### Improved: Code Review Polish Items (4 Minor Improvements)

Based on a code review that found the module structure well-organized, these 4 optional polish items were identified and implemented:

#### 1. Configurable WebSocket Constants

**Files changed:** `src/config.rs`, `src/websocket.rs`

**Problem:** WebSocket connection parameters were hardcoded constants:
```rust
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
```

**Solution:** Moved to `Config` struct with environment variables:
- `WS_MESSAGE_TIMEOUT_SECS` (default: 30)
- `WS_INITIAL_RETRY_DELAY_SECS` (default: 1)
- `WS_MAX_RETRY_DELAY_SECS` (default: 60)

**Impact:** Operators can tune connection parameters without recompiling.

#### 2. Archive Runs Immediately on Startup

**Files changed:** `src/archive.rs`

**Problem:** The archive loop slept first, so first archive happened 1 hour after startup:
```rust
loop {
    sleep(Duration::from_secs(3600)).await;  // Sleep first
    archive_snapshots(...).await;
}
```

**Solution:** Reordered to archive first, then sleep:
```rust
loop {
    archive_snapshots(...).await;  // Archive first
    sleep(Duration::from_secs(archive_interval_secs)).await;
}
```

Also made interval configurable via `ARCHIVE_INTERVAL_SECS` (default: 3600).

**Impact:** Data is archived immediately on startup, useful after restarts.

#### 3. Health Handler Uses Shared Connection State

**Files changed:** `src/http.rs`, `src/websocket.rs`, `src/models.rs`

**Problem:** Health handler iterated through all Prometheus metrics to find connection status:
```rust
for family in &metric_families {
    if family.get_name() == "collector_websocket_connected" { ... }
}
```

**Solution:** Added `ConnectionState` (`Arc<RwLock<HashMap<String, bool>>>`) shared between WebSocket workers and HTTP handler:
```rust
// In websocket.rs - update on connect/disconnect
conn_state.write().await.insert(key, connected);

// In http.rs - read for health check
let state = conn_state.read().await;
let connections_up = state.values().filter(|&&v| v).count();
```

**Impact:** O(1) health check instead of O(n) metric iteration.

#### 4. Simplified Parquet Construction with itertools

**Files changed:** `Cargo.toml`, `src/archive.rs`

**Problem:** Verbose manual tuple extraction (17 lines):
```rust
let mut exchanges = Vec::with_capacity(snapshot_count);
let mut symbols = Vec::with_capacity(snapshot_count);
// ... 4 more vectors
for (ex, sym, dt, seq, ts, d) in snapshots {
    exchanges.push(ex);
    symbols.push(sym);
    // ... 4 more pushes
}
```

**Solution:** Using `itertools::multiunzip` (3 lines):
```rust
let (exchanges, symbols, data_types, seq_ids, timestamps, data_col): (
    Vec<String>, Vec<String>, Vec<String>, Vec<String>, Vec<i64>, Vec<String>,
) = multiunzip(snapshots);
```

**Impact:** Cleaner, more idiomatic Rust. Added `itertools = "0.14"` dependency.

---

### Refactored: Moved Functions to Appropriate Modules

**Files changed:** `src/main.rs`, `src/archive.rs`, `src/http.rs`

**Problem:** `main.rs` contained utility functions that belonged in domain-specific modules:
- `create_s3_client()` - S3 client creation
- `run_liveness_probe()` - periodic health logging

**Solution:** Moved functions to their logical modules:

1. **`create_s3_client()` → `archive.rs`**
   - S3 client is only used by archive operations
   - Keeps all S3-related code in one module
   - `archive.rs` already imports aws_sdk_s3 types

2. **`run_liveness_probe()` → `http.rs`**
   - Liveness is a health/observability concern
   - Groups with existing `/health` and `/ready` endpoints

3. **`init_tracing()` stays in `main.rs`**
   - Initialization code that runs once at startup
   - Common Rust pattern to keep init functions in main

**Impact:** `main.rs` is now purely orchestration (~100 lines). Each module owns its complete domain.

---

### Added: WebSocket Connection Health Monitoring

**Files changed:** `src/websocket.rs`

**Problem:** The WebSocket implementation could detect explicit connection failures but couldn't detect "silent failures" where:
- The TCP connection stays open but the server stops sending data
- Network issues don't immediately close the socket
- The connection appears alive but is actually stale

With 100ms message frequency from Binance, if no message arrives for 30+ seconds, something is wrong.

**Solution:** Implemented application-level timeout with exponential backoff:

1. **Message timeout detection:**
   ```rust
   const MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);

   match timeout(MESSAGE_TIMEOUT, read.next()).await {
       Ok(Some(Ok(msg))) => { /* process */ }
       Ok(Some(Err(e))) => break,  // WebSocket error
       Ok(None) => break,          // Stream ended
       Err(_) => break,            // TIMEOUT - reconnect
   }
   ```

2. **Exponential backoff for reconnection:**
   ```rust
   const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
   const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

   struct ExponentialBackoff {
       current_delay: Duration,
   }

   impl ExponentialBackoff {
       fn next_delay(&mut self) -> Duration {
           let delay = self.current_delay;
           self.current_delay = (self.current_delay * 2).min(MAX_RETRY_DELAY);
           delay
       }

       fn reset(&mut self) {
           self.current_delay = INITIAL_RETRY_DELAY;
       }
   }
   ```

3. **Backoff behavior:**
   - Resets to 1 second after successful connection
   - Doubles after each failure: 1s → 2s → 4s → 8s → ... → 60s (max)
   - Prevents hammering the server during extended outages

**Benefits:**
- Detects stale connections within 30 seconds
- Zero runtime overhead (tokio timeout is essentially free)
- Self-contained, no external dependencies
- Prevents rapid reconnection loops during outages

**Trade-offs:**
- 30-second timeout is tuned for Binance's 100ms message frequency
- May need adjustment for lower-frequency data sources

**Impact:** System now automatically recovers from silent connection failures that would previously cause indefinite data loss.

---

### Added: Prometheus Metrics and HTTP Health Endpoints

**Files changed:** `Cargo.toml`, `src/metrics.rs` (new), `src/http.rs` (new), `src/config.rs`, `src/main.rs`, `src/websocket.rs`, `src/db.rs`, `src/archive.rs`, `src/utils.rs`

**Problem:** The system had only stdout/file logging visible in Portainer. For a multi-exchange data collector, operators need:
- Real-time visibility into per-exchange connection health
- Message throughput and drop rates
- Database write latency histograms
- S3 upload success/failure tracking
- HTTP health endpoint for Docker/K8s probes

**Solution:** Implemented Prometheus metrics with axum HTTP server:

1. **New dependencies:**
   ```toml
   prometheus = "0.13"
   lazy_static = "1.5"
   axum = "0.8"
   ```

2. **Metrics exposed (`/metrics` endpoint on port 9090):**

   **WebSocket metrics:**
   - `collector_websocket_connected{exchange,symbol}` - connection status gauge
   - `collector_websocket_reconnects_total{exchange,symbol}` - reconnection counter
   - `collector_last_message_timestamp_seconds{exchange,symbol}` - staleness detection
   - `collector_messages_received_total{exchange,symbol,data_type}` - throughput counter
   - `collector_messages_dropped_total` - backpressure indicator
   - `collector_message_timeouts_total{exchange,symbol}` - stale connection events

   **Database metrics:**
   - `collector_channel_queue_depth` - batch buffer size
   - `collector_db_write_seconds{operation}` - write latency histogram
   - `collector_db_snapshots_written_total` - successful writes
   - `collector_db_insert_errors_total` - insert failures

   **Archive metrics:**
   - `collector_archives_completed_total` - successful archive cycles
   - `collector_snapshots_archived_total` - records archived to S3
   - `collector_s3_upload_seconds{status}` - upload latency histogram
   - `collector_s3_upload_failures_total` - upload failures
   - `collector_s3_upload_retries_total` - retry attempts

   **Application metrics:**
   - `collector_start_timestamp_seconds` - uptime calculation

3. **HTTP endpoints:**
   - `GET /metrics` - Prometheus scrape endpoint
   - `GET /health` - JSON health status with connection states
   - `GET /ready` - Simple readiness probe

4. **Health endpoint response:**
   ```json
   {
     "status": "healthy",
     "uptime_seconds": 3847,
     "connections": {"up": 1, "total": 1},
     "messages_dropped": 0
   }
   ```

5. **Configuration:**
   ```bash
   METRICS_PORT=9090  # Default port for HTTP server
   ```

**Grafana integration:**
Add Prometheus as a data source, then create dashboards with:
- Messages/sec by exchange (stacked graph)
- Connection status (up/down indicator)
- Last message age (staleness alert)
- DB write latency percentiles (p50, p95, p99)
- S3 upload success rate

**Docker Compose addition:**
```yaml
prometheus:
  image: prom/prometheus:latest
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml
  ports:
    - "9091:9090"

grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"
```

**Benefits:**
- Real-time per-exchange visibility
- Latency histograms for performance analysis
- Health endpoint for container orchestration
- Industry-standard Prometheus format
- Zero-overhead when not scraped

**Impact:** Production-ready observability. Operators can now monitor all exchanges in Grafana, set alerts for connection issues or high drop rates, and integrate with existing monitoring infrastructure.

---

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
