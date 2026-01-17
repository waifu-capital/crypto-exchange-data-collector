# Changelog

## 2026-01-17

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
   BUCKET_NAME=crypto-market-data  # Single bucket for all data
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
   crypto-market-data/
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
1. Create bucket: `aws s3 mb s3://crypto-market-data`
2. Set env var: `BUCKET_NAME=crypto-market-data`
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
