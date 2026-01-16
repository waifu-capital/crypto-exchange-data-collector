# Data Collection Architecture Improvements - Phase 2

This document outlines additional shortcomings identified after the Phase 1 improvements and recommended solutions.

**Phase 1 (Completed):** WAL mode, sqlx migration, backpressure handling, microsecond timestamps, multi-exchange schema, error handling, S3 retry, dynamic region.

---

## Current Architecture Overview

```
WebSocket -> Channel (1000 buffer) -> SQLite (batched writes, 5s interval) -> Parquet -> S3
```

All Phase 1 issues from the original PLAN.md have been addressed. This document covers remaining improvements.

---

## Shortcomings & Solutions

### 1. No Graceful Shutdown Handling

**Problem:** The application has no signal handling (SIGTERM, SIGINT). When the process is killed:
- Data in the channel buffer (up to 1000 messages) is lost
- Data in the batch buffer (up to 5 seconds worth) is lost
- No opportunity to flush before exit
- Database connections not cleanly closed

This means data loss occurs on every deployment, restart, container eviction, or manual stop.

**Approach 1: Tokio Signal + Cancellation Token**

Use `tokio::signal` with a `CancellationToken` to coordinate graceful shutdown across all tasks.

```rust
use tokio::signal;
use tokio_util::sync::CancellationToken;

let shutdown_token = CancellationToken::new();

// In main, spawn a signal handler
let token = shutdown_token.clone();
tokio::spawn(async move {
    signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
    println_with_timestamp!("Shutdown signal received, flushing buffers...");
    token.cancel();
});

// In workers, check for cancellation
tokio::select! {
    _ = shutdown_token.cancelled() => {
        // Flush remaining data and exit
        break;
    }
    // ... normal operations
}
```

**Pros:** Clean, idiomatic Tokio pattern, coordinates multiple tasks
**Cons:** Requires `tokio-util` dependency, modest refactoring

**Approach 2: Shared AtomicBool Flag**

Simple atomic flag checked in each loop iteration.

```rust
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

// Signal handler sets flag
tokio::spawn(async move {
    signal::ctrl_c().await.ok();
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
});

// Workers check flag
loop {
    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        // Flush and break
    }
    // ... normal work
}
```

**Pros:** No new dependencies, simple to understand
**Cons:** Polling-based, less elegant than cancellation token

**Approach 3: Channel-Based Shutdown**

Use a broadcast channel to notify all workers of shutdown.

```rust
let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

// Pass shutdown_rx.subscribe() to each worker
// Workers select! on their shutdown receiver
```

**Pros:** Explicit message passing, works well with existing channel pattern
**Cons:** More boilerplate, another channel to manage

**Files to modify:** `src/main.rs` (main function, db_worker, websocket_worker)

---

### 2. Misleading Function Name

**Problem:** `schedule_daily_task` actually runs **hourly** (every 3600 seconds), not daily. This is confusing for anyone reading or maintaining the code.

**Approach 1: Rename to `schedule_hourly_archive`**

Simple rename that accurately describes behavior.

```rust
async fn schedule_hourly_archive(db_pool: SqlitePool, ...) {
    loop {
        let sleep_duration = 3600;
        println_with_timestamp!("Next archive in {} seconds.", sleep_duration);
        sleep(Duration::from_secs(sleep_duration)).await;
        archive_snapshots(...).await;
    }
}
```

**Pros:** Minimal change, accurate naming
**Cons:** None

**Approach 2: Make Interval Configurable**

Add `ARCHIVE_INTERVAL_SECS` environment variable and rename function generically.

```rust
async fn run_archive_scheduler(db_pool: SqlitePool, ..., interval_secs: u64) {
    loop {
        sleep(Duration::from_secs(interval_secs)).await;
        archive_snapshots(...).await;
    }
}

// In main:
let archive_interval = env::var("ARCHIVE_INTERVAL_SECS")
    .unwrap_or("3600".to_string())
    .parse::<u64>()
    .unwrap_or(3600);
```

**Pros:** Flexible, production-ready
**Cons:** Slightly more code

**Files to modify:** `src/main.rs` (function name, optionally main)

---

### 3. WebSocket Reconnection Uses Fixed Delay

**Problem:** Currently uses a fixed 5-second delay between reconnection attempts. If Binance is having extended issues (maintenance, outage), this:
- Hammers their server unnecessarily
- Could trigger rate limiting or IP bans
- Wastes resources during prolonged outages

**Approach 1: Manual Exponential Backoff**

Track consecutive failures and increase delay.

```rust
let base_delay = 5;
let max_delay = 300; // 5 minutes
let mut consecutive_failures = 0u32;

loop {
    match connect_async(&url).await {
        Ok((ws_stream, _)) => {
            consecutive_failures = 0;  // Reset on success
            // ... handle messages
        },
        Err(e) => {
            consecutive_failures += 1;
            println_with_timestamp!("Connection failed: {}", e);
        }
    }

    let delay = (base_delay * 2u64.pow(consecutive_failures.min(6))).min(max_delay);
    println_with_timestamp!("Reconnecting in {} seconds...", delay);
    sleep(Duration::from_secs(delay)).await;
}
```

Delay progression: 5s → 10s → 20s → 40s → 80s → 160s → 300s (capped)

**Pros:** No new dependencies, full control
**Cons:** Manual implementation

**Approach 2: Use `tokio-retry` (Already in Dependencies)**

Leverage existing dependency for WebSocket connection.

```rust
use tokio_retry::{Retry, strategy::ExponentialBackoff};

let retry_strategy = ExponentialBackoff::from_millis(5000)
    .factor(2)
    .max_delay(Duration::from_secs(300))
    .map(|d| d); // infinite retries

Retry::spawn(retry_strategy, || async {
    connect_async(&url).await
}).await
```

**Pros:** Consistent with S3 retry pattern, battle-tested
**Cons:** Retry crate designed for finite attempts, needs adaptation for infinite reconnect

**Approach 3: Backoff with Jitter**

Add randomization to prevent thundering herd if multiple collectors restart simultaneously.

```rust
let base_delay = 5;
let delay = base_delay * 2u64.pow(consecutive_failures.min(6));
let jitter = rand::random::<u64>() % (delay / 2 + 1);
let final_delay = (delay + jitter).min(300);
```

**Pros:** Better behavior in multi-instance deployments
**Cons:** Slightly more complex

**Files to modify:** `src/main.rs` (websocket_worker function)

---

### 4. No Gap Detection for Orderbook Sequence

**Problem:** Binance orderbook snapshots include `lastUpdateId` which should be sequential. The code doesn't detect if IDs are non-sequential (gaps). Missing an orderbook update means:
- Downstream orderbook reconstruction could be invalid
- Analysis based on the data could be incorrect
- No visibility into data quality issues

**Approach 1: Track and Warn on Gaps**

Maintain last seen ID, log warnings when gaps detected.

```rust
let mut last_update_id: Option<u64> = None;

// After parsing snapshot:
let current_id = snapshot["lastUpdateId"].as_u64().unwrap_or(0);

if let Some(last_id) = last_update_id {
    if current_id != last_id + 1 && current_id > last_id {
        println_with_timestamp!(
            "WARNING: Gap detected in lastUpdateId: {} -> {} (missing {})",
            last_id, current_id, current_id - last_id - 1
        );
    } else if current_id <= last_id {
        println_with_timestamp!(
            "WARNING: Out-of-order or duplicate lastUpdateId: {} after {}",
            current_id, last_id
        );
    }
}
last_update_id = Some(current_id);
```

**Pros:** Simple, provides visibility
**Cons:** Just logs, doesn't recover missing data

**Approach 2: Track Gaps with Metrics Counter**

Add atomic counter for gap events, visible in liveness probe.

```rust
static GAP_EVENTS: AtomicU64 = AtomicU64::new(0);

// On gap detection:
GAP_EVENTS.fetch_add(1, Ordering::Relaxed);

// In liveness probe:
println_with_timestamp!(
    "Liveness: dropped={}, gaps={}",
    DROPPED_SNAPSHOTS.load(Ordering::Relaxed),
    GAP_EVENTS.load(Ordering::Relaxed)
);
```

**Pros:** Aggregated visibility, good for monitoring
**Cons:** Still doesn't recover data

**Approach 3: Store Gap Information in Database**

Record gap events as metadata for later analysis.

```rust
// Add gaps table
CREATE TABLE IF NOT EXISTS gaps (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    expected_id INTEGER NOT NULL,
    actual_id INTEGER NOT NULL,
    timestamp INTEGER NOT NULL
);

// On gap, insert record
```

**Pros:** Persistent record of data quality issues
**Cons:** More complexity, additional table

**Files to modify:** `src/main.rs` (websocket_worker, optionally init_database)

---

### 5. Unbounded Batch Growth

**Problem:** The `batch` vector in `db_worker` grows without limit between interval ticks. Under extreme conditions (very high message rate + slow database), the batch could grow unbounded, causing:
- Excessive memory usage
- Potential OOM crash
- Very large transactions that could timeout

**Approach 1: Max Batch Size Trigger**

Flush early if batch exceeds a threshold.

```rust
const MAX_BATCH_SIZE: usize = 10_000;

loop {
    tokio::select! {
        Some(snapshot) = db_rx.recv() => {
            batch.push(snapshot);
            if batch.len() >= MAX_BATCH_SIZE {
                flush_batch(&db_pool, &mut batch).await;
            }
        },
        _ = interval.tick() => {
            if !batch.is_empty() {
                flush_batch(&db_pool, &mut batch).await;
            }
        }
    }
}
```

**Pros:** Bounds memory, prevents huge transactions
**Cons:** May increase write frequency under load

**Approach 2: Configurable via Environment Variable**

Allow tuning without code changes.

```rust
let max_batch_size = env::var("MAX_BATCH_SIZE")
    .unwrap_or("10000".to_string())
    .parse::<usize>()
    .unwrap_or(10_000);
```

**Pros:** Operational flexibility
**Cons:** Another config to manage

**Approach 3: Adaptive Batch Size**

Dynamically adjust based on processing time.

```rust
let start = Instant::now();
// ... flush batch
let elapsed = start.elapsed();

// If flush took > 1 second, reduce max batch size
// If flush took < 100ms, increase max batch size
```

**Pros:** Self-tuning
**Cons:** More complex, may oscillate

**Files to modify:** `src/main.rs` (db_worker function)

---

### 6. No Metrics/Observability

**Problem:** The application only has basic println logging. For production operation, you'd want:
- Prometheus metrics for dashboards and alerting
- Structured logging for log aggregation (ELK, Datadog, etc.)
- Health check endpoint for load balancers/orchestrators

**Approach 1: Add Prometheus Metrics**

Use `prometheus` crate to expose metrics.

```rust
use prometheus::{Counter, Gauge, register_counter, register_gauge};

lazy_static! {
    static ref MESSAGES_RECEIVED: Counter = register_counter!(
        "messages_received_total", "Total messages received from WebSocket"
    ).unwrap();
    static ref MESSAGES_DROPPED: Counter = register_counter!(
        "messages_dropped_total", "Messages dropped due to backpressure"
    ).unwrap();
    static ref BATCH_SIZE: Gauge = register_gauge!(
        "batch_size", "Current batch size waiting to be flushed"
    ).unwrap();
    static ref CHANNEL_DEPTH: Gauge = register_gauge!(
        "channel_depth", "Current number of messages in channel"
    ).unwrap();
}

// Expose via HTTP endpoint or push to Prometheus Pushgateway
```

**Pros:** Industry standard, integrates with Grafana
**Cons:** New dependencies, HTTP server needed for scraping

**Approach 2: Structured JSON Logging**

Replace println with structured logs using `tracing` + `tracing-subscriber`.

```rust
use tracing::{info, warn, error, instrument};
use tracing_subscriber::fmt::format::json;

// Setup
tracing_subscriber::fmt().json().init();

// Usage
info!(exchange = "binance", symbol = %market_symbol, "Connected to WebSocket");
warn!(dropped = count, "Channel full, dropped snapshot");
```

**Pros:** Machine-parseable, works with any log aggregation
**Cons:** Requires tracing migration, different log format

**Approach 3: Simple Health Check File**

Write timestamp to a file periodically, external monitoring checks file age.

```rust
// In liveness probe
std::fs::write("/tmp/collector-health", SystemTime::now()...)?;
```

**Pros:** Dead simple, no dependencies
**Cons:** Limited information, filesystem-based

**Files to modify:** `src/main.rs`, `Cargo.toml`

---

### 7. Hardcoded Configuration

**Problem:** Many values are hardcoded or have defaults buried in code:
- WebSocket URL pattern (`wss://stream.binance.com:9443/ws/...`)
- Retry delays (5s WebSocket, 1s S3 base)
- Channel buffer size (1000)
- Archive interval (3600s)
- Max connections (5)

This makes operational tuning difficult without code changes.

**Approach 1: Environment Variables for All Config**

Expose everything as env vars with sensible defaults.

```rust
struct Config {
    aws_access_key: String,
    aws_secret_key: String,
    aws_region: String,
    market_symbol: String,
    batch_interval_secs: u64,
    archive_interval_secs: u64,
    channel_buffer_size: usize,
    max_db_connections: u32,
    ws_reconnect_base_delay_secs: u64,
    home_server_name: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            batch_interval_secs: env::var("BATCH_INTERVAL_SECS")
                .unwrap_or("5".into()).parse().unwrap_or(5),
            // ... etc
        }
    }
}
```

**Pros:** 12-factor app compliant, container-friendly
**Cons:** Many env vars to document

**Approach 2: TOML/YAML Config File**

Use a configuration file with `config` crate.

```toml
# config.toml
[aws]
region = "us-west-2"

[websocket]
reconnect_base_delay_secs = 5
reconnect_max_delay_secs = 300

[database]
batch_interval_secs = 5
max_connections = 5

[archive]
interval_secs = 3600
```

**Pros:** Cleaner than many env vars, self-documenting
**Cons:** File management in containers, new dependency

**Approach 3: Hybrid (File + Env Override)**

Load from file, allow env vars to override.

```rust
// config crate supports this pattern
let config = Config::builder()
    .add_source(File::with_name("config"))
    .add_source(Environment::with_prefix("COLLECTOR"))
    .build()?;
```

**Pros:** Best of both worlds
**Cons:** Most complex setup

**Files to modify:** `src/main.rs`, `Cargo.toml`, new `config.toml`

---

### 8. No Tests

**Problem:** The codebase has no unit or integration tests. For a data collection system, this means:
- No confidence in correctness after changes
- Refactoring is risky
- Bugs discovered in production

**Approach 1: Unit Tests for Pure Functions**

Test JSON parsing, timestamp generation, gap detection logic.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_binance_orderbook() {
        let json = r#"{"lastUpdateId": 123, "bids": [], "asks": []}"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(value["lastUpdateId"].as_u64(), Some(123));
    }

    #[test]
    fn test_gap_detection() {
        // Test gap detection logic
    }
}
```

**Pros:** Fast, no external dependencies
**Cons:** Limited coverage of integration points

**Approach 2: Integration Tests with Test Containers**

Use `testcontainers` for SQLite/S3 integration tests.

```rust
#[tokio::test]
async fn test_archive_flow() {
    // Setup test database
    // Insert test data
    // Run archive_snapshots
    // Verify Parquet file created
    // Verify data deleted from DB
}
```

**Pros:** Tests real integration points
**Cons:** Slower, more complex setup

**Approach 3: Mock-Based Testing**

Use traits and mocks for external dependencies.

```rust
#[async_trait]
trait S3Uploader {
    async fn upload(&self, path: &Path, bucket: &str, key: &str) -> Result<()>;
}

// Real implementation uses aws_sdk_s3
// Test implementation records calls
```

**Pros:** Fast, isolated tests
**Cons:** Requires refactoring to use traits

**Files to modify:** `src/main.rs` (add test module), possibly `tests/` directory

---

## Implementation Priority

| Priority | Issue | Impact | Effort |
|----------|-------|--------|--------|
| High | Graceful shutdown | Data loss prevention | Medium |
| High | Misleading function name | Code clarity | Trivial |
| Medium | WebSocket reconnect backoff | Operational stability | Low |
| Medium | Gap detection | Data quality visibility | Low |
| Medium | Unbounded batch | Memory safety | Low |
| Low | Metrics/Observability | Production readiness | High |
| Low | Configuration management | Maintainability | Medium |
| Low | Tests | Reliability | High |

---

## Recommended Implementation Order

1. **Misleading function name** - Trivial fix, do immediately
2. **Unbounded batch** - Simple safeguard, quick win
3. **WebSocket reconnect backoff** - Low effort, operational benefit
4. **Gap detection** - Low effort, data quality visibility
5. **Graceful shutdown** - Medium effort but critical for deployments
6. **Configuration** - When operational needs arise
7. **Metrics** - When scaling to production
8. **Tests** - Ongoing, start with critical paths
