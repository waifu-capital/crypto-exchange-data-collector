# Data Persistence Architecture Improvements

This document outlines the shortcomings of the current data collection architecture and recommended solutions.

---

## Current Architecture Overview

The application collects orderbook snapshots from Binance WebSocket, buffers them in SQLite, and periodically archives to S3 as Parquet files.

```
WebSocket -> Channel (100 buffer) -> SQLite (batch writes) -> Parquet -> S3
```

---

## Shortcomings & Solutions

### 1. Data Loss Windows

**Problem:** If the process crashes between WebSocket receipt and batch commit, all buffered data in the channel is lost. The channel buffer (100 messages) is entirely in-memory with no durability.

**Solution:**
- Enable SQLite WAL mode for better crash recovery:
  ```rust
  conn.execute("PRAGMA journal_mode=WAL", []).unwrap();
  ```
- Reduce batch interval or implement hybrid approach: immediate writes for critical data, batched for high-throughput periods
- Consider adding a persistent queue (e.g., write to append-only file before channel)

**Files to modify:** `src/main.rs` (lines 106-119, db_worker function)

---

### 2. Risky Archive-Then-Delete Sequence

**Problem:** The current flow uploads to S3, then deletes from SQLite (lines 269-273). This creates two failure scenarios:
- Upload succeeds, crash before delete → duplicates
- Delete happens, upload silently failed → data loss

**Solution:**
- Add a `processed` or `archived` flag instead of immediate deletion:
  ```sql
  ALTER TABLE snapshots ADD COLUMN archived INTEGER DEFAULT 0;
  ```
- Mark records as archived after successful S3 upload
- Delete only after confirming S3 object exists (via HEAD request)
- Implement periodic cleanup of archived records after retention period

**Files to modify:** `src/main.rs` (lines 109-118 schema, lines 225-274 archive function)

---

### 3. Blocking I/O in Async Runtime

**Problem:** `rusqlite` is synchronous, and `std::sync::Mutex::lock()` in an async context can cause thread starvation and potential deadlocks.

**Solution:**
- Wrap SQLite operations in `tokio::task::spawn_blocking`:
  ```rust
  let snapshots = tokio::task::spawn_blocking(move || {
      let conn = db_conn.lock().unwrap();
      // ... SQLite operations
  }).await.unwrap();
  ```
- Alternatively, use `tokio::sync::Mutex` for the connection wrapper
- Consider migrating to an async-native database (sqlx with SQLite)

**Files to modify:** `src/main.rs` (lines 108, 131, 230, 271)

---

### 4. No Backpressure Handling

**Problem:** If WebSocket data arrives faster than it can be processed, the channel fills up (capacity 100), the sender blocks, and WebSocket reads stall. This can cause the WebSocket connection to be dropped by the server.

**Solution:**
- Increase channel capacity based on expected throughput
- Use `try_send` with overflow handling:
  ```rust
  if db_tx.try_send((query, params)).is_err() {
      println_with_timestamp!("Warning: channel full, dropping snapshot");
      // Or implement overflow buffer
  }
  ```
- Add metrics for channel utilization to detect capacity issues
- Consider bounded queue with disk spillover for overflow

**Files to modify:** `src/main.rs` (line 56, lines 203-213)

---

### 5. Poor Timestamp Resolution

**Problem:** Timestamps use second-level granularity (line 204) for data arriving at 100ms intervals. This means ~10 records share the same timestamp, making it impossible to reconstruct exact event ordering.

**Solution:**
- Use millisecond or microsecond timestamps:
  ```rust
  let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis()  // or as_micros()
      .to_string();
  ```
- Update schema to use INTEGER for timestamp (more efficient than TEXT)
- Add sequence number for guaranteed ordering within same millisecond

**Files to modify:** `src/main.rs` (line 204, lines 109-118 schema)

---

### 6. Schema Design

**Assessment:** The current schema is **appropriate** for this use case.

Since SQLite serves purely as a transient buffer before S3 archival, the only operations are:
1. Bulk insert (streaming writes)
2. Bulk select all (archive export)
3. Bulk delete (post-archive cleanup)

**Why the current approach is correct:**
- No indexes needed - there are no WHERE clauses on individual records
- No normalization needed - adds write overhead with zero query benefit
- JSON blob storage is optimal - minimizes write latency and schema complexity
- Analytical queries happen on Parquet files in S3, not in SQLite

**No changes required.**

---

### 7. No Idempotency / Duplicate Handling

**Problem:** No mechanism to detect or prevent duplicate processing if the same data is received twice (e.g., after reconnection).

**Solution:**
- Use `lastUpdateId` as a deduplication key:
  ```sql
  CREATE UNIQUE INDEX idx_snapshots_update_id ON snapshots(lastUpdateId);
  ```
- Use `INSERT OR IGNORE` to skip duplicates:
  ```rust
  let query = "INSERT OR IGNORE INTO snapshots ...";
  ```
- Track last processed `lastUpdateId` to detect gaps in sequence

**Files to modify:** `src/main.rs` (lines 109-118, line 209)

---

### 8. Error Handling

**Problem:** Extensive use of `.unwrap()` throughout the codebase. Any error causes a panic, potentially losing buffered data.

**Solution:**
- Replace `.unwrap()` with proper error handling using `Result` and `?` operator
- Implement graceful degradation (log errors, continue processing)
- Add retry logic for transient failures (S3 uploads, WebSocket reconnection)
- Example:
  ```rust
  match db_tx.send((query, params)).await {
      Ok(_) => {},
      Err(e) => {
          println_with_timestamp!("Failed to queue snapshot: {}", e);
          // Handle error appropriately
      }
  }
  ```

**Files to modify:** Throughout `src/main.rs`

---

### 9. S3 Upload Reliability

**Problem:** S3 upload has no retry logic and uses `.unwrap()` (line 279). Transient network failures cause panics.

**Solution:**
- Add exponential backoff retry:
  ```rust
  async fn upload_to_s3_with_retry(/* ... */, max_retries: u32) -> Result<(), Error> {
      for attempt in 0..max_retries {
          match client.put_object()/* ... */.send().await {
              Ok(_) => return Ok(()),
              Err(e) if attempt < max_retries - 1 => {
                  let delay = Duration::from_secs(2u64.pow(attempt));
                  sleep(delay).await;
              }
              Err(e) => return Err(e),
          }
      }
  }
  ```
- Verify upload success with HEAD request before marking as archived

**Files to modify:** `src/main.rs` (lines 277-281)

---

### 10. Hardcoded Region in Bucket Creation

**Problem:** Bucket creation uses hardcoded `UsWest2` region (line 90) despite having `AWS_REGION` env var.

**Solution:**
- Pass region dynamically:
  ```rust
  let location_constraint = match aws_region.as_str() {
      "us-east-1" => None,  // us-east-1 doesn't use location constraint
      region => Some(BucketLocationConstraint::from(region)),
  };
  ```

**Files to modify:** `src/main.rs` (lines 88-91)

---

## Implementation Priority

| Priority | Issue | Impact | Effort |
|----------|-------|--------|--------|
| High | Archive-then-delete risk | Data loss | Medium |
| High | Error handling | Crashes/data loss | High |
| High | Blocking I/O in async | Performance | Medium |
| Medium | Data loss windows (WAL) | Data loss | Low |
| Medium | Timestamp resolution | Data quality | Low |
| Medium | S3 upload reliability | Data loss | Medium |
| Low | Backpressure handling | Stability | Medium |
| Low | Idempotency | Data quality | Low |
| Low | Hardcoded region | Correctness | Low |
| N/A | Schema design | N/A | N/A | *(current design is appropriate)*

---

## Alternative Architectures to Consider

For production workloads with stricter requirements:

1. **TimescaleDB/QuestDB** - Purpose-built time-series databases with better compression and query performance
2. **Kafka + ClickHouse** - For high-throughput with replay capability
3. **AWS Kinesis Firehose** - Managed streaming to S3 with automatic batching and retry
4. **DuckDB** - Embedded analytics database with native Parquet support
