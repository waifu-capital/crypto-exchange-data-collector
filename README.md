# Crypto Exchange Data Collector

A production-grade Rust application for collecting real-time market data from multiple cryptocurrency exchanges. Designed for quant trading systems requiring reliable, low-latency data ingestion with minimal data loss.

## Key Features

### Multi-Exchange Support

Collect orderbook and trade data from 5 major exchanges simultaneously:

| Exchange | Orderbook | Trades | Symbol Format |
|----------|-----------|--------|---------------|
| Binance | `@depth20@100ms` | `@trade` | `btcusdt` |
| Coinbase | `level2` | `matches` | `BTC-USD` |
| Upbit | `orderbook` | `trade` | `KRW-BTC` |
| OKX | `books` | `trades` | `BTC-USDT` |
| Bybit | `orderbook.50` | `publicTrade` | `BTCUSDT` |

### Single Process, Multiple Markets

One process handles all exchanges and symbols. Configure everything in a single TOML file:

```toml
[[markets]]
exchange = "binance"
symbols = ["btcusdt", "ethusdt", "solusdt"]

[[markets]]
exchange = "coinbase"
symbols = ["BTC-USD", "ETH-USD"]
```

### Data Integrity

- **Sequence gap detection** - Detects missing messages in exchange streams
- **Duplicate prevention** - Multi-column unique constraints prevent duplicate records
- **Atomic archiving** - S3 uploads verified before local data deletion
- **Separate tables** - Orderbook and trade data stored in dedicated SQLite tables

### Production Observability

- **Prometheus metrics** at `/metrics` (port 9090)
- **Health endpoint** at `/health` with connection status
- **Structured logging** - JSON logs for aggregation, pretty stdout for humans
- **Latency tracking** - Exchange-to-collector latency histograms

## Quick Start

### Prerequisites

- Rust 2024 edition (1.85+)
- AWS credentials with S3 access

### 1. Clone and Build

```bash
git clone https://github.com/waifu-capital/crypto-exchange-data-collector.git
cd crypto-exchange-data-collector
cargo build --release
```

### 2. Configure

Create `config.toml` (or copy the example):

```toml
[collector]
feeds = ["orderbook", "trades"]
batch_interval_secs = 5
archive_interval_secs = 3600
metrics_port = 9090

[aws]
region = "us-west-2"
bucket = "your-bucket-name"

[database]
path = "data/collector.db"

[[markets]]
exchange = "binance"
symbols = ["btcusdt", "ethusdt"]

[[markets]]
exchange = "coinbase"
symbols = ["BTC-USD"]
```

### 3. Set Environment Variables

```bash
export AWS_ACCESS_KEY="your-access-key"
export AWS_SECRET_KEY="your-secret-key"
```

### 4. Run

```bash
./target/release/crypto-exchange-data-collector
```

Or with custom config path:

```bash
CONFIG_PATH=/path/to/config.toml ./target/release/crypto-exchange-data-collector
```

## Testing

### Unit Tests

```bash
cargo test
```

### Live Smoke Tests

Connect to real exchange WebSockets and verify message parsing:

```bash
# Run all smoke tests
cargo test -- --ignored

# Single exchange
cargo test live_binance -- --ignored
cargo test live_coinbase -- --ignored
cargo test live_upbit -- --ignored
cargo test live_okx -- --ignored
cargo test live_bybit -- --ignored
```

## Configuration Reference

### `[collector]`

| Option | Default | Description |
|--------|---------|-------------|
| `feeds` | `["orderbook"]` | Data types to collect: `"orderbook"`, `"trades"` |
| `batch_interval_secs` | `5` | Flush interval for SQLite writes |
| `archive_interval_secs` | `3600` | S3 archive interval (1 hour) |
| `metrics_port` | `9090` | Prometheus metrics HTTP port |
| `log_retention_days` | `1` | Days to keep log files |

### `[aws]`

| Option | Default | Description |
|--------|---------|-------------|
| `region` | `us-west-2` | AWS region for S3 |
| `bucket` | `crypto-market-data` | S3 bucket name |
| `home_server_name` | (none) | Optional server identifier in S3 path |

### `[database]`

| Option | Default | Description |
|--------|---------|-------------|
| `path` | `data/collector.db` | SQLite database path |

### `[archive]`

| Option | Default | Description |
|--------|---------|-------------|
| `dir` | `data/archive` | Temporary directory for Parquet files |

### `[websocket]`

| Option | Default | Description |
|--------|---------|-------------|
| `message_timeout_secs` | `30` | Stale connection detection timeout |
| `initial_retry_delay_secs` | `1` | Initial reconnect delay |
| `max_retry_delay_secs` | `60` | Maximum reconnect delay (exponential backoff) |

### `[[markets]]`

| Option | Required | Description |
|--------|----------|-------------|
| `exchange` | Yes | Exchange name: `binance`, `coinbase`, `upbit`, `okx`, `bybit` |
| `symbols` | Yes | List of trading pair symbols |

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | Prometheus metrics |
| `GET /health` | JSON health status with connection states |
| `GET /ready` | Simple readiness probe |

### Health Response Example

```json
{
  "status": "healthy",
  "uptime_seconds": 3847,
  "connections": {"up": 6, "total": 6},
  "messages_dropped": 0
}
```

---

## Architecture

### Data Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Binance   │    │  Coinbase   │    │    Bybit    │
│  WebSocket  │    │  WebSocket  │    │  WebSocket  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └────────────┬─────┴──────────────────┘
                    │
                    ▼
           ┌────────────────┐
           │  MPSC Channel  │  (bounded, 1000 × market pairs)
           └────────┬───────┘
                    │
                    ▼
           ┌────────────────┐
           │   DB Worker    │  (batched writes every 5s)
           └────────┬───────┘
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
┌───────────────┐       ┌───────────────┐
│   orderbooks  │       │    trades     │
│    (SQLite)   │       │   (SQLite)    │
└───────┬───────┘       └───────┬───────┘
        │                       │
        └───────────┬───────────┘
                    │
                    ▼
           ┌────────────────┐
           │    Archiver    │  (hourly)
           └────────┬───────┘
                    │
                    ▼
           ┌────────────────┐
           │   S3 Bucket    │  (Parquet files)
           └────────────────┘
```

### S3 Storage Structure

```
{bucket}/
├── binance/
│   ├── btcusdt/
│   │   ├── orderbook/
│   │   │   └── 2026-01-17/
│   │   │       └── 1737100800000.parquet
│   │   └── trade/
│   │       └── 2026-01-17/
│   │           └── 1737100800000.parquet
│   └── ethusdt/
│       └── ...
└── coinbase/
    └── BTC-USD/
        └── ...
```

With `home_server_name` configured, an additional level is added:
```
{bucket}/{exchange}/{symbol}/{data_type}/{server}/{date}/{timestamp}.parquet
```

### Database Schema

Two separate tables for orderbook and trade data:

```sql
CREATE TABLE orderbooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_sequence_id TEXT NOT NULL,
    timestamp_collector INTEGER NOT NULL,  -- microseconds
    timestamp_exchange INTEGER NOT NULL,   -- microseconds
    data TEXT NOT NULL,                    -- raw JSON
    UNIQUE(exchange, symbol, exchange_sequence_id)
);

CREATE TABLE trades (
    -- same schema as orderbooks
);
```

### Module Structure

```
src/
├── main.rs          # Entry point, worker orchestration
├── config.rs        # TOML configuration parsing
├── models.rs        # MarketEvent, DataType, SequenceTracker
├── db.rs            # SQLite operations, batched writes
├── websocket.rs     # WebSocket connections, message handling
├── archive.rs       # Parquet creation, S3 upload
├── http.rs          # Prometheus metrics, health endpoints
├── metrics.rs       # Metric definitions
├── utils.rs         # Log cleanup utilities
└── exchanges/
    ├── mod.rs       # Exchange trait definition
    ├── binance.rs
    ├── coinbase.rs
    ├── upbit.rs
    ├── okx.rs
    └── bybit.rs
```

## Additional Features

### Graceful Shutdown

Ctrl+C triggers coordinated shutdown:
1. All WebSocket workers close connections cleanly
2. DB worker flushes any buffered data
3. Individual timeouts prevent stuck workers from blocking shutdown

### Backpressure Handling

- Non-blocking channel sends (never stall WebSocket)
- 1000 × market_pairs buffer size
- Dropped messages counted and exposed via metrics

### Connection Resilience

- **Stale connection detection** - Reconnects if no message received within timeout
- **Exponential backoff** - 1s → 2s → 4s → ... → 60s max with ±25% jitter
- **Parse error circuit breaker** - Reconnects if >50% of messages fail to parse

### Timestamp Precision

All timestamps stored in **microseconds** since Unix epoch:
- `timestamp_collector` - When we received the message
- `timestamp_exchange` - Exchange's event timestamp

### Sequence Tracking

Detects gaps in exchange message sequences:
- Gap detection (missed messages)
- Out-of-order detection
- Duplicate detection

Exposed via Prometheus metrics for alerting.

## Prometheus Metrics

### WebSocket

- `collector_websocket_connected{exchange,symbol}` - Connection status (1/0)
- `collector_websocket_reconnects_total{exchange,symbol}` - Reconnection count
- `collector_messages_received_total{exchange,symbol,data_type}` - Message throughput
- `collector_messages_dropped_total` - Backpressure drops
- `collector_message_timeouts_total{exchange,symbol}` - Stale connection events

### Latency

- `collector_latency_exchange_to_collector_ms{exchange,symbol,data_type}` - Histogram

### Sequences

- `collector_sequence_gaps_total{exchange,symbol,data_type}` - Gap count
- `collector_sequence_gap_size{exchange,symbol,data_type}` - Gap size histogram
- `collector_sequence_out_of_order_total{exchange,symbol,data_type}`
- `collector_sequence_duplicates_total{exchange,symbol,data_type}`

### Database

- `collector_db_write_seconds{operation}` - Write latency histogram
- `collector_db_snapshots_written_total` - Successful writes
- `collector_db_insert_errors_total{error_type}` - Insert failures by type

### Archive

- `collector_archives_completed_total` - Successful archive cycles
- `collector_snapshots_archived_total` - Records archived to S3
- `collector_s3_upload_seconds{status}` - Upload latency
- `collector_archive_failures_total{stage}` - Failures by stage

## Docker

```dockerfile
FROM rust:1.85 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/crypto-exchange-data-collector /usr/local/bin/
COPY config.toml /etc/collector/config.toml
ENV CONFIG_PATH=/etc/collector/config.toml
CMD ["crypto-exchange-data-collector"]
```

```bash
docker build -t crypto-collector .
docker run -d \
  -e AWS_ACCESS_KEY=xxx \
  -e AWS_SECRET_KEY=xxx \
  -p 9090:9090 \
  -v $(pwd)/data:/app/data \
  crypto-collector
```

## License

MIT
