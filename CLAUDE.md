# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build
cargo build --release

# Unit tests
cargo test

# Live smoke tests (connects to real exchange WebSockets)
cargo test -- --ignored

# Single exchange smoke test
cargo test live_binance -- --ignored
cargo test live_coinbase -- --ignored
cargo test live_upbit -- --ignored
cargo test live_okx -- --ignored
cargo test live_bybit -- --ignored

# Run
./target/release/crypto-exchange-data-collector
CONFIG_PATH=/path/to/config.toml ./target/release/crypto-exchange-data-collector
```

## Architecture Overview

Real-time cryptocurrency market data collector supporting 5 exchanges (Binance, Coinbase, Upbit, OKX, Bybit) with direct Parquet streaming to S3.

### Data Flow

```
WebSocket Workers → MPSC Channel → Parquet Worker → Parquet Files → Upload Worker → S3
(per market/feed)   (bounded 1000×N)  (streaming)    (rotation)      (verified)
```

### Key Design Decisions

- **Direct Parquet streaming** - No intermediate database; messages stream directly to Parquet files with 1000-row buffering per writer
- **Safe S3 deletion** - Local files deleted ONLY after S3 upload verification via HEAD request
- **Bounded memory** - Fixed buffer sizes prevent OOM under high message rates
- **Decoupled workers** - WebSocket writes never block on S3 operations

### Module Structure

```
src/
├── main.rs          # Worker orchestration, shutdown coordination
├── config.rs        # TOML parsing, market configuration
├── models.rs        # MarketEvent, DataType, WriterKey
├── websocket.rs     # WebSocket connections, exponential backoff, parse error circuit breaker
├── parquet.rs       # Streaming Parquet writer, file rotation (1 hour OR 500MB)
├── upload.rs        # S3 upload worker with retry logic
├── s3.rs            # S3 client creation
├── http.rs          # Prometheus /metrics, /health, /ready endpoints
├── metrics.rs       # Prometheus metric definitions (lazy_static)
├── utils.rs         # Log file cleanup
└── exchanges/       # Exchange-specific WebSocket implementations
    ├── mod.rs       # Exchange trait, message routing, live tests
    ├── binance.rs
    ├── coinbase.rs  # Includes JWT authentication for level2 orderbook
    ├── upbit.rs
    ├── okx.rs
    └── bybit.rs
```

### Exchange Trait Pattern

All exchanges implement the `Exchange` trait in `src/exchanges/mod.rs`:
- `name()` - Exchange identifier
- `websocket_url(symbol)` - WebSocket endpoint URL
- `build_subscribe_messages(symbol, feeds)` - Subscription payloads
- `parse_message(text)` - JSON → `Vec<ExchangeMessage>`

### Supported Exchanges

| Exchange | Symbol Format | Notes |
|----------|---------------|-------|
| Binance | `btcusdt` | US geo-blocking (use `base_url` override) |
| Coinbase | `BTC-USD` | JWT auth required for orderbook |
| Upbit | `KRW-BTC` | Regional restrictions |
| OKX | `BTC-USDT` | |
| Bybit | `BTCUSDT` | |

### Configuration

Main sections in `config.toml`:
- `[collector]` - metrics_port, log_retention_days
- `[aws]` - region, bucket, home_server_name
- `[storage]` - mode (s3/local/both), data_dir, local_path
- `[websocket]` - message_timeout_secs, retry delays
- `[[markets]]` - exchange, symbols, feeds, optional base_url override

### Environment Variables

- `AWS_ACCESS_KEY`, `AWS_SECRET_KEY` - S3 credentials
- `COINBASE_API_KEY`, `COINBASE_API_SECRET_FILE` - Coinbase JWT auth
- `CONFIG_PATH` - Custom config file path
- `RUST_LOG` - Log level (default: info)

### Timestamp Convention

All timestamps are **microseconds** since Unix epoch:
- `timestamp_collector` - When application received the message
- `timestamp_exchange` - Exchange's original event timestamp

### S3 Path Structure

```
{bucket}/{exchange}/{symbol}/{data_type}/{server}/{date}/{timestamp}.parquet
```
