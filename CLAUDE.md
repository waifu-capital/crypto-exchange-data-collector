# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important Rules

1. **Read this file first** - At the start of every conversation, read CLAUDE.md to ensure you follow all guidelines and understand the codebase conventions.

2. **Document all changes in CHANGES.md** - After making any code changes, add a dated entry to CHANGES.md describing: what changed, why, and how. Follow the existing format in that file.

3. **Never revert previous fixes without explicit justification** - Before undoing a previous change (especially one documented in CHANGES.md), carefully review WHY that change was made. If reverting, document the specific reason why the original fix was wrong or circumstances have changed.

4. **Write detailed commit messages** - Every commit must follow this format:
   - First line: conventional commit type and short summary (e.g., `fix: resolve ping starvation in WebSocket select! loop`)
   - Empty line
   - Detailed body explaining WHAT changed and WHY, using bullet points for multiple changes
   - End with `Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>`

   Example:
   ```
   fix: resolve ping starvation in WebSocket select! loop

   Remove `biased` keyword from tokio::select! that was causing ping timer
   to be starved under high message volume. Add backup ping check after
   each message for sustained load scenarios.

   This fixes OKX and Coinbase "Connection reset without closing handshake"
   errors caused by missing keepalive pings.

   Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
   ```

5. **Apply expert domain knowledge** - When working on code, apply deep expertise relevant to the domain. For example:
   - Networking code: understand TCP/IP, WebSocket protocols, keepalive mechanisms, connection timeouts
   - Cryptography: understand security implications, proper key handling, timing attacks
   - Concurrency: understand async runtimes, race conditions, deadlocks, fair scheduling
   - Database: understand transactions, indexing, query optimization
   - Always consult official documentation and RFCs when dealing with protocols.

6. **Present multiple implementation options** - For non-trivial changes, always present 2-3 implementation approaches with:
   - Brief description of each approach
   - Pros and cons of each
   - Your recommendation with reasoning

   This demonstrates thorough analysis and lets the user make informed decisions. Skip this only for trivial fixes (typos, obvious bugs).

7. **Ensure code quality and linting** - Before committing, verify the code passes all quality checks:
   - **Rust**: `cargo build` (no errors), `cargo test` (all pass), `cargo clippy` (no warnings)
   - **Python**: `ruff check` (no errors), `black --check` (formatted), `pytest` (all pass)
   - **TypeScript/JavaScript**: `npm run build`, `npm run lint`, `npm test`
   - Fix any issues before committing. Never commit code with linting errors or failing tests.

8. **Write clean, elegant, DRY code** - After making changes, review the surrounding code and ensure:
   - **DRY (Don't Repeat Yourself)**: Extract duplicated logic into helper functions
   - **Readability**: Code should be easy to read and understand at a glance
   - **Simplicity**: Prefer simple solutions over clever ones; reduce complexity where possible
   - **Comments**: Add clear comments for non-obvious logic. If code is convoluted and can't be simplified, explain WHY with comments
   - **Consistency**: Follow existing patterns and naming conventions in the codebase

   Good code reads like well-written prose. If you have to read something twice to understand it, it needs refactoring or comments.

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
