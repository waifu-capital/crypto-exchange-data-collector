# Exchange WebSocket API Reference

This document provides WebSocket API specifications for all supported exchanges, covering orderbook snapshots and trade feeds.

---

## Binance

**Documentation:** https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

### Connection
```
wss://stream.binance.com:9443/ws/<stream_name>
```

### Orderbook Streams

| Stream | Update Speed | Description |
|--------|--------------|-------------|
| `<symbol>@depth<levels>` | 1000ms | Partial depth (5, 10, or 20 levels) |
| `<symbol>@depth<levels>@100ms` | 100ms | Partial depth, faster updates |
| `<symbol>@depth` | 1000ms | Differential updates |
| `<symbol>@depth@100ms` | 100ms | Differential updates, faster |

**Subscription (URL-based):**
```
wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms
```

**Subscription (message-based):**
```json
{
  "method": "SUBSCRIBE",
  "params": ["btcusdt@depth20@100ms"],
  "id": 1
}
```

**Message format:**
```json
{
  "lastUpdateId": 160,
  "bids": [["0.0024", "10"]],
  "asks": [["0.0026", "100"]]
}
```

**Sequence ID:** `lastUpdateId`

### Trade Streams

| Stream | Description |
|--------|-------------|
| `<symbol>@trade` | Raw trades |
| `<symbol>@aggTrade` | Aggregated trades (multiple fills from same taker order) |

**Message format:**
```json
{
  "e": "trade",
  "E": 1672515782136,
  "s": "BTCUSDT",
  "t": 12345,
  "p": "0.001",
  "q": "100",
  "T": 1672515782136,
  "m": true
}
```

**Sequence ID:** `t` (trade ID)

### Rate Limits
- 5 incoming messages per second per connection
- Max 1024 streams per connection
- 300 connections per 5 minutes per IP
- Connections expire after 24 hours

---

## Coinbase

**Documentation:** https://docs.cdp.coinbase.com/exchange/websocket-feed/overview

### Connection
```
wss://ws-feed.exchange.coinbase.com
```

### Subscription Format
```json
{
  "type": "subscribe",
  "product_ids": ["BTC-USD", "ETH-USD"],
  "channels": ["level2", "matches"]
}
```

### Orderbook Channels

| Channel | Update Speed | Description |
|---------|--------------|-------------|
| `level2` | Real-time | Full orderbook with guaranteed delivery |
| `level2_batch` | 50ms batched | Same data, batched for efficiency |

**Snapshot message:**
```json
{
  "type": "snapshot",
  "product_id": "BTC-USD",
  "bids": [["10101.10", "0.45054140"]],
  "asks": [["10102.55", "0.57753524"]]
}
```

**Update message (l2update):**
```json
{
  "type": "l2update",
  "product_id": "BTC-USD",
  "time": "2019-08-14T20:42:27.265Z",
  "changes": [["buy", "10101.80", "0.162567"]]
}
```

Size of `"0"` means remove the price level.

**Sequence ID:** `sequence` (increasing integer per product)

### Trade Channel

| Channel | Description |
|---------|-------------|
| `matches` | Trade executions |

**Message format:**
```json
{
  "type": "match",
  "trade_id": 10,
  "sequence": 50,
  "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
  "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
  "time": "2014-11-07T08:19:27.028459Z",
  "product_id": "BTC-USD",
  "size": "5.23512",
  "price": "400.23",
  "side": "sell"
}
```

**Sequence ID:** `sequence`

### Symbol Format
- Uppercase with hyphen: `BTC-USD`, `ETH-EUR`

---

## Upbit

**Documentation:** https://docs.upbit.com/reference/websocket-orderbook

### Connection
```
wss://api.upbit.com/websocket/v1
```

### Subscription Format
```json
[
  {"ticket": "unique-uuid"},
  {"type": "orderbook", "codes": ["KRW-BTC", "KRW-ETH"]},
  {"format": "DEFAULT"}
]
```

Options:
- `is_only_snapshot`: Boolean, snapshot only
- `is_only_realtime`: Boolean, real-time only

### Orderbook Feed

**Message format:**
```json
{
  "ty": "orderbook",
  "cd": "KRW-BTC",
  "tas": 12.345,
  "tbs": 23.456,
  "obu": [
    {"ap": 50000000, "as": 0.1, "bp": 49999000, "bs": 0.2}
  ],
  "tms": 1672515782136,
  "st": "REALTIME"
}
```

| Field | Abbrev | Description |
|-------|--------|-------------|
| type | ty | "orderbook" |
| code | cd | Trading pair |
| total_ask_size | tas | Total sell liquidity |
| total_bid_size | tbs | Total buy liquidity |
| orderbook_units | obu | Price levels array |
| timestamp | tms | Unix milliseconds |
| stream_type | st | "SNAPSHOT" or "REALTIME" |

**Sequence ID:** `tms` (timestamp)

### Trade Feed

**Subscription:**
```json
[
  {"ticket": "unique-uuid"},
  {"type": "trade", "codes": ["KRW-BTC"]},
  {"format": "DEFAULT"}
]
```

**Message format:**
```json
{
  "ty": "trade",
  "cd": "KRW-BTC",
  "tp": 50000000,
  "tv": 0.001,
  "ab": "BID",
  "ttms": 1672515782136,
  "st": "REALTIME"
}
```

| Field | Abbrev | Description |
|-------|--------|-------------|
| trade_price | tp | Execution price |
| trade_volume | tv | Execution quantity |
| ask_bid | ab | "ASK" (sell) or "BID" (buy) |
| trade_timestamp | ttms | Execution timestamp (ms) |
| sequential_id | sid | Unique trade ID |

**Sequence ID:** `sid` (sequential_id)

### Symbol Format
- Quote-Base order, uppercase with hyphen: `KRW-BTC`, `BTC-ETH`

---

## OKX

**Documentation:** https://www.okx.com/docs-v5/en/#order-book-trading-ws

### Connection
```
wss://ws.okx.com:8443/ws/v5/public
```

### Subscription Format
```json
{
  "op": "subscribe",
  "args": [
    {"channel": "books", "instId": "BTC-USDT"}
  ]
}
```

### Orderbook Channels

| Channel | Update Speed | Description |
|---------|--------------|-------------|
| `books` | 100ms | 400 levels, incremental |
| `books5` | 100ms | 5 levels, snapshot only |
| `books50-l2-tbt` | 10ms | 50 levels, tick-by-tick |
| `books-l2-tbt` | 10ms | 400 levels, tick-by-tick |

**Message format:**
```json
{
  "arg": {"channel": "books", "instId": "BTC-USDT"},
  "action": "snapshot",
  "data": [{
    "asks": [["41006.8", "0.60038921", "0", "1"]],
    "bids": [["41006.3", "0.30178218", "0", "2"]],
    "ts": "1672515782136",
    "seqId": 123456789
  }]
}
```

Array format: `[price, size, deprecated, num_orders]`

**Sequence ID:** `seqId`

### Trade Channel

**Subscription:**
```json
{
  "op": "subscribe",
  "args": [{"channel": "trades", "instId": "BTC-USDT"}]
}
```

**Message format:**
```json
{
  "arg": {"channel": "trades", "instId": "BTC-USDT"},
  "data": [{
    "instId": "BTC-USDT",
    "tradeId": "130639474",
    "px": "42219.9",
    "sz": "0.12060306",
    "side": "buy",
    "ts": "1672515782136"
  }]
}
```

**Sequence ID:** `tradeId`

### Rate Limits
- 3 requests per second per IP
- 480 subscription requests per hour

### Symbol Format
- Uppercase with hyphen: `BTC-USDT`, `ETH-USDT`

---

## Bybit

**Documentation:** https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook

### Connection
```
wss://stream.bybit.com/v5/public/spot
wss://stream.bybit.com/v5/public/linear  (futures)
```

### Subscription Format
```json
{
  "op": "subscribe",
  "args": ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"]
}
```

### Orderbook Topics

| Topic | Update Speed | Description |
|-------|--------------|-------------|
| `orderbook.1.<symbol>` | 10ms | 1 level (best bid/ask) |
| `orderbook.50.<symbol>` | 20ms | 50 levels |
| `orderbook.200.<symbol>` | 100-200ms | 200 levels |
| `orderbook.1000.<symbol>` | 200ms | 1000 levels |

**Message format:**
```json
{
  "topic": "orderbook.50.BTCUSDT",
  "type": "snapshot",
  "ts": 1672515782136,
  "data": {
    "s": "BTCUSDT",
    "b": [["41006.3", "0.30178218"]],
    "a": [["41006.8", "0.60038921"]],
    "u": 123456789,
    "seq": 987654321
  },
  "cts": 1672515782100
}
```

| Field | Description |
|-------|-------------|
| type | "snapshot" or "delta" |
| ts | System timestamp (ms) |
| cts | Matching engine timestamp |
| u | Update ID |
| seq | Cross sequence (for comparing depths) |
| b | Bids `[[price, size], ...]` |
| a | Asks `[[price, size], ...]` |

Size of `"0"` means remove the price level.

**Sequence ID:** `u` (update ID) or `seq`

### Trade Topic

**Topic:** `publicTrade.<symbol>`

**Message format:**
```json
{
  "topic": "publicTrade.BTCUSDT",
  "type": "snapshot",
  "ts": 1672515782136,
  "data": [{
    "i": "2100000000007764175",
    "T": 1672515782136,
    "p": "16578.50",
    "v": "0.001",
    "S": "Buy",
    "s": "BTCUSDT",
    "BT": false
  }]
}
```

| Field | Description |
|-------|-------------|
| i | Trade ID |
| T | Trade timestamp (ms) |
| p | Price |
| v | Volume |
| S | Side ("Buy" or "Sell") |
| BT | Block trade flag |

**Sequence ID:** `i` (trade ID)

### Symbol Format
- Uppercase, no separator: `BTCUSDT`, `ETHUSDT`

---

## Summary Table

| Exchange | Orderbook Stream | Trade Stream | Sequence ID | Symbol Format |
|----------|------------------|--------------|-------------|---------------|
| Binance | `@depth20@100ms` | `@trade` | `lastUpdateId` / `t` | `btcusdt` |
| Coinbase | `level2` | `matches` | `sequence` | `BTC-USD` |
| Upbit | `orderbook` | `trade` | `tms` / `sid` | `KRW-BTC` |
| OKX | `books` | `trades` | `seqId` / `tradeId` | `BTC-USDT` |
| Bybit | `orderbook.50` | `publicTrade` | `u` / `i` | `BTCUSDT` |
