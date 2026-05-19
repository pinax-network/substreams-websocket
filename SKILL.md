# substreams-websocket — agent guide

This is a Substreams-to-WebSocket fan-out server. It runs one or more `db_out`-style Substreams packages and broadcasts decoded `sf.substreams.sink.database.v1.DatabaseChanges` block payloads as JSON over WebSocket.

If you're an AI agent or programmatic client, this page tells you everything you need to know to connect and consume the stream. Source: <https://github.com/pinax-network/substreams-websocket>.

## Server URL conventions

Stream selectors are `<network>@<stream>` (Binance market-streams style). `*` is a wildcard on either side. `<network>` is a chain identifier (e.g. `solana-mainnet`, `ethereum-mainnet`). `<stream>` is the operator-chosen display label for a stream (e.g. `swaps`, `transfers`).

Two URL modes:

| URL | Behavior |
|-----|----------|
| `/ws/<a>` | Single stream — raw JSON payload per block. |
| `/ws/<a>/<b>/...` | Multiple streams — every payload wrapped as `{"stream":"<network>@<stream>","data":<raw>}`. |
| `/stream?streams=<a>/<b>/...` | Combined query mode — always wraps. |

Bare `/ws` (no streams) returns HTTP 400. Use `/ws/*@*` to subscribe to everything explicitly.

Examples:

```
wss://<host>/ws/solana-mainnet@swaps
wss://<host>/ws/solana-mainnet@swaps/ethereum-mainnet@transfers
wss://<host>/ws/*@swaps
wss://<host>/stream?streams=*@*
```

## Welcome message

On connect, the server sends a single `session` message describing every configured stream and the connection's parsed subscriptions.

```json
{
  "type": "session",
  "status": "connected",
  "client_id": 1,
  "streams": [
    {
      "stream": "swaps",
      "network": "solana-mainnet",
      "module": "db_out",
      "manifest": "https://.../svm-dex-v0.5.1.spkg",
      "module_hash": "bd388f2e39f5dcc237cfbdb8d6c96d9e5678c797"
    }
  ],
  "subscriptions": ["solana-mainnet@swaps"],
  "wrap_envelope": false
}
```

- `streams` lists every stream the server knows about.
- `subscriptions` is what this connection will actually receive (filtered set).
- `wrap_envelope` tells you whether subsequent payloads are wrapped in `{"stream","data"}` or sent raw.

## Block payload shape

One message per non-empty block. Empty blocks are skipped.

```json
{
  "stream": "swaps",
  "network": "solana-mainnet",
  "block_num": 350000000,
  "block_hash": "Gsk6...",
  "timestamp": "2026-05-13 17:00:00",
  "cursor": "Mloz_-WpoBoZ...",
  "module_hash": "bd388f2e...",
  "events": [
    {
      "@table": "swaps",
      "input_amount": "1287000000",
      "input_mint": "So11111111111111111111111111111111111111112",
      "output_amount": "6848381008732",
      "output_mint": "13muFY...",
      "protocol": "raydium_cpmm",
      "user": "F2MUE..."
    }
  ]
}
```

Field reference:

- `stream` / `network` — together identify which stream this block belongs to.
- `block_num`, `block_hash`, `timestamp` — block-level metadata. Timestamps are UTC `YYYY-MM-DD HH:MM:SS`.
- `cursor` — opaque Substreams cursor. Persist it if you want to backfill from this exact position via a direct Substreams gRPC client. The server itself does not replay.
- `module_hash` — canonical 40-hex SHA-1 of the Substreams output module. Use it to detect spkg upgrades.
- `events` — array of `TableChange` rows, in source order. Each event has `@table` (DB table name; prefixed to avoid collision with a column literally called `table`) plus the column `name → value` pairs.
- All values inside `events[*]` are strings on the wire (per DatabaseChanges proto). Numeric types are stringified; the agent must parse.
- The keys `block_num`, `block_hash`, `timestamp`, `minute` are stripped from each event because they duplicate top-level meta.
- Upstream `ordinal`, `operation`, `pk`/`composite_pk`, `update_op` are dropped — never surfaced.

When `wrap_envelope` is `true`, the same object is delivered nested under `data`:

```json
{ "stream": "solana-mainnet@swaps", "data": { /* the above */ } }
```

## Stream lifecycle messages

Same connection, separate envelope identified by `"type": "stream"`. Filtered the same way as block payloads.

```
{ "type": "stream", "status": "started",   "stream": "...", "network": "...", "module_hash": "..." }
{ "type": "stream", "status": "completed", ... }
{ "type": "stream", "status": "error",     ..., "message": "..." }
{ "type": "stream", "status": "fatal",     ..., "message": "..." }
{ "type": "stream", "status": "undo",      ..., "last_valid_block": 350000000 }
```

`undo` fires on chain reorganizations. Roll back any state materialized past `last_valid_block`.

## Live SUBSCRIBE / UNSUBSCRIBE / LIST_SUBSCRIPTIONS

After connect, send JSON text frames to mutate the subscription set. Each command is a single object; the server replies with one object.

### Request envelope

```json
{ "method": "<METHOD>", "params": [<string>, ...], "id": <any json> }
```

- `method` — `SUBSCRIBE`, `UNSUBSCRIBE`, or `LIST_SUBSCRIPTIONS`. Case-sensitive.
- `params` — array of `network@stream` selectors. Required for `SUBSCRIBE`/`UNSUBSCRIBE`. Ignored for `LIST_SUBSCRIPTIONS`.
- `id` — echoed on the reply. Use a number to correlate.

### Reply envelope

```json
{ "result": null | [<string>, ...], "id": <echoed> }
{ "error":  "<message>",            "id": <echoed-or-null> }
```

Invalid commands do **not** close the connection.

### Examples

```json
{ "method": "SUBSCRIBE",         "params": ["solana-mainnet@swaps"], "id": 1 }
// -> { "result": null, "id": 1 }

{ "method": "UNSUBSCRIBE",       "params": ["solana-mainnet@swaps"], "id": 2 }
// -> { "result": null, "id": 2 }

{ "method": "LIST_SUBSCRIPTIONS", "id": 3 }
// -> { "result": ["solana-mainnet@swaps"], "id": 3 }
```

### Rules

- `SUBSCRIBE` is idempotent. Re-subscribing is a no-op.
- `UNSUBSCRIBE` silently ignores unknown selectors. To remove a wildcard, pass the exact wildcard form.
- `wrap_envelope` is fixed at upgrade time. To change envelope mode, reconnect.
- `SUBSCRIBE` does not push a snapshot. Only future blocks are delivered.

## Cursors and exact resume

Every block payload includes `cursor`. If you want exactly-once consumption across reconnects, persist the last cursor you successfully processed and reconnect to the upstream Substreams source directly (not this WebSocket — this server has no replay buffer). The cursor is opaque to consumers.

## Heartbeats

The server sends WebSocket ping frames every `SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS` (default 180s). Standard WebSocket clients pong automatically. The server closes connections that don't pong within `SUBSTREAMS_WEBSOCKET_HEARTBEAT_TIMEOUT_SECS` (default 600s).

## Common agent recipes

### "Tail every event from a single stream"

```
wss://<host>/ws/solana-mainnet@swaps
```

Payloads come raw, no envelope. Parse top-level fields directly.

### "Tail two streams and demultiplex"

```
wss://<host>/ws/solana-mainnet@swaps/solana-mainnet@transfers
```

Each frame is `{"stream":"<id>","data":...}`. Switch on `stream` to dispatch.

### "Tail every swaps stream across chains"

```
wss://<host>/ws/*@swaps
```

### "Connect, then narrow"

```
1. Connect to wss://<host>/ws/*@*
2. Receive welcome → inspect `streams[]` to see what's available
3. Send LIST_SUBSCRIPTIONS to confirm current set
4. Send UNSUBSCRIBE / SUBSCRIBE to narrow
```

### "Detect spkg upgrades"

Compare each broadcast's `module_hash` with the one you saw in the welcome message. If they differ, the operator deployed a new spkg — schema may have changed.

## What this server does NOT do

- **No replay buffer.** Disconnected clients miss broadcasts during the gap. Use the `cursor` field to reconnect upstream directly for backfill.
- **No payload transformation.** Field values are pass-through strings from the source DatabaseChanges. Numeric parsing, decimal handling, base58 / hex encoding are the consumer's responsibility.
- **No authentication.** The server itself is open; access control is the operator's deploy concern.
- **No persistence of historical messages.** Once a block is broadcast, it's gone unless a connected subscriber received it.

## When to use this server

Good fit:
- Real-time consumption of Substreams `DatabaseChanges` over WebSocket
- Fan-out to many subscribers from a single Substreams reader
- Cross-chain consumption from a unified URL pattern

Bad fit:
- Historical backfill (use Substreams gRPC directly with the cursor)
- Snapshotting / point-in-time queries (use a SQL sink instead)
- Anything other than `sf.substreams.sink.database.v1.DatabaseChanges` outputs
