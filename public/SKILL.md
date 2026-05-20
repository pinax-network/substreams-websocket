# substreams-websocket — agent guide

This is a Substreams-to-WebSocket fan-out server. It runs one or more `db_out`-style Substreams packages and broadcasts decoded `sf.substreams.sink.database.v1.DatabaseChanges` block payloads as JSON over WebSocket.

If you're an AI agent or programmatic client, this page tells you everything you need to know to connect and consume the stream. Source: <https://github.com/pinax-network/substreams-websocket>.

## Server URL conventions

Stream selectors are `<network>@<table>` (Binance market-streams style). `*` is a wildcard on either side. `<network>` is a chain identifier (e.g. `solana-mainnet`, `ethereum-mainnet`). `<table>` is the DatabaseChanges table emitted by the spkg's `db_out` module (e.g. `swaps`, `transfers`, `spl_transfers`).

Two URL modes:

| URL | Behavior |
|-----|----------|
| `/ws/<a>` | Single channel — raw JSON payload per block. |
| `/ws/<a>/<b>/...` | Multiple channels — every payload wrapped as `{"stream":"<network>@<table>","data":<raw>}`. |
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
      "network": "solana-mainnet",
      "module": "db_out",
      "manifest": "https://.../svm-dex-v0.5.1.spkg",
      "module_hash": "bd388f2e39f5dcc237cfbdb8d6c96d9e5678c797",
      "package_name": "svm_dex",
      "package_version": "v0.5.1",
      "tables": ["swaps"]
    }
  ],
  "subscriptions": ["solana-mainnet@swaps"],
  "wrap_envelope": false
}
```

- `streams` lists every Substreams source the server reads. Each entry is identified by `(network, package_name, package_version, module_hash)` — there is no operator-defined name. The optional `tables` array advertises which DatabaseChanges tables that spkg emits, so clients can build a discovery UI without waiting for blocks.
- `subscriptions` is what this connection will actually receive (filtered set). Selectors are `<network>@<table>` where `<table>` is a DatabaseChanges table emitted by the spkg's `db_out`.
- `wrap_envelope` tells you whether subsequent payloads are wrapped in `{"stream","data"}` or sent raw.

## Block payload shape

One message per `(network, table)` group per block. A spkg that emits both `swaps` and `transfers` produces **two** per-table broadcasts per block.

```json
{
  "network": "solana-mainnet",
  "table": "swaps",
  "block_num": 350000000,
  "block_hash": "Gsk6...",
  "timestamp": "2026-05-13 17:00:00",
  "module_hash": "bd388f2e...",
  "events": [
    {
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

- `network` + `table` — together identify the subscription channel.
- `block_num`, `block_hash`, `timestamp` — block-level metadata. Timestamps are UTC `YYYY-MM-DD HH:MM:SS`. `block_num` is also the resume key for reconnects.
- `module_hash` — canonical 40-hex SHA-1 of the Substreams output module. Use it to detect spkg upgrades.
- `events` — array of rows for this table only, in source order. The per-event `@table` prefix is dropped since the parent payload already names the table.
- All values inside `events[*]` are strings on the wire (per DatabaseChanges proto). Numeric types are stringified; the agent must parse.
- The keys `block_num`, `block_hash`, `timestamp`, `minute` are stripped from each event because they duplicate top-level meta.
- Upstream `ordinal`, `operation`, `pk`/`composite_pk`, `update_op` are dropped — never surfaced.

When `wrap_envelope` is `true`, the same object is delivered nested under `data`:

```json
{ "stream": "solana-mainnet@swaps", "data": { /* the above */ } }
```

## Stream lifecycle messages

Same connection, separate envelope identified by `"type": "stream"`. Lifecycle messages are delivered to **every** connected client regardless of stream subscription — they carry spkg provenance (`package_name`, `package_version`, `module_hash`) so clients can route on their own.

```
{ "type": "stream", "status": "started",   "network": "...", "package_name": "...", "package_version": "...", "module_hash": "..." }
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

## Event filters

Reduce bandwidth by asking the server to drop non-matching events before delivery. Pass `?filter=<url-encoded-json>` on the WebSocket upgrade, or use the live `SET_FILTER` / `CLEAR_FILTER` / `LIST_FILTERS` JSON commands. Filters are scoped per explicit `network@stream` selector. Wildcards always pass everything through.

```
ws://host/ws/solana-mainnet@swaps?filter=%7B%22protocol%22%3A%22raydium_cpmm%22%7D
```

```json
{ "method": "SET_FILTER",
  "params": ["solana-mainnet@swaps", { "protocol": "raydium_cpmm", "user": ["a","b"] }],
  "id": 1 }
```

Semantics: string equality only; fields are AND'd; values within a field are OR'd; events missing the filtered field are dropped. If every event of a block is dropped, the block is skipped for that client. Top-level fields (`block_num`, `network`, `module_hash`) are not filterable.

See [`docs/filters.md`](https://github.com/pinax-network/substreams-websocket/blob/main/docs/filters.md) for full reference + common filter shapes per stream type.

## Reconnects and replay

The server retains a recent time window per spkg on disk (default 600 seconds, controlled by `SUBSTREAMS_WEBSOCKET_REPLAY_SECONDS`). On reconnect, pass `?from_timestamp=<n>` (Unix epoch seconds or UTC `YYYY-MM-DD HH:MM:SS`) to receive every block with `timestamp_seconds > n` from the on-disk window before the live stream resumes.

```
ws://host/ws/solana-mainnet@swaps?from_timestamp=1715619600
ws://host/stream?streams=solana-mainnet@swaps&from_timestamp=2026-05-13T17:00:00Z
```

If `from_timestamp` falls below the oldest retained timestamp, the server emits a `gap` lifecycle message instead and continues live:

```json
{ "type": "stream", "status": "gap",
  "network": "solana-mainnet",
  "requested_block": 100,
  "oldest_buffered_block": 500,
  "reason": "requested block outside replay window" }
```

Use `block_num + 1` from the latest payload you successfully processed. Wildcard selectors (`*@swaps`, `solana-mainnet@*`) skip replay — there is no concrete file to scan. Cursor handling stays internal to the server; clients deal in block numbers only.

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

- **Bounded replay only.** The on-disk replay log holds a `REPLAY_SECONDS` window per spkg (default 600s). For older history, use Substreams gRPC directly with the desired `start_block`.
- **No payload transformation.** Field values are pass-through strings from the source DatabaseChanges. Numeric parsing, decimal handling, base58 / hex encoding are the consumer's responsibility.
- **No authentication.** The server itself is open; access control is the operator's deploy concern.
- **No persistence of historical messages.** Once a block is broadcast, it's gone unless a connected subscriber received it.

## When to use this server

Good fit:
- Real-time consumption of Substreams `DatabaseChanges` over WebSocket
- Fan-out to many subscribers from a single Substreams reader
- Cross-chain consumption from a unified URL pattern

Bad fit:
- Historical backfill (use Substreams gRPC directly with a `start_block`)
- Snapshotting / point-in-time queries (use a SQL sink instead)
- Anything other than `sf.substreams.sink.database.v1.DatabaseChanges` outputs
