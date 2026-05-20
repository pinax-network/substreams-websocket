# Replay log

Persistent per-spkg JSONL log for client reconnects. Lets a WebSocket client reconnect after a server restart, container redeploy, or network blip and receive every block it missed (within the retention window) without going back to Substreams gRPC.

## File layout

One file per `(network, package_name, package_version, module_hash)`. Each line is the **whole** Substreams block JSON, including events for every `@table` the spkg emits in that block.

```
SUBSTREAMS_WEBSOCKET_REPLAY_DIR/
  solana-mainnet-svm_dex@v0.5.1-bd388f2e.jsonl
  solana-mainnet-svm_transfers@v0.3.0-673da4a7.jsonl
  ethereum-mainnet-evm_dex@v0.7.0-aaaaaaaa.jsonl
```

- Append-only via `O_APPEND`. One JSON object per line. `timestamp_seconds` (Unix epoch, integer) is parsed for resume + reorg truncation.
- Stored shape is the *unrouted* block (events still carry `@table`). At read time, the server filters by `@table` and strips it before sending per-table sub-blocks to clients — matching the live broadcast shape.
- No fsync per write. Kernel page cache + periodic flush. Hard power loss may drop the last few seconds; Substreams cursor resume re-emits those on the next read.

## Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `SUBSTREAMS_WEBSOCKET_REPLAY_SECONDS` | `600` | Retention window in seconds per spkg. `0` disables the replay log entirely. |
| `SUBSTREAMS_WEBSOCKET_REPLAY_DIR` | `./replay` | Directory for JSONL files. Mount on the same volume as `cursors/` for PaaS deploys. |

Window is chain-agnostic — same setting works across Solana (~400ms/block), Ethereum (~12s/block), TVM, etc. Default 600s (10 minutes) covers a typical Railway / Fly redeploy.

## Trim policy

Lazy. Trim fires once the window (`newest_seen_ts - oldest_seen_ts`) grows past `REPLAY_SECONDS + 10% headroom`. Trim rewrites the file dropping any line older than `newest_seen_ts - REPLAY_SECONDS`. With 10% headroom the file rewrites once per `headroom` seconds, not on every append. Rewrite is atomic via `tmp file + rename`.

## Reconnect protocol

Clients pass `?from_timestamp=<n>` on either URL mode. The value is either:

- A Unix epoch integer (seconds): `?from_timestamp=1715619600`
- A UTC timestamp string: `?from_timestamp=2026-05-13%2017:00:00` (URL-encode the space) or `?from_timestamp=2026-05-13T17:00:00Z`

```
ws://host/ws/solana-mainnet@swaps?from_timestamp=1715619600
ws://host/stream?streams=solana-mainnet@swaps&from_timestamp=1715619600
```

Server behavior, per subscribed `<network>@<table>` selector:

- Scans every `<network>-*.jsonl` in the directory (multiple spkgs can feed the same table; their replay logs are merged on read).
- For each line with `timestamp_seconds > n`, filters `events[]` to rows whose `@table == <table>`. Strips `@table` from each event. Adds top-level `table` to the payload. Sends if any events survived.
- **`n` in window** (`n >= oldest_retained_timestamp`): replay matching blocks, oldest first, before live takes over.
- **`n` below window** (`n < oldest_retained_timestamp`): emit one `gap` lifecycle message, then continue live.
- **`n` at or above newest retained**: no replay, continue live.
- **Wildcard selector** (`*@swaps`, `solana-mainnet@*`, `*@*`): skipped — no single `(network, table)` to anchor replay on. Wildcards always start live.

`gap` envelope:

```json
{ "type": "stream", "status": "gap",
  "network": "solana-mainnet",
  "table": "swaps",
  "requested_timestamp": 1715000000,
  "oldest_buffered_timestamp": 1715619300,
  "reason": "requested timestamp outside replay window" }
```

Clients seeing `gap` should either accept the hole or open a Substreams gRPC stream from `requested_timestamp` to backfill before consuming the live feed.

## Why timestamp, not block number

Block numbers are per-chain. A Solana block at slot 350,000,000 has no relationship to an Ethereum block at height 22,000,000. The same `block_num` value would mean different things on different networks, and combining replay across chains under a single selector (`*@swaps`) was impossible to anchor.

Unix epoch seconds is a chain-agnostic, monotonic time index. A client tracking activity across `solana-mainnet@swaps` and `ethereum-mainnet@swaps` uses one `from_timestamp` value for both.

## Reorg handling

On `BlockUndoSignal`, the server truncates the affected spkg's replay log to drop every line with `block_num > last_valid_block`. Replay never serves undone forks. Same truncation also rewrites the cursor file via the existing `BlockUndoSignal.last_valid_cursor` path.

## What it does not do

- **No durable guarantee on power loss.** No fsync per write. Last few blocks may be lost on hard kernel crash.
- **No cross-replica fan-out.** Replay is per-process on local disk. Multiple containers behind a load balancer each have their own log; clients hitting different replicas get different windows.
- **No historical backfill.** Beyond `REPLAY_SECONDS`, use Substreams gRPC directly.
- **No wildcard replay.** `*@table`, `network@*`, `*@*` do not replay — they always start live.

## PaaS deployment

Mount a volume at `SUBSTREAMS_WEBSOCKET_REPLAY_DIR` so the log survives container restarts. Same volume as `cursors/` is fine. See [`railway.md`](railway.md) for the Railway-specific recipe.

## Operational notes

- Wipe one spkg's replay log: `rm <REPLAY_DIR>/<network>-<package_name>@<package_version>-<module_hash>.jsonl`.
- Inspect: `tail <REPLAY_DIR>/<file>.jsonl | jq .`.
- Disk usage estimate: `spkgs × (REPLAY_SECONDS / avg_block_time) × avg_block_bytes`. For 30 Solana spkgs × (600s / 0.4s) × 5 KB ≈ ~225 MB.
