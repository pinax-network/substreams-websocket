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
| `SUBSTREAMS_WEBSOCKET_REPLAY_SECONDS` | `3600` | Retention window in seconds per spkg. `0` disables the replay log entirely. |
| `SUBSTREAMS_WEBSOCKET_REPLAY_DIR` | `./replay` | Directory for JSONL files. Mount on the same volume as `cursors/` for PaaS deploys. |

Window is chain-agnostic — same setting works across Solana (~400ms/block), Ethereum (~12s/block), TVM, etc. Default 3600s (1 hour) covers a typical Railway / Fly redeploy plus cron-like consumers (a 15-minute alerter or hourly indexer) that wake well past a 10-minute window. Tune down on small PaaS volumes — see disk usage below.

### Per-tier retention is a proxy concern

There is intentionally no per-subscriber or per-tier retention knob. The server has no client auth (fan-out is not a security boundary — see [`decisions.md`](decisions.md)); a single global window is retained and every subscriber picks any point within it via `?from_timestamp=`. Gate access to a longer window in front of the server (Cloudflare Access, nginx `auth_request`), not here.

## Trim policy

Lazy. Trim fires once the window (`newest_seen_ts - oldest_seen_ts`) grows past `REPLAY_SECONDS + 10% headroom`. Trim rewrites the file dropping any line older than `newest_seen_ts - REPLAY_SECONDS`. With 10% headroom the file rewrites once per `headroom` seconds, not on every append. Rewrite is atomic via `tmp file + rename`.

## Reconnect protocol

Clients resume by passing **one** of two mutually exclusive query parameters on either URL mode:

- `?from_timestamp=<n>` — chain-agnostic, works for any selector. The value is either a Unix epoch integer (`?from_timestamp=1715619600`) or a UTC timestamp string (`?from_timestamp=2026-05-13%2017:00:00`, URL-encode the space; or `?from_timestamp=2026-05-13T17:00:00Z`). Replays blocks with `timestamp_seconds > n`.
- `?from_block=<n>` — per-chain block number. Replays blocks with `block_num > n`. Because block numbers mean different things on different chains, this is **only accepted for a single concrete `<network>@<table>` selector**; a wildcard (`*@…`, `…@*`) or multi-selector connection returns `400`. Resume the next block with `block_num + 1` from the last payload you processed.

Passing both, or `from_block` with a wildcard/multi-selector, is a `400`.

```
ws://host/ws/solana-mainnet@swaps?from_timestamp=1715619600
ws://host/stream?streams=solana-mainnet@swaps&from_timestamp=1715619600
ws://host/ws/solana-mainnet@swaps?from_block=350000000
```

Server behavior, per subscribed `<network>@<table>` selector (`n` is the resume cursor in the chosen unit):

- Scans every `<network>-*.jsonl` in the directory (multiple spkgs can feed the same table; their replay logs are merged on read).
- For each line past `n` (`timestamp_seconds > n` or `block_num > n`), filters `events[]` to rows whose `@table == <table>`. Strips `@table` from each event. Adds top-level `table` to the payload. Sends if any events survived. Block-mode results are ordered ascending by `block_num`.
- **`n` in window** (`n >= oldest_retained`): replay matching blocks, oldest first, before live takes over.
- **`n` below window** (`n < oldest_retained`): emit one `gap` lifecycle message, then continue live.
- **`n` at or above newest retained**: no replay, continue live.
- **Wildcard selector** (`*@swaps`, `solana-mainnet@*`, `*@*`): replays too (timestamp mode). `from_timestamp` is chain-agnostic, so a `*` resolves against every matching replay file and each retained block is expanded into concrete per-`network@table` frames (one frame per matched table), oldest first, before live takes over. `oldest_retained` for the gap check is the earliest timestamp across every matched file. Replayed frames carry their concrete `network`/`table` (and, in wrap mode, a concrete `network@table` envelope) — never the wildcard. The per-stream `tables` allowlist is honored for free: only allowed tables were ever written to the log, so expanding `*` never surfaces a dropped table. **Note:** wildcard replay is `from_timestamp` only — `from_block` is per-chain and rejected for wildcard/multi-selector connections.

`gap` envelope — the fields match the resume unit you used:

```json
// from_timestamp
{ "type": "stream", "status": "gap",
  "network": "solana-mainnet", "table": "swaps",
  "requested_timestamp": 1715000000,
  "oldest_buffered_timestamp": 1715619300,
  "reason": "requested timestamp outside replay window" }

// from_block
{ "type": "stream", "status": "gap",
  "network": "solana-mainnet", "table": "swaps",
  "requested_block": 100,
  "oldest_buffered_block": 350000000,
  "reason": "requested block outside replay window" }
```

Clients seeing `gap` should either accept the hole or open a Substreams gRPC stream from the requested point to backfill before consuming the live feed.

## Timestamp vs block number

Both resume keys are first-class; pick by how you track progress.

`?from_timestamp=` is the chain-agnostic default. A Solana block at slot 350,000,000 has no relationship to an Ethereum block at height 22,000,000, so block numbers can't anchor a cross-chain resume — but Unix epoch seconds can. A client tracking `solana-mainnet@swaps` and `ethereum-mainnet@swaps` uses one `from_timestamp` for both, and it's the only option for wildcard/multi-network selectors.

`?from_block=` exists because most consumers already track `block_num + 1` from the last payload they processed — resuming by block skips a chain-by-chain block↔timestamp mapping step. It's scoped to a single concrete `<network>@<table>` since the block axis is per-chain.

## Reorg handling

On `BlockUndoSignal`, the server truncates the affected spkg's replay log to drop every line with `block_num > last_valid_block`. Replay never serves undone forks. Same truncation also rewrites the cursor file via the existing `BlockUndoSignal.last_valid_cursor` path.

## What it does not do

- **No durable guarantee on power loss.** No fsync per write. Last few blocks may be lost on hard kernel crash.
- **No cross-replica fan-out.** Replay is per-process on local disk. Multiple containers behind a load balancer each have their own log; clients hitting different replicas get different windows.
- **No historical backfill.** Beyond `REPLAY_SECONDS`, use Substreams gRPC directly.

## PaaS deployment

Mount a volume at `SUBSTREAMS_WEBSOCKET_REPLAY_DIR` so the log survives container restarts. Same volume as `cursors/` is fine. See [`railway.md`](railway.md) for the Railway-specific recipe.

## Operational notes

- Wipe one spkg's replay log: `rm <REPLAY_DIR>/<network>-<package_name>@<package_version>-<module_hash>.jsonl`.
- Inspect: `tail <REPLAY_DIR>/<file>.jsonl | jq .`.
- Disk usage estimate: `spkgs × (REPLAY_SECONDS / avg_block_time) × avg_block_bytes`. Scales linearly with the window. For 30 Solana spkgs × (3600s / 0.4s) × 5 KB ≈ ~1.35 GB at the default 3600s (the same fleet at the old 600s default was ≈ ~225 MB). On a small PaaS volume, lower `SUBSTREAMS_WEBSOCKET_REPLAY_SECONDS` accordingly.
