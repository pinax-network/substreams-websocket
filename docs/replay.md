# Replay log

Persistent per-stream JSONL log for client reconnects. Lets a WebSocket client reconnect after a server restart, container redeploy, or network blip and receive every block it missed (within the retention window) without going back to Substreams gRPC.

## File layout

```
SUBSTREAMS_WEBSOCKET_REPLAY_DIR/
  solana-mainnet@swaps.jsonl
  solana-mainnet@transfers.jsonl
  ethereum-mainnet@swaps.jsonl
```

- One file per `(network, stream)`.
- Append-only via `O_APPEND`. One JSON object per line. `block_num` is parsed for resume + reorg truncation.
- No fsync per write. Kernel page cache + periodic flush. Hard power loss may drop the last few seconds; Substreams cursor resume re-emits those on the next read.

## Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `SUBSTREAMS_WEBSOCKET_REPLAY_BLOCKS` | `1000` | Rows retained per stream. `0` disables the replay log entirely (no files created). |
| `SUBSTREAMS_WEBSOCKET_REPLAY_DIR` | `./replay` | Directory for JSONL files. Mount on the same volume as `cursors/` for PaaS deploys. |

Single global knob applied to every stream. Block count, not bytes — chain speed determines wall-clock retention:

- Solana ~400ms/block → 1000 blocks ≈ 6 min
- Ethereum ~12s/block → 1000 blocks ≈ 3 hours

Both cover a typical container redeploy. Tune down if memory or disk pressure matters.

## Trim policy

Lazy. Threshold = `REPLAY_BLOCKS + max(ceil(REPLAY_BLOCKS * 0.10), 1)`. Once the file reaches the threshold, it is rewritten to keep the most recent `REPLAY_BLOCKS` lines. With 10% headroom, the file rewrites once per `headroom` appends, not on every append. Rewrite is atomic via `tmp file + rename`.

## Reconnect protocol

Clients pass `?from_block=<n>` on either URL mode:

```
ws://host/ws/solana-mainnet@swaps?from_block=350000123
ws://host/stream?streams=solana-mainnet@swaps&from_block=350000123
```

Server behavior, per subscribed `network@stream` selector:

- **`n` in window** (`n + 1 >= oldest_retained_block`): replay every line with `block_num > n`, oldest first, before live stream takes over.
- **`n` below window** (`n + 1 < oldest_retained_block`): emit one `gap` lifecycle message, then continue live.
- **`n` above newest retained**: no replay, continue live.
- **Wildcard selector** (`*@swaps`, `solana-mainnet@*`): skipped — no concrete file to scan. Wildcards always get live-only.

`gap` envelope:

```json
{ "type": "stream", "status": "gap",
  "stream": "swaps", "network": "solana-mainnet",
  "requested_block": 100,
  "oldest_buffered_block": 500,
  "reason": "requested block outside replay window" }
```

Clients seeing `gap` should either accept the hole or open a Substreams gRPC stream from `requested_block` to backfill before consuming the live feed.

## Reorg handling

On `BlockUndoSignal`, the server truncates each affected stream's replay log to drop every line with `block_num > last_valid_block`. Replay never serves undone forks. Same truncation also rewrites the cursor file via the existing `BlockUndoSignal.last_valid_cursor` path.

## What it does not do

- **No durable guarantee on power loss.** No fsync per write. Last few blocks may be lost on hard kernel crash.
- **No cross-replica fan-out.** Replay is per-process on local disk. Multiple containers behind a load balancer each have their own log; clients hitting different replicas get different windows. Cross-replica replay requires a shared store (Redis stream, Kafka, NATS JetStream) — out of scope.
- **No historical backfill.** Beyond `REPLAY_BLOCKS`, use Substreams gRPC directly.
- **No wildcard replay.** `*@stream` and `network@*` selectors do not replay — they always start live.

## PaaS deployment

Mount a volume at `SUBSTREAMS_WEBSOCKET_REPLAY_DIR` so the log survives container restarts. Same volume as `cursors/` is fine. See [`railway.md`](railway.md) for the Railway-specific recipe.

## Operational notes

- Wipe one stream's replay log: `rm <REPLAY_DIR>/<network>@<stream>.jsonl`.
- Inspect: `tail <REPLAY_DIR>/<network>@<stream>.jsonl | jq .`.
- Disk usage estimate: `streams × REPLAY_BLOCKS × avg_block_bytes`. For 30 Solana streams × 1000 × 5 KB = ~150 MB.
