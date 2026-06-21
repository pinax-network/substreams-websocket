# Substreams + DatabaseChanges

Background and external references for the Substreams plumbing in this repo. Audience: contributors making non-trivial changes to the decoder, gRPC client, or module-hash logic.

## What Substreams is

A streaming-first ETL on top of Firehose. Each module is a small WASM program that consumes blocks and emits structured output. Modules compose into a DAG; the leaf modules feed sinks.

- Project: <https://substreams.streamingfast.io/>
- Concepts overview: <https://substreams.streamingfast.io/concepts>
- Pinax Solana endpoint we test against: `https://solana.substreams.pinax.network:443`

## gRPC service shape

We use `sf.substreams.rpc.v2.Stream/Blocks`. The proto definitions are not vendored: `build.rs` imports them from the buf.build registry (`buf build <module> --as-file-descriptor-set`), pinned per module in `BUF_MODULES`. Never copy or hand-edit proto files — to upgrade, bump the pinned ref in `build.rs`.

- BSR module: <https://buf.build/streamingfast/substreams>
- Service proto source: <https://github.com/streamingfast/substreams/blob/develop/proto/sf/substreams/rpc/v2/service.proto>
- We require **gzip request compression** — Pinax rejects uncompressed gRPC with `400 Bad Request` and body `no supported compression found.`
- We bumped the tonic decoded-message cap to **64 MiB** (default 4 MiB) because Solana SPL transfers blocks routinely exceed 4 MiB after gzip decompression. The cap is configurable via `SUBSTREAMS_MAX_DECODE_MESSAGE_BYTES` (global) or per-stream `max_decode_message_bytes`; raise it for chains whose per-block output exceeds 64 MiB (e.g. Hyperliquid hypercore, which otherwise fails every block with `Error decompressing: size limit, of 67108864 bytes, exceeded`).
- HTTP/2 + TCP keepalive enabled to survive long-idle gRPC streams getting reaped by upstream proxies (`h2 protocol error: ... ConnectionReset`).
- **HTTP/2 flow control is tuned for large streaming messages.** hyper's default initial window is 64 KiB, which caps a single gRPC stream at roughly `window / RTT` (a 64 KiB window over a 50 ms RTT is only ~1.3 MiB/s). DatabaseChanges blocks for busy chains run to several MiB after decompression, so on a high-RTT link to the endpoint the default window can stall delivery between `WINDOW_UPDATE` round-trips. We raise the per-stream initial window to 8 MiB (`STREAM_WINDOW_BYTES`) and enable adaptive flow control (`http2_adaptive_window`) so the connection window grows from a BDP estimate. This is defensive hardening — it matters when the WS server sits far (network-wise) from the Substreams endpoint or pulls very large blocks.

## Head latency / lag

The WebSocket feed is the *live edge* (see [`decisions.md`](decisions.md)). Head lag is `now − block_timestamp` at the moment a block is received. The server already exports this for the WS side as the Prometheus gauge **`substreams_websocket_head_block_time_drift`** (labelled by `stream`/`network`/`table`/`spkg`/`endpoint`), and logs `last_drift_secs` / `max_drift_secs` in the periodic `block hot-path profile` line. Compare that gauge against the same `now − block_timestamp` measured by a direct Substreams consumer (`substreams run … -o clock` prints each block's `age`).

**Where the lag actually lives (measured).** A live A/B — the public WS (`wss://ws.pinax.network/ws/<net>@<table>`) against a direct `substreams run` from the same endpoint, matched block-by-block — shows the WS mirrors the upstream Substreams stream **within ~100 ms**, even on heavy streams (Solana SPL transfers: ~300 KB/block avg, 740 KB max, ~2.5 blocks/s). The dominant lag is **upstream** (chain → Firehose → Substreams), and for high-throughput chains it is **bursty**: in one 90 s Solana window head lag was p50 ≈ 2.4 s, p90 ≈ 13.8 s, max ≈ 18.5 s. A report like "WS 13 s vs SS 5 s" is almost always that one bursty distribution sampled at two different instants — not the WS adding a fixed offset. Confirm with the matched A/B before assuming the server is at fault.

Things that are commonly blamed but, in testing against the Pinax endpoints, do **not** move head lag:

- **`SUBSTREAMS_PRODUCTION_MODE`** — production and development mode delivered the live head within ~20 ms of each other (matched, same endpoint). Production mode's parallel back-processing helps *backfill*, which this server does not do (live-only, no replay log), so leave it `false` to match development-mode semantics — but it is not a latency lever at the head.
- **`SUBSTREAMS_FINAL_BLOCKS_ONLY`** — `true` makes the feed wait for finality, so it trails head by the chain's finality depth *by design*. Keep it `false` for the lowest-latency live feed. (This is a real offset, but a chosen one — not a bug.)

## DatabaseChanges sink

We accept only `sf.substreams.sink.database.v1.DatabaseChanges` as the module output type. Anything else fails fast at startup.

- BSR module: <https://buf.build/streamingfast/substreams-sink-database-changes> (imported by `build.rs`, pinned in `BUF_MODULES`)
- Upstream proto source: <https://github.com/streamingfast/substreams-sink-database-changes/blob/develop/proto/sf/substreams/sink/database/v1/database.proto>

### Wire shape (raw, before we normalize)

```
DatabaseChanges {
  table_changes: [
    TableChange {
      table: "swaps",
      ordinal: 0,
      operation: OPERATION_CREATE,
      pk / composite_pk: ...,
      fields: [
        Field { name: "input_amount", value: "1287000000", update_op: UPDATE_OP_SET },
        ...
      ]
    },
    ...
  ]
}
```

### What we drop on the way out

- `ordinal`, `operation`, `pk`/`composite_pk`, `update_op` — never surfaced
- Field values keep their string form (DatabaseChanges contract; numeric parsing is the consumer's job)
- Per-row keys that duplicate top-level meta (`block_num`, `block_hash`, `timestamp`, `minute`) are stripped from each event
- Events with no surviving columns after the strip (e.g. a `blocks` row whose only fields were the four above) are dropped entirely

See [`skills/SKILL.md`](../skills/SKILL.md) for the resulting on-wire shape.

## Module hash

We use the canonical Substreams module hash to key cursor files. Algorithm sources:

- Go reference (authoritative): [`streamingfast/substreams/manifest/signature.go`](https://github.com/streamingfast/substreams/blob/develop/manifest/signature.go)
- JS port (cross-checked): [`substreams-js/.../create-module-hash.ts`](https://github.com/substreams-js/substreams-js/blob/main/packages/core/src/manifest/signature/create-module-hash.ts)

Our implementation lives at [`src/module_hash.rs`](../src/module_hash.rs). It is a faithful port — same SHA-1 inputs, same field-name strings, same ancestor-by-shortest-path enumeration.

Cross-check by running `substreams info <spkg>` from the official CLI; the `Hash` column should match our `module_hash` field exactly.

## Module-output type URL

Two forms exist in the wild:

- Bare: `proto:sf.substreams.sink.database.v1.DatabaseChanges` (this is what `Module.output.type` carries in the manifest)
- Any-URL: `type.googleapis.com/sf.substreams.sink.database.v1.DatabaseChanges` (Google.Protobuf.Any convention)

We check the bare form against `Module.output.type` at startup. Both forms are exported as `SUPPORTED_OUTPUT_TYPE` and `SUPPORTED_OUTPUT_TYPE_URL` from `src/decoder.rs`.

## Cursor

Every `BlockScopedData` carries an opaque `cursor` string. We persist the latest cursor per `(network, module_hash)` to disk in `SUBSTREAMS_WEBSOCKET_CURSORS_DIR`. On restart, the cursor becomes the `start_cursor` of the next `Blocks` request, resuming exactly where we left off.

`BlockUndoSignal.last_valid_cursor` is also persisted — on a chain reorg, the new cursor is the safe resume point.

See [`cursors-and-resume.md`](cursors-and-resume.md) for what "exact resume" does and does not mean.
