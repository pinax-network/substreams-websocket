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
- We bumped the tonic decoded-message cap to **64 MiB** (default 4 MiB) because Solana SPL transfers blocks routinely exceed 4 MiB after gzip decompression.
- HTTP/2 + TCP keepalive enabled to survive long-idle gRPC streams getting reaped by upstream proxies (`h2 protocol error: ... ConnectionReset`).

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

See [`public/SKILL.md`](../public/SKILL.md) for the resulting on-wire shape.

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
