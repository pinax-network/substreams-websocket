# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A single-binary Rust server that consumes Substreams gRPC streams (one per configured spkg), decodes their `sf.substreams.sink.database.v1.DatabaseChanges` output into per-table JSON, and fans the result out to subscribed WebSocket clients. Clients subscribe by `<network>@<table>` (Binance market-stream URL convention).

`protoc` and Rust 1.90+ are required to build. `build.rs` compiles the `proto/sf/...` definitions via `tonic-prost-build` on every build.

## Common commands

```bash
cargo fmt --all -- --check        # CI runs this; must pass
cargo test --workspace --locked   # full test suite — no live endpoint required
cargo test --workspace <name>     # single test by name (e.g. `rejects_duplicate_network_manifest_module`)
cargo build --bin substreams-websocket --locked
cargo check --workspace

# run the server locally (reads .env + ./streams.toml)
./target/debug/substreams-websocket serve

# one-shot debug stream (no WS, prints decoded events)
./target/debug/substreams-websocket stream <spkg-url-or-path> db_out --endpoint <grpc-url> --max-messages 5
```

The test suite injects a synthesized `DatabaseChanges` block through the broadcast pipeline and verifies a real WebSocket client receives the expected JSON — there is no live gRPC dependency.

## Architecture

The `lib.rs` re-exports a small public surface; everything wires together in `src/server.rs::serve_with_shutdown`. Read it first when changing server behavior.

### Module map

- `src/main.rs` — Clap CLI + env mapping. Builds `Config` from `streams.toml` (or inline `SUBSTREAMS_WEBSOCKET_STREAMS_TOML` env var, which wins) and per-stream defaults. Two subcommands: `serve`, `stream`.
- `src/config.rs` — `Config`, `StreamConfig`, `SubstreamsConfig`, `WebSocketConfig`, `ReplayConfig`. Pure data + `Config::validate()`. Validation rejects duplicate `(network, manifest, module)` triples.
- `src/substreams.rs` — gRPC client. JWT exchange (`api_key → bearer`), HTTP/2 + TCP keepalive tuning, 64 MiB decode cap, package loading from local path or HTTPS, `StreamEvent` enum.
- `src/module_hash.rs` — Canonical Substreams module-hash computation (matches `substreams info` output). Identity key for cursor + replay files.
- `src/decoder.rs` — Decodes `DatabaseChanges` → `DatabaseChangesBlockMessage`. Each row carries an `@table` field; block-level meta (`block_num`, `block_hash`, `timestamp`, `minute`) is hoisted to the envelope and stripped from rows.
- `src/event_filter.rs` — Server-side row filtering by client-supplied `key=value` constraints (capped by `MAX_FILTER_FIELDS` / `MAX_FILTER_VALUES`).
- `src/cursor.rs` — Persistent cursor store. Files: `cursors/<network>-<package_name>@<package_version>-<module_hash>.cursor`. Atomic write via tmp + rename, no fsync.
- `src/replay.rs` — JSONL per-spkg replay log for client reconnect (`?from_timestamp=`). Append-only, lazy trim by `timestamp_seconds` window.
- `src/server.rs` — Axum routes, per-client mpsc fan-out, subscription bookkeeping, graceful shutdown. Routes mirror Binance: `/ws/<network>@<table>/...`, `/stream?streams=<a>/<b>`, plus `/SKILL.md`, `/llms.txt`, `/streams`, `/healthz`, landing page.
- `proto/sf/...` — Vendored Substreams + DatabaseChanges proto definitions, compiled by `build.rs`.
- `public/` — Served at runtime: `index.html` (landing), `SKILL.md` (on-wire message reference — the canonical client contract), `llms.txt`, `favicon.png`.

### Stream identity, not stream names

There is no operator-supplied stream name. Identity is derived from the loaded `.spkg`'s `Package.package_meta[0]` (`name`, `version`) plus the canonical `module_hash`. A missing `package_meta` fails fast at startup. This anchors cursor and replay filenames to the spkg, so renaming a TOML entry has no effect and upgrading a spkg writes to a fresh file.

Clients subscribe by `<network>@<table>` where `<table>` is the DatabaseChanges table emitted by `db_out` — not by anything in `streams.toml`.

### Data flow (per stream)

1. `prepare_streams` loads each `.spkg` in parallel, computes `module_hash`, builds `StreamMeta` for the welcome message. A failed load is recorded but does not abort the server — the per-stream task surfaces the error to clients.
2. `spawn_streams` launches one tokio task per stream that connects via `SubstreamsClient`, resumes from the persisted cursor, and pushes each `StreamEvent::Block` through `decode_database_changes`.
3. The decoder yields a `DatabaseChangesBlockMessage`. The server groups rows by `@table`, applies per-client filters, appends the unrouted block to the replay log, then `try_send`s to each subscribed client's mpsc buffer.
4. The cursor file is rewritten on every block and undo signal.
5. Reorgs (`StreamEvent::Undo`) broadcast an `undo` envelope — clients roll back their own state; the server never re-emits the undone blocks.

### Backpressure + shutdown

Per-client outbound is a bounded mpsc. `try_send` failures increment a per-client counter; once it reaches `slow_client_drop_limit` (default 100, `0` disables) the connection is force-closed with `Close(1013 "slow client backpressure")`.

On SIGTERM/SIGINT: `/healthz` flips to 503 so a reverse proxy can drain new connections, then every client receives a `Close` frame, then axum waits up to `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS` for clients to disconnect before tearing down. Order matters — see `serve_with_shutdown` in `src/server.rs`.

## Load-bearing design decisions

`docs/decisions.md` is the authoritative log. Highlights:

- **DatabaseChanges is the only accepted output type.** No per-module decoders. Anything new is a manifest change, not a Rust change.
- **No client-side auth on the WebSocket.** Fan-out is not a security boundary; put auth in front (Cloudflare Access, nginx auth_request, etc.).
- **`@table` per-row, other meta block-scoped.** Only `@`-prefixed key on rows is `@table`.
- **Connecting without a subscription yields zero traffic, not all-streams.** A `*` wildcard must be explicit.
- **`serde_json` with `preserve_order`** — column order matches the wire, not alphabetical.
- **Inline `SUBSTREAMS_WEBSOCKET_STREAMS_TOML` env wins over file path** — for Railway/Fly/Heroku where the filesystem isn't writable.

## Things to know before changing client-facing JSON

`public/SKILL.md` is the on-wire contract (session message, block envelope, undo envelope, command envelope, error shapes). It is served at `GET /SKILL.md` at runtime and is what clients read. Any change to broadcast or response JSON requires updating this file — and probably `docs/binance-websocket.md` if you change URL or command conventions.

## Extended documentation

`docs/` is non-trivial and worth checking when a task touches its area:

- `docs/auth.md` — three auth modes (api-key→JWT, raw bearer, header passthrough).
- `docs/backpressure.md` — slow-client drop semantics.
- `docs/cursors-and-resume.md` — file naming, atomic writes, what "exact resume" actually means.
- `docs/replay.md` — JSONL replay log format and `?from_timestamp=` reconnect protocol.
- `docs/filters.md` — client-supplied event filters.
- `docs/graceful-shutdown.md` — the drain sequence above, in more detail.
- `docs/envoy.md`, `docs/railway.md` — reverse proxy + PaaS deployment recipes.
- `docs/binance-websocket.md` — URL convention rationale.
- `docs/substreams.md` — module-hash computation, gRPC tuning.
