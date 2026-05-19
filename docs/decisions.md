# Design decisions

Non-obvious choices made during development. Each entry: what we picked, what we rejected, and why. The "why" is the load-bearing part — if it ages out, revisit the choice.

## DatabaseChanges-only

We accept exactly one Substreams output type: `sf.substreams.sink.database.v1.DatabaseChanges`. Bespoke per-module decoders (SVM swaps, EVM transfers, etc.) were removed.

Rejected: a registry of decoders keyed by `Module.output.type`.

Why: DatabaseChanges is the de-facto sink-side schema in the Substreams ecosystem. Anything we'd hand-write as a decoder is already expressible as a DatabaseChanges-emitting module. Keeping one wire format means one normalization path, one set of tests, one mental model. Adding a new chain or new event is a manifest change, not a Rust change.

## `@table` row prefix, drop everything else `@`-prefixed

Each row is emitted with a `@table` key (`@table=swaps`). Other meta (`block_num`, `block_hash`, `timestamp`, `minute`) live at the top level of the envelope, not on each row, and have no `@` prefix.

Rejected: prefixing every meta field with `@` to namespace it from user columns.

Why: `@table` is the only meta that *must* live per-row (multi-table modules emit heterogeneous rows in one block). The rest is block-scoped and lifting it to the envelope is cheaper than repeating it. The single `@` collision guard is enough to prevent user-column conflicts in practice.

## Required explicit `subscribe=` on WS connect

Connecting without a `subscribe` query param (on `/ws/<a>/<b>` paths the route itself defines the subs; on `/stream` clients must list them) yields zero traffic, not all-streams.

Rejected: default-to-all-streams when no subscription is provided.

Why: defaulting to all-streams turns a fat-fingered connect into a firehose. We'd rather an empty connection than an accidental full feed — clients who want everything can pass `*` explicitly.

## Binance URL conventions

`<network>@<stream>` selectors, `/ws/<a>/<b>`, `/stream?streams=<a>/<b>`, JSON `{method, params, id}` commands. See [`binance-websocket.md`](binance-websocket.md).

Rejected: REST-style `/ws/<network>/<stream>`, GraphQL subscriptions, server-sent events.

Why: Binance's convention is the most widely known WebSocket market-data shape. Any developer who has integrated a Binance client can map onto ours without reading the docs. The cost of mirroring is near-zero; the cost of inventing our own is paid by every integrator.

## `serde_json` with `preserve_order`

Output JSON keeps DatabaseChanges field order from the wire, not alphabetical.

Rejected: default BTreeMap ordering.

Why: humans reading the JSON want the columns in the order the module emitted them (typically pk → indexed cols → payload). Sort order makes diffs noisier and hides intent.

## 64 MiB gRPC decode cap

Bumped from tonic's 4 MiB default.

Rejected: keeping default and asking providers to split.

Why: Solana SPL-transfer blocks routinely exceed 4 MiB after gzip decompression. The cap is per-message; oversizing it costs nothing on normal blocks and avoids a hard failure on the fat ones.

## HTTP/2 + TCP keepalive

30s HTTP/2 keepalive, 20s timeout, keepalive while idle, TCP keepalive at 30s.

Rejected: relying on the default reconnect loop alone.

Why: Pinax (and any upstream behind a cloud LB) reaps idle gRPC streams with `h2 protocol error: ConnectionReset`. Keepalive frames keep the path warm and turn a "silent stall + reconnect storm" into "no stall at all."

## No client-side auth on the WebSocket

Anyone who hits the server gets every configured stream.

Rejected: per-token allowlists, JWT verification, IP allowlists in-process.

Why: this server is a fan-out, not a security boundary. Auth belongs in front (Cloudflare Access, nginx auth_request, Tailscale). Bolting auth in here would either duplicate the upstream's model badly or constrain operators who already have a reverse proxy.

## Single binary + inline TOML for PaaS

`SUBSTREAMS_WEBSOCKET_STREAMS_TOML` env var wins over the file path.

Rejected: requiring a writable filesystem for the streams config.

Why: Railway / Fly / Heroku deploys don't have a place to drop a config file without baking it into the image. Inline TOML lets the platform's env-var UI be the config plane.
