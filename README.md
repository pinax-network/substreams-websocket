# substreams-websocket

Stream decoded Substreams `DatabaseChanges` block outputs over a single WebSocket fan-out server. Configure one or more `(network, name)` streams in a TOML file, point them at any `db_out`-style Substreams package, and every connected WebSocket client receives the flattened JSON for every block — across SVM, EVM, and any future chain that emits `sf.substreams.sink.database.v1.DatabaseChanges`.

- **One generic decoder** for every supported chain. No bespoke proto per package.
- **Resume on restart** — per-stream cursors are persisted on disk and replayed automatically.
- **Cross-chain identity** — streams are addressed by `(network, stream-name)`, so `(solana-mainnet, swaps)` and `(ethereum-mainnet, swaps)` coexist cleanly.
- **JWT auth** built in for Pinax/StreamingFast endpoints.
- **Prebuilt tarballs** for Linux x86_64 / aarch64 and macOS x86_64 / aarch64.

---

## Install

### Pre-built binary (recommended)

Grab the tarball that matches your platform from the [latest release](https://github.com/pinax-network/substreams-websocket/releases/latest):

```bash
# Linux x86_64
curl -L -o substreams-websocket.tar.gz \
  https://github.com/pinax-network/substreams-websocket/releases/latest/download/substreams-websocket-linux-x86_64.tar.gz
tar xzf substreams-websocket.tar.gz
cd substreams-websocket-linux-x86_64
```

Each tarball includes the binary, `streams.example.toml`, and `.env.example`.

### From source

```bash
cargo install --git https://github.com/pinax-network/substreams-websocket --bin substreams-websocket
```

Requires Rust 1.90+ and `protoc` (the build script compiles `.proto` files).

---

## Quickstart (5 minutes)

```bash
# 1. Configure credentials
cp .env.example .env
# edit .env and set SUBSTREAMS_API_KEY=<your Pinax key>

# 2. Define your streams (or use the bundled example)
cp streams.example.toml streams.toml

# 3. Run the server
./substreams-websocket serve

# 4. Sanity-check
curl http://127.0.0.1:8080/healthz
# -> ok

# 5. Open a WebSocket and watch blocks roll in
npx wscat -c ws://127.0.0.1:8080/ws
```

You don't pass `--streams` because the server picks up `./streams.toml` by default.

---

## How it works

```
+-------------------+        +--------------------+        +------------------+
|  Substreams gRPC  | -----> |  substreams-       | -----> |  WebSocket       |
|  (Pinax/SF)       |        |  websocket server  |        |  subscribers     |
+-------------------+        +--------------------+        +------------------+
        ^                          |        ^
        |                          v        |
        +-- JWT exchange           cursors/  +-- 1 broadcast per non-empty block
            (api-key -> bearer)    on disk
```

- Each `[[streams]]` entry runs an independent gRPC reader against its endpoint.
- Decoded `DatabaseChanges` block outputs are flattened into a single JSON message and broadcast to every connected client.
- The Substreams cursor for that `(network, module_hash)` is persisted to disk after every block — restart resumes exactly where it left off.

---

## Configuration

Configuration is split deliberately: **secrets and single-value runtime settings live in `.env`**, the list of streams lives in **`streams.toml`**. This keeps secrets out of source control and the TOML readable.

### `.env`

| Group | Variable | Default | Purpose |
|-------|----------|---------|---------|
| Auth | `SUBSTREAMS_API_KEY` | _(unset)_ | Pinax/StreamingFast API key. Exchanged for a JWT at startup. |
| Auth | `SUBSTREAMS_TOKEN` | _(unset)_ | Raw bearer token (skip the JWT exchange). |
| Auth | `SUBSTREAMS_AUTH_URL` | `https://auth.pinax.network/v1/auth/issue` | JWT issuer. Set to `none` to disable the exchange. |
| Auth | `SUBSTREAMS_API_KEY_HEADER` | `X-Api-Key` | Header used when `SUBSTREAMS_AUTH_URL=none`. |
| Runtime | `SUBSTREAMS_PRODUCTION_MODE` | `false` | Tell Substreams to skip dev outputs. |
| Runtime | `SUBSTREAMS_FINAL_BLOCKS_ONLY` | `false` | Skip un-finalized blocks. |
| Runtime | `SUBSTREAMS_PLAINTEXT` / `SUBSTREAMS_INSECURE` | `false` | TLS toggles for non-public endpoints. |
| Server | `SUBSTREAMS_WEBSOCKET_STREAMS` | `./streams.toml` | Path to the streams TOML. |
| Server | `SUBSTREAMS_WEBSOCKET_LISTEN` | `127.0.0.1:8080` | HTTP/WS listen address. |
| Server | `SUBSTREAMS_WEBSOCKET_WS_PATH` | `/ws` | WebSocket route. |
| Server | `SUBSTREAMS_WEBSOCKET_HEALTH_PATH` | `/healthz` | Health-check route. |
| Server | `SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS` | `180` | Server ping every N seconds. |
| Server | `SUBSTREAMS_WEBSOCKET_HEARTBEAT_TIMEOUT_SECS` | `600` | Disconnect clients that don't pong within. |
| Server | `SUBSTREAMS_WEBSOCKET_MAX_CLIENTS` | `1024` | Concurrent connection cap. |
| Server | `SUBSTREAMS_WEBSOCKET_CLIENT_BUFFER_SIZE` | `1024` | Per-client outbound message buffer. |
| Server | `SUBSTREAMS_WEBSOCKET_CURSORS_DIR` | `./cursors` | Where per-stream cursor files are persisted. |

Every variable also has a matching CLI flag (`--listen`, `--ws-path`, ...). Run `substreams-websocket serve --help` for the full list.

### Authentication

The default flow matches the official `substreams` CLI:

1. You set `SUBSTREAMS_API_KEY=<key>`.
2. On startup the server POSTs `{"api_key": "..."}` to `SUBSTREAMS_AUTH_URL`.
3. It receives `{"token": "...jwt..."}` and uses `Authorization: Bearer <jwt>` for every gRPC request.

Alternatives:

- Skip the exchange entirely: set `SUBSTREAMS_AUTH_URL=none`. The API key is sent verbatim in the configured header (`X-Api-Key` by default). Use this for graph-node-style endpoints.
- Provide a pre-issued JWT: set `SUBSTREAMS_TOKEN=<jwt>` and leave `SUBSTREAMS_API_KEY` empty.

### `streams.toml`

This file is **only** an array of streams. No global block, no secrets.

```toml
[[streams]]
name = "swaps"
network = "solana-mainnet"
endpoint = "https://solana.substreams.pinax.network:443"
manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-dex-v0.5.1/svm-dex-v0.5.1.spkg"
# module defaults to "db_out"

[[streams]]
name = "transfers"
network = "solana-mainnet"
endpoint = "https://solana.substreams.pinax.network:443"
manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-transfers-v0.3.0/svm-transfers-v0.3.0.spkg"

[[streams]]
name = "swaps"               # same display name, different network -- fine
network = "ethereum-mainnet"
endpoint = "https://eth.substreams.pinax.network:443"
manifest = "https://github.com/pinax-network/substreams-evm/releases/download/evm-dex-v0.7.0/evm-dex-v0.7.0.spkg"

[[streams]]
name = "transfers"
network = "ethereum-mainnet"
endpoint = "https://eth.substreams.pinax.network:443"
manifest = "https://github.com/pinax-network/substreams-evm/releases/download/evm-transfers-v0.4.0/evm-transfers-v0.4.0.spkg"
```

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| `name` | yes | -- | Free-form display string. Forms `(network, name)` identity with `network`. |
| `network` | yes | -- | Chain identifier (`solana-mainnet`, `ethereum-mainnet`, ...). |
| `endpoint` | yes | -- | Substreams gRPC endpoint URL. |
| `manifest` | yes | -- | Local path or HTTPS URL of the `.spkg`. |
| `module` | no | `db_out` | Substreams output module. **Must emit `proto:sf.substreams.sink.database.v1.DatabaseChanges`.** Anything else fails fast at startup. |
| `start_block` | no | `"-1"` | Negative = relative to chain head. A persisted cursor on disk overrides this on resume. |
| `stop_block` | no | `"0"` | `"0"` = stream indefinitely. |
| `params` | no | `[]` | Substreams parameters, as `"module=value"` strings. |
| `token` / `api_key` / `api_key_header` / `auth_url` | no | from `.env` | Per-stream overrides if a stream needs different credentials. |

Validation runs before any gRPC connection. The server refuses to start on:
- a missing `endpoint`, `network`, or `start_block`
- duplicate `(network, name)` pairs

A stream whose `module` does not emit DatabaseChanges starts up but immediately broadcasts a `stream` `error` message and the task exits:

```
module "X" output type "proto:foo.bar.v1.Events" is not supported;
only proto:sf.substreams.sink.database.v1.DatabaseChanges is accepted
```

---

## WebSocket API

Connect to `ws://<host>:<port>/ws`. The server speaks four message types.

### Subscribing to specific streams

By default a connection receives **every** event from **every** configured stream. To narrow the firehose, pass one or more `subscribe=<network>:<stream>` query parameters when connecting. The server pushes a payload only when the entry's `(network, stream)` matches at least one filter.

```
# only Solana swaps
ws://host:8080/ws?subscribe=solana-mainnet:swaps

# Solana swaps AND Ethereum transfers
ws://host:8080/ws?subscribe=solana-mainnet:swaps&subscribe=ethereum-mainnet:transfers

# comma-separated form (equivalent to above)
ws://host:8080/ws?subscribe=solana-mainnet:swaps,ethereum-mainnet:transfers

# every "swaps" stream regardless of chain
ws://host:8080/ws?subscribe=*:swaps

# every Solana stream
ws://host:8080/ws?subscribe=solana-mainnet:*
```

- `*` is the wildcard for either field.
- Multiple filters are **OR**-combined.
- An entry without a `:` is rejected with HTTP 400.
- The session welcome message echoes the parsed filter (see below) so clients can confirm what was applied. The welcome itself is always sent.
- Stream lifecycle (`started`, `error`, `undo`, ...) messages are filtered the same way — a client subscribed to `solana-mainnet:swaps` does not see lifecycle events from other streams.

### 1. `session` -- sent once on connect

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
    },
    {
      "stream": "transfers",
      "network": "solana-mainnet",
      "module": "db_out",
      "manifest": "https://.../svm-transfers-v0.3.0.spkg",
      "module_hash": "673da4a738be4a99d6dc3c421f2b5744d4c7a2b9"
    }
  ],
  "filter": [
    { "network": "solana-mainnet", "stream": "swaps" }
  ]
}
```

`filter` echoes whatever `subscribe=` entries the client passed on the URL. An empty array means "no filter — match every stream". `*` is preserved verbatim for wildcards.

The `module_hash` is the canonical Substreams SHA-1 of the configured output module. Compare it to `substreams info <spkg>` to detect spkg upgrades.

### 2. `stream` -- lifecycle and error events

```json
{ "type": "stream", "status": "started",      "stream": "swaps", "network": "solana-mainnet", "module_hash": "..." }
{ "type": "stream", "status": "completed",    "stream": "swaps", "network": "solana-mainnet", "module_hash": "..." }
{ "type": "stream", "status": "error",        "stream": "swaps", "network": "solana-mainnet", "module_hash": "...", "message": "..." }
{ "type": "stream", "status": "decode_error", "stream": "swaps", "network": "solana-mainnet", "module_hash": "...", "message": "..." }
{ "type": "stream", "status": "fatal",        "stream": "swaps", "network": "solana-mainnet", "module_hash": "...", "message": "..." }
{ "type": "stream", "status": "undo",         "stream": "swaps", "network": "solana-mainnet", "module_hash": "...", "last_valid_block": 350000000 }
```

`undo` fires on chain reorganizations. Subscribers should roll back any state they materialized past `last_valid_block`.

### 3. Block payload -- every non-empty block

This is the message you'll see 99% of the time:

```json
{
  "stream": "swaps",
  "network": "solana-mainnet",
  "block_num": 350000000,
  "block_hash": "Gsk6bMgk5dYTemYQNZG9fuNZRoGynukQpzwGccUCZCJF",
  "timestamp": "2026-05-13 17:00:00",
  "cursor": "Mloz_-WpoBoZeTYC9KQEZAA6P09CXAA3NEBzMTpYNw...",
  "module_hash": "bd388f2e39f5dcc237cfbdb8d6c96d9e5678c797",
  "events": [
    {
      "@table": "swaps",
      "amm": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
      "amm_pool": "8ZT5BBW3WRpvCwPiadE6jiocQfriMjW7DSfXR2pF6YcT",
      "fee": "7005000",
      "fee_payer": "F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8",
      "input_amount": "1287000000",
      "input_mint": "So11111111111111111111111111111111111111112",
      "output_amount": "6848381008732",
      "output_mint": "13muFYDBUvgNpyDQSZ4eQTVHNoWaGhykonvyZWdGbonk",
      "protocol": "raydium_cpmm",
      "signature": "4kKAK8GFTrdmqsMqny7Bvh4Ume5vWqsw9BHeVUwEiPefbdxAYSnzWF38QV4iV1Y7Q3WnddGkfKbCyxtn4NoqoKuD",
      "user": "F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8"
    }
  ]
}
```

**Top-level fields** (always present):

| Field | Type | Meaning |
|-------|------|---------|
| `stream` | string | The TOML `[[streams]].name`. Use with `network` to identify which stream this is. |
| `network` | string | Chain network. |
| `block_num` | integer | Block height. |
| `block_hash` | string | Block hash (chain-native encoding, base58 on SVM, hex on EVM). |
| `timestamp` | string | UTC timestamp `YYYY-MM-DD HH:MM:SS`. |
| `cursor` | string | Opaque Substreams cursor for this block. Persist it if you want to resume from this exact position on your end. |
| `module_hash` | string | 40-char hex SHA-1 of the source module. Subscribers can use it to detect spkg upgrades. |
| `events` | array | One object per `TableChange` from the upstream `DatabaseChanges`, in source order. |

**Per-event fields**:

- `@table` (string) -- name of the source DB table. The `@` prefix is a collision guard so a real DB column literally named `table` cannot shadow it.
- Every other key is a column `name → value` pair, **values are strings on the wire** (DatabaseChanges contract; numeric types are stringified).
- Per-row keys that duplicate top-level fields (`block_num`, `block_hash`, `timestamp`, `minute`) are stripped from each event.
- The following upstream fields are **never** surfaced: `ordinal`, `operation` (CREATE/UPDATE/...), `pk` / `composite_pk`, `update_op`.

### 4. WebSocket-level pings

The server sends WebSocket ping frames every `SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS` (default 180s). Standard WebSocket clients answer with pong automatically; the server closes connections that fall silent for `SUBSTREAMS_WEBSOCKET_HEARTBEAT_TIMEOUT_SECS` (default 600s).

---

## Cursors and replay

For each `(network, module_hash)` the server writes a file to `$SUBSTREAMS_WEBSOCKET_CURSORS_DIR`:

```
cursors/
  solana-mainnet-bd388f2e39f5dcc237cfbdb8d6c96d9e5678c797.cursor    # SVM swaps
  solana-mainnet-673da4a738be4a99d6dc3c421f2b5744d4c7a2b9.cursor    # SVM transfers
  ethereum-mainnet-<hash>.cursor
```

The file holds the latest Substreams cursor for that stream -- overwritten on every block, no history. On restart, each stream's cursor is loaded and used as `start_cursor`, so resume is exact.

**Important:** this is *internal* recovery state. The server has no replay buffer for WebSocket subscribers. If a client disconnects, it misses whatever was broadcast during the gap. The `cursor` field on every broadcast lets subscribers persist their own checkpoint and reconnect directly to the Substreams source to backfill if needed.

The cursor file naming uses the **module hash**, not the stream name, so renaming a stream in `streams.toml` does not lose progress as long as the underlying `.spkg` is unchanged. Conversely, swapping in a new `.spkg` (which changes the hash) starts a fresh cursor.

---

## CLI

```
substreams-websocket serve     # run the WebSocket fanout server (default)
substreams-websocket stream    # one-shot read from a single Substreams package (debugging)
```

```bash
# Run server with explicit overrides
substreams-websocket serve \
  --streams ./streams.toml \
  --listen 0.0.0.0:8080 \
  --cursors-dir /var/lib/substreams-websocket/cursors

# One-shot debug stream -- print the first 5 blocks from a package
substreams-websocket stream \
  https://github.com/pinax-network/substreams-svm/releases/download/svm-dex-v0.5.1/svm-dex-v0.5.1.spkg \
  db_out \
  --endpoint https://solana.substreams.pinax.network:443 \
  --start-block -10 \
  --max-messages 5
```

---

## Operating

### Development checks

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

The test suite includes a functional test that injects a synthesized `DatabaseChanges` block through the broadcast pipeline and verifies a connected WebSocket client receives the expected JSON -- no live endpoint required.

### Docker / systemd

There is no Dockerfile or unit file in-tree yet. The binary is a single static-ish executable, so any minimal container or `systemd` unit pointing at the release tarball is sufficient. PRs welcome.

---

## Known limitations

- **No replay buffer.** Disconnected clients miss broadcasts during the gap.
- **No subscribe protocol after connect.** The filter is fixed at connection time via the URL `subscribe=` query parameter. Reconnect with a different filter to change subscriptions.
- **One output type.** Only `sf.substreams.sink.database.v1.DatabaseChanges` is supported.

---

## License

MIT.
