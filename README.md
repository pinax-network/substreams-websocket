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
npx wscat -c 'ws://127.0.0.1:8080/ws/*@*'
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
| Server | `SUBSTREAMS_WEBSOCKET_STREAM_PATH` | `/stream` | Query-mode route (Binance combined-stream style). |
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

The WebSocket API mirrors Binance's market-streams URL conventions. Stream selectors are written `<network>@<stream>` (e.g. `solana-mainnet@swaps`). `*` is a wildcard on either side.

### Connecting

Two modes are supported:

**`ws` mode** — streams in the URL path:

```
# Single stream — raw payloads, no envelope
ws://host:8080/ws/solana-mainnet@swaps

# Multiple streams — payloads wrapped: {"stream":"<id>","data":<raw>}
ws://host:8080/ws/solana-mainnet@swaps/ethereum-mainnet@transfers

# Wildcards
ws://host:8080/ws/*@swaps              # every "swaps" stream
ws://host:8080/ws/solana-mainnet@*     # every Solana stream
ws://host:8080/ws/*@*                  # every stream (always wrapped — >1 stream)
```

**`stream` mode** — streams in query string. Always wraps payloads:

```
ws://host:8080/stream?streams=solana-mainnet@swaps/ethereum-mainnet@transfers
ws://host:8080/stream?streams=*@swaps
ws://host:8080/stream?streams=*@*
```

Connecting to bare `/ws` with no path streams is rejected with `HTTP 400`. The server pushes a payload only when the broadcasted `(network, stream)` matches at least one of the connection's subscriptions.

### Live SUBSCRIBE / UNSUBSCRIBE / LIST_SUBSCRIPTIONS

Mirrors Binance's "Live Subscribing/Unsubscribing to streams" protocol. After the WebSocket upgrade, send JSON text frames to mutate or inspect the per-connection subscription set. Each command is one JSON object on the request side; the server replies with one JSON object per command on the same socket.

#### Request envelope

```json
{ "method": "<METHOD>", "params": [<string>, ...], "id": <any json value> }
```

| Field    | Required | Notes |
|----------|----------|-------|
| `method` | yes      | `SUBSCRIBE`, `UNSUBSCRIBE`, or `LIST_SUBSCRIPTIONS`. Case-sensitive. |
| `params` | depends  | Array of `network@stream` selectors. Required for `SUBSCRIBE` / `UNSUBSCRIBE`, ignored for `LIST_SUBSCRIPTIONS`. `*` is allowed on either side. |
| `id`     | optional | Echoed verbatim on the reply. Use a number or string to correlate. If omitted, the reply carries `"id": null`. |

#### Reply envelope

Success:
```json
{ "result": <null | array-of-strings>, "id": <echoed> }
```

Error:
```json
{ "error": "<message>", "id": <echoed-or-null> }
```

The socket continues normally after an error — invalid commands do **not** close the connection.

#### `SUBSCRIBE` — add to the subscription set

```json
// request
{ "method": "SUBSCRIBE",
  "params": ["solana-mainnet@swaps", "ethereum-mainnet@transfers"],
  "id": 1 }

// reply
{ "result": null, "id": 1 }
```

- Idempotent: re-subscribing to an existing selector is a no-op.
- Wildcards are accepted: `SUBSCRIBE ["*@swaps"]` subscribes to every `swaps` stream across all networks.
- A single bad selector (missing `@`, empty string, etc.) rejects the whole command — the existing set is unchanged.

#### `UNSUBSCRIBE` — remove from the subscription set

```json
// request
{ "method": "UNSUBSCRIBE",
  "params": ["ethereum-mainnet@transfers"],
  "id": 2 }

// reply
{ "result": null, "id": 2 }
```

- Unknown selectors are silently ignored — `result` is always `null`.
- To remove a wildcard subscription, pass the same wildcard form (`*@swaps` matches the wildcard entry, not the individual streams it expanded to).

#### `LIST_SUBSCRIPTIONS` — inspect the current set

```json
// request
{ "method": "LIST_SUBSCRIPTIONS", "id": 3 }

// reply
{ "result": ["solana-mainnet@swaps", "ethereum-mainnet@transfers"], "id": 3 }
```

The array preserves insertion order. Wildcards are returned in their original `*` form, not expanded.

#### Error responses

| Cause | Example reply |
|-------|---------------|
| Not valid JSON | `{"error": "invalid command: expected value at line 1 column 1", "id": null}` |
| Missing `@` separator in a param | `{"error": "stream selector \"foo\" must be \\`network@stream\\`", "id": 1}` |
| Empty selector in a param | `{"error": "stream selector must not be empty", "id": 1}` |
| Unknown method | `{"error": "unknown method \"DESTROY\"; expected SUBSCRIBE, UNSUBSCRIBE, or LIST_SUBSCRIPTIONS", "id": 99}` |

#### Worked example

```text
client → { "method": "LIST_SUBSCRIPTIONS", "id": 1 }
server ← { "result": ["solana-mainnet@swaps"], "id": 1 }

client → { "method": "SUBSCRIBE", "params": ["ethereum-mainnet@*"], "id": 2 }
server ← { "result": null, "id": 2 }

client → { "method": "LIST_SUBSCRIPTIONS", "id": 3 }
server ← { "result": ["solana-mainnet@swaps", "ethereum-mainnet@*"], "id": 3 }

client → { "method": "UNSUBSCRIBE", "params": ["solana-mainnet@swaps"], "id": 4 }
server ← { "result": null, "id": 4 }

client → { "method": "LIST_SUBSCRIPTIONS", "id": 5 }
server ← { "result": ["ethereum-mainnet@*"], "id": 5 }
```

#### Notes

- The connection's `wrap_envelope` mode is fixed at upgrade time (`/stream` and multi-stream `/ws/<a>/<b>` always wrap; single-stream `/ws/<a>` never wraps). `SUBSCRIBE` / `UNSUBSCRIBE` mutate the subscription set but **not** the envelope mode — to change envelope behavior, reconnect with a different URL.
- The server does not push a snapshot when `SUBSCRIBE` adds a new stream — only future blocks for that stream are delivered. Use `cursor` from your last received payload (or reconnect to the Substreams source directly) for backfill.
- There is no batching limit, but `SUBSCRIBE` with many wildcards is effectively equivalent to `*@*` — prefer the explicit wildcard.

### Lifecycle messages and filtering

Stream lifecycle messages (`started`, `error`, `undo`, ...) are filtered the same way as block payloads: a client subscribed to `solana-mainnet@swaps` does not see lifecycle events from other streams. The session welcome itself is always sent and echoes the parsed subscriptions and envelope mode.

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
  "subscriptions": ["solana-mainnet@swaps"],
  "wrap_envelope": false
}
```

`subscriptions` echoes whatever stream selectors the client passed in the URL path or query. `wrap_envelope` is `true` for `/stream?streams=...` and for `/ws` connections with more than one stream — when `true`, every payload (block and lifecycle) is wrapped as `{"stream":"<network>@<stream>","data":<payload>}` so clients can demultiplex.

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

### Docker

A multi-stage `Dockerfile` is in the repo root. The runtime image is `debian:bookworm-slim` with CA roots installed, runs as non-root `uid 10001`, and exposes port `8080`.

Pre-built images are published to GitHub Container Registry on every `v*` tag:

```bash
docker pull ghcr.io/pinax-network/substreams-websocket:latest
# or pin a version:
docker pull ghcr.io/pinax-network/substreams-websocket:0.2.1
```

Tags published per release: `{version}` (e.g. `0.2.1`), `{major}.{minor}` (`0.2`), `{major}` (`0`), and `latest`.

Build locally:

```bash
docker build -t substreams-websocket .

docker run --rm -p 8080:8080 \
  -e SUBSTREAMS_API_KEY="$YOUR_KEY" \
  -e SUBSTREAMS_WEBSOCKET_STREAMS_TOML="$(cat streams.toml)" \
  -v $(pwd)/cursors:/data/cursors \
  substreams-websocket
```

Defaults baked into the image:

- `SUBSTREAMS_WEBSOCKET_LISTEN=0.0.0.0:8080` — bind on all interfaces.
- `SUBSTREAMS_WEBSOCKET_CURSORS_DIR=/data/cursors` — mount a volume here in production.

### Railway / Fly / Heroku — env-only deploys

PaaS environments with no writable filesystem accept streams as **inline TOML** via env. Set `SUBSTREAMS_WEBSOCKET_STREAMS_TOML` to the full TOML content; the server parses it directly and skips the `--streams` file lookup.

**Railway recipe:**

1. **Create the service.** Connect this repo to a Railway project. Railway detects the `Dockerfile` and builds it. (If you prefer a Nixpacks build, remove the `Dockerfile` from `.railwayignore`; the Dockerfile path is simpler.)

2. **Set env vars** in the Railway "Variables" tab:

   ```
   SUBSTREAMS_API_KEY              = <your Pinax key>
   SUBSTREAMS_WEBSOCKET_STREAMS_TOML = """
     [[streams]]
     name = "swaps"
     network = "solana-mainnet"
     endpoint = "https://solana.substreams.pinax.network:443"
     manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-dex-v0.5.1/svm-dex-v0.5.1.spkg"

     [[streams]]
     name = "transfers"
     network = "solana-mainnet"
     endpoint = "https://solana.substreams.pinax.network:443"
     manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-transfers-v0.3.0/svm-transfers-v0.3.0.spkg"
   """
   ```

   Railway's UI supports multiline values. Paste the TOML inline; do not wrap it in quotes from a shell here-doc.

3. **Attach a volume** for cursors so progress survives redeploys:
   - In Railway, **Settings → Volumes → Add volume**.
   - Mount path: `/data/cursors`.
   - The Dockerfile already exports `SUBSTREAMS_WEBSOCKET_CURSORS_DIR=/data/cursors`. No extra env var needed.

4. **Expose the WebSocket port.** Railway auto-generates a public URL on port `8080` (the `EXPOSE` directive in the Dockerfile). Connect with:
   ```
   wss://<your-service>.up.railway.app/ws/solana-mainnet@swaps
   ```

5. **Optional tuning.** Override any of the defaults by adding env vars:
   ```
   SUBSTREAMS_WEBSOCKET_MAX_CLIENTS=4096
   SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS=180
   SUBSTREAMS_PRODUCTION_MODE=true
   ```

The same recipe works for any other env-only PaaS — only the volume-mount step changes per provider.

---

## Known limitations

- **No replay buffer.** Disconnected clients miss broadcasts during the gap.
- **One output type.** Only `sf.substreams.sink.database.v1.DatabaseChanges` is supported.

---

## License

MIT.
