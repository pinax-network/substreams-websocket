# substreams-websocket

Stream decoded Substreams `DatabaseChanges` block outputs over a single WebSocket fan-out server. Configure one or more Substreams sources in TOML, point them at any `db_out`-style package; every connected client receives per-table JSON for every block — SVM, EVM, any chain emitting `sf.substreams.sink.database.v1.DatabaseChanges`. Clients subscribe by `<network>@<table>`.

- **One generic decoder** for every supported chain.
- **Resume on restart** — per-stream cursors persisted to disk.
- **Cross-chain identity** — clients subscribe by `(network, table)`; same table name on different chains coexists cleanly.
- **JWT auth** built in for Pinax/StreamingFast endpoints.
- **Prometheus metrics** on `/metrics` covering connections, broadcasts, Substreams reader.
- **Prebuilt tarballs** for Linux x86_64/aarch64, macOS x86_64/aarch64.

Extended reference docs live under [`docs/`](docs/). On-wire message shape: [`skills/SKILL.md`](skills/SKILL.md).

---

## Install

Pre-built binary:

```bash
curl -L -o substreams-websocket.tar.gz \
  https://github.com/pinax-network/substreams-websocket/releases/latest/download/substreams-websocket-linux-x86_64.tar.gz
tar xzf substreams-websocket.tar.gz
cd substreams-websocket-linux-x86_64
```

From source (Rust 1.90+, [`buf`](https://buf.build/docs/installation)):

```bash
cargo install --git https://github.com/pinax-network/substreams-websocket --bin substreams-websocket
```

---

## Quickstart

```bash
cp .env.example .env             # set SUBSTREAMS_API_KEY=<pinax key>
cp streams.example.yaml streams.yaml
./substreams-websocket serve
curl http://127.0.0.1:8080/healthz                  # -> ok
curl http://127.0.0.1:8080/openapi                  # -> OpenAPI 3.1 doc
npx wscat -c 'ws://127.0.0.1:8080/ws/*@*'
```

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

Each `[[streams]]` entry runs an independent gRPC reader. Decoded `DatabaseChanges` outputs are flattened to a single JSON message and broadcast. Cursor for `(network, module_hash)` persisted on every block — restart resumes exactly.

Details: [`docs/substreams.md`](docs/substreams.md), [`docs/cursors-and-resume.md`](docs/cursors-and-resume.md).

---

## Configuration

Secrets + single-value runtime settings in `.env`. Streams list in `streams.yaml`.

### `.env`

| Group | Variable | Default | Purpose |
|-------|----------|---------|---------|
| Auth | `SUBSTREAMS_API_KEY` | _(unset)_ | Pinax key. Exchanged for JWT at startup. |
| Auth | `SUBSTREAMS_TOKEN` | _(unset)_ | Raw bearer token (skip exchange). |
| Auth | `SUBSTREAMS_AUTH_URL` | `https://auth.pinax.network/v1/auth/issue` | JWT issuer. `none` disables exchange. |
| Auth | `SUBSTREAMS_API_KEY_HEADER` | `X-Api-Key` | Header when `SUBSTREAMS_AUTH_URL=none`. |
| Runtime | `SUBSTREAMS_PRODUCTION_MODE` | `false` | Skip dev outputs. |
| Runtime | `SUBSTREAMS_FINAL_BLOCKS_ONLY` | `false` | Skip un-finalized blocks. |
| Runtime | `SUBSTREAMS_PLAINTEXT` / `SUBSTREAMS_INSECURE` | `false` | TLS toggles. |
| Server | `SUBSTREAMS_WEBSOCKET_STREAMS` | `./streams.yaml` | Path to streams config. Format detected from extension (`.yaml` \| `.yml` \| `.toml`). |
| Server | `SUBSTREAMS_WEBSOCKET_STREAMS_YAML` | _(unset)_ | Inline YAML. Wins over file path. |
| Server | `SUBSTREAMS_WEBSOCKET_STREAMS_TOML` | _(unset)_ | Inline TOML. Same shape as the YAML form. YAML wins if both set. |
| Server | `SUBSTREAMS_WEBSOCKET_LISTEN` | `127.0.0.1:8080` | HTTP/WS listen address. |
| Server | `SUBSTREAMS_WEBSOCKET_WS_PATH` | `/ws` | WebSocket route. |
| Server | `SUBSTREAMS_WEBSOCKET_STREAM_PATH` | `/stream` | Query-mode route. |
| Server | `SUBSTREAMS_WEBSOCKET_HEALTH_PATH` | `/healthz` | Health route. |
| Server | `SUBSTREAMS_WEBSOCKET_METRICS_PATH` | `/metrics` | Prometheus metrics route. Empty disables. |
| Server | `SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS` | `180` | Ping interval. |
| Server | `SUBSTREAMS_WEBSOCKET_HEARTBEAT_TIMEOUT_SECS` | `600` | Disconnect after silence. |
| Server | `SUBSTREAMS_WEBSOCKET_MAX_CLIENTS` | `1024` | Connection cap. |
| Server | `SUBSTREAMS_WEBSOCKET_CLIENT_BUFFER_SIZE` | `1024` | Per-client outbound buffer. |
| Server | `SUBSTREAMS_WEBSOCKET_SLOW_CLIENT_DROP_LIMIT` | `100` | Force-close a client after N consecutive `try_send` drops on a saturated buffer. `0` disables. |
| Server | `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS` | `10` | On SIGTERM/SIGINT, send `Close` to every client and wait up to this long for them to disconnect before exiting. |
| Server | `SUBSTREAMS_WEBSOCKET_CURSORS_DIR` | `./cursors` | Cursor file directory. |
| Server | `SUBSTREAMS_WEBSOCKET_MAX_FILTER_FIELDS` | `16` | Max keys in one client-supplied event filter. |
| Server | `SUBSTREAMS_WEBSOCKET_MAX_FILTER_VALUES` | `64` | Max total string values across one event filter. |

Every variable has a matching CLI flag. `substreams-websocket serve --help` for full list.

Auth modes (api-key→JWT, raw bearer, header passthrough): [`docs/auth.md`](docs/auth.md).

### `streams.yaml` (or `streams.toml`)

YAML is the default. TOML is also accepted — same shape. The server detects from the file extension (`.yaml`, `.yml`, `.toml`) or from the env var name (`STREAMS_YAML` vs `STREAMS_TOML`). See [`streams.example.yaml`](streams.example.yaml).

#### `streams.yaml`

Array of Substreams sources. No global block, no secrets, **no operator-supplied name** — stream identity is derived from the loaded `.spkg` (`package_name` + `package_version` from `Package.package_meta[0]`, plus the canonical `module_hash`). Clients subscribe by `<network>@<table>` where `<table>` is the DatabaseChanges table emitted by `db_out`.

```yaml
streams:
  - network: solana-mainnet
    endpoint: https://solana.substreams.pinax.network:443
    manifest: https://github.com/pinax-network/substreams-svm/releases/download/svm-dex-v0.5.1/svm-dex-v0.5.1.spkg
    # module defaults to "db_out"

  - network: ethereum-mainnet
    endpoint: https://eth.substreams.pinax.network:443
    manifest: https://github.com/pinax-network/substreams-evm/releases/download/evm-dex-v0.7.0/evm-dex-v0.7.0.spkg
```

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| `network` | yes | -- | Chain id (`solana-mainnet`, `ethereum-mainnet`, ...). |
| `endpoint` | yes | -- | Substreams gRPC URL. |
| `manifest` | yes | -- | Local path or HTTPS URL of `.spkg`. The spkg's `package_meta[0].name` + `version` are required (used for cursor file naming). |
| `module` | no | `db_out` | Must emit `proto:sf.substreams.sink.database.v1.DatabaseChanges`. |
| `start_block` | no | `"-1"` | Negative = relative to head. Persisted cursor wins on resume. |
| `stop_block` | no | `"0"` | `"0"` = indefinite. |
| `params` | no | `[]` | `"module=value"` strings. |
| `tables` | no | `[]` | Operator-declared DatabaseChanges tables this spkg emits (e.g. `["swaps", "transfers"]`). Surfaced in the welcome message for client-side discovery. Empty = runtime discovery only. |
| `token` / `api_key` / `api_key_header` / `auth_url` | no | from `.env` | Per-stream overrides. |

Validation refuses duplicate `(network, manifest, module)` triples. Non-DatabaseChanges output or missing `package_meta` fails fast at startup.

Cursor files are named `<network>-<package_name>@<package_version>-<module_hash>.cursor`.

---

## WebSocket API

URL conventions mirror Binance market streams. Selectors: `<network>@<table>` (`solana-mainnet@swaps`). `*` wildcard on either side. `<table>` is the DatabaseChanges table emitted by the spkg's `db_out` module.

```
ws://host:8080/ws/solana-mainnet@swaps                # single, raw
ws://host:8080/ws/solana-mainnet@swaps/ethereum-mainnet@transfers   # multi, wrapped
ws://host:8080/stream?streams=*@swaps                 # query mode, wrapped
```

Live `SUBSCRIBE` / `UNSUBSCRIBE` / `LIST_SUBSCRIPTIONS` JSON commands also supported.

Full message shape (session, stream, block payload, undo) + command envelope: [`skills/SKILL.md`](skills/SKILL.md). Why these conventions: [`docs/binance-websocket.md`](docs/binance-websocket.md).

---

## CLI

```
substreams-websocket serve     # WebSocket fan-out server
substreams-websocket stream    # one-shot read for debugging
```

```bash
substreams-websocket serve \
  --streams ./streams.yaml \
  --listen 0.0.0.0:8080 \
  --cursors-dir /var/lib/substreams-websocket/cursors

substreams-websocket stream \
  https://github.com/pinax-network/substreams-svm/releases/download/svm-dex-v0.5.1/svm-dex-v0.5.1.spkg \
  db_out \
  --endpoint https://solana.substreams.pinax.network:443 \
  --start-block -10 \
  --max-messages 5
```

---

## Docker

```bash
docker pull ghcr.io/pinax-network/substreams-websocket:latest

docker run --rm -p 8080:8080 \
  -e SUBSTREAMS_API_KEY="$YOUR_KEY" \
  -e SUBSTREAMS_WEBSOCKET_LISTEN=0.0.0.0:8080 \
  -e SUBSTREAMS_WEBSOCKET_STREAMS_YAML="$(cat streams.yaml)" \
  -v $(pwd)/cursors:/app/cursors \
  ghcr.io/pinax-network/substreams-websocket:latest serve
```

Tags per release: `{version}`, `{major}.{minor}`, `{major}`, `latest`.

Railway / Fly / Heroku recipe (inline YAML, volume mount): [`docs/railway.md`](docs/railway.md).

---

## Development

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

Test suite injects a synthesized `DatabaseChanges` block through the broadcast pipeline and verifies a connected WebSocket client receives expected JSON — no live endpoint required.

Design decisions log: [`docs/decisions.md`](docs/decisions.md).

### Log levels

Set `SUBSTREAMS_WEBSOCKET_LOG_LEVEL` (or `--log-level`). Accepts any [`tracing` `EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) string. Default `info`.

| Level | What you see |
|-------|--------------|
| `info` (default) | Server lifecycle, client connect/disconnect (with duration), `SUBSCRIBE` / `UNSUBSCRIBE` / `LIST_SUBSCRIPTIONS` commands, Substreams stream start/restart/error, cursor resume. |
| `debug` | Above + one line per block broadcast (`stream`, `network`, `block_num`, `events`, `delivered`). |
| `trace` | Above + raw payload size and delivery counts per broadcast for stream-status messages too. |

Per-module overrides: `SUBSTREAMS_WEBSOCKET_LOG_LEVEL=info,substreams_websocket::server=debug`.

---

## Metrics

The server exposes Prometheus metrics on `/metrics` (configurable via `SUBSTREAMS_WEBSOCKET_METRICS_PATH`; set empty to disable). All metrics are namespaced `substreams_websocket_*` and cover connections, subscription commands, broadcasts, Substreams reader, cursor saves, and shutdown drain.

```
GET /metrics
# substreams_websocket_active_connections 4
# substreams_websocket_broadcast_blocks_total{network="solana-mainnet",table="swaps"} 1234
# ...
```

Full catalog + recommended PromQL queries: [`docs/metrics.md`](docs/metrics.md). Cardinality is bounded — `module_hash` and `client_id` are deliberately not labels.

---

## Known limitations

- **Live-only feed, no replay.** Disconnected clients miss broadcasts during the gap; `?from_timestamp=` / `?from_block=` are rejected with HTTP 400. Use Substreams directly to backfill by block, cursor, or timestamp.
- **One output type.** Only `sf.substreams.sink.database.v1.DatabaseChanges`.

---

## License

MIT.
