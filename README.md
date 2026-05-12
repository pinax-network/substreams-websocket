# substreams-websocket

Substreams to WebSocket: stream decoded Substreams block outputs over a
WebSocket fanout server. Ships with built-in decoders for SVM DEX swaps
(`dex.swaps.v1.Events`) and SPL token transfers (`solana.spl.token.v1.Events`).

## Quickstart

```bash
cargo run --bin substreams-websocket -- serve --config config.example.toml
```

Health check:

```bash
curl http://127.0.0.1:8080/healthz
```

WebSocket client (Node):

```bash
npx wscat -c ws://127.0.0.1:8080/ws
```

## Configuration

Two sources, merged in order: `.env` (or `--env-file`) → CLI flags / TOML file
passed via `--config`. TOML wins for stream definitions; environment variables
back-fill defaults for the top-level `[substreams]` block and `[websocket]`
listener settings.

### TOML

```toml
[websocket]
listen = "127.0.0.1:8080"
ws_path = "/ws"
health_path = "/healthz"
heartbeat_interval_secs = 180
heartbeat_timeout_secs = 600
max_clients = 1024
client_buffer_size = 1024

[substreams]
endpoint = "https://solana.substreams.pinax.network:443"
network = "solana-mainnet"
stop_block = "0"
production_mode = false
final_blocks_only = false

[[streams]]
name = "swaps"
manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-dex-v0.5.1/dex-swaps-v0.5.1.spkg"
module = "map_events"

[[streams]]
name = "transfers"
manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-transfers-v0.3.0/spl-token-v0.3.0.spkg"
module = "map_events"
```

Valid `name` values: `swaps`, `transfers`. The decoder is selected from this
name. Each `[[streams]]` entry runs in its own Substreams reader task and
broadcasts decoded block messages to every connected WebSocket client.

### Environment variables

| Variable | Purpose |
|----------|---------|
| `SUBSTREAMS_ENDPOINT` | Default gRPC endpoint for `[substreams]` |
| `SUBSTREAMS_NETWORK` | Network tag included in decoded messages |
| `SUBSTREAMS_START_BLOCK` / `SUBSTREAMS_STOP_BLOCK` | Block range |
| `SUBSTREAMS_PARAMS` | Comma-separated module params |
| `SUBSTREAMS_PRODUCTION_MODE` / `SUBSTREAMS_FINAL_BLOCKS_ONLY` | Stream flags |
| `SUBSTREAMS_API_KEY` / `SUBSTREAMS_TOKEN` | Substreams auth (one required for live streams) |
| `SUBSTREAMS_WEBSOCKET_CONFIG` | Path to TOML config file |
| `SUBSTREAMS_WEBSOCKET_LISTEN` | Listen address |
| `SUBSTREAMS_WEBSOCKET_WS_PATH` / `SUBSTREAMS_WEBSOCKET_HEALTH_PATH` | HTTP route paths |
| `SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS` / `_TIMEOUT_SECS` | Heartbeat window |
| `SUBSTREAMS_WEBSOCKET_MAX_CLIENTS` | Concurrent client cap |
| `SUBSTREAMS_WEBSOCKET_CLIENT_BUFFER_SIZE` | Per-client outbound buffer (messages) |

See `.env.example` for the full list and defaults.

### Credentials

Live Substreams endpoints require either an API key (`SUBSTREAMS_API_KEY`,
sent as `X-Api-Key`) or a bearer token (`SUBSTREAMS_TOKEN`). For
`solana.substreams.pinax.network`, request access at https://pinax.network.

## WebSocket API

### Connect

```
ws://<host>:<port>/ws
```

### Session message

On connect, the server sends a JSON `session` message describing every active
stream:

```json
{
  "type": "session",
  "status": "connected",
  "client_id": 1,
  "streams": [
    { "name": "swaps", "network": "solana-mainnet", "module": "map_events", "manifest": "..." },
    { "name": "transfers", "network": "solana-mainnet", "module": "map_events", "manifest": "..." }
  ]
}
```

### Stream status messages

Lifecycle and error events are emitted as `{"type":"stream", "status": ...}`:

- `started` — Substreams reader connected
- `completed` — reader reached stop block
- `error` — reader failed to start or terminated; `message` carries the cause
- `decode_error` — payload could not be decoded; `message` carries the cause
- `fatal` — upstream emitted a fatal error
- `undo` — chain reorg; `last_valid_block` indicates the safe block

### Decoded block messages

For each block with at least one decodable transaction, the server sends one
message per stream. Byte fields are base58.

`swaps` (one per block, all transactions and swaps inline):

```json
{
  "type": "swaps",
  "network": "solana-mainnet",
  "block": { "number": 1234, "hash": "...", "timestamp": "2026-05-12 17:00:00" },
  "transactions": [
    {
      "signature": "...", "fee_payer": "...", "signers": ["..."],
      "fee": 5000, "compute_units_consumed": 77777,
      "swaps": [
        { "protocol": "raydium_amm_v4", "program_id": "...", "stack_height": 2,
          "amm": "...", "amm_pool": "...", "user": "...",
          "input_mint": "...", "input_amount": 1000,
          "output_mint": "...", "output_amount": 2000 }
      ]
    }
  ]
}
```

`transfers` mirrors the same block-level shape with a `transfers` array
per transaction. Only `transfer`, `mint`, and `burn` SPL instructions are
surfaced.

### Heartbeats

The server sends a WebSocket ping every `heartbeat_interval_secs` (default
180s) carrying the client id as payload. If no pong is received within
`heartbeat_timeout_secs` (default 600s), the server closes the connection.
Clients should respond to pings with pongs; most WebSocket libraries do this
automatically.

### Backpressure

Each client has a bounded outbound buffer (`client_buffer_size`). If a client
cannot keep up, individual broadcast messages are dropped for that client
(logged as a warning) rather than blocking the Substreams ingestion loop. The
connection itself is not closed for slow consumption alone — only stale
clients (no pong within timeout) are evicted.

## Operating

### Local development

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

The test suite includes a functional test that injects a synthesized
Substreams block event through the broadcast pipeline and verifies a
connected WebSocket client receives the decoded JSON message — no live
Substreams endpoint required.

### Verify Substreams connectivity

```bash
cargo run --bin substreams-websocket -- stream ./dex-swaps-v0.5.1.spkg map_events \
  --endpoint https://solana.substreams.pinax.network:443 \
  --network solana-mainnet \
  --max-messages 1
```

### Run the server

```bash
cp .env.example .env
# Fill in SUBSTREAMS_API_KEY or SUBSTREAMS_TOKEN
cargo run --bin substreams-websocket -- serve --config config.example.toml
```

## Demo

The bundled `config.example.toml` streams the SVM DEX swaps package
(`dex-swaps-v0.5.1.spkg`) and the SPL token transfers package
(`spl-token-v0.3.0.spkg`) from `solana.substreams.pinax.network`. Start the
server, connect a WebSocket client to `/ws`, and you will receive the session
message followed by decoded swap and transfer blocks as they arrive.
