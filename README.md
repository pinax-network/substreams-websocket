# substreams-websocket

Substreams to WebSocket

## Status

This project is in early implementation. The current CLI can load configuration
from `.env` and command-line arguments, start an Axum HTTP/WebSocket server, and
serve a health endpoint.

## Quickstart

```bash
cargo run --bin substreams-websocket -- serve --config config.example.toml
```

To serve swaps and SPL token transfers for Solana mainnet from the same
WebSocket server, define both streams in TOML:

```toml
[substreams]
endpoint = "https://solana.substreams.pinax.network:443"
network = "solana-mainnet"

[[streams]]
name = "swaps"
manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-dex-v0.5.1/dex-swaps-v0.5.1.spkg"
module = "map_events"

[[streams]]
name = "transfers"
manifest = "https://github.com/pinax-network/substreams-svm/releases/download/svm-transfers-v0.3.0/spl-token-v0.3.0.spkg"
module = "map_events"
```

To verify Substreams manifest loading and gRPC connectivity directly:

```bash
cargo run --bin substreams-websocket -- stream ./dex-swaps-v0.5.1.spkg map_events \
  --endpoint https://solana.substreams.pinax.network:443 \
  --network solana-mainnet \
  --max-messages 1
```

Useful development checks:

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

The server listens on `127.0.0.1:8080` by default:

```bash
curl http://127.0.0.1:8080/healthz
```

WebSocket clients connect to `/ws` by default. The server sends heartbeat ping
frames every 180 seconds and disconnects clients that do not respond with pong
frames within 600 seconds.

On startup, the server starts one Substreams read per configured stream and
broadcasts decoded block messages from each stream to every connected WebSocket
client. The welcome message includes a `streams` array describing the active
stream names, networks, manifests, and modules.

The configured Substreams network is included in the WebSocket session message
and decoded block messages.

SVM DEX swap payloads are decoded from `dex.swaps.v1.Events` into one JSON-ready
message per block. Each message keeps shared `block` metadata and carries every
transaction for that block under `transactions`, with each transaction carrying
its own `swaps` array. Block fields are exposed as `number`, `hash`, and
`timestamp`. Solana byte fields are encoded as base58.

SPL token transfer payloads are decoded from `solana.spl.token.v1.Events` into
the same block-level shape, with each transaction carrying a `transfers` array.
Only transfer, mint, and burn instructions are surfaced; other SPL token
instruction variants are ignored.

See `.env.example` for the initial environment variables.
