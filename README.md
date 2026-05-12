# substreams-websocket

Substreams to WebSocket

## Status

This project is in early implementation. The current CLI can load configuration
from `.env` and command-line arguments, start an Axum HTTP/WebSocket server, and
serve a health endpoint.

## Quickstart

```bash
cargo run --bin substreams-websocket -- serve ./dex-swaps-v0.5.1.spkg map_events
```

To verify Substreams package loading and gRPC connectivity directly:

```bash
cargo run --bin substreams-websocket -- stream ./dex-swaps-v0.5.1.spkg map_events \
  --endpoint https://solana.substreams.pinax.network:443 \
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

See `.env.example` for the initial environment variables.
