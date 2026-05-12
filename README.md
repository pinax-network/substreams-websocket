# substreams-websocket

Substreams to WebSocket

## Status

This project is in early implementation. The current CLI can load configuration
from `.env` and command-line arguments, start an Axum HTTP/WebSocket server, and
serve a health endpoint.

## Quickstart

```bash
cargo run --bin substreams-websocket -- serve ./dex-swaps-v0.5.1.spkg swaps
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

See `.env.example` for the initial environment variables.
