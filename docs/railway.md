# Railway deployment

Specifics for running this server on [Railway](https://railway.app). Other PaaS (Fly, Render, Heroku) work the same way — substitute the env-var UI.

## What Railway provides

- Build from `Dockerfile` at repo root. Multi-stage `rust:1.93-bookworm → debian:bookworm-slim`. No `railway.json` / `nixpacks.toml` needed.
- A public HTTPS endpoint terminating at `:8080` inside the container. We bind to `0.0.0.0:8080` via `SUBSTREAMS_WEBSOCKET_LISTEN`.
- Per-service env vars edited in the dashboard. No secrets file needed.
- Ephemeral filesystem unless a volume is mounted.

## Required env vars

Paste into the Variables tab:

```
SUBSTREAMS_API_KEY=<pinax key>
SUBSTREAMS_AUTH_URL=https://auth.pinax.network/v1/auth/issue
SUBSTREAMS_WEBSOCKET_LISTEN=0.0.0.0:8080
SUBSTREAMS_WEBSOCKET_STREAMS_YAML=<inline YAML, see below>
```

`SUBSTREAMS_WEBSOCKET_STREAMS_YAML` is the full contents of a `streams.yaml`, pasted as the env value. It wins over `SUBSTREAMS_WEBSOCKET_STREAMS` (the file path). Required on Railway because the image has no writable place to drop a config file. TOML equivalent (`SUBSTREAMS_WEBSOCKET_STREAMS_TOML`) is also accepted.

Example value:

```yaml
streams:
  - network: solana-mainnet
    endpoint: https://solana.substreams.pinax.network:443
    manifest: https://github.com/pinax-network/substreams-solana/releases/download/swaps-v0.1.0/swaps-v0.1.0.spkg
    module: db_out
```

Railway's env-var input accepts multi-line values. Paste with line breaks preserved.

## Cursor persistence (volume)

Without a volume, every redeploy starts from `initial_block` and re-syncs. For long-running deploys:

1. Add a Volume in the service settings. Mount path: `/data/cursors`.
2. Set `SUBSTREAMS_WEBSOCKET_CURSORS_DIR=/data/cursors`.

Cursor files are small (~200 bytes each), one per stream. A 100 MB volume is overkill but the smallest Railway offers.

## Health check

Set Healthcheck Path in the service's Settings → Networking to `/healthz`. Default heartbeat semantics from `SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS` apply — the endpoint returns 200 as long as the server process is up. It does not assert upstream Substreams health.

## Metrics

Prometheus scrape lives at `/metrics` on the same listener (configurable via `SUBSTREAMS_WEBSOCKET_METRICS_PATH`). Point any Prometheus / Grafana Cloud / VictoriaMetrics agent at `<your-service>.up.railway.app/metrics`. See [`metrics.md`](metrics.md) for the metric catalog.

## Image source

Two options:

1. **Build from repo.** Point the service at the GitHub repo; Railway runs the `Dockerfile`. Build takes ~3–4 minutes (Rust release).
2. **Pull pre-built image from GHCR.** Use `ghcr.io/pinax-network/substreams-websocket:vX.Y.Z`. Published by `.github/workflows/docker-publish.yml` on every `v*` tag. Faster cold deploys, pins to a known version.

## Gotchas

- **WebSocket termination.** Railway's edge proxy upgrades WebSocket connections transparently. No special config needed.
- **24-hour disconnects.** Railway does not enforce a 24-hour cap (unlike Binance). Long-lived clients stay connected.
- **Cold start cursor loss.** First deploy with no volume = full re-sync from `initial_block`. On Solana this can be hours of catch-up. Mount a volume before going to prod.
- **Empty `SUBSTREAMS_WEBSOCKET_STREAMS_YAML` / `_TOML`.** clap reports the var as `Some("")` when the field is blank in the dashboard. The server treats empty/whitespace as unset and falls back to the file path. Delete the variable entirely if you don't want it.
- **Memory.** Solana with the 64 MiB gRPC decode cap can spike memory on fat blocks. The starter plan's 512 MB has been observed to hold; bump to 1 GB if you see OOM-kills under load.

## Troubleshooting workflow (fast path)

When something "just doesn't stream," run this order before changing code:

1. `GET /healthz` — verify process health first.
2. `GET /streams` — verify your expected `network` and table(s) are present.
3. Connect to `wss://<host>/ws/*@*` and read the `session` message to confirm discoverable streams.
4. Only then debug consumer code and subscription selectors.

This order is especially useful on Railway when log tailing is delayed.

### Common errors and fixes

- **Python import failure**
  - Error: `ModuleNotFoundError: No module named 'websockets'`
  - Fix: add `websockets` to `requirements.txt` and redeploy.

- **Client keyword mismatch after dependency upgrade**
  - Error: `TypeError: ... unexpected keyword argument ...` during WebSocket connect
  - Fix: check the installed client library version and use its expected connect kwargs (for example `extra_headers` vs `additional_headers` in Python `websockets`).

- **Busy Solana blocks disconnect client**
  - Signal: close code `1009` (`message too big`) or intermittent disconnects on high-volume blocks
  - Fix: increase your client max frame/message size to at least `32 MiB` (for example `max_size=32 * 1024 * 1024`).

- **No data despite successful connect**
  - Signal: socket opens but no block payloads arrive
  - Fix: verify selector correctness (`<network>@<table>`), confirm table existence in `/streams` or `session.streams[].tables`. Wildcards (`*@*`, `*@swaps`) do replay on `?from_timestamp=` — the server expands them to concrete per-`network@table` frames.

## References

- Railway docs: <https://docs.railway.com/>
- Volumes: <https://docs.railway.com/reference/volumes>
- GHCR workflow: [`.github/workflows/docker-publish.yml`](../.github/workflows/docker-publish.yml)
