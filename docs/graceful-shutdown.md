# Graceful shutdown

On `SIGTERM` or `SIGINT`, the server stops accepting new HTTP/WebSocket connections, sends a `Close` frame to every connected client, waits up to `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS` for the registry to drain, then exits.

## Why

Hard-kill of an in-process WebSocket connection looks like a TCP RST to the client — most libraries surface this as an opaque "connection reset" with no diagnostic info. A clean `Close` frame with code `1001` (`GOING_AWAY`) tells the client this was intentional, lets it skip its usual error-path retry/backoff, and reconnect immediately.

Combined with [`replay.md`](replay.md), the reconnect-and-resume is near-seamless: client closes, reconnects in <1s with `?from_timestamp=<last_seen>`, replay log fills the 1–2s gap.

## Sequence

1. `SIGTERM` or `SIGINT` received.
2. `/healthz` flips to return `503 draining`. Reverse proxies (Envoy, nginx, ALB) running active health checks remove this replica from rotation within one health-check cycle.
3. axum's `with_graceful_shutdown` stops accepting new connections — the listener still exists, but new HTTP requests are refused.
4. Server iterates the client registry and sends `Message::Close(CloseFrame { code: 1001, reason: "server shutting down" })` to every client.
5. Server polls the registry every 50ms. When all clients have closed their side of the socket, drain completes immediately.
6. If `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS` elapses with clients still attached, drain logs a `WARN` with the remaining count and yields. axum then tears down sockets hard.
7. Per-stream Substreams reader tasks are aborted. Cursor + replay log writes already on disk are preserved.

For Envoy-specific health-check configuration, see [`envoy.md`](envoy.md).

## Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS` | `10` | Wall-clock budget for clients to ack `Close` and disconnect. `0` skips the wait (effectively a hard close). |

10s is comfortable for a typical browser / wscat / SDK client to ack and reconnect. Tune up if you have slow clients; tune down if you need fast container churn.

## What it does not do

- **Does not retain WebSocket connections across restarts.** A WebSocket is bound to a TCP socket which is bound to a process; both die on exit. The drain just makes the disconnect *clean* instead of abrupt.
- **Does not pause Substreams gRPC readers.** The stream tasks are aborted at the very end. Any block they were mid-process gets re-emitted by Substreams cursor resume on the next start.
- **Does not coordinate with the next container.** That is the load balancer's job (blue/green, drain weights, etc.). The drain just minimizes the per-connection blast.

## Combined with replay

The recommended client pattern:

```
1. On Close frame (code 1001) or any disconnect:
2.   reconnect with ?from_timestamp=<last timestamp_seconds seen>
3.   server replays the gap, then live stream resumes
```

With the default 10s drain + a 1000-block replay window, a Railway redeploy is invisible to the client application layer beyond a 1–2s pause.
