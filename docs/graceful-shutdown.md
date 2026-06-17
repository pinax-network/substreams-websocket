# Graceful shutdown

On `SIGTERM` or `SIGINT`, the server stops accepting new HTTP/WebSocket connections, sends a `Close` frame to every connected client, waits up to `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS` for the registry to drain, then exits.

## Why

Hard-kill of an in-process WebSocket connection looks like a TCP RST to the client — most libraries surface this as an opaque "connection reset" with no diagnostic info. A clean `Close` frame with code `1001` (`GOING_AWAY`) tells the client this was intentional, lets it skip its usual error-path retry/backoff, and reconnect immediately.

The clean close lets the client reconnect in <1s and resume the live feed. The feed is live-only, so blocks produced during the 1–2s gap are missed; backfill via Substreams if the application can't tolerate the gap.

## Sequence

1. `SIGTERM` or `SIGINT` received.
2. `/healthz` flips to return `503 draining`. Reverse proxies (Envoy, nginx, ALB) running active health checks remove this replica from rotation within one health-check cycle.
3. axum's `with_graceful_shutdown` stops accepting new connections — the listener still exists, but new HTTP requests are refused.
4. Server iterates the client registry and sends `Message::Close(CloseFrame { code: 1001, reason: "server shutting down" })` to every client.
5. Server polls the registry every 50ms. When all clients have closed their side of the socket, drain completes immediately.
6. If `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS` elapses with clients still attached, drain logs a `WARN` with the remaining count and yields. axum then tears down sockets hard.
7. Per-stream Substreams reader tasks are aborted. Cursor writes already on disk are preserved.

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

## Client reconnect pattern

The recommended client pattern:

```
1. On Close frame (code 1001) or any disconnect:
2.   reconnect to the same selector — the live stream resumes
3.   if the gap matters, backfill missed blocks via Substreams
     (resume from the last block / timestamp you saw)
```

With the default 10s drain, a Railway redeploy is a 1–2s pause for the client application layer. Because the feed is live-only, any blocks produced during that pause are not buffered for replay — use Substreams to fill the hole if your application needs exactly-once delivery.
