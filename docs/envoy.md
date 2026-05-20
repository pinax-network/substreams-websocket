# Envoy in front of substreams-websocket

Operational notes for running this server behind [Envoy](https://www.envoyproxy.io/) as a reverse proxy. Not used in production today — captured for when we get there. nginx and HAProxy share most of the same concerns.

## Why a proxy

- TLS termination, single ingress for many backends.
- Connection-level rate limiting / auth (not in scope of this server).
- Blue/green deploys without exposing every backend replica's port.

## What works out of the box

- **WebSocket upgrade.** Envoy proxies `Connection: Upgrade` + `Upgrade: websocket` transparently when the HTTP filter has `upgrade_configs: [{ upgrade_type: websocket }]`. The server's `/ws/<...>` and `/stream` routes work unchanged.
- **`Close` frame propagation.** Envoy forwards WS Close frames in both directions. The graceful-shutdown `Close(1001, "server shutting down")` reaches downstream clients verbatim.
- **`?from_timestamp=<n>` resume.** Query string is preserved through the proxy.
- **`/metrics` Prometheus scrape.** Plain HTTP GET, served on the same listener. Route Envoy normally; no upgrade headers needed.

## What needs Envoy-side config

### 1. Active health check against `/healthz`

The server now returns `503` from `/healthz` while shutdown drain is in flight (see [`graceful-shutdown.md`](graceful-shutdown.md)). Envoy needs an active health check so it removes the replica from the upstream cluster as soon as drain begins, instead of routing new connections into a draining backend.

```yaml
clusters:
- name: substreams_websocket
  health_checks:
  - timeout: 1s
    interval: 2s
    unhealthy_threshold: 1
    healthy_threshold: 1
    http_health_check:
      path: /healthz
      expected_statuses: [{ start: 200, end: 201 }]
```

Tight interval + `unhealthy_threshold: 1` means Envoy reacts within one health-check cycle (~2s). Combined with the server's `SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS=10`, you get: health flip → Envoy stops new traffic in ~2s → 8s remaining for existing clients to ack `Close` and reconnect → process exits.

### 2. Idle / stream timeouts must not fight the heartbeat

The server pings clients every `SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS` (default 180s) and kills connections silent for `SUBSTREAMS_WEBSOCKET_HEARTBEAT_TIMEOUT_SECS` (default 600s). Envoy has its own idle timeouts and will close a WebSocket it considers idle, **regardless of whether the backend is happily streaming pings**.

```yaml
route:
  cluster: substreams_websocket
  upgrade_configs:
  - upgrade_type: websocket
  # WebSocket idle window. Set comfortably above the server's heartbeat
  # interval. 0 disables, but most operators want a bound — pick something
  # like 1h.
  timeout: 0s
  idle_timeout: 3600s
```

`timeout: 0s` disables the per-route HTTP timeout (mandatory for long-lived WS). `idle_timeout: 3600s` is the WS-specific idle bound. Keep it well above `HEARTBEAT_INTERVAL`. If you tune the server's heartbeat *up*, you must tune Envoy's `idle_timeout` up too.

### 3. HTTP/1.1 to the upstream

The server's axum stack does HTTP/1.1-upgrade WebSocket. Envoy can do `extended_connect` over HTTP/2 to backends, but that path is not yet exercised here. Force HTTP/1.1:

```yaml
clusters:
- name: substreams_websocket
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
      explicit_http_config:
        http_protocol_options: {}    # HTTP/1.1
```

### 4. Per-connection buffer for fat blocks

Solana SPL-transfer blocks regularly serialize to >1 MiB on the WS wire. Envoy's default `per_connection_buffer_limit_bytes` is 1 MiB. Raise it:

```yaml
clusters:
- name: substreams_websocket
  per_connection_buffer_limit_bytes: 33554432   # 32 MiB
```

Same on the listener side if Envoy terminates TLS.

## Graceful deploy flow with Envoy

End-to-end sequence for a Railway-style rolling redeploy with Envoy in front:

1. Orchestrator brings up new backend container.
2. Active health check on `/healthz` flips it to healthy → Envoy adds it to the cluster.
3. Orchestrator sends `SIGTERM` to old backend container.
4. Old backend flips `/healthz` to `503` immediately.
5. Within one health-check cycle (~2s), Envoy marks old backend unhealthy and stops sending new connections there.
6. Old backend's `drain` step sends `Close(1001)` to existing clients. Clients receive a clean close (Envoy passes it through), reconnect to Envoy, get routed to new backend.
7. Old backend exits after drain timeout or when registry empties.

End-to-end client experience: one `Close(1001)` event, immediate reconnect, replay log (see [`replay.md`](replay.md)) fills the gap, application layer sees a 1–2s pause.

## What does not work transparently

- **Envoy hot restart.** Downstream WS connections still die when Envoy itself restarts, regardless of backend drain. Combined system survives backend restarts cleanly, not proxy restarts.
- **Sticky sessions.** Not needed today (no per-client server state beyond the in-memory subscription set, which the client re-asserts on reconnect via the URL or `SUBSCRIBE`).
- **WebSocket compression negotiation.** Envoy does not negotiate `permessage-deflate` for upgraded connections — the upgrade is passed through and any deflate negotiation happens between the server and the client end-to-end. The server does not enable `permessage-deflate` today.

## References

- Envoy WebSocket support: <https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upgrades/upgrades>
- Active health checks: <https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/health_checking>
- Graceful drain semantics on this server: [`graceful-shutdown.md`](graceful-shutdown.md)
