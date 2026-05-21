# Prometheus metrics

The server exposes a Prometheus scrape endpoint on `/metrics` (configurable via `SUBSTREAMS_WEBSOCKET_METRICS_PATH`). Set the env to an empty string to disable the route entirely.

All metrics are namespaced `substreams_websocket_*`. Naming follows Prometheus conventions: `_total` on monotonic counters, `_seconds` on duration histograms, gauges with no suffix.

## Connection lifecycle

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `substreams_websocket_connections_total` | counter | — | Lifetime WS connects accepted. |
| `substreams_websocket_disconnections_total` | counter | — | Lifetime WS disconnects of any cause. |
| `substreams_websocket_active_connections` | gauge | — | Currently connected clients. |
| `substreams_websocket_rejected_connections_total` | counter | — | Connect attempts denied because `max_clients` was reached. |
| `substreams_websocket_connection_duration_seconds` | histogram | — | Distribution of completed connection lifetimes. |
| `substreams_websocket_force_closed_total` | counter | — | Force-closes triggered by `SLOW_CLIENT_DROP_LIMIT`. |

## Subscription commands

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `substreams_websocket_commands_total` | counter | `method` | One increment per JSON command received (`SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_FILTER`, `CLEAR_FILTER`, `LIST_FILTERS`, plus unknown). |

## Broadcasts

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `substreams_websocket_broadcast_blocks_total` | counter | `network`, `table` | Per-table block payloads dispatched. |
| `substreams_websocket_broadcast_events_total` | counter | `network`, `table` | Sum of events across those payloads. |
| `substreams_websocket_broadcast_delivered_total` | counter | `network`, `table` | Successful `try_send` calls. |
| `substreams_websocket_broadcast_dropped_total` | counter | `network`, `table` | Failed `try_send` calls (saturated outbound). |
| `substreams_websocket_broadcast_filtered_skipped_total` | counter | `network`, `table` | Broadcasts skipped because a client's filter dropped every event. |
| `substreams_websocket_lifecycle_broadcasts_total` | counter | `status` | Lifecycle messages dispatched: `started`, `error`, `completed`, `fatal`, `decode_error`, `undo`, `gap`. |

## Substreams reader

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `substreams_websocket_substreams_blocks_total` | counter | `network`, `package_name`, `package_version` | Block events received from upstream. |
| `substreams_websocket_substreams_errors_total` | counter | `network`, `package_name`, `package_version`, `kind` | `kind`: `transient` (stream ended after progress), `fatal` (errored), `decode` (DatabaseChanges parse failed). |
| `substreams_websocket_substreams_reconnects_total` | counter | `network`, `package_name`, `package_version` | Retry-loop reconnect attempts. |
| `substreams_websocket_substreams_undo_total` | counter | `network`, `package_name`, `package_version` | `BlockUndoSignal` events. |

## Replay log

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `substreams_websocket_replay_appends_total` | counter | `network`, `package_name`, `package_version`, `outcome` | `outcome`: `success` / `error`. |
| `substreams_websocket_replay_append_bytes_total` | counter | `network`, `package_name`, `package_version` | Bytes appended to the replay log post-serialization. |
| `substreams_websocket_replay_reads_total` | counter | `network`, `table`, `outcome` | `outcome`: `replayed` / `empty` / `gap`. |
| `substreams_websocket_replay_blocks_delivered_total` | counter | `network`, `table` | Block payloads delivered via replay. |

## Shutdown

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `substreams_websocket_drain_initiated_total` | counter | — | Graceful drains triggered. |
| `substreams_websocket_draining` | gauge | — | `1` while `/healthz` is returning 503 during drain, `0` otherwise. |

## Cardinality

Label sets are kept small on purpose:

- `network`, `table`, `package_name`, `package_version` are surfaced where they make sense.
- `module_hash` is **not** a label — high-churn (changes on every spkg upgrade), low operational value once `package_version` is present.
- `client_id` is **not** a label — would blow up on a busy server.

Outcome-style labels (`success`, `error`, `gap`, `transient`, ...) stay finite per metric.

## Scrape config

```yaml
scrape_configs:
  - job_name: substreams-websocket
    static_configs:
      - targets: ['<host>:8080']
    scrape_interval: 15s
    metrics_path: /metrics
```

## Useful queries

```promql
# Connection churn
rate(substreams_websocket_connections_total[5m])
rate(substreams_websocket_disconnections_total[5m])

# Broadcast throughput per channel
sum by (network, table) (rate(substreams_websocket_broadcast_blocks_total[1m]))

# Drop rate vs delivery
rate(substreams_websocket_broadcast_dropped_total[5m])
  / rate(substreams_websocket_broadcast_delivered_total[5m])

# Substreams reconnect rate (alert on > 0.1/min)
rate(substreams_websocket_substreams_reconnects_total[5m])

# Replay gap signal (clients reconnecting outside the window)
rate(substreams_websocket_replay_reads_total{outcome="gap"}[5m])

# p99 connection lifetime
histogram_quantile(0.99, sum by (le) (rate(substreams_websocket_connection_duration_seconds_bucket[10m])))
```
