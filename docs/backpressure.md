# WebSocket backpressure

How the server protects itself when a connected client can't read messages as fast as the upstream Substreams stream produces them.

## The hot path

Each WebSocket client owns a bounded `tokio::mpsc` channel sized by `SUBSTREAMS_WEBSOCKET_CLIENT_BUFFER_SIZE` (default `1024`). A dedicated writer task pulls from the channel and writes to the socket.

Broadcast workflow per block:

1. Substreams reader decodes block, emits per-table payloads.
2. For each connected client whose subscription matches, broadcast does **non-blocking** `tx.try_send(message)`.
3. If `try_send` returns `Err(Full)`, the message is dropped on the floor.

`try_send` is the first line of defense: a slow client never blocks the broadcast loop or anyone else. But unbounded drop logs from a single stuck consumer can drown the rest of the operational signal, and a permanently-stuck consumer just sits there forever consuming an fd + an mpsc + a writer task.

## Force-close threshold

The server tracks **consecutive failed sends** per client:

- Every successful `try_send` resets the counter to `0`.
- Every failure increments it (and the lifetime `total_drops`).
- When the counter reaches `SUBSTREAMS_WEBSOCKET_SLOW_CLIENT_DROP_LIMIT` (default `100`), the server sends `Close(1013 "slow client backpressure")` and unregisters the connection.

`1013` is the IANA "Try Again Later" code — the closest standard to "backend dropped you for being slow." Clients that handle reconnect logic will pick this up and re-establish.

## No send toward a client can stall its teardown

A full buffer means the peer isn't draining its socket fast enough — possibly not at all — so nothing in the connection's lifecycle may block on it unboundedly, or the teardown machinery would hang on exactly the clients it exists to evict. Every lifecycle send is either non-blocking or raced against the heartbeat reaper:

- **Heartbeat pings** use `try_send`. On a full buffer the ping is skipped — with no ping delivered there's no pong, so the heartbeat timeout fires on schedule and reaps the connection.
- **Command replies and pong responses** use `try_send`. On a full buffer they're dropped (the client isn't reading them anyway) so the connection loop keeps polling the disconnect signal.
- **Welcome message and [replay](replay.md) catch-up** use blocking sends by design — replay frames must not be dropped, so a full buffer there is flow control, not an error. The whole setup phase is raced against the reaper's disconnect signal, and replay send progress counts as liveness (a send only completes when the peer is draining its socket). A healthy client whose replay outlasts the heartbeat timeout is left alone; a peer that connects and never reads stalls the sends, goes stale, and is torn down on heartbeat timeout.
- **Final flush at teardown** is bounded to 5 seconds. A peer stuck in zero-window TCP can leave the writer task blocked mid-`send` indefinitely; after the grace the writer is aborted so disconnect accounting (`active_connections` gauge, `disconnections_total`, duration histogram) always completes.

Without these, a dead-but-not-closed peer (NAT timeout, killed laptop, stalled proxy) would freeze its connection's accounting forever: the gauge climbs and never decreases, and `disconnections_total` stays near zero even as pods rotate.

## Configuration

| Variable | Default | Behavior |
|---|---|---|
| `SUBSTREAMS_WEBSOCKET_CLIENT_BUFFER_SIZE` | `1024` | mpsc capacity per client. Higher = more in-flight messages tolerated; lower = drops fire sooner. |
| `SUBSTREAMS_WEBSOCKET_SLOW_CLIENT_DROP_LIMIT` | `100` | Consecutive drops before force-close. `0` disables — frames keep dropping but the connection lives forever. |

For most loads, the default `100 × 1024-buffer` means a client tolerates a brief pause (one drained buffer worth) before the threshold fires. A truly broken client gets evicted after ~100 broadcasts of dead silence on their side.

## Log throttling

The per-drop `warn!` is logged on the **first** drop and every **100th** thereafter so a saturated client doesn't flood the log between threshold crossings. The force-close itself emits one `WARN` with the lifetime `total_drops`.

## Disconnect log

When a client disconnects (clean Close, heartbeat timeout, ttl, force-close, or browser close) the `WebSocket client disconnected` `info` log carries `total_drops` so operators can audit who was lagging.

## What it does not do

- **No per-block coalescing.** Each block produces one `try_send` per matching client. Future work could batch consecutive blocks or coalesce when the buffer is near full.
- **No fairness.** A slow client doesn't slow down a fast client (try_send is non-blocking), but it also doesn't get priority dropped before other channels. Drops are scattered across the broadcast loop in arrival order.
- **No retransmission.** Dropped frames are not resent. The client can use [`?from_timestamp=`](replay.md) on reconnect to fetch them from the replay log if it's within the retention window.

## Tuning notes

- **High broadcast rate + slow downstream**: lower `CLIENT_BUFFER_SIZE` so drops fire sooner and the operator sees `slow client` warns faster. Coupled with the default drop limit, a misbehaving consumer is gone within seconds.
- **Bursty downstream (browser tabs, mobile)**: keep the default 1024 buffer + 100 drop limit. A typical browser stall of a few hundred milliseconds is absorbed by the buffer without dropping.
- **Server-to-server consumers that should never get evicted**: set `SLOW_CLIENT_DROP_LIMIT=0`. Drops still happen but the connection stays open indefinitely.
