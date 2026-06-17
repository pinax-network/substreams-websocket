//! Prometheus metrics for the WebSocket server.
//!
//! All metrics are namespaced with `substreams_websocket_*`. The exporter is
//! initialized once at startup and surfaced over `/metrics` on the same
//! axum listener as the WebSocket routes.
//!
//! ## Naming conventions
//!
//! - `_total` suffix on monotonic counters.
//! - `_seconds` suffix on duration histograms.
//! - Gauges have no suffix.
//!
//! ## Label cardinality
//!
//! We deliberately keep label sets small to avoid cardinality blow-up:
//!
//! - `network`, `table`, `package_name`, `package_version` are surfaced where
//!   they make sense. `module_hash` is **not** a label (high churn, low
//!   operational value once `package_version` is present).
//! - Command names (`SUBSCRIBE`, `SET_FILTER`, ...) are labels on the
//!   command counters.
//! - `outcome` is `success` / `error` / `decode_error` / `gap` / etc. on
//!   the relevant counters.
//!
//! See `docs/metrics.md` for the canonical list.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global metrics recorder + return a render handle that serves
/// the `/metrics` endpoint. Safe to call multiple times — `OnceLock::get_or_init`
/// serializes the install across threads so concurrent callers race to read
/// the same handle, never to call `install_recorder` twice (which would fail
/// with `FailedToSetGlobalRecorder`).
pub fn init() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            let handle = PrometheusBuilder::new()
                .install_recorder()
                .expect("install Prometheus recorder");
            // Pre-describe metrics so dashboards and alerts can key off
            // names even before they're incremented for the first time.
            describe();
            handle
        })
        .clone()
}

/// Render the current metrics scrape body.
pub fn render() -> String {
    HANDLE
        .get()
        .map(|h| h.render())
        .unwrap_or_else(|| "# metrics recorder not initialized\n".to_owned())
}

fn describe() {
    use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

    // ---- WebSocket connections --------------------------------------------
    describe_counter!(
        "substreams_websocket_connections_total",
        "Lifetime count of WebSocket clients that established a connection."
    );
    describe_counter!(
        "substreams_websocket_disconnections_total",
        "Lifetime count of WebSocket client disconnects, labelled by reason."
    );
    describe_gauge!(
        "substreams_websocket_active_connections",
        "Currently connected WebSocket clients."
    );
    describe_counter!(
        "substreams_websocket_rejected_connections_total",
        "Connection attempts rejected because `max_clients` was reached."
    );
    describe_histogram!(
        "substreams_websocket_connection_duration_seconds",
        Unit::Seconds,
        "Distribution of completed WebSocket connection lifetimes."
    );

    // ---- Subscription commands --------------------------------------------
    describe_counter!(
        "substreams_websocket_commands_total",
        "WebSocket JSON commands received, labelled by method + outcome."
    );

    // ---- Broadcast --------------------------------------------------------
    describe_counter!(
        "substreams_websocket_broadcast_blocks_total",
        "Per-table block payloads dispatched, labelled by network + table."
    );
    describe_counter!(
        "substreams_websocket_broadcast_events_total",
        "Total events surfaced across all per-table broadcasts."
    );
    describe_counter!(
        "substreams_websocket_broadcast_delivered_total",
        "Per-client `try_send` successes summed across broadcasts."
    );
    describe_counter!(
        "substreams_websocket_broadcast_dropped_total",
        "Per-client `try_send` failures (saturated outbound buffer)."
    );
    describe_counter!(
        "substreams_websocket_broadcast_filtered_skipped_total",
        "Per-client broadcasts skipped because the client's filter dropped \
         every event in the block."
    );
    describe_counter!(
        "substreams_websocket_force_closed_total",
        "Connections force-closed for backpressure (Close 1013)."
    );
    describe_counter!(
        "substreams_websocket_lifecycle_broadcasts_total",
        "Lifecycle messages dispatched (started, error, completed, undo, ...)."
    );

    // ---- Substreams reader ------------------------------------------------
    describe_counter!(
        "substreams_websocket_substreams_blocks_total",
        "Substreams block events received from upstream, labelled by stream."
    );
    describe_counter!(
        "substreams_websocket_substreams_errors_total",
        "Substreams read failures, labelled by stream + kind (transient | \
         fatal | decode)."
    );
    describe_counter!(
        "substreams_websocket_substreams_undo_total",
        "BlockUndoSignal events received from upstream."
    );
    describe_counter!(
        "substreams_websocket_substreams_reconnects_total",
        "Substreams gRPC reconnect attempts after an error."
    );
    describe_gauge!(
        "substreams_websocket_head_block_number",
        "Latest block number observed per stream-table. Labelled by \
         stream/network/table/spkg/endpoint."
    );
    describe_gauge!(
        "substreams_websocket_head_block_time_drift",
        Unit::Seconds,
        "Lag (now - block_timestamp) in seconds for the latest block per \
         stream-table. Labelled by stream/network/table/spkg/endpoint."
    );

    // ---- Block hot-path ---------------------------------------------------
    describe_counter!(
        "substreams_websocket_blocks_skipped_total",
        "Blocks whose decode/serialize/fan-out was skipped because the stream \
         had no subscribers (the feed is live-only)."
    );

    // ---- Cursor store -----------------------------------------------------
    describe_counter!(
        "substreams_websocket_cursor_saves_total",
        "Cursor persistence operations, labelled by outcome (success | error)."
    );

    // ---- Shutdown / drain -------------------------------------------------
    describe_counter!(
        "substreams_websocket_drain_initiated_total",
        "Number of times graceful drain has been triggered."
    );
    describe_gauge!(
        "substreams_websocket_draining",
        "1 while `/healthz` is returning 503 during shutdown drain, 0 otherwise."
    );
}
