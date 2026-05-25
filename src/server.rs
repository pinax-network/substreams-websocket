use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::{RwLock, mpsc, oneshot},
    time::Instant,
};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, trace, warn};

use crate::{
    BlockContext, Config, CursorStore, EventFilter, EventFilterSet, ReplayLog,
    SUPPORTED_OUTPUT_TYPE, StreamConfig, StreamEvent, SubstreamsClient, apply_filter_in_place,
    compute_module_hash_hex, decode_database_changes, substreams::load_package,
};

type ClientId = u64;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },

    #[error("server error: {0}")]
    Serve(#[from] std::io::Error),
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    clients: ClientRegistry,
    /// Per-stream metadata visible to WebSocket clients in the welcome
    /// message. Indexes match `config.streams`. Empty when a stream's
    /// package failed to load (hash will be empty string).
    streams_meta: Arc<Vec<StreamMeta>>,
    replay: ReplayLog,
    /// Set on SIGTERM/SIGINT. While true, `/healthz` returns 503 so a
    /// reverse proxy (Envoy, nginx, ALB) can drain this replica before
    /// the WebSocket drain completes.
    draining: Arc<AtomicBool>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct StreamMeta {
    network: String,
    module: String,
    manifest: String,
    module_hash: String,
    /// Top-level spkg name from Package.package_meta[0].name. Required.
    package_name: String,
    /// Top-level spkg version from Package.package_meta[0].version. Required.
    package_version: String,
    /// Package description sourced from PackageMetadata.description or PackageMetadata.doc.
    #[serde(skip_serializing_if = "String::is_empty")]
    description: String,
    /// Operator-declared list of DatabaseChanges tables this spkg is expected
    /// to emit. Lets clients discover available `<network>@<table>` channels
    /// from the welcome message without waiting for events.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tables: Vec<String>,
}

pub async fn serve(config: Config) -> Result<(), ServerError> {
    serve_with_shutdown(config, shutdown_signal()).await
}

pub async fn serve_with_shutdown(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    let config = Arc::new(config);

    // Install the Prometheus recorder before any code path increments a
    // counter, so all metrics land in the same registry that `/metrics`
    // renders. Safe to call repeatedly — first call wins.
    crate::metrics::init();

    // Pre-load every Substreams package in parallel so the welcome message can
    // expose each stream's module_hash. Failed loads land as empty `StreamMeta`
    // here and re-fail inside the per-stream task with a clear error to clients.
    let prepared = prepare_streams(&config).await;
    let streams_meta = Arc::new(
        prepared
            .iter()
            .map(|prep| prep.meta.clone())
            .collect::<Vec<_>>(),
    );

    let replay = ReplayLog::new(config.replay.dir.clone(), config.replay.max_seconds);

    let clients = ClientRegistry::default();
    clients.set_slow_client_drop_limit(config.websocket.slow_client_drop_limit);

    let state = AppState {
        config: config.clone(),
        clients,
        streams_meta,
        replay,
        draining: Arc::new(AtomicBool::new(false)),
    };

    let listen = state.config.websocket.listen;
    let ws_path = state.config.websocket.ws_path.clone();
    let health_path = state.config.websocket.health_path.clone();
    let drain_timeout = state.config.websocket.shutdown_drain_timeout;
    let clients = state.clients.clone();
    let draining = state.draining.clone();
    let stream_tasks = spawn_streams(&state, prepared);
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|source| ServerError::Bind {
            addr: listen,
            source,
        })?;

    // Wrap the caller-provided shutdown so we drain WebSocket clients before
    // axum's graceful_shutdown stops accepting connections. Order matters:
    // (1) wait for caller signal, (2) send Close to each client, (3) wait up
    // to drain_timeout for them to disconnect, (4) yield → axum tears down.
    let shutdown_with_drain = async move {
        shutdown.await;
        // Flip /healthz to 503 first so reverse proxies (Envoy, nginx, ALB)
        // mark this replica unhealthy and stop routing new connections to it
        // before we send Close frames.
        draining.store(true, Ordering::SeqCst);
        metrics::counter!("substreams_websocket_drain_initiated_total").increment(1);
        metrics::gauge!("substreams_websocket_draining").set(1.0);
        clients.drain("server shutting down", drain_timeout).await;
        metrics::gauge!("substreams_websocket_draining").set(0.0);
    };

    let result = serve_listener(listener, app, ws_path, health_path, shutdown_with_drain).await;

    for task in stream_tasks {
        task.abort();
    }

    result
}

fn build_app(state: AppState) -> Router {
    // Routes match the Binance-style URL layout:
    //   GET /                               — Scalar-style API reference (in-browser try-it client)
    //   GET /SKILL.md                       — agent-oriented reference (markdown)
    //   GET /llms.txt                       — short llms.txt for AI crawlers
    //   GET <health_path>                   — health check
    //   GET <ws_path>                       — connect with no streams (errors)
    //   GET <ws_path>/{*streams}            — path mode: /ws/<a>/<b>/...
    //   GET <stream_path>?streams=<a>/<b>   — query mode: always wrapped envelope
    let ws_root = state.config.websocket.ws_path.clone();
    let ws_wildcard = format!("{}/{{*streams}}", ws_root.trim_end_matches('/'));
    let stream_path = state.config.websocket.stream_path.clone();
    let metrics_path = state.config.websocket.metrics_path.clone();
    let mut router = Router::new()
        .route("/", get(landing_html))
        .route("/streams", get(streams_json))
        .route("/SKILL.md", get(skill_md))
        .route("/llms.txt", get(llms_txt))
        .route("/favicon.ico", get(favicon_png))
        .route("/favicon.png", get(favicon_png))
        .route(&state.config.websocket.health_path, get(health))
        .route(&ws_root, get(websocket_no_streams))
        .route(&ws_wildcard, get(websocket_path))
        .route(&stream_path, get(websocket_stream_query));
    if !metrics_path.is_empty() {
        router = router.route(&metrics_path, get(metrics_endpoint));
    }
    router.layer(TraceLayer::new_for_http()).with_state(state)
}

async fn metrics_endpoint() -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        crate::metrics::render(),
    )
}

const LANDING_HTML: &str = include_str!("../public/index.html");
const SKILL_MD: &str = include_str!("../public/SKILL.md");
const LLMS_TXT: &str = include_str!("../public/llms.txt");
const FAVICON_PNG: &[u8] = include_bytes!("../public/favicon.png");

async fn landing_html() -> impl IntoResponse {
    ([("content-type", "text/html; charset=utf-8")], LANDING_HTML)
}

async fn skill_md() -> impl IntoResponse {
    ([("content-type", "text/markdown; charset=utf-8")], SKILL_MD)
}

async fn llms_txt() -> impl IntoResponse {
    ([("content-type", "text/plain; charset=utf-8")], LLMS_TXT)
}

async fn streams_json(State(state): State<AppState>) -> impl IntoResponse {
    let body =
        serde_json::to_string(state.streams_meta.as_ref()).unwrap_or_else(|_| "[]".to_owned());
    ([("content-type", "application/json; charset=utf-8")], body)
}

async fn favicon_png() -> impl IntoResponse {
    (
        [
            ("content-type", "image/png"),
            ("cache-control", "public, max-age=86400"),
        ],
        FAVICON_PNG,
    )
}

/// Outcome of pre-loading a single stream's package and computing its module
/// hash. On failure, `package` is `None` and `error` carries the cause —
/// the per-stream task will re-report it to clients.
struct PreparedStream {
    config: StreamConfig,
    meta: StreamMeta,
    package: Option<crate::substreams::pb::sf::substreams::v1::Package>,
    error: Option<String>,
}

async fn prepare_streams(config: &Config) -> Vec<PreparedStream> {
    let prepares = config.streams.iter().cloned().map(prepare_stream);
    futures_util::future::join_all(prepares).await
}

async fn prepare_stream(stream: StreamConfig) -> PreparedStream {
    let network = stream.substreams.network.clone().unwrap_or_default();
    let manifest = stream.substreams.manifest.clone();
    let module = stream.substreams.module.clone();
    let tables = stream.tables.clone();

    let make_meta = |module_hash: String,
                     pkg: Option<&crate::substreams::pb::sf::substreams::v1::Package>|
     -> StreamMeta {
        let (pkg_name, pkg_version, description) = pkg
            .and_then(|p| p.package_meta.first())
            .map(|m| {
                (
                    m.name.clone(),
                    m.version.clone(),
                    if !m.description.is_empty() {
                        m.description.clone()
                    } else {
                        m.doc.clone()
                    },
                )
            })
            .unwrap_or_default();
        StreamMeta {
            network: network.clone(),
            module: module.clone(),
            manifest: manifest.clone(),
            module_hash,
            package_name: pkg_name,
            package_version: pkg_version,
            description,
            tables: tables.clone(),
        }
    };

    let package = match load_package(&manifest).await {
        Ok(package) => package,
        Err(error) => {
            return PreparedStream {
                config: stream,
                meta: make_meta(String::new(), None),
                package: None,
                error: Some(error.to_string()),
            };
        }
    };

    let Some(modules_pb) = package.modules.as_ref() else {
        return PreparedStream {
            config: stream,
            meta: make_meta(String::new(), Some(&package)),
            package: None,
            error: Some("package contains no modules".to_owned()),
        };
    };

    // Require non-empty package_name + package_version so cursor + replay
    // file naming is unambiguous. Without these, two unrelated spkgs could
    // collide on `<network>-@-<hash>` and trash each other's state.
    let pkg_meta = package.package_meta.first();
    let pkg_name = pkg_meta.map(|m| m.name.as_str()).unwrap_or("");
    let pkg_version = pkg_meta.map(|m| m.version.as_str()).unwrap_or("");
    if pkg_name.is_empty() || pkg_version.is_empty() {
        return PreparedStream {
            config: stream,
            meta: make_meta(String::new(), Some(&package)),
            package: None,
            error: Some(
                "package metadata must include both name and version (Package.package_meta[0])"
                    .to_owned(),
            ),
        };
    }

    let module_def = modules_pb.modules.iter().find(|m| m.name == module);
    let output_type = module_def
        .and_then(|m| m.output.as_ref().map(|o| o.r#type.clone()))
        .unwrap_or_default();
    if output_type != SUPPORTED_OUTPUT_TYPE {
        return PreparedStream {
            config: stream,
            meta: make_meta(String::new(), Some(&package)),
            package: None,
            error: Some(format!(
                "module {module:?} output type {output_type:?} is not supported; only {SUPPORTED_OUTPUT_TYPE} is accepted"
            )),
        };
    }

    match compute_module_hash_hex(modules_pb, &module) {
        Ok(module_hash) => PreparedStream {
            config: stream,
            meta: make_meta(module_hash, Some(&package)),
            package: Some(package),
            error: None,
        },
        Err(error) => PreparedStream {
            config: stream,
            meta: make_meta(String::new(), Some(&package)),
            package: None,
            error: Some(error.to_string()),
        },
    }
}

fn spawn_streams(
    state: &AppState,
    prepared: Vec<PreparedStream>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let cursors = CursorStore::new(state.config.cursors_dir.clone());
    let replay = state.replay.clone();
    prepared
        .into_iter()
        .map(|prep| {
            let clients = state.clients.clone();
            let cursors = cursors.clone();
            let replay = replay.clone();
            tokio::spawn(async move {
                run_substream(prep, clients, cursors, replay).await;
            })
        })
        .collect()
}

/// Initial backoff after an error. Doubles each failure, capped at
/// `RESTART_BACKOFF_MAX`. Resets to `RESTART_BACKOFF_MIN` whenever the
/// stream gets past the first connect.
const RESTART_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(15);

/// Runtime identity for one Substreams source. Used for cursor / replay
/// file naming and as the spkg-side breadcrumb in lifecycle messages.
#[derive(Debug, Clone)]
struct StreamIdentity {
    network: String,
    package_name: String,
    package_version: String,
    module_hash: String,
    /// Manifest path (local file or HTTPS URL). Surfaced as the `spkg`
    /// label on per-stream gauges.
    manifest: String,
    /// gRPC endpoint we read from. Surfaced as the `endpoint` label on
    /// per-stream gauges.
    endpoint: String,
    /// Operator-declared table names this stream emits. Used to label
    /// per-stream gauges even when a given block contains no rows for a
    /// table. Falls back to tables observed in a block when empty.
    tables: Vec<String>,
}

impl StreamIdentity {
    fn display(&self) -> String {
        format!(
            "{}-{}@{}-{}",
            self.network, self.package_name, self.package_version, self.module_hash
        )
    }
}

async fn run_substream(
    prep: PreparedStream,
    clients: ClientRegistry,
    cursors: CursorStore,
    replay: ReplayLog,
) {
    let PreparedStream {
        mut config,
        meta,
        package,
        error,
    } = prep;
    let identity = StreamIdentity {
        network: meta.network.clone(),
        package_name: meta.package_name.clone(),
        package_version: meta.package_version.clone(),
        module_hash: meta.module_hash.clone(),
        manifest: meta.manifest.clone(),
        endpoint: config.substreams.endpoint.clone().unwrap_or_default(),
        tables: meta.tables.clone(),
    };

    if let Some(error) = error {
        error!(stream = %identity.display(), error, "stream preparation failed");
        clients
            .broadcast_lifecycle(stream_status(&identity, "error", error))
            .await;
        return;
    }
    let package = package.expect("prepared stream without error must have package");

    let mut backoff = RESTART_BACKOFF_MIN;

    loop {
        // Reload cursor from disk on every retry so we resume from the latest
        // persisted position, not whatever the previous run started with.
        match cursors
            .load(
                &identity.network,
                &identity.package_name,
                &identity.package_version,
                &identity.module_hash,
            )
            .await
        {
            Ok(Some(cursor)) => {
                info!(
                    stream = %identity.display(),
                    cursor_len = cursor.len(),
                    "resuming Substreams read from persisted cursor"
                );
                config.substreams.start_cursor = Some(cursor);
            }
            Ok(None) => {
                config.substreams.start_cursor = None;
            }
            Err(error) => {
                warn!(stream = %identity.display(), %error, "failed to load cursor; starting from configured block");
                config.substreams.start_cursor = None;
            }
        }

        info!(
            stream = %identity.display(),
            module = %config.substreams.module,
            manifest = %config.substreams.manifest,
            start_block = ?config.substreams.start_block,
            has_cursor = config.substreams.start_cursor.is_some(),
            "starting Substreams read"
        );

        let client = SubstreamsClient::new(config.substreams.clone());
        let mut substream = match client.stream_with_package(package.clone()).await {
            Ok(substream) => substream,
            Err(error) => {
                let msg = error.to_string();
                error!(stream = %identity.display(), %error, backoff_secs = backoff.as_secs(), "Substreams read failed to start; will retry");
                clients
                    .broadcast_lifecycle(stream_status(&identity, "error", msg))
                    .await;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
                continue;
            }
        };

        clients
            .broadcast_lifecycle(stream_status(&identity, "started", String::new()))
            .await;

        let outcome = read_loop(&identity, &clients, &cursors, &replay, &mut substream).await;
        match outcome {
            ReadOutcome::Completed => {
                info!(stream = %identity.display(), "Substreams read completed");
                clients
                    .broadcast_lifecycle(stream_status(&identity, "completed", String::new()))
                    .await;
                return;
            }
            ReadOutcome::ProducedBlock => {
                // We made progress this attempt: reset backoff before the next retry.
                backoff = RESTART_BACKOFF_MIN;
                error!(stream = %identity.display(), "Substreams read stream ended unexpectedly; will retry");
                metrics::counter!(
                    "substreams_websocket_substreams_errors_total",
                    "network" => identity.network.clone(),
                    "package_name" => identity.package_name.clone(),
                    "package_version" => identity.package_version.clone(),
                    "kind" => "transient"
                )
                .increment(1);
                metrics::counter!(
                    "substreams_websocket_substreams_reconnects_total",
                    "network" => identity.network.clone(),
                    "package_name" => identity.package_name.clone(),
                    "package_version" => identity.package_version.clone()
                )
                .increment(1);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
            }
            ReadOutcome::Errored(err) => {
                error!(stream = %identity.display(), error = %err, backoff_secs = backoff.as_secs(), "Substreams read failed; will retry");
                clients
                    .broadcast_lifecycle(stream_status(&identity, "error", err))
                    .await;
                metrics::counter!(
                    "substreams_websocket_substreams_errors_total",
                    "network" => identity.network.clone(),
                    "package_name" => identity.package_name.clone(),
                    "package_version" => identity.package_version.clone(),
                    "kind" => "fatal"
                )
                .increment(1);
                metrics::counter!(
                    "substreams_websocket_substreams_reconnects_total",
                    "network" => identity.network.clone(),
                    "package_name" => identity.package_name.clone(),
                    "package_version" => identity.package_version.clone()
                )
                .increment(1);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
            }
        }
    }
}

enum ReadOutcome {
    /// Server closed the stream cleanly (e.g. reached stop_block).
    Completed,
    /// Stream ended without an explicit error but we did produce at least one
    /// block — treat as a retryable disconnect.
    ProducedBlock,
    /// Stream returned an error.
    Errored(String),
}

async fn read_loop(
    identity: &StreamIdentity,
    clients: &ClientRegistry,
    cursors: &CursorStore,
    replay: &ReplayLog,
    substream: &mut crate::substreams::SubstreamsStream,
) -> ReadOutcome {
    let mut produced_any = false;
    loop {
        match substream.next_event().await {
            Ok(Some(event)) => {
                if matches!(event, StreamEvent::Block { .. }) {
                    produced_any = true;
                }
                handle_substream_event(identity, clients, cursors, replay, event).await;
            }
            Ok(None) => {
                return if produced_any {
                    ReadOutcome::ProducedBlock
                } else {
                    ReadOutcome::Completed
                };
            }
            Err(error) => return ReadOutcome::Errored(error.to_string()),
        }
    }
}

async fn handle_substream_event(
    identity: &StreamIdentity,
    clients: &ClientRegistry,
    cursors: &CursorStore,
    replay: &ReplayLog,
    event: StreamEvent,
) {
    match event {
        StreamEvent::Block {
            number,
            id,
            timestamp,
            timestamp_seconds,
            output_type_url: _,
            payload,
            cursor,
        } => {
            let context = BlockContext {
                block_num: number,
                block_hash: id,
                timestamp,
                timestamp_seconds,
                network: identity.network.clone(),
                module_hash: identity.module_hash.clone(),
            };
            metrics::counter!(
                "substreams_websocket_substreams_blocks_total",
                "network" => identity.network.clone(),
                "package_name" => identity.package_name.clone(),
                "package_version" => identity.package_version.clone()
            )
            .increment(1);
            let decoded = match decode_database_changes(&payload, context) {
                Ok(decoded) => decoded,
                Err(error) => {
                    warn!(stream = %identity.display(), %error, "failed to decode Substreams block output");
                    metrics::counter!(
                        "substreams_websocket_substreams_errors_total",
                        "network" => identity.network.clone(),
                        "package_name" => identity.package_name.clone(),
                        "package_version" => identity.package_version.clone(),
                        "kind" => "decode"
                    )
                    .increment(1);
                    clients
                        .broadcast_lifecycle(stream_status(
                            identity,
                            "decode_error",
                            error.to_string(),
                        ))
                        .await;
                    return;
                }
            };

            update_head_block_gauges(identity, &decoded);

            if !decoded.events.is_empty() {
                let block_num = decoded.block_num;
                let total_events = decoded.events.len();
                let block_value = match serde_json::to_value(&decoded) {
                    Ok(value) => value,
                    Err(error) => {
                        warn!(stream = %identity.display(), %error, "failed to serialize decoded block");
                        return;
                    }
                };

                // Persist the whole block (mixed tables) keyed by spkg
                // provenance. Replay readers split per-table at scan time.
                let block_text = block_value.to_string();
                let block_bytes = block_text.len() as u64;
                if let Err(error) = replay
                    .append(
                        &identity.network,
                        &identity.package_name,
                        &identity.package_version,
                        &identity.module_hash,
                        decoded.timestamp_seconds,
                        &block_text,
                    )
                    .await
                {
                    warn!(stream = %identity.display(), %error, "failed to append replay log");
                    metrics::counter!(
                        "substreams_websocket_replay_appends_total",
                        "network" => identity.network.clone(),
                        "package_name" => identity.package_name.clone(),
                        "package_version" => identity.package_version.clone(),
                        "outcome" => "error"
                    )
                    .increment(1);
                } else {
                    metrics::counter!(
                        "substreams_websocket_replay_appends_total",
                        "network" => identity.network.clone(),
                        "package_name" => identity.package_name.clone(),
                        "package_version" => identity.package_version.clone(),
                        "outcome" => "success"
                    )
                    .increment(1);
                    metrics::counter!(
                        "substreams_websocket_replay_append_bytes_total",
                        "network" => identity.network.clone(),
                        "package_name" => identity.package_name.clone(),
                        "package_version" => identity.package_version.clone()
                    )
                    .increment(block_bytes);
                }

                // Group events by @table and broadcast one per-table payload
                // per group. Clients subscribed to (network, table) match.
                let groups = group_events_by_table(&decoded);
                for (table, events) in &groups {
                    let per_table = build_table_payload(&decoded, table, events);
                    let delivered = clients
                        .broadcast_block(&identity.network, table, per_table)
                        .await;
                    debug!(
                        stream = %identity.display(),
                        table,
                        block_num,
                        events = events.len(),
                        delivered,
                        "broadcast table payload"
                    );
                }
                let _ = total_events; // surfaced via per-table debug above
            }

            if let Err(error) = cursors
                .save(
                    &identity.network,
                    &identity.package_name,
                    &identity.package_version,
                    &identity.module_hash,
                    &cursor,
                )
                .await
            {
                warn!(stream = %identity.display(), %error, "failed to persist Substreams cursor");
                metrics::counter!(
                    "substreams_websocket_cursor_saves_total",
                    "network" => identity.network.clone(),
                    "package_name" => identity.package_name.clone(),
                    "package_version" => identity.package_version.clone(),
                    "outcome" => "error"
                )
                .increment(1);
            } else {
                metrics::counter!(
                    "substreams_websocket_cursor_saves_total",
                    "network" => identity.network.clone(),
                    "package_name" => identity.package_name.clone(),
                    "package_version" => identity.package_version.clone(),
                    "outcome" => "success"
                )
                .increment(1);
            }
        }
        StreamEvent::Fatal { message } => {
            clients
                .broadcast_lifecycle(stream_status(identity, "fatal", message))
                .await;
        }
        StreamEvent::Undo {
            last_valid_block,
            last_valid_cursor,
        } => {
            metrics::counter!(
                "substreams_websocket_substreams_undo_total",
                "network" => identity.network.clone(),
                "package_name" => identity.package_name.clone(),
                "package_version" => identity.package_version.clone()
            )
            .increment(1);
            clients
                .broadcast_lifecycle(serde_json::json!({
                    "type": "stream",
                    "status": "undo",
                    "network": identity.network,
                    "package_name": identity.package_name,
                    "package_version": identity.package_version,
                    "module_hash": identity.module_hash,
                    "last_valid_block": last_valid_block,
                }))
                .await;
            if let Err(error) = cursors
                .save(
                    &identity.network,
                    &identity.package_name,
                    &identity.package_version,
                    &identity.module_hash,
                    &last_valid_cursor,
                )
                .await
            {
                warn!(stream = %identity.display(), %error, "failed to persist last-valid cursor");
            }
            if let Err(error) = replay
                .truncate_after_block(
                    &identity.network,
                    &identity.package_name,
                    &identity.package_version,
                    &identity.module_hash,
                    last_valid_block,
                )
                .await
            {
                warn!(stream = %identity.display(), %error, "failed to truncate replay log after reorg");
            }
        }
        StreamEvent::Session { .. }
        | StreamEvent::Progress { .. }
        | StreamEvent::SnapshotData { .. }
        | StreamEvent::SnapshotComplete
        | StreamEvent::Unknown => {}
    }
}

/// Update per-stream head-block gauges. One block applies to every table
/// emitted by the same spkg, so we update gauges for each operator-declared
/// table (or, when none are declared, the set of tables that appear in this
/// block's events). Drift is `max(0, now - block_timestamp)` seconds.
fn update_head_block_gauges(
    identity: &StreamIdentity,
    decoded: &crate::DatabaseChangesBlockMessage,
) {
    // `as_secs_f64` preserves sub-second precision on `now`. Block
    // timestamps themselves are integer seconds, so drift granularity is
    // bounded by that — but for slow blocks the fractional part of `now`
    // matters. Negative drift is passed through (not clamped): it surfaces
    // clock skew between this server and the block producer, which is a
    // real signal operators may want to alert on.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let drift = now_secs - decoded.timestamp_seconds as f64;
    let block_num = decoded.block_num as f64;

    // Borrow declared tables when present; only allocate when we need the
    // fallback (no operator-declared tables — derive from event `@table`s).
    let observed: Vec<String>;
    let tables: &[String] = if !identity.tables.is_empty() {
        &identity.tables
    } else {
        let mut acc: Vec<String> = Vec::new();
        for ev in &decoded.events {
            if let Some(t) = ev.get("@table").and_then(serde_json::Value::as_str)
                && !acc.iter().any(|s| s == t)
            {
                acc.push(t.to_owned());
            }
        }
        observed = acc;
        &observed
    };

    for table in tables {
        let stream_label = format!("{}@{}", identity.network, table);
        metrics::gauge!(
            "substreams_websocket_head_block_number",
            "stream" => stream_label.clone(),
            "network" => identity.network.clone(),
            "table" => table.clone(),
            "spkg" => identity.manifest.clone(),
            "endpoint" => identity.endpoint.clone(),
        )
        .set(block_num);
        metrics::gauge!(
            "substreams_websocket_head_block_time_drift",
            "stream" => stream_label,
            "network" => identity.network.clone(),
            "table" => table.clone(),
            "spkg" => identity.manifest.clone(),
            "endpoint" => identity.endpoint.clone(),
        )
        .set(drift);
    }
}

/// Group decoded events by their `@table` field. Returns an order-preserving
/// list of `(table_name, events)` pairs — events keep the order they had in
/// the original block, only the per-table grouping is added.
fn group_events_by_table(
    decoded: &crate::DatabaseChangesBlockMessage,
) -> Vec<(String, Vec<&serde_json::Map<String, serde_json::Value>>)> {
    let mut groups: Vec<(String, Vec<&serde_json::Map<String, serde_json::Value>>)> = Vec::new();
    for event in &decoded.events {
        let Some(table) = event.get("@table").and_then(serde_json::Value::as_str) else {
            continue;
        };
        match groups.iter_mut().find(|(t, _)| t == table) {
            Some((_, vec)) => vec.push(event),
            None => groups.push((table.to_owned(), vec![event])),
        }
    }
    groups
}

/// Build the per-table broadcast payload. Drops the per-event `@table`
/// prefix since the whole payload is now scoped to one table.
fn build_table_payload(
    decoded: &crate::DatabaseChangesBlockMessage,
    table: &str,
    events: &[&serde_json::Map<String, serde_json::Value>],
) -> serde_json::Value {
    // Build the per-event map by iterating the source in order and skipping
    // `@table` — `serde_json::Map::remove` under the `preserve_order`
    // feature is `swap_remove`, which would scramble field order by moving
    // the last entry into the `@table` slot.
    let mut out_events = Vec::with_capacity(events.len());
    for event in events {
        let mut stripped = serde_json::Map::with_capacity(event.len().saturating_sub(1));
        for (k, v) in event.iter() {
            if k == "@table" {
                continue;
            }
            stripped.insert(k.clone(), v.clone());
        }
        out_events.push(serde_json::Value::Object(stripped));
    }
    serde_json::json!({
        "network": decoded.network,
        "table": table,
        "block_num": decoded.block_num,
        "block_hash": decoded.block_hash,
        "timestamp": decoded.timestamp,
        "timestamp_seconds": decoded.timestamp_seconds,
        "module_hash": decoded.module_hash,
        "events": out_events,
    })
}

fn stream_status(identity: &StreamIdentity, status: &str, message: String) -> serde_json::Value {
    let mut value = serde_json::json!({
        "type": "stream",
        "status": status,
        "network": identity.network,
        "package_name": identity.package_name,
        "package_version": identity.package_version,
        "module_hash": identity.module_hash,
    });

    if !message.is_empty() {
        value["message"] = serde_json::Value::String(message);
    }

    value
}

async fn serve_listener(
    listener: tokio::net::TcpListener,
    app: Router,
    ws_path: String,
    health_path: String,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    let listen = listener.local_addr().map_err(ServerError::Serve)?;

    info!(
        listen = %listen,
        ws_path = %ws_path,
        health_path = %health_path,
        "starting Substreams WebSocket server"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServerError::Serve)
}

async fn health(State(state): State<AppState>) -> Response {
    if state.draining.load(Ordering::SeqCst) {
        (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response()
    } else {
        (StatusCode::OK, "ok").into_response()
    }
}

/// Bare `/ws` connect without any streams in the path. Rejected — clients
/// must list streams either in the URL path or use `/stream?streams=`.
async fn websocket_no_streams(State(_state): State<AppState>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        "at least one `network@stream` selector is required: either /ws/<network@stream> or /stream?streams=<network@stream>",
    )
        .into_response()
}

/// Path mode: `/ws/<network@stream>` (single) or `/ws/<a>/<b>/...` (multi).
/// Single-stream connections receive raw payloads; multi-stream wraps each
/// outgoing message in `{"stream":"...","data":...}`.
async fn websocket_path(
    State(state): State<AppState>,
    axum::extract::Path(streams_path): axum::extract::Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    if state.clients.active_count().await >= state.config.websocket.max_clients {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let entries = match parse_stream_list(&streams_path) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let from_timestamp = match parse_from_timestamp(raw_query.as_deref()) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };

    let event_filters = match parse_filter_query(
        raw_query.as_deref(),
        &entries,
        state.config.websocket.max_filter_fields,
        state.config.websocket.max_filter_values,
    ) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let wrap_envelope = entries.len() > 1;
    let filter = StreamFilter {
        entries,
        wrap_envelope,
        event_filters,
    };
    ws.on_upgrade(move |socket| handle_socket(state, filter, from_timestamp, socket))
        .into_response()
}

/// Query mode: `/stream?streams=<a>/<b>/...`. Always wraps payloads in the
/// `{"stream":"...","data":...}` envelope, matching Binance combined streams.
async fn websocket_stream_query(
    State(state): State<AppState>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    if state.clients.active_count().await >= state.config.websocket.max_clients {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let raw = raw_query.unwrap_or_default();
    let streams_value = url_query_pairs(&raw)
        .find(|(k, _)| k == "streams")
        .map(|(_, v)| v)
        .unwrap_or_default();
    if streams_value.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "missing `streams` query parameter; expected `?streams=<network@stream>[/<...>]`",
        )
            .into_response();
    }
    let entries = match parse_stream_list(&streams_value) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let from_timestamp = match parse_from_timestamp(Some(&raw)) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let event_filters = match parse_filter_query(
        Some(&raw),
        &entries,
        state.config.websocket.max_filter_fields,
        state.config.websocket.max_filter_values,
    ) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let filter = StreamFilter {
        entries,
        wrap_envelope: true,
        event_filters,
    };
    ws.on_upgrade(move |socket| handle_socket(state, filter, from_timestamp, socket))
        .into_response()
}

/// Parse `?filter=<url-encoded-json>` on the WS upgrade URL. The filter
/// object is applied to every explicit `network@stream` entry in the URL
/// (wildcards are skipped — they always pass every event through).
/// Returns an empty `EventFilterSet` when `?filter=` is absent.
fn parse_filter_query(
    raw_query: Option<&str>,
    entries: &[StreamId],
    max_fields: usize,
    max_values: usize,
) -> Result<EventFilterSet, String> {
    let Some(raw) = raw_query else {
        return Ok(EventFilterSet::default());
    };
    let Some((_, value)) = url_query_pairs(raw).find(|(k, _)| k == "filter") else {
        return Ok(EventFilterSet::default());
    };
    if value.is_empty() {
        return Ok(EventFilterSet::default());
    }
    let filter = EventFilter::from_str(&value, max_fields, max_values)
        .map_err(|e| format!("invalid `filter` query: {e}"))?;
    let mut set = EventFilterSet::default();
    // Store the filter under each subscribed selector verbatim, wildcards
    // included. `EventFilterSet::matching` resolves wildcards at broadcast
    // time, so `/ws/*@*?filter=...` applies the filter to every channel.
    for entry in entries {
        set.set(entry.to_wire(), filter.clone());
    }
    Ok(set)
}

/// Replay buffered blocks for every explicit `<network>@<table>` selector
/// with `timestamp_seconds > from_timestamp`. Wildcard selectors are skipped
/// — replay is anchored to a single `(network, table)` pair. For each
/// explicit selector with at least one retained block, either replay matching
/// blocks (oldest first) or emit a `gap` lifecycle message when
/// `from_timestamp` falls below the oldest retained timestamp.
async fn replay_for_client(
    replay: &ReplayLog,
    filter: &StreamFilter,
    from_timestamp: i64,
    client_id: ClientId,
    outbound: &mpsc::Sender<Message>,
) -> Result<(), String> {
    if !replay.is_enabled() {
        return Ok(());
    }

    for entry in &filter.entries {
        let (Some(network), Some(table)) = (entry.network.as_deref(), entry.stream.as_deref())
        else {
            // Wildcards skipped — no single `(network, table)` to resume on.
            continue;
        };
        let stream = table;

        let result = replay
            .read_from(Some(network), Some(stream), from_timestamp)
            .await
            .map_err(|e| e.to_string())?;

        let oldest = match result.oldest {
            Some(v) => v,
            None => continue,
        };

        if from_timestamp < oldest {
            // Resume point below the retained window — tell client there is a
            // gap and continue with live stream only.
            let gap = serde_json::json!({
                "type": "stream",
                "status": "gap",
                "network": network,
                "table": stream,
                "requested_timestamp": from_timestamp,
                "oldest_buffered_timestamp": oldest,
                "reason": "requested timestamp outside replay window",
            });
            let text = if filter.wrap_envelope {
                format!(
                    r#"{{"stream":"{network}@{stream}","data":{}}}"#,
                    gap.to_string()
                )
            } else {
                gap.to_string()
            };
            if outbound.send(Message::Text(text.into())).await.is_err() {
                return Err("outbound channel closed during replay".to_owned());
            }
            info!(
                client_id,
                network,
                stream,
                from_timestamp,
                oldest_buffered_timestamp = oldest,
                "replay gap"
            );
            metrics::counter!(
                "substreams_websocket_replay_reads_total",
                "network" => network.to_owned(),
                "table" => stream.to_owned(),
                "outcome" => "gap"
            )
            .increment(1);
            continue;
        }

        let selector = format!("{network}@{stream}");
        let matching_filters = filter.event_filters.matching(network, stream);
        let mut replayed: usize = 0;
        for (_ts, raw_text) in result.blocks {
            let payload_text = if !matching_filters.is_empty() {
                let mut block = match serde_json::from_str::<serde_json::Value>(&raw_text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let mut remaining = 0usize;
                for f in &matching_filters {
                    remaining = apply_filter_in_place(&mut block, f);
                    if remaining == 0 {
                        break;
                    }
                }
                if remaining == 0 {
                    continue;
                }
                block.to_string()
            } else {
                raw_text
            };
            let text = if filter.wrap_envelope {
                format!(r#"{{"stream":"{selector}","data":{payload_text}}}"#)
            } else {
                payload_text
            };
            if outbound.send(Message::Text(text.into())).await.is_err() {
                return Err("outbound channel closed during replay".to_owned());
            }
            replayed += 1;
        }

        if replayed > 0 {
            info!(
                client_id,
                network, stream, from_timestamp, replayed, "replay delivered"
            );
            metrics::counter!(
                "substreams_websocket_replay_reads_total",
                "network" => network.to_owned(),
                "table" => stream.to_owned(),
                "outcome" => "replayed"
            )
            .increment(1);
            metrics::counter!(
                "substreams_websocket_replay_blocks_delivered_total",
                "network" => network.to_owned(),
                "table" => stream.to_owned()
            )
            .increment(replayed as u64);
        } else {
            metrics::counter!(
                "substreams_websocket_replay_reads_total",
                "network" => network.to_owned(),
                "table" => stream.to_owned(),
                "outcome" => "empty"
            )
            .increment(1);
        }
    }

    Ok(())
}

/// Parse `?from_timestamp=<n>`. Accepts a Unix epoch seconds integer or an
/// ISO 8601 / RFC 3339 UTC timestamp string (subset: `YYYY-MM-DD HH:MM:SS`,
/// `YYYY-MM-DDTHH:MM:SS`, optional trailing `Z`).
fn parse_from_timestamp(raw_query: Option<&str>) -> Result<Option<i64>, String> {
    let Some(raw) = raw_query else {
        return Ok(None);
    };
    let Some((_, value)) = url_query_pairs(raw).find(|(k, _)| k == "from_timestamp") else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    if let Ok(n) = value.parse::<i64>() {
        return Ok(Some(n));
    }
    parse_iso_timestamp(&value).map(Some).ok_or_else(|| {
        format!("from_timestamp must be epoch seconds or `YYYY-MM-DD HH:MM:SS` UTC; got {value:?}")
    })
}

/// Tiny UTC timestamp parser. Accepts the formats we actually emit:
///   `YYYY-MM-DD HH:MM:SS`
///   `YYYY-MM-DDTHH:MM:SS`
///   either form with a trailing `Z`
/// Returns `None` on any deviation. Fractional seconds are not supported.
fn parse_iso_timestamp(raw: &str) -> Option<i64> {
    let s = raw.trim().trim_end_matches('Z');
    let bytes = s.as_bytes();
    if bytes.len() != 19 {
        return None;
    }
    let sep = bytes[10] as char;
    if sep != ' ' && sep != 'T' {
        return None;
    }
    let year: i32 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hour: u32 = s.get(11..13)?.parse().ok()?;
    let minute: u32 = s.get(14..16)?.parse().ok()?;
    let second: u32 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour >= 24
        || minute >= 60
        || second >= 60
    {
        return None;
    }
    Some(epoch_seconds_utc(year, month, day, hour, minute, second))
}

/// Days from civil date (Howard Hinnant's algorithm). Output is days since
/// `1970-01-01`. UTC-only — no leap-second handling.
fn epoch_seconds_utc(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> i64 {
    let year = if m <= 2 { y - 1 } else { y };
    let era = year.div_euclid(400);
    let yoe = (year - era * 400) as u32;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era as i64 * 146_097 + doe as i64 - 719_468;
    days * 86_400 + (hh as i64) * 3600 + (mm as i64) * 60 + ss as i64
}

async fn handle_socket(
    state: AppState,
    filter: StreamFilter,
    from_timestamp: Option<i64>,
    socket: WebSocket,
) {
    let Some((client, filter_handle)) = state.clients.register(&state.config, filter.clone()).await
    else {
        warn!("rejecting WebSocket client because max client count was reached");
        metrics::counter!("substreams_websocket_rejected_connections_total").increment(1);
        return;
    };

    let initial_subs: Vec<String> = filter.entries.iter().map(StreamId::to_wire).collect();
    info!(
        client_id = client.name,
        subscriptions = ?initial_subs,
        wrap_envelope = filter.wrap_envelope,
        "WebSocket client connected"
    );
    metrics::counter!("substreams_websocket_connections_total").increment(1);
    metrics::gauge!("substreams_websocket_active_connections").increment(1);

    let (mut sender, mut receiver) = socket.split();
    let mut messages = client.rx;
    let outbound = client.tx.clone();
    let client_id = client.name;
    let total_drops = client.total_drops.clone();
    let connected_at = Instant::now();
    let last_pong_at = Arc::new(RwLock::new(connected_at));
    let (disconnect_tx, mut disconnect_rx) = oneshot::channel();

    let writer = tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            if sender.send(message).await.is_err() {
                break;
            }
        }
    });

    let heartbeat = tokio::spawn(run_heartbeat(
        client_id,
        outbound.clone(),
        Arc::clone(&last_pong_at),
        state.config.websocket.heartbeat_interval,
        state.config.websocket.heartbeat_timeout,
        state.config.websocket.connection_ttl,
        connected_at,
        disconnect_tx,
    ));

    let welcome = serde_json::json!({
        "type": "session",
        "status": "connected",
        "client_id": client_id,
        "streams": state.streams_meta.as_ref(),
        "subscriptions": initial_subs,
        "wrap_envelope": filter.wrap_envelope,
    });

    if outbound
        .send(Message::Text(welcome.to_string().into()))
        .await
        .is_err()
    {
        state.clients.unregister(client_id).await;
        heartbeat.abort();
        writer.abort();
        return;
    }

    if let Some(from) = from_timestamp
        && let Err(reason) =
            replay_for_client(&state.replay, &filter, from, client_id, &outbound).await
    {
        warn!(client_id, %reason, "replay failed; continuing with live stream only");
    }

    loop {
        tokio::select! {
            message = receiver.next() => {
                let Some(message) = message else {
                    break;
                };

                match message {
                    Ok(Message::Text(text)) => {
                        debug!(client_id, %text, "received WebSocket text message");
                        let reply = handle_subscription_command(
                            client_id,
                            &filter_handle,
                            state.config.websocket.max_filter_fields,
                            state.config.websocket.max_filter_values,
                            text.as_str(),
                        )
                        .await;
                        if outbound.send(Message::Text(reply.into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        debug!("received WebSocket binary message");
                    }
                    Ok(Message::Ping(payload)) => {
                        if outbound.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {
                        *last_pong_at.write().await = Instant::now();
                        debug!(client_id, "received WebSocket pong");
                    }
                    Ok(Message::Close(_)) => break,
                    Err(error) => {
                        debug!(%error, "WebSocket client error");
                        break;
                    }
                }
            }
            reason = &mut disconnect_rx => {
                if let Ok(reason) = reason {
                    info!(client_id, %reason, "disconnecting WebSocket client");
                }
                break;
            }
        }
    }

    state.clients.unregister(client_id).await;
    heartbeat.abort();
    drop(outbound);
    let _ = writer.await;
    let duration = connected_at.elapsed();
    let drops = total_drops.load(Ordering::Relaxed);
    info!(
        client_id,
        duration_secs = duration.as_secs(),
        total_drops = drops,
        "WebSocket client disconnected"
    );
    metrics::gauge!("substreams_websocket_active_connections").decrement(1);
    metrics::counter!("substreams_websocket_disconnections_total").increment(1);
    metrics::histogram!("substreams_websocket_connection_duration_seconds")
        .record(duration.as_secs_f64());
}

/// A single `network@stream` selector. Either field may be `*` (`None`) for
/// a wildcard match.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamId {
    network: Option<String>,
    stream: Option<String>,
}

impl StreamId {
    /// Display in Binance-style `network@stream` form, with `*` for wildcards.
    fn to_wire(&self) -> String {
        format!(
            "{}@{}",
            self.network.as_deref().unwrap_or("*"),
            self.stream.as_deref().unwrap_or("*"),
        )
    }

    /// Parse a single `network@stream` string. `*` on either side is a
    /// wildcard. Rejects empty input and any value missing the `@` separator.
    fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("stream selector must not be empty".to_owned());
        }
        let (net, stream) = match trimmed.split_once('@') {
            Some((n, s)) => (n, s),
            None => {
                return Err(format!(
                    "stream selector {trimmed:?} must be `network@stream`"
                ));
            }
        };
        Ok(StreamId {
            network: parse_wildcard(net),
            stream: parse_wildcard(stream),
        })
    }

    fn matches(&self, network: &str, stream: &str) -> bool {
        self.network.as_deref().map_or(true, |n| n == network)
            && self.stream.as_deref().map_or(true, |s| s == stream)
    }
}

/// Per-client subscription set. Empty = match nothing.
#[derive(Debug, Clone, Default)]
struct StreamFilter {
    entries: Vec<StreamId>,
    /// When `true` every payload is wrapped in `{"stream":"...","data":...}`.
    /// Set by the route (`/stream` always wraps; `/ws/<one>` does not; `/ws`
    /// with multiple path segments does).
    wrap_envelope: bool,
    /// Optional per-selector event filter. Keyed by explicit
    /// `network@stream` selector (no wildcards). Wildcard subscriptions
    /// always pass every event through.
    event_filters: EventFilterSet,
}

impl StreamFilter {
    fn matches(&self, network: &str, stream: &str) -> bool {
        self.entries.iter().any(|e| e.matches(network, stream))
    }

    fn list(&self) -> Vec<String> {
        self.entries.iter().map(StreamId::to_wire).collect()
    }

    fn add(&mut self, id: StreamId) {
        if !self.entries.contains(&id) {
            self.entries.push(id);
        }
    }

    fn remove(&mut self, id: &StreamId) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e != id);
        self.entries.len() != before
    }
}

/// Parse a `/`-separated list of `network@stream` selectors (used by both the
/// `/ws/<a>/<b>/...` path and the `/stream?streams=<a>/<b>` query).
fn parse_stream_list(raw: &str) -> Result<Vec<StreamId>, String> {
    let mut out = Vec::new();
    for piece in raw.split('/') {
        if piece.is_empty() {
            continue;
        }
        out.push(StreamId::parse(piece)?);
    }
    if out.is_empty() {
        return Err(
            "at least one `network@stream` selector is required (use `*@*` for all streams)"
                .to_owned(),
        );
    }
    Ok(out)
}

fn parse_wildcard(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "*" {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// Minimal `application/x-www-form-urlencoded` parser for our query strings.
/// Avoids pulling in a full URL crate just for two query params.
fn url_query_pairs(raw: &str) -> impl Iterator<Item = (String, String)> + '_ {
    raw.split('&').filter_map(|pair| {
        if pair.is_empty() {
            return None;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        Some((percent_decode(k), percent_decode(v)))
    })
}

fn percent_decode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => out.push(' '),
            b'%' => {
                let hi = bytes.next();
                let lo = bytes.next();
                if let (Some(hi), Some(lo)) = (hi, lo)
                    && let (Some(hi), Some(lo)) = (hex_digit(hi), hex_digit(lo))
                {
                    out.push(((hi << 4) | lo) as char);
                } else {
                    out.push('%');
                    if let Some(hi) = hi {
                        out.push(hi as char);
                    }
                    if let Some(lo) = lo {
                        out.push(lo as char);
                    }
                }
            }
            other => out.push(other as char),
        }
    }
    out
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Default)]
struct ClientRegistry {
    next_id: Arc<AtomicU64>,
    clients: Arc<RwLock<HashMap<ClientId, ClientHandle>>>,
    /// Mirror of `WebSocketConfig::slow_client_drop_limit` cached in an
    /// atomic so broadcast hot-paths can read it without touching the
    /// `Config` Arc.
    slow_drop_limit: Arc<AtomicU64>,
}

impl ClientRegistry {
    /// Cache the drop limit on the registry so hot broadcast paths read it
    /// from an `AtomicU64` rather than walking the `Config` Arc.
    fn set_slow_client_drop_limit(&self, limit: u64) {
        self.slow_drop_limit.store(limit, Ordering::Relaxed);
    }

    async fn register(
        &self,
        config: &Config,
        filter: StreamFilter,
    ) -> Option<(RegisteredClient, Arc<RwLock<StreamFilter>>)> {
        let mut clients = self.clients.write().await;
        if clients.len() >= config.websocket.max_clients {
            return None;
        }

        let name = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = mpsc::channel(config.websocket.client_buffer_size);
        let filter = Arc::new(RwLock::new(filter));
        let consecutive_drops = Arc::new(AtomicU64::new(0));
        let total_drops = Arc::new(AtomicU64::new(0));
        clients.insert(
            name,
            ClientHandle {
                tx: tx.clone(),
                filter: Arc::clone(&filter),
                consecutive_drops: consecutive_drops.clone(),
                total_drops: total_drops.clone(),
            },
        );

        Some((
            RegisteredClient {
                name,
                tx,
                rx,
                total_drops,
            },
            filter,
        ))
    }

    async fn unregister(&self, name: ClientId) {
        self.clients.write().await.remove(&name);
    }

    async fn active_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Send a `Close` frame to every connected client, then wait up to
    /// `timeout` for the registry to drain. Returns the number of clients
    /// still attached when the timeout fired (zero on clean drain).
    async fn drain(&self, reason: &str, timeout: Duration) -> usize {
        use axum::extract::ws::CloseFrame;

        let snapshot: Vec<(ClientId, mpsc::Sender<Message>)> = {
            let clients = self.clients.read().await;
            clients
                .iter()
                .map(|(id, handle)| (*id, handle.tx.clone()))
                .collect()
        };

        if snapshot.is_empty() {
            return 0;
        }

        info!(
            count = snapshot.len(),
            timeout_secs = timeout.as_secs(),
            "draining WebSocket clients on shutdown"
        );

        let close = Message::Close(Some(CloseFrame {
            code: 1001, // GOING_AWAY
            reason: reason.to_owned().into(),
        }));

        for (_, tx) in &snapshot {
            let _ = tx.try_send(close.clone());
        }

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = self.clients.read().await.len();
            if remaining == 0 {
                return 0;
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    remaining,
                    "shutdown drain timeout reached; remaining clients will be force-closed"
                );
                return remaining;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Lifecycle messages (`started`, `error`, `completed`, `decode_error`,
    /// `fatal`, `undo`, `gap`) are delivered to **every** connected client
    /// regardless of stream subscription. They carry spkg provenance
    /// (`package_name`, `package_version`, `module_hash`) so clients can
    /// route on their own. Per-connection envelope wrapping is respected.
    async fn broadcast_lifecycle(&self, value: serde_json::Value) -> usize {
        let raw_text = value.to_string();
        let mut slow_to_close: Vec<ClientId> = Vec::new();
        let mut delivered: usize = 0;
        let limit = self.slow_client_drop_limit();
        {
            let clients = self.clients.read().await;
            for (client_id, client) in clients.iter() {
                let filter = client.filter.read().await;
                let text = if filter.wrap_envelope {
                    let network = value
                        .get("network")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    format!(r#"{{"stream":"{network}@__lifecycle__","data":{raw_text}}}"#)
                } else {
                    raw_text.clone()
                };
                drop(filter);
                let outcome =
                    backpressured_send(*client_id, client, Message::Text(text.into()), limit);
                if outcome.delivered {
                    delivered += 1;
                }
                if outcome.must_close {
                    slow_to_close.push(*client_id);
                }
            }
        }
        let status = value
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        metrics::counter!(
            "substreams_websocket_lifecycle_broadcasts_total",
            "status" => status.to_owned()
        )
        .increment(1);
        self.close_slow(&slow_to_close).await;
        delivered
    }

    /// Block-payload broadcast that respects per-client event filters. For
    /// each matching client without a filter, the original serialized JSON
    /// is reused. For each matching client with a filter, the block is
    /// cloned, `events[]` is filtered in place, and re-serialized. If the
    /// filter drops every event, that client is skipped entirely (no
    /// zero-event broadcasts).
    async fn broadcast_block(
        &self,
        network: &str,
        stream: &str,
        block: serde_json::Value,
    ) -> usize {
        let selector = format!("{network}@{stream}");
        let unfiltered_text = block.to_string();
        let unfiltered_wrapped = format!(r#"{{"stream":"{selector}","data":{unfiltered_text}}}"#);
        let limit = self.slow_client_drop_limit();
        let event_count = block
            .get("events")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0) as u64;
        let clients = self.clients.read().await;
        let mut delivered: usize = 0;
        let mut dropped: u64 = 0;
        let mut filter_skipped: u64 = 0;
        let mut slow_to_close: Vec<ClientId> = Vec::new();

        for (client_id, client) in clients.iter() {
            let filter = client.filter.read().await;
            if !filter.matches(network, stream) {
                continue;
            }
            let matching_filters = filter.event_filters.matching(network, stream);
            let text = if !matching_filters.is_empty() {
                let mut block_copy = block.clone();
                let mut remaining = 0usize;
                for f in &matching_filters {
                    remaining = apply_filter_in_place(&mut block_copy, f);
                    if remaining == 0 {
                        break;
                    }
                }
                if remaining == 0 {
                    filter_skipped += 1;
                    continue;
                }
                let filtered_text = block_copy.to_string();
                if filter.wrap_envelope {
                    format!(r#"{{"stream":"{selector}","data":{filtered_text}}}"#)
                } else {
                    filtered_text
                }
            } else if filter.wrap_envelope {
                unfiltered_wrapped.clone()
            } else {
                unfiltered_text.clone()
            };
            drop(filter);
            let outcome = backpressured_send(*client_id, client, Message::Text(text.into()), limit);
            if outcome.delivered {
                delivered += 1;
            } else {
                dropped += 1;
            }
            if outcome.must_close {
                slow_to_close.push(*client_id);
            }
        }

        trace!(
            network,
            stream,
            delivered,
            total_clients = clients.len(),
            "broadcast block"
        );
        drop(clients);

        metrics::counter!(
            "substreams_websocket_broadcast_blocks_total",
            "network" => network.to_owned(),
            "table" => stream.to_owned()
        )
        .increment(1);
        metrics::counter!(
            "substreams_websocket_broadcast_events_total",
            "network" => network.to_owned(),
            "table" => stream.to_owned()
        )
        .increment(event_count);
        metrics::counter!(
            "substreams_websocket_broadcast_delivered_total",
            "network" => network.to_owned(),
            "table" => stream.to_owned()
        )
        .increment(delivered as u64);
        if dropped > 0 {
            metrics::counter!(
                "substreams_websocket_broadcast_dropped_total",
                "network" => network.to_owned(),
                "table" => stream.to_owned()
            )
            .increment(dropped);
        }
        if filter_skipped > 0 {
            metrics::counter!(
                "substreams_websocket_broadcast_filtered_skipped_total",
                "network" => network.to_owned(),
                "table" => stream.to_owned()
            )
            .increment(filter_skipped);
        }

        self.close_slow(&slow_to_close).await;
        delivered
    }

    fn slow_client_drop_limit(&self) -> u64 {
        self.slow_drop_limit.load(Ordering::Relaxed)
    }

    /// Send a synthetic `Close(1013 "slow client backpressure")` to every
    /// listed client and unregister them. Called once per broadcast pass
    /// after the read lock is released. 1013 = "Try Again Later" — closest
    /// standard code for "backend dropped you for being slow".
    async fn close_slow(&self, ids: &[ClientId]) {
        if ids.is_empty() {
            return;
        }
        use axum::extract::ws::CloseFrame;
        let close = Message::Close(Some(CloseFrame {
            code: 1013,
            reason: "slow client backpressure".to_owned().into(),
        }));
        let mut clients = self.clients.write().await;
        for id in ids {
            if let Some(client) = clients.remove(id) {
                let total = client.total_drops.load(Ordering::Relaxed);
                warn!(
                    client_id = id,
                    total_drops = total,
                    "force-closing slow WebSocket client (backpressure limit reached)"
                );
                let _ = client.tx.try_send(close.clone());
                metrics::counter!("substreams_websocket_force_closed_total").increment(1);
            }
        }
    }
}

/// Outcome of a single backpressured send: did the frame land on the
/// per-client outbound channel, and did this drop push the client past the
/// force-close threshold?
struct SendOutcome {
    delivered: bool,
    must_close: bool,
}

fn backpressured_send(
    client_id: ClientId,
    client: &ClientHandle,
    msg: Message,
    drop_limit: u64,
) -> SendOutcome {
    match client.tx.try_send(msg) {
        Ok(()) => {
            client.consecutive_drops.store(0, Ordering::Relaxed);
            SendOutcome {
                delivered: true,
                must_close: false,
            }
        }
        Err(_) => {
            let consecutive = client.consecutive_drops.fetch_add(1, Ordering::Relaxed) + 1;
            client.total_drops.fetch_add(1, Ordering::Relaxed);
            // Log throttle — first drop, every 100th after that — so a
            // saturated client doesn't flood the log between threshold
            // crossings.
            if consecutive == 1 || consecutive.is_multiple_of(100) {
                warn!(
                    client_id,
                    consecutive, "dropping broadcast message for slow WebSocket client"
                );
            }
            SendOutcome {
                delivered: false,
                must_close: drop_limit > 0 && consecutive >= drop_limit,
            }
        }
    }
}

#[derive(Clone)]
struct ClientHandle {
    tx: mpsc::Sender<Message>,
    filter: Arc<RwLock<StreamFilter>>,
    /// Consecutive failed `try_send` calls. Reset to 0 on every success.
    /// When this crosses `slow_client_drop_limit`, the broadcast site flags
    /// the client for forced close to relieve backpressure on the bus.
    consecutive_drops: Arc<AtomicU64>,
    /// Total dropped frames over the lifetime of the connection. Surfaced
    /// in the disconnect log so operators can see who was slow.
    total_drops: Arc<AtomicU64>,
}

struct RegisteredClient {
    name: ClientId,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    total_drops: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Copy)]
enum DisconnectReason {
    HeartbeatTimeout,
    ConnectionTtl,
    OutboundClosed,
}

impl std::fmt::Display for DisconnectReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeartbeatTimeout => formatter.write_str("heartbeat timeout"),
            Self::ConnectionTtl => formatter.write_str("connection ttl reached"),
            Self::OutboundClosed => formatter.write_str("outbound channel closed"),
        }
    }
}

/// Handle a Binance-style `{method, params, id}` text message on a connected
/// socket. Returns the JSON reply the client should receive.
///
/// Supported methods:
/// - `SUBSCRIBE`           — params: `["network@stream", ...]`
/// - `UNSUBSCRIBE`         — params: `["network@stream", ...]`
/// - `LIST_SUBSCRIPTIONS`  — no params, returns the current subscriptions
/// - `SET_FILTER`          — params: `["network@stream", { "field": "value" | [...] }]`
/// - `CLEAR_FILTER`        — params: `["network@stream"]`
/// - `LIST_FILTERS`        — no params, returns the current filter map
async fn handle_subscription_command(
    client_id: ClientId,
    filter: &Arc<RwLock<StreamFilter>>,
    max_filter_fields: usize,
    max_filter_values: usize,
    text: &str,
) -> String {
    #[derive(serde::Deserialize)]
    struct Command {
        method: String,
        #[serde(default)]
        params: Vec<serde_json::Value>,
        id: Option<serde_json::Value>,
    }

    let cmd = match serde_json::from_str::<Command>(text) {
        Ok(c) => c,
        Err(error) => {
            warn!(client_id, %error, "invalid WebSocket command JSON");
            return serde_json::json!({
                "error": format!("invalid command: {error}"),
                "id": serde_json::Value::Null,
            })
            .to_string();
        }
    };

    let id = cmd.id.unwrap_or(serde_json::Value::Null);

    metrics::counter!(
        "substreams_websocket_commands_total",
        "method" => cmd.method.clone()
    )
    .increment(1);

    // Extract string params for selector-only methods. Returns error reply
    // when any param is not a string.
    fn string_params(params: &[serde_json::Value]) -> Result<Vec<String>, String> {
        let mut out = Vec::with_capacity(params.len());
        for p in params {
            let Some(s) = p.as_str() else {
                return Err("params must be strings".to_owned());
            };
            out.push(s.to_owned());
        }
        Ok(out)
    }

    match cmd.method.as_str() {
        "SUBSCRIBE" => {
            let raw = match string_params(&cmd.params) {
                Ok(v) => v,
                Err(error) => {
                    return serde_json::json!({ "error": error, "id": id }).to_string();
                }
            };
            let mut parsed = Vec::with_capacity(raw.len());
            for p in &raw {
                match StreamId::parse(p) {
                    Ok(id) => parsed.push(id),
                    Err(error) => {
                        warn!(
                            client_id,
                            params = ?raw,
                            %error,
                            "SUBSCRIBE rejected: invalid selector"
                        );
                        return serde_json::json!({ "error": error, "id": id }).to_string();
                    }
                }
            }
            let (added, total) = {
                let mut guard = filter.write().await;
                let before = guard.list().len();
                for id in parsed {
                    guard.add(id);
                }
                let total = guard.list().len();
                (total.saturating_sub(before), total)
            };
            info!(
                client_id,
                params = ?raw,
                added,
                total,
                "SUBSCRIBE"
            );
            serde_json::json!({ "result": serde_json::Value::Null, "id": id }).to_string()
        }
        "UNSUBSCRIBE" => {
            let raw = match string_params(&cmd.params) {
                Ok(v) => v,
                Err(error) => {
                    return serde_json::json!({ "error": error, "id": id }).to_string();
                }
            };
            let mut parsed = Vec::with_capacity(raw.len());
            for p in &raw {
                match StreamId::parse(p) {
                    Ok(id) => parsed.push(id),
                    Err(error) => {
                        warn!(
                            client_id,
                            params = ?raw,
                            %error,
                            "UNSUBSCRIBE rejected: invalid selector"
                        );
                        return serde_json::json!({ "error": error, "id": id }).to_string();
                    }
                }
            }
            let (removed, total) = {
                let mut guard = filter.write().await;
                let before = guard.list().len();
                for id in &parsed {
                    guard.remove(id);
                }
                let total = guard.list().len();
                (before.saturating_sub(total), total)
            };
            info!(
                client_id,
                params = ?raw,
                removed,
                total,
                "UNSUBSCRIBE"
            );
            serde_json::json!({ "result": serde_json::Value::Null, "id": id }).to_string()
        }
        "LIST_SUBSCRIPTIONS" => {
            let list = filter.read().await.list();
            info!(client_id, count = list.len(), "LIST_SUBSCRIPTIONS");
            serde_json::json!({ "result": list, "id": id }).to_string()
        }
        "SET_FILTER" => {
            if cmd.params.len() != 2 {
                return serde_json::json!({
                    "error": "SET_FILTER expects [selector, filter-object]",
                    "id": id,
                })
                .to_string();
            }
            let Some(selector) = cmd.params[0].as_str() else {
                return serde_json::json!({
                    "error": "SET_FILTER param[0] must be a `network@table` selector",
                    "id": id,
                })
                .to_string();
            };
            // Wildcards on either side are allowed: `*@*`, `<network>@*`,
            // `*@<table>`. Filter is stored under the wildcard literal; at
            // broadcast time, every stored filter whose selector matches the
            // outgoing (network, table) contributes — all must pass for the
            // event to be delivered.
            if StreamId::parse(selector).is_err() {
                return serde_json::json!({
                    "error": format!("invalid selector {selector:?}"),
                    "id": id,
                })
                .to_string();
            }
            let parsed_filter = match EventFilter::from_json(
                &cmd.params[1],
                max_filter_fields,
                max_filter_values,
            ) {
                Ok(f) => f,
                Err(error) => {
                    warn!(client_id, selector, %error, "SET_FILTER rejected");
                    return serde_json::json!({
                        "error": error.to_string(),
                        "id": id,
                    })
                    .to_string();
                }
            };
            {
                let mut guard = filter.write().await;
                guard.event_filters.set(selector.to_owned(), parsed_filter);
            }
            info!(client_id, selector, "SET_FILTER");
            serde_json::json!({ "result": serde_json::Value::Null, "id": id }).to_string()
        }
        "CLEAR_FILTER" => {
            let raw = match string_params(&cmd.params) {
                Ok(v) => v,
                Err(error) => {
                    return serde_json::json!({ "error": error, "id": id }).to_string();
                }
            };
            if raw.is_empty() {
                return serde_json::json!({
                    "error": "CLEAR_FILTER expects at least one selector",
                    "id": id,
                })
                .to_string();
            }
            {
                let mut guard = filter.write().await;
                for selector in &raw {
                    guard.event_filters.remove(selector);
                }
            }
            info!(client_id, params = ?raw, "CLEAR_FILTER");
            serde_json::json!({ "result": serde_json::Value::Null, "id": id }).to_string()
        }
        "LIST_FILTERS" => {
            let list = filter.read().await.event_filters.list();
            info!(client_id, count = list.len(), "LIST_FILTERS");
            serde_json::json!({ "result": list, "id": id }).to_string()
        }
        other => {
            warn!(client_id, method = %other, "unknown WebSocket method");
            serde_json::json!({
                "error": format!("unknown method {other:?}; expected SUBSCRIBE, UNSUBSCRIBE, LIST_SUBSCRIPTIONS, SET_FILTER, CLEAR_FILTER, or LIST_FILTERS"),
                "id": id,
            })
            .to_string()
        }
    }
}

async fn run_heartbeat(
    client_id: ClientId,
    outbound: mpsc::Sender<Message>,
    last_pong_at: Arc<RwLock<Instant>>,
    interval: Duration,
    timeout: Duration,
    connection_ttl: Option<Duration>,
    connected_at: Instant,
    disconnect: oneshot::Sender<DisconnectReason>,
) {
    let reason = loop {
        tokio::time::sleep(interval).await;

        let now = Instant::now();
        if connection_ttl.is_some_and(|ttl| now.duration_since(connected_at) >= ttl) {
            break DisconnectReason::ConnectionTtl;
        }

        let last_pong_at = *last_pong_at.read().await;
        if now.duration_since(last_pong_at) >= timeout {
            break DisconnectReason::HeartbeatTimeout;
        }

        let payload = client_id.to_string();
        if outbound.send(Message::Ping(payload.into())).await.is_err() {
            break DisconnectReason::OutboundClosed;
        }

        debug!(client_id, "sent WebSocket heartbeat ping");
    };

    let _ = outbound.send(Message::Close(None)).await;
    let _ = disconnect.send(reason);
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to listen for SIGINT");
        }
    };

    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                warn!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT; starting graceful shutdown"),
        _ = sigterm => info!("received SIGTERM; starting graceful shutdown"),
    }
}

#[cfg(test)]
mod filter_tests {
    use super::{StreamFilter, StreamId, parse_stream_list};
    use crate::EventFilterSet;

    fn id(network: Option<&str>, stream: Option<&str>) -> StreamId {
        StreamId {
            network: network.map(str::to_owned),
            stream: stream.map(str::to_owned),
        }
    }

    #[test]
    fn empty_filter_matches_nothing() {
        let f = StreamFilter::default();
        assert!(!f.matches("solana-mainnet", "swaps"));
    }

    #[test]
    fn wildcard_star_star_matches_everything() {
        let id = StreamId::parse("*@*").unwrap();
        let f = StreamFilter {
            entries: vec![id],
            wrap_envelope: false,
            event_filters: EventFilterSet::default(),
        };
        assert!(f.matches("solana-mainnet", "swaps"));
        assert!(f.matches("anything", "anywhere"));
    }

    #[test]
    fn exact_pair_matches_only_that_pair() {
        let f = StreamFilter {
            entries: vec![id(Some("solana-mainnet"), Some("swaps"))],
            wrap_envelope: false,
            event_filters: EventFilterSet::default(),
        };
        assert!(f.matches("solana-mainnet", "swaps"));
        assert!(!f.matches("solana-mainnet", "transfers"));
        assert!(!f.matches("ethereum-mainnet", "swaps"));
    }

    #[test]
    fn wildcard_network_matches_stream_across_chains() {
        let f = StreamFilter {
            entries: vec![id(None, Some("swaps"))],
            wrap_envelope: false,
            event_filters: EventFilterSet::default(),
        };
        assert!(f.matches("solana-mainnet", "swaps"));
        assert!(f.matches("ethereum-mainnet", "swaps"));
        assert!(!f.matches("solana-mainnet", "transfers"));
    }

    #[test]
    fn wildcard_stream_matches_all_streams_on_network() {
        let f = StreamFilter {
            entries: vec![id(Some("solana-mainnet"), None)],
            wrap_envelope: false,
            event_filters: EventFilterSet::default(),
        };
        assert!(f.matches("solana-mainnet", "swaps"));
        assert!(f.matches("solana-mainnet", "transfers"));
        assert!(!f.matches("ethereum-mainnet", "swaps"));
    }

    #[test]
    fn multiple_entries_are_or() {
        let f = StreamFilter {
            entries: vec![
                id(Some("solana-mainnet"), Some("swaps")),
                id(Some("ethereum-mainnet"), Some("transfers")),
            ],
            wrap_envelope: false,
            event_filters: EventFilterSet::default(),
        };
        assert!(f.matches("solana-mainnet", "swaps"));
        assert!(f.matches("ethereum-mainnet", "transfers"));
        assert!(!f.matches("solana-mainnet", "transfers"));
        assert!(!f.matches("ethereum-mainnet", "swaps"));
    }

    #[test]
    fn parses_single_path_stream() {
        let list = parse_stream_list("solana-mainnet@swaps").unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].matches("solana-mainnet", "swaps"));
    }

    #[test]
    fn parses_multi_path_streams() {
        let list = parse_stream_list("solana-mainnet@swaps/ethereum-mainnet@transfers").unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].to_wire(), "solana-mainnet@swaps");
        assert_eq!(list[1].to_wire(), "ethereum-mainnet@transfers");
    }

    #[test]
    fn parses_wildcards_via_at_separator() {
        let list = parse_stream_list("*@swaps/solana-mainnet@*").unwrap();
        assert_eq!(list.len(), 2);
        assert!(list[0].matches("ethereum-mainnet", "swaps"));
        assert!(list[1].matches("solana-mainnet", "anything"));
    }

    #[test]
    fn rejects_malformed_selector() {
        let err = parse_stream_list("notvalid").unwrap_err();
        assert!(err.contains("network@stream"), "got: {err}");
    }

    #[test]
    fn rejects_empty_list() {
        let err = parse_stream_list("").unwrap_err();
        assert!(err.contains("at least one"), "got: {err}");
    }

    #[test]
    fn stream_id_to_wire_roundtrips() {
        let s = StreamId::parse("solana-mainnet@swaps").unwrap();
        assert_eq!(s.to_wire(), "solana-mainnet@swaps");
        let w = StreamId::parse("*@swaps").unwrap();
        assert_eq!(w.to_wire(), "*@swaps");
        let nw = StreamId::parse("solana-mainnet@*").unwrap();
        assert_eq!(nw.to_wire(), "solana-mainnet@*");
    }

    #[test]
    fn filter_add_and_remove() {
        let mut f = StreamFilter::default();
        f.add(StreamId::parse("solana-mainnet@swaps").unwrap());
        f.add(StreamId::parse("ethereum-mainnet@swaps").unwrap());
        assert_eq!(f.list().len(), 2);
        // Adding the same entry is a no-op.
        f.add(StreamId::parse("solana-mainnet@swaps").unwrap());
        assert_eq!(f.list().len(), 2);
        // Removing a known entry returns true.
        assert!(f.remove(&StreamId::parse("solana-mainnet@swaps").unwrap()));
        assert_eq!(f.list(), vec!["ethereum-mainnet@swaps".to_owned()]);
        // Removing an unknown entry returns false.
        assert!(!f.remove(&StreamId::parse("solana-mainnet@swaps").unwrap()));
    }

    #[tokio::test]
    async fn subscribe_command_adds_entries() {
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(StreamFilter::default()));
        let reply = super::handle_subscription_command(
            0,
            &filter,
            16,
            64,
            r#"{"method":"SUBSCRIBE","params":["solana-mainnet@swaps","ethereum-mainnet@transfers"],"id":7}"#,
        )
        .await;
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["id"], 7);
        assert!(value["result"].is_null());
        let entries = filter.read().await.list();
        assert_eq!(
            entries,
            vec![
                "solana-mainnet@swaps".to_owned(),
                "ethereum-mainnet@transfers".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn unsubscribe_command_removes_entries() {
        let mut start = StreamFilter::default();
        start.add(StreamId::parse("solana-mainnet@swaps").unwrap());
        start.add(StreamId::parse("ethereum-mainnet@transfers").unwrap());
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(start));
        let reply = super::handle_subscription_command(
            0,
            &filter,
            16,
            64,
            r#"{"method":"UNSUBSCRIBE","params":["solana-mainnet@swaps"],"id":12}"#,
        )
        .await;
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["id"], 12);
        assert_eq!(
            filter.read().await.list(),
            vec!["ethereum-mainnet@transfers".to_owned()]
        );
    }

    #[tokio::test]
    async fn list_subscriptions_returns_current_set() {
        let mut start = StreamFilter::default();
        start.add(StreamId::parse("solana-mainnet@swaps").unwrap());
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(start));
        let reply = super::handle_subscription_command(
            0,
            &filter,
            16,
            64,
            r#"{"method":"LIST_SUBSCRIPTIONS","id":3}"#,
        )
        .await;
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["id"], 3);
        assert_eq!(value["result"], serde_json::json!(["solana-mainnet@swaps"]));
    }

    #[tokio::test]
    async fn unknown_method_returns_error() {
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(StreamFilter::default()));
        let reply = super::handle_subscription_command(
            0,
            &filter,
            16,
            64,
            r#"{"method":"DESTROY","id":99}"#,
        )
        .await;
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["id"], 99);
        assert!(value["error"].as_str().unwrap().contains("unknown method"));
    }

    #[tokio::test]
    async fn invalid_json_returns_error() {
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(StreamFilter::default()));
        let reply = super::handle_subscription_command(0, &filter, 16, 64, "not json").await;
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert!(value["error"].as_str().unwrap().contains("invalid command"));
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use futures_util::StreamExt;
    use prost::Message as ProstMessage;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;
    use tokio_tungstenite::{connect_async, tungstenite::Message as TungsteniteMessage};

    use super::*;
    use crate::config::ReplayConfig;
    use crate::decoder::pb::sf::substreams::sink::database::v1::{
        DatabaseChanges, Field, TableChange,
    };
    use crate::{StreamEvent, SubstreamsConfig, WebSocketConfig};

    #[tokio::test]
    async fn health_route_returns_ok() {
        let server = TestServer::start(config()).await;

        let mut stream = tokio::net::TcpStream::connect(server.addr)
            .await
            .expect("health tcp connection succeeds");

        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("health request writes");

        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("health response reads");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("ok"));
    }

    #[tokio::test]
    async fn metrics_route_renders_prometheus_payload() {
        let server = TestServer::start(config()).await;

        let mut stream = tokio::net::TcpStream::connect(server.addr)
            .await
            .expect("metrics tcp connection succeeds");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("metrics request writes");

        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("metrics response reads");

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "expected 200, got: {response}"
        );
        assert!(
            response.contains("text/plain"),
            "missing prometheus content-type"
        );
        // metrics-exporter-prometheus only emits a counter / gauge that has
        // actually been touched. Drive one and re-scrape to confirm it
        // renders. (`describe!` alone seeds help text, not the metric line.)
        metrics::counter!("substreams_websocket_test_canary_total").increment(1);
        let mut stream2 = tokio::net::TcpStream::connect(server.addr)
            .await
            .expect("second metrics tcp connection");
        stream2
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("metrics request writes");
        let mut second = String::new();
        stream2
            .read_to_string(&mut second)
            .await
            .expect("metrics body reads");
        assert!(
            second.contains("substreams_websocket_test_canary_total"),
            "scrape after counter touch must include the metric: {second}"
        );
    }

    #[tokio::test]
    async fn websocket_connects_and_receives_welcome_message() {
        let server = TestServer::start(config()).await;

        let (mut socket, _) = connect_async(format!("ws://{}/stream?streams=*@*", server.addr))
            .await
            .expect("websocket connects");

        let message = socket
            .next()
            .await
            .expect("welcome message")
            .expect("valid websocket message");

        let TungsteniteMessage::Text(text) = message else {
            panic!("expected text welcome message");
        };

        let body: serde_json::Value = serde_json::from_str(&text).expect("welcome json");
        assert_eq!(body["type"], "session");
        assert_eq!(body["status"], "connected");
        assert!(
            body["streams"][0].get("stream").is_none(),
            "welcome streams entries no longer carry a `stream` name"
        );
        assert_eq!(body["streams"][0]["module"], "db_out");
        assert_eq!(
            body["streams"][0]["tables"],
            serde_json::json!(["swaps", "transfers"])
        );
        assert_eq!(body["streams"][0]["network"], "solana-mainnet");
        assert_eq!(body["streams"][0]["manifest"], "./demo.spkg");
        assert!(body["client_id"].as_u64().is_some());
        assert_eq!(body["subscriptions"], serde_json::json!(["*@*"]));
        assert_eq!(body["wrap_envelope"], true);

        socket
            .close(None)
            .await
            .expect("client closes websocket cleanly");
    }

    #[tokio::test]
    async fn websocket_rejects_connections_over_limit() {
        let mut cfg = config();
        cfg.websocket.max_clients = 1;
        let server = TestServer::start(cfg).await;

        let (_socket, _) = connect_async(format!("ws://{}/stream?streams=*@*", server.addr))
            .await
            .expect("first websocket connects");

        let second = connect_async(format!("ws://{}/stream?streams=*@*", server.addr)).await;
        assert!(second.is_err(), "second websocket should be rejected");
    }

    #[tokio::test]
    async fn websocket_receives_heartbeat_ping() {
        let mut cfg = config();
        cfg.websocket.heartbeat_interval = Duration::from_millis(25);
        cfg.websocket.heartbeat_timeout = Duration::from_secs(1);
        let server = TestServer::start(cfg).await;

        let (mut socket, _) = connect_async(format!("ws://{}/stream?streams=*@*", server.addr))
            .await
            .expect("websocket connects");

        let _welcome = socket.next().await.expect("welcome").expect("welcome ok");

        let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("heartbeat arrives before timeout")
            .expect("heartbeat message")
            .expect("heartbeat message ok");

        assert!(
            matches!(message, TungsteniteMessage::Ping(_)),
            "expected heartbeat ping, got {message:?}"
        );
    }

    #[tokio::test]
    async fn websocket_disconnects_stale_client() {
        let mut cfg = config();
        cfg.websocket.max_clients = 1;
        cfg.websocket.heartbeat_interval = Duration::from_millis(20);
        cfg.websocket.heartbeat_timeout = Duration::from_millis(80);
        let server = TestServer::start(cfg).await;

        let (mut stale_socket, _) =
            connect_async(format!("ws://{}/stream?streams=*@*", server.addr))
                .await
                .expect("first websocket connects");
        let _welcome = stale_socket
            .next()
            .await
            .expect("welcome")
            .expect("welcome ok");

        let mut connected_after_eviction = false;
        for _ in 0..40 {
            if connect_async(format!("ws://{}/stream?streams=*@*", server.addr))
                .await
                .is_ok()
            {
                connected_after_eviction = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert!(
            connected_after_eviction,
            "a new client should connect after stale client eviction"
        );
    }

    #[tokio::test]
    async fn websocket_disconnects_after_connection_ttl() {
        let mut cfg = config();
        cfg.websocket.max_clients = 1;
        cfg.websocket.heartbeat_interval = Duration::from_millis(20);
        cfg.websocket.heartbeat_timeout = Duration::from_secs(1);
        cfg.websocket.connection_ttl = Some(Duration::from_millis(80));
        let server = TestServer::start(cfg).await;

        let (mut socket, _) = connect_async(format!("ws://{}/stream?streams=*@*", server.addr))
            .await
            .expect("websocket connects");
        let _welcome = socket.next().await.expect("welcome").expect("welcome ok");

        let mut disconnected = false;
        for _ in 0..40 {
            match tokio::time::timeout(Duration::from_millis(25), socket.next()).await {
                Ok(None) => {
                    disconnected = true;
                    break;
                }
                Ok(Some(Ok(TungsteniteMessage::Close(_)))) => {
                    disconnected = true;
                    break;
                }
                Ok(Some(_)) | Err(_) => {}
            }
        }

        assert!(
            disconnected,
            "client should disconnect after connection ttl"
        );
    }

    #[tokio::test]
    async fn websocket_client_receives_decoded_database_changes_block() {
        let server = TestServer::start(config()).await;

        let (mut socket, _) = connect_async(format!("ws://{}/stream?streams=*@*", server.addr))
            .await
            .expect("websocket connects");

        let _welcome = socket.next().await.expect("welcome").expect("welcome ok");

        // Wait until the WS client is registered with the server before injecting.
        for _ in 0..40 {
            if server.clients.active_count().await > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![
                    Field {
                        name: "input_amount".to_owned(),
                        value: "1287000000".to_owned(),
                        update_op: 1,
                    },
                    Field {
                        name: "block_num".to_owned(),
                        value: "ignored".to_owned(),
                        update_op: 1,
                    },
                ],
                primary_key: None,
            }],
        };
        let mut payload = Vec::new();
        changes.encode(&mut payload).expect("events encode");

        let cursors_dir = tempfile::tempdir().expect("cursor tempdir");
        let cursors = CursorStore::new(cursors_dir.path());
        let replay = ReplayLog::disabled();
        handle_substream_event(
            &test_identity(),
            &server.clients,
            &cursors,
            &replay,
            StreamEvent::Block {
                number: 999,
                id: "block-999".to_owned(),
                timestamp: "2026-05-13 17:30:00".to_owned(),
                timestamp_seconds: 1_778_772_600,
                output_type_url: String::new(),
                payload,
                cursor: "abc123".to_owned(),
            },
        )
        .await;

        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("decoded block arrives")
            .expect("decoded block message")
            .expect("decoded block ok");

        let TungsteniteMessage::Text(text) = message else {
            panic!("expected text decoded block, got {message:?}");
        };

        let envelope: serde_json::Value = serde_json::from_str(&text).expect("decoded json");
        // `/stream?streams=*@*` wraps in {"stream":"<network>@<table>","data":...}
        assert_eq!(envelope["stream"], "solana-mainnet@swaps");
        let body = &envelope["data"];
        assert_eq!(body["table"], "swaps");
        assert_eq!(body["network"], "solana-mainnet");
        assert_eq!(body["block_num"], 999);
        assert_eq!(body["block_hash"], "block-999");
        assert_eq!(body["timestamp"], "2026-05-13 17:30:00");
        assert!(
            body.get("cursor").is_none(),
            "cursor must not be surfaced on broadcast payloads"
        );
        assert!(body.get("block").is_none(), "no nested 'block' object");
        assert!(body.get("type").is_none());
        assert!(body.get("changes").is_none());
        assert!(
            body["events"][0].get("@table").is_none(),
            "@table is stripped now that the parent payload carries `table`"
        );
        assert_eq!(body["events"][0]["input_amount"], "1287000000");
        // block_num stripped because it duplicates top-level block_num
        assert!(body["events"][0].get("block_num").is_none());
        assert!(
            body["events"][0].get("table").is_none(),
            "bare 'table' must not appear"
        );

        socket.close(None).await.expect("client closes cleanly");
    }

    #[tokio::test]
    async fn websocket_replays_blocks_above_from_timestamp() {
        let replay_dir = tempfile::tempdir().expect("replay tempdir");
        let mut cfg = config();
        cfg.replay = ReplayConfig {
            max_seconds: 3600,
            dir: replay_dir.path().to_path_buf(),
        };
        let server = TestServer::start(cfg).await;

        for n in 0..5i64 {
            let ts = 1_000_000 + n;
            let payload = serde_json::json!({
                "network": "solana-mainnet",
                "block_num": 100 + n as u64,
                "timestamp_seconds": ts,
                "events": [{ "@table": "swaps", "user": format!("u{ts}") }],
            })
            .to_string();
            server
                .replay
                .append(
                    "solana-mainnet",
                    "svm_swaps",
                    "v0.1.0",
                    "deadbeef",
                    ts,
                    &payload,
                )
                .await
                .expect("seed replay");
        }

        let (mut socket, _) = connect_async(format!(
            "ws://{}/ws/solana-mainnet@swaps?from_timestamp=1000001",
            server.addr
        ))
        .await
        .expect("websocket connects");

        let welcome = socket.next().await.expect("welcome").expect("welcome ok");
        let welcome_text = match welcome {
            TungsteniteMessage::Text(t) => t.to_string(),
            other => panic!("expected text welcome, got {other:?}"),
        };
        assert!(welcome_text.contains("\"type\":\"session\""));

        let mut got = Vec::new();
        for _ in 0..3 {
            let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("replay block arrives")
                .expect("websocket open")
                .expect("frame");
            let text = match msg {
                TungsteniteMessage::Text(t) => t.to_string(),
                other => panic!("expected text, got {other:?}"),
            };
            let value: serde_json::Value = serde_json::from_str(&text).expect("json");
            got.push(
                value["timestamp_seconds"]
                    .as_i64()
                    .expect("timestamp_seconds"),
            );
        }
        assert_eq!(got, vec![1_000_002, 1_000_003, 1_000_004]);

        socket.close(None).await.expect("clean close");
    }

    #[tokio::test]
    async fn websocket_emits_gap_when_from_timestamp_below_window() {
        let replay_dir = tempfile::tempdir().expect("replay tempdir");
        let mut cfg = config();
        cfg.replay = ReplayConfig {
            max_seconds: 3600,
            dir: replay_dir.path().to_path_buf(),
        };
        let server = TestServer::start(cfg).await;

        for n in 0..5i64 {
            let ts = 5_000_000 + n;
            let payload = serde_json::json!({
                "network": "solana-mainnet",
                "block_num": 500 + n as u64,
                "timestamp_seconds": ts,
                "events": [{ "@table": "swaps", "user": format!("u{ts}") }],
            })
            .to_string();
            server
                .replay
                .append(
                    "solana-mainnet",
                    "svm_swaps",
                    "v0.1.0",
                    "deadbeef",
                    ts,
                    &payload,
                )
                .await
                .expect("seed replay");
        }

        let (mut socket, _) = connect_async(format!(
            "ws://{}/ws/solana-mainnet@swaps?from_timestamp=10",
            server.addr
        ))
        .await
        .expect("websocket connects");

        let _welcome = socket.next().await.expect("welcome").expect("welcome ok");

        let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("gap arrives")
            .expect("open")
            .expect("frame");
        let text = match msg {
            TungsteniteMessage::Text(t) => t.to_string(),
            other => panic!("expected text, got {other:?}"),
        };
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(value["type"], "stream");
        assert_eq!(value["status"], "gap");
        assert_eq!(value["requested_timestamp"], 10);
        assert_eq!(value["oldest_buffered_timestamp"], 5_000_000);

        socket.close(None).await.expect("clean close");
    }

    #[tokio::test]
    async fn filter_query_drops_non_matching_events_on_broadcast() {
        let server = TestServer::start(config()).await;

        // Build a block with three events, two protocols.
        let payload = build_swap_payload(&[
            ("raydium_cpmm", "user_a"),
            ("pump_fun", "user_b"),
            ("raydium_cpmm", "user_c"),
        ]);

        // Connect with ?filter=protocol=raydium_cpmm
        let filter_param = urlencoding::encode(r#"{"protocol":"raydium_cpmm"}"#).to_string();
        let url = format!(
            "ws://{}/ws/solana-mainnet@swaps?filter={}",
            server.addr, filter_param
        );
        let (mut socket, _) = connect_async(&url).await.expect("websocket connects");
        let _welcome = socket.next().await.expect("welcome").expect("ok");

        // Push the synthesized block through the broadcast pipeline.
        deliver_block(&server, payload).await;

        let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("block arrives")
            .expect("open")
            .expect("frame");
        let text = match msg {
            TungsteniteMessage::Text(t) => t.to_string(),
            other => panic!("expected text, got {other:?}"),
        };
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        let events = value["events"].as_array().expect("events array");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["user"], "user_a");
        assert_eq!(events[1]["user"], "user_c");

        socket.close(None).await.expect("clean close");
    }

    #[tokio::test]
    async fn filter_query_skips_block_when_no_events_match() {
        let server = TestServer::start(config()).await;
        let payload = build_swap_payload(&[("pump_fun", "user_a"), ("meteora", "user_b")]);

        let filter_param = urlencoding::encode(r#"{"protocol":"raydium_cpmm"}"#).to_string();
        let url = format!(
            "ws://{}/ws/solana-mainnet@swaps?filter={}",
            server.addr, filter_param
        );
        let (mut socket, _) = connect_async(&url).await.expect("websocket connects");
        let _welcome = socket.next().await.expect("welcome").expect("ok");

        deliver_block(&server, payload).await;

        // Block should be skipped entirely. Verify by sending a heartbeat or
        // by relying on timeout: no frame arrives within a short window.
        let result = tokio::time::timeout(Duration::from_millis(300), socket.next()).await;
        assert!(
            result.is_err(),
            "block with no matching events must not be delivered"
        );

        socket.close(None).await.expect("clean close");
    }

    #[tokio::test]
    async fn set_filter_command_takes_effect_on_next_block() {
        let server = TestServer::start(config()).await;

        let (mut socket, _) =
            connect_async(format!("ws://{}/ws/solana-mainnet@swaps", server.addr))
                .await
                .expect("websocket connects");
        let _welcome = socket.next().await.expect("welcome").expect("ok");

        // Apply SET_FILTER live.
        socket
            .send(TungsteniteMessage::Text(
                r#"{"method":"SET_FILTER","params":["solana-mainnet@swaps",{"user":"user_b"}],"id":1}"#
                    .into(),
            ))
            .await
            .expect("send SET_FILTER");
        let reply = socket.next().await.expect("reply").expect("ok");
        let reply_text = match reply {
            TungsteniteMessage::Text(t) => t.to_string(),
            other => panic!("expected text, got {other:?}"),
        };
        let reply_value: serde_json::Value = serde_json::from_str(&reply_text).expect("reply json");
        assert_eq!(reply_value["id"], 1);
        assert!(reply_value["result"].is_null());

        let payload = build_swap_payload(&[
            ("raydium_cpmm", "user_a"),
            ("raydium_cpmm", "user_b"),
            ("raydium_cpmm", "user_c"),
        ]);
        deliver_block(&server, payload).await;

        let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("block arrives")
            .expect("open")
            .expect("frame");
        let text = match msg {
            TungsteniteMessage::Text(t) => t.to_string(),
            other => panic!("expected text, got {other:?}"),
        };
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        let events = value["events"].as_array().expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["user"], "user_b");

        socket.close(None).await.expect("clean close");
    }

    /// Build a synthesized DatabaseChanges payload with the given
    /// `(protocol, user)` pairs as `swaps` table rows.
    fn build_swap_payload(rows: &[(&str, &str)]) -> Vec<u8> {
        let changes = DatabaseChanges {
            table_changes: rows
                .iter()
                .map(|(protocol, user)| TableChange {
                    table: "swaps".to_owned(),
                    ordinal: 0,
                    operation: 1,
                    fields: vec![
                        Field {
                            name: "protocol".to_owned(),
                            value: (*protocol).to_owned(),
                            update_op: 1,
                        },
                        Field {
                            name: "user".to_owned(),
                            value: (*user).to_owned(),
                            update_op: 1,
                        },
                    ],
                    primary_key: None,
                })
                .collect(),
        };
        let mut payload = Vec::new();
        changes.encode(&mut payload).expect("encode");
        payload
    }

    async fn deliver_block(server: &TestServer, payload: Vec<u8>) {
        let _ = server;
        let cursors_dir = tempfile::tempdir().expect("cursor tempdir");
        let cursors = CursorStore::new(cursors_dir.path());
        let replay = ReplayLog::disabled();
        super::handle_substream_event(
            &test_identity(),
            &server.clients,
            &cursors,
            &replay,
            StreamEvent::Block {
                number: 999,
                id: "block-999".to_owned(),
                timestamp: "2026-05-13 17:30:00".to_owned(),
                timestamp_seconds: 1_778_772_600,
                output_type_url: String::new(),
                payload,
                cursor: "abc123".to_owned(),
            },
        )
        .await;
    }

    fn test_identity() -> super::StreamIdentity {
        super::StreamIdentity {
            network: "solana-mainnet".to_owned(),
            package_name: "svm_swaps".to_owned(),
            package_version: "v0.1.0".to_owned(),
            module_hash: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_owned(),
            manifest: "./svm-swaps.spkg".to_owned(),
            endpoint: "https://test.endpoint:443".to_owned(),
            tables: vec!["swaps".to_owned()],
        }
    }

    #[tokio::test]
    async fn health_returns_503_during_drain() {
        let mut cfg = config();
        cfg.websocket.shutdown_drain_timeout = Duration::from_secs(5);
        let mut server = TestServer::start(cfg).await;

        // Open a WS client and never reply to Close so drain stays in flight
        // long enough for us to observe /healthz returning 503.
        let (mut socket, _) = connect_async(format!("ws://{}/ws/*@*", server.addr))
            .await
            .expect("websocket connects");
        let _welcome = socket.next().await.expect("welcome").expect("ok");

        server.fire_shutdown();

        // Give the drain task a chance to flip the flag.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut stream = tokio::net::TcpStream::connect(server.addr)
            .await
            .expect("health tcp connection during drain");
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("health request writes");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("health response reads");
        assert!(
            response.starts_with("HTTP/1.1 503"),
            "expected 503 during drain, got: {response}"
        );
    }

    #[tokio::test]
    async fn shutdown_sends_close_frame_to_connected_clients() {
        let mut cfg = config();
        cfg.websocket.shutdown_drain_timeout = Duration::from_secs(2);
        let mut server = TestServer::start(cfg).await;

        let (mut socket, _) = connect_async(format!("ws://{}/ws/*@*", server.addr))
            .await
            .expect("websocket connects");
        let _welcome = socket.next().await.expect("welcome").expect("welcome ok");

        server.fire_shutdown();

        let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("close frame arrives within drain window")
            .expect("websocket open")
            .expect("frame ok");
        let close = match frame {
            TungsteniteMessage::Close(close) => close,
            other => panic!("expected close frame, got {other:?}"),
        };
        let close = close.expect("close frame carries a payload");
        assert_eq!(
            close.code,
            tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away
        );
        assert_eq!(close.reason, "server shutting down");
    }

    #[tokio::test]
    async fn backpressured_send_flags_close_after_drop_limit() {
        // Drive `backpressured_send` directly with a saturated 1-slot mpsc.
        // First send lands; subsequent sends fail until the consecutive-
        // drop counter crosses the limit, at which point `must_close` is
        // returned.
        let (tx, _rx) = tokio::sync::mpsc::channel::<axum::extract::ws::Message>(1);
        let consecutive_drops = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_drops = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(StreamFilter::default()));
        let client = super::ClientHandle {
            tx,
            filter,
            consecutive_drops: consecutive_drops.clone(),
            total_drops: total_drops.clone(),
        };
        let frame = || axum::extract::ws::Message::Text("payload".into());

        // First send: fits in the 1-slot buffer.
        let out = super::backpressured_send(1, &client, frame(), 3);
        assert!(out.delivered, "first send must land");
        assert!(!out.must_close);

        // Next 2 sends saturate the buffer → drops, but below the limit.
        for _ in 0..2 {
            let out = super::backpressured_send(1, &client, frame(), 3);
            assert!(!out.delivered);
            assert!(!out.must_close);
        }

        // 3rd consecutive drop crosses the limit → must_close.
        let out = super::backpressured_send(1, &client, frame(), 3);
        assert!(!out.delivered);
        assert!(
            out.must_close,
            "must_close should fire on the limit-th drop"
        );
        assert_eq!(
            consecutive_drops.load(std::sync::atomic::Ordering::Relaxed),
            3
        );
        assert_eq!(total_drops.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn backpressured_send_resets_consecutive_drops_on_success() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<axum::extract::ws::Message>(1);
        let consecutive_drops = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_drops = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(StreamFilter::default()));
        let client = super::ClientHandle {
            tx,
            filter,
            consecutive_drops: consecutive_drops.clone(),
            total_drops: total_drops.clone(),
        };
        let frame = || axum::extract::ws::Message::Text("payload".into());

        // Fill, then two drops, then drain so the next send succeeds.
        assert!(super::backpressured_send(1, &client, frame(), 100).delivered);
        for _ in 0..2 {
            super::backpressured_send(1, &client, frame(), 100);
        }
        assert_eq!(
            consecutive_drops.load(std::sync::atomic::Ordering::Relaxed),
            2
        );

        // Drain the buffer; next try_send fits → counter resets to 0.
        let _ = rx.try_recv();
        let out = super::backpressured_send(1, &client, frame(), 100);
        assert!(out.delivered);
        assert_eq!(
            consecutive_drops.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        // Total drops should still reflect the 2 misses earlier.
        assert_eq!(total_drops.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    struct TestServer {
        addr: SocketAddr,
        clients: ClientRegistry,
        config: Arc<Config>,
        replay: ReplayLog,
        shutdown: Option<oneshot::Sender<()>>,
    }

    impl TestServer {
        async fn start(config: Config) -> Self {
            // Tests share the global metrics recorder via OnceLock — first
            // call installs it, subsequent calls are no-ops. The /metrics
            // route is wired through build_app even when this is called.
            crate::metrics::init();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("test listener binds");
            let addr = listener.local_addr().expect("test listener address");
            let ws_path = config.websocket.ws_path.clone();
            let health_path = config.websocket.health_path.clone();
            let streams_meta = Arc::new(
                config
                    .streams
                    .iter()
                    .map(|s| StreamMeta {
                        network: s.substreams.network.clone().unwrap_or_default(),
                        module: s.substreams.module.clone(),
                        manifest: s.substreams.manifest.clone(),
                        module_hash: String::new(),
                        package_name: String::new(),
                        package_version: String::new(),
                        description: String::new(),
                        tables: s.tables.clone(),
                    })
                    .collect::<Vec<_>>(),
            );
            let replay = ReplayLog::new(config.replay.dir.clone(), config.replay.max_seconds);
            let draining = Arc::new(AtomicBool::new(false));
            let state = AppState {
                config: Arc::new(config),
                clients: ClientRegistry::default(),
                streams_meta,
                replay: replay.clone(),
                draining: draining.clone(),
            };
            let clients = state.clients.clone();
            let config = state.config.clone();
            let drain_timeout = config.websocket.shutdown_drain_timeout;
            let drain_clients = clients.clone();
            let app = build_app(state);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            tokio::spawn(async move {
                serve_listener(listener, app, ws_path, health_path, async move {
                    let _ = shutdown_rx.await;
                    draining.store(true, Ordering::SeqCst);
                    drain_clients
                        .drain("server shutting down", drain_timeout)
                        .await;
                })
                .await
                .expect("test server exits cleanly");
            });

            wait_for_server(addr).await;

            Self {
                addr,
                clients,
                config,
                replay,
                shutdown: Some(shutdown_tx),
            }
        }

        fn fire_shutdown(&mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
        }
    }

    async fn wait_for_server(addr: SocketAddr) {
        for _ in 0..40 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        panic!("test server did not start");
    }

    fn config() -> Config {
        Config {
            streams: vec![StreamConfig {
                tables: vec!["swaps".to_owned(), "transfers".to_owned()],
                substreams: SubstreamsConfig {
                    manifest: "./demo.spkg".to_owned(),
                    module: "db_out".to_owned(),
                    endpoint: None,
                    network: Some("solana-mainnet".to_owned()),
                    start_block: None,
                    start_cursor: None,
                    stop_block: "0".to_owned(),
                    params: Vec::new(),
                    plaintext: false,
                    insecure: false,
                    production_mode: false,
                    final_blocks_only: false,
                    token: None,
                    api_key: None,
                    api_key_header: "X-Api-Key".to_owned(),
                    auth_url: None,
                },
            }],
            websocket: WebSocketConfig {
                listen: "127.0.0.1:0".parse().expect("listen address"),
                ws_path: "/ws".to_owned(),
                stream_path: "/stream".to_owned(),
                metrics_path: "/metrics".to_owned(),
                health_path: "/healthz".to_owned(),
                heartbeat_interval: Duration::from_secs(180),
                heartbeat_timeout: Duration::from_secs(600),
                connection_ttl: None,
                max_clients: 16,
                client_buffer_size: 16,
                shutdown_drain_timeout: Duration::from_secs(1),
                max_filter_fields: 16,
                max_filter_values: 64,
                slow_client_drop_limit: 0,
            },
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
            replay: ReplayConfig {
                max_seconds: 0,
                dir: std::path::PathBuf::from("/tmp/replay-test"),
            },
        }
    }

    /// Find a `<metric>{...labels...} <value>` line in a Prometheus scrape
    /// body that contains the given metric name and all of the given
    /// `label="value"` substrings, and parse out its trailing float value.
    /// Returns `None` if no matching line exists.
    fn find_metric_value(rendered: &str, metric: &str, label_matchers: &[&str]) -> Option<f64> {
        rendered.lines().find_map(|line| {
            if !line.starts_with(metric) {
                return None;
            }
            if !label_matchers.iter().all(|m| line.contains(m)) {
                return None;
            }
            line.rsplit_once(' ')
                .and_then(|(_, v)| v.parse::<f64>().ok())
        })
    }

    #[test]
    fn update_head_block_gauges_emits_all_five_labels_per_declared_table() {
        // Use process-unique label values so this test's gauge series can't
        // collide with the other gauge tests (the metrics registry is a
        // process-wide singleton).
        crate::metrics::init();

        let identity = super::StreamIdentity {
            network: "gauge-test-net-decl".to_owned(),
            package_name: "pkg".to_owned(),
            package_version: "v1".to_owned(),
            module_hash: "h".to_owned(),
            manifest: "./gauge-test-decl.spkg".to_owned(),
            endpoint: "https://gauge-test-decl:443".to_owned(),
            tables: vec!["swaps".to_owned(), "transfers".to_owned()],
        };

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is past UNIX epoch")
            .as_secs() as i64;
        let block_ts = now_secs - 30;

        let decoded = crate::DatabaseChangesBlockMessage {
            network: identity.network.clone(),
            block_num: 12345,
            block_hash: "0xabc".to_owned(),
            timestamp: "2026-05-13 17:30:00".to_owned(),
            timestamp_seconds: block_ts,
            module_hash: identity.module_hash.clone(),
            // Empty events: declared tables must still receive gauge updates
            // because head_block_number is a property of the source spkg, not
            // of any individual row.
            events: vec![],
        };

        super::update_head_block_gauges(&identity, &decoded);

        let rendered = crate::metrics::render();

        for table in ["swaps", "transfers"] {
            let stream = format!("gauge-test-net-decl@{table}");
            let matchers = [
                "stream=\"".to_owned() + &stream + "\"",
                "network=\"gauge-test-net-decl\"".to_owned(),
                "table=\"".to_owned() + table + "\"",
                "spkg=\"./gauge-test-decl.spkg\"".to_owned(),
                "endpoint=\"https://gauge-test-decl:443\"".to_owned(),
            ];
            let matchers_ref: Vec<&str> = matchers.iter().map(String::as_str).collect();

            let block_num = find_metric_value(
                &rendered,
                "substreams_websocket_head_block_number",
                &matchers_ref,
            )
            .unwrap_or_else(|| {
                panic!("head_block_number gauge missing for {stream} in:\n{rendered}")
            });
            assert_eq!(block_num, 12345.0, "head_block_number for {stream}");

            let drift = find_metric_value(
                &rendered,
                "substreams_websocket_head_block_time_drift",
                &matchers_ref,
            )
            .unwrap_or_else(|| {
                panic!("head_block_time_drift gauge missing for {stream} in:\n{rendered}")
            });
            assert!(
                (30.0..120.0).contains(&drift),
                "drift {drift} for {stream} should be ~30s (block_ts was now-30); rendered:\n{rendered}"
            );
        }
    }

    #[test]
    fn update_head_block_gauges_falls_back_to_observed_tables_when_undeclared() {
        crate::metrics::init();

        // Empty `tables`: gauges must fall back to whatever @table values
        // appear in the block's events.
        let identity = super::StreamIdentity {
            network: "gauge-test-net-obs".to_owned(),
            package_name: "pkg".to_owned(),
            package_version: "v1".to_owned(),
            module_hash: "h".to_owned(),
            manifest: "./gauge-test-obs.spkg".to_owned(),
            endpoint: "https://gauge-test-obs:443".to_owned(),
            tables: Vec::new(),
        };

        let mk_event = |table: &str| {
            let mut m = serde_json::Map::new();
            m.insert("@table".to_owned(), serde_json::Value::String(table.into()));
            m
        };
        let decoded = crate::DatabaseChangesBlockMessage {
            network: identity.network.clone(),
            block_num: 999,
            block_hash: "0xdef".to_owned(),
            timestamp: "2026-05-13 17:30:00".to_owned(),
            timestamp_seconds: 1_700_000_000,
            module_hash: identity.module_hash.clone(),
            events: vec![mk_event("swaps"), mk_event("pools"), mk_event("swaps")],
        };

        super::update_head_block_gauges(&identity, &decoded);

        let rendered = crate::metrics::render();

        for table in ["swaps", "pools"] {
            let stream = format!("gauge-test-net-obs@{table}");
            let stream_match = format!("stream=\"{stream}\"");
            let table_match = format!("table=\"{table}\"");
            let matchers = [
                stream_match.as_str(),
                "network=\"gauge-test-net-obs\"",
                table_match.as_str(),
                "spkg=\"./gauge-test-obs.spkg\"",
                "endpoint=\"https://gauge-test-obs:443\"",
            ];

            let block_num = find_metric_value(
                &rendered,
                "substreams_websocket_head_block_number",
                &matchers,
            )
            .unwrap_or_else(|| {
                panic!("head_block_number gauge missing for {stream} in:\n{rendered}")
            });
            assert_eq!(block_num, 999.0);
        }

        // A table NOT observed in events and NOT declared must not emit.
        assert!(
            !rendered.contains("stream=\"gauge-test-net-obs@unrelated\""),
            "unexpected gauge for table that never appeared in events"
        );
    }

    #[test]
    fn update_head_block_gauges_passes_negative_drift_through() {
        // A block timestamped in the future (block producer ahead of us, or
        // clock skew) must surface as negative drift — operators may want to
        // alert on `drift < -X` to detect time-sync issues. Clamping to 0
        // would erase that signal.
        crate::metrics::init();

        let identity = super::StreamIdentity {
            network: "gauge-test-net-skew".to_owned(),
            package_name: "pkg".to_owned(),
            package_version: "v1".to_owned(),
            module_hash: "h".to_owned(),
            manifest: "./gauge-test-skew.spkg".to_owned(),
            endpoint: "https://gauge-test-skew:443".to_owned(),
            tables: vec!["swaps".to_owned()],
        };

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is past UNIX epoch")
            .as_secs() as i64;
        // Block timestamped 60s in the future of our clock.
        let block_ts = now_secs + 60;

        let decoded = crate::DatabaseChangesBlockMessage {
            network: identity.network.clone(),
            block_num: 42,
            block_hash: "0xskew".to_owned(),
            timestamp: "2099-01-01 00:00:00".to_owned(),
            timestamp_seconds: block_ts,
            module_hash: identity.module_hash.clone(),
            events: vec![],
        };

        super::update_head_block_gauges(&identity, &decoded);

        let rendered = crate::metrics::render();
        let matchers = ["stream=\"gauge-test-net-skew@swaps\""];
        let drift = find_metric_value(
            &rendered,
            "substreams_websocket_head_block_time_drift",
            &matchers,
        )
        .unwrap_or_else(|| panic!("drift gauge missing in:\n{rendered}"));

        // Drift ≈ -60s. Allow generous slack for slow CI:
        // drift = now_f64 - (now_secs + 60), and now_f64 - now_secs ∈ [0, 1+ε)
        // so drift lands somewhere in (-60, -59) under sane scheduling, with
        // wider headroom on a loaded test runner.
        assert!(
            (-65.0..-55.0).contains(&drift),
            "expected drift near -60s for future-dated block, got {drift}; rendered:\n{rendered}"
        );
    }
}
