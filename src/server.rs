use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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
    BlockContext, Config, CursorStore, ReplayLog, SUPPORTED_OUTPUT_TYPE, StreamConfig, StreamEvent,
    SubstreamsClient, compute_module_hash_hex, decode_database_changes, substreams::load_package,
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
}

#[derive(Debug, Clone, serde::Serialize)]
struct StreamMeta {
    #[serde(rename = "stream")]
    name: String,
    network: String,
    module: String,
    manifest: String,
    module_hash: String,
    /// Top-level spkg name from Package.package_meta[0].name, when present.
    #[serde(skip_serializing_if = "String::is_empty")]
    package_name: String,
    /// Top-level spkg version from Package.package_meta[0].version, when present.
    #[serde(skip_serializing_if = "String::is_empty")]
    package_version: String,
    /// Package description sourced from PackageMetadata.description or PackageMetadata.doc.
    #[serde(skip_serializing_if = "String::is_empty")]
    description: String,
}

pub async fn serve(config: Config) -> Result<(), ServerError> {
    serve_with_shutdown(config, shutdown_signal()).await
}

pub async fn serve_with_shutdown(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    let config = Arc::new(config);

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

    let replay = ReplayLog::new(config.replay.dir.clone(), config.replay.max_blocks);

    let state = AppState {
        config: config.clone(),
        clients: ClientRegistry::default(),
        streams_meta,
        replay,
    };

    let listen = state.config.websocket.listen;
    let ws_path = state.config.websocket.ws_path.clone();
    let health_path = state.config.websocket.health_path.clone();
    let stream_tasks = spawn_streams(&state, prepared);
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|source| ServerError::Bind {
            addr: listen,
            source,
        })?;

    let result = serve_listener(listener, app, ws_path, health_path, shutdown).await;

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
    Router::new()
        .route("/", get(landing_html))
        .route("/streams", get(streams_json))
        .route("/SKILL.md", get(skill_md))
        .route("/llms.txt", get(llms_txt))
        .route("/favicon.ico", get(favicon_png))
        .route("/favicon.png", get(favicon_png))
        .route(&state.config.websocket.health_path, get(health))
        .route(&ws_root, get(websocket_no_streams))
        .route(&ws_wildcard, get(websocket_path))
        .route(&stream_path, get(websocket_stream_query))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
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
    let name = stream.name.clone();

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
            name: name.clone(),
            network: network.clone(),
            module: module.clone(),
            manifest: manifest.clone(),
            module_hash,
            package_name: pkg_name,
            package_version: pkg_version,
            description,
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
    let network = meta.network.clone();
    let module_hash = meta.module_hash.clone();

    if let Some(error) = error {
        error!(stream = %config.name, error, "stream preparation failed");
        clients
            .broadcast_matching(
                &network,
                &config.name,
                stream_status(&config, &module_hash, "error", error),
            )
            .await;
        return;
    }
    let package = package.expect("prepared stream without error must have package");

    let mut backoff = RESTART_BACKOFF_MIN;

    loop {
        // Reload cursor from disk on every retry so we resume from the latest
        // persisted position, not whatever the previous run started with.
        match cursors.load(&network, &module_hash).await {
            Ok(Some(cursor)) => {
                info!(
                    stream = %config.name,
                    network = %network,
                    module_hash = %module_hash,
                    cursor_len = cursor.len(),
                    "resuming Substreams read from persisted cursor"
                );
                config.substreams.start_cursor = Some(cursor);
            }
            Ok(None) => {
                config.substreams.start_cursor = None;
            }
            Err(error) => {
                warn!(stream = %config.name, network = %network, %error, "failed to load cursor; starting from configured block");
                config.substreams.start_cursor = None;
            }
        }

        info!(
            stream = %config.name,
            network = %network,
            module = %config.substreams.module,
            module_hash = %module_hash,
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
                error!(stream = %config.name, %error, backoff_secs = backoff.as_secs(), "Substreams read failed to start; will retry");
                clients
                    .broadcast_matching(
                        &network,
                        &config.name,
                        stream_status(&config, &module_hash, "error", msg),
                    )
                    .await;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
                continue;
            }
        };

        clients
            .broadcast_matching(
                &network,
                &config.name,
                stream_status(&config, &module_hash, "started", String::new()),
            )
            .await;

        let outcome = read_loop(
            &config,
            &clients,
            &cursors,
            &replay,
            &module_hash,
            &mut substream,
        )
        .await;
        match outcome {
            ReadOutcome::Completed => {
                info!(stream = %config.name, "Substreams read completed");
                clients
                    .broadcast_matching(
                        &network,
                        &config.name,
                        stream_status(&config, &module_hash, "completed", String::new()),
                    )
                    .await;
                return;
            }
            ReadOutcome::ProducedBlock => {
                // We made progress this attempt: reset backoff before the next retry.
                backoff = RESTART_BACKOFF_MIN;
                error!(stream = %config.name, "Substreams read stream ended unexpectedly; will retry");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
            }
            ReadOutcome::Errored(err) => {
                error!(stream = %config.name, error = %err, backoff_secs = backoff.as_secs(), "Substreams read failed; will retry");
                clients
                    .broadcast_matching(
                        &network,
                        &config.name,
                        stream_status(&config, &module_hash, "error", err),
                    )
                    .await;
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
    config: &StreamConfig,
    clients: &ClientRegistry,
    cursors: &CursorStore,
    replay: &ReplayLog,
    module_hash: &str,
    substream: &mut crate::substreams::SubstreamsStream,
) -> ReadOutcome {
    let mut produced_any = false;
    loop {
        match substream.next_event().await {
            Ok(Some(event)) => {
                if matches!(event, StreamEvent::Block { .. }) {
                    produced_any = true;
                }
                handle_substream_event(config, clients, cursors, replay, module_hash, event).await;
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
    config: &StreamConfig,
    clients: &ClientRegistry,
    cursors: &CursorStore,
    replay: &ReplayLog,
    module_hash: &str,
    event: StreamEvent,
) {
    let network = config.substreams.network.clone().unwrap_or_default();
    let stream_name = config.name.as_str();

    match event {
        StreamEvent::Block {
            number,
            id,
            timestamp,
            output_type_url: _,
            payload,
            cursor,
        } => {
            let context = BlockContext {
                block_num: number,
                block_hash: id,
                timestamp,
                network: network.clone(),
                module_hash: module_hash.to_owned(),
            };
            let decoded = match decode_database_changes(&config.name, &payload, context) {
                Ok(decoded) => decoded,
                Err(error) => {
                    warn!(stream = %config.name, %error, "failed to decode Substreams block output");
                    clients
                        .broadcast_matching(
                            &network,
                            &config.name,
                            stream_status(config, module_hash, "decode_error", error.to_string()),
                        )
                        .await;
                    return;
                }
            };

            // Skip empty-block broadcasts. Cursor is still persisted so resume
            // continues from this block on restart.
            if !decoded.events.is_empty() {
                let block_num = decoded.block_num;
                let event_count = decoded.events.len();
                let json = match serde_json::to_value(decoded) {
                    Ok(value) => value,
                    Err(error) => {
                        warn!(stream = %config.name, %error, "failed to serialize decoded block");
                        return;
                    }
                };
                let payload_text = json.to_string();
                if let Err(error) = replay.append(&network, &config.name, &payload_text).await {
                    warn!(stream = %config.name, %error, "failed to append replay log");
                }
                let delivered = clients
                    .broadcast_matching_text(&network, &config.name, payload_text)
                    .await;
                debug!(
                    stream = %config.name,
                    network = %network,
                    block_num,
                    events = event_count,
                    delivered,
                    "broadcast block"
                );
            }

            if let Err(error) = cursors.save(&network, module_hash, &cursor).await {
                warn!(stream = %config.name, %error, "failed to persist Substreams cursor");
            }
        }
        StreamEvent::Fatal { message } => {
            clients
                .broadcast_matching(
                    &network,
                    &config.name,
                    stream_status(config, module_hash, "fatal", message),
                )
                .await;
        }
        StreamEvent::Undo {
            last_valid_block,
            last_valid_cursor,
        } => {
            clients
                .broadcast_matching(
                    &network,
                    &config.name,
                    serde_json::json!({
                        "type": "stream",
                        "status": "undo",
                        "stream": stream_name,
                        "network": network,
                        "module_hash": module_hash,
                        "last_valid_block": last_valid_block,
                    }),
                )
                .await;
            if let Err(error) = cursors
                .save(&network, module_hash, &last_valid_cursor)
                .await
            {
                warn!(stream = %config.name, %error, "failed to persist last-valid cursor");
            }
            if let Err(error) = replay
                .truncate_after(&network, &config.name, last_valid_block)
                .await
            {
                warn!(stream = %config.name, %error, "failed to truncate replay log after reorg");
            }
        }
        StreamEvent::Session { .. }
        | StreamEvent::Progress { .. }
        | StreamEvent::SnapshotData { .. }
        | StreamEvent::SnapshotComplete
        | StreamEvent::Unknown => {}
    }
}

fn stream_status(
    config: &StreamConfig,
    module_hash: &str,
    status: &str,
    message: String,
) -> serde_json::Value {
    let mut value = serde_json::json!({
        "type": "stream",
        "status": status,
        "stream": config.name,
        "network": config.substreams.network.clone().unwrap_or_default(),
        "module_hash": module_hash,
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

async fn health() -> &'static str {
    "ok"
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
    let from_block = match parse_from_block(raw_query.as_deref()) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let wrap_envelope = entries.len() > 1;
    let filter = StreamFilter {
        entries,
        wrap_envelope,
    };
    ws.on_upgrade(move |socket| handle_socket(state, filter, from_block, socket))
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
    let from_block = match parse_from_block(Some(&raw)) {
        Ok(v) => v,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let filter = StreamFilter {
        entries,
        wrap_envelope: true,
    };
    ws.on_upgrade(move |socket| handle_socket(state, filter, from_block, socket))
        .into_response()
}

/// Replay buffered blocks for every explicit `network@stream` selector with
/// `block_num > from_block`. Wildcard selectors are skipped — they have no
/// concrete file to scan and cannot resume from a single window. For each
/// explicit selector with at least one retained block, either replay matching
/// blocks (oldest first) or emit a `gap` lifecycle message when `from_block`
/// falls below the oldest retained block.
async fn replay_for_client(
    replay: &ReplayLog,
    filter: &StreamFilter,
    from_block: u64,
    client_id: ClientId,
    outbound: &mpsc::Sender<Message>,
) -> Result<(), String> {
    if !replay.is_enabled() {
        return Ok(());
    }

    for entry in &filter.entries {
        let (Some(network), Some(stream)) = (entry.network.as_deref(), entry.stream.as_deref())
        else {
            // Wildcards skipped — no concrete log file to scan.
            continue;
        };

        let result = replay
            .read_from(network, stream, from_block)
            .await
            .map_err(|e| e.to_string())?;

        let oldest = match result.oldest {
            Some(v) => v,
            None => continue,
        };

        if from_block + 1 < oldest {
            // Resume point below the retained window — tell client there is a
            // gap and continue with live stream only.
            let gap = serde_json::json!({
                "type": "stream",
                "status": "gap",
                "stream": stream,
                "network": network,
                "requested_block": from_block,
                "oldest_buffered_block": oldest,
                "reason": "requested block outside replay window",
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
                from_block,
                oldest_buffered_block = oldest,
                "replay gap"
            );
            continue;
        }

        let replayed = result.blocks.len();
        for (_block_num, raw_text) in result.blocks {
            let text = if filter.wrap_envelope {
                format!(r#"{{"stream":"{network}@{stream}","data":{raw_text}}}"#)
            } else {
                raw_text
            };
            if outbound.send(Message::Text(text.into())).await.is_err() {
                return Err("outbound channel closed during replay".to_owned());
            }
        }

        if replayed > 0 {
            info!(
                client_id,
                network, stream, from_block, replayed, "replay delivered"
            );
        }
    }

    Ok(())
}

fn parse_from_block(raw_query: Option<&str>) -> Result<Option<u64>, &'static str> {
    let Some(raw) = raw_query else {
        return Ok(None);
    };
    let Some((_, value)) = url_query_pairs(raw).find(|(k, _)| k == "from_block") else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| "from_block must be a non-negative integer")
}

async fn handle_socket(
    state: AppState,
    filter: StreamFilter,
    from_block: Option<u64>,
    socket: WebSocket,
) {
    let Some((client, filter_handle)) = state.clients.register(&state.config, filter.clone()).await
    else {
        warn!("rejecting WebSocket client because max client count was reached");
        return;
    };

    let initial_subs: Vec<String> = filter.entries.iter().map(StreamId::to_wire).collect();
    info!(
        client_id = client.name,
        subscriptions = ?initial_subs,
        wrap_envelope = filter.wrap_envelope,
        "WebSocket client connected"
    );

    let (mut sender, mut receiver) = socket.split();
    let mut messages = client.rx;
    let outbound = client.tx.clone();
    let client_id = client.name;
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

    if let Some(from) = from_block
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
                        let reply = handle_subscription_command(client_id, &filter_handle, text.as_str()).await;
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
    info!(
        client_id,
        duration_secs = connected_at.elapsed().as_secs(),
        "WebSocket client disconnected"
    );
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
}

impl ClientRegistry {
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
        clients.insert(
            name,
            ClientHandle {
                tx: tx.clone(),
                filter: Arc::clone(&filter),
            },
        );

        Some((RegisteredClient { name, tx, rx }, filter))
    }

    async fn unregister(&self, name: ClientId) {
        self.clients.write().await.remove(&name);
    }

    async fn active_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Send `value` to every client whose filter accepts `(network, stream)`.
    /// Clients with `wrap_envelope = true` get the message wrapped in
    /// `{"stream":"<network>@<stream>","data":<value>}`; otherwise the raw
    /// value is sent.
    async fn broadcast_matching(
        &self,
        network: &str,
        stream: &str,
        value: serde_json::Value,
    ) -> usize {
        self.broadcast_matching_text(network, stream, value.to_string())
            .await
    }

    async fn broadcast_matching_text(
        &self,
        network: &str,
        stream: &str,
        raw_text: String,
    ) -> usize {
        let wrapped_text = format!(r#"{{"stream":"{network}@{stream}","data":{raw_text}}}"#);
        let payload_bytes = raw_text.len();
        let raw_message = Message::Text(raw_text.into());
        let wrapped_message = Message::Text(wrapped_text.into());
        let clients = self.clients.read().await;
        let mut delivered: usize = 0;

        for (client_id, client) in clients.iter() {
            let filter = client.filter.read().await;
            if !filter.matches(network, stream) {
                continue;
            }
            let msg = if filter.wrap_envelope {
                wrapped_message.clone()
            } else {
                raw_message.clone()
            };
            drop(filter);
            if client.tx.try_send(msg).is_err() {
                warn!(
                    client_id,
                    "dropping broadcast message for slow WebSocket client"
                );
            } else {
                delivered += 1;
            }
        }

        trace!(
            network,
            stream,
            payload_bytes,
            delivered,
            total_clients = clients.len(),
            "broadcast"
        );

        delivered
    }
}

#[derive(Clone)]
struct ClientHandle {
    tx: mpsc::Sender<Message>,
    filter: Arc<RwLock<StreamFilter>>,
}

struct RegisteredClient {
    name: ClientId,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
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
async fn handle_subscription_command(
    client_id: ClientId,
    filter: &Arc<RwLock<StreamFilter>>,
    text: &str,
) -> String {
    #[derive(serde::Deserialize)]
    struct Command {
        method: String,
        #[serde(default)]
        params: Vec<String>,
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

    match cmd.method.as_str() {
        "SUBSCRIBE" => {
            let mut parsed = Vec::with_capacity(cmd.params.len());
            for p in &cmd.params {
                match StreamId::parse(p) {
                    Ok(id) => parsed.push(id),
                    Err(error) => {
                        warn!(
                            client_id,
                            params = ?cmd.params,
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
                params = ?cmd.params,
                added,
                total,
                "SUBSCRIBE"
            );
            serde_json::json!({ "result": serde_json::Value::Null, "id": id }).to_string()
        }
        "UNSUBSCRIBE" => {
            let mut parsed = Vec::with_capacity(cmd.params.len());
            for p in &cmd.params {
                match StreamId::parse(p) {
                    Ok(id) => parsed.push(id),
                    Err(error) => {
                        warn!(
                            client_id,
                            params = ?cmd.params,
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
                params = ?cmd.params,
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
        other => {
            warn!(client_id, method = %other, "unknown WebSocket method");
            serde_json::json!({
                "error": format!("unknown method {other:?}; expected SUBSCRIBE, UNSUBSCRIBE, or LIST_SUBSCRIPTIONS"),
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
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "failed to listen for shutdown signal");
    }
}

#[cfg(test)]
mod filter_tests {
    use super::{StreamFilter, StreamId, parse_stream_list};

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
        };
        assert!(f.matches("solana-mainnet", "swaps"));
        assert!(f.matches("anything", "anywhere"));
    }

    #[test]
    fn exact_pair_matches_only_that_pair() {
        let f = StreamFilter {
            entries: vec![id(Some("solana-mainnet"), Some("swaps"))],
            wrap_envelope: false,
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
        let reply =
            super::handle_subscription_command(0, &filter, r#"{"method":"DESTROY","id":99}"#).await;
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["id"], 99);
        assert!(value["error"].as_str().unwrap().contains("unknown method"));
    }

    #[tokio::test]
    async fn invalid_json_returns_error() {
        let filter = std::sync::Arc::new(tokio::sync::RwLock::new(StreamFilter::default()));
        let reply = super::handle_subscription_command(0, &filter, "not json").await;
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
        assert_eq!(body["streams"][0]["stream"], "swaps");
        assert_eq!(body["streams"][0]["module"], "db_out");
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

        let stream_config = server.config.streams[0].clone();
        let cursors_dir = tempfile::tempdir().expect("cursor tempdir");
        let cursors = CursorStore::new(cursors_dir.path());
        let replay = ReplayLog::disabled();
        handle_substream_event(
            &stream_config,
            &server.clients,
            &cursors,
            &replay,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            StreamEvent::Block {
                number: 999,
                id: "block-999".to_owned(),
                timestamp: "2026-05-13 17:30:00".to_owned(),
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
        // `/stream?streams=*@*` wraps in {"stream":"<network>@<stream>","data":...}
        assert_eq!(envelope["stream"], "solana-mainnet@swaps");
        let body = &envelope["data"];
        assert_eq!(body["stream"], "swaps");
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
        assert_eq!(body["events"][0]["@table"], "swaps");
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
    async fn websocket_replays_blocks_above_from_block() {
        let replay_dir = tempfile::tempdir().expect("replay tempdir");
        let mut cfg = config();
        cfg.replay = ReplayConfig {
            max_blocks: 100,
            dir: replay_dir.path().to_path_buf(),
        };
        let server = TestServer::start(cfg).await;

        for n in 100..105u64 {
            let payload = serde_json::json!({
                "stream": "swaps",
                "network": "solana-mainnet",
                "block_num": n,
                "events": [],
            })
            .to_string();
            server
                .replay
                .append("solana-mainnet", "swaps", &payload)
                .await
                .expect("seed replay");
        }

        let (mut socket, _) = connect_async(format!(
            "ws://{}/ws/solana-mainnet@swaps?from_block=101",
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
            got.push(value["block_num"].as_u64().expect("block_num"));
        }
        assert_eq!(got, vec![102, 103, 104]);

        socket.close(None).await.expect("clean close");
    }

    #[tokio::test]
    async fn websocket_emits_gap_when_from_block_below_window() {
        let replay_dir = tempfile::tempdir().expect("replay tempdir");
        let mut cfg = config();
        cfg.replay = ReplayConfig {
            max_blocks: 100,
            dir: replay_dir.path().to_path_buf(),
        };
        let server = TestServer::start(cfg).await;

        for n in 500..505u64 {
            let payload = serde_json::json!({
                "stream": "swaps",
                "network": "solana-mainnet",
                "block_num": n,
                "events": [],
            })
            .to_string();
            server
                .replay
                .append("solana-mainnet", "swaps", &payload)
                .await
                .expect("seed replay");
        }

        let (mut socket, _) = connect_async(format!(
            "ws://{}/ws/solana-mainnet@swaps?from_block=10",
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
        assert_eq!(value["requested_block"], 10);
        assert_eq!(value["oldest_buffered_block"], 500);

        socket.close(None).await.expect("clean close");
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
                        name: s.name.clone(),
                        network: s.substreams.network.clone().unwrap_or_default(),
                        module: s.substreams.module.clone(),
                        manifest: s.substreams.manifest.clone(),
                        module_hash: String::new(),
                        package_name: String::new(),
                        package_version: String::new(),
                        description: String::new(),
                    })
                    .collect::<Vec<_>>(),
            );
            let replay = ReplayLog::new(config.replay.dir.clone(), config.replay.max_blocks);
            let state = AppState {
                config: Arc::new(config),
                clients: ClientRegistry::default(),
                streams_meta,
                replay: replay.clone(),
            };
            let clients = state.clients.clone();
            let config = state.config.clone();
            let app = build_app(state);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            tokio::spawn(async move {
                serve_listener(listener, app, ws_path, health_path, async {
                    let _ = shutdown_rx.await;
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
                name: "swaps".to_owned(),
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
                health_path: "/healthz".to_owned(),
                heartbeat_interval: Duration::from_secs(180),
                heartbeat_timeout: Duration::from_secs(600),
                connection_ttl: None,
                max_clients: 16,
                client_buffer_size: 16,
            },
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
            replay: ReplayConfig {
                max_blocks: 0,
                dir: std::path::PathBuf::from("/tmp/replay-test"),
            },
        }
    }
}
