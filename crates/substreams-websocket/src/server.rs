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
use tracing::{debug, error, info, warn};

use crate::{
    BlockContext, Config, CursorStore, StreamConfig, StreamEvent, StreamName, SubstreamsClient,
    compute_module_hash_hex, decode_swaps, decode_transfers, substreams::load_package,
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
}

pub async fn serve(config: Config) -> Result<(), ServerError> {
    serve_with_shutdown(config, shutdown_signal()).await
}

pub async fn serve_with_shutdown(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    let state = AppState {
        config: Arc::new(config),
        clients: ClientRegistry::default(),
    };

    let listen = state.config.websocket.listen;
    let ws_path = state.config.websocket.ws_path.clone();
    let health_path = state.config.websocket.health_path.clone();
    let stream_tasks = spawn_streams(&state);
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
    Router::new()
        .route(&state.config.websocket.health_path, get(health))
        .route(&state.config.websocket.ws_path, get(websocket))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn spawn_streams(state: &AppState) -> Vec<tokio::task::JoinHandle<()>> {
    let cursors = CursorStore::new(state.config.cursors_dir.clone());
    state
        .config
        .streams
        .iter()
        .cloned()
        .map(|stream| {
            let clients = state.clients.clone();
            let cursors = cursors.clone();
            tokio::spawn(async move {
                run_substream(stream, clients, cursors).await;
            })
        })
        .collect()
}

fn stream_metadata(stream: &StreamConfig) -> serde_json::Value {
    serde_json::json!({
        "name": stream.name.as_str(),
        "network": stream.substreams.network.clone().unwrap_or_default(),
        "module": stream.substreams.module,
        "manifest": stream.substreams.manifest,
    })
}

async fn run_substream(mut stream: StreamConfig, clients: ClientRegistry, cursors: CursorStore) {
    let network = stream.substreams.network.clone().unwrap_or_default();

    let package = match load_package(&stream.substreams.manifest).await {
        Ok(package) => package,
        Err(error) => {
            error!(stream = %stream.name, %error, "failed to load Substreams package");
            clients
                .broadcast_json(stream_status(&stream, "error", error.to_string()))
                .await;
            return;
        }
    };

    let module_hash = match package
        .modules
        .as_ref()
        .ok_or("package contains no modules".to_owned())
        .and_then(|modules| {
            compute_module_hash_hex(modules, &stream.substreams.module).map_err(|e| e.to_string())
        }) {
        Ok(hash) => hash,
        Err(error) => {
            error!(stream = %stream.name, error, "failed to compute module hash");
            clients
                .broadcast_json(stream_status(&stream, "error", error))
                .await;
            return;
        }
    };

    match cursors.load(&network, &module_hash).await {
        Ok(Some(cursor)) => {
            info!(
                stream = %stream.name,
                network = %network,
                module_hash = %module_hash,
                cursor_len = cursor.len(),
                "resuming Substreams read from persisted cursor"
            );
            stream.substreams.start_cursor = Some(cursor);
        }
        Ok(None) => {}
        Err(error) => {
            warn!(stream = %stream.name, network = %network, %error, "failed to load cursor; starting from configured block");
        }
    }

    info!(
        stream = %stream.name,
        network = %network,
        module = %stream.substreams.module,
        module_hash = %module_hash,
        manifest = %stream.substreams.manifest,
        start_block = ?stream.substreams.start_block,
        has_cursor = stream.substreams.start_cursor.is_some(),
        "starting Substreams read"
    );

    let client = SubstreamsClient::new(stream.substreams.clone());
    let mut substream = match client.stream_with_package(package).await {
        Ok(substream) => substream,
        Err(error) => {
            error!(stream = %stream.name, %error, "Substreams read failed to start");
            clients
                .broadcast_json(stream_status(&stream, "error", error.to_string()))
                .await;
            return;
        }
    };

    clients
        .broadcast_json(stream_status(&stream, "started", String::new()))
        .await;

    loop {
        let event = match substream.next_event().await {
            Ok(Some(event)) => event,
            Ok(None) => {
                info!(stream = %stream.name, "Substreams read completed");
                clients
                    .broadcast_json(stream_status(&stream, "completed", String::new()))
                    .await;
                break;
            }
            Err(error) => {
                error!(stream = %stream.name, %error, "Substreams read failed");
                clients
                    .broadcast_json(stream_status(&stream, "error", error.to_string()))
                    .await;
                break;
            }
        };

        handle_substream_event(&stream, &clients, &cursors, &module_hash, event).await;
    }
}

async fn handle_substream_event(
    stream: &StreamConfig,
    clients: &ClientRegistry,
    cursors: &CursorStore,
    module_hash: &str,
    event: StreamEvent,
) {
    let network = stream.substreams.network.clone().unwrap_or_default();
    let name = stream.name.as_str();

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
            };
            let decoded = match decode_stream_payload(stream.name, &payload, context) {
                Ok(decoded) => decoded,
                Err(error) => {
                    warn!(stream = %stream.name, %error, "failed to decode Substreams block output");
                    clients
                        .broadcast_json(stream_status(stream, "decode_error", error.to_string()))
                        .await;
                    return;
                }
            };

            clients.broadcast_json(decoded).await;

            if let Err(error) = cursors.save(&network, module_hash, &cursor).await {
                warn!(stream = %stream.name, %error, "failed to persist Substreams cursor");
            }
        }
        StreamEvent::Fatal { message } => {
            clients
                .broadcast_json(stream_status(stream, "fatal", message))
                .await;
        }
        StreamEvent::Undo {
            last_valid_block,
            last_valid_cursor,
        } => {
            clients
                .broadcast_json(serde_json::json!({
                    "type": "stream",
                    "status": "undo",
                    "name": name,
                    "network": network,
                    "last_valid_block": last_valid_block,
                }))
                .await;
            if let Err(error) = cursors
                .save(&network, module_hash, &last_valid_cursor)
                .await
            {
                warn!(stream = %stream.name, %error, "failed to persist last-valid cursor");
            }
        }
        StreamEvent::Session { .. }
        | StreamEvent::Progress { .. }
        | StreamEvent::SnapshotData { .. }
        | StreamEvent::SnapshotComplete
        | StreamEvent::Unknown => {}
    }
}

fn decode_stream_payload(
    name: StreamName,
    payload: &[u8],
    context: BlockContext,
) -> Result<serde_json::Value, crate::DecodeError> {
    match name {
        StreamName::Swaps => serde_json::to_value(decode_swaps(payload, context)?),
        StreamName::Transfers => serde_json::to_value(decode_transfers(payload, context)?),
    }
    .map_err(crate::DecodeError::Serialize)
}

fn stream_status(stream: &StreamConfig, status: &str, message: String) -> serde_json::Value {
    let mut value = serde_json::json!({
        "type": "stream",
        "status": status,
        "name": stream.name.as_str(),
        "network": stream.substreams.network.clone().unwrap_or_default(),
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

async fn websocket(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    if state.clients.active_count().await >= state.config.websocket.max_clients {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(state, socket))
        .into_response()
}

async fn handle_socket(state: AppState, socket: WebSocket) {
    let Some(client) = state.clients.register(&state.config).await else {
        warn!("rejecting WebSocket client because max client count was reached");
        return;
    };

    info!(client_id = client.name, "WebSocket client connected");

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
        "streams": state.config.streams.iter().map(stream_metadata).collect::<Vec<_>>(),
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

    loop {
        tokio::select! {
            message = receiver.next() => {
                let Some(message) = message else {
                    break;
                };

                match message {
                    Ok(Message::Text(text)) => {
                        debug!(%text, "received WebSocket text message");
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
    info!(client_id, "WebSocket client disconnected");
}

#[derive(Clone, Default)]
struct ClientRegistry {
    next_id: Arc<AtomicU64>,
    clients: Arc<RwLock<HashMap<ClientId, ClientHandle>>>,
}

impl ClientRegistry {
    async fn register(&self, config: &Config) -> Option<RegisteredClient> {
        let mut clients = self.clients.write().await;
        if clients.len() >= config.websocket.max_clients {
            return None;
        }

        let name = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = mpsc::channel(config.websocket.client_buffer_size);
        clients.insert(name, ClientHandle { tx: tx.clone() });

        Some(RegisteredClient { name, tx, rx })
    }

    async fn unregister(&self, name: ClientId) {
        self.clients.write().await.remove(&name);
    }

    async fn active_count(&self) -> usize {
        self.clients.read().await.len()
    }

    async fn broadcast_json(&self, value: serde_json::Value) {
        let message = Message::Text(value.to_string().into());
        let clients = self.clients.read().await;

        for (client_id, client) in clients.iter() {
            if client.tx.try_send(message.clone()).is_err() {
                warn!(
                    client_id,
                    "dropping broadcast message for slow WebSocket client"
                );
            }
        }
    }
}

#[derive(Clone)]
struct ClientHandle {
    tx: mpsc::Sender<Message>,
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
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use futures_util::StreamExt;
    use prost::Message as ProstMessage;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;
    use tokio_tungstenite::{connect_async, tungstenite::Message as TungsteniteMessage};

    use super::*;
    use crate::decoder::pb::dex::swaps::v1::{Events as SwapEvents, Protocol, Swap, Transaction};
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

        let (mut socket, _) = connect_async(format!("ws://{}/ws", server.addr))
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
        assert_eq!(body["streams"][0]["name"], "swaps");
        assert_eq!(body["streams"][0]["name"], "swaps");
        assert_eq!(body["streams"][0]["module"], "swaps");
        assert_eq!(body["streams"][0]["network"], "solana-mainnet");
        assert_eq!(body["streams"][0]["manifest"], "./demo.spkg");
        assert!(body["client_id"].as_u64().is_some());

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

        let (_socket, _) = connect_async(format!("ws://{}/ws", server.addr))
            .await
            .expect("first websocket connects");

        let second = connect_async(format!("ws://{}/ws", server.addr)).await;
        assert!(second.is_err(), "second websocket should be rejected");
    }

    #[tokio::test]
    async fn websocket_receives_heartbeat_ping() {
        let mut cfg = config();
        cfg.websocket.heartbeat_interval = Duration::from_millis(25);
        cfg.websocket.heartbeat_timeout = Duration::from_secs(1);
        let server = TestServer::start(cfg).await;

        let (mut socket, _) = connect_async(format!("ws://{}/ws", server.addr))
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

        let (mut stale_socket, _) = connect_async(format!("ws://{}/ws", server.addr))
            .await
            .expect("first websocket connects");
        let _welcome = stale_socket
            .next()
            .await
            .expect("welcome")
            .expect("welcome ok");

        let mut connected_after_eviction = false;
        for _ in 0..40 {
            if connect_async(format!("ws://{}/ws", server.addr))
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

        let (mut socket, _) = connect_async(format!("ws://{}/ws", server.addr))
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
    async fn websocket_client_receives_decoded_swap_block() {
        let server = TestServer::start(config()).await;

        let (mut socket, _) = connect_async(format!("ws://{}/ws", server.addr))
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

        let events = SwapEvents {
            transactions: vec![Transaction {
                signature: vec![1u8; 32],
                fee_payer: vec![2u8; 32],
                signers: vec![vec![2u8; 32]],
                fee: 5_000,
                compute_units_consumed: 1_234,
                swaps: vec![Swap {
                    protocol: Protocol::RaydiumAmmV4 as i32,
                    program_id: vec![3u8; 32],
                    stack_height: 1,
                    amm: vec![4u8; 32],
                    amm_pool: vec![5u8; 32],
                    user: vec![6u8; 32],
                    input_mint: vec![7u8; 32],
                    input_amount: 1_000,
                    output_mint: vec![8u8; 32],
                    output_amount: 2_000,
                }],
            }],
        };
        let mut payload = Vec::new();
        events.encode(&mut payload).expect("events encode");

        let stream = server.config.streams[0].clone();
        let cursors_dir = tempfile::tempdir().expect("cursor tempdir");
        let cursors = CursorStore::new(cursors_dir.path());
        handle_substream_event(
            &stream,
            &server.clients,
            &cursors,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            StreamEvent::Block {
                number: 999,
                id: "block-999".to_owned(),
                timestamp: "2026-05-12 17:30:00".to_owned(),
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

        let body: serde_json::Value = serde_json::from_str(&text).expect("decoded json");
        assert_eq!(body["type"], "swaps");
        assert_eq!(body["network"], "solana-mainnet");
        assert_eq!(body["block"]["number"], 999);
        assert_eq!(body["block"]["hash"], "block-999");
        assert_eq!(
            body["transactions"][0]["swaps"][0]["protocol"],
            "raydium_amm_v4"
        );
        assert_eq!(body["transactions"][0]["swaps"][0]["input_amount"], 1_000);
        assert_eq!(body["transactions"][0]["swaps"][0]["output_amount"], 2_000);

        socket.close(None).await.expect("client closes cleanly");
    }

    struct TestServer {
        addr: SocketAddr,
        clients: ClientRegistry,
        config: Arc<Config>,
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
            let state = AppState {
                config: Arc::new(config),
                clients: ClientRegistry::default(),
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
                name: StreamName::Swaps,
                substreams: SubstreamsConfig {
                    manifest: "./demo.spkg".to_owned(),
                    module: "swaps".to_owned(),
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
                health_path: "/healthz".to_owned(),
                heartbeat_interval: Duration::from_secs(180),
                heartbeat_timeout: Duration::from_secs(600),
                connection_ttl: None,
                max_clients: 16,
                client_buffer_size: 16,
            },
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
        }
    }
}
