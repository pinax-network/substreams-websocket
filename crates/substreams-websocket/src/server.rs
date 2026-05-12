use std::{net::SocketAddr, sync::Arc};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use tower_http::trace::TraceLayer;
use tracing::{debug, info};

use crate::Config;

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
}

pub async fn serve(config: Config) -> Result<(), ServerError> {
    let state = AppState {
        config: Arc::new(config),
    };

    let listen = state.config.websocket.listen;
    let ws_path = state.config.websocket.ws_path.clone();
    let health_path = state.config.websocket.health_path.clone();
    let app = Router::new()
        .route(&state.config.websocket.health_path, get(health))
        .route(&state.config.websocket.ws_path, get(websocket))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|source| ServerError::Bind {
            addr: listen,
            source,
        })?;

    info!(
        listen = %listen,
        ws_path = %ws_path,
        health_path = %health_path,
        "starting Substreams WebSocket server"
    );

    axum::serve(listener, app).await.map_err(ServerError::Serve)
}

async fn health() -> &'static str {
    "ok"
}

async fn websocket(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(state, socket))
}

async fn handle_socket(state: AppState, socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();

    let welcome = serde_json::json!({
        "type": "session",
        "status": "connected",
        "module": state.config.substreams.module,
        "package": state.config.substreams.package,
    });

    if sender
        .send(Message::Text(welcome.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Text(text)) => {
                debug!(%text, "received WebSocket text message");
            }
            Ok(Message::Binary(_)) => {
                debug!("received WebSocket binary message");
            }
            Ok(Message::Ping(payload)) => {
                if sender.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("received WebSocket pong");
            }
            Ok(Message::Close(_)) => break,
            Err(error) => {
                debug!(%error, "WebSocket client error");
                break;
            }
        }
    }
}
