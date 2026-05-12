use std::{net::SocketAddr, time::Duration};

#[derive(Debug, Clone)]
pub struct Config {
    pub substreams: SubstreamsConfig,
    pub websocket: WebSocketConfig,
}

#[derive(Debug, Clone)]
pub struct SubstreamsConfig {
    pub package: String,
    pub module: String,
    pub endpoint: Option<String>,
    pub start_block: Option<String>,
    pub stop_block: String,
    pub cursor: Option<String>,
    pub params: Vec<String>,
    pub plaintext: bool,
    pub insecure: bool,
    pub token: Option<String>,
    pub api_key: Option<String>,
    pub api_key_header: String,
}

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub listen: SocketAddr,
    pub ws_path: String,
    pub health_path: String,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub connection_ttl: Option<Duration>,
    pub max_clients: usize,
    pub client_buffer_size: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{field} must start with '/'")]
    PathMustStartWithSlash { field: &'static str },

    #[error("heartbeat timeout must be greater than heartbeat interval")]
    InvalidHeartbeatWindow,

    #[error("max clients must be greater than zero")]
    InvalidMaxClients,

    #[error("client buffer size must be greater than zero")]
    InvalidClientBufferSize,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_path("ws_path", &self.websocket.ws_path)?;
        validate_path("health_path", &self.websocket.health_path)?;

        if self.websocket.heartbeat_timeout <= self.websocket.heartbeat_interval {
            return Err(ConfigError::InvalidHeartbeatWindow);
        }

        if self.websocket.max_clients == 0 {
            return Err(ConfigError::InvalidMaxClients);
        }

        if self.websocket.client_buffer_size == 0 {
            return Err(ConfigError::InvalidClientBufferSize);
        }

        Ok(())
    }
}

fn validate_path(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.starts_with('/') {
        Ok(())
    } else {
        Err(ConfigError::PathMustStartWithSlash { field })
    }
}
