use std::{net::SocketAddr, time::Duration};

#[derive(Debug, Clone)]
pub struct Config {
    pub streams: Vec<StreamConfig>,
    pub websocket: WebSocketConfig,
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub id: String,
    pub decoder: StreamDecoder,
    pub substreams: SubstreamsConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDecoder {
    Swaps,
    Transfers,
}

impl StreamDecoder {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Swaps => "swaps",
            Self::Transfers => "transfers",
        }
    }
}

impl std::fmt::Display for StreamDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for StreamDecoder {
    type Err = StreamDecoderParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "swaps" => Ok(Self::Swaps),
            "transfers" => Ok(Self::Transfers),
            _ => Err(StreamDecoderParseError {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid stream decoder {value:?}, expected swaps or transfers")]
pub struct StreamDecoderParseError {
    value: String,
}

#[derive(Debug, Clone)]
pub struct SubstreamsConfig {
    pub package: String,
    pub module: String,
    pub endpoint: Option<String>,
    pub network: Option<String>,
    pub start_block: Option<String>,
    pub stop_block: String,
    pub params: Vec<String>,
    pub plaintext: bool,
    pub insecure: bool,
    pub production_mode: bool,
    pub final_blocks_only: bool,
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

    #[error("at least one stream must be configured")]
    NoStreams,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.streams.is_empty() {
            return Err(ConfigError::NoStreams);
        }

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
