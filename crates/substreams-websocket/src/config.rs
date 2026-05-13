use std::{net::SocketAddr, time::Duration};

#[derive(Debug, Clone)]
pub struct Config {
    pub streams: Vec<StreamConfig>,
    pub websocket: WebSocketConfig,
    pub cursors_dir: std::path::PathBuf,
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub name: StreamName,
    pub substreams: SubstreamsConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamName {
    Swaps,
    Transfers,
}

impl StreamName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Swaps => "swaps",
            Self::Transfers => "transfers",
        }
    }
}

impl std::fmt::Display for StreamName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for StreamName {
    type Err = StreamNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "swaps" => Ok(Self::Swaps),
            "transfers" => Ok(Self::Transfers),
            _ => Err(StreamNameParseError {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid stream name {value:?}, expected swaps or transfers")]
pub struct StreamNameParseError {
    value: String,
}

#[derive(Debug, Clone)]
pub struct SubstreamsConfig {
    pub manifest: String,
    pub module: String,
    pub endpoint: Option<String>,
    pub network: Option<String>,
    pub start_block: Option<String>,
    pub start_cursor: Option<String>,
    pub stop_block: String,
    pub params: Vec<String>,
    pub plaintext: bool,
    pub insecure: bool,
    pub production_mode: bool,
    pub final_blocks_only: bool,
    pub token: Option<String>,
    pub api_key: Option<String>,
    pub api_key_header: String,
    pub auth_url: Option<String>,
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

    #[error("stream {index} ({name}) is missing a Substreams endpoint")]
    MissingStreamEndpoint { index: usize, name: String },

    #[error("stream {index} ({name}) is missing a network")]
    MissingStreamNetwork { index: usize, name: String },

    #[error("duplicate stream registration: network={network:?} name={name:?}")]
    DuplicateStream { network: String, name: String },

    #[error("stream {index} ({name}) is missing a start_block")]
    MissingStreamStartBlock { index: usize, name: String },
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.streams.is_empty() {
            return Err(ConfigError::NoStreams);
        }

        let mut seen = std::collections::HashSet::<(String, &'static str)>::new();
        for (index, stream) in self.streams.iter().enumerate() {
            let name = stream.name.as_str();
            let endpoint = stream
                .substreams
                .endpoint
                .as_deref()
                .map(str::trim)
                .unwrap_or("");
            if endpoint.is_empty() {
                return Err(ConfigError::MissingStreamEndpoint {
                    index,
                    name: name.to_owned(),
                });
            }

            let network = stream
                .substreams
                .network
                .as_deref()
                .map(str::trim)
                .unwrap_or("");
            if network.is_empty() {
                return Err(ConfigError::MissingStreamNetwork {
                    index,
                    name: name.to_owned(),
                });
            }

            let start_block = stream
                .substreams
                .start_block
                .as_deref()
                .map(str::trim)
                .unwrap_or("");
            if start_block.is_empty() {
                return Err(ConfigError::MissingStreamStartBlock {
                    index,
                    name: name.to_owned(),
                });
            }

            if !seen.insert((network.to_owned(), name)) {
                return Err(ConfigError::DuplicateStream {
                    network: network.to_owned(),
                    name: name.to_owned(),
                });
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(name: StreamName, network: &str, endpoint: &str) -> StreamConfig {
        StreamConfig {
            name,
            substreams: SubstreamsConfig {
                manifest: "./demo.spkg".to_owned(),
                module: "map_events".to_owned(),
                endpoint: Some(endpoint.to_owned()),
                network: Some(network.to_owned()),
                start_block: Some("-1".to_owned()),
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
        }
    }

    fn websocket() -> WebSocketConfig {
        WebSocketConfig {
            listen: "127.0.0.1:0".parse().expect("listen"),
            ws_path: "/ws".to_owned(),
            health_path: "/healthz".to_owned(),
            heartbeat_interval: Duration::from_secs(60),
            heartbeat_timeout: Duration::from_secs(180),
            connection_ttl: None,
            max_clients: 16,
            client_buffer_size: 16,
        }
    }

    #[test]
    fn rejects_streams_missing_endpoint() {
        let mut stream = stream(StreamName::Swaps, "solana-mainnet", "");
        stream.substreams.endpoint = None;
        let config = Config {
            streams: vec![stream],
            websocket: websocket(),
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MissingStreamEndpoint { .. })
        ));
    }

    #[test]
    fn rejects_streams_missing_network() {
        let mut stream = stream(StreamName::Transfers, "", "https://e:443");
        stream.substreams.network = None;
        let config = Config {
            streams: vec![stream],
            websocket: websocket(),
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MissingStreamNetwork { .. })
        ));
    }

    #[test]
    fn rejects_streams_missing_start_block() {
        let mut s = stream(StreamName::Swaps, "solana-mainnet", "https://e:443");
        s.substreams.start_block = None;
        let config = Config {
            streams: vec![s],
            websocket: websocket(),
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MissingStreamStartBlock { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_network_name_pairs() {
        let config = Config {
            streams: vec![
                stream(StreamName::Transfers, "solana-mainnet", "https://e:443"),
                stream(StreamName::Transfers, "solana-mainnet", "https://e:443"),
            ],
            websocket: websocket(),
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::DuplicateStream { .. })
        ));
    }

    #[test]
    fn allows_same_name_on_different_networks() {
        let config = Config {
            streams: vec![
                stream(StreamName::Transfers, "solana-mainnet", "https://a:443"),
                stream(StreamName::Transfers, "ethereum-mainnet", "https://b:443"),
            ],
            websocket: websocket(),
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
        };
        config.validate().expect("distinct networks are allowed");
    }
}
