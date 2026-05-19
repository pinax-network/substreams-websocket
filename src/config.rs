use std::{net::SocketAddr, time::Duration};

#[derive(Debug, Clone)]
pub struct Config {
    pub streams: Vec<StreamConfig>,
    pub websocket: WebSocketConfig,
    pub cursors_dir: std::path::PathBuf,
    pub replay: ReplayConfig,
}

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Number of recent blocks retained per spkg as JSONL on disk.
    /// `0` disables the replay log entirely.
    pub max_blocks: usize,
    pub dir: std::path::PathBuf,
}

/// One Substreams source the server reads from. Identity is derived from the
/// loaded `.spkg` (`package_name`, `package_version`, `module_hash`) — there
/// is no operator-supplied name. Subscribers identify streams by their event
/// `@table`, not by anything in this struct.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub substreams: SubstreamsConfig,
    /// Operator-declared list of DatabaseChanges tables this spkg is expected
    /// to emit (`swaps`, `transfers`, ...). Surfaced in the WebSocket welcome
    /// message so subscribers can discover available `<network>@<table>`
    /// channels without waiting for a block to land. Optional — empty means
    /// "tables are discovered at runtime from event `@table` fields".
    pub tables: Vec<String>,
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
    /// Query-mode WebSocket route. Default `/stream`. Accepts
    /// `?streams=<network@table>/<...>` and always wraps payloads in
    /// `{"stream":"...","data":...}`.
    pub stream_path: String,
    pub health_path: String,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub connection_ttl: Option<Duration>,
    pub max_clients: usize,
    pub client_buffer_size: usize,
    pub shutdown_drain_timeout: Duration,
    pub max_filter_fields: usize,
    pub max_filter_values: usize,
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

    #[error("stream {index} (manifest={manifest}) is missing a Substreams endpoint")]
    MissingStreamEndpoint { index: usize, manifest: String },

    #[error("stream {index} (manifest={manifest}) is missing a network")]
    MissingStreamNetwork { index: usize, manifest: String },

    #[error(
        "duplicate stream registration: network={network:?} manifest={manifest:?} module={module:?}"
    )]
    DuplicateStream {
        network: String,
        manifest: String,
        module: String,
    },

    #[error("stream {index} (manifest={manifest}) is missing a start_block")]
    MissingStreamStartBlock { index: usize, manifest: String },
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.streams.is_empty() {
            return Err(ConfigError::NoStreams);
        }

        let mut seen = std::collections::HashSet::<(String, String, String)>::new();
        for (index, stream) in self.streams.iter().enumerate() {
            let manifest = stream.substreams.manifest.as_str();
            let endpoint = stream
                .substreams
                .endpoint
                .as_deref()
                .map(str::trim)
                .unwrap_or("");
            if endpoint.is_empty() {
                return Err(ConfigError::MissingStreamEndpoint {
                    index,
                    manifest: manifest.to_owned(),
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
                    manifest: manifest.to_owned(),
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
                    manifest: manifest.to_owned(),
                });
            }

            let module = stream.substreams.module.clone();
            if !seen.insert((network.to_owned(), manifest.to_owned(), module.clone())) {
                return Err(ConfigError::DuplicateStream {
                    network: network.to_owned(),
                    manifest: manifest.to_owned(),
                    module,
                });
            }
        }

        validate_path("ws_path", &self.websocket.ws_path)?;
        validate_path("stream_path", &self.websocket.stream_path)?;
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

    fn stream(manifest: &str, network: &str, endpoint: &str) -> StreamConfig {
        StreamConfig {
            tables: Vec::new(),
            substreams: SubstreamsConfig {
                manifest: manifest.to_owned(),
                module: "db_out".to_owned(),
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
            stream_path: "/stream".to_owned(),
            health_path: "/healthz".to_owned(),
            heartbeat_interval: Duration::from_secs(60),
            heartbeat_timeout: Duration::from_secs(180),
            connection_ttl: None,
            max_clients: 16,
            client_buffer_size: 16,
            shutdown_drain_timeout: Duration::from_secs(1),
            max_filter_fields: 16,
            max_filter_values: 64,
        }
    }

    fn cfg(streams: Vec<StreamConfig>) -> Config {
        Config {
            streams,
            websocket: websocket(),
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
            replay: ReplayConfig {
                max_blocks: 0,
                dir: std::path::PathBuf::from("/tmp/replay-test"),
            },
        }
    }

    #[test]
    fn rejects_streams_missing_endpoint() {
        let mut s = stream("./svm-dex.spkg", "solana-mainnet", "");
        s.substreams.endpoint = None;
        assert!(matches!(
            cfg(vec![s]).validate(),
            Err(ConfigError::MissingStreamEndpoint { .. })
        ));
    }

    #[test]
    fn rejects_streams_missing_network() {
        let mut s = stream("./svm-transfers.spkg", "", "https://e:443");
        s.substreams.network = None;
        assert!(matches!(
            cfg(vec![s]).validate(),
            Err(ConfigError::MissingStreamNetwork { .. })
        ));
    }

    #[test]
    fn rejects_streams_missing_start_block() {
        let mut s = stream("./svm-dex.spkg", "solana-mainnet", "https://e:443");
        s.substreams.start_block = None;
        assert!(matches!(
            cfg(vec![s]).validate(),
            Err(ConfigError::MissingStreamStartBlock { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_network_manifest_module() {
        let config = cfg(vec![
            stream("./svm-dex.spkg", "solana-mainnet", "https://e:443"),
            stream("./svm-dex.spkg", "solana-mainnet", "https://e:443"),
        ]);
        assert!(matches!(
            config.validate(),
            Err(ConfigError::DuplicateStream { .. })
        ));
    }

    #[test]
    fn allows_same_manifest_on_different_networks() {
        let config = cfg(vec![
            stream("./svm-dex.spkg", "solana-mainnet", "https://a:443"),
            stream("./svm-dex.spkg", "ethereum-mainnet", "https://b:443"),
        ]);
        config.validate().expect("distinct networks are allowed");
    }
}
