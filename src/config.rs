use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

/// Default gRPC decoded-message size cap (64 MiB). Substreams DatabaseChanges
/// payloads for high-throughput chains can exceed tonic's 4 MiB default after
/// decompression; chains with very large per-block output (e.g. Hyperliquid
/// hypercore) can exceed even this and must raise it per stream.
pub const DEFAULT_MAX_DECODE_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub streams: Vec<StreamConfig>,
    pub websocket: WebSocketConfig,
    pub cursors_dir: std::path::PathBuf,
    /// Max age (seconds) of a persisted cursor before it is ignored on
    /// startup. A cursor file whose last-modified time is older than this is
    /// treated as stale: the stream starts from the configured `start_block`
    /// (default `-1` = chain head) instead of replaying every block from the
    /// stale position to head. Guards against the catch-up firehose after the
    /// server has been down a while. `0` disables the check (always resume
    /// from the cursor, however old).
    pub cursor_max_age_secs: u64,
    /// Other names clients may use for configured networks, e.g. the Token
    /// API's `mainnet` for `eth`.
    pub network_aliases: Arc<NetworkAliases>,
}

/// Other names clients may use for a configured network, kept while clients
/// move from one naming to another (the Token API's `mainnet` to the Pinax
/// id `eth`). A selector may name either; the server resolves an alias to its
/// network for matching, and each client keeps seeing the name it subscribed
/// with. A network has at most one alias.
#[derive(Debug, Clone, Default)]
pub struct NetworkAliases {
    /// alias -> configured network
    networks: HashMap<String, String>,
    /// configured network -> alias
    aliases: HashMap<String, String>,
    /// The name a client subscribed through a network wildcard sees.
    pub wildcard_names: WildcardNetworkNames,
}

/// Which name a client that subscribed through a network wildcard (`*@swaps`)
/// sees for a network that has an alias. A client that names the network sees
/// the name it used either way.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WildcardNetworkNames {
    /// The configured network id.
    #[default]
    Network,
    /// The alias, so wildcard clients keep the names they saw before the
    /// network was renamed.
    Alias,
}

impl NetworkAliases {
    /// Builds the alias table from `(alias, network)` pairs. Use
    /// [`Config::validate`] to check it against the configured streams.
    pub fn new(
        pairs: impl IntoIterator<Item = (String, String)>,
        wildcard_names: WildcardNetworkNames,
    ) -> Result<Self, ConfigError> {
        let mut table = Self {
            wildcard_names,
            ..Self::default()
        };
        for (alias, network) in pairs {
            let alias = alias.trim().to_owned();
            let network = network.trim().to_owned();
            if !is_plain_network_name(&alias) {
                return Err(ConfigError::InvalidNetworkAlias { alias });
            }
            if alias == network {
                return Err(ConfigError::NetworkAliasLoop { alias });
            }
            if let Some(existing) = table.aliases.get(&network) {
                return Err(ConfigError::DuplicateNetworkAlias {
                    network,
                    aliases: [existing.clone(), alias],
                });
            }
            table.aliases.insert(network.clone(), alias.clone());
            table.networks.insert(alias, network);
        }
        Ok(table)
    }

    /// The configured network `name` stands for: its network when `name` is
    /// an alias, otherwise `name` itself.
    pub fn network<'a>(&'a self, name: &'a str) -> &'a str {
        self.networks.get(name).map_or(name, String::as_str)
    }

    /// The alias of a configured network, if it has one.
    pub fn alias(&self, network: &str) -> Option<&str> {
        self.aliases.get(network).map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.networks.is_empty()
    }

    /// `(alias, network)` pairs, sorted by alias.
    pub fn pairs(&self) -> Vec<(&str, &str)> {
        let mut pairs: Vec<_> = self
            .networks
            .iter()
            .map(|(alias, network)| (alias.as_str(), network.as_str()))
            .collect();
        pairs.sort_unstable();
        pairs
    }
}

/// Whether `name` can stand in a `network@table` selector as a network: not
/// empty, and free of the selector syntax (`@`, `,`, `/`, `*`) and whitespace.
fn is_plain_network_name(name: &str) -> bool {
    !name.is_empty()
        && !name
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '@' | ',' | '/' | '*'))
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
    /// channels without waiting for a block to land.
    ///
    /// Doubles as a per-stream allowlist: when non-empty, only rows whose
    /// `@table` is declared here are broadcast — any other table the spkg
    /// emits is dropped as noise. Optional — empty
    /// means "tables are discovered at runtime from event `@table` fields" and
    /// every emitted table passes through unfiltered.
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
    /// Max decoded size (bytes) for an inbound gRPC `Response` message. Maps to
    /// tonic's `max_decoding_message_size`. Default
    /// [`DEFAULT_MAX_DECODE_MESSAGE_BYTES`]; raise for streams whose per-block
    /// DatabaseChanges output exceeds 64 MiB after decompression.
    pub max_decode_message_bytes: usize,
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
    /// HTTP path that serves Prometheus metrics. Default `/metrics`. Set to
    /// empty to disable the endpoint.
    pub metrics_path: String,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub connection_ttl: Option<Duration>,
    pub max_clients: usize,
    pub client_buffer_size: usize,
    pub shutdown_drain_timeout: Duration,
    pub max_filter_fields: usize,
    pub max_filter_values: usize,
    /// Number of consecutive failed `try_send` calls before a client is
    /// force-closed with `Close(1013 "slow client backpressure")`. Set to
    /// `0` to disable — clients then keep their connection forever and only
    /// individual frames are dropped on a saturated buffer. Default 100.
    pub slow_client_drop_limit: u64,
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

    #[error("network alias {alias:?} must be a plain name (no whitespace, `@`, `,`, `/` or `*`)")]
    InvalidNetworkAlias { alias: String },

    #[error("network alias {alias:?} names itself")]
    NetworkAliasLoop { alias: String },

    #[error("network {network:?} has more than one alias: {aliases:?}")]
    DuplicateNetworkAlias {
        network: String,
        aliases: [String; 2],
    },

    #[error("network alias {alias:?} is also a configured network")]
    NetworkAliasShadowsNetwork { alias: String },

    #[error("network alias {alias:?} points at {network:?}, which no stream is configured for")]
    NetworkAliasUnknownNetwork { alias: String, network: String },
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

        let networks: std::collections::HashSet<&str> = self
            .streams
            .iter()
            .filter_map(|s| s.substreams.network.as_deref().map(str::trim))
            .collect();
        for (alias, network) in self.network_aliases.pairs() {
            if networks.contains(alias) {
                return Err(ConfigError::NetworkAliasShadowsNetwork {
                    alias: alias.to_owned(),
                });
            }
            if !networks.contains(network) {
                return Err(ConfigError::NetworkAliasUnknownNetwork {
                    alias: alias.to_owned(),
                    network: network.to_owned(),
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
                max_decode_message_bytes: DEFAULT_MAX_DECODE_MESSAGE_BYTES,
            },
        }
    }

    fn websocket() -> WebSocketConfig {
        WebSocketConfig {
            listen: "127.0.0.1:0".parse().expect("listen"),
            ws_path: "/ws".to_owned(),
            stream_path: "/stream".to_owned(),
            metrics_path: "/metrics".to_owned(),
            health_path: "/healthz".to_owned(),
            heartbeat_interval: Duration::from_secs(60),
            heartbeat_timeout: Duration::from_secs(180),
            connection_ttl: None,
            max_clients: 16,
            client_buffer_size: 16,
            shutdown_drain_timeout: Duration::from_secs(1),
            max_filter_fields: 16,
            max_filter_values: 64,
            slow_client_drop_limit: 0,
        }
    }

    fn cfg(streams: Vec<StreamConfig>) -> Config {
        Config {
            streams,
            websocket: websocket(),
            cursors_dir: std::path::PathBuf::from("/tmp/cursors-test"),
            cursor_max_age_secs: 0,
            network_aliases: Arc::default(),
        }
    }

    fn aliases(pairs: &[(&str, &str)]) -> Result<NetworkAliases, ConfigError> {
        NetworkAliases::new(
            pairs
                .iter()
                .map(|(a, n)| ((*a).to_owned(), (*n).to_owned())),
            WildcardNetworkNames::Network,
        )
    }

    #[test]
    fn resolves_aliases_to_their_network() {
        let table = aliases(&[("mainnet", "eth"), ("arbitrum-one", "arbone")]).unwrap();
        assert_eq!(table.network("mainnet"), "eth");
        assert_eq!(table.network("eth"), "eth");
        assert_eq!(table.network("base"), "base");
        assert_eq!(table.alias("eth"), Some("mainnet"));
        assert_eq!(table.alias("base"), None);
        assert_eq!(
            table.pairs(),
            vec![("arbitrum-one", "arbone"), ("mainnet", "eth")]
        );
    }

    #[test]
    fn rejects_malformed_aliases() {
        for alias in ["", "main net", "main@net", "a,b", "a/b", "*"] {
            assert!(
                matches!(
                    aliases(&[(alias, "eth")]),
                    Err(ConfigError::InvalidNetworkAlias { .. })
                ),
                "{alias:?} must be rejected"
            );
        }
        assert!(matches!(
            aliases(&[("eth", "eth")]),
            Err(ConfigError::NetworkAliasLoop { .. })
        ));
        assert!(matches!(
            aliases(&[("mainnet", "eth"), ("ethereum", "eth")]),
            Err(ConfigError::DuplicateNetworkAlias { .. })
        ));
    }

    #[test]
    fn validates_aliases_against_configured_networks() {
        let mut config = cfg(vec![
            stream("./evm.spkg", "eth", "https://a:443"),
            stream("./evm.spkg", "base", "https://b:443"),
        ]);
        config.network_aliases = Arc::new(aliases(&[("mainnet", "eth")]).unwrap());
        config.validate().expect("an alias of a configured network");

        config.network_aliases = Arc::new(aliases(&[("base", "eth")]).unwrap());
        assert!(matches!(
            config.validate(),
            Err(ConfigError::NetworkAliasShadowsNetwork { .. })
        ));

        config.network_aliases = Arc::new(aliases(&[("arbitrum-one", "arbone")]).unwrap());
        assert!(matches!(
            config.validate(),
            Err(ConfigError::NetworkAliasUnknownNetwork { .. })
        ));
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
