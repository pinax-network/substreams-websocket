use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use substreams_websocket::{
    Config, StreamConfig, StreamEvent, StreamName, SubstreamsClient, SubstreamsConfig,
    WebSocketConfig,
};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "substreams-websocket")]
#[command(about = "Stream Substreams module output to WebSocket clients")]
struct Cli {
    #[arg(long, env = "SUBSTREAMS_WEBSOCKET_LOG_LEVEL", default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve(ServeArgs),
    Stream(StreamArgs),
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Path to the server TOML config.
    #[arg(short, long, env = "SUBSTREAMS_WEBSOCKET_CONFIG")]
    config: PathBuf,
}

#[derive(Debug, Args)]
struct StreamArgs {
    #[command(flatten)]
    substreams: SubstreamsArgs,

    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_MAX_STREAM_MESSAGES",
        default_value_t = 10
    )]
    max_messages: usize,
}

#[derive(Debug, Args)]
struct SubstreamsArgs {
    /// Local path or URL to a Substreams .spkg manifest.
    manifest: String,

    /// Output module to stream.
    module: String,

    #[arg(short, long, env = "SUBSTREAMS_ENDPOINT")]
    endpoint: Option<String>,

    #[arg(long, env = "SUBSTREAMS_NETWORK")]
    network: Option<String>,

    #[arg(
        short = 's',
        long,
        env = "SUBSTREAMS_START_BLOCK",
        allow_hyphen_values = true
    )]
    start_block: Option<String>,

    #[arg(short = 't', long, env = "SUBSTREAMS_STOP_BLOCK", default_value = "0")]
    stop_block: String,

    #[arg(short = 'p', long = "params", env = "SUBSTREAMS_PARAMS")]
    params: Vec<String>,

    #[arg(long, env = "SUBSTREAMS_PLAINTEXT")]
    plaintext: bool,

    #[arg(long, env = "SUBSTREAMS_INSECURE")]
    insecure: bool,

    #[arg(long, env = "SUBSTREAMS_PRODUCTION_MODE")]
    production_mode: bool,

    #[arg(long, env = "SUBSTREAMS_FINAL_BLOCKS_ONLY")]
    final_blocks_only: bool,

    #[arg(long, env = "SUBSTREAMS_TOKEN", hide_env_values = true)]
    token: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_KEY_HEADER", default_value = "X-Api-Key")]
    api_key_header: String,

    /// Pinax-style auth endpoint that exchanges an API key for a short-lived JWT.
    #[arg(long, env = "SUBSTREAMS_AUTH_URL")]
    auth_url: Option<String>,
}

#[derive(Debug, Args)]
struct WebSocketArgs {
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_LISTEN",
        default_value = "127.0.0.1:8080"
    )]
    listen: SocketAddr,

    #[arg(long, env = "SUBSTREAMS_WEBSOCKET_WS_PATH", default_value = "/ws")]
    ws_path: String,

    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_HEALTH_PATH",
        default_value = "/healthz"
    )]
    health_path: String,

    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_HEARTBEAT_INTERVAL_SECS",
        default_value_t = 180
    )]
    heartbeat_interval_secs: u64,

    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_HEARTBEAT_TIMEOUT_SECS",
        default_value_t = 600
    )]
    heartbeat_timeout_secs: u64,

    #[arg(long, env = "SUBSTREAMS_WEBSOCKET_CONNECTION_TTL_SECS")]
    connection_ttl_secs: Option<u64>,

    #[arg(long, env = "SUBSTREAMS_WEBSOCKET_MAX_CLIENTS", default_value_t = 1024)]
    max_clients: usize,

    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_CLIENT_BUFFER_SIZE",
        default_value_t = 1024
    )]
    client_buffer_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();

    init_tracing(&cli.log_level)?;

    match cli.command {
        Command::Serve(args) => {
            let config = args.load_config().await?;
            config.validate()?;
            substreams_websocket::serve(config).await?;
        }
        Command::Stream(args) => {
            let max_messages = args.max_messages;
            let client = SubstreamsClient::new(args.substreams.into_config());
            let mut stream = client.stream().await?;
            let mut seen = 0usize;

            while seen < max_messages {
                let Some(event) = stream.next_event().await? else {
                    break;
                };
                seen += 1;
                println!("{}", format_stream_event(event));
            }
        }
    }

    Ok(())
}

impl ServeArgs {
    async fn load_config(self) -> anyhow::Result<Config> {
        let contents = tokio::fs::read_to_string(&self.config)
            .await
            .with_context(|| format!("failed to read config {}", self.config.display()))?;
        let config = toml::from_str::<FileConfig>(&contents)
            .with_context(|| format!("failed to parse config {}", self.config.display()))?;

        Ok(config.into_config())
    }
}

impl SubstreamsArgs {
    fn into_config(self) -> SubstreamsConfig {
        SubstreamsConfig {
            manifest: self.manifest,
            module: self.module,
            endpoint: self.endpoint,
            network: self.network,
            start_block: self.start_block,
            stop_block: self.stop_block,
            params: self.params,
            plaintext: self.plaintext,
            insecure: self.insecure,
            production_mode: self.production_mode,
            final_blocks_only: self.final_blocks_only,
            token: self.token,
            api_key: self.api_key,
            api_key_header: self.api_key_header,
            auth_url: self.auth_url,
        }
    }
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    websocket: FileWebSocketConfig,
    substreams: FileSubstreamsDefaults,
    streams: Vec<FileStreamConfig>,
}

#[derive(Debug, Deserialize)]
struct FileWebSocketConfig {
    #[serde(default = "default_listen")]
    listen: SocketAddr,
    #[serde(default = "default_ws_path")]
    ws_path: String,
    #[serde(default = "default_health_path")]
    health_path: String,
    #[serde(default = "default_heartbeat_interval_secs")]
    heartbeat_interval_secs: u64,
    #[serde(default = "default_heartbeat_timeout_secs")]
    heartbeat_timeout_secs: u64,
    connection_ttl_secs: Option<u64>,
    #[serde(default = "default_max_clients")]
    max_clients: usize,
    #[serde(default = "default_client_buffer_size")]
    client_buffer_size: usize,
}

#[derive(Debug, Deserialize)]
struct FileSubstreamsDefaults {
    endpoint: Option<String>,
    network: Option<String>,
    start_block: Option<String>,
    #[serde(default = "default_stop_block")]
    stop_block: String,
    #[serde(default)]
    params: Vec<String>,
    #[serde(default)]
    plaintext: bool,
    #[serde(default)]
    insecure: bool,
    #[serde(default)]
    production_mode: bool,
    #[serde(default)]
    final_blocks_only: bool,
    token: Option<String>,
    api_key: Option<String>,
    #[serde(default = "default_api_key_header")]
    api_key_header: String,
    auth_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FileStreamConfig {
    name: StreamName,
    manifest: String,
    module: String,
    endpoint: Option<String>,
    network: Option<String>,
    start_block: Option<String>,
    stop_block: Option<String>,
    #[serde(default)]
    params: Vec<String>,
    plaintext: Option<bool>,
    insecure: Option<bool>,
    production_mode: Option<bool>,
    final_blocks_only: Option<bool>,
    token: Option<String>,
    api_key: Option<String>,
    api_key_header: Option<String>,
    auth_url: Option<String>,
}

impl FileConfig {
    fn into_config(mut self) -> Config {
        self.substreams.apply_env_overrides();
        Config {
            streams: self
                .streams
                .into_iter()
                .map(|stream| stream.into_config(&self.substreams))
                .collect(),
            websocket: self.websocket.into_config(),
        }
    }
}

impl FileSubstreamsDefaults {
    /// Backfill secrets and connection settings from environment variables when
    /// the TOML file leaves them unset. Lets operators keep secrets in `.env`.
    fn apply_env_overrides(&mut self) {
        self.endpoint = env_or(self.endpoint.take(), "SUBSTREAMS_ENDPOINT");
        self.network = env_or(self.network.take(), "SUBSTREAMS_NETWORK");
        self.start_block = env_or(self.start_block.take(), "SUBSTREAMS_START_BLOCK");
        if let Some(value) = std::env::var("SUBSTREAMS_STOP_BLOCK")
            .ok()
            .filter(|v| !v.is_empty())
        {
            self.stop_block = value;
        }
        self.token = env_or(self.token.take(), "SUBSTREAMS_TOKEN");
        self.api_key = env_or(self.api_key.take(), "SUBSTREAMS_API_KEY");
        self.auth_url = env_or(self.auth_url.take(), "SUBSTREAMS_AUTH_URL");
    }
}

fn env_or(current: Option<String>, key: &str) -> Option<String> {
    current.or_else(|| std::env::var(key).ok().filter(|value| !value.is_empty()))
}

impl FileWebSocketConfig {
    fn into_config(self) -> WebSocketConfig {
        WebSocketConfig {
            listen: self.listen,
            ws_path: self.ws_path,
            health_path: self.health_path,
            heartbeat_interval: Duration::from_secs(self.heartbeat_interval_secs),
            heartbeat_timeout: Duration::from_secs(self.heartbeat_timeout_secs),
            connection_ttl: self.connection_ttl_secs.map(Duration::from_secs),
            max_clients: self.max_clients,
            client_buffer_size: self.client_buffer_size,
        }
    }
}

impl FileStreamConfig {
    fn into_config(self, defaults: &FileSubstreamsDefaults) -> StreamConfig {
        StreamConfig {
            name: self.name,
            substreams: SubstreamsConfig {
                manifest: self.manifest,
                module: self.module,
                endpoint: self.endpoint.or_else(|| defaults.endpoint.clone()),
                network: self.network.or_else(|| defaults.network.clone()),
                start_block: self.start_block.or_else(|| defaults.start_block.clone()),
                stop_block: self
                    .stop_block
                    .unwrap_or_else(|| defaults.stop_block.clone()),
                params: if self.params.is_empty() {
                    defaults.params.clone()
                } else {
                    self.params
                },
                plaintext: self.plaintext.unwrap_or(defaults.plaintext),
                insecure: self.insecure.unwrap_or(defaults.insecure),
                production_mode: self.production_mode.unwrap_or(defaults.production_mode),
                final_blocks_only: self.final_blocks_only.unwrap_or(defaults.final_blocks_only),
                token: self.token.or_else(|| defaults.token.clone()),
                api_key: self.api_key.or_else(|| defaults.api_key.clone()),
                api_key_header: self
                    .api_key_header
                    .unwrap_or_else(|| defaults.api_key_header.clone()),
                auth_url: self.auth_url.or_else(|| defaults.auth_url.clone()),
            },
        }
    }
}

fn default_listen() -> SocketAddr {
    "127.0.0.1:8080".parse().expect("default listen parses")
}

fn default_ws_path() -> String {
    "/ws".to_owned()
}

fn default_health_path() -> String {
    "/healthz".to_owned()
}

fn default_heartbeat_interval_secs() -> u64 {
    180
}

fn default_heartbeat_timeout_secs() -> u64 {
    600
}

fn default_max_clients() -> usize {
    1024
}

fn default_client_buffer_size() -> usize {
    1024
}

fn default_stop_block() -> String {
    "0".to_owned()
}

fn default_api_key_header() -> String {
    "X-Api-Key".to_owned()
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level)
        .with_context(|| format!("invalid log level {log_level:?}"))?;

    fmt().with_env_filter(filter).init();
    Ok(())
}

fn format_stream_event(event: StreamEvent) -> String {
    match event {
        StreamEvent::Session {
            trace_id,
            resolved_start_block,
            chain_head,
        } => format!(
            "session trace_id={trace_id} resolved_start_block={resolved_start_block} chain_head={chain_head}"
        ),
        StreamEvent::Progress { modules } => format!("progress modules={modules}"),
        StreamEvent::Block {
            number,
            id,
            timestamp,
            output_type_url,
            payload,
        } => format!(
            "block block_num={number} block_hash={id} timestamp={timestamp} output_type_url={output_type_url} payload_len={}",
            payload.len()
        ),
        StreamEvent::Undo { last_valid_block } => {
            format!("undo last_valid_block={last_valid_block}")
        }
        StreamEvent::Fatal { message } => format!("fatal message={message}"),
        StreamEvent::SnapshotData {
            module_name,
            sent_keys,
            total_keys,
        } => format!(
            "snapshot_data module={module_name} sent_keys={sent_keys} total_keys={total_keys}"
        ),
        StreamEvent::SnapshotComplete => "snapshot_complete".to_owned(),
        StreamEvent::Unknown => "unknown".to_owned(),
    }
}
