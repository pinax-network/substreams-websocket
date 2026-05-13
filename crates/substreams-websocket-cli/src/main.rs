use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use substreams_websocket::{
    Config, StreamConfig, StreamEvent, SubstreamsClient, SubstreamsConfig, WebSocketConfig,
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
    /// Path to the streams TOML file. The file contains only the
    /// `[[streams]]` array — single-value settings come from env/flags.
    #[arg(
        short,
        long,
        env = "SUBSTREAMS_WEBSOCKET_STREAMS",
        default_value = "./streams.toml"
    )]
    streams: PathBuf,

    #[command(flatten)]
    websocket: WebSocketArgs,

    #[command(flatten)]
    substreams_defaults: SubstreamsServeDefaults,

    /// Directory where per-stream cursor files are persisted.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_CURSORS_DIR",
        default_value = "./cursors"
    )]
    cursors_dir: PathBuf,
}

#[derive(Debug, Args, Clone)]
struct SubstreamsServeDefaults {
    #[arg(long, env = "SUBSTREAMS_PRODUCTION_MODE")]
    production_mode: bool,

    #[arg(long, env = "SUBSTREAMS_FINAL_BLOCKS_ONLY")]
    final_blocks_only: bool,

    #[arg(long, env = "SUBSTREAMS_PLAINTEXT")]
    plaintext: bool,

    #[arg(long, env = "SUBSTREAMS_INSECURE")]
    insecure: bool,

    #[arg(long, env = "SUBSTREAMS_TOKEN", hide_env_values = true)]
    token: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_KEY_HEADER", default_value = "X-Api-Key")]
    api_key_header: String,

    #[arg(long, env = "SUBSTREAMS_AUTH_URL")]
    auth_url: Option<String>,
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

impl WebSocketArgs {
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
        let contents = tokio::fs::read_to_string(&self.streams)
            .await
            .with_context(|| format!("failed to read streams file {}", self.streams.display()))?;
        let file = toml::from_str::<FileConfig>(&contents)
            .with_context(|| format!("failed to parse streams file {}", self.streams.display()))?;

        Ok(Config {
            streams: file
                .streams
                .into_iter()
                .map(|stream| stream.into_config(&self.substreams_defaults))
                .collect(),
            websocket: self.websocket.into_config(),
            cursors_dir: self.cursors_dir,
        })
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
            start_cursor: None,
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
    streams: Vec<FileStreamConfig>,
}

#[derive(Debug, Deserialize)]
struct FileStreamConfig {
    name: String,
    network: String,
    endpoint: String,
    manifest: String,
    #[serde(default = "default_module")]
    module: String,
    start_block: Option<String>,
    stop_block: Option<String>,
    #[serde(default)]
    params: Vec<String>,
    token: Option<String>,
    api_key: Option<String>,
    api_key_header: Option<String>,
    auth_url: Option<String>,
}

impl FileStreamConfig {
    fn into_config(self, defaults: &SubstreamsServeDefaults) -> StreamConfig {
        StreamConfig {
            name: self.name,
            substreams: SubstreamsConfig {
                manifest: self.manifest,
                module: self.module,
                endpoint: Some(self.endpoint),
                network: Some(self.network),
                start_block: Some(self.start_block.unwrap_or_else(|| "-1".to_owned())),
                start_cursor: None,
                stop_block: self.stop_block.unwrap_or_else(|| "0".to_owned()),
                params: self.params,
                plaintext: defaults.plaintext,
                insecure: defaults.insecure,
                production_mode: defaults.production_mode,
                final_blocks_only: defaults.final_blocks_only,
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

fn default_module() -> String {
    "db_out".to_owned()
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
            cursor,
        } => format!(
            "block block_num={number} block_hash={id} timestamp={timestamp} output_type_url={output_type_url} payload_len={} cursor_len={}",
            payload.len(),
            cursor.len()
        ),
        StreamEvent::Undo {
            last_valid_block,
            last_valid_cursor,
        } => {
            format!(
                "undo last_valid_block={last_valid_block} cursor_len={}",
                last_valid_cursor.len()
            )
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
