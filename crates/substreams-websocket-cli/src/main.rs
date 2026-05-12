use std::{net::SocketAddr, time::Duration};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use substreams_websocket::{
    Config, StreamEvent, SubstreamsClient, SubstreamsConfig, WebSocketConfig,
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
    #[command(flatten)]
    substreams: SubstreamsArgs,

    #[command(flatten)]
    websocket: WebSocketArgs,
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
    /// Local path or URL to a Substreams .spkg package.
    package: String,

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

    #[arg(short, long, env = "SUBSTREAMS_CURSOR")]
    cursor: Option<String>,

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
            let config = args.into_config();
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
    fn into_config(self) -> Config {
        Config {
            substreams: self.substreams.into_config(),
            websocket: WebSocketConfig {
                listen: self.websocket.listen,
                ws_path: self.websocket.ws_path,
                health_path: self.websocket.health_path,
                heartbeat_interval: Duration::from_secs(self.websocket.heartbeat_interval_secs),
                heartbeat_timeout: Duration::from_secs(self.websocket.heartbeat_timeout_secs),
                connection_ttl: self.websocket.connection_ttl_secs.map(Duration::from_secs),
                max_clients: self.websocket.max_clients,
                client_buffer_size: self.websocket.client_buffer_size,
            },
        }
    }
}

impl SubstreamsArgs {
    fn into_config(self) -> SubstreamsConfig {
        SubstreamsConfig {
            package: self.package,
            module: self.module,
            endpoint: self.endpoint,
            network: self.network,
            start_block: self.start_block,
            stop_block: self.stop_block,
            cursor: self.cursor,
            params: self.params,
            plaintext: self.plaintext,
            insecure: self.insecure,
            production_mode: self.production_mode,
            final_blocks_only: self.final_blocks_only,
            token: self.token,
            api_key: self.api_key,
            api_key_header: self.api_key_header,
        }
    }
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
            cursor,
            final_block_height,
            output_type_url,
            payload,
        } => format!(
            "block number={number} id={id} final_block_height={final_block_height} output_type_url={output_type_url} payload_len={} cursor={cursor}",
            payload.len()
        ),
        StreamEvent::Undo {
            last_valid_block,
            last_valid_cursor,
        } => format!("undo last_valid_block={last_valid_block} cursor={last_valid_cursor}"),
        StreamEvent::Fatal { message } => format!("fatal message={message}"),
        StreamEvent::SnapshotData {
            module_name,
            sent_keys,
            total_keys,
        } => format!(
            "snapshot_data module={module_name} sent_keys={sent_keys} total_keys={total_keys}"
        ),
        StreamEvent::SnapshotComplete { cursor } => format!("snapshot_complete cursor={cursor}"),
        StreamEvent::Unknown => "unknown".to_owned(),
    }
}
