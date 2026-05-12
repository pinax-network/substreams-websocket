use std::{net::SocketAddr, time::Duration};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use substreams_websocket::{Config, SubstreamsConfig, WebSocketConfig};
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
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Local path or URL to a Substreams .spkg package.
    package: String,

    /// Output module to stream.
    module: String,

    #[arg(short, long, env = "SUBSTREAMS_ENDPOINT")]
    endpoint: Option<String>,

    #[arg(short = 's', long, env = "SUBSTREAMS_START_BLOCK")]
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

    #[arg(long, env = "SUBSTREAMS_TOKEN", hide_env_values = true)]
    token: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_KEY_HEADER", default_value = "X-Api-Key")]
    api_key_header: String,

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
    }

    Ok(())
}

impl ServeArgs {
    fn into_config(self) -> Config {
        Config {
            substreams: SubstreamsConfig {
                package: self.package,
                module: self.module,
                endpoint: self.endpoint,
                start_block: self.start_block,
                stop_block: self.stop_block,
                cursor: self.cursor,
                params: self.params,
                plaintext: self.plaintext,
                insecure: self.insecure,
                token: self.token,
                api_key: self.api_key,
                api_key_header: self.api_key_header,
            },
            websocket: WebSocketConfig {
                listen: self.listen,
                ws_path: self.ws_path,
                health_path: self.health_path,
                heartbeat_interval: Duration::from_secs(self.heartbeat_interval_secs),
                heartbeat_timeout: Duration::from_secs(self.heartbeat_timeout_secs),
                connection_ttl: self.connection_ttl_secs.map(Duration::from_secs),
                max_clients: self.max_clients,
                client_buffer_size: self.client_buffer_size,
            },
        }
    }
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level)
        .with_context(|| format!("invalid log level {log_level:?}"))?;

    fmt().with_env_filter(filter).init();
    Ok(())
}
