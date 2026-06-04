use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use substreams_websocket::{
    Config, StreamConfig, StreamEvent, SubstreamsClient, SubstreamsConfig, WebSocketConfig,
};
use tracing::info;
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
    /// Path to the streams config file (YAML or TOML). The file contains
    /// only the `streams:` / `[[streams]]` array — single-value settings
    /// come from env/flags. Format is detected from the extension
    /// (`.yaml` | `.yml` | `.toml`).
    #[arg(
        short,
        long,
        env = "SUBSTREAMS_WEBSOCKET_STREAMS",
        default_value = "./streams.yaml"
    )]
    streams: PathBuf,

    /// Inline streams YAML content. Wins over `--streams` when set.
    /// Useful for env-only PaaS deploys (Railway, Fly, Heroku).
    #[arg(long, env = "SUBSTREAMS_WEBSOCKET_STREAMS_YAML")]
    streams_yaml: Option<String>,

    /// Inline streams TOML content. Wins over `--streams` when set. Same
    /// shape as the YAML form, just in TOML. If both `STREAMS_YAML` and
    /// `STREAMS_TOML` are set, YAML takes precedence.
    #[arg(long, env = "SUBSTREAMS_WEBSOCKET_STREAMS_TOML")]
    streams_toml: Option<String>,

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

    /// Replay log retention window in seconds. The server keeps every block
    /// whose `timestamp_seconds` is within `[newest_seen - N, newest_seen]`
    /// per spkg. Default 600s (10 minutes). `0` disables the replay log.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_REPLAY_SECONDS",
        default_value_t = 600
    )]
    replay_seconds: u64,

    /// Directory where per-stream JSONL replay logs are persisted.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_REPLAY_DIR",
        default_value = "./replay"
    )]
    replay_dir: PathBuf,

    /// Max age in seconds of a persisted cursor before it is ignored on
    /// startup. If a cursor file is older than this, the stream starts from
    /// its configured `start_block` (default `-1` = chain head) instead of
    /// replaying every block from the stale cursor to head. Default 600s
    /// (10 minutes). `0` disables the check (always resume from the cursor).
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_CURSOR_MAX_AGE_SECS",
        default_value_t = 600
    )]
    cursor_max_age_secs: u64,
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
        env = "SUBSTREAMS_WEBSOCKET_STREAM_PATH",
        default_value = "/stream"
    )]
    stream_path: String,

    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_HEALTH_PATH",
        default_value = "/healthz"
    )]
    health_path: String,

    /// HTTP path that serves Prometheus metrics. Empty disables the endpoint.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_METRICS_PATH",
        default_value = "/metrics"
    )]
    metrics_path: String,

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

    /// On SIGTERM/SIGINT, send a `Close` frame to every connected client and
    /// wait up to this long for them to disconnect before exiting.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_SHUTDOWN_DRAIN_SECS",
        default_value_t = 10
    )]
    shutdown_drain_secs: u64,

    /// Maximum keys allowed in a single client-supplied event filter.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_MAX_FILTER_FIELDS",
        default_value_t = 16
    )]
    max_filter_fields: usize,

    /// Maximum total string values across one event filter.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_MAX_FILTER_VALUES",
        default_value_t = 64
    )]
    max_filter_values: usize,

    /// Force-close a WebSocket client after this many consecutive
    /// `try_send` drops on a saturated outbound buffer. `0` disables —
    /// frames are still dropped per-send but the connection is never
    /// killed for backpressure. Default 100.
    #[arg(
        long,
        env = "SUBSTREAMS_WEBSOCKET_SLOW_CLIENT_DROP_LIMIT",
        default_value_t = 100
    )]
    slow_client_drop_limit: u64,
}

impl WebSocketArgs {
    fn into_config(self) -> WebSocketConfig {
        WebSocketConfig {
            listen: self.listen,
            ws_path: self.ws_path,
            stream_path: self.stream_path,
            metrics_path: self.metrics_path,
            health_path: self.health_path,
            heartbeat_interval: Duration::from_secs(self.heartbeat_interval_secs),
            heartbeat_timeout: Duration::from_secs(self.heartbeat_timeout_secs),
            connection_ttl: self.connection_ttl_secs.map(Duration::from_secs),
            max_clients: self.max_clients,
            client_buffer_size: self.client_buffer_size,
            shutdown_drain_timeout: Duration::from_secs(self.shutdown_drain_secs),
            max_filter_fields: self.max_filter_fields,
            max_filter_values: self.max_filter_values,
            slow_client_drop_limit: self.slow_client_drop_limit,
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
            log_serve_args(&args);
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

/// Wire format of an inline / on-disk streams config.
#[derive(Debug, Clone, Copy)]
enum StreamsFormat {
    Toml,
    Yaml,
}

impl StreamsFormat {
    fn from_path(path: &std::path::Path) -> Self {
        match path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("toml") => Self::Toml,
            _ => Self::Yaml,
        }
    }

    fn parse(self, raw: &str) -> anyhow::Result<FileConfig> {
        match self {
            Self::Toml => toml::from_str::<FileConfig>(raw).map_err(|e| anyhow::anyhow!(e)),
            Self::Yaml => serde_yaml::from_str::<FileConfig>(raw).map_err(|e| anyhow::anyhow!(e)),
        }
    }
}

impl ServeArgs {
    async fn load_config(self) -> anyhow::Result<Config> {
        // Inline content wins over the file path. YAML is checked first
        // (preferred format), then TOML. Both are trimmed first so an
        // empty env var is treated as unset (lets operators keep both vars
        // declared without one tripping the other on PaaS).
        let inline_yaml = self
            .streams_yaml
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let inline_toml = self
            .streams_toml
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let (contents, source_label, format) = if let Some(inline) = inline_yaml {
            (
                inline.to_owned(),
                "SUBSTREAMS_WEBSOCKET_STREAMS_YAML".to_owned(),
                StreamsFormat::Yaml,
            )
        } else if let Some(inline) = inline_toml {
            (
                inline.to_owned(),
                "SUBSTREAMS_WEBSOCKET_STREAMS_TOML".to_owned(),
                StreamsFormat::Toml,
            )
        } else {
            let path = self.streams.clone();
            let format = StreamsFormat::from_path(&path);
            let contents = tokio::fs::read_to_string(&path).await.with_context(|| {
                format!(
                    "failed to read streams from {}. Set SUBSTREAMS_WEBSOCKET_STREAMS_YAML \
                     or SUBSTREAMS_WEBSOCKET_STREAMS_TOML to inject the stream list directly \
                     via env, or point SUBSTREAMS_WEBSOCKET_STREAMS at a readable file",
                    path.display()
                )
            })?;
            (contents, path.display().to_string(), format)
        };

        let file = format
            .parse(&contents)
            .with_context(|| format!("failed to parse streams from {source_label}"))?;

        Ok(Config {
            streams: file
                .streams
                .into_iter()
                .map(|stream| stream.into_config(&self.substreams_defaults))
                .collect(),
            websocket: self.websocket.into_config(),
            cursors_dir: self.cursors_dir,
            replay: substreams_websocket::config::ReplayConfig {
                max_seconds: self.replay_seconds,
                dir: self.replay_dir,
            },
            cursor_max_age_secs: self.cursor_max_age_secs,
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
    network: String,
    endpoint: String,
    manifest: String,
    #[serde(default = "default_module")]
    module: String,
    start_block: Option<String>,
    stop_block: Option<String>,
    #[serde(default)]
    params: Vec<String>,
    #[serde(default)]
    tables: Vec<String>,
    token: Option<String>,
    api_key: Option<String>,
    api_key_header: Option<String>,
    auth_url: Option<String>,
    production_mode: Option<bool>,
    final_blocks_only: Option<bool>,
}

impl FileStreamConfig {
    fn into_config(self, defaults: &SubstreamsServeDefaults) -> StreamConfig {
        StreamConfig {
            tables: self.tables,
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

fn default_module() -> String {
    "db_out".to_owned()
}

/// Log the top-level `serve` knobs and Substreams defaults that get
/// merged into each stream. `token` / `api_key` are credential-bearing
/// and rendered as a boolean set/unset, never their values.
fn log_serve_args(args: &ServeArgs) {
    let d = &args.substreams_defaults;
    info!(
        streams_file = %args.streams.display(),
        streams_yaml_inline = args.streams_yaml.is_some(),
        streams_toml_inline = args.streams_toml.is_some(),
        production_mode = d.production_mode,
        final_blocks_only = d.final_blocks_only,
        plaintext = d.plaintext,
        insecure = d.insecure,
        token_set = d.token.is_some(),
        api_key_set = d.api_key.is_some(),
        api_key_header = %d.api_key_header,
        auth_url = ?d.auth_url,
        "substreams defaults (applied to streams that don't override)"
    );
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    // Falls back to `info` for an empty/whitespace value so an accidental
    // `SUBSTREAMS_WEBSOCKET_LOG_LEVEL=` doesn't silence the binary.
    let user_part = log_level.trim();
    let user_part = if user_part.is_empty() {
        "info"
    } else {
        user_part
    };

    // The transport-layer crates (h2, hyper, tonic, …) emit per-frame
    // chatter at DEBUG / TRACE. Quiet them — but only when the user's
    // global level is DEBUG or TRACE. If the user asked for `warn` or
    // `error`, adding `h2=info` would actually *raise* h2's level (more
    // specific directive wins), so we skip the caps and respect their
    // global. `--log-level=debug,h2=trace` still works because the user
    // override is appended last in the filter string.
    let wants_verbose = user_part
        .split(',')
        .map(str::trim)
        .any(|piece| matches!(piece.to_ascii_lowercase().as_str(), "debug" | "trace"));

    let combined = if wants_verbose {
        format!(
            "h2=info,hyper=info,hyper_util=info,tower=info,tower_http=info,\
             tonic=info,rustls=info,axum=info,{user_part}"
        )
    } else {
        user_part.to_owned()
    };
    let filter = EnvFilter::try_new(&combined)
        .with_context(|| format!("invalid log filter {log_level:?}"))?;
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
        StreamEvent::Progress {
            running_jobs,
            processed_blocks,
        } => format!("progress running_jobs={running_jobs} processed_blocks={processed_blocks}"),
        StreamEvent::Block {
            number,
            id,
            timestamp,
            timestamp_seconds,
            output_type_url,
            payload,
            cursor,
        } => format!(
            "block block_num={number} block_hash={id} timestamp={timestamp} timestamp_seconds={timestamp_seconds} output_type_url={output_type_url} payload_len={} cursor_len={}",
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

#[cfg(test)]
mod tests {
    use super::{FileConfig, StreamsFormat};

    const TOML: &str = r#"
[[streams]]
network = "solana-mainnet"
endpoint = "https://solana.substreams.pinax.network:443"
manifest = "https://example.com/svm-dex-v0.5.1.spkg"
tables = ["swaps"]
"#;

    const YAML: &str = r#"
streams:
  - network: solana-mainnet
    endpoint: https://solana.substreams.pinax.network:443
    manifest: https://example.com/svm-dex-v0.5.1.spkg
    tables:
      - swaps
"#;

    #[test]
    fn parses_toml() {
        let cfg: FileConfig = StreamsFormat::Toml.parse(TOML).expect("toml");
        assert_eq!(cfg.streams.len(), 1);
        assert_eq!(cfg.streams[0].network, "solana-mainnet");
        assert_eq!(cfg.streams[0].tables, vec!["swaps".to_owned()]);
    }

    #[test]
    fn parses_yaml() {
        let cfg: FileConfig = StreamsFormat::Yaml.parse(YAML).expect("yaml");
        assert_eq!(cfg.streams.len(), 1);
        assert_eq!(cfg.streams[0].network, "solana-mainnet");
        assert_eq!(cfg.streams[0].tables, vec!["swaps".to_owned()]);
    }

    #[test]
    fn format_from_path_picks_yaml_for_yml_and_yaml() {
        use std::path::Path;
        assert!(matches!(
            StreamsFormat::from_path(Path::new("./streams.yaml")),
            StreamsFormat::Yaml
        ));
        assert!(matches!(
            StreamsFormat::from_path(Path::new("./streams.yml")),
            StreamsFormat::Yaml
        ));
        assert!(matches!(
            StreamsFormat::from_path(Path::new("./streams.toml")),
            StreamsFormat::Toml
        ));
        // Unknown / missing extension falls back to YAML.
        assert!(matches!(
            StreamsFormat::from_path(Path::new("./streams")),
            StreamsFormat::Yaml
        ));
    }
}
