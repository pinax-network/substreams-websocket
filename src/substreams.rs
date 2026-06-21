use std::{collections::HashMap, path::Path};

use http::uri::InvalidUri;
use prost::Message;
use tonic::{
    Request,
    metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataValue},
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use tracing::{debug, info};

use crate::SubstreamsConfig;

pub mod pb {
    pub mod sf {
        // Not used directly; the sf.substreams.rpc generated clients reference
        // sf.firehose.v2 types (EndpointInfo service) via relative paths.
        pub mod firehose {
            pub mod v2 {
                tonic::include_proto!("sf.firehose.v2");
            }
        }

        pub mod substreams {
            pub mod v1 {
                tonic::include_proto!("sf.substreams.v1");
            }

            pub mod rpc {
                pub mod v2 {
                    tonic::include_proto!("sf.substreams.rpc.v2");
                }

                pub mod v3 {
                    tonic::include_proto!("sf.substreams.rpc.v3");
                }
            }
        }
    }
}

use pb::sf::substreams::{
    rpc::v2::{Request as BlocksRequest, Response, response, stream_client::StreamClient},
    v1::Package,
};
use tonic::codec::CompressionEncoding;

#[derive(Debug, thiserror::Error)]
pub enum SubstreamsError {
    #[error("Substreams endpoint is required")]
    MissingEndpoint,

    #[error("failed to exchange Substreams API key for JWT at {url}: {error}")]
    AuthRequest {
        url: String,
        #[source]
        error: reqwest::Error,
    },

    #[error("Substreams auth endpoint {url} returned status {status}: {body}")]
    AuthStatus {
        url: String,
        status: http::StatusCode,
        body: String,
    },

    #[error("Substreams auth endpoint {url} returned no token in response: {body}")]
    AuthMissingToken { url: String, body: String },

    #[error("invalid endpoint URI: {0}")]
    InvalidEndpoint(#[from] InvalidUri),

    #[error("invalid metadata header {name:?}: {source}")]
    InvalidMetadataName {
        name: String,
        source: tonic::metadata::errors::InvalidMetadataKey,
    },

    #[error("invalid metadata value for {name:?}: {source}")]
    InvalidMetadataValue {
        name: String,
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    #[error("invalid block number {value:?}: {source}")]
    InvalidBlockNumber {
        value: String,
        source: std::num::ParseIntError,
    },

    #[error("invalid params value {value:?}, expected module=value")]
    InvalidParam { value: String },

    #[error("failed to read manifest {source}: {error}")]
    ReadPackage {
        source: String,
        #[source]
        error: std::io::Error,
    },

    #[error("failed to fetch manifest {source}: {error}")]
    FetchPackage {
        source: String,
        #[source]
        error: reqwest::Error,
    },

    #[error("failed to decompress zstd manifest {source}: {error}")]
    DecompressPackage {
        source: String,
        #[source]
        error: std::io::Error,
    },

    #[error("failed to decode manifest {source}: {error}")]
    DecodePackage {
        source: String,
        #[source]
        error: prost::DecodeError,
    },

    #[error(
        "Substreams manifest does not define module {module:?}; available modules: {available}"
    )]
    MissingModule { module: String, available: String },

    #[error("failed to connect to Substreams endpoint: {0}")]
    Connect(#[from] tonic::transport::Error),

    #[error("Substreams stream error: {0}")]
    Status(#[from] tonic::Status),
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Session {
        trace_id: String,
        resolved_start_block: u64,
        chain_head: u64,
    },
    Progress {
        running_jobs: usize,
        processed_blocks: u64,
    },
    Block {
        number: u64,
        id: String,
        timestamp: String,
        /// Unix epoch seconds of the block timestamp. Machine-friendly
        /// companion to `timestamp`, surfaced for client-side head drift.
        timestamp_seconds: i64,
        output_type_url: String,
        payload: Vec<u8>,
        cursor: String,
    },
    Undo {
        last_valid_block: u64,
        last_valid_cursor: String,
    },
    Fatal {
        message: String,
    },
    SnapshotData {
        module_name: String,
        sent_keys: u64,
        total_keys: u64,
    },
    SnapshotComplete,
    Unknown,
}

pub struct SubstreamsClient {
    config: SubstreamsConfig,
}

impl SubstreamsClient {
    pub fn new(config: SubstreamsConfig) -> Self {
        Self { config }
    }

    pub async fn stream(self) -> Result<SubstreamsStream, SubstreamsError> {
        let package = load_package(&self.config.manifest).await?;
        self.stream_with_package(package).await
    }

    pub async fn stream_with_package(
        mut self,
        package: Package,
    ) -> Result<SubstreamsStream, SubstreamsError> {
        resolve_auth_token(&mut self.config).await?;
        ensure_module_exists(&package, &self.config.module)?;
        let request = build_blocks_request(&self.config, package)?;
        let channel = connect_channel(&self.config).await?;
        // Decoded message cap (default 64 MiB, configurable per stream).
        // Substreams DatabaseChanges payloads for chains with many transactions
        // per block (e.g. Solana SPL transfers) routinely exceed tonic's 4 MiB
        // default after gzip decompression; some chains exceed 64 MiB and raise
        // this via `SUBSTREAMS_MAX_DECODE_MESSAGE_BYTES` / per-stream config.
        let mut client = StreamClient::new(channel)
            .max_decoding_message_size(self.config.max_decode_message_bytes)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);
        let mut request = Request::new(request);
        apply_auth_metadata(request.metadata_mut(), &self.config)?;
        let stream = client.blocks(request).await?.into_inner();

        Ok(SubstreamsStream { stream })
    }
}

pub struct SubstreamsStream {
    stream: tonic::Streaming<Response>,
}

impl SubstreamsStream {
    pub async fn next_event(&mut self) -> Result<Option<StreamEvent>, SubstreamsError> {
        let Some(response) = self.stream.message().await? else {
            return Ok(None);
        };

        Ok(Some(StreamEvent::from(response)))
    }
}

impl From<Response> for StreamEvent {
    fn from(response: Response) -> Self {
        match response.message {
            Some(response::Message::Session(session)) => Self::Session {
                trace_id: session.trace_id,
                resolved_start_block: session.resolved_start_block,
                chain_head: session.chain_head,
            },
            Some(response::Message::Progress(progress)) => Self::Progress {
                running_jobs: progress.running_jobs.len(),
                processed_blocks: progress.processed_blocks,
            },
            Some(response::Message::BlockScopedData(data)) => {
                let cursor = data.cursor.clone();
                let clock = data.clock.unwrap_or_default();
                let timestamp_seconds = clock.timestamp.as_ref().map(|t| t.seconds).unwrap_or(0);
                let timestamp = format_clickhouse_timestamp(clock.timestamp);
                let output = data.output.and_then(|output| output.map_output);
                let output_type_url = output
                    .as_ref()
                    .map(|any| any.type_url.clone())
                    .unwrap_or_default();
                let payload = output.map(|any| any.value).unwrap_or_default();

                Self::Block {
                    number: clock.number,
                    id: clock.id,
                    timestamp,
                    timestamp_seconds,
                    output_type_url,
                    payload,
                    cursor,
                }
            }
            Some(response::Message::BlockUndoSignal(undo)) => Self::Undo {
                last_valid_block: undo.last_valid_block.map(|block| block.number).unwrap_or(0),
                last_valid_cursor: undo.last_valid_cursor,
            },
            Some(response::Message::FatalError(error)) => Self::Fatal {
                message: if error.module.is_empty() {
                    error.reason
                } else {
                    format!("module {}: {}", error.module, error.reason)
                },
            },
            Some(response::Message::DebugSnapshotData(data)) => Self::SnapshotData {
                module_name: data.module_name,
                sent_keys: data.sent_keys,
                total_keys: data.total_keys,
            },
            Some(response::Message::DebugSnapshotComplete(_)) => Self::SnapshotComplete,
            None => Self::Unknown,
        }
    }
}

fn format_clickhouse_timestamp(timestamp: Option<prost_types::Timestamp>) -> String {
    let value = timestamp.unwrap_or_default().to_string();
    let value = value
        .split_once('.')
        .map(|(prefix, _)| prefix)
        .unwrap_or_else(|| value.trim_end_matches('Z'));

    value.replace('T', " ")
}

pub async fn load_package(source: &str) -> Result<Package, SubstreamsError> {
    let bytes = load_package_bytes(source).await?;
    let bytes = maybe_decompress_zstd(source, bytes)?;
    Package::decode(bytes.as_slice()).map_err(|error| SubstreamsError::DecodePackage {
        source: source.to_owned(),
        error,
    })
}

async fn load_package_bytes(source: &str) -> Result<Vec<u8>, SubstreamsError> {
    if source.starts_with("http://") || source.starts_with("https://") {
        let response =
            reqwest::get(source)
                .await
                .map_err(|error| SubstreamsError::FetchPackage {
                    source: source.to_owned(),
                    error,
                })?;
        let response =
            response
                .error_for_status()
                .map_err(|error| SubstreamsError::FetchPackage {
                    source: source.to_owned(),
                    error,
                })?;

        return response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| SubstreamsError::FetchPackage {
                source: source.to_owned(),
                error,
            });
    }

    tokio::fs::read(source)
        .await
        .map_err(|error| SubstreamsError::ReadPackage {
            source: source.to_owned(),
            error,
        })
}

fn maybe_decompress_zstd(source: &str, bytes: Vec<u8>) -> Result<Vec<u8>, SubstreamsError> {
    let is_zstd = Path::new(source)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| matches!(extension, "zst" | "zstd"));

    if !is_zstd {
        return Ok(bytes);
    }

    zstd::decode_all(bytes.as_slice()).map_err(|error| SubstreamsError::DecompressPackage {
        source: source.to_owned(),
        error,
    })
}

fn ensure_module_exists(manifest: &Package, module: &str) -> Result<(), SubstreamsError> {
    let Some(modules) = manifest.modules.as_ref() else {
        return Err(SubstreamsError::MissingModule {
            module: module.to_owned(),
            available: "none".to_owned(),
        });
    };

    if modules.modules.iter().any(|item| item.name == module) {
        return Ok(());
    }

    let available = modules
        .modules
        .iter()
        .map(|module| module.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    Err(SubstreamsError::MissingModule {
        module: module.to_owned(),
        available: if available.is_empty() {
            "none".to_owned()
        } else {
            available
        },
    })
}

pub fn build_blocks_request(
    config: &SubstreamsConfig,
    package: Package,
) -> Result<BlocksRequest, SubstreamsError> {
    let modules = apply_params(package.modules.clone().unwrap_or_default(), &config.params)?;
    Ok(BlocksRequest {
        start_block_num: parse_start_block(config.start_block.as_deref())?,
        start_cursor: config.start_cursor.clone().unwrap_or_default(),
        stop_block_num: parse_stop_block(&config.stop_block)?,
        final_blocks_only: config.final_blocks_only,
        production_mode: config.production_mode,
        output_module: config.module.clone(),
        modules: Some(modules),
        debug_initial_store_snapshot_for_modules: Vec::new(),
        noop_mode: false,
        limit_processed_blocks: 0,
        dev_output_modules: Vec::new(),
        progress_messages_interval_ms: 0,
        partial_blocks: false,
    })
}

/// Apply `module=value` params to module inputs as Substreams parameter values.
fn apply_params(
    mut modules: pb::sf::substreams::v1::Modules,
    raw_params: &[String],
) -> Result<pb::sf::substreams::v1::Modules, SubstreamsError> {
    let params = parse_params(raw_params)?;
    for (module_name, value) in params {
        let Some(module) = modules
            .modules
            .iter_mut()
            .find(|module| module.name == module_name)
        else {
            continue;
        };
        if let Some(first) = module.inputs.first_mut() {
            if let Some(pb::sf::substreams::v1::module::input::Input::Params(p)) =
                first.input.as_mut()
            {
                p.value = value;
            }
        }
    }
    Ok(modules)
}

fn parse_start_block(value: Option<&str>) -> Result<i64, SubstreamsError> {
    value
        .filter(|value| !value.is_empty())
        .unwrap_or("0")
        .parse()
        .map_err(|source| SubstreamsError::InvalidBlockNumber {
            value: value.unwrap_or("0").to_owned(),
            source,
        })
}

fn parse_stop_block(value: &str) -> Result<u64, SubstreamsError> {
    value
        .parse()
        .map_err(|source| SubstreamsError::InvalidBlockNumber {
            value: value.to_owned(),
            source,
        })
}

fn parse_params(values: &[String]) -> Result<HashMap<String, String>, SubstreamsError> {
    let mut params = HashMap::new();
    for value in values {
        let Some((module, param)) = value.split_once('=') else {
            return Err(SubstreamsError::InvalidParam {
                value: value.clone(),
            });
        };

        if module.is_empty() {
            return Err(SubstreamsError::InvalidParam {
                value: value.clone(),
            });
        }

        params.insert(module.to_owned(), param.to_owned());
    }

    Ok(params)
}

/// Initial HTTP/2 per-stream flow-control window (8 MiB). Large enough that a
/// multi-MiB Substreams block transfers in a single window without waiting on
/// WINDOW_UPDATE credit, well clear of hyper's 64 KiB default. The
/// connection-level window grows beyond this via adaptive flow control.
const STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;

async fn connect_channel(config: &SubstreamsConfig) -> Result<Channel, SubstreamsError> {
    let endpoint = config
        .endpoint
        .as_deref()
        .ok_or(SubstreamsError::MissingEndpoint)?;
    let endpoint = normalize_endpoint(endpoint, config.plaintext);
    let uses_tls = endpoint.starts_with("https://");
    let mut endpoint = Endpoint::from_shared(endpoint)?
        // Send HTTP/2 PING frames every 30s even when the stream is idle. Many
        // upstream proxies / load balancers reset long-lived gRPC streams that
        // appear idle (no DATA frames), surfacing as `h2 protocol error:
        // error reading a body from connection: Io(ConnectionReset)`. Keepalive
        // pings keep the path warm.
        .http2_keep_alive_interval(std::time::Duration::from_secs(30))
        .keep_alive_timeout(std::time::Duration::from_secs(20))
        .keep_alive_while_idle(true)
        // TCP-level keepalive as a second line of defense.
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        // Faster than the default to detect dead peers sooner.
        .tcp_nodelay(true)
        // HTTP/2 flow control. hyper defaults the initial window to 64 KiB,
        // which caps single-stream throughput at roughly `window / RTT`
        // (e.g. a 64 KiB window over a 50 ms RTT is only ~1.3 MiB/s). Substreams
        // DatabaseChanges payloads for busy chains routinely run to several MiB
        // per block after decompression, so on a high-RTT link to the endpoint
        // the default window can stall delivery between WINDOW_UPDATE round-trips.
        // Enable adaptive flow control (hyper grows the connection window from a
        // BDP estimate) and raise the per-stream initial window so a single
        // large block transfers without round-tripping for credit. Defensive:
        // when the server is co-located with the endpoint this is a no-op, but
        // it removes a real ceiling for distant or very-large-block streams.
        .initial_stream_window_size(STREAM_WINDOW_BYTES)
        .http2_adaptive_window(true);

    if uses_tls && !config.insecure {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?;
    }

    Ok(endpoint.connect().await?)
}

fn normalize_endpoint(endpoint: &str, plaintext: bool) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else if plaintext {
        format!("http://{endpoint}")
    } else {
        format!("https://{endpoint}")
    }
}

fn apply_auth_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    config: &SubstreamsConfig,
) -> Result<(), SubstreamsError> {
    if let Some(token) = &config.token {
        let value = format!("Bearer {token}")
            .parse::<MetadataValue<_>>()
            .map_err(|source| SubstreamsError::InvalidMetadataValue {
                name: "authorization".to_owned(),
                source,
            })?;
        metadata.insert("authorization", value);
        return Ok(());
    }

    if let Some(api_key) = &config.api_key {
        let header = config
            .api_key_header
            .parse::<AsciiMetadataKey>()
            .map_err(|source| SubstreamsError::InvalidMetadataName {
                name: config.api_key_header.clone(),
                source,
            })?;
        let value = AsciiMetadataValue::try_from(api_key.as_str()).map_err(|source| {
            SubstreamsError::InvalidMetadataValue {
                name: config.api_key_header.clone(),
                source,
            }
        })?;
        metadata.insert(header, value);
    }

    Ok(())
}

pub const DEFAULT_AUTH_URL: &str = "https://auth.pinax.network/v1/auth/issue";

/// If an API key is configured but no bearer token, exchange the API key for a
/// short-lived JWT at the configured auth URL (Pinax/StreamingFast style) and
/// store it on the config so the request uses `Authorization: Bearer <jwt>`.
async fn resolve_auth_token(config: &mut SubstreamsConfig) -> Result<(), SubstreamsError> {
    if config.token.is_some() || config.api_key.is_none() {
        return Ok(());
    }

    let url = config
        .auth_url
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_AUTH_URL.to_owned());

    // Setting SUBSTREAMS_AUTH_URL="none" or "off" disables the JWT exchange and
    // sends the API key directly via the configured api_key_header instead.
    if matches!(url.as_str(), "none" | "off" | "disabled") {
        return Ok(());
    }

    let api_key = config.api_key.clone().expect("api_key checked above");

    let response = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({ "api_key": api_key }))
        .send()
        .await
        .map_err(|error| SubstreamsError::AuthRequest {
            url: url.clone(),
            error,
        })?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(SubstreamsError::AuthStatus { url, status, body });
    }

    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|_| SubstreamsError::AuthMissingToken {
            url: url.clone(),
            body: body.clone(),
        })?;
    let token = parsed
        .get("token")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .ok_or_else(|| SubstreamsError::AuthMissingToken {
            url: url.clone(),
            body: body.clone(),
        })?;

    info!(
        url = %url,
        token_len = token.len(),
        "exchanged Substreams API key for JWT"
    );
    debug!(token = %token, "Substreams JWT issued");
    config.token = Some(token);
    config.api_key = None;
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::substreams::pb::sf::substreams::v1::{
        Module, Modules, Package, module, module::KindMap,
    };

    #[test]
    fn builds_request_from_config() {
        let config = config();
        let request = build_blocks_request(&config, manifest()).expect("request builds");

        assert_eq!(request.start_block_num, -10);
        assert_eq!(request.stop_block_num, 100);
        assert_eq!(request.start_cursor, "");
        assert_eq!(request.output_module, "swaps");
        assert!(request.production_mode);
        assert!(request.final_blocks_only);
        assert!(request.modules.is_some());
    }

    #[test]
    fn rejects_invalid_params() {
        let mut config = config();
        config.params = vec!["bad-param".to_owned()];

        let error = build_blocks_request(&config, manifest()).expect_err("params reject");
        assert!(matches!(error, SubstreamsError::InvalidParam { .. }));
    }

    #[test]
    fn package_round_trip_decodes() {
        let manifest = manifest();
        let mut bytes = Vec::new();
        manifest.encode(&mut bytes).expect("manifest encodes");

        let decoded = Package::decode(bytes.as_slice()).expect("manifest decodes");
        ensure_module_exists(&decoded, "swaps").expect("module exists");
    }

    #[test]
    fn formats_timestamps_for_clickhouse_datetime() {
        let timestamp = prost_types::Timestamp {
            seconds: 1_778_608_800,
            nanos: 123_000_000,
        };

        assert_eq!(
            format_clickhouse_timestamp(Some(timestamp)),
            "2026-05-12 18:00:00"
        );
    }

    fn config() -> SubstreamsConfig {
        SubstreamsConfig {
            manifest: "./demo.spkg".to_owned(),
            module: "swaps".to_owned(),
            endpoint: Some("localhost:9000".to_owned()),
            network: Some("mainnet".to_owned()),
            start_block: Some("-10".to_owned()),
            start_cursor: None,
            stop_block: "100".to_owned(),
            params: vec!["swaps=protocol=raydium".to_owned()],
            plaintext: true,
            insecure: false,
            production_mode: true,
            final_blocks_only: true,
            token: None,
            api_key: None,
            api_key_header: "X-Api-Key".to_owned(),
            auth_url: None,
            max_decode_message_bytes: crate::config::DEFAULT_MAX_DECODE_MESSAGE_BYTES,
        }
    }

    fn manifest() -> Package {
        Package {
            version: 1,
            modules: Some(Modules {
                modules: vec![Module {
                    name: "swaps".to_owned(),
                    kind: Some(module::Kind::KindMap(KindMap {
                        output_type: "sf.substreams.svm.dex.v1.Events".to_owned(),
                    })),
                    ..Default::default()
                }],
                binaries: Vec::new(),
            }),
            ..Default::default()
        }
    }
}
