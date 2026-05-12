use std::{collections::HashMap, path::Path};

use http::uri::InvalidUri;
use prost::Message;
use tonic::{
    Request,
    metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataValue},
    transport::{Channel, ClientTlsConfig, Endpoint},
};

use crate::SubstreamsConfig;

pub mod pb {
    pub mod sf {
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
    rpc::{
        v2::{Response, response},
        v3::{Request as BlocksRequest, stream_client::StreamClient},
    },
    v1::Package,
};

#[derive(Debug, thiserror::Error)]
pub enum SubstreamsError {
    #[error("Substreams endpoint is required")]
    MissingEndpoint,

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

    #[error("failed to read package {source}: {error}")]
    ReadPackage {
        source: String,
        #[source]
        error: std::io::Error,
    },

    #[error("failed to fetch package {source}: {error}")]
    FetchPackage {
        source: String,
        #[source]
        error: reqwest::Error,
    },

    #[error("failed to decompress zstd package {source}: {error}")]
    DecompressPackage {
        source: String,
        #[source]
        error: std::io::Error,
    },

    #[error("failed to decode package {source}: {error}")]
    DecodePackage {
        source: String,
        #[source]
        error: prost::DecodeError,
    },

    #[error("Substreams package does not define module {module:?}; available modules: {available}")]
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
        modules: usize,
    },
    Block {
        number: u64,
        id: String,
        timestamp: String,
        output_type_url: String,
        payload: Vec<u8>,
    },
    Undo {
        last_valid_block: u64,
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
        let package = load_package(&self.config.package).await?;
        ensure_module_exists(&package, &self.config.module)?;
        let request = build_blocks_request(&self.config, package)?;
        let channel = connect_channel(&self.config).await?;
        let mut client = StreamClient::new(channel);
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
                modules: progress.modules.len(),
            },
            Some(response::Message::BlockScopedData(data)) => {
                let clock = data.clock.unwrap_or_default();
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
                    output_type_url,
                    payload,
                }
            }
            Some(response::Message::BlockUndoSignal(undo)) => Self::Undo {
                last_valid_block: undo.last_valid_block.map(|block| block.number).unwrap_or(0),
            },
            Some(response::Message::FatalError(error)) => Self::Fatal {
                message: error.message,
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

fn ensure_module_exists(package: &Package, module: &str) -> Result<(), SubstreamsError> {
    let Some(modules) = package.modules.as_ref() else {
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
    Ok(BlocksRequest {
        start_block_num: parse_start_block(config.start_block.as_deref())?,
        start_cursor: String::new(),
        stop_block_num: parse_stop_block(&config.stop_block)?,
        final_blocks_only: config.final_blocks_only,
        production_mode: config.production_mode,
        output_module: config.module.clone(),
        package: Some(package),
        params: parse_params(&config.params)?,
        network: config.network.clone().unwrap_or_default(),
        debug_initial_store_snapshot_for_modules: Vec::new(),
        noop_mode: false,
        limit_processed_blocks: 0,
        dev_output_modules: Vec::new(),
        progress_messages_interval_ms: 0,
        partial_blocks: false,
    })
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

async fn connect_channel(config: &SubstreamsConfig) -> Result<Channel, SubstreamsError> {
    let endpoint = config
        .endpoint
        .as_deref()
        .ok_or(SubstreamsError::MissingEndpoint)?;
    let endpoint = normalize_endpoint(endpoint, config.plaintext);
    let uses_tls = endpoint.starts_with("https://");
    let mut endpoint = Endpoint::from_shared(endpoint)?;

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
        let request = build_blocks_request(&config, package()).expect("request builds");

        assert_eq!(request.start_block_num, -10);
        assert_eq!(request.stop_block_num, 100);
        assert_eq!(request.start_cursor, "");
        assert_eq!(request.output_module, "swaps");
        assert_eq!(request.network, "mainnet");
        assert_eq!(
            request.params.get("swaps"),
            Some(&"protocol=raydium".to_owned())
        );
        assert!(request.production_mode);
        assert!(request.final_blocks_only);
        assert!(request.package.is_some());
    }

    #[test]
    fn rejects_invalid_params() {
        let mut config = config();
        config.params = vec!["bad-param".to_owned()];

        let error = build_blocks_request(&config, package()).expect_err("params reject");
        assert!(matches!(error, SubstreamsError::InvalidParam { .. }));
    }

    #[test]
    fn package_round_trip_decodes() {
        let package = package();
        let mut bytes = Vec::new();
        package.encode(&mut bytes).expect("package encodes");

        let decoded = Package::decode(bytes.as_slice()).expect("package decodes");
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
            package: "./demo.spkg".to_owned(),
            module: "swaps".to_owned(),
            endpoint: Some("localhost:9000".to_owned()),
            network: Some("mainnet".to_owned()),
            start_block: Some("-10".to_owned()),
            stop_block: "100".to_owned(),
            params: vec!["swaps=protocol=raydium".to_owned()],
            plaintext: true,
            insecure: false,
            production_mode: true,
            final_blocks_only: true,
            token: None,
            api_key: None,
            api_key_header: "X-Api-Key".to_owned(),
        }
    }

    fn package() -> Package {
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
