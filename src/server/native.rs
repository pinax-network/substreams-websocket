//! Host-owned decoded sources reuse the WebSocket protocol without a Substreams reader.
use super::*;
use std::collections::HashSet;

/// Explicit discovery identity and admission bounds for one host-owned source.
/// The host owns decoding, canonical continuity and a stable module identity.
#[derive(Debug, Clone)]
pub struct NativeSourceConfig {
    pub network: String,
    pub tables: Vec<String>,
    pub module: String,
    pub module_hash: String,
    pub package_name: String,
    pub package_version: String,
    pub description: String,
    /// Maximum encoded JSON size of a complete block before table grouping.
    pub max_block_bytes: usize,
    pub max_events: usize,
}

impl NativeSourceConfig {
    pub(super) fn metadata(&self, aliases: &NetworkAliases) -> StreamMeta {
        StreamMeta {
            network: self.network.clone(),
            network_alias: aliases.alias(&self.network).map(str::to_owned),
            module: self.module.clone(),
            manifest: format!("native:{}", self.module_hash),
            module_hash: self.module_hash.clone(),
            package_name: self.package_name.clone(),
            package_version: self.package_version.clone(),
            description: self.description.clone(),
            tables: self.tables.clone(),
        }
    }

    fn identity(&self) -> StreamIdentity {
        StreamIdentity {
            network: self.network.clone(),
            manifest: format!("native:{}", self.module_hash),
            endpoint: String::new(),
            module_hash: self.module_hash.clone(),
            package_name: self.package_name.clone(),
            package_version: self.package_version.clone(),
            tables: self.tables.clone(),
        }
    }
}

fn plain(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

pub(super) fn validate(config: &Config, sources: &[NativeSourceConfig]) -> Result<(), ServerError> {
    let mut channels = HashSet::new();
    for source in sources {
        if ![
            &source.network,
            &source.module,
            &source.module_hash,
            &source.package_name,
            &source.package_version,
        ]
        .into_iter()
        .all(|s| plain(s))
            || source.description.len() > 4096
            || source.description.chars().any(char::is_control)
            || source.tables.is_empty()
            || source.tables.len() > 256
            || !(1..=256 * 1024 * 1024).contains(&source.max_block_bytes)
            || !(1..=1_000_000).contains(&source.max_events)
        {
            return Err(ServerError::NativeConfig);
        }
        for table in &source.tables {
            if !plain(table)
                || !channels.insert((&source.network, table))
                || config.streams.iter().any(|fallback| {
                    fallback.substreams.network.as_deref().map(str::trim)
                        == Some(source.network.as_str())
                        && (fallback.tables.is_empty() || fallback.tables.contains(table))
                })
            {
                return Err(ServerError::NativeConfig);
            }
        }
    }
    Ok(())
}

impl Config {
    /// Pure validation for an embedded combination of Substreams and native sources.
    /// Native and fallback sources may share a network, but never a channel.
    pub fn validate_with_native(&self, sources: &[NativeSourceConfig]) -> Result<(), ServerError> {
        validate(self, sources)?;
        self.validate_with_native_networks(
            &sources
                .iter()
                .map(|s| s.network.as_str())
                .collect::<Vec<_>>(),
        )?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NativePublishError {
    #[error("native source session is inactive")]
    Inactive,
    #[error("native source publication is invalid or exceeds its bounds")]
    InvalidBlock,
}

/// Single-owner ingress. Calls fan out directly through the existing bounded
/// client queues; no source queue, package, cursor or network connection exists.
/// Calls must follow canonical order. Dropping or ending the session clears its
/// readiness immediately, including when the host task is cancelled.
pub struct NativePublisher {
    source: NativeSourceConfig,
    clients: ClientRegistry,
    health: crate::SourceHealth,
    index: usize,
    read: Option<crate::source_health::ReadActivity>,
}

impl NativePublisher {
    pub(super) fn new(
        source: NativeSourceConfig,
        clients: ClientRegistry,
        health: crate::SourceHealth,
        index: usize,
    ) -> Self {
        Self {
            source,
            clients,
            health,
            index,
            read: None,
        }
    }

    fn active(&self) -> Result<(), NativePublishError> {
        if self.read.is_none() || self.health.snapshot(Duration::ZERO).draining {
            Err(NativePublishError::Inactive)
        } else {
            Ok(())
        }
    }

    /// Start a new live session. It becomes ready only on activity or a valid block.
    pub async fn started(&mut self) -> Result<(), NativePublishError> {
        self.inactive();
        if self.health.snapshot(Duration::ZERO).draining {
            return Err(NativePublishError::Inactive);
        }
        self.read = Some(self.health.start_read(self.index));
        self.clients
            .broadcast_lifecycle(stream_status(
                &self.source.identity(),
                "started",
                String::new(),
            ))
            .await;
        Ok(())
    }

    /// Record a validated source response even when it produced no records.
    pub fn activity(&mut self) -> Result<(), NativePublishError> {
        self.active()?;
        self.read.as_mut().unwrap().observe(&StreamEvent::Progress {
            running_jobs: 0,
            processed_blocks: 0,
        });
        Ok(())
    }

    /// End the current session without emitting a frame. Safe in a Drop guard.
    pub fn inactive(&mut self) {
        self.read = None;
    }

    /// Publish complete decoded rows, including their `@table` key. The context's
    /// network and module hash must match this source. All other row values must
    /// be strings or arrays of strings, matching DatabaseChanges normalization.
    /// The host supplies the existing UTC `YYYY-MM-DD HH:MM:SS` timestamp.
    /// Invalid blocks are rejected atomically and make the session inactive.
    pub async fn publish_block(
        &mut self,
        context: BlockContext,
        events: Vec<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<(), NativePublishError> {
        self.active()?;
        let block = crate::DatabaseChangesBlockMessage {
            network: context.network,
            block_num: context.block_num,
            block_hash: context.block_hash,
            timestamp: context.timestamp,
            timestamp_seconds: context.timestamp_seconds,
            module_hash: context.module_hash,
            events,
        };
        let valid = block.network == self.source.network
            && block.module_hash == self.source.module_hash
            && !block.block_hash.is_empty()
            && block.block_hash.len() <= 256
            && !block.block_hash.chars().any(char::is_control)
            && !block.timestamp.is_empty()
            && block.timestamp.len() <= 64
            && !block.timestamp.chars().any(char::is_control)
            && block.events.len() <= self.source.max_events
            && block.events.iter().all(|row| {
                row.len() > 1
                    && row.len() <= 1024
                    && row
                        .get("@table")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|table| self.source.tables.iter().any(|v| v == table))
                    && row.iter().all(|(key, value)| {
                        (key == "@table" || plain(key))
                            && match value {
                                serde_json::Value::String(_) => true,
                                serde_json::Value::Array(values) => {
                                    values.iter().all(serde_json::Value::is_string)
                                }
                                _ => false,
                            }
                    })
            })
            && within_size(&block, self.source.max_block_bytes);
        if !valid {
            self.inactive();
            return Err(NativePublishError::InvalidBlock);
        }
        self.activity()?;
        update_head_block_gauges(
            &self.source.identity(),
            block.block_num,
            block.timestamp_seconds,
            &block.events,
        );
        for (table, events) in group_events_by_table(&block) {
            self.clients
                .broadcast_block(
                    &self.source.network,
                    &table,
                    build_table_payload(&block, &table, &events),
                )
                .await;
        }
        Ok(())
    }

    pub async fn undo(&mut self, last_valid_block: u64) -> Result<(), NativePublishError> {
        self.active()?;
        self.activity()?;
        self.clients.broadcast_lifecycle(serde_json::json!({
            "type": "stream", "status": "undo", "network": self.source.network,
            "package_name": self.source.package_name, "package_version": self.source.package_version,
            "module_hash": self.source.module_hash, "last_valid_block": last_valid_block,
        })).await;
        Ok(())
    }

    async fn end(&mut self, status: &str) -> Result<(), NativePublishError> {
        self.active()?;
        self.failure(status).await
    }

    // A failed connection or rejected block can leave no active session. The
    // host must still be able to tell clients why availability ended.
    async fn failure(&mut self, status: &str) -> Result<(), NativePublishError> {
        if self.health.snapshot(Duration::ZERO).draining {
            return Err(NativePublishError::Inactive);
        }
        self.inactive();
        self.clients
            .broadcast_lifecycle(stream_status(
                &self.source.identity(),
                status,
                String::new(),
            ))
            .await;
        Ok(())
    }

    pub async fn error(&mut self) -> Result<(), NativePublishError> {
        self.failure("error").await
    }
    pub async fn decode_error(&mut self) -> Result<(), NativePublishError> {
        self.failure("decode_error").await
    }
    pub async fn completed(&mut self) -> Result<(), NativePublishError> {
        self.end("completed").await
    }
}

/// Serialize into a counting sink, stopping at the bound without allocating a
/// second unbounded copy of host-provided rows.
fn within_size(value: &impl serde::Serialize, maximum: usize) -> bool {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("bound exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(maximum), value).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use serde_json::{Value, json};

    fn source() -> NativeSourceConfig {
        NativeSourceConfig {
            network: "eth".into(),
            tables: vec!["transfers".into()],
            module: "transfers".into(),
            module_hash: "0123456789abcdef".into(),
            package_name: "native_transfers".into(),
            package_version: "1".into(),
            description: "Native transfer rows".into(),
            max_block_bytes: 4096,
            max_events: 8,
        }
    }
    fn config() -> Config {
        Config {
            streams: vec![],
            cursors_dir: "unused-native-cursors".into(),
            cursor_max_age_secs: 0,
            network_aliases: Arc::new(
                NetworkAliases::new(
                    [("mainnet".into(), "eth".into())],
                    WildcardNetworkNames::Network,
                )
                .unwrap(),
            ),
            websocket: crate::WebSocketConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                ui_auth_url: None,
                ws_path: "/ws".into(),
                stream_path: "/stream".into(),
                health_path: "/healthz".into(),
                metrics_path: "/metrics".into(),
                heartbeat_interval: Duration::from_secs(180),
                heartbeat_timeout: Duration::from_secs(600),
                connection_ttl: None,
                max_clients: 16,
                client_buffer_size: 16,
                shutdown_drain_timeout: Duration::from_millis(10),
                max_filter_fields: 16,
                max_filter_values: 64,
                slow_client_drop_limit: 2,
            },
        }
    }
    fn context(number: u64) -> BlockContext {
        BlockContext {
            block_num: number,
            block_hash: format!("block-{number}"),
            timestamp: "2026-10-22 20:00:00".into(),
            timestamp_seconds: 1792699200,
            network: "eth".into(),
            module_hash: source().module_hash,
        }
    }
    fn rows() -> Vec<serde_json::Map<String, Value>> {
        [
            json!({"@table":"transfers", "from":"alice", "amount":"9007199254740993"}),
            json!({"@table":"transfers", "from":"bob", "amount":"2"}),
        ]
        .into_iter()
        .map(|v| v.as_object().unwrap().clone())
        .collect()
    }
    type Socket = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;
    async fn next(socket: &mut Socket) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn native_only_discovery_routes_filters_and_undo_share_existing_protocol() {
        let mut server = EmbeddedServer::prepare_with_native(config(), vec![source()])
            .await
            .unwrap();
        assert!(server.stream_tasks.is_empty());
        let health = server.source_health();
        let mut publisher = server.native_publisher(0).unwrap();
        assert!(server.native_publisher(0).is_none());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = server.router(EmbeddedRoutes::default());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let discovery: Value = reqwest::get(format!("http://{address}/streams"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(discovery[0]["package_name"], "native_transfers");
        assert_eq!(discovery[0]["manifest"], "native:0123456789abcdef");
        assert_eq!(discovery[0]["network_alias"], "mainnet");
        let (mut raw, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/ws/mainnet@transfers?filter=from:alice"
        ))
        .await
        .unwrap();
        let (mut wrapped, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/stream?streams=eth@transfers"
        ))
        .await
        .unwrap();
        assert_eq!(
            next(&mut raw).await["subscriptions"],
            json!(["mainnet@transfers"])
        );
        next(&mut wrapped).await;
        publisher.started().await.unwrap();
        assert_eq!(next(&mut raw).await["status"], "started");
        assert_eq!(next(&mut wrapped).await["data"]["status"], "started");
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 0);
        publisher.publish_block(context(100), rows()).await.unwrap();
        let block = next(&mut raw).await;
        assert_eq!(
            block,
            json!({"network":"mainnet", "table":"transfers", "block_num":100,
            "block_hash":"block-100", "timestamp":"2026-10-22 20:00:00", "timestamp_seconds":1792699200i64,
            "module_hash":"0123456789abcdef", "events":[{"from":"alice", "amount":"9007199254740993"}]})
        );
        let block = next(&mut wrapped).await;
        assert_eq!(block["stream"], "eth@transfers");
        assert_eq!(block["data"]["events"].as_array().unwrap().len(), 2);
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 1);
        publisher.undo(99).await.unwrap();
        assert_eq!(next(&mut raw).await["last_valid_block"], 99);
        assert_eq!(next(&mut wrapped).await["data"]["status"], "undo");
        publisher.publish_block(context(101), rows()).await.unwrap();
        assert_eq!(next(&mut raw).await["block_num"], 101);
        assert_eq!(next(&mut wrapped).await["data"]["block_num"], 101);
        let mut invalid = context(102);
        invalid.module_hash = "other".into();
        assert_eq!(
            publisher.publish_block(invalid, rows()).await,
            Err(NativePublishError::InvalidBlock)
        );
        publisher.decode_error().await.unwrap();
        assert_eq!(next(&mut raw).await["status"], "decode_error");
        assert_eq!(next(&mut wrapped).await["data"]["status"], "decode_error");
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 0);
        publisher.error().await.unwrap();
        assert_eq!(next(&mut raw).await["status"], "error");
        assert_eq!(next(&mut wrapped).await["data"]["status"], "error");
        raw.close(None).await.unwrap();
        wrapped.close(None).await.unwrap();
        drop(publisher);
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 0);
        drop(server);
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn health_tracks_empty_blocks_inactivity_errors_restart_drop_and_drain() {
        let mut server = EmbeddedServer::prepare_with_native(config(), vec![source()])
            .await
            .unwrap();
        let health = server.source_health();
        let mut publisher = server.native_publisher(0).unwrap();
        assert_eq!(
            health.snapshot(Duration::from_secs(120)).configured_sources,
            1
        );
        assert_eq!(
            publisher.publish_block(context(1), rows()).await,
            Err(NativePublishError::Inactive)
        );
        publisher.started().await.unwrap();
        publisher.publish_block(context(1), vec![]).await.unwrap();
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 1);
        tokio::time::advance(Duration::from_secs(121)).await;
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 0);
        publisher.activity().unwrap();
        publisher.error().await.unwrap();
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 0);
        assert_eq!(publisher.undo(0).await, Err(NativePublishError::Inactive));
        publisher.started().await.unwrap();
        publisher.activity().unwrap();
        publisher.decode_error().await.unwrap();
        assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 0);
        publisher.started().await.unwrap();
        publisher.activity().unwrap();
        publisher.completed().await.unwrap();
        publisher.started().await.unwrap();
        publisher.activity().unwrap();
        drop(server);
        assert!(health.snapshot(Duration::from_secs(120)).draining);
        assert_eq!(
            publisher.publish_block(context(2), rows()).await,
            Err(NativePublishError::Inactive)
        );
        assert_eq!(publisher.started().await, Err(NativePublishError::Inactive));
    }

    #[tokio::test]
    async fn invalid_blocks_are_atomic_and_require_a_new_session() {
        let mut server = EmbeddedServer::prepare_with_native(config(), vec![source()])
            .await
            .unwrap();
        let mut publisher = server.native_publisher(0).unwrap();
        let health = server.source_health();
        for variant in 0..6 {
            publisher.started().await.unwrap();
            publisher.activity().unwrap();
            let mut ctx = context(100);
            let mut events = rows();
            match variant {
                0 => ctx.network = "other".into(),
                1 => ctx.module_hash = "other".into(),
                2 => events[1]
                    .insert("@table".into(), "other".into())
                    .map(|_| ())
                    .unwrap(),
                3 => events[1]
                    .insert("amount".into(), 1.into())
                    .map(|_| ())
                    .unwrap(),
                4 => events[1]
                    .insert("amount".into(), "x".repeat(4096).into())
                    .map(|_| ())
                    .unwrap(),
                _ => events = (0..5).flat_map(|_| rows()).collect(),
            }
            assert_eq!(
                publisher.publish_block(ctx, events).await,
                Err(NativePublishError::InvalidBlock)
            );
            assert_eq!(health.snapshot(Duration::from_secs(120)).active_sources, 0);
            assert_eq!(
                publisher.publish_block(context(101), rows()).await,
                Err(NativePublishError::Inactive)
            );
        }
    }

    #[test]
    fn validation_rejects_invalid_identity_bounds_aliases_and_channel_overlap() {
        let cfg = config();
        assert!(cfg.validate().is_err());
        assert!(cfg.validate_with_native(&[source()]).is_ok());
        assert!(cfg.validate_with_native(&[source(), source()]).is_err());
        let mut other = source();
        other.tables = vec!["swaps".into()];
        assert!(cfg.validate_with_native(&[source(), other.clone()]).is_ok());
        other.network = "other".into();
        assert!(cfg.validate_with_native(&[other]).is_err());
        for variant in 0..7 {
            let mut bad = source();
            match variant {
                0 => bad.max_block_bytes = 0,
                1 => bad.max_events = 0,
                2 => bad.network = "eth@transfers".into(),
                3 => bad.module_hash.clear(),
                4 => bad.tables.clear(),
                5 => bad.tables.push("transfers".into()),
                _ => bad.description = "unsafe\ntext".into(),
            }
            assert!(cfg.validate_with_native(&[bad]).is_err());
        }
        let mut cfg = cfg;
        cfg.streams.push(crate::StreamConfig {
            tables: vec!["transfers".into()],
            substreams: crate::SubstreamsConfig {
                manifest: "missing.spkg".into(),
                module: "db_out".into(),
                endpoint: Some("https://unused.invalid".into()),
                network: Some("eth".into()),
                start_block: Some("-1".into()),
                start_cursor: None,
                stop_block: "0".into(),
                params: vec![],
                plaintext: false,
                insecure: false,
                production_mode: true,
                final_blocks_only: false,
                token: None,
                api_key: None,
                api_key_header: "x-api-key".into(),
                auth_url: None,
                max_decode_message_bytes: 4096,
            },
        });
        assert!(cfg.validate_with_native(&[source()]).is_err());
        cfg.streams[0].tables.clear();
        assert!(cfg.validate_with_native(&[source()]).is_err());
        cfg.streams[0].tables = vec!["swaps".into()];
        assert!(cfg.validate_with_native(&[source()]).is_ok());
    }
}
