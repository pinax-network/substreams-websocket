use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde_json::Value;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};

const TRIM_HEADROOM_FRACTION: f32 = 0.10;

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("replay I/O failed: {0}")]
    Io(#[from] io::Error),

    #[error("replay payload is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("replay payload missing required block_num field")]
    MissingBlockNum,
}

/// One append-only JSONL file per `(network, package_name, package_version,
/// module_hash)`. Each line is the **whole** block JSON (network, block_num,
/// timestamp, module_hash, events). On resume, callers filter events by
/// table at read time, since one block may carry events for multiple tables.
#[derive(Clone)]
pub struct ReplayLog {
    inner: Option<Arc<ReplayInner>>,
}

struct ReplayInner {
    dir: PathBuf,
    max_blocks: usize,
    trim_threshold: usize,
    streams: Mutex<HashMap<String, StreamState>>,
}

struct StreamState {
    path: PathBuf,
    line_count: usize,
}

/// File-name key derived from spkg provenance + network.
fn file_key(
    network: &str,
    package_name: &str,
    package_version: &str,
    module_hash_hex: &str,
) -> String {
    format!(
        "{}-{}@{}-{}",
        sanitize(network),
        sanitize(package_name),
        sanitize(package_version),
        sanitize(module_hash_hex),
    )
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl ReplayLog {
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn new(dir: impl Into<PathBuf>, max_blocks: usize) -> Self {
        if max_blocks == 0 {
            return Self::disabled();
        }
        let max_blocks_f = max_blocks as f32;
        let headroom = (max_blocks_f * TRIM_HEADROOM_FRACTION).ceil() as usize;
        Self {
            inner: Some(Arc::new(ReplayInner {
                dir: dir.into(),
                max_blocks,
                trim_threshold: max_blocks + headroom.max(1),
                streams: Mutex::new(HashMap::new()),
            })),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn max_blocks(&self) -> usize {
        self.inner.as_ref().map(|i| i.max_blocks).unwrap_or(0)
    }

    /// Append one block payload to the spkg-provenance-keyed JSONL file.
    pub async fn append(
        &self,
        network: &str,
        package_name: &str,
        package_version: &str,
        module_hash_hex: &str,
        payload: &str,
    ) -> Result<(), ReplayError> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };

        let key = file_key(network, package_name, package_version, module_hash_hex);
        let mut guard = inner.streams.lock().await;
        let state = match guard.get_mut(&key) {
            Some(state) => state,
            None => {
                tokio::fs::create_dir_all(&inner.dir).await?;
                let path = inner.dir.join(format!("{key}.jsonl"));
                let line_count = count_lines(&path).await?;
                guard.insert(
                    key.clone(),
                    StreamState {
                        path: path.clone(),
                        line_count,
                    },
                );
                guard.get_mut(&key).expect("just inserted")
            }
        };

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&state.path)
            .await?;
        file.write_all(payload.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;
        state.line_count += 1;

        if state.line_count >= inner.trim_threshold {
            trim_to(&state.path, inner.max_blocks).await?;
            state.line_count = inner.max_blocks;
        }
        Ok(())
    }

    /// Truncate the spkg log at the first row with `block_num > last_valid_block`.
    pub async fn truncate_after(
        &self,
        network: &str,
        package_name: &str,
        package_version: &str,
        module_hash_hex: &str,
        last_valid_block: u64,
    ) -> Result<(), ReplayError> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };

        let key = file_key(network, package_name, package_version, module_hash_hex);
        let path = inner.dir.join(format!("{key}.jsonl"));
        if !path.exists() {
            return Ok(());
        }

        let file = File::open(&path).await?;
        let mut reader = BufReader::new(file).lines();
        let mut kept = Vec::new();
        while let Some(line) = reader.next_line().await? {
            if line.is_empty() {
                continue;
            }
            let block_num = parse_block_num(&line)?;
            if block_num <= last_valid_block {
                kept.push(line);
            }
        }

        rewrite_lines(&path, &kept).await?;

        let mut guard = inner.streams.lock().await;
        if let Some(state) = guard.get_mut(&key) {
            state.line_count = kept.len();
        }
        Ok(())
    }

    /// Read every retained block with `block_num > from_block` that carries at
    /// least one event whose `@table == target_table` (or any table when
    /// `target_table` is `None`). Scans every `.jsonl` file in the directory
    /// whose name starts with `<network>-` (or all files when `network_filter`
    /// is `None`). Returns per-table sub-blocks: each entry holds the
    /// filtered block JSON with `events[]` restricted to rows matching the
    /// target table.
    pub async fn read_from(
        &self,
        network_filter: Option<&str>,
        target_table: Option<&str>,
        from_block: u64,
    ) -> Result<ReadResult, ReplayError> {
        let Some(inner) = &self.inner else {
            return Ok(ReadResult::default());
        };
        if !inner.dir.exists() {
            return Ok(ReadResult::default());
        }

        let mut dir = tokio::fs::read_dir(&inner.dir).await?;
        let mut matched_files = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(stem) = name.strip_suffix(".jsonl") else {
                continue;
            };
            if let Some(net) = network_filter
                && !stem.starts_with(&format!("{}-", sanitize(net)))
            {
                continue;
            }
            matched_files.push(entry.path());
        }

        let mut blocks: Vec<(u64, String)> = Vec::new();
        let mut oldest: Option<u64> = None;

        for path in matched_files {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file).lines();
            while let Some(line) = reader.next_line().await? {
                if line.is_empty() {
                    continue;
                }
                let mut value: Value = serde_json::from_str(&line)?;
                let block_num = value
                    .get("block_num")
                    .and_then(Value::as_u64)
                    .ok_or(ReplayError::MissingBlockNum)?;
                if oldest.map_or(true, |o| block_num < o) {
                    oldest = Some(block_num);
                }
                if block_num <= from_block {
                    continue;
                }
                if let Some(table) = target_table {
                    filter_events_by_table(&mut value, table);
                    let has_events = value
                        .get("events")
                        .and_then(Value::as_array)
                        .map(|a| !a.is_empty())
                        .unwrap_or(false);
                    if !has_events {
                        continue;
                    }
                    // Rewrite the top-level `table` field so the per-table
                    // payload looks identical to a live broadcast.
                    if let Some(obj) = value.as_object_mut() {
                        obj.insert("table".to_owned(), Value::String(table.to_owned()));
                    }
                }
                blocks.push((block_num, value.to_string()));
            }
        }

        blocks.sort_by_key(|(n, _)| *n);
        Ok(ReadResult { blocks, oldest })
    }
}

fn filter_events_by_table(block: &mut Value, target_table: &str) {
    let Some(events) = block.get_mut("events").and_then(Value::as_array_mut) else {
        return;
    };
    events.retain(|event| {
        event
            .get("@table")
            .and_then(Value::as_str)
            .map(|t| t == target_table)
            .unwrap_or(false)
    });
    // Drop the per-event @table prefix — the parent block now carries `table`
    // at the top level. Rebuild the map in iteration order so we preserve the
    // original field ordering; `serde_json::Map::remove` under the
    // `preserve_order` feature is `swap_remove` and would scramble it.
    for event in events {
        if let Some(obj) = event.as_object_mut() {
            let mut rebuilt = serde_json::Map::with_capacity(obj.len().saturating_sub(1));
            for (k, v) in obj.iter() {
                if k == "@table" {
                    continue;
                }
                rebuilt.insert(k.clone(), v.clone());
            }
            *obj = rebuilt;
        }
    }
}

#[derive(Default, Debug)]
pub struct ReadResult {
    pub blocks: Vec<(u64, String)>,
    pub oldest: Option<u64>,
}

async fn count_lines(path: &Path) -> Result<usize, io::Error> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file).lines();
    let mut count = 0usize;
    while let Some(line) = reader.next_line().await? {
        if !line.is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

async fn trim_to(path: &Path, keep: usize) -> Result<(), io::Error> {
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file).lines();
    let mut all = Vec::new();
    while let Some(line) = reader.next_line().await? {
        if !line.is_empty() {
            all.push(line);
        }
    }
    let drop_n = all.len().saturating_sub(keep);
    let tail = &all[drop_n..];
    rewrite_lines(path, tail).await
}

async fn rewrite_lines(path: &Path, lines: &[String]) -> Result<(), io::Error> {
    let tmp_path = path.with_extension("jsonl.tmp");
    let mut tmp = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;
    for line in lines {
        tmp.write_all(line.as_bytes()).await?;
        tmp.write_all(b"\n").await?;
    }
    tmp.flush().await?;
    drop(tmp);
    tokio::fs::rename(tmp_path, path).await
}

fn parse_block_num(line: &str) -> Result<u64, ReplayError> {
    let value: Value = serde_json::from_str(line)?;
    value
        .get("block_num")
        .and_then(Value::as_u64)
        .ok_or(ReplayError::MissingBlockNum)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PKG: &str = "svm_swaps";
    const VER: &str = "v0.1.0";
    const HASH: &str = "deadbeef";

    fn block_with_tables(block_num: u64, tables: &[&str]) -> String {
        let events: Vec<serde_json::Value> = tables
            .iter()
            .map(|t| serde_json::json!({ "@table": *t, "id": format!("evt-{block_num}-{t}") }))
            .collect();
        serde_json::json!({
            "network": "solana-mainnet",
            "block_num": block_num,
            "events": events,
        })
        .to_string()
    }

    fn tmpdir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "substreams-websocket-replay-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    #[tokio::test]
    async fn read_by_table_returns_filtered_blocks_above_from_block() {
        let dir = tmpdir("by_table");
        let log = ReplayLog::new(&dir, 100);
        for n in 100..110 {
            log.append(
                "solana-mainnet",
                PKG,
                VER,
                HASH,
                &block_with_tables(n, &["swaps", "transfers"]),
            )
            .await
            .unwrap();
        }
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 105)
            .await
            .unwrap();
        assert_eq!(result.blocks.len(), 4);
        for (block_num, raw) in &result.blocks {
            let v: Value = serde_json::from_str(raw).unwrap();
            assert_eq!(v["table"], "swaps");
            let events = v["events"].as_array().unwrap();
            assert_eq!(events.len(), 1);
            assert!(events[0].get("@table").is_none(), "@table must be stripped");
            assert_eq!(events[0]["id"], format!("evt-{block_num}-swaps"));
        }
        assert_eq!(result.oldest, Some(100));
    }

    #[tokio::test]
    async fn read_by_table_skips_blocks_without_matching_events() {
        let dir = tmpdir("no_match");
        let log = ReplayLog::new(&dir, 100);
        // odd blocks carry only `transfers`, even blocks carry only `swaps`
        for n in 100..104 {
            let tables: &[&str] = if n % 2 == 0 {
                &["swaps"]
            } else {
                &["transfers"]
            };
            log.append(
                "solana-mainnet",
                PKG,
                VER,
                HASH,
                &block_with_tables(n, tables),
            )
            .await
            .unwrap();
        }
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 99)
            .await
            .unwrap();
        assert_eq!(result.blocks.len(), 2);
        let block_nums: Vec<u64> = result.blocks.iter().map(|(n, _)| *n).collect();
        assert_eq!(block_nums, vec![100, 102]);
    }

    #[tokio::test]
    async fn cross_file_scan_merges_two_spkgs_into_one_table_stream() {
        let dir = tmpdir("cross");
        let log = ReplayLog::new(&dir, 100);
        log.append(
            "solana-mainnet",
            "svm_dex",
            "v0.5.0",
            "aaaa",
            &block_with_tables(100, &["swaps"]),
        )
        .await
        .unwrap();
        log.append(
            "solana-mainnet",
            "svm_pump",
            "v0.2.0",
            "bbbb",
            &block_with_tables(101, &["swaps"]),
        )
        .await
        .unwrap();
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 0)
            .await
            .unwrap();
        let nums: Vec<u64> = result.blocks.iter().map(|(n, _)| *n).collect();
        assert_eq!(nums, vec![100, 101]);
    }

    #[tokio::test]
    async fn truncate_after_drops_blocks_above_last_valid() {
        let dir = tmpdir("reorg");
        let log = ReplayLog::new(&dir, 100);
        for n in 100..110 {
            log.append(
                "solana-mainnet",
                PKG,
                VER,
                HASH,
                &block_with_tables(n, &["swaps"]),
            )
            .await
            .unwrap();
        }
        log.truncate_after("solana-mainnet", PKG, VER, HASH, 104)
            .await
            .unwrap();
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 0)
            .await
            .unwrap();
        let nums: Vec<u64> = result.blocks.iter().map(|(n, _)| *n).collect();
        assert_eq!(nums, vec![100, 101, 102, 103, 104]);
    }

    #[tokio::test]
    async fn disabled_log_is_noop() {
        let log = ReplayLog::disabled();
        assert!(!log.is_enabled());
        log.append(
            "solana-mainnet",
            PKG,
            VER,
            HASH,
            &block_with_tables(1, &["swaps"]),
        )
        .await
        .unwrap();
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 0)
            .await
            .unwrap();
        assert!(result.blocks.is_empty());
        assert!(result.oldest.is_none());
    }
}
