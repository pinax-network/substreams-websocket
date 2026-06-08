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

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("replay I/O failed: {0}")]
    Io(#[from] io::Error),

    #[error("replay payload is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("replay payload missing required timestamp_seconds field")]
    MissingTimestamp,
}

/// One append-only JSONL file per `(network, package_name, package_version,
/// module_hash)`. Each line is the **whole** block JSON (network, block_num,
/// timestamp, timestamp_seconds, module_hash, events). On resume, callers
/// filter events by `@table` at read time, since one block may carry events
/// for multiple tables. Trim is time-windowed: blocks older than
/// `max_seconds` relative to the newest line are dropped lazily.
#[derive(Clone)]
pub struct ReplayLog {
    inner: Option<Arc<ReplayInner>>,
}

struct ReplayInner {
    dir: PathBuf,
    max_seconds: u64,
    /// Trim hysteresis. Trim fires only when the oldest line lags the newest
    /// by more than `max_seconds + trim_headroom_seconds` so we don't rewrite
    /// the file on every append.
    trim_headroom_seconds: u64,
    streams: Mutex<HashMap<String, StreamState>>,
}

struct StreamState {
    path: PathBuf,
    /// Persistent append-mode handle. Opened once when this spkg first
    /// appends and kept alive for the lifetime of the process. Closed +
    /// reopened after `rewrite_lines` so the rename target is the file the
    /// handle points at.
    file: File,
    newest_ts: i64,
    oldest_ts: i64,
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

    pub fn new(dir: impl Into<PathBuf>, max_seconds: u64) -> Self {
        if max_seconds == 0 {
            return Self::disabled();
        }
        // 10% headroom — Solana at ~400ms/block means trim fires once per ~360s
        // window at the default 3600s retention. Ethereum at ~12s/block trims
        // roughly the same number of times because the file grows slower.
        let trim_headroom_seconds = (max_seconds as f32 * 0.10).ceil() as u64;
        Self {
            inner: Some(Arc::new(ReplayInner {
                dir: dir.into(),
                max_seconds,
                trim_headroom_seconds: trim_headroom_seconds.max(1),
                streams: Mutex::new(HashMap::new()),
            })),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn max_seconds(&self) -> u64 {
        self.inner.as_ref().map(|i| i.max_seconds).unwrap_or(0)
    }

    /// Append one block payload to the spkg-provenance-keyed JSONL file.
    /// `timestamp_seconds` is the block's Unix epoch; the replay window is
    /// computed against this value, not the wall clock.
    pub async fn append(
        &self,
        network: &str,
        package_name: &str,
        package_version: &str,
        module_hash_hex: &str,
        timestamp_seconds: i64,
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
                let (oldest_ts, newest_ts) = scan_timestamp_bounds(&path).await?;
                let file = open_append(&path).await?;
                guard.insert(
                    key.clone(),
                    StreamState {
                        path: path.clone(),
                        file,
                        oldest_ts,
                        newest_ts,
                    },
                );
                guard.get_mut(&key).expect("just inserted")
            }
        };

        // Single open per spkg — reuse the persistent file handle for every
        // append. Avoids the open/close churn that was burning fds (and
        // exposing us to "background task failed" from the blocking pool
        // when the OS pushed back on file ops).
        state.file.write_all(payload.as_bytes()).await?;
        state.file.write_all(b"\n").await?;
        state.file.flush().await?;

        if state.newest_ts == 0 || timestamp_seconds > state.newest_ts {
            state.newest_ts = timestamp_seconds;
        }
        if state.oldest_ts == 0 {
            state.oldest_ts = timestamp_seconds;
        }

        // Lazy trim. Threshold = max_seconds + headroom_seconds. When the
        // window grows past it, rewrite the file dropping any line older than
        // `newest_ts - max_seconds`.
        let window = state.newest_ts.saturating_sub(state.oldest_ts) as u64;
        let trim_threshold = inner.max_seconds + inner.trim_headroom_seconds;
        if window >= trim_threshold {
            let cutoff = state.newest_ts - inner.max_seconds as i64;
            // Rewrite renames a tmp file over the live path; reopen our
            // persistent handle so subsequent writes target the new file.
            let new_oldest = trim_older_than(&state.path, cutoff).await?;
            state.file = open_append(&state.path).await?;
            state.oldest_ts = new_oldest.unwrap_or(state.newest_ts);
        }
        Ok(())
    }

    /// Truncate the spkg log at the first row with `block_num > last_valid_block`.
    /// Used on `BlockUndoSignal` so replay never serves undone forks.
    pub async fn truncate_after_block(
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
            let (oldest, newest) = bounds_of(&kept);
            state.oldest_ts = oldest;
            state.newest_ts = newest;
            // Reopen the persistent handle so it points at the post-rename
            // file. Old handle is dropped (closed) here.
            state.file = open_append(&state.path).await?;
        }
        Ok(())
    }

    /// Read every retained block with `timestamp_seconds > from_timestamp`
    /// that carries at least one event whose `@table == target_table`.
    /// Scans every `.jsonl` file in the directory whose name starts with
    /// `<network>-` (or all files when `network_filter` is `None`). Returns
    /// per-table sub-blocks ordered by `timestamp_seconds`.
    pub async fn read_from(
        &self,
        network_filter: Option<&str>,
        target_table: Option<&str>,
        from_timestamp: i64,
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

        let mut blocks: Vec<(i64, String)> = Vec::new();
        let mut oldest: Option<i64> = None;

        for path in matched_files {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file).lines();
            while let Some(line) = reader.next_line().await? {
                if line.is_empty() {
                    continue;
                }
                let mut value: Value = serde_json::from_str(&line)?;
                let ts = value
                    .get("timestamp_seconds")
                    .and_then(Value::as_i64)
                    .ok_or(ReplayError::MissingTimestamp)?;
                if oldest.map_or(true, |o| ts < o) {
                    oldest = Some(ts);
                }
                if ts <= from_timestamp {
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
                blocks.push((ts, value.to_string()));
            }
        }

        blocks.sort_by_key(|(ts, _)| *ts);
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
    /// `(timestamp_seconds, raw_json)` ordered ascending by timestamp.
    pub blocks: Vec<(i64, String)>,
    /// Smallest `timestamp_seconds` seen across every scanned file before
    /// the from-filter was applied. `None` if no files matched.
    pub oldest: Option<i64>,
}

async fn open_append(path: &Path) -> Result<File, io::Error> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

async fn scan_timestamp_bounds(path: &Path) -> Result<(i64, i64), io::Error> {
    if !path.exists() {
        return Ok((0, 0));
    }
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file).lines();
    let mut oldest: i64 = 0;
    let mut newest: i64 = 0;
    while let Some(line) = reader.next_line().await? {
        if line.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(&line) {
            if let Some(ts) = value.get("timestamp_seconds").and_then(Value::as_i64) {
                if oldest == 0 || ts < oldest {
                    oldest = ts;
                }
                if ts > newest {
                    newest = ts;
                }
            }
        }
    }
    Ok((oldest, newest))
}

/// Rewrite `path` keeping only lines whose `timestamp_seconds >= cutoff`.
/// Returns the smallest retained timestamp, or `None` if the file is empty
/// after trim.
async fn trim_older_than(path: &Path, cutoff: i64) -> Result<Option<i64>, io::Error> {
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file).lines();
    let mut kept = Vec::new();
    let mut smallest: Option<i64> = None;
    while let Some(line) = reader.next_line().await? {
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let ts = value
            .get("timestamp_seconds")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if ts >= cutoff {
            if smallest.map_or(true, |s| ts < s) {
                smallest = Some(ts);
            }
            kept.push(line);
        }
    }
    rewrite_lines(path, &kept).await?;
    Ok(smallest)
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
    Ok(value.get("block_num").and_then(Value::as_u64).unwrap_or(0))
}

fn bounds_of(lines: &[String]) -> (i64, i64) {
    let mut oldest: i64 = 0;
    let mut newest: i64 = 0;
    for line in lines {
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            if let Some(ts) = value.get("timestamp_seconds").and_then(Value::as_i64) {
                if oldest == 0 || ts < oldest {
                    oldest = ts;
                }
                if ts > newest {
                    newest = ts;
                }
            }
        }
    }
    (oldest, newest)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PKG: &str = "svm_swaps";
    const VER: &str = "v0.1.0";
    const HASH: &str = "deadbeef";

    fn block_with_tables(timestamp_seconds: i64, block_num: u64, tables: &[&str]) -> String {
        let events: Vec<serde_json::Value> = tables
            .iter()
            .map(|t| serde_json::json!({ "@table": *t, "id": format!("evt-{timestamp_seconds}-{t}") }))
            .collect();
        serde_json::json!({
            "network": "solana-mainnet",
            "block_num": block_num,
            "timestamp_seconds": timestamp_seconds,
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
    async fn read_by_table_returns_filtered_blocks_above_from_timestamp() {
        let dir = tmpdir("by_table");
        let log = ReplayLog::new(&dir, 600);
        for n in 0..10 {
            let ts = 1_000_000 + n;
            log.append(
                "solana-mainnet",
                PKG,
                VER,
                HASH,
                ts,
                &block_with_tables(ts, 100 + n as u64, &["swaps", "transfers"]),
            )
            .await
            .unwrap();
        }
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 1_000_005)
            .await
            .unwrap();
        assert_eq!(result.blocks.len(), 4);
        for (ts, raw) in &result.blocks {
            let v: Value = serde_json::from_str(raw).unwrap();
            assert_eq!(v["table"], "swaps");
            let events = v["events"].as_array().unwrap();
            assert_eq!(events.len(), 1);
            assert!(events[0].get("@table").is_none(), "@table must be stripped");
            assert_eq!(events[0]["id"], format!("evt-{ts}-swaps"));
        }
        assert_eq!(result.oldest, Some(1_000_000));
    }

    #[tokio::test]
    async fn time_window_trim_drops_old_blocks() {
        let dir = tmpdir("trim");
        // 10s window. Append 30 blocks spaced 1s apart — expect only the last
        // ~10–11s worth (with 10% headroom).
        let log = ReplayLog::new(&dir, 10);
        for n in 0..30 {
            let ts = 2_000_000 + n;
            log.append(
                "solana-mainnet",
                PKG,
                VER,
                HASH,
                ts,
                &block_with_tables(ts, 100 + n as u64, &["swaps"]),
            )
            .await
            .unwrap();
        }
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 0)
            .await
            .unwrap();
        // Window allows up to 10s + 1s headroom; the trim cutoff is
        // newest_ts - 10, so the oldest retained line is at most 10s old.
        let oldest_kept = result.blocks.first().map(|(ts, _)| *ts).unwrap_or(0);
        let newest_kept = result.blocks.last().map(|(ts, _)| *ts).unwrap_or(0);
        assert!(
            newest_kept - oldest_kept <= 11,
            "expected window <= 11s, got {}",
            newest_kept - oldest_kept
        );
        assert_eq!(newest_kept, 2_000_029);
    }

    #[tokio::test]
    async fn cross_file_scan_merges_two_spkgs_into_one_table_stream() {
        let dir = tmpdir("cross");
        let log = ReplayLog::new(&dir, 600);
        log.append(
            "solana-mainnet",
            "svm_dex",
            "v0.5.0",
            "aaaa",
            1_000_000,
            &block_with_tables(1_000_000, 100, &["swaps"]),
        )
        .await
        .unwrap();
        log.append(
            "solana-mainnet",
            "svm_pump",
            "v0.2.0",
            "bbbb",
            1_000_001,
            &block_with_tables(1_000_001, 100, &["swaps"]),
        )
        .await
        .unwrap();
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 0)
            .await
            .unwrap();
        let ts: Vec<i64> = result.blocks.iter().map(|(t, _)| *t).collect();
        assert_eq!(ts, vec![1_000_000, 1_000_001]);
    }

    #[tokio::test]
    async fn truncate_after_block_drops_blocks_above_last_valid() {
        let dir = tmpdir("reorg");
        let log = ReplayLog::new(&dir, 600);
        for n in 0..10 {
            let ts = 3_000_000 + n;
            log.append(
                "solana-mainnet",
                PKG,
                VER,
                HASH,
                ts,
                &block_with_tables(ts, 100 + n as u64, &["swaps"]),
            )
            .await
            .unwrap();
        }
        log.truncate_after_block("solana-mainnet", PKG, VER, HASH, 104)
            .await
            .unwrap();
        let result = log
            .read_from(Some("solana-mainnet"), Some("swaps"), 0)
            .await
            .unwrap();
        assert_eq!(result.blocks.len(), 5);
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
            1_000_000,
            &block_with_tables(1_000_000, 100, &["swaps"]),
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
