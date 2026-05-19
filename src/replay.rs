use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};

/// Maximum percentage headroom over `max_blocks` before a lazy trim fires.
/// At `max_blocks = 1000` and `headroom = 0.10`, the file is allowed to grow
/// to 1100 lines before being rewritten down to 1000. Avoids per-block
/// rewrites at the cost of a small overshoot.
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

/// Per-stream append-only JSONL replay log. One file per `(network, stream)`
/// pair, named `{network}@{stream}.jsonl` under `dir`. Holds the last
/// `max_blocks` payloads. Disabled when `max_blocks == 0`.
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

impl ReplayLog {
    /// Disabled instance — `append` and `read_from` are no-ops / always return
    /// gap. Equivalent to `new(_, 0)`.
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

    /// Append one JSONL payload for `(network, stream)`. The payload must be
    /// pre-serialized JSON ending without a trailing newline (one will be
    /// added). Triggers a lazy trim once line count exceeds the threshold.
    pub async fn append(
        &self,
        network: &str,
        stream: &str,
        payload: &str,
    ) -> Result<(), ReplayError> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };

        let key = format!("{network}@{stream}");
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

    /// Read every retained payload with `block_num > from_block`, ordered by
    /// `block_num` ascending. Returns `Ok((blocks, oldest))` where `oldest` is
    /// the smallest `block_num` present on disk (None if empty). Callers use
    /// `oldest` to decide whether to emit a `gap` lifecycle message:
    /// `from_block < oldest - 1` means the resume point is below the window.
    pub async fn read_from(
        &self,
        network: &str,
        stream: &str,
        from_block: u64,
    ) -> Result<ReadResult, ReplayError> {
        let Some(inner) = &self.inner else {
            return Ok(ReadResult::default());
        };

        let path = inner.dir.join(format!("{network}@{stream}.jsonl"));
        if !path.exists() {
            return Ok(ReadResult::default());
        }

        let file = File::open(&path).await?;
        let mut reader = BufReader::new(file).lines();
        let mut blocks = Vec::new();
        let mut oldest: Option<u64> = None;

        while let Some(line) = reader.next_line().await? {
            if line.is_empty() {
                continue;
            }
            let block_num = parse_block_num(&line)?;
            if oldest.is_none() {
                oldest = Some(block_num);
            }
            if block_num > from_block {
                blocks.push((block_num, line));
            }
        }
        Ok(ReadResult { blocks, oldest })
    }

    /// Drop every payload with `block_num > last_valid_block`. Called on
    /// `BlockUndoSignal` so replay never serves undone forks.
    pub async fn truncate_after(
        &self,
        network: &str,
        stream: &str,
        last_valid_block: u64,
    ) -> Result<(), ReplayError> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };

        let key = format!("{network}@{stream}");
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
}

#[derive(Default, Debug)]
pub struct ReadResult {
    /// Payloads with `block_num > from_block`, ordered ascending.
    pub blocks: Vec<(u64, String)>,
    /// Smallest `block_num` present on disk, regardless of `from_block`.
    /// `None` if the file is empty / missing.
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
    let drop = all.len().saturating_sub(keep);
    let tail = &all[drop..];
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
    let value: serde_json::Value = serde_json::from_str(line)?;
    value
        .get("block_num")
        .and_then(serde_json::Value::as_u64)
        .ok_or(ReplayError::MissingBlockNum)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(block_num: u64) -> String {
        serde_json::json!({
            "stream": "swaps",
            "network": "solana-mainnet",
            "block_num": block_num,
            "events": [],
        })
        .to_string()
    }

    fn tmpdir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "substreams-websocket-replay-{}-{}",
            name,
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    #[tokio::test]
    async fn append_and_read_returns_blocks_above_from_block() {
        let dir = tmpdir("happy");
        let log = ReplayLog::new(&dir, 100);
        for n in 100..110 {
            log.append("solana-mainnet", "swaps", &payload(n))
                .await
                .unwrap();
        }
        let result = log.read_from("solana-mainnet", "swaps", 105).await.unwrap();
        let block_nums: Vec<u64> = result.blocks.iter().map(|(n, _)| *n).collect();
        assert_eq!(block_nums, vec![106, 107, 108, 109]);
        assert_eq!(result.oldest, Some(100));
    }

    #[tokio::test]
    async fn read_with_future_from_block_returns_empty() {
        let dir = tmpdir("future");
        let log = ReplayLog::new(&dir, 100);
        for n in 100..105 {
            log.append("solana-mainnet", "swaps", &payload(n))
                .await
                .unwrap();
        }
        let result = log
            .read_from("solana-mainnet", "swaps", 9_999)
            .await
            .unwrap();
        assert!(result.blocks.is_empty());
        assert_eq!(result.oldest, Some(100));
    }

    #[tokio::test]
    async fn read_with_missing_file_reports_no_oldest() {
        let dir = tmpdir("missing");
        let log = ReplayLog::new(&dir, 100);
        let result = log.read_from("solana-mainnet", "swaps", 0).await.unwrap();
        assert!(result.blocks.is_empty());
        assert!(result.oldest.is_none());
    }

    #[tokio::test]
    async fn lazy_trim_keeps_only_max_blocks_after_threshold() {
        let dir = tmpdir("trim");
        let log = ReplayLog::new(&dir, 10);
        // threshold = 10 + ceil(10*0.10) = 11, so on the 12th append we trim
        // back down to 10. Append 15 to verify we end exactly at 10 holding
        // the last 10 block_nums.
        for n in 100..115 {
            log.append("solana-mainnet", "swaps", &payload(n))
                .await
                .unwrap();
        }
        let result = log.read_from("solana-mainnet", "swaps", 0).await.unwrap();
        assert_eq!(result.blocks.len(), 10);
        let block_nums: Vec<u64> = result.blocks.iter().map(|(n, _)| *n).collect();
        assert_eq!(block_nums, (105..115).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn truncate_after_drops_blocks_above_last_valid() {
        let dir = tmpdir("reorg");
        let log = ReplayLog::new(&dir, 100);
        for n in 100..110 {
            log.append("solana-mainnet", "swaps", &payload(n))
                .await
                .unwrap();
        }
        log.truncate_after("solana-mainnet", "swaps", 104)
            .await
            .unwrap();
        let result = log.read_from("solana-mainnet", "swaps", 0).await.unwrap();
        let block_nums: Vec<u64> = result.blocks.iter().map(|(n, _)| *n).collect();
        assert_eq!(block_nums, vec![100, 101, 102, 103, 104]);
    }

    #[tokio::test]
    async fn disabled_log_is_noop() {
        let log = ReplayLog::disabled();
        assert!(!log.is_enabled());
        log.append("solana-mainnet", "swaps", &payload(1))
            .await
            .unwrap();
        let result = log.read_from("solana-mainnet", "swaps", 0).await.unwrap();
        assert!(result.blocks.is_empty());
        assert!(result.oldest.is_none());
    }

    #[tokio::test]
    async fn max_blocks_zero_is_disabled() {
        let dir = tmpdir("zero");
        let log = ReplayLog::new(&dir, 0);
        assert!(!log.is_enabled());
        log.append("solana-mainnet", "swaps", &payload(1))
            .await
            .unwrap();
        assert!(!dir.join("solana-mainnet@swaps.jsonl").exists());
    }

    #[tokio::test]
    async fn line_count_reloaded_on_restart() {
        let dir = tmpdir("restart");
        {
            let log = ReplayLog::new(&dir, 5);
            for n in 100..103 {
                log.append("solana-mainnet", "swaps", &payload(n))
                    .await
                    .unwrap();
            }
        }
        // New instance, same dir — must not trim existing content prematurely.
        let log = ReplayLog::new(&dir, 5);
        log.append("solana-mainnet", "swaps", &payload(103))
            .await
            .unwrap();
        let result = log.read_from("solana-mainnet", "swaps", 0).await.unwrap();
        assert_eq!(result.blocks.len(), 4);
    }
}
