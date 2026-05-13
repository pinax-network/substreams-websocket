use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::{fs, io::AsyncWriteExt};

/// Per-stream cursor persistence. One file per (network, name) under `dir`.
#[derive(Debug, Clone)]
pub struct CursorStore {
    dir: PathBuf,
}

impl CursorStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn path(&self, network: &str, name: &str) -> PathBuf {
        self.dir
            .join(format!("{}-{}.cursor", sanitize(network), sanitize(name)))
    }

    pub async fn load(&self, network: &str, name: &str) -> io::Result<Option<String>> {
        match fs::read_to_string(self.path(network, name)).await {
            Ok(value) => {
                let trimmed = value.trim().to_owned();
                Ok(if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                })
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Persist atomically: write to `<path>.tmp` then rename.
    pub async fn save(&self, network: &str, name: &str, cursor: &str) -> io::Result<()> {
        if cursor.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&self.dir).await?;
        let final_path = self.path(network, name);
        let tmp_path = final_path.with_extension("cursor.tmp");
        {
            let mut file = fs::File::create(&tmp_path).await?;
            file.write_all(cursor.as_bytes()).await?;
            file.sync_all().await?;
        }
        fs::rename(tmp_path, final_path).await
    }
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn dir_from_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn roundtrips_cursor() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        assert_eq!(store.load("solana-mainnet", "swaps").await.unwrap(), None);

        store
            .save("solana-mainnet", "swaps", "abc123")
            .await
            .expect("save");
        assert_eq!(
            store.load("solana-mainnet", "swaps").await.unwrap(),
            Some("abc123".to_owned())
        );

        store
            .save("solana-mainnet", "swaps", "def456")
            .await
            .expect("save");
        assert_eq!(
            store.load("solana-mainnet", "swaps").await.unwrap(),
            Some("def456".to_owned())
        );
    }

    #[tokio::test]
    async fn isolates_streams() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        store.save("net-a", "swaps", "AAA").await.unwrap();
        store.save("net-b", "swaps", "BBB").await.unwrap();
        store.save("net-a", "transfers", "CCC").await.unwrap();
        assert_eq!(
            store.load("net-a", "swaps").await.unwrap().as_deref(),
            Some("AAA")
        );
        assert_eq!(
            store.load("net-b", "swaps").await.unwrap().as_deref(),
            Some("BBB")
        );
        assert_eq!(
            store.load("net-a", "transfers").await.unwrap().as_deref(),
            Some("CCC")
        );
    }
}
