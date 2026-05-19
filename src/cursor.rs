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

    /// Cursor file path keyed by `{network}-{module_hash_hex}`. The same
    /// `.spkg` module hash may be run against multiple chains (e.g. EVM
    /// mainnet vs Arbitrum), so the network is part of the cursor identity.
    pub fn path(&self, network: &str, module_hash_hex: &str) -> PathBuf {
        self.dir.join(format!(
            "{}-{}.cursor",
            sanitize(network),
            sanitize(module_hash_hex)
        ))
    }

    pub async fn load(&self, network: &str, module_hash_hex: &str) -> io::Result<Option<String>> {
        match fs::read_to_string(self.path(network, module_hash_hex)).await {
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
    pub async fn save(&self, network: &str, module_hash_hex: &str, cursor: &str) -> io::Result<()> {
        if cursor.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&self.dir).await?;
        let final_path = self.path(network, module_hash_hex);
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
        let net = "solana-mainnet";
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(store.load(net, hash).await.unwrap(), None);

        store.save(net, hash, "abc123").await.expect("save");
        assert_eq!(
            store.load(net, hash).await.unwrap(),
            Some("abc123".to_owned())
        );

        store.save(net, hash, "def456").await.expect("save");
        assert_eq!(
            store.load(net, hash).await.unwrap(),
            Some("def456".to_owned())
        );
    }

    #[tokio::test]
    async fn same_hash_on_different_networks_is_isolated() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        let hash = "1111111111111111111111111111111111111111";
        store.save("ethereum-mainnet", hash, "ETH").await.unwrap();
        store.save("arbitrum-one", hash, "ARB").await.unwrap();
        store.save("base-mainnet", hash, "BASE").await.unwrap();
        assert_eq!(
            store
                .load("ethereum-mainnet", hash)
                .await
                .unwrap()
                .as_deref(),
            Some("ETH")
        );
        assert_eq!(
            store.load("arbitrum-one", hash).await.unwrap().as_deref(),
            Some("ARB")
        );
        assert_eq!(
            store.load("base-mainnet", hash).await.unwrap().as_deref(),
            Some("BASE")
        );
    }

    #[tokio::test]
    async fn different_hashes_on_same_network_isolated() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        let net = "solana-mainnet";
        let h1 = "2222222222222222222222222222222222222222";
        let h2 = "3333333333333333333333333333333333333333";
        store.save(net, h1, "AAA").await.unwrap();
        store.save(net, h2, "BBB").await.unwrap();
        assert_eq!(store.load(net, h1).await.unwrap().as_deref(), Some("AAA"));
        assert_eq!(store.load(net, h2).await.unwrap().as_deref(), Some("BBB"));
    }
}
