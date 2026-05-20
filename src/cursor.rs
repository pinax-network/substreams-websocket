use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

/// Per-stream cursor persistence. One file per
/// `(network, package_name, package_version, module_hash)` under `dir`.
///
/// Open-once: the first `save` for a given key creates (or opens) the file
/// and keeps a persistent handle. Subsequent saves seek to 0, write, and
/// truncate. Avoids the open/close churn that previously triggered
/// `background task failed` errors from the tokio blocking pool when fd
/// pressure was high.
#[derive(Debug, Clone)]
pub struct CursorStore {
    dir: PathBuf,
    handles: Arc<Mutex<HashMap<String, File>>>,
}

impl CursorStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Cursor file path keyed by `{network}-{pkg_name}@{pkg_version}-{hash}`.
    pub fn path(
        &self,
        network: &str,
        package_name: &str,
        package_version: &str,
        module_hash_hex: &str,
    ) -> PathBuf {
        self.dir.join(format!(
            "{}-{}@{}-{}.cursor",
            sanitize(network),
            sanitize(package_name),
            sanitize(package_version),
            sanitize(module_hash_hex)
        ))
    }

    pub async fn load(
        &self,
        network: &str,
        package_name: &str,
        package_version: &str,
        module_hash_hex: &str,
    ) -> io::Result<Option<String>> {
        match fs::read_to_string(self.path(network, package_name, package_version, module_hash_hex))
            .await
        {
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

    /// Persist a cursor. Reuses a persistent file handle per key. Writes are
    /// in-place: seek 0 → write → set_len → flush. Not crash-atomic — on a
    /// hard kernel crash the file may contain a partial cursor; on next
    /// startup, `load` reads the partial value, the Substreams server
    /// rejects it, and we fall back to the configured `start_block`.
    /// Acceptable trade for the 10–100× drop in syscalls per block.
    pub async fn save(
        &self,
        network: &str,
        package_name: &str,
        package_version: &str,
        module_hash_hex: &str,
        cursor: &str,
    ) -> io::Result<()> {
        if cursor.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&self.dir).await?;
        let key = format!(
            "{}-{}@{}-{}",
            sanitize(network),
            sanitize(package_name),
            sanitize(package_version),
            sanitize(module_hash_hex),
        );
        let path = self.path(network, package_name, package_version, module_hash_hex);

        let mut guard = self.handles.lock().await;
        let file = match guard.get_mut(&key) {
            Some(file) => file,
            None => {
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .await?;
                guard.insert(key.clone(), file);
                guard.get_mut(&key).expect("just inserted")
            }
        };

        file.seek(std::io::SeekFrom::Start(0)).await?;
        file.write_all(cursor.as_bytes()).await?;
        let new_len = cursor.as_bytes().len() as u64;
        file.set_len(new_len).await?;
        file.flush().await?;
        Ok(())
    }
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

pub fn dir_from_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const PKG: &str = "svm_transfers";
    const VER: &str = "v0.3.0";

    #[tokio::test]
    async fn roundtrips_cursor() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        let net = "solana-mainnet";
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(store.load(net, PKG, VER, hash).await.unwrap(), None);

        store
            .save(net, PKG, VER, hash, "abc123")
            .await
            .expect("save");
        assert_eq!(
            store.load(net, PKG, VER, hash).await.unwrap(),
            Some("abc123".to_owned())
        );

        // Shorter follow-up overwrite must truncate the trailing bytes from
        // the previous (longer) cursor — set_len does the work.
        store.save(net, PKG, VER, hash, "de").await.expect("save");
        assert_eq!(
            store.load(net, PKG, VER, hash).await.unwrap(),
            Some("de".to_owned())
        );

        store
            .save(net, PKG, VER, hash, "def456")
            .await
            .expect("save");
        assert_eq!(
            store.load(net, PKG, VER, hash).await.unwrap(),
            Some("def456".to_owned())
        );
    }

    #[tokio::test]
    async fn same_hash_on_different_networks_is_isolated() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        let hash = "1111111111111111111111111111111111111111";
        store
            .save("ethereum-mainnet", PKG, VER, hash, "ETH")
            .await
            .unwrap();
        store
            .save("arbitrum-one", PKG, VER, hash, "ARB")
            .await
            .unwrap();
        store
            .save("base-mainnet", PKG, VER, hash, "BASE")
            .await
            .unwrap();
        assert_eq!(
            store
                .load("ethereum-mainnet", PKG, VER, hash)
                .await
                .unwrap()
                .as_deref(),
            Some("ETH")
        );
        assert_eq!(
            store
                .load("arbitrum-one", PKG, VER, hash)
                .await
                .unwrap()
                .as_deref(),
            Some("ARB")
        );
        assert_eq!(
            store
                .load("base-mainnet", PKG, VER, hash)
                .await
                .unwrap()
                .as_deref(),
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
        store.save(net, PKG, VER, h1, "AAA").await.unwrap();
        store.save(net, PKG, VER, h2, "BBB").await.unwrap();
        assert_eq!(
            store.load(net, PKG, VER, h1).await.unwrap().as_deref(),
            Some("AAA")
        );
        assert_eq!(
            store.load(net, PKG, VER, h2).await.unwrap().as_deref(),
            Some("BBB")
        );
    }

    #[tokio::test]
    async fn different_pkg_versions_isolated() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        let net = "solana-mainnet";
        let hash = "4444444444444444444444444444444444444444";
        store.save(net, PKG, "v0.3.0", hash, "OLD").await.unwrap();
        store.save(net, PKG, "v0.4.0", hash, "NEW").await.unwrap();
        assert_eq!(
            store
                .load(net, PKG, "v0.3.0", hash)
                .await
                .unwrap()
                .as_deref(),
            Some("OLD")
        );
        assert_eq!(
            store
                .load(net, PKG, "v0.4.0", hash)
                .await
                .unwrap()
                .as_deref(),
            Some("NEW")
        );
    }
}
