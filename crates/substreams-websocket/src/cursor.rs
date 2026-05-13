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

    /// Cursor file path for a given module hash hex string.
    pub fn path(&self, module_hash_hex: &str) -> PathBuf {
        self.dir
            .join(format!("{}.cursor", sanitize(module_hash_hex)))
    }

    pub async fn load(&self, module_hash_hex: &str) -> io::Result<Option<String>> {
        match fs::read_to_string(self.path(module_hash_hex)).await {
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
    pub async fn save(&self, module_hash_hex: &str, cursor: &str) -> io::Result<()> {
        if cursor.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&self.dir).await?;
        let final_path = self.path(module_hash_hex);
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
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(store.load(hash).await.unwrap(), None);

        store.save(hash, "abc123").await.expect("save");
        assert_eq!(store.load(hash).await.unwrap(), Some("abc123".to_owned()));

        store.save(hash, "def456").await.expect("save");
        assert_eq!(store.load(hash).await.unwrap(), Some("def456".to_owned()));
    }

    #[tokio::test]
    async fn isolates_streams_by_hash() {
        let dir = tempdir().expect("tempdir");
        let store = CursorStore::new(dir.path());
        let h1 = "1111111111111111111111111111111111111111";
        let h2 = "2222222222222222222222222222222222222222";
        let h3 = "3333333333333333333333333333333333333333";
        store.save(h1, "AAA").await.unwrap();
        store.save(h2, "BBB").await.unwrap();
        store.save(h3, "CCC").await.unwrap();
        assert_eq!(store.load(h1).await.unwrap().as_deref(), Some("AAA"));
        assert_eq!(store.load(h2).await.unwrap().as_deref(), Some("BBB"));
        assert_eq!(store.load(h3).await.unwrap().as_deref(), Some("CCC"));
    }
}
