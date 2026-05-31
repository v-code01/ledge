use std::path::Path;

use async_trait::async_trait;
use ledge_core::{LedgeError, Result};

use crate::io::IoBackend;

/// Tokio-based I/O back-end.
///
/// Uses `tokio::fs` for all file operations so that disk I/O does not block
/// the async executor. Parent directory creation is performed with
/// `create_dir_all`, which is idempotent and safe to call concurrently.
#[derive(Debug, Clone, Copy)]
pub struct TokioIoBackend;

#[async_trait]
impl IoBackend for TokioIoBackend {
    /// Write `data` to `path`, creating parent directories as needed.
    ///
    /// Complexity: O(d + n) where d is directory depth and n is data length.
    /// Side effects: creates directories and overwrites any existing file.
    async fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(LedgeError::Io)?;
        }
        tokio::fs::write(path, data).await.map_err(LedgeError::Io)
    }

    /// Read the full contents of `path`.
    ///
    /// Returns `LedgeError::Io` wrapping the underlying OS error when the file
    /// is absent or unreadable — callers can pattern-match on the inner
    /// `io::ErrorKind` to distinguish not-found from permission errors.
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        tokio::fs::read(path).await.map_err(LedgeError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::IoBackend;
    use tempfile::tempdir;

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let backend = TokioIoBackend;
        let path = dir.path().join("hello.bin");
        backend
            .write_file(&path, b"ledge io backend test payload")
            .await
            .unwrap();
        let got = backend.read_file(&path).await.unwrap();
        assert_eq!(got, b"ledge io backend test payload" as &[u8]);
    }

    #[tokio::test]
    async fn read_missing_returns_io_error() {
        let dir = tempdir().unwrap();
        let backend = TokioIoBackend;
        let result = backend.read_file(&dir.path().join("nope.bin")).await;
        assert!(matches!(result.unwrap_err(), ledge_core::LedgeError::Io(_)));
    }

    #[tokio::test]
    async fn write_creates_parent_dirs() {
        let dir = tempdir().unwrap();
        let backend = TokioIoBackend;
        let path = dir.path().join("a/b/c/data.bin");
        backend.write_file(&path, b"nested").await.unwrap();
        assert_eq!(backend.read_file(&path).await.unwrap(), b"nested" as &[u8]);
    }
}
