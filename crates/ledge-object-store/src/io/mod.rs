use std::path::Path;

use async_trait::async_trait;
use ledge_core::Result;

pub mod tokio_backend;

/// Pluggable I/O back-end used by the object store.
///
/// Implementors must be `Send + Sync` so they can be shared across async tasks.
/// The trait is intentionally minimal: write a byte slice to a path, read it
/// back. Higher-level concerns (content addressing, checksumming, compaction)
/// live above this layer.
#[async_trait]
pub trait IoBackend: Send + Sync {
    /// Atomically write `data` to `path`, creating all parent directories if
    /// they do not already exist.
    async fn write_file(&self, path: &Path, data: &[u8]) -> Result<()>;

    /// Read the full contents of `path` into a heap-allocated buffer.
    ///
    /// Returns `LedgeError::Io` if the file does not exist or is unreadable.
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>>;
}

// Both Linux and non-Linux targets use TokioIoBackend for now.
// The io-uring specialisation (PosixUringBackend) is wired in Task 9.
pub use tokio_backend::TokioIoBackend as PlatformIo;
