use crate::{ObjectId, RefEntry};

/// All error variants surfaced by the Ledge storage engine.
///
/// Each variant carries the minimum context needed to diagnose the failure at
/// the call site without requiring callers to reach into internal state.
#[derive(Debug, thiserror::Error)]
pub enum LedgeError {
    /// A content-addressed object with the given id was not present in the
    /// object store.
    #[error("object not found: {0}")]
    NotFound(ObjectId),

    /// A compare-and-swap on a ref failed because another writer updated the
    /// ref concurrently. `current` holds the value observed at the time of the
    /// conflict so the caller can decide whether to retry.
    #[error("ref conflict: current is {current:?}")]
    Conflict { current: RefEntry },

    /// The ref name supplied by the caller violates Ledge's naming rules (no
    /// consecutive slashes, must not start/end with slash, etc.).
    #[error("invalid ref name: {0}")]
    InvalidRefName(String),

    /// An underlying OS / file-system I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Persistent storage returned data that failed integrity validation (bad
    /// checksum, truncated record, unexpected magic bytes, …).
    #[error("data corruption: {0}")]
    Corruption(String),

    /// A transient infrastructure fault prevented the operation from completing,
    /// but the data is intact and the request is safe to retry: no shard leader
    /// elected yet, an unreachable peer, a failed linearizability barrier, or a
    /// transient Raft `client_write` error. Distinct from [`Self::Corruption`],
    /// which signals a fatal integrity failure that retrying cannot fix.
    #[error("service unavailable: {0}")]
    Unavailable(String),

    /// A per-tenant quota was exceeded (Phase 4d-3). The message NAMES the
    /// resource with a stable prefix so the HTTP layer can classify it:
    /// `"requests: …"` → 429 Too Many Requests (rate limit); every other prefix
    /// (`"workspaces: …"`, `"durable_bytes: …"`, `"object_count: …"`) → 507
    /// Insufficient Storage. The body carries the reason; it NEVER names another
    /// tenant. Distinct from [`Self::Unavailable`] (retryable infra fault): a
    /// quota denial is a deliberate, client-actionable backpressure signal.
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),
}

/// Shorthand `Result` type alias used throughout the Ledge crates.
pub type Result<T> = std::result::Result<T, LedgeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_display() {
        let id = ObjectId::from_bytes([0u8; 32]);
        let err = LedgeError::NotFound(id);
        let msg = err.to_string();
        assert!(msg.starts_with("object not found:"), "got: {msg}");
    }

    #[test]
    fn test_invalid_ref_name_display() {
        let err = LedgeError::InvalidRefName("bad//ref".to_string());
        let msg = err.to_string();
        assert!(msg.contains("invalid ref name"), "got: {msg}");
    }

    #[test]
    fn test_conflict_display() {
        use crate::RefEntry;
        let entry = RefEntry {
            target: ObjectId::from_bytes([1u8; 32]),
            hlc: 42,
            version: 1,
        };
        let err = LedgeError::Conflict { current: entry };
        let msg = err.to_string();
        assert!(msg.contains("ref conflict"), "got: {msg}");
    }

    #[test]
    fn test_io_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "disk gone");
        let err: LedgeError = io_err.into();
        assert!(matches!(err, LedgeError::Io(_)));
    }

    #[test]
    fn test_corruption_display() {
        let err = LedgeError::Corruption("bad crc".to_string());
        assert!(err.to_string().contains("data corruption"));
    }

    #[test]
    fn test_unavailable_display() {
        let err = LedgeError::Unavailable("no leader elected".to_string());
        let msg = err.to_string();
        assert!(msg.starts_with("service unavailable:"), "got: {msg}");
        assert!(msg.contains("no leader elected"), "got: {msg}");
    }

    #[test]
    fn test_quota_exceeded_display() {
        let err = LedgeError::QuotaExceeded("workspaces: 2 limit reached".to_string());
        let msg = err.to_string();
        assert!(msg.starts_with("quota exceeded:"), "got: {msg}");
        assert!(msg.contains("workspaces: 2 limit reached"), "got: {msg}");
    }

    #[test]
    fn test_result_type_alias() {
        fn returns_result() -> Result<u32> {
            Ok(42)
        }
        assert_eq!(returns_result().unwrap(), 42);
    }
}
