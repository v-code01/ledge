//! S3 (object-storage) cold tier for immutable pack bodies. Over `object_store`
//! (InMemory in tests, AmazonS3/MinIO in prod). Keys are prefixed; all errors
//! map to [`LedgeError::Unavailable`] (a retryable infra fault).
//!
//! Verified against `object_store` 0.11.2:
//! - `ObjectStore::put(&Path, PutPayload)` — `Vec<u8>` → `PutPayload` via `From`.
//! - `ObjectStore::get(&Path) -> GetResult`; `GetResult::bytes(self) -> Bytes`.
//! - `ObjectStore::head` / `delete` return `Error::NotFound { .. }` on miss.
use std::sync::Arc;

use ledge_core::{LedgeError, Result};
use object_store::path::Path as OsPath;
use object_store::ObjectStore;

/// Cold-tier handle over an `object_store` backend. Cheap to clone-share via the
/// inner `Arc`. Keys are joined under `prefix` so multiple logical tiers can
/// share one bucket without collision.
pub struct S3Tier {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl S3Tier {
    /// In-process backend for tests (and a no-durability dev mode). No network.
    pub fn in_memory(prefix: &str) -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: prefix.to_string(),
        }
    }

    /// Build an AmazonS3/MinIO-backed tier from raw credentials. `endpoint`
    /// being `Some` selects path-style + HTTP-allowed addressing (MinIO and most
    /// self-hosted S3-compatible stores); `None` uses real AWS S3.
    pub fn from_parts(
        endpoint: Option<&str>,
        region: &str,
        bucket: &str,
        key_id: &str,
        secret: &str,
        prefix: &str,
    ) -> Result<Self> {
        let mut b = object_store::aws::AmazonS3Builder::new()
            .with_region(region)
            .with_bucket_name(bucket)
            .with_access_key_id(key_id)
            .with_secret_access_key(secret);
        if let Some(ep) = endpoint {
            b = b
                .with_endpoint(ep)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false);
        }
        let store = b
            .build()
            .map_err(|e| LedgeError::Unavailable(format!("s3 init: {e}")))?;
        Ok(Self {
            store: Arc::new(store),
            prefix: prefix.to_string(),
        })
    }

    /// Join a logical key under the configured prefix into an `object_store` path.
    fn path(&self, key: &str) -> OsPath {
        OsPath::from(format!("{}/{key}", self.prefix))
    }

    /// Upload a pack body. Idempotent overwrite (immutable content ⇒ same bytes).
    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.store
            .put(&self.path(key), bytes.into())
            .await
            .map(|_| ())
            .map_err(|e| LedgeError::Unavailable(format!("s3 put {key}: {e}")))
    }

    /// Fetch a pack body in full. Streams the object then collects to `Vec<u8>`.
    pub async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let r = self
            .store
            .get(&self.path(key))
            .await
            .map_err(|e| LedgeError::Unavailable(format!("s3 get {key}: {e}")))?;
        let b = r
            .bytes()
            .await
            .map_err(|e| LedgeError::Unavailable(format!("s3 body {key}: {e}")))?;
        Ok(b.to_vec())
    }

    /// Existence check. A `NotFound` is the expected negative answer (`Ok(false)`),
    /// NOT an error; any other failure is a tier fault.
    pub async fn head(&self, key: &str) -> Result<bool> {
        match self.store.head(&self.path(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(LedgeError::Unavailable(format!("s3 head {key}: {e}"))),
        }
    }

    /// Delete a pack body. Idempotent: deleting an absent key is `Ok(())`.
    pub async fn delete(&self, key: &str) -> Result<()> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(LedgeError::Unavailable(format!("s3 delete {key}: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn s3tier_roundtrip_inmemory() {
        let t = S3Tier::in_memory("ledge");
        assert!(!t.head("packs/x.pack").await.unwrap());
        t.put("packs/x.pack", b"hello pack".to_vec()).await.unwrap();
        assert!(t.head("packs/x.pack").await.unwrap());
        assert_eq!(t.get("packs/x.pack").await.unwrap(), b"hello pack".to_vec());
        t.delete("packs/x.pack").await.unwrap();
        assert!(!t.head("packs/x.pack").await.unwrap());
    }
}
