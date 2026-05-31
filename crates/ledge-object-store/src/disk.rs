use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng as _;
use sha1::Digest as _;
use tracing::instrument;

use ledge_core::{LedgeError, ObjectId, ObjectStore, Result};

/// Content-addressed object store backed by the local filesystem.
///
/// Layout mirrors Git's loose-object layout for tooling compatibility:
///
/// ```text
/// <data_dir>/objects/
///     tmp/            ← write-then-rename staging area
///     <XX>/           ← first two hex digits of BLAKE3 id
///       <YY>/         ← next two hex digits
///         <full-64-hex-id>   ← the object file
/// ```
///
/// # Object file format
/// ```text
/// bytes  0..20  — SHA-1 of "blob <len>\0<content>"  (Git-compatible)
/// bytes 20..24  — reserved, always zero
/// bytes 24..    — raw content
/// ```
///
/// # Invariants
/// - Writes are atomic: content is written to `tmp/` then `rename(2)`'d to its
///   final path. A crash between the two produces an orphan in `tmp/` but never
///   a partial object file at the canonical path.
/// - Idempotency: if the final path already exists the rename is a no-op on
///   POSIX (atomic replacement of identical data).  No locking is required.
pub struct DiskObjectStore {
    data_dir: PathBuf,
}

impl DiskObjectStore {
    /// Create (or open) an object store rooted at `data_dir`.
    ///
    /// Creates `<data_dir>/objects/tmp/` on first call.  All subsequent calls
    /// are idempotent.
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(data_dir.join("objects").join("tmp"))
            .map_err(LedgeError::Io)?;
        Ok(Self { data_dir })
    }

    /// Canonical path for an object identified by `id`.
    pub fn object_path(&self, id: &ObjectId) -> PathBuf {
        let hex = id.to_hex();
        self.data_dir
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
    }

    /// Return the Git-compatible SHA-1 stored in the 20-byte header of an
    /// already-written object file.
    ///
    /// # Errors
    /// Returns [`LedgeError::NotFound`] if the object does not exist.
    /// Returns [`LedgeError::Corruption`] if the file is shorter than 20 bytes.
    #[instrument(skip(self), fields(id = %id.to_hex()))]
    pub async fn sha1_of(&self, id: ObjectId) -> Result<[u8; 20]> {
        use tokio::io::AsyncReadExt as _;
        let path = self.object_path(&id);
        let mut file = tokio::fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                LedgeError::NotFound(id)
            } else {
                LedgeError::Io(e)
            }
        })?;
        let mut header = [0u8; 24];
        let n = file.read(&mut header).await.map_err(LedgeError::Io)?;
        if n < 20 {
            return Err(LedgeError::Corruption(format!(
                "object {} header truncated: {n} bytes",
                id.to_hex()
            )));
        }
        Ok(header[..20].try_into().unwrap())
    }

    /// Generate a unique temporary file path inside the staging directory.
    fn tmp_path(&self) -> PathBuf {
        let suffix: u64 = rand::thread_rng().gen();
        self.data_dir
            .join("objects")
            .join("tmp")
            .join(suffix.to_string())
    }

    /// Core write logic shared by [`write`] and [`write_batch`].
    ///
    /// Algorithm:
    /// 1. Hash the content with BLAKE3 (object id) and SHA-1 (header).
    /// 2. Build the 24-byte header followed by raw content.
    /// 3. Write to a random tmp path, then `rename` to the canonical path.
    ///    POSIX `rename(2)` is atomic; concurrent writers produce identical
    ///    data so the last rename wins without corruption.
    async fn write_inner(&self, content: &[u8]) -> Result<ObjectId> {
        // Compute both digests in a single pass over content.
        let mut blake3_hasher = blake3::Hasher::new();
        let mut sha1_hasher = sha1::Sha1::new();
        // SHA-1 uses Git's blob header: "blob <len>\0"
        sha1_hasher.update(format!("blob {}\0", content.len()).as_bytes());
        blake3_hasher.update(content);
        sha1_hasher.update(content);

        let blake3_bytes: [u8; 32] = blake3_hasher.finalize().into();
        let sha1_bytes: [u8; 20] = sha1_hasher.finalize().into();
        let id = ObjectId::from_bytes(blake3_bytes);

        // Build the on-disk payload: [sha1:20][reserved:4][content]
        let mut payload = Vec::with_capacity(24 + content.len());
        payload.extend_from_slice(&sha1_bytes);
        payload.extend_from_slice(&[0u8; 4]); // reserved, always zero
        payload.extend_from_slice(content);

        let tmp = self.tmp_path();
        let final_path = self.object_path(&id);

        // Ensure the two-level fan-out directory exists.
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(LedgeError::Io)?;
        }

        // Write to tmp, then atomically rename to final path.
        // If the final path already exists (idempotent write), rename replaces
        // it with identical data — safe because content-addressed objects are
        // immutable.  The tmp file is always cleaned up by the OS on rename.
        tokio::fs::write(&tmp, &payload)
            .await
            .map_err(LedgeError::Io)?;
        tokio::fs::rename(&tmp, &final_path)
            .await
            .map_err(LedgeError::Io)?;

        Ok(id)
    }
}

#[async_trait]
impl ObjectStore for DiskObjectStore {
    /// Write `content` to the store, returning its BLAKE3-addressed [`ObjectId`].
    ///
    /// Content-addressed deduplication: if the object already exists this is a
    /// no-op (the rename overwrites an identical file) and the same id is returned.
    async fn write(&self, content: Bytes) -> Result<ObjectId> {
        self.write_inner(&content).await
    }

    /// Write multiple objects, returning their ids in input order.
    ///
    /// Each object is written by a dedicated [`tokio::spawn`]'d task, giving
    /// the runtime the opportunity to overlap I/O.  The result vector preserves
    /// the original ordering by collecting join handles in sequence.
    async fn write_batch(&self, contents: Vec<Bytes>) -> Result<Vec<ObjectId>> {
        // Construct a lightweight DiskObjectStore per task by cloning data_dir.
        // PathBuf is a heap pointer + length — cheap to clone.
        // We stay inside the impl block so the private field access is valid.
        let handles: Vec<_> = contents
            .into_iter()
            .map(|c| {
                let data_dir = self.data_dir.clone();
                tokio::spawn(async move {
                    DiskObjectStore { data_dir }.write(c).await
                })
            })
            .collect();

        let mut ids = Vec::with_capacity(handles.len());
        for handle in handles {
            ids.push(
                handle
                    .await
                    .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))??,
            );
        }
        Ok(ids)
    }

    /// Read and return the raw content bytes for `id`.
    ///
    /// # Errors
    /// Returns [`LedgeError::NotFound`] when no object with that id exists.
    /// Returns [`LedgeError::Corruption`] when the file is shorter than the
    /// 24-byte header.
    async fn read(&self, id: ObjectId) -> Result<Bytes> {
        let raw = tokio::fs::read(self.object_path(&id))
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    LedgeError::NotFound(id)
                } else {
                    LedgeError::Io(e)
                }
            })?;

        if raw.len() < 24 {
            return Err(LedgeError::Corruption(format!(
                "object {} too short: {} bytes",
                id.to_hex(),
                raw.len()
            )));
        }

        // Skip the 24-byte header and return only the content.
        Ok(Bytes::from(raw[24..].to_vec()))
    }

    /// Return `true` if an object for `id` is present in the store.
    async fn exists(&self, id: ObjectId) -> Result<bool> {
        match tokio::fs::metadata(self.object_path(&id)).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(LedgeError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ledge_core::{LedgeError, ObjectId, ObjectStore};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_store() -> (DiskObjectStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = DiskObjectStore::new(dir.path().to_path_buf()).unwrap();
        (store, dir)
    }

    // ── Task 8: write path ────────────────────────────────────────────────────

    #[tokio::test]
    async fn write_same_content_returns_same_id() {
        let (store, _dir) = make_store();
        let c = Bytes::from_static(b"hello ledge");
        assert_eq!(
            store.write(c.clone()).await.unwrap(),
            store.write(c).await.unwrap()
        );
    }

    #[tokio::test]
    async fn write_creates_fanout_path() {
        let (store, dir) = make_store();
        let id = store
            .write(Bytes::from_static(b"fanout path test"))
            .await
            .unwrap();
        let hex = id.to_hex();
        assert!(dir
            .path()
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
            .exists());
    }

    #[tokio::test]
    async fn write_file_has_24_byte_header() {
        let (store, dir) = make_store();
        let content = b"header layout check";
        let id = store
            .write(Bytes::copy_from_slice(content))
            .await
            .unwrap();
        let hex = id.to_hex();
        let raw = std::fs::read(
            dir.path()
                .join("objects")
                .join(&hex[..2])
                .join(&hex[2..4])
                .join(&hex),
        )
        .unwrap();
        assert_eq!(raw.len(), 24 + content.len());
        assert_eq!(&raw[20..24], &[0u8; 4]);
        assert_eq!(&raw[24..], content as &[u8]);
    }

    #[tokio::test]
    async fn write_header_sha1_matches_git_blob_hash() {
        use sha1::Digest as _;
        let (store, dir) = make_store();
        let content = b"git sha1 compatibility check";
        let id = store
            .write(Bytes::copy_from_slice(content))
            .await
            .unwrap();
        let hex = id.to_hex();
        let raw = std::fs::read(
            dir.path()
                .join("objects")
                .join(&hex[..2])
                .join(&hex[2..4])
                .join(&hex),
        )
        .unwrap();
        let stored: [u8; 20] = raw[..20].try_into().unwrap();
        let mut h = sha1::Sha1::new();
        h.update(format!("blob {}\0", content.len()).as_bytes());
        h.update(content);
        assert_eq!(stored, <[u8; 20]>::from(h.finalize()));
    }

    #[tokio::test]
    async fn write_leaves_no_tmp_files() {
        let (store, dir) = make_store();
        store
            .write(Bytes::from_static(b"cleanup test"))
            .await
            .unwrap();
        let tmp = dir.path().join("objects").join("tmp");
        if tmp.exists() {
            assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
        }
    }

    // ── Task 9: read path + sha1_of + write_batch + concurrent ───────────────

    #[tokio::test]
    async fn read_returns_original_content() {
        let (store, _dir) = make_store();
        let c = Bytes::from_static(b"round-trip payload");
        let id = store.write(c.clone()).await.unwrap();
        assert_eq!(store.read(id).await.unwrap(), c);
    }

    #[tokio::test]
    async fn read_missing_returns_not_found() {
        let (store, _dir) = make_store();
        assert!(matches!(
            store.read(ObjectId::from_bytes([0u8; 32])).await,
            Err(LedgeError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn exists_false_for_missing() {
        let (store, _dir) = make_store();
        assert!(!store
            .exists(ObjectId::from_bytes([0u8; 32]))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn exists_true_after_write() {
        let (store, _dir) = make_store();
        let id = store
            .write(Bytes::from_static(b"existence"))
            .await
            .unwrap();
        assert!(store.exists(id).await.unwrap());
    }

    #[tokio::test]
    async fn sha1_of_matches_git_blob_hash() {
        use sha1::Digest as _;
        let (store, _dir) = make_store();
        let content = b"sha1_of correctness";
        let id = store
            .write(Bytes::copy_from_slice(content))
            .await
            .unwrap();
        let sha1 = store.sha1_of(id).await.unwrap();
        let mut h = sha1::Sha1::new();
        h.update(format!("blob {}\0", content.len()).as_bytes());
        h.update(content);
        assert_eq!(sha1, <[u8; 20]>::from(h.finalize()));
    }

    #[tokio::test]
    async fn sha1_of_missing_returns_not_found() {
        let (store, _dir) = make_store();
        assert!(matches!(
            store
                .sha1_of(ObjectId::from_bytes([0xdeu8; 32]))
                .await,
            Err(LedgeError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn write_batch_ids_in_order() {
        let (store, _dir) = make_store();
        let cs: Vec<Bytes> = (0u8..8).map(|i| Bytes::from(vec![i; 64])).collect();
        let ids = store.write_batch(cs.clone()).await.unwrap();
        assert_eq!(ids.len(), 8);
        for (c, id) in cs.into_iter().zip(ids.iter()) {
            assert_eq!(store.write(c).await.unwrap(), *id);
        }
    }

    #[tokio::test]
    async fn concurrent_same_content_idempotent() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        let content = Bytes::from_static(b"concurrent idempotency test payload");
        let handles: Vec<_> = (0..64)
            .map(|_| {
                let s = Arc::clone(&store);
                let c = content.clone();
                tokio::spawn(async move { s.write(c).await.unwrap() })
            })
            .collect();
        let ids: Vec<ObjectId> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        let first = ids[0];
        assert!(ids.iter().all(|id| *id == first));
        let hex = first.to_hex();
        assert!(dir
            .path()
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
            .exists());
        let tmp = dir.path().join("objects").join("tmp");
        assert!(
            std::fs::read_dir(&tmp).unwrap().count() == 0,
            "tmp files leaked"
        );
    }

    #[tokio::test]
    async fn concurrent_unique_objects_all_stored() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        let handles: Vec<_> = (0u8..64)
            .map(|i| {
                let s = Arc::clone(&store);
                tokio::spawn(async move {
                    let c = Bytes::from(vec![i; 256]);
                    let id = s.write(c.clone()).await.unwrap();
                    (id, c)
                })
            })
            .collect();
        for result in futures::future::join_all(handles).await {
            let (id, original) = result.unwrap();
            assert_eq!(store.read(id).await.unwrap(), original);
        }
    }
}
