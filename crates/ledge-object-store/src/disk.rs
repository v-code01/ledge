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
/// bytes  0..20  — SHA-1 of "<typename> <len>\0<content>"  (Git-compatible)
/// byte     20   — git object type (1=commit, 2=tree, 3=blob, 4=tag)
/// bytes 21..24  — reserved, always zero
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

    /// Git object type tags (pack/loose object kinds).
    /// 1=commit, 2=tree, 3=blob, 4=tag.
    ///
    /// Write `content` tagged with its git object `git_type`. The stored 20-byte
    /// header SHA-1 is the canonical git object id: SHA1("<typename> <len>\0<content>").
    /// The type byte is stored in the first reserved header byte so the fetch path
    /// can reconstruct a correctly-typed pack and serve the correct SHA-1.
    pub async fn write_git_object(
        &self,
        git_type: u8,
        content: bytes::Bytes,
    ) -> ledge_core::Result<ObjectId> {
        let type_name = match git_type {
            1 => "commit",
            2 => "tree",
            3 => "blob",
            4 => "tag",
            other => {
                return Err(ledge_core::LedgeError::Corruption(format!(
                    "unknown git object type {other}"
                )))
            }
        };
        // BLAKE3 address over raw content.
        let blake3_hash: [u8; 32] = blake3::hash(&content).into();
        let id = ObjectId::from_bytes(blake3_hash);
        // Canonical git SHA-1 over "<type> <len>\0<content>".
        let mut sha1_hasher = sha1::Sha1::new();
        sha1::Digest::update(
            &mut sha1_hasher,
            format!("{type_name} {}\0", content.len()).as_bytes(),
        );
        sha1::Digest::update(&mut sha1_hasher, &content);
        let sha1_hash: [u8; 20] = sha1::Digest::finalize(sha1_hasher).into();

        let mut payload = Vec::with_capacity(24 + content.len());
        payload.extend_from_slice(&sha1_hash);
        payload.push(git_type); // reserved[0] = git type
        payload.extend_from_slice(&[0u8; 3]); // reserved[1..4]
        payload.extend_from_slice(&content);

        let tmp = self.tmp_path();
        let final_path = self.object_path(&id);
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(ledge_core::LedgeError::Io)?;
        }
        tokio::fs::write(&tmp, &payload)
            .await
            .map_err(ledge_core::LedgeError::Io)?;
        tokio::fs::rename(&tmp, &final_path)
            .await
            .map_err(ledge_core::LedgeError::Io)?;
        Ok(id)
    }

    /// Build a `git-SHA-1 → ObjectId` index by scanning every loose object.
    ///
    /// Walks `<data_dir>/objects/<XX>/<YY>/<id>` (skipping the `tmp/` staging
    /// dir) and reads each object's 24-byte header to recover the git SHA-1.
    /// This is the reverse map needed by the fetch path to resolve child git
    /// SHA-1s discovered while walking a commit's reachable object graph
    /// (commit → tree → blob), since the store is BLAKE3-addressed and git
    /// references objects by SHA-1.
    ///
    /// Complexity is O(N) in the number of stored objects; acceptable for the
    /// clone/fetch use case where a repo's object count is bounded.
    pub async fn sha1_index(
        &self,
    ) -> ledge_core::Result<std::collections::HashMap<[u8; 20], ObjectId>> {
        use tokio::io::AsyncReadExt as _;
        let mut map = std::collections::HashMap::new();
        let objects_dir = self.data_dir.join("objects");
        let mut lvl1 = match tokio::fs::read_dir(&objects_dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(map),
            Err(e) => return Err(ledge_core::LedgeError::Io(e)),
        };
        while let Some(d1) = lvl1.next_entry().await.map_err(ledge_core::LedgeError::Io)? {
            let name1 = d1.file_name();
            // Skip the write-staging directory; only 2-hex fan-out dirs hold objects.
            if name1 == std::ffi::OsStr::new("tmp") {
                continue;
            }
            if !d1
                .file_type()
                .await
                .map_err(ledge_core::LedgeError::Io)?
                .is_dir()
            {
                continue;
            }
            let mut lvl2 = tokio::fs::read_dir(d1.path())
                .await
                .map_err(ledge_core::LedgeError::Io)?;
            while let Some(d2) = lvl2.next_entry().await.map_err(ledge_core::LedgeError::Io)? {
                if !d2
                    .file_type()
                    .await
                    .map_err(ledge_core::LedgeError::Io)?
                    .is_dir()
                {
                    continue;
                }
                let mut files = tokio::fs::read_dir(d2.path())
                    .await
                    .map_err(ledge_core::LedgeError::Io)?;
                while let Some(f) = files.next_entry().await.map_err(ledge_core::LedgeError::Io)? {
                    let hex = f.file_name();
                    let hex = match hex.to_str() {
                        Some(h) if h.len() == 64 => h,
                        _ => continue,
                    };
                    let id = match ObjectId::from_hex(hex) {
                        Ok(id) => id,
                        Err(_) => continue,
                    };
                    let mut file = tokio::fs::File::open(f.path())
                        .await
                        .map_err(ledge_core::LedgeError::Io)?;
                    let mut header = [0u8; 24];
                    let n = file.read(&mut header).await.map_err(ledge_core::LedgeError::Io)?;
                    if n < 24 {
                        continue;
                    }
                    let sha1: [u8; 20] = header[..20].try_into().unwrap();
                    map.insert(sha1, id);
                }
            }
        }
        Ok(map)
    }

    /// Enumerate the [`ObjectId`] of every loose object currently stored.
    ///
    /// Walks the same `<data_dir>/objects/<XX>/<YY>/<id>` fan-out as
    /// [`Self::sha1_index`], skipping the `tmp/` staging dir, but stops at the
    /// filename: each 64-hex file name is parsed straight into an `ObjectId`
    /// with no header read. This is the candidate-set source for GC
    /// (mark-and-sweep): every id returned here is a deletion candidate unless
    /// proven reachable.
    ///
    /// A missing `objects/` directory yields an empty vector (a freshly opened,
    /// never-written store). Non-directory entries and names that are not
    /// 64-hex are skipped defensively.
    ///
    /// Complexity: O(N) in the number of stored objects; no file contents are
    /// opened, so it is strictly cheaper than [`Self::sha1_index`].
    pub async fn list_all_ids(&self) -> ledge_core::Result<Vec<ObjectId>> {
        let mut ids = Vec::new();
        let objects_dir = self.data_dir.join("objects");
        let mut lvl1 = match tokio::fs::read_dir(&objects_dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
            Err(e) => return Err(LedgeError::Io(e)),
        };
        while let Some(d1) = lvl1.next_entry().await.map_err(LedgeError::Io)? {
            // Skip the write-staging directory; only 2-hex fan-out dirs hold objects.
            if d1.file_name() == std::ffi::OsStr::new("tmp") {
                continue;
            }
            if !d1.file_type().await.map_err(LedgeError::Io)?.is_dir() {
                continue;
            }
            let mut lvl2 = tokio::fs::read_dir(d1.path()).await.map_err(LedgeError::Io)?;
            while let Some(d2) = lvl2.next_entry().await.map_err(LedgeError::Io)? {
                if !d2.file_type().await.map_err(LedgeError::Io)?.is_dir() {
                    continue;
                }
                let mut files = tokio::fs::read_dir(d2.path()).await.map_err(LedgeError::Io)?;
                while let Some(f) = files.next_entry().await.map_err(LedgeError::Io)? {
                    let name = f.file_name();
                    let hex = match name.to_str() {
                        Some(h) if h.len() == 64 => h,
                        _ => continue,
                    };
                    if let Ok(id) = ObjectId::from_hex(hex) {
                        ids.push(id);
                    }
                }
            }
        }
        Ok(ids)
    }

    /// Remove the object file for `id`.
    ///
    /// **Idempotent:** a missing file is treated as success (`Ok(())`), because
    /// GC sweeps and lease release may both attempt to delete the same object,
    /// and a crash mid-sweep means the next pass re-attempts deletes that have
    /// already happened. Only the empty leaf file is removed; the `<XX>/<YY>/`
    /// fan-out directories are intentionally left in place to avoid an rmdir
    /// race with a concurrent writer creating a sibling object.
    ///
    /// Any I/O error other than "not found" is surfaced as [`LedgeError::Io`].
    pub async fn delete(&self, id: ObjectId) -> ledge_core::Result<()> {
        match tokio::fs::remove_file(self.object_path(&id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(LedgeError::Io(e)),
        }
    }

    /// Read the git object type byte from the header (reserved[0]).
    pub async fn git_type_of(&self, id: ObjectId) -> ledge_core::Result<u8> {
        use tokio::io::AsyncReadExt as _;
        let path = self.object_path(&id);
        let mut file = tokio::fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ledge_core::LedgeError::NotFound(id)
            } else {
                ledge_core::LedgeError::Io(e)
            }
        })?;
        let mut header = [0u8; 24];
        let n = file
            .read(&mut header)
            .await
            .map_err(ledge_core::LedgeError::Io)?;
        if n < 24 {
            return Err(ledge_core::LedgeError::Corruption(format!(
                "object {} header truncated",
                id.to_hex()
            )));
        }
        Ok(header[20])
    }

    /// Generate a unique temporary file path inside the staging directory.
    fn tmp_path(&self) -> PathBuf {
        let suffix: u64 = rand::thread_rng().gen();
        self.data_dir
            .join("objects")
            .join("tmp")
            .join(suffix.to_string())
    }

}

#[async_trait]
impl ObjectStore for DiskObjectStore {
    /// Write `content` to the store, returning its BLAKE3-addressed [`ObjectId`].
    ///
    /// Content-addressed deduplication: if the object already exists this is a
    /// no-op (the rename overwrites an identical file) and the same id is returned.
    ///
    /// Plain `write` stores raw content as a git blob (type=3), keeping the
    /// blob SHA-1 / header layout used by the existing object-store callers.
    async fn write(&self, content: Bytes) -> Result<ObjectId> {
        self.write_git_object(3, content).await
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

    #[tokio::test]
    async fn list_all_ids_returns_every_written_object() {
        let (store, _dir) = make_store();
        // Write three distinct objects (distinct content → distinct ids).
        let id_a = store.write(Bytes::from_static(b"alpha")).await.unwrap();
        let id_b = store.write(Bytes::from_static(b"beta")).await.unwrap();
        let id_c = store.write(Bytes::from_static(b"gamma")).await.unwrap();

        let mut ids = store.list_all_ids().await.unwrap();
        ids.sort_by_key(|id| *id.as_bytes());

        let mut expected = vec![id_a, id_b, id_c];
        expected.sort_by_key(|id| *id.as_bytes());

        assert_eq!(ids, expected, "list_all_ids must return exactly the written ids");
    }

    #[tokio::test]
    async fn list_all_ids_empty_store_is_empty() {
        let (store, _dir) = make_store();
        assert!(store.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_removes_object_then_exists_is_false() {
        let (store, _dir) = make_store();
        let id = store.write(Bytes::from_static(b"to be deleted")).await.unwrap();
        assert!(store.exists(id).await.unwrap());
        store.delete(id).await.unwrap();
        assert!(!store.exists(id).await.unwrap(), "object must be gone after delete");
    }

    #[tokio::test]
    async fn delete_missing_id_is_ok() {
        let (store, _dir) = make_store();
        // Deleting an id that was never written is a no-op (idempotent).
        store
            .delete(ObjectId::from_bytes([0x11u8; 32]))
            .await
            .expect("delete of a missing object must be Ok");
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
        // reserved[0] now holds the git object type byte (3 = blob for `write`).
        assert_eq!(raw[20], 3);
        assert_eq!(&raw[21..24], &[0u8; 3]);
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
