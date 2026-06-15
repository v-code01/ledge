use crate::{ObjectId, RefEntry, RefName, Result};
use bytes::Bytes;
use std::sync::Arc;

/// Content-addressed object storage.
///
/// All writes are content-addressed: the caller supplies raw bytes and receives
/// back the BLAKE3 digest that permanently identifies that content.  Reads are
/// immutable; a given [`ObjectId`] always returns the same bytes.
///
/// # Object safety
/// The trait is object-safe — implementors must be `Send + Sync` so that
/// `Arc<dyn ObjectStore>` can be shared across async tasks.
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Write `content` to the store, returning its content-addressed [`ObjectId`].
    ///
    /// If an object with the same id already exists the write is a no-op and
    /// the id is returned unchanged (content-addressed deduplication).
    async fn write(&self, content: Bytes) -> Result<ObjectId>;

    /// Write multiple objects in one logical call.
    ///
    /// Implementations may pipeline or batch the underlying I/O.  Returns ids
    /// in the same order as `contents`.
    async fn write_batch(&self, contents: Vec<Bytes>) -> Result<Vec<ObjectId>>;

    /// Read the bytes for the given `id`.
    ///
    /// # Errors
    /// Returns [`crate::LedgeError::NotFound`] if no object with that id exists.
    async fn read(&self, id: ObjectId) -> Result<Bytes>;

    /// Return `true` if the store already contains an object for `id`.
    async fn exists(&self, id: ObjectId) -> Result<bool>;
}

/// Mutable, versioned ref store with optimistic concurrency control.
///
/// Each ref maps a [`RefName`] to an [`ObjectId`] plus versioning metadata.
/// All mutations go through a compare-and-swap: the caller must supply the
/// `expected` current target so the store can reject stale writers.
///
/// # Object safety
/// The trait is object-safe — implementors must be `Send + Sync`.
#[async_trait::async_trait]
pub trait RefStore: Send + Sync {
    /// Return the current [`RefEntry`] for `name`, or `None` if it does not exist.
    async fn get(&self, name: &RefName) -> Result<Option<RefEntry>>;

    /// Atomically set `name` to `new`, checking `expected` first.
    ///
    /// - `expected = None` — create a new ref; fails if it already exists with
    ///   any target.
    /// - `expected = Some(id)` — update only if the current target equals `id`;
    ///   fails with [`crate::LedgeError::Conflict`] if it does not, or with
    ///   [`crate::LedgeError::NotFound`] if the ref does not exist at all.
    ///
    /// On success returns the freshly committed [`RefEntry`] (version already
    /// incremented).
    async fn update(
        &self,
        name: &RefName,
        new: ObjectId,
        expected: Option<ObjectId>,
    ) -> Result<RefEntry>;

    /// Delete the ref `name`, verifying that it still points to `expected`.
    ///
    /// # Errors
    /// - [`crate::LedgeError::NotFound`] if the ref does not exist.
    /// - [`crate::LedgeError::Conflict`] if the current target differs from `expected`.
    async fn delete(&self, name: &RefName, expected: ObjectId) -> Result<()>;

    /// Return all refs whose name starts with `prefix`, in unspecified order.
    async fn list(&self, prefix: &str) -> Result<Vec<(RefName, RefEntry)>>;

    /// Capture a consistent, point-in-time snapshot of all refs.
    ///
    /// The snapshot is immutable and does not reflect subsequent mutations.
    fn snapshot(&self) -> Arc<dyn RefSnapshot>;
}

/// An immutable, point-in-time view of the ref namespace.
///
/// Obtained via [`RefStore::snapshot`].  All reads are non-blocking and
/// allocation-cheap relative to round-tripping through the store.
pub trait RefSnapshot: Send + Sync {
    /// Look up a single ref by name.
    fn get(&self, name: &RefName) -> Option<RefEntry>;

    /// Return all refs whose name starts with `prefix`.
    fn list(&self, prefix: &str) -> Vec<(RefName, RefEntry)>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LedgeError, ObjectId, RefEntry, RefName};
    use bytes::Bytes;
    use std::sync::Arc;

    struct MemObjectStore {
        data: std::sync::Mutex<std::collections::HashMap<ObjectId, Bytes>>,
    }
    impl MemObjectStore {
        fn new() -> Self {
            Self {
                data: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for MemObjectStore {
        async fn write(&self, content: Bytes) -> crate::Result<ObjectId> {
            let id = ObjectId::from(blake3::hash(&content));
            self.data.lock().unwrap().insert(id, content);
            Ok(id)
        }
        async fn write_batch(&self, contents: Vec<Bytes>) -> crate::Result<Vec<ObjectId>> {
            let mut ids = Vec::with_capacity(contents.len());
            for c in contents {
                ids.push(self.write(c).await?);
            }
            Ok(ids)
        }
        async fn read(&self, id: ObjectId) -> crate::Result<Bytes> {
            self.data
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .ok_or(LedgeError::NotFound(id))
        }
        async fn exists(&self, id: ObjectId) -> crate::Result<bool> {
            Ok(self.data.lock().unwrap().contains_key(&id))
        }
    }

    struct MemRefStore {
        data: std::sync::Mutex<std::collections::HashMap<RefName, RefEntry>>,
        ver: std::sync::atomic::AtomicU64,
    }
    impl MemRefStore {
        fn new() -> Self {
            Self {
                data: std::sync::Mutex::new(Default::default()),
                ver: std::sync::atomic::AtomicU64::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl RefStore for MemRefStore {
        async fn get(&self, name: &RefName) -> crate::Result<Option<RefEntry>> {
            Ok(self.data.lock().unwrap().get(name).cloned())
        }
        async fn update(
            &self,
            name: &RefName,
            new: ObjectId,
            expected: Option<ObjectId>,
        ) -> crate::Result<RefEntry> {
            let mut map = self.data.lock().unwrap();
            let current = map.get(name).cloned();
            match (expected, &current) {
                (Some(exp), Some(cur)) if cur.target != exp => {
                    return Err(LedgeError::Conflict {
                        current: cur.clone(),
                    })
                }
                (Some(_), None) => return Err(LedgeError::NotFound(new)),
                _ => {}
            }
            let version = self.ver.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let entry = RefEntry {
                target: new,
                hlc: version,
                version,
            };
            map.insert(name.clone(), entry.clone());
            Ok(entry)
        }
        async fn delete(&self, name: &RefName, expected: ObjectId) -> crate::Result<()> {
            let mut map = self.data.lock().unwrap();
            match map.get(name) {
                None => return Err(LedgeError::NotFound(expected)),
                Some(cur) if cur.target != expected => {
                    return Err(LedgeError::Conflict {
                        current: cur.clone(),
                    })
                }
                _ => {}
            }
            map.remove(name);
            Ok(())
        }
        async fn list(&self, prefix: &str) -> crate::Result<Vec<(RefName, RefEntry)>> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.as_str().starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        }
        fn snapshot(&self) -> Arc<dyn RefSnapshot> {
            Arc::new(MemSnap(self.data.lock().unwrap().clone()))
        }
    }

    struct MemSnap(std::collections::HashMap<RefName, RefEntry>);
    impl RefSnapshot for MemSnap {
        fn get(&self, name: &RefName) -> Option<RefEntry> {
            self.0.get(name).cloned()
        }
        fn list(&self, prefix: &str) -> Vec<(RefName, RefEntry)> {
            self.0
                .iter()
                .filter(|(k, _)| k.as_str().starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
    }

    #[tokio::test]
    async fn test_object_store_write_read_roundtrip() {
        let s = MemObjectStore::new();
        let c = Bytes::from_static(b"hello ledge");
        let id = s.write(c.clone()).await.unwrap();
        assert_eq!(s.read(id).await.unwrap(), c);
    }

    #[tokio::test]
    async fn test_object_store_exists() {
        let s = MemObjectStore::new();
        let id = s.write(Bytes::from_static(b"exist")).await.unwrap();
        assert!(s.exists(id).await.unwrap());
        assert!(!s.exists(ObjectId::from_bytes([0u8; 32])).await.unwrap());
    }

    #[tokio::test]
    async fn test_object_store_not_found() {
        let s = MemObjectStore::new();
        let r = s.read(ObjectId::from_bytes([0xffu8; 32])).await;
        assert!(matches!(r, Err(LedgeError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_object_store_write_batch() {
        let s = MemObjectStore::new();
        let cs = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ];
        let ids = s.write_batch(cs.clone()).await.unwrap();
        assert_eq!(ids.len(), 3);
        for (id, c) in ids.iter().zip(cs.iter()) {
            assert_eq!(&s.read(*id).await.unwrap(), c);
        }
    }

    #[tokio::test]
    async fn test_object_store_deduplication() {
        let s = MemObjectStore::new();
        let c = Bytes::from_static(b"same");
        assert_eq!(s.write(c.clone()).await.unwrap(), s.write(c).await.unwrap());
    }

    #[tokio::test]
    async fn test_ref_store_create_and_get() {
        let s = MemRefStore::new();
        let n = RefName::new("refs/heads/main").unwrap();
        let t = ObjectId::from_bytes([1u8; 32]);
        let e = s.update(&n, t, None).await.unwrap();
        assert_eq!(e.target, t);
        assert_eq!(e.version, 1);
        assert_eq!(s.get(&n).await.unwrap().unwrap(), e);
    }

    #[tokio::test]
    async fn test_ref_store_conflict() {
        let s = MemRefStore::new();
        let n = RefName::new("refs/heads/main").unwrap();
        s.update(&n, ObjectId::from_bytes([1u8; 32]), None)
            .await
            .unwrap();
        let r = s
            .update(
                &n,
                ObjectId::from_bytes([2u8; 32]),
                Some(ObjectId::from_bytes([99u8; 32])),
            )
            .await;
        assert!(matches!(r, Err(LedgeError::Conflict { .. })));
    }

    #[tokio::test]
    async fn test_ref_store_delete() {
        let s = MemRefStore::new();
        let n = RefName::new("refs/heads/del").unwrap();
        let t = ObjectId::from_bytes([0xddu8; 32]);
        s.update(&n, t, None).await.unwrap();
        s.delete(&n, t).await.unwrap();
        assert!(s.get(&n).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_ref_store_delete_wrong_expected() {
        let s = MemRefStore::new();
        let n = RefName::new("refs/heads/conflict").unwrap();
        let t = ObjectId::from_bytes([0xaau8; 32]);
        s.update(&n, t, None).await.unwrap();
        // Delete with wrong expected ObjectId must return Conflict
        let wrong = ObjectId::from_bytes([0xbbu8; 32]);
        let r = s.delete(&n, wrong).await;
        assert!(matches!(r, Err(crate::LedgeError::Conflict { .. })));
        // Ref must still exist after failed delete
        assert!(s.get(&n).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_ref_store_list_prefix() {
        let s = MemRefStore::new();
        for r in ["refs/heads/main", "refs/heads/dev", "refs/tags/v1"] {
            s.update(
                &RefName::new(r).unwrap(),
                ObjectId::from_bytes([1u8; 32]),
                None,
            )
            .await
            .unwrap();
        }
        assert_eq!(s.list("refs/heads/").await.unwrap().len(), 2);
        assert_eq!(s.list("refs/tags/").await.unwrap().len(), 1);
        assert_eq!(s.list("refs/").await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_ref_snapshot_isolation() {
        let s = MemRefStore::new();
        let n = RefName::new("refs/heads/main").unwrap();
        let t1 = ObjectId::from_bytes([1u8; 32]);
        let t2 = ObjectId::from_bytes([2u8; 32]);
        s.update(&n, t1, None).await.unwrap();
        let snap = s.snapshot();
        s.update(&n, t2, Some(t1)).await.unwrap();
        assert_eq!(snap.get(&n).unwrap().target, t1);
        assert_eq!(s.get(&n).await.unwrap().unwrap().target, t2);
    }

    #[tokio::test]
    async fn test_trait_object_safety() {
        let os: Arc<dyn ObjectStore> = Arc::new(MemObjectStore::new());
        let rs: Arc<dyn RefStore> = Arc::new(MemRefStore::new());
        let id = os.write(Bytes::from_static(b"safety")).await.unwrap();
        assert!(os.exists(id).await.unwrap());
        let n = RefName::new("refs/heads/safety").unwrap();
        rs.update(&n, id, None).await.unwrap();
        assert!(rs.get(&n).await.unwrap().is_some());
    }
}
