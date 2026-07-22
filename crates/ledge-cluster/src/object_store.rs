//! Within-shard quorum-replicated, content-addressed object store.
//!
//! [`ReplicatedObjectStore`] wraps a local [`DiskObjectStore`] plus the OTHER
//! replicas of the same shard (modelled here as [`ObjectPeer`]s) and implements
//! [`ledge_core::ObjectStore`], so it is a drop-in for the single-node store at
//! the `Arc<dyn ObjectStore>` seam.
//!
//! # Replication model
//! Objects are **content-addressed**: the [`ObjectId`] is the BLAKE3 digest of
//! the raw content. The canonical write path (`ObjectStore::write`) ships the
//! *raw content* to every peer; each peer independently re-derives the identical
//! 24-byte object header (git SHA-1 + type byte) via
//! [`DiskObjectStore::write`]. Because the header is a deterministic function of
//! the raw content + git type, **the SHA-1 / type header is reproduced
//! byte-for-byte on every replica without shipping the header bytes**. For
//! non-blob git objects (commit/tree/tag), the typed inherent method
//! [`ReplicatedObjectStore::write_git_object`] carries the git type tag so the
//! header type byte is preserved across replicas too.
//!
//! # Quorum & durability (spec §2.5, invariant D6)
//! A `write` returns `Ok(id)` only once the object is durable on a **quorum** of
//! the replica set (`n/2 + 1` of `n = local + peers`). The local write always
//! counts as one ack. **Returning from `write` means the object is
//! quorum-durable** — it is therefore safe to commit a `RefUpdate` that
//! references it (the caller / `ClusterRefStore` enforces this ordering; see the
//! `write` doc-comment and the 5.6 ordering-contract test).
//!
//! # Anti-entropy
//! `read`/`exists` self-repair: if the object is missing locally they fetch it
//! from a peer that has it, verify the content address (a peer cannot hand back
//! bytes that hash to a different id without being rejected as
//! [`LedgeError::Corruption`]), and repair the local replica.

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt as _};

use ledge_core::{LedgeError, ObjectId, ObjectStore, Result};
use ledge_object_store::DiskObjectStore;

/// A reachable object replica peer.
///
/// In tests this wraps a sibling [`DiskObjectStore`] in the same process; in
/// production (Task 6) it is an HTTP client to the peer's object endpoint. The
/// trait is the swap seam — no [`ReplicatedObjectStore`] logic changes when the
/// in-process registry is replaced by RPC.
#[async_trait]
pub trait ObjectPeer: Send + Sync {
    /// Replicate `content` (whose content address is `id`) to this peer.
    ///
    /// Idempotent: re-putting an already-present object is a no-op success, not
    /// an error (content addressing makes a re-put indistinguishable from the
    /// original).
    async fn put(&self, id: &ObjectId, content: &[u8]) -> Result<()>;

    /// Replicate a typed git object so the peer reproduces the same type byte.
    async fn put_git(&self, id: &ObjectId, git_type: u8, content: &[u8]) -> Result<()>;

    /// Fetch the raw content for `id` from this peer, or `None` if absent.
    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>>;

    /// Whether this peer holds an object for `id`.
    async fn has(&self, id: &ObjectId) -> Result<bool>;
}

/// In-process peer backed by a sibling [`DiskObjectStore`] (test vehicle).
pub struct LocalObjectPeer {
    store: Arc<DiskObjectStore>,
}

impl LocalObjectPeer {
    /// Wrap a sibling store as a peer.
    pub fn new(store: Arc<DiskObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ObjectPeer for LocalObjectPeer {
    async fn put(&self, id: &ObjectId, content: &[u8]) -> Result<()> {
        // Content-addressed: the store derives the id from the bytes; the put is
        // a no-op if the object already exists (idempotent).
        let got = self.store.write(Bytes::copy_from_slice(content)).await?;
        debug_assert_eq!(&got, id, "content address must match on put");
        // Reject a peer that somehow stored bytes under a different address.
        if &got != id {
            return Err(LedgeError::Corruption(format!(
                "peer put for {} produced address {}",
                id.to_hex(),
                got.to_hex()
            )));
        }
        Ok(())
    }

    async fn put_git(&self, id: &ObjectId, git_type: u8, content: &[u8]) -> Result<()> {
        let got = self
            .store
            .write_git_object(git_type, Bytes::copy_from_slice(content))
            .await?;
        if &got != id {
            return Err(LedgeError::Corruption(format!(
                "peer put_git for {} produced address {}",
                id.to_hex(),
                got.to_hex()
            )));
        }
        Ok(())
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        match self.store.read(*id).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(LedgeError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn has(&self, id: &ObjectId) -> Result<bool> {
        self.store.exists(*id).await
    }
}

/// Quorum-replicated object store over a shard's object peers.
///
/// Implements [`ledge_core::ObjectStore`] — assignable to `Arc<dyn ObjectStore>`.
pub struct ReplicatedObjectStore {
    local: Arc<DiskObjectStore>,
    /// The OTHER replicas of this shard (never self).
    ///
    /// Behind [`ArcSwap`] so the replication peer set is **live-swappable**
    /// (Phase 4g reconfiguration): a runtime reconfigure atomically replaces the
    /// set so new voters start receiving pushed object writes and removed voters
    /// stop, without tearing down the store. Readers (`write`/`read`/`exists`)
    /// snapshot the current set with one `load()` per call.
    peers: ArcSwap<Vec<Arc<dyn ObjectPeer>>>,
}

impl ReplicatedObjectStore {
    /// Build a replicated store over a `local` store and the shard's `peers`.
    pub fn new(local: Arc<DiskObjectStore>, peers: Vec<Arc<dyn ObjectPeer>>) -> Self {
        Self {
            local,
            peers: ArcSwap::from_pointee(peers),
        }
    }

    /// The local store (for tests / wiring that needs the underlying handle).
    pub fn local(&self) -> &Arc<DiskObjectStore> {
        &self.local
    }

    /// Majority quorum over `n = local + peers`.
    ///
    /// Uses the standard Raft majority `n/2 + 1`, which equals the spec's
    /// `⌈(n+1)/2⌉` for all `n ≥ 1` (proven by [`tests::quorum_matches_ceil`]).
    /// The local write always contributes one ack.
    pub fn quorum(&self) -> usize {
        let n = self.peers.load().len() + 1;
        n / 2 + 1
    }

    /// Live count of replication peers (introspection / tests).
    pub fn peer_count(&self) -> usize {
        self.peers.load().len()
    }

    /// Atomically replace the replication peer set (Phase 4g reconfiguration):
    /// new voters receive pushed object writes; removed voters drop out.
    pub fn set_peers(&self, peers: Vec<Arc<dyn ObjectPeer>>) {
        self.peers.store(Arc::new(peers));
    }
}

#[async_trait]
impl ObjectStore for ReplicatedObjectStore {
    /// Write `content` (stored as a git blob) and return its [`ObjectId`] once it
    /// is **quorum-durable**.
    ///
    /// # Ordering contract (D6, spec §2.5 / §4 step 2)
    /// A `RefUpdate` that references an object MUST be proposed only *after* this
    /// method returns for that object: returning means the object is present on a
    /// quorum, so the ref can never commit pointing at an object that a quorum
    /// lacks. `ClusterRefStore::update` is the enforcing caller.
    async fn write(&self, content: Bytes) -> Result<ObjectId> {
        // 1. Local write derives the content-addressed id (counts as 1 ack).
        let id = self.local.write(content.clone()).await?;
        // 2. Fan replication out to peers; count acks until quorum.
        let need = self.quorum();
        let mut acks = 1usize;
        if acks >= need {
            return Ok(id); // single-replica shard: local write is the quorum.
        }
        // Snapshot the live peer set once (owned-clone of the cheap Arcs) so we
        // never hold the ArcSwap `Guard` across an `.await` (the Guard is not
        // `Send`-friendly to carry across awaits; the per-write set is fixed).
        let peers: Vec<Arc<dyn ObjectPeer>> = self.peers.load().iter().cloned().collect();
        let mut futs = FuturesUnordered::new();
        for p in &peers {
            let p = p.clone();
            let content = content.clone();
            futs.push(async move { p.put(&id, &content).await });
        }
        let mut errs = Vec::new();
        while let Some(res) = futs.next().await {
            match res {
                Ok(()) => acks += 1,
                Err(e) => errs.push(e),
            }
            if acks >= need {
                // Quorum-durable: return for latency, but let the remaining peer
                // puts finish best-effort in the background so replicas converge
                // (a missing replica also self-repairs via anti-entropy on read).
                drain_in_background(futs);
                break;
            }
        }
        if acks >= need {
            Ok(id)
        } else {
            Err(LedgeError::Unavailable(format!(
                "object {} reached {acks}/{need} acks; errors: {errs:?}",
                id.to_hex()
            )))
        }
    }

    /// Write each object independently to quorum; returns once **every** object
    /// in the batch is quorum-durable (the batch form of the D6 contract).
    async fn write_batch(&self, contents: Vec<Bytes>) -> Result<Vec<ObjectId>> {
        // Sequential is correct and simple; per-object replication could be
        // pipelined later behind a benchmark (the contract is unchanged).
        let mut ids = Vec::with_capacity(contents.len());
        for c in contents {
            ids.push(self.write(c).await?);
        }
        Ok(ids)
    }

    /// Read `id`'s content; if missing locally, fetch from a peer that has it,
    /// verify its content address, and repair the local replica (anti-entropy).
    async fn read(&self, id: ObjectId) -> Result<Bytes> {
        match self.local.read(id).await {
            Ok(bytes) => return Ok(bytes),
            Err(LedgeError::NotFound(_)) => {} // fall through to peer fetch
            Err(e) => return Err(e),
        }
        let peers: Vec<Arc<dyn ObjectPeer>> = self.peers.load().iter().cloned().collect();
        // Try EVERY peer: an unreachable one must not fail a read that a later
        // replica can satisfy. Errors are remembered so we can tell "no replica
        // has it" (clean NotFound) from "some replica was unreachable" (retryable
        // Unavailable) — reporting NotFound for an object that merely couldn't be
        // reached would be a false negative the GC/fetch paths must never see.
        let mut peer_errs: Vec<LedgeError> = Vec::new();
        for p in &peers {
            match p.get(&id).await {
                Ok(Some(bytes)) => {
                    // Content addressing makes the fetch verifiable + idempotent:
                    // the local re-write rederives the address; a mismatch means
                    // the peer handed us tampered/corrupt bytes.
                    let stored = self.local.write(bytes.clone()).await?;
                    if stored != id {
                        return Err(LedgeError::Corruption(format!(
                            "peer object {} hashed to {}",
                            id.to_hex(),
                            stored.to_hex()
                        )));
                    }
                    return Ok(bytes);
                }
                Ok(None) => {} // this peer cleanly lacks it — try the next
                Err(e) => peer_errs.push(e), // unreachable — remember, try the next
            }
        }
        if peer_errs.is_empty() {
            Err(LedgeError::NotFound(id))
        } else {
            Err(LedgeError::Unavailable(format!(
                "object {} absent locally and {} peer(s) unreachable: {peer_errs:?}",
                id.to_hex(),
                peer_errs.len()
            )))
        }
    }

    /// Whether the object exists locally or on any peer.
    async fn exists(&self, id: ObjectId) -> Result<bool> {
        if self.local.exists(id).await? {
            return Ok(true);
        }
        let peers: Vec<Arc<dyn ObjectPeer>> = self.peers.load().iter().cloned().collect();
        // Consult every peer; a single unreachable replica must not mask another
        // that holds the object. Only after all peers answer "no" (Ok(false)) is
        // the object absent. If some were unreachable we cannot claim absence, so
        // the fault is surfaced as retryable rather than a false `Ok(false)`.
        let mut peer_errs: Vec<LedgeError> = Vec::new();
        for p in &peers {
            match p.has(&id).await {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(e) => peer_errs.push(e),
            }
        }
        if peer_errs.is_empty() {
            Ok(false)
        } else {
            Err(LedgeError::Unavailable(format!(
                "exists({}) inconclusive: {} peer(s) unreachable: {peer_errs:?}",
                id.to_hex(),
                peer_errs.len()
            )))
        }
    }
}

impl ReplicatedObjectStore {
    /// Typed write: store `content` as the given git object `git_type`
    /// (1=commit, 2=tree, 3=blob, 4=tag) and replicate the type tag so every
    /// replica reproduces the identical 24-byte header (SHA-1 + type byte).
    ///
    /// Same quorum-durability + ordering contract as [`ObjectStore::write`].
    pub async fn write_git_object(&self, git_type: u8, content: Bytes) -> Result<ObjectId> {
        let id = self
            .local
            .write_git_object(git_type, content.clone())
            .await?;
        let need = self.quorum();
        let mut acks = 1usize;
        if acks >= need {
            return Ok(id);
        }
        let peers: Vec<Arc<dyn ObjectPeer>> = self.peers.load().iter().cloned().collect();
        let mut futs = FuturesUnordered::new();
        for p in &peers {
            let p = p.clone();
            let content = content.clone();
            futs.push(async move { p.put_git(&id, git_type, &content).await });
        }
        let mut errs = Vec::new();
        while let Some(res) = futs.next().await {
            match res {
                Ok(()) => acks += 1,
                Err(e) => errs.push(e),
            }
            if acks >= need {
                drain_in_background(futs);
                break;
            }
        }
        if acks >= need {
            Ok(id)
        } else {
            Err(LedgeError::Unavailable(format!(
                "git object {} reached {acks}/{need} acks; errors: {errs:?}",
                id.to_hex()
            )))
        }
    }
}

/// Drive the remaining post-quorum peer puts to completion on a detached task.
///
/// Quorum durability is already achieved when this is called; these puts are
/// best-effort convergence. Errors are intentionally swallowed (anti-entropy on
/// read is the durable repair path). The stream is `'static` because every
/// captured future owns its `Arc<dyn ObjectPeer>` + copied id + owned content.
fn drain_in_background<F>(mut futs: FuturesUnordered<F>)
where
    F: std::future::Future<Output = Result<()>> + Send + 'static,
{
    if futs.is_empty() {
        return;
    }
    tokio::spawn(async move { while futs.next().await.is_some() {} });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A peer that is always unreachable — every call errors.
    struct ErrPeer;
    #[async_trait]
    impl ObjectPeer for ErrPeer {
        async fn put(&self, _: &ObjectId, _: &[u8]) -> Result<()> {
            Err(LedgeError::Unavailable("peer down".into()))
        }
        async fn put_git(&self, _: &ObjectId, _: u8, _: &[u8]) -> Result<()> {
            Err(LedgeError::Unavailable("peer down".into()))
        }
        async fn get(&self, _: &ObjectId) -> Result<Option<Bytes>> {
            Err(LedgeError::Unavailable("peer down".into()))
        }
        async fn has(&self, _: &ObjectId) -> Result<bool> {
            Err(LedgeError::Unavailable("peer down".into()))
        }
    }

    fn disk() -> (Arc<DiskObjectStore>, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        (s, dir)
    }

    /// A read that misses locally must self-heal from a peer that HAS the object,
    /// even when an earlier peer in the set is unreachable. The `?` on the peer
    /// fetch used to abort the whole read on the first peer's error, so a single
    /// down replica failed reads that another replica could satisfy — the opposite
    /// of what multi-replica read-repair is for.
    #[tokio::test]
    async fn read_heals_past_an_unreachable_peer() {
        let (local, _d0) = disk();
        let (good, _d1) = disk();
        let content = Bytes::from_static(b"object behind a down peer");
        let id = good.write(content.clone()).await.unwrap();

        // Peer order: the unreachable one FIRST, the holder SECOND.
        let peers: Vec<Arc<dyn ObjectPeer>> =
            vec![Arc::new(ErrPeer), Arc::new(LocalObjectPeer::new(good))];
        let store = ReplicatedObjectStore::new(local, peers);

        let got = store
            .read(id)
            .await
            .expect("read must heal from the good peer");
        assert_eq!(got, content);
    }

    /// `exists` likewise must consult every peer, not stop at the first error.
    #[tokio::test]
    async fn exists_checks_past_an_unreachable_peer() {
        let (local, _d0) = disk();
        let (good, _d1) = disk();
        let id = good
            .write(Bytes::from_static(b"present on a later peer"))
            .await
            .unwrap();
        let peers: Vec<Arc<dyn ObjectPeer>> =
            vec![Arc::new(ErrPeer), Arc::new(LocalObjectPeer::new(good))];
        let store = ReplicatedObjectStore::new(local, peers);
        assert!(
            store.exists(id).await.unwrap(),
            "exists must find it on peer 2"
        );
    }

    /// When NO peer has the object and none errored, the read is a clean NotFound.
    #[tokio::test]
    async fn read_absent_everywhere_is_not_found() {
        let (local, _d0) = disk();
        let (empty_peer, _d1) = disk();
        let store =
            ReplicatedObjectStore::new(local, vec![Arc::new(LocalObjectPeer::new(empty_peer))]);
        let missing = ObjectId::from_bytes([0x11; 32]);
        assert!(matches!(
            store.read(missing).await,
            Err(LedgeError::NotFound(_))
        ));
    }

    #[test]
    fn set_peers_swaps_peer_set() {
        let dir = tempfile::TempDir::new().unwrap();
        let local = std::sync::Arc::new(
            ledge_object_store::DiskObjectStore::new(dir.path().to_path_buf()).unwrap(),
        );
        let store = ReplicatedObjectStore::new(local.clone(), vec![]);
        assert_eq!(store.peer_count(), 0);
        let peer: std::sync::Arc<dyn ObjectPeer> = std::sync::Arc::new(LocalObjectPeer::new(local));
        store.set_peers(vec![peer]);
        assert_eq!(store.peer_count(), 1);
    }

    #[test]
    fn quorum_matches_ceil() {
        // n/2 + 1 (Raft majority) must equal ⌈(n+1)/2⌉ for n ∈ 1..=6.
        for n in 1usize..=6 {
            let majority = n / 2 + 1;
            let ceil = (n + 1).div_ceil(2);
            assert_eq!(majority, ceil, "quorum formula mismatch at n={n}");
        }
    }
}
