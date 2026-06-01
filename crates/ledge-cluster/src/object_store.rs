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
    peers: Vec<Arc<dyn ObjectPeer>>,
}

impl ReplicatedObjectStore {
    /// Build a replicated store over a `local` store and the shard's `peers`.
    pub fn new(local: Arc<DiskObjectStore>, peers: Vec<Arc<dyn ObjectPeer>>) -> Self {
        Self { local, peers }
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
        let n = self.peers.len() + 1;
        n / 2 + 1
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
        let mut futs = FuturesUnordered::new();
        for p in &self.peers {
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
        for p in &self.peers {
            if let Some(bytes) = p.get(&id).await? {
                // Content addressing makes the fetch verifiable + idempotent:
                // the local re-write rederives the address; a mismatch means the
                // peer handed us tampered/corrupt bytes.
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
        }
        Err(LedgeError::NotFound(id))
    }

    /// Whether the object exists locally or on any peer.
    async fn exists(&self, id: ObjectId) -> Result<bool> {
        if self.local.exists(id).await? {
            return Ok(true);
        }
        for p in &self.peers {
            if p.has(&id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl ReplicatedObjectStore {
    /// Typed write: store `content` as the given git object `git_type`
    /// (1=commit, 2=tree, 3=blob, 4=tag) and replicate the type tag so every
    /// replica reproduces the identical 24-byte header (SHA-1 + type byte).
    ///
    /// Same quorum-durability + ordering contract as [`ObjectStore::write`].
    pub async fn write_git_object(&self, git_type: u8, content: Bytes) -> Result<ObjectId> {
        let id = self.local.write_git_object(git_type, content.clone()).await?;
        let need = self.quorum();
        let mut acks = 1usize;
        if acks >= need {
            return Ok(id);
        }
        let mut futs = FuturesUnordered::new();
        for p in &self.peers {
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
