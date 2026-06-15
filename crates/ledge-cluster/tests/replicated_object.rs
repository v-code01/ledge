//! Task 5 integration tests: [`ReplicatedObjectStore`] — within-shard,
//! content-addressed, quorum object replication with anti-entropy read repair.
//!
//! Covers Steps 5.1–5.6 of the Phase 3 plan:
//! - 5.1 write replicates to all peers (+5.1b full convergence)
//! - 5.2 quorum tolerance: one peer down still succeeds; two down fails
//! - 5.3 read fetches from a peer when local is missing (anti-entropy repair)
//! - 5.4 content-addressing idempotency: same content twice → same id, no error
//! - 5.5 exists + write_batch (local + peer fallback, quorum durability)
//! - 5.6 ordering contract: a ref update references a quorum-durable object
//!
//! The peer set is an in-process registry of sibling `DiskObjectStore`s; the
//! `ObjectPeer` trait is the seam that Task 6 swaps for HTTP peers unchanged.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;

use ledge_cluster::object_store::{LocalObjectPeer, ObjectPeer, ReplicatedObjectStore};
use ledge_core::{LedgeError, ObjectId, ObjectStore};
use ledge_object_store::DiskObjectStore;

/// Build a fresh empty local `DiskObjectStore` (tempdir kept alive by caller).
fn build_empty_local() -> (Arc<DiskObjectStore>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
    (store, dir)
}

/// A built replica group: the local store, the peer handles (`ObjectPeer`), the
/// underlying peer stores (for assertions), and the tempdirs that keep them
/// alive for the test's lifetime.
type ReplicaGroup = (
    Arc<DiskObjectStore>,
    Vec<Arc<dyn ObjectPeer>>,
    Vec<Arc<DiskObjectStore>>,
    Vec<TempDir>,
);

/// 3-replica object group: a local store + two peer stores, with the two peer
/// stores wrapped as `ObjectPeer`s. Returns the tempdirs to keep them alive.
fn build_3_replica_object_group() -> ReplicaGroup {
    let (local, d0) = build_empty_local();
    let (p0, d1) = build_empty_local();
    let (p1, d2) = build_empty_local();
    let peers: Vec<Arc<dyn ObjectPeer>> = vec![
        Arc::new(LocalObjectPeer::new(p0.clone())),
        Arc::new(LocalObjectPeer::new(p1.clone())),
    ];
    (local, peers, vec![p0, p1], vec![d0, d1, d2])
}

/// Poll until every peer store holds `id`, or panic. Used for the 5.1b
/// convergence assertion (replication completes, not just quorum).
async fn await_object_on_all(peer_stores: &[Arc<DiskObjectStore>], id: &ObjectId) {
    for _ in 0..200 {
        let mut all = true;
        for ps in peer_stores {
            if !ps.exists(*id).await.unwrap() {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("object {} did not converge to all peers", id.to_hex());
}

// ── Step 5.1 — write replicates (quorum on return) + 5.1b full convergence ──

#[tokio::test]
async fn write_replicates_to_all_peers() {
    let (local, peers, peer_stores, _dirs) = build_3_replica_object_group();
    let store = ReplicatedObjectStore::new(local.clone(), peers);

    let id = store
        .write(Bytes::from_static(b"hello-object"))
        .await
        .unwrap();

    // On return the object is quorum-durable: local + at least one peer have it.
    assert!(
        local.exists(id).await.unwrap(),
        "local must have the object"
    );
    let mut on_peers = 0usize;
    for ps in &peer_stores {
        if ps.exists(id).await.unwrap() {
            on_peers += 1;
        }
    }
    // local(1) + on_peers >= quorum (2 of 3).
    assert!(
        1 + on_peers >= store.quorum(),
        "write must be quorum-durable"
    );
}

#[tokio::test]
async fn write_converges_to_all_peers() {
    // 5.1b: with all peers healthy, replication eventually reaches every peer
    // (the puts past quorum complete in the background / on the next poll).
    let (local, peers, peer_stores, _dirs) = build_3_replica_object_group();
    let store = ReplicatedObjectStore::new(local.clone(), peers);

    let id = store.write(Bytes::from_static(b"converge")).await.unwrap();
    await_object_on_all(&peer_stores, &id).await;
}

// ── Step 5.2 — quorum tolerance ──────────────────────────────────────────────

struct FailingPeer;
#[async_trait::async_trait]
impl ObjectPeer for FailingPeer {
    async fn put(&self, _: &ObjectId, _: &[u8]) -> ledge_core::Result<()> {
        Err(LedgeError::Unavailable("peer down".into()))
    }
    async fn put_git(&self, _: &ObjectId, _: u8, _: &[u8]) -> ledge_core::Result<()> {
        Err(LedgeError::Unavailable("peer down".into()))
    }
    async fn get(&self, _: &ObjectId) -> ledge_core::Result<Option<Bytes>> {
        Ok(None)
    }
    async fn has(&self, _: &ObjectId) -> ledge_core::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn write_succeeds_at_quorum_with_one_peer_down() {
    let (local, mut peers, _stores, _dirs) = build_3_replica_object_group(); // n=3, quorum=2
    peers[1] = Arc::new(FailingPeer); // one replica down
    let store = ReplicatedObjectStore::new(local.clone(), peers);

    // local(1) + 1 healthy peer = 2 acks == quorum(3) → success.
    let id = store.write(Bytes::from_static(b"survives")).await.unwrap();
    assert!(local.exists(id).await.unwrap());
}

#[tokio::test]
async fn write_fails_below_quorum_with_two_peers_down() {
    let (local, mut peers, _stores, _dirs) = build_3_replica_object_group(); // n=3, quorum=2
    peers[0] = Arc::new(FailingPeer);
    peers[1] = Arc::new(FailingPeer); // both peers down → only local ack (1 < 2)
    let store = ReplicatedObjectStore::new(local.clone(), peers);

    let r = store.write(Bytes::from_static(b"doomed")).await;
    assert!(
        matches!(r, Err(LedgeError::Unavailable(_))),
        "below quorum must be Unavailable, got {r:?}"
    );
}

// ── Step 5.3 — anti-entropy read repair ──────────────────────────────────────

#[tokio::test]
async fn read_fetches_from_peer_when_local_missing() {
    let (local, peers, peer_stores, _dirs) = build_3_replica_object_group();
    // Put the object ONLY on a peer store, never on local.
    let id = peer_stores[0]
        .write(Bytes::from_static(b"only-on-peer"))
        .await
        .unwrap();
    assert!(!local.exists(id).await.unwrap());

    let store = ReplicatedObjectStore::new(local.clone(), peers);
    let bytes = store.read(id).await.unwrap();
    assert_eq!(&bytes[..], b"only-on-peer");
    // Anti-entropy: the read repaired the local replica.
    assert!(
        local.exists(id).await.unwrap(),
        "read must repair local replica"
    );
}

/// A peer that returns bytes whose content address does NOT match the requested
/// id — the corruption guard must reject it.
struct TamperingPeer {
    bytes: Bytes,
}
#[async_trait::async_trait]
impl ObjectPeer for TamperingPeer {
    async fn put(&self, _: &ObjectId, _: &[u8]) -> ledge_core::Result<()> {
        Ok(())
    }
    async fn put_git(&self, _: &ObjectId, _: u8, _: &[u8]) -> ledge_core::Result<()> {
        Ok(())
    }
    async fn get(&self, _: &ObjectId) -> ledge_core::Result<Option<Bytes>> {
        Ok(Some(self.bytes.clone()))
    }
    async fn has(&self, _: &ObjectId) -> ledge_core::Result<bool> {
        Ok(true)
    }
}

#[tokio::test]
async fn read_rejects_peer_with_mismatched_content_address() {
    let (local, _peers, _stores, _dirs) = build_3_replica_object_group();
    // Ask for one id but the peer serves different bytes (→ different address).
    let requested = ObjectId::from_bytes([0x42u8; 32]);
    let tamper: Arc<dyn ObjectPeer> = Arc::new(TamperingPeer {
        bytes: Bytes::from_static(b"not-the-requested-object"),
    });
    let store = ReplicatedObjectStore::new(local, vec![tamper]);

    let r = store.read(requested).await;
    assert!(
        matches!(r, Err(LedgeError::Corruption(_))),
        "tampered peer bytes must be rejected as Corruption, got {r:?}"
    );
}

// ── Step 5.4 — content-addressing idempotency ────────────────────────────────

#[tokio::test]
async fn write_is_idempotent_on_identical_content() {
    let (local, peers, _stores, _dirs) = build_3_replica_object_group();
    let store = ReplicatedObjectStore::new(local, peers);

    let id1 = store.write(Bytes::from_static(b"dup")).await.unwrap();
    let id2 = store.write(Bytes::from_static(b"dup")).await.unwrap(); // must NOT error
    assert_eq!(id1, id2, "identical content must yield identical id");
}

// ── Step 5.5 — exists + write_batch ──────────────────────────────────────────

async fn count_has(peer_stores: &[Arc<DiskObjectStore>], id: &ObjectId) -> usize {
    let mut n = 0;
    for ps in peer_stores {
        if ps.exists(*id).await.unwrap() {
            n += 1;
        }
    }
    n
}

#[tokio::test]
async fn exists_and_write_batch() {
    let (local, peers, peer_stores, _dirs) = build_3_replica_object_group();
    let store = ReplicatedObjectStore::new(local.clone(), peers);
    let quorum = store.quorum();

    let ids = store
        .write_batch(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ])
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    for id in &ids {
        assert!(store.exists(*id).await.unwrap());
        // Each object reached a quorum across the replica set.
        let on_peers = count_has(&peer_stores, id).await;
        assert!(
            1 + on_peers >= quorum,
            "batch object must be quorum-durable"
        );
    }

    // `exists` falls back to peers when local is missing.
    let peer_only = peer_stores[0]
        .write(Bytes::from_static(b"peer-only"))
        .await
        .unwrap();
    let (local2, _d) = build_empty_local();
    let fallback_peers: Vec<Arc<dyn ObjectPeer>> = peer_stores
        .iter()
        .map(|s| Arc::new(LocalObjectPeer::new(s.clone())) as Arc<dyn ObjectPeer>)
        .collect();
    let store2 = ReplicatedObjectStore::new(local2, fallback_peers);
    assert!(
        store2.exists(peer_only).await.unwrap(),
        "exists must fall back to peers"
    );
}

#[tokio::test]
async fn typed_write_preserves_git_type_across_replicas() {
    // Replicating a typed (non-blob) git object must reproduce the type byte on
    // every replica: write a commit (type=1) and assert the type survives.
    let (local, peers, peer_stores, _dirs) = build_3_replica_object_group();
    let store = ReplicatedObjectStore::new(local.clone(), peers);

    let body = Bytes::from_static(b"tree 0\nauthor x <x> 0 +0000\n\nmsg\n");
    let id = store.write_git_object(1, body).await.unwrap();
    await_object_on_all(&peer_stores, &id).await;

    assert_eq!(local.git_type_of(id).await.unwrap(), 1);
    for ps in &peer_stores {
        assert_eq!(
            ps.git_type_of(id).await.unwrap(),
            1,
            "type byte must replicate"
        );
    }
}

// ── Step 5.6 — ordering contract (composes Task 4 + Task 5) ──────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_update_references_quorum_durable_object() {
    use ledge_cluster::testkit::MultiShardCluster;
    use ledge_core::RefName;
    use ledge_core::RefStore as _;

    // A 1-shard × 3-node cluster gives us the ClusterRefStore (Task 4); the
    // ReplicatedObjectStore (Task 5) replicates over a sibling 3-store group
    // standing in for the same shard's object replicas.
    let h = MultiShardCluster::start(1, &[1, 2, 3]).await;
    let refs = h.cluster_ref_store(1);

    let (local, peers, peer_stores, _dirs) = build_3_replica_object_group();
    let objs = ReplicatedObjectStore::new(local.clone(), peers);

    // Correct composition: write object to quorum FIRST, then ref-update.
    let oid = objs
        .write(Bytes::from_static(b"commit-bytes"))
        .await
        .unwrap();

    // Invariant the ordering contract guarantees: when the ref is allowed to
    // commit, the object is already present on a quorum of replicas.
    let mut have = 0;
    if local.exists(oid).await.unwrap() {
        have += 1;
    }
    have += count_has(&peer_stores, &oid).await;
    assert!(
        have >= objs.quorum(),
        "object must be on a quorum before the ref commits"
    );

    // Now the ref update referencing the quorum-durable object is safe.
    let name = RefName::new("refs/heads/main").unwrap();
    let entry = refs.update(&name, oid, None).await.unwrap();
    assert_eq!(entry.target, oid);
}
