//! The `AtomicCommit` seam: an all-or-nothing multi-ref promotion abstraction
//! the workspace layer depends on, with a single-node implementation
//! ([`LocalAtomicCommit`]) over [`RefStoreImpl::commit_batch`].
//!
//! # Why this lives in `ledge-ref-store` (crate-cycle constraint)
//! The clustered implementation (`TxnCoordinator`) lives in `ledge-cluster`, but
//! the trait itself MUST live here: `ledge-workspace` (which injects the seam)
//! already depends on `ledge-ref-store`, and `ledge-cluster` depends on
//! `ledge-workspace`. Putting `AtomicCommit` in `ledge-cluster` would force
//! `ledge-workspace → ledge-cluster`, closing a dependency cycle. Defining it in
//! `ledge-ref-store` (a leaf both can reach) keeps the graph acyclic.
//!
//! # Atomicity guarantee
//! `commit_atomic` returns `Committed` only when EVERY mapped ref advanced, and
//! `Aborted` only when NONE did. There is no partial outcome. On a single node
//! this is a single `ArcSwap` root swap; clustered it rests on the 2PC commit
//! point (spec §4.4/§3.1).

use std::sync::Arc;

use async_trait::async_trait;

use ledge_core::{ObjectId, RefEntry, RefName, Result};

use crate::store::{CommitBatchError, RefStoreImpl};

/// One ref to promote: durable name, target to install, CAS precondition
/// (`None` ⇒ create-only / the ref must currently be absent).
pub type Mapping = (RefName, ObjectId, Option<ObjectId>);

/// The all-or-nothing result of an atomic multi-ref commit.
#[derive(Debug, Clone)]
pub enum AtomicCommitResult {
    /// Every durable ref was advanced; carries each `(name, new committed entry)`.
    Committed(Vec<(RefName, RefEntry)>),
    /// No durable ref was advanced. `conflicts` names the refs whose precondition
    /// failed (or that were locked by a live txn); `reason` is human-readable.
    Aborted {
        /// The refs that caused the abort (precondition failed / contended).
        conflicts: Vec<RefName>,
        /// A human-readable abort reason for logs/metrics labels.
        reason: String,
    },
}

/// The commit seam the workspace layer depends on. Single-node injects
/// [`LocalAtomicCommit`]; clustered injects `ledge_cluster::TxnCoordinator`.
#[async_trait]
pub trait AtomicCommit: Send + Sync {
    /// Promote `mappings` atomically (all-or-nothing). Never partial.
    async fn commit_atomic(&self, mappings: Vec<Mapping>) -> Result<AtomicCommitResult>;
}

/// Single-node atomic commit: delegates to [`RefStoreImpl::commit_batch`], which
/// evaluates all CAS preconditions over the current ART root and publishes either
/// all applies or none via one `ArcSwap` swap + one WAL frame per applied ref
/// (spec §4.4). No 2PC — atomicity is free on a single node.
pub struct LocalAtomicCommit {
    store: Arc<RefStoreImpl>,
}

impl LocalAtomicCommit {
    /// Wrap a concrete single-node ref store.
    pub fn new(store: Arc<RefStoreImpl>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl AtomicCommit for LocalAtomicCommit {
    async fn commit_atomic(&self, mappings: Vec<Mapping>) -> Result<AtomicCommitResult> {
        // An empty batch is trivially committed (nothing to advance).
        if mappings.is_empty() {
            return Ok(AtomicCommitResult::Committed(Vec::new()));
        }

        // `commit_batch` takes caller-supplied HLCs (the deterministic apply
        // contract): tick one per op from the store's shared HLC. The batch is
        // all-or-nothing, so the ticks are consumed iff the swap publishes.
        let ops: Vec<(RefName, ObjectId, Option<ObjectId>, u64)> = mappings
            .iter()
            .map(|(name, target, expected)| {
                (name.clone(), *target, *expected, self.store.hlc().tick())
            })
            .collect();

        match self.store.commit_batch(ops).await {
            // Every precondition held: `applied[i]` is the new committed entry for
            // `mappings[i]` in input order.
            Ok(applied) => {
                let committed = mappings
                    .into_iter()
                    .zip(applied)
                    .map(|((name, _, _), entry)| (name, entry))
                    .collect();
                Ok(AtomicCommitResult::Committed(committed))
            }
            // Any precondition failed ⇒ no ref advanced. `conflicts` carries the
            // failing `(name, current_committed)` pairs.
            Err(CommitBatchError::Conflicts(conflicts)) => {
                let names: Vec<RefName> = conflicts.into_iter().map(|(name, _)| name).collect();
                Ok(AtomicCommitResult::Aborted {
                    reason: format!("{} ref precondition(s) failed", names.len()),
                    conflicts: names,
                })
            }
            // The batch applied in memory but did not persist. On a single node
            // the ref-WAL is the only durability layer, so refuse to ack the
            // commit: propagate the I/O error rather than tell the client a write
            // that can vanish on restart succeeded.
            Err(CommitBatchError::NotDurable { source, .. }) => Err(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::{RefStore, HLC};
    use tempfile::TempDir;

    fn oid(n: u8) -> ObjectId {
        let mut b = [0u8; 32];
        b[31] = n;
        ObjectId::from_bytes(b)
    }

    /// The trait is object-safe (can be stored as `Arc<dyn AtomicCommit>`), which
    /// the workspace manager requires. Compile-time proof via a coercion.
    #[test]
    fn atomic_commit_is_object_safe() {
        fn _takes(_c: Arc<dyn AtomicCommit>) {}
        // No instance needed; this asserts the trait is dyn-compatible.
    }

    /// LocalAtomicCommit promotes all refs via a single atomic root swap, or none
    /// on a precondition failure (spec §4.4 single-node).
    #[tokio::test]
    async fn local_atomic_commit_all_or_nothing() {
        let dir = TempDir::new().unwrap();
        let hlc = Arc::new(HLC::new());
        let store = Arc::new(RefStoreImpl::open(dir.path().join("refs"), hlc).unwrap());
        let a = RefName::new("refs/heads/a").unwrap();
        let b = RefName::new("refs/heads/b").unwrap();

        let lac = LocalAtomicCommit::new(store.clone());

        // Both create-only (expected None) → Committed, both visible.
        let res = lac
            .commit_atomic(vec![(a.clone(), oid(1), None), (b.clone(), oid(2), None)])
            .await
            .unwrap();
        assert!(matches!(res, AtomicCommitResult::Committed(ref v) if v.len() == 2));
        assert_eq!(store.get(&a).await.unwrap().unwrap().target, oid(1));
        assert_eq!(store.get(&b).await.unwrap().unwrap().target, oid(2));

        // a's precondition now stale (it is oid(1), we claim None=create) → the
        // WHOLE batch aborts; b must NOT advance to oid(9).
        let res2 = lac
            .commit_atomic(vec![
                (a.clone(), oid(7), None), // create-only but a exists → conflict
                (b.clone(), oid(9), Some(oid(2))),
            ])
            .await
            .unwrap();
        match res2 {
            AtomicCommitResult::Aborted { conflicts, .. } => {
                assert!(conflicts.contains(&a), "a must be the conflict");
            }
            other => panic!("expected Aborted, got {other:?}"),
        }
        // Atomicity: neither ref advanced.
        assert_eq!(store.get(&a).await.unwrap().unwrap().target, oid(1));
        assert_eq!(store.get(&b).await.unwrap().unwrap().target, oid(2));
    }

    /// On a single node the ref-WAL is the only durability layer, so a WAL write
    /// failure must surface as an error — never a false "Committed". The batch is
    /// visible in memory, but the client is told the commit did not durably land.
    #[tokio::test]
    async fn local_atomic_commit_propagates_wal_failure() {
        let dir = TempDir::new().unwrap();
        let hlc = Arc::new(HLC::new());
        let store = Arc::new(RefStoreImpl::open(dir.path().join("refs"), hlc).unwrap());
        let lac = LocalAtomicCommit::new(store.clone());

        store.fail_next_wal_append();
        let res = lac
            .commit_atomic(vec![(RefName::new("refs/heads/z").unwrap(), oid(5), None)])
            .await;
        assert!(
            res.is_err(),
            "a non-durable commit must not be acked as Committed"
        );
    }
}
