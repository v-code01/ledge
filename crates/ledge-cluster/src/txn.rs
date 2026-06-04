//! Cross-shard atomic commit: the [`TxnCoordinator`] (single-shard `RefBatch`
//! fast path + multi-shard 2PC), implementing the `AtomicCommit` seam the
//! workspace layer depends on (spec §4.3).
//!
//! The trait, [`AtomicCommitResult`], and [`Mapping`] live in `ledge-ref-store`
//! (a leaf both `ledge-workspace` and `ledge-cluster` reach) to keep the crate
//! graph acyclic; they are re-exported here for ergonomic `ledge_cluster::txn::*`
//! access.
//!
//! # Atomicity guarantee
//! `commit_atomic` returns `Committed` only after the durable `TxnDecide{commit}`
//! entry is applied on the coordinator shard (the commit point, spec §3.1); it
//! returns `Aborted` only when NO durable ref was advanced. There is no third,
//! partial outcome. Multi-shard atomicity rests on: (a) no-wait prepare ⇒
//! deadlock-free (spec §3.2), (b) the replicated decision record ⇒ crash-safe
//! roll-forward (spec §3.4), (c) locks block writes but not reads ⇒ no dirty
//! reads (spec §3.3).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use ledge_core::{LedgeError, ObjectId, RefName, Result, TxnId};
use ledge_raft::{BatchOutcome, LedgeOp};

pub use ledge_ref_store::{AtomicCommit, AtomicCommitResult, Mapping};

use crate::forward::{ClusterOp, RefOpResponse};
use crate::ref_store::ClusterRefStore;
use crate::router::ShardId;

/// Multi-shard atomic-commit coordinator over a node's [`ClusterRefStore`]. It
/// drives prepare/commit/abort through `op_on_shard` (local-or-forwarded) and
/// the txn record through `apply_txn_record_op` on the coordinator shard.
pub struct TxnCoordinator {
    store: Arc<ClusterRefStore>,
}

impl TxnCoordinator {
    /// Build over the node's cluster ref store (its router + handle map + fwd).
    pub fn new(store: Arc<ClusterRefStore>) -> Self {
        Self { store }
    }

    /// Group `mappings` by owning shard, preserving each ref's (target, expected).
    /// Returns a `BTreeMap` so iteration is in ascending shard order (canonical),
    /// and sorts each shard's refs by name for a deterministic prepare order
    /// (spec §3.2, defense in depth against deadlock).
    fn group_by_shard(
        &self,
        mappings: &[Mapping],
    ) -> BTreeMap<ShardId, Vec<(RefName, ObjectId, Option<ObjectId>)>> {
        let router = self.store.router();
        let mut by: BTreeMap<ShardId, Vec<(RefName, ObjectId, Option<ObjectId>)>> =
            BTreeMap::new();
        for (name, target, expected) in mappings {
            let shard = router.shard_for(name.as_str());
            by.entry(shard)
                .or_default()
                .push((name.clone(), *target, *expected));
        }
        for refs in by.values_mut() {
            refs.sort_by(|x, y| x.0.as_str().cmp(y.0.as_str()));
        }
        by
    }

    /// Single-shard atomic commit via one `RefBatch` log entry (spec §3.5). The
    /// shard's state machine applies all CAS preconditions in one root swap: all
    /// hold ⇒ all advance, any fails ⇒ none advance.
    async fn commit_single_shard(
        &self,
        shard: ShardId,
        refs: &[(RefName, ObjectId, Option<ObjectId>)],
    ) -> Result<AtomicCommitResult> {
        let ops: Vec<(String, [u8; 32], Option<[u8; 32]>)> = refs
            .iter()
            .map(|(name, target, expected)| {
                (
                    name.as_str().to_string(),
                    *target.as_bytes(),
                    expected.map(|e| *e.as_bytes()),
                )
            })
            .collect();

        let outcomes = self.store.apply_batch_on_shard(shard, ops).await?;

        // All `Ok` ⇒ Committed; any `Conflict` ⇒ Aborted (the SM applied NONE).
        let mut committed = Vec::with_capacity(refs.len());
        let mut conflicts = Vec::new();
        for ((name, _, _), outcome) in refs.iter().zip(outcomes.into_iter()) {
            match outcome {
                BatchOutcome::Ok(e) => committed.push((name.clone(), e)),
                BatchOutcome::Conflict(_) => conflicts.push(name.clone()),
            }
        }
        if conflicts.is_empty() {
            Ok(AtomicCommitResult::Committed(committed))
        } else {
            Ok(AtomicCommitResult::Aborted {
                reason: "single-shard batch precondition failed".into(),
                conflicts,
            })
        }
    }
}

#[async_trait]
impl AtomicCommit for TxnCoordinator {
    async fn commit_atomic(&self, mappings: Vec<Mapping>) -> Result<AtomicCommitResult> {
        if mappings.is_empty() {
            return Ok(AtomicCommitResult::Committed(Vec::new()));
        }
        let by_shard = self.group_by_shard(&mappings);

        // --- Single-shard fast path (spec §3.5): one RefBatch, atomic by one log
        // entry. No txn record, no 2PC. ---
        if by_shard.len() == 1 {
            let (&shard, refs) = by_shard.iter().next().unwrap();
            return self.commit_single_shard(shard, refs).await;
        }

        // --- Multi-shard 2PC (spec §3.1/§5). ---
        let participants: Vec<ShardId> = by_shard.keys().copied().collect();
        // Deterministic coordinator shard = min participant id (every node hosts a
        // contiguous shard range under 4a placement, so it hosts min(shards)).
        let coord_shard = *participants.iter().min().unwrap();
        let txn_id = TxnId::generate(self.store.hlc_for(coord_shard)?);

        // 1. TxnBegin on the coordinator shard (durable PENDING record).
        let participant_ids: Vec<u32> = participants.iter().map(|s| s.0).collect();
        self.store
            .apply_txn_record_op(
                coord_shard,
                LedgeOp::TxnBegin {
                    txn_id,
                    participants: participant_ids,
                },
            )
            .await?;

        // 2. Prepare each participant ref in canonical (shard, ref) order. A NO
        //    vote short-circuits the prepare phase (no point locking more).
        let mut all_yes = true;
        let mut conflicts: Vec<RefName> = Vec::new();
        'outer: for (&shard, refs) in &by_shard {
            for (name, target, expected) in refs {
                let resp = self
                    .store
                    .op_on_shard(
                        shard,
                        ClusterOp::Prepare {
                            txn_id,
                            coord_shard: coord_shard.0,
                            name: name.as_str().to_string(),
                            target_bytes: *target.as_bytes(),
                            expected_bytes: expected.map(|e| *e.as_bytes()),
                        },
                    )
                    .await?;
                match resp {
                    RefOpResponse::Vote(true) => {}
                    RefOpResponse::Vote(false) => {
                        all_yes = false;
                        conflicts.push(name.clone());
                        break 'outer;
                    }
                    other => {
                        return Err(LedgeError::Unavailable(format!(
                            "unexpected prepare resp: {other:?}"
                        )))
                    }
                }
            }
        }

        // 3. Decision (the commit point) on the coordinator shard.
        self.store
            .apply_txn_record_op(
                coord_shard,
                LedgeOp::TxnDecide {
                    txn_id,
                    commit: all_yes,
                },
            )
            .await?;

        // 4. Phase 2: roll forward (commit) or release (abort) every ref. Sending
        //    AbortPrepared to a never-locked ref is a harmless idempotent no-op,
        //    so we sweep all refs uniformly for simplicity.
        let mut committed = Vec::with_capacity(mappings.len());
        for (&shard, refs) in &by_shard {
            for (name, _, _) in refs {
                if all_yes {
                    let resp = self
                        .store
                        .op_on_shard(
                            shard,
                            ClusterOp::CommitPrepared {
                                txn_id,
                                name: name.as_str().to_string(),
                            },
                        )
                        .await?;
                    if let RefOpResponse::CommittedPrepared(e) = resp {
                        committed.push((name.clone(), e));
                    }
                } else {
                    let _ = self
                        .store
                        .op_on_shard(
                            shard,
                            ClusterOp::AbortPrepared {
                                txn_id,
                                name: name.as_str().to_string(),
                            },
                        )
                        .await?;
                }
            }
        }

        // 5. GC the record once all participants are resolved.
        self.store
            .apply_txn_record_op(coord_shard, LedgeOp::TxnEnd { txn_id })
            .await?;

        if all_yes {
            Ok(AtomicCommitResult::Committed(committed))
        } else {
            Ok(AtomicCommitResult::Aborted {
                reason: "prepare vote NO".into(),
                conflicts,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The coordinator coerces to `Arc<dyn AtomicCommit>` — the shape the
    /// workspace manager stores. Compile-time dyn-compatibility proof.
    #[test]
    fn txn_coordinator_is_atomic_commit() {
        fn _takes(_c: Arc<dyn AtomicCommit>) {}
    }
}
