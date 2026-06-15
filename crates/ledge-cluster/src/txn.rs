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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use ledge_core::{LedgeError, ObjectId, RefName, Result, TxnId};
use ledge_raft::{BatchOutcome, LedgeOp, TxnDecision};

pub use ledge_ref_store::{AtomicCommit, AtomicCommitResult, Mapping};

use crate::forward::{ClusterOp, RefOpResponse};
use crate::ref_store::ClusterRefStore;
use crate::router::ShardId;

/// Prometheus series names emitted by the coordinator/resolver at the true site
/// (spec §7). Re-declared identically in `ledge-server::metrics` so both crates
/// agree on the series (mirrors `forward::REF_OP_FORWARDED_TOTAL`). The
/// counters/histogram below are emitted only on the multi-shard 2PC path; the
/// single-shard fast path and single-node deploys never touch these series.
pub const TXN_STARTED_TOTAL: &str = "ledge_txn_started_total";
pub const TXN_COMMITTED_TOTAL: &str = "ledge_txn_committed_total";
pub const TXN_ABORTED_TOTAL: &str = "ledge_txn_aborted_total";
pub const TXN_RECOVERED_TOTAL: &str = "ledge_txn_recovered_total";
pub const TXN_PREPARE_VOTES_TOTAL: &str = "ledge_txn_prepare_votes_total";
pub const TXN_DURATION: &str = "ledge_txn_duration_seconds";

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
        let mut by: BTreeMap<ShardId, Vec<(RefName, ObjectId, Option<ObjectId>)>> = BTreeMap::new();
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
        for ((name, _, _), outcome) in refs.iter().zip(outcomes) {
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
    #[tracing::instrument(
        skip(self, mappings),
        fields(mappings = mappings.len(), txn_id, participants, decision)
    )]
    async fn commit_atomic(&self, mappings: Vec<Mapping>) -> Result<AtomicCommitResult> {
        if mappings.is_empty() {
            return Ok(AtomicCommitResult::Committed(Vec::new()));
        }
        let by_shard = self.group_by_shard(&mappings);

        // --- Single-shard fast path (spec §3.5): one RefBatch, atomic by one log
        // entry. No txn record, no 2PC. The `ledge_txn_*` series are deliberately
        // NOT emitted here — they count multi-shard 2PC transactions only. ---
        if by_shard.len() == 1 {
            let (&shard, refs) = by_shard.iter().next().unwrap();
            return self.commit_single_shard(shard, refs).await;
        }

        // --- Multi-shard 2PC (spec §3.1/§5). This is the only path that emits the
        // `ledge_txn_*` transaction metrics (spec §7). ---
        metrics::counter!(TXN_STARTED_TOTAL).increment(1);
        let started = std::time::Instant::now();
        let participants: Vec<ShardId> = by_shard.keys().copied().collect();
        // Deterministic coordinator shard = min participant id (every node hosts a
        // contiguous shard range under 4a placement, so it hosts min(shards)).
        let coord_shard = *participants.iter().min().unwrap();
        let txn_id = TxnId::generate(self.store.hlc_for(coord_shard)?);
        tracing::Span::current().record("txn_id", tracing::field::display(txn_id));
        tracing::Span::current().record("participants", participants.len());

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
                    RefOpResponse::Vote(true) => {
                        metrics::counter!(TXN_PREPARE_VOTES_TOTAL, "vote" => "yes").increment(1);
                    }
                    RefOpResponse::Vote(false) => {
                        metrics::counter!(TXN_PREPARE_VOTES_TOTAL, "vote" => "no").increment(1);
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
            metrics::counter!(TXN_COMMITTED_TOTAL).increment(1);
            tracing::Span::current().record("decision", "commit");
        } else {
            metrics::counter!(TXN_ABORTED_TOTAL, "reason" => "prepare_no").increment(1);
            tracing::Span::current().record("decision", "abort");
        }
        // Wall time of the whole multi-shard 2PC (begin → end), spec §7.
        metrics::histogram!(TXN_DURATION).record(started.elapsed().as_secs_f64());

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

/// Background sweeper that resolves orphaned (coordinator-crashed) transactions
/// by re-driving phase 2 against the durable coordinator-shard decision record
/// (spec §3.4). For every prepared lock it finds:
///
/// - **`Commit` decision** ⇒ roll FORWARD: idempotently re-issue `CommitPrepared`
///   to the participant (safe because apply is idempotent — a duplicate is a
///   benign ack).
/// - **`Abort` decision** ⇒ roll back: idempotently `AbortPrepared` (release).
/// - **`Pending` / no record, past the TTL** ⇒ **PRESUMED ABORT**: `AbortPrepared`
///   (release the no-wait lock).
///
/// Once every participant lock for a txn is resolved, the resolver records an
/// explicit `Abort` decision for presumed-abort txns (so a concurrent reader of
/// the coordinator record sees a terminal decision, not a stale `Pending`) and
/// then GCs the record with `TxnEnd`.
///
/// # Presumed-abort safety invariant (the load-bearing argument)
/// The resolver NEVER rolls a txn forward unless the coordinator shard holds a
/// durable `TxnDecide{Commit}` record. The `TxnDecide{Commit}` log entry is the
/// SOLE authorization to commit, and the coordinator only sends `CommitPrepared`
/// to any participant AFTER that entry is durable (spec §3.1). Therefore a txn
/// whose coordinator died before reaching the commit point has, by construction,
/// never told any participant to commit — so presuming abort (releasing every
/// lock) can never contradict a commit that already happened. Presumed abort
/// only ever RELEASES a lock; it never installs a staged value. The decision
/// record is the single source of truth and it is monotone (Pending → terminal),
/// so this is sound under crash + retry.
pub struct TxnResolver {
    store: Arc<ClusterRefStore>,
    /// A `Pending`/absent decision older than this is presumed-abort.
    ttl: Duration,
}

impl TxnResolver {
    /// Build over a node's cluster ref store with a default 30s presumed-abort
    /// TTL.
    pub fn new(store: Arc<ClusterRefStore>) -> Self {
        Self {
            store,
            ttl: Duration::from_secs(30),
        }
    }

    /// Override the presumed-abort TTL (tests use `Duration::ZERO` for an
    /// immediate presumed-abort).
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Scan every locally-hosted shard's prepared locks and resolve each against
    /// its coordinator-shard decision. Returns the count of locks resolved (rolled
    /// forward or released) this pass. Idempotent and retry-safe: a lock already
    /// resolved by a concurrent access or a prior pass is simply not seen again.
    pub async fn resolve_once(&self) -> Result<usize> {
        let mut resolved = 0usize;
        // Track which (txn, coord_shard) pairs were presumed-abort so we can write
        // a terminal Abort decision + GC the record after the lock sweep.
        let mut presumed_abort: BTreeSet<(TxnId, u32)> = BTreeSet::new();
        // Track every coord-shard-resolved txn whose record we should GC.
        let mut to_end: BTreeSet<(TxnId, u32)> = BTreeSet::new();

        for (shard, locks) in self.store.prepared_locks_by_shard().await? {
            for (name, intent) in locks {
                let coord_shard = ShardId(intent.coord_shard);
                let decision = self.read_decision(coord_shard, intent.txn_id).await?;

                let roll_forward = match decision {
                    Some(TxnDecision::Commit) => true,
                    Some(TxnDecision::Abort) => false,
                    // No terminal decision: presumed-abort past the TTL. With a
                    // ZERO TTL we always release; otherwise the lock's age gates
                    // it. SAFETY: this only releases — never commits — so it can
                    // never contradict a real commit (see the invariant above).
                    None | Some(TxnDecision::Pending) => {
                        if !self.lock_is_stale(&intent) {
                            continue; // too fresh; leave for a later pass
                        }
                        presumed_abort.insert((intent.txn_id, intent.coord_shard));
                        false
                    }
                };

                let op = if roll_forward {
                    ClusterOp::CommitPrepared {
                        txn_id: intent.txn_id,
                        name: name.clone(),
                    }
                } else {
                    ClusterOp::AbortPrepared {
                        txn_id: intent.txn_id,
                        name: name.clone(),
                    }
                };
                self.store.op_on_shard(shard, op).await?;
                resolved += 1;
                // One prepared lock resolved by crash recovery (rolled forward on
                // a Commit decision or released on presumed-abort), spec §7.
                metrics::counter!(TXN_RECOVERED_TOTAL).increment(1);
                to_end.insert((intent.txn_id, intent.coord_shard));
            }
        }

        // Phase 3: finalize each resolved txn's coordinator record. For a
        // presumed-abort txn, stamp a terminal Abort (monotone Pending → Abort) so
        // a concurrent reader observes a decision, not a stale Pending. Then GC the
        // record. Both ops only run where THIS node hosts the coord shard (the 4a
        // placement guarantee); a non-local coord shard is left for the node that
        // hosts it (best-effort, idempotent).
        for (txn_id, coord_shard) in to_end {
            let coord = ShardId(coord_shard);
            if !self.store.hosts_locally(coord) {
                continue;
            }
            if presumed_abort.contains(&(txn_id, coord_shard)) {
                self.store
                    .apply_txn_record_op(
                        coord,
                        LedgeOp::TxnDecide {
                            txn_id,
                            commit: false,
                        },
                    )
                    .await?;
            }
            self.store
                .apply_txn_record_op(coord, LedgeOp::TxnEnd { txn_id })
                .await?;
        }

        Ok(resolved)
    }

    /// Read the durable decision for `txn_id` from its coordinator shard
    /// (local-or-forwarded, linearizable via the `TxnStatus` op).
    async fn read_decision(
        &self,
        coord_shard: ShardId,
        txn_id: TxnId,
    ) -> Result<Option<TxnDecision>> {
        match self
            .store
            .op_on_shard(
                coord_shard,
                ClusterOp::TxnStatus {
                    txn_id,
                    coord_shard: coord_shard.0,
                },
            )
            .await?
        {
            RefOpResponse::TxnDecisionResp(d) => Ok(d),
            other => Err(LedgeError::Unavailable(format!(
                "unexpected txn-status resp: {other:?}"
            ))),
        }
    }

    /// Whether a no-/pending-decision lock is older than the presumed-abort TTL.
    /// With a ZERO TTL this is always true (immediate presumed-abort, used by
    /// tests). For a non-zero TTL we compare the lock's staged HLC physical-time
    /// component against `now - ttl`: the HLC packs wall-clock milliseconds in its
    /// high bits (`ledge_core::HLC`, layout `[63..20]=ms, [19..0]=logical`), so
    /// `staged_hlc >> HLC_LOGICAL_BITS` is the lock's creation time in ms. A lock
    /// younger than the TTL is left for a later pass (the in-doubt coordinator may
    /// still be alive and about to write its decision).
    fn lock_is_stale(&self, intent: &ledge_raft::PreparedIntent) -> bool {
        /// Logical-counter bit width of `ledge_core::HLC` (the wall-ms field
        /// starts above this). Kept in sync with `ledge_core::HLC`'s layout.
        const HLC_LOGICAL_BITS: u64 = 20;
        if self.ttl.is_zero() {
            return true;
        }
        let created_ms = intent.staged_hlc >> HLC_LOGICAL_BITS;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // Saturating: a clock that went backwards yields age 0 (not yet stale).
        let age_ms = now_ms.saturating_sub(created_ms);
        age_ms >= self.ttl.as_millis() as u64
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
