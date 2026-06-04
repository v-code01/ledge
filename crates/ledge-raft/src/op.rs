//! `LedgeOp` (Raft log command) and `LedgeResp` (applied result).

use ledge_core::{ObjectId, RefEntry, RefName, TxnId};
use ledge_ref_store::{AppliedOp, AppliedOutcome};
use ledge_workspace::{id::WorkspaceId, lease::Lease};
use serde::{Deserialize, Serialize};

/// One ref CAS within a single-shard atomic [`LedgeOp::RefBatch`]. Wire form
/// mirrors `RefUpdate` (String name, raw `[u8; 32]` ids) for serde-trivial
/// replication; converted to `RefName`/`ObjectId` at apply time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchOp {
    pub name: String,
    pub target: [u8; 32],
    pub expected: Option<[u8; 32]>,
    pub hlc: u64,
}

/// A replicable Ledge mutation. The HLC is leader-assigned at propose time and
/// carried here so every replica applies the identical timestamp. Object ids are
/// stored as raw 32-byte arrays for a stable, serde-trivial wire form.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgeOp {
    /// Create-or-update a ref under CAS, stamping the entry with `hlc`.
    RefUpdate {
        name: String,
        target_bytes: [u8; 32],
        expected_bytes: Option<[u8; 32]>,
        hlc: u64,
    },
    /// Delete a ref under CAS; `hlc` records the tombstone time.
    RefDelete {
        name: String,
        expected_bytes: [u8; 32],
        hlc: u64,
    },
    /// Lease upsert; the full lease is serialized into the op (it already carries
    /// its own leader-assigned hlc + generation).
    LeasePut { lease: Lease },
    /// Lease tombstone by workspace id.
    LeaseTombstone { id: WorkspaceId, hlc: u64 },

    /// Phase-1 2PC prepare: vote-yes + take a no-wait lock iff the CAS holds and
    /// the ref is not already prepared; else vote-no. Carries `coord_shard` so the
    /// participant can locate the coordinator's durable decision during recovery.
    RefPrepare {
        txn_id: TxnId,
        coord_shard: u32,
        name: String,
        target: [u8; 32],
        expected: Option<[u8; 32]>,
        hlc: u64,
    },
    /// Phase-2 2PC commit: roll the prepared intent forward (promote staged value,
    /// release lock). Idempotent.
    RefCommitPrepared { txn_id: TxnId, name: String },
    /// Phase-2 2PC abort: release the prepared lock without applying. Idempotent.
    RefAbortPrepared { txn_id: TxnId, name: String },
    /// Single-shard atomic multi-ref CAS (all-or-nothing in one ART-root swap).
    RefBatch { ops: Vec<BatchOp> },
    /// Coordinator-shard: open a transaction record in `Pending` state.
    TxnBegin { txn_id: TxnId, participants: Vec<u32> },
    /// Coordinator-shard COMMIT POINT: set the durable decision (commit/abort).
    TxnDecide { txn_id: TxnId, commit: bool },
    /// Coordinator-shard: GC the transaction record once all participants resolved.
    TxnEnd { txn_id: TxnId },
}

/// Per-ref result of a single-shard atomic [`LedgeOp::RefBatch`]. `Ok` carries the
/// newly-committed entry; `Conflict` carries the current committed entry of a ref
/// that blocked the (all-or-nothing) batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchOutcome {
    Ok(RefEntry),
    Conflict(RefEntry),
}

/// Durable transaction decision recorded on the coordinator shard.
///
/// `Pending` ⟹ in-doubt; `Commit`/`Abort` are terminal. Presumed-abort: the
/// ABSENCE of a record (`TxnState(None)`) is treated as Abort by recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnDecision {
    Pending,
    Commit,
    Abort,
}

/// The applied result returned through `client_write`. Mirrors `AppliedOutcome`
/// for refs, plus the lease ack.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgeResp {
    /// Ref was created or updated; carries the committed entry.
    RefUpdated(RefEntry),
    /// CAS precondition failed; carries the current entry observed at apply.
    Conflict(RefEntry),
    /// Target ref did not exist for an update-with-expected or a delete.
    NotFound,
    /// Ref was deleted.
    Deleted,
    /// A lease op (put/tombstone) was applied.
    LeaseOk,
    /// A Raft no-op entry (`Blank` leader heartbeat or `Membership` change) was
    /// applied. Carries no application result; distinct from `LeaseOk` so the
    /// wire result is not misattributed to a lease.
    Noop,

    /// `RefPrepare` vote: `true` ⟹ YES (lock taken), `false` ⟹ NO (no lock).
    Vote(bool),
    /// `RefBatch` per-ref outcomes, in input order.
    BatchResult(Vec<BatchOutcome>),
    /// Durable transaction-record query/mutation result. `None` ⟹ no record
    /// (presumed-abort), `Some(decision)` ⟹ the current durable decision.
    TxnState(Option<TxnDecision>),
    /// `RefCommitPrepared` promoted the staged value; carries the new committed entry.
    CommittedPrepared(RefEntry),
    /// `RefAbortPrepared` released the prepared lock (or was an idempotent no-op).
    AbortedPrepared,
}

impl LedgeOp {
    /// Convert a ref op into the storage-primitive `AppliedOp`. Lease ops return
    /// `None` (they are applied via `LeaseStore`, not `apply_op`).
    ///
    /// # Errors / fallibility
    /// `RefName::new` can reject a malformed name; since the leader validated the
    /// name before proposing, a malformed name in a committed entry is a
    /// corruption-class invariant violation — the state machine treats it as a
    /// hard error (see `StateMachineStore::apply_one`).
    pub fn to_applied(&self) -> Option<Result<AppliedOp, ledge_core::LedgeError>> {
        match self {
            LedgeOp::RefUpdate {
                name,
                target_bytes,
                expected_bytes,
                hlc,
            } => Some(RefName::new(name).map(|n| AppliedOp::Update {
                name: n,
                target: ObjectId::from_bytes(*target_bytes),
                expected: expected_bytes.map(ObjectId::from_bytes),
                hlc: *hlc,
            })),
            LedgeOp::RefDelete {
                name,
                expected_bytes,
                hlc,
            } => Some(RefName::new(name).map(|n| AppliedOp::Delete {
                name: n,
                expected: ObjectId::from_bytes(*expected_bytes),
                hlc: *hlc,
            })),
            LedgeOp::RefPrepare {
                txn_id,
                coord_shard,
                name,
                target,
                expected,
                hlc,
            } => Some(RefName::new(name).map(|n| AppliedOp::Prepare {
                txn_id: *txn_id,
                coord_shard: *coord_shard,
                name: n,
                target: ObjectId::from_bytes(*target),
                expected: expected.map(ObjectId::from_bytes),
                hlc: *hlc,
            })),
            LedgeOp::RefCommitPrepared { txn_id, name } => Some(
                RefName::new(name).map(|n| AppliedOp::CommitPrepared {
                    txn_id: *txn_id,
                    name: n,
                }),
            ),
            LedgeOp::RefAbortPrepared { txn_id, name } => Some(
                RefName::new(name).map(|n| AppliedOp::AbortPrepared {
                    txn_id: *txn_id,
                    name: n,
                }),
            ),
            // Lease ops apply via `LeaseStore`; the batch + txn-record ops apply
            // directly in `apply_one` (no single `AppliedOp` equivalent).
            LedgeOp::LeasePut { .. }
            | LedgeOp::LeaseTombstone { .. }
            | LedgeOp::RefBatch { .. }
            | LedgeOp::TxnBegin { .. }
            | LedgeOp::TxnDecide { .. }
            | LedgeOp::TxnEnd { .. } => None,
        }
    }
}

/// Map an `AppliedOutcome` to the wire `LedgeResp`.
pub fn outcome_to_resp(outcome: AppliedOutcome) -> LedgeResp {
    match outcome {
        AppliedOutcome::Updated(e) => LedgeResp::RefUpdated(e),
        AppliedOutcome::Conflict(e) => LedgeResp::Conflict(e),
        AppliedOutcome::NotFound => LedgeResp::NotFound,
        AppliedOutcome::Deleted => LedgeResp::Deleted,
        // 2PC outcomes. The `apply_one` Prepare/Commit/Abort arms also match these
        // directly, but the mapping is centralized here (single source of truth)
        // so both paths agree. Note Section 1's `CommitedPrepared` spelling (one
        // `t`) maps to the `LedgeResp::CommittedPrepared` (two `t`) wire variant.
        AppliedOutcome::VoteYes => LedgeResp::Vote(true),
        AppliedOutcome::VoteNo => LedgeResp::Vote(false),
        AppliedOutcome::CommitedPrepared(e) => LedgeResp::CommittedPrepared(e),
        AppliedOutcome::AbortedPrepared => LedgeResp::AbortedPrepared,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> bincode::config::Configuration {
        bincode::config::standard()
    }

    #[test]
    fn ref_2pc_ops_convert_to_applied() {
        let txn = TxnId::from_bytes([1u8; 16]);
        let p = LedgeOp::RefPrepare {
            txn_id: txn,
            coord_shard: 3,
            name: "refs/heads/m".into(),
            target: [8u8; 32],
            expected: Some([9u8; 32]),
            hlc: 77,
        };
        match p.to_applied().unwrap().unwrap() {
            AppliedOp::Prepare {
                txn_id,
                coord_shard,
                name,
                target,
                expected,
                hlc,
            } => {
                assert_eq!(txn_id, txn);
                assert_eq!(coord_shard, 3);
                assert_eq!(name.as_str(), "refs/heads/m");
                assert_eq!(target, ObjectId::from_bytes([8u8; 32]));
                assert_eq!(expected, Some(ObjectId::from_bytes([9u8; 32])));
                assert_eq!(hlc, 77);
            }
            other => panic!("expected Prepare, got {other:?}"),
        }

        match (LedgeOp::RefCommitPrepared {
            txn_id: txn,
            name: "refs/heads/m".into(),
        })
        .to_applied()
        .unwrap()
        .unwrap()
        {
            AppliedOp::CommitPrepared { txn_id, name } => {
                assert_eq!(txn_id, txn);
                assert_eq!(name.as_str(), "refs/heads/m");
            }
            other => panic!("expected CommitPrepared, got {other:?}"),
        }

        match (LedgeOp::RefAbortPrepared {
            txn_id: txn,
            name: "refs/heads/m".into(),
        })
        .to_applied()
        .unwrap()
        .unwrap()
        {
            AppliedOp::AbortPrepared { txn_id, name } => {
                assert_eq!(txn_id, txn);
                assert_eq!(name.as_str(), "refs/heads/m");
            }
            other => panic!("expected AbortPrepared, got {other:?}"),
        }

        // Batch + txn-record ops have no single AppliedOp equivalent.
        assert!(LedgeOp::RefBatch { ops: vec![] }.to_applied().is_none());
        assert!(LedgeOp::TxnBegin {
            txn_id: txn,
            participants: vec![]
        }
        .to_applied()
        .is_none());
        assert!(LedgeOp::TxnDecide {
            txn_id: txn,
            commit: true
        }
        .to_applied()
        .is_none());
        assert!(LedgeOp::TxnEnd { txn_id: txn }.to_applied().is_none());
    }

    #[test]
    fn outcome_to_resp_maps_2pc_outcomes() {
        let e = RefEntry {
            target: ObjectId::from_bytes([5u8; 32]),
            hlc: 1,
            version: 1,
        };
        assert_eq!(outcome_to_resp(AppliedOutcome::VoteYes), LedgeResp::Vote(true));
        assert_eq!(outcome_to_resp(AppliedOutcome::VoteNo), LedgeResp::Vote(false));
        assert_eq!(
            outcome_to_resp(AppliedOutcome::CommitedPrepared(e.clone())),
            LedgeResp::CommittedPrepared(e)
        );
        assert_eq!(
            outcome_to_resp(AppliedOutcome::AbortedPrepared),
            LedgeResp::AbortedPrepared
        );
    }

    #[test]
    fn ledge_resp_2pc_variants_serde_roundtrip() {
        let e = RefEntry {
            target: ObjectId::from_bytes([1u8; 32]),
            hlc: 9,
            version: 2,
        };
        let resps = vec![
            LedgeResp::Vote(true),
            LedgeResp::Vote(false),
            LedgeResp::BatchResult(vec![
                BatchOutcome::Ok(e.clone()),
                BatchOutcome::Conflict(e.clone()),
            ]),
            LedgeResp::TxnState(Some(TxnDecision::Pending)),
            LedgeResp::TxnState(Some(TxnDecision::Commit)),
            LedgeResp::TxnState(Some(TxnDecision::Abort)),
            LedgeResp::TxnState(None),
            LedgeResp::CommittedPrepared(e.clone()),
            LedgeResp::AbortedPrepared,
        ];
        for r in resps {
            let bytes = bincode::serde::encode_to_vec(&r, cfg()).unwrap();
            let (back, _): (LedgeResp, _) =
                bincode::serde::decode_from_slice(&bytes, cfg()).unwrap();
            assert_eq!(r, back);
        }
    }

    #[test]
    fn ledge_op_2pc_variants_serde_roundtrip() {
        let txn = TxnId::from_bytes([7u8; 16]);
        let ops = vec![
            LedgeOp::RefPrepare {
                txn_id: txn,
                coord_shard: 0,
                name: "refs/heads/main".into(),
                target: [1u8; 32],
                expected: Some([2u8; 32]),
                hlc: 10,
            },
            LedgeOp::RefCommitPrepared {
                txn_id: txn,
                name: "refs/heads/main".into(),
            },
            LedgeOp::RefAbortPrepared {
                txn_id: txn,
                name: "refs/heads/main".into(),
            },
            LedgeOp::RefBatch {
                ops: vec![
                    BatchOp {
                        name: "refs/heads/a".into(),
                        target: [3u8; 32],
                        expected: None,
                        hlc: 11,
                    },
                    BatchOp {
                        name: "refs/heads/b".into(),
                        target: [4u8; 32],
                        expected: Some([5u8; 32]),
                        hlc: 12,
                    },
                ],
            },
            LedgeOp::TxnBegin {
                txn_id: txn,
                participants: vec![0, 1],
            },
            LedgeOp::TxnDecide {
                txn_id: txn,
                commit: true,
            },
            LedgeOp::TxnEnd { txn_id: txn },
        ];
        for op in ops {
            let bytes = bincode::serde::encode_to_vec(&op, cfg()).unwrap();
            let (back, _): (LedgeOp, _) =
                bincode::serde::decode_from_slice(&bytes, cfg()).unwrap();
            assert_eq!(op, back);
        }
    }

    #[test]
    fn ledge_op_serde_roundtrip_all_variants() {
        let ops = vec![
            LedgeOp::RefUpdate {
                name: "refs/heads/main".into(),
                target_bytes: [1u8; 32],
                expected_bytes: Some([2u8; 32]),
                hlc: 42,
            },
            LedgeOp::RefDelete {
                name: "refs/heads/x".into(),
                expected_bytes: [3u8; 32],
                hlc: 7,
            },
            LedgeOp::LeaseTombstone {
                id: WorkspaceId::from_bytes([9u8; 16]),
                hlc: 99,
            },
            LedgeOp::LeasePut {
                lease: Lease {
                    id: WorkspaceId::from_bytes([4u8; 16]),
                    source_refs: vec!["refs/heads/main".into()],
                    created_at_ms: 10,
                    expires_at_ms: 1000,
                    hlc: 55,
                    generation: 1,
                },
            },
        ];
        for op in ops {
            let bytes = bincode::serde::encode_to_vec(&op, cfg()).unwrap();
            let (back, _): (LedgeOp, _) = bincode::serde::decode_from_slice(&bytes, cfg()).unwrap();
            assert_eq!(op, back);
        }
    }

    #[test]
    fn ref_update_converts_to_applied_update() {
        let op = LedgeOp::RefUpdate {
            name: "refs/heads/main".into(),
            target_bytes: [5u8; 32],
            expected_bytes: None,
            hlc: 1234,
        };
        let applied = op.to_applied().unwrap().unwrap();
        match applied {
            AppliedOp::Update {
                name,
                target,
                expected,
                hlc,
            } => {
                assert_eq!(name.as_str(), "refs/heads/main");
                assert_eq!(target, ObjectId::from_bytes([5u8; 32]));
                assert_eq!(expected, None);
                assert_eq!(hlc, 1234);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn ref_delete_converts_to_applied_delete() {
        let op = LedgeOp::RefDelete {
            name: "refs/heads/x".into(),
            expected_bytes: [7u8; 32],
            hlc: 9,
        };
        let applied = op.to_applied().unwrap().unwrap();
        match applied {
            AppliedOp::Delete {
                name,
                expected,
                hlc,
            } => {
                assert_eq!(name.as_str(), "refs/heads/x");
                assert_eq!(expected, ObjectId::from_bytes([7u8; 32]));
                assert_eq!(hlc, 9);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn lease_ops_have_no_applied_op() {
        let op = LedgeOp::LeaseTombstone {
            id: WorkspaceId::from_bytes([0u8; 16]),
            hlc: 1,
        };
        assert!(op.to_applied().is_none());
    }
}
