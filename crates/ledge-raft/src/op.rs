//! `LedgeOp` (Raft log command) and `LedgeResp` (applied result).

use ledge_core::{ObjectId, RefEntry, RefName};
use ledge_ref_store::{AppliedOp, AppliedOutcome};
use ledge_workspace::{id::WorkspaceId, lease::Lease};
use serde::{Deserialize, Serialize};

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
            LedgeOp::LeasePut { .. } | LedgeOp::LeaseTombstone { .. } => None,
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
        // 2PC outcomes (VoteYes/VoteNo/CommitedPrepared/AbortedPrepared) are
        // produced only by the `Prepare`/`CommitPrepared`/`AbortPrepared`
        // apply_op arms. No `LedgeOp` proposes those yet (cross-shard 2PC is
        // wired through Raft in a later Phase-4b task), so they cannot reach
        // this single-ref response mapper. The arm exists to keep the match
        // exhaustive; it is unreachable on the current code path.
        AppliedOutcome::VoteYes
        | AppliedOutcome::VoteNo
        | AppliedOutcome::CommitedPrepared(_)
        | AppliedOutcome::AbortedPrepared => {
            unreachable!("2PC AppliedOutcome reached the single-ref Raft resp mapper")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> bincode::config::Configuration {
        bincode::config::standard()
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
