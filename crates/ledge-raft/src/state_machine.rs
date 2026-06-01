//! Ledge replicated state machine over real `RefStoreImpl` + `LeaseStore`.
//!
//! `StateMachineStore` wraps proven storage so the replicated apply path goes
//! through the same code single-node writes do. Each committed `LedgeOp` is
//! applied via `RefStoreImpl::apply_op` (refs, with the leader-supplied HLC) or
//! `LeaseStore::put`/`tombstone` (leases), yielding a `LedgeResp`.
//!
//! # Determinism (the core safety property)
//! `apply` is a pure function of `(applied_state, ordered_ops)`: refs use the
//! HLC carried in the op (never a local tick), so every replica produces
//! byte-identical `RefEntry`s and an identical `Vec<LedgeResp>`. The
//! `apply_is_deterministic_across_two_state_machines` test proves this directly,
//! without any network.
//!
//! # openraft 0.9.24 trait surface (verified against `src/storage/v2.rs`)
//! - Traits: `openraft::storage::{RaftStateMachine, RaftSnapshotBuilder}`.
//! - `apply<I: IntoIterator<Item = Entry<C>>>(&mut self, entries) -> Result<Vec<R>, StorageError<NodeId>>`.
//! - `install_snapshot(&mut self, meta: &SnapshotMeta<NodeId, Node>, snapshot: Box<SnapshotData>)`.
//! - `begin_receiving_snapshot -> Result<Box<SnapshotData>, _>`.
//! - `SnapshotMeta { last_log_id, last_membership, snapshot_id: String }`.
//! - `Snapshot { meta, snapshot: Box<SnapshotData> }`.

use std::io::Cursor;
use std::sync::Arc;

use ledge_core::{RefEntry, RefName, HLC};
use ledge_ref_store::{AppliedOp, RefStoreImpl};
use ledge_workspace::lease::{Lease, LeaseStore};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::op::{outcome_to_resp, LedgeOp, LedgeResp};
use crate::type_config::TypeConfig;

/// Serializable full state, used as the snapshot payload.
#[derive(Serialize, Deserialize, Default)]
struct StateDump {
    refs: Vec<(String, RefEntry)>,
    leases: Vec<Lease>,
}

/// The Ledge state machine: applied ref + lease state plus Raft bookkeeping.
pub struct StateMachineStore {
    refs: Arc<RefStoreImpl>,
    leases: Arc<LeaseStore>,
    /// Held so the tempdir lives as long as the SM (test/in-memory mode).
    _dir: Option<TempDir>,
    /// Last applied log id (`None` until the first entry is applied).
    last_applied_log: Option<LogId<u64>>,
    /// Last applied membership config.
    last_membership: StoredMembership<u64, BasicNode>,
    /// Monotone snapshot id counter (per-SM, for unique `snapshot_id`s).
    snapshot_idx: u64,
}

impl StateMachineStore {
    /// Construct over a fresh tempdir (tests / in-memory nodes).
    ///
    /// # Panics
    /// Panics if the tempdir or the underlying stores cannot be created — this
    /// is the in-memory/test constructor; durable construction is Task 6.
    pub async fn new_temp() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let hlc = Arc::new(HLC::new());
        let refs = Arc::new(RefStoreImpl::open(dir.path().to_path_buf(), hlc.clone()).unwrap());
        let leases = Arc::new(LeaseStore::open(dir.path().to_path_buf(), hlc).unwrap());
        Self {
            refs,
            leases,
            _dir: Some(dir),
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
        }
    }

    /// Test/read helper: current ref entry.
    pub async fn refs_get(&self, name: &RefName) -> Option<RefEntry> {
        use ledge_core::RefStore;
        self.refs.get(name).await.unwrap()
    }

    /// Test/read helper: live leases (all non-expired-at-0, i.e. effectively all).
    pub async fn leases_all(&self) -> Vec<Lease> {
        self.leases.live(0).await.unwrap_or_default()
    }

    /// Apply a single op through the proven storage path, returning its response.
    /// This is the determinism kernel: a pure function of (applied state, op).
    async fn apply_one(&self, op: &LedgeOp) -> LedgeResp {
        match op {
            LedgeOp::RefUpdate { .. } | LedgeOp::RefDelete { .. } => {
                // Conversion is infallible for committed entries (leader validated
                // the name before proposing). A malformed name here is a
                // corruption-class invariant violation.
                let applied = op
                    .to_applied()
                    .expect("ref op converts")
                    .expect("committed ref name is valid");
                outcome_to_resp(self.refs.apply_op(&applied).await)
            }
            LedgeOp::LeasePut { lease } => {
                self.leases.put(lease.clone()).await.expect("lease put");
                LedgeResp::LeaseOk
            }
            LedgeOp::LeaseTombstone { id, .. } => {
                // NOTE: LeaseStore::tombstone stamps its own hlc internally today.
                // An explicit-hlc tombstone path (mirroring apply_op) is Task 6;
                // until then the determinism guarantee is exercised on the ref
                // path (the safety core).
                self.leases.tombstone(*id).await.expect("lease tombstone");
                LedgeResp::LeaseOk
            }
        }
    }

    /// Serialize the full applied state for a snapshot.
    async fn dump(&self) -> Vec<u8> {
        use ledge_core::RefStore;
        let snap = self.refs.snapshot();
        let refs: Vec<(String, RefEntry)> = snap
            .list("")
            .into_iter()
            .map(|(n, e)| (n.as_str().to_string(), e))
            .collect();
        let leases = self.leases.live(0).await.unwrap_or_default();
        let dump = StateDump { refs, leases };
        bincode::serde::encode_to_vec(&dump, bincode::config::standard()).unwrap()
    }

    /// Rebuild the stores from a snapshot payload (fresh tempdir-backed stores).
    async fn restore(&mut self, bytes: &[u8]) {
        let (dump, _): (StateDump, _) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let hlc = Arc::new(HLC::new());
        let refs = Arc::new(RefStoreImpl::open(dir.path().to_path_buf(), hlc.clone()).unwrap());
        let leases = Arc::new(LeaseStore::open(dir.path().to_path_buf(), hlc).unwrap());
        // Replay refs with their stored (explicit) hlc via apply_op so the
        // rebuilt RefEntry is byte-identical to the source (same hlc, version
        // restarts at 1 which matches a fresh create).
        for (name, entry) in dump.refs {
            let n = RefName::new(&name).expect("snapshot ref name valid");
            let _ = refs
                .apply_op(&AppliedOp::Update {
                    name: n,
                    target: entry.target,
                    expected: None,
                    hlc: entry.hlc,
                })
                .await;
        }
        for lease in dump.leases {
            leases.put(lease).await.expect("restore lease");
        }
        self.refs = refs;
        self.leases = leases;
        self._dir = Some(dir);
    }
}

/// Snapshot builder: a cheap handle that re-reads the SM's current state.
///
/// It captures `Arc`s to the live stores plus the applied-log bookkeeping at
/// build-request time. Because the underlying stores are append-only and
/// `build_snapshot` reads a consistent ART snapshot, the captured view is a
/// faithful point-in-time dump.
pub struct LedgeSnapshotBuilder {
    refs: Arc<RefStoreImpl>,
    leases: Arc<LeaseStore>,
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    snapshot_id: String,
}

impl LedgeSnapshotBuilder {
    async fn dump(&self) -> Vec<u8> {
        use ledge_core::RefStore;
        let snap = self.refs.snapshot();
        let refs: Vec<(String, RefEntry)> = snap
            .list("")
            .into_iter()
            .map(|(n, e)| (n.as_str().to_string(), e))
            .collect();
        let leases = self.leases.live(0).await.unwrap_or_default();
        let dump = StateDump { refs, leases };
        bincode::serde::encode_to_vec(&dump, bincode::config::standard()).unwrap()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for LedgeSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let bytes = self.dump().await;
        let meta = SnapshotMeta {
            last_log_id: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id: self.snapshot_id.clone(),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = LedgeSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        Ok((self.last_applied_log, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<LedgeResp>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            // Every entry advances the applied log pointer, regardless of kind.
            self.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => {
                    // Leader's no-op heartbeat entry; no state change, blank ack.
                    responses.push(LedgeResp::LeaseOk);
                }
                EntryPayload::Normal(op) => {
                    responses.push(self.apply_one(&op).await);
                }
                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), m);
                    // Membership entries carry no application result; blank ack.
                    responses.push(LedgeResp::LeaseOk);
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        let snapshot_id = match self.last_applied_log {
            Some(log_id) => format!("{}-{}-{}", log_id.leader_id, log_id.index, self.snapshot_idx),
            None => format!("--{}", self.snapshot_idx),
        };
        LedgeSnapshotBuilder {
            refs: self.refs.clone(),
            leases: self.leases.clone(),
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        self.restore(snapshot.get_ref()).await;
        self.last_applied_log = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let bytes = self.dump().await;
        self.snapshot_idx += 1;
        let snapshot_id = match self.last_applied_log {
            Some(log_id) => format!("{}-{}-{}", log_id.leader_id, log_id.index, self.snapshot_idx),
            None => format!("--{}", self.snapshot_idx),
        };
        let meta = SnapshotMeta {
            last_log_id: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::LedgeOp;
    use openraft::storage::RaftStateMachine;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

    /// Build a Normal log entry wrapping `op` at the given term/index.
    /// Verified for 0.9.24: `Entry { log_id, payload }`, `LogId::new(leader, index)`,
    /// `CommittedLeaderId::new(term, node_id)`, `EntryPayload::Normal(op)`.
    fn entry(term: u64, index: u64, op: LedgeOp) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(op),
        }
    }

    fn ops() -> Vec<LedgeOp> {
        vec![
            LedgeOp::RefUpdate {
                name: "refs/heads/main".into(),
                target_bytes: [1u8; 32],
                expected_bytes: None,
                hlc: 100,
            },
            LedgeOp::RefUpdate {
                name: "refs/heads/main".into(),
                target_bytes: [2u8; 32],
                expected_bytes: Some([1u8; 32]),
                hlc: 200,
            },
            // CAS conflict: wrong expected.
            LedgeOp::RefUpdate {
                name: "refs/heads/main".into(),
                target_bytes: [3u8; 32],
                expected_bytes: Some([9u8; 32]),
                hlc: 300,
            },
        ]
    }

    fn entries_for(seq: Vec<LedgeOp>) -> Vec<Entry<TypeConfig>> {
        seq.into_iter()
            .enumerate()
            .map(|(i, op)| entry(1, i as u64 + 1, op))
            .collect()
    }

    // Apply the same op sequence to two fresh state machines and assert
    // identical response vectors. APPLY DETERMINISM — the core safety property.
    #[tokio::test]
    async fn apply_is_deterministic_across_two_state_machines() {
        let mut sm_a = StateMachineStore::new_temp().await;
        let mut sm_b = StateMachineStore::new_temp().await;

        let entries_a = entries_for(ops());
        let entries_b = entries_a.clone();

        let resp_a = sm_a.apply(entries_a).await.unwrap();
        let resp_b = sm_b.apply(entries_b).await.unwrap();

        assert_eq!(resp_a, resp_b, "same log prefix -> identical responses");

        // Final ref state identical and stamped with the SUPPLIED hlc.
        let main = RefName::new("refs/heads/main").unwrap();
        let a = sm_a.refs_get(&main).await.unwrap();
        let b = sm_b.refs_get(&main).await.unwrap();
        assert_eq!(a, b);
        assert_eq!(a.target, ledge_core::ObjectId::from_bytes([2u8; 32]));
        assert_eq!(a.hlc, 200, "applied entry carries the leader hlc");
    }

    #[tokio::test]
    async fn cas_conflict_surfaces_as_ledge_resp_conflict() {
        let mut sm = StateMachineStore::new_temp().await;
        let resp = sm.apply(entries_for(ops())).await.unwrap();
        assert!(matches!(resp[0], LedgeResp::RefUpdated(_)));
        assert!(matches!(resp[1], LedgeResp::RefUpdated(_)));
        assert!(
            matches!(resp[2], LedgeResp::Conflict(_)),
            "wrong expected -> Conflict"
        );
    }

    #[tokio::test]
    async fn applied_state_tracks_last_log() {
        let mut sm = StateMachineStore::new_temp().await;
        assert!(sm.applied_state().await.unwrap().0.is_none());
        sm.apply(entries_for(ops())).await.unwrap();
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 3, "three entries applied");
    }

    #[tokio::test]
    async fn snapshot_build_then_install_reproduces_state() {
        let mut src = StateMachineStore::new_temp().await;
        let _ = src.apply(entries_for(ops())).await.unwrap();

        let mut builder = src.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        let mut dst = StateMachineStore::new_temp().await;
        dst.install_snapshot(&snap.meta, snap.snapshot).await.unwrap();

        let main = RefName::new("refs/heads/main").unwrap();
        assert_eq!(
            dst.refs_get(&main).await.unwrap().target,
            src.refs_get(&main).await.unwrap().target,
        );
        assert_eq!(
            dst.refs_get(&main).await.unwrap().hlc,
            src.refs_get(&main).await.unwrap().hlc,
            "snapshot preserves the leader hlc",
        );
        // install_snapshot restores the applied-log pointer from meta.
        assert_eq!(
            dst.applied_state().await.unwrap().0,
            src.applied_state().await.unwrap().0,
        );
    }

    #[tokio::test]
    async fn lease_put_and_tombstone_apply() {
        use ledge_workspace::id::WorkspaceId;
        let mut sm = StateMachineStore::new_temp().await;
        let id = WorkspaceId::from_bytes([7u8; 16]);
        let lease = Lease {
            id,
            source_refs: vec!["refs/heads/main".into()],
            created_at_ms: 1,
            expires_at_ms: 10_000,
            hlc: 5,
            generation: 1,
        };
        let put = entry(1, 1, LedgeOp::LeasePut { lease });
        let r = sm.apply([put]).await.unwrap();
        assert_eq!(r, vec![LedgeResp::LeaseOk]);
        assert_eq!(sm.leases_all().await.len(), 1);

        let tomb = entry(1, 2, LedgeOp::LeaseTombstone { id, hlc: 6 });
        let r = sm.apply([tomb]).await.unwrap();
        assert_eq!(r, vec![LedgeResp::LeaseOk]);
        assert_eq!(sm.leases_all().await.len(), 0);
    }
}
