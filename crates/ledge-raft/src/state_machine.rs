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

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use ledge_core::{RefEntry, RefName, TxnId, HLC};
use ledge_ref_store::RefStoreImpl;
use ledge_workspace::id::WorkspaceId;
use ledge_workspace::lease::{Lease, LeaseStore};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::op::{outcome_to_resp, LedgeOp, LedgeResp, TxnDecision};
use crate::type_config::TypeConfig;

/// Durable per-transaction record kept on the coordinator shard's state machine.
///
/// `decision` is the in-doubt/terminal status (`Pending`→`Commit`/`Abort`);
/// `participants` is the shard set the coordinator must drive to resolution.
/// Stored in a `BTreeMap` keyed by `TxnId` so snapshot bytes are deterministic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TxnRecord {
    decision: TxnDecision,
    participants: Vec<u32>,
}

/// Serializable full state, used as the snapshot payload.
///
/// `txn_records` is a sorted `Vec` (from the `BTreeMap`) so two replicas dumping
/// the same applied state emit byte-identical snapshots.
#[derive(Serialize, Deserialize, Default)]
struct StateDump {
    refs: Vec<(String, RefEntry)>,
    leases: Vec<Lease>,
    txn_records: Vec<(TxnId, TxnRecord)>,
}

/// Durable applied-state metadata, persisted on every `apply`/`install_snapshot`
/// in the disk-backed (`open`) mode. openraft consults `applied_state()` on
/// startup to learn where to resume; without this the SM would report `None`
/// after a restart and openraft would attempt to replay log entries that have
/// already been purged behind a snapshot — silently losing committed state.
#[derive(Serialize, Deserialize, Default)]
struct PersistedMeta {
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

/// File name (under `data_dir`) for the durable applied-state metadata.
const META_FILE: &str = "applied-meta";
/// File name (under `data_dir`) for the durable current snapshot.
const SNAPSHOT_FILE: &str = "snapshot";

/// A durably-persisted snapshot: its `SnapshotMeta` fields plus the raw
/// `StateDump` payload. Persisting the meta alongside the bytes lets
/// `get_current_snapshot` return a snapshot whose coverage (`last_log_id`,
/// membership, id) is exactly what it was at build/install time — NOT the
/// SM's current `last_applied_log`, which may have advanced past the snapshot.
#[derive(Serialize, Deserialize)]
struct PersistedSnapshot {
    last_log_id: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    snapshot_id: String,
    payload: Vec<u8>,
}

/// The applied stores, swapped atomically as a unit on snapshot install so a
/// shared [`ReadHandle`] always observes a consistent (refs, leases) pair and
/// never a torn mix of pre- and post-install state. `_dir` is held here so the
/// backing tempdir lives exactly as long as the stores it backs.
struct Stores {
    refs: Arc<RefStoreImpl>,
    leases: Arc<LeaseStore>,
    /// Held so the tempdir lives as long as the stores (test/in-memory mode).
    _dir: Option<TempDir>,
}

/// The Ledge state machine: applied ref + lease state plus Raft bookkeeping.
pub struct StateMachineStore {
    /// Shared, atomically-swappable stores. `Raft` owns the SM and mutates
    /// through `&mut self`, but a cloned [`ReadHandle`] shares this same cell,
    /// so tests can read each replica's applied state directly. `ArcSwap` makes
    /// the snapshot-install replacement visible to those handles.
    stores: Arc<ArcSwap<Stores>>,
    /// Durable transaction records (coordinator-shard 2PC state), keyed by
    /// `TxnId`. Wrapped in `Arc<ArcSwap<_>>` (copy-on-write) so the write path
    /// stays on `&self` and a cloned [`ReadHandle`] shares the same lock-free
    /// read cell — mirroring how `stores` is shared. Part of the SM snapshot.
    txn_records: Arc<ArcSwap<BTreeMap<TxnId, TxnRecord>>>,
    /// Last applied log id (`None` until the first entry is applied).
    last_applied_log: Option<LogId<u64>>,
    /// Last applied membership config.
    last_membership: StoredMembership<u64, BasicNode>,
    /// Monotone snapshot id counter (per-SM, for unique `snapshot_id`s).
    snapshot_idx: u64,
    /// Durable root directory. `Some` ⟺ disk-backed (`open`): applied-state
    /// metadata + the current snapshot are persisted here so the SM survives a
    /// restart even after the Raft log is purged post-snapshot. `None` ⟺ the
    /// in-memory/test (`new_temp`) path: no metadata/snapshot persistence (the
    /// wrapped stores' own tempdirs vanish on drop, as tests expect).
    durable_dir: Option<PathBuf>,
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
            stores: Arc::new(ArcSwap::from_pointee(Stores {
                refs,
                leases,
                _dir: Some(dir),
            })),
            txn_records: Arc::new(ArcSwap::from_pointee(BTreeMap::new())),
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
            durable_dir: None,
        }
    }

    /// Open a durable state machine rooted at `data_dir`. The wrapped RefStoreImpl
    /// and LeaseStore persist to `data_dir` (WAL-backed), so applied state survives
    /// restart even after the Raft log is purged post-snapshot. Also persists the
    /// last-applied log id, last-membership, and current snapshot durably so openraft
    /// can resume without replaying purged entries.
    ///
    /// # Restart-safety contract
    /// On reopen this constructor reconstructs three pieces of durable state:
    ///
    /// 1. Applied refs/leases — the wrapped `RefStoreImpl`/`LeaseStore` replay their
    ///    own WALs on `open`, so the applied ref/lease set is exactly what it was at
    ///    the last write.
    /// 2. Applied-state metadata — `last_applied_log` + `last_membership` are loaded
    ///    from `data_dir/applied-meta` so `applied_state()` returns the true resume
    ///    point (not `None`). This is what stops openraft from replaying purged log
    ///    entries.
    /// 3. Current snapshot — if a snapshot was persisted to `data_dir/snapshot`,
    ///    `get_current_snapshot()` serves it after restart so a lagging follower can
    ///    be caught up even though the covered log prefix is gone.
    ///
    /// The apply/determinism logic is unchanged; only durability of (applied-state
    /// metadata + snapshot) is added on top of the already-durable stores.
    ///
    /// # Errors
    /// Propagates store-open or metadata-read I/O / corruption errors.
    pub async fn open(data_dir: PathBuf, hlc: Arc<HLC>) -> ledge_core::Result<Self> {
        std::fs::create_dir_all(&data_dir).map_err(ledge_core::LedgeError::Io)?;
        // The wrapped stores replay their own WALs on open → durable applied state.
        let refs = Arc::new(RefStoreImpl::open(data_dir.join("refs"), hlc.clone())?);
        let leases = Arc::new(LeaseStore::open(data_dir.join("leases"), hlc)?);

        // Load the durable applied-state metadata, if present. Absent ⟺ first boot.
        let meta = Self::load_meta(&data_dir)?;

        Ok(Self {
            stores: Arc::new(ArcSwap::from_pointee(Stores {
                refs,
                leases,
                _dir: None, // durable: no tempdir; the stores own real paths.
            })),
            // Reconstructed from {snapshot restore} + {post-snapshot log replay}
            // by openraft on resume; starts empty on a plain (no-snapshot) reopen.
            txn_records: Arc::new(ArcSwap::from_pointee(BTreeMap::new())),
            last_applied_log: meta.last_applied_log,
            last_membership: meta.last_membership,
            snapshot_idx: 0,
            durable_dir: Some(data_dir),
        })
    }

    /// Load persisted applied-state metadata from `dir/applied-meta`.
    /// A missing file yields the default (first-boot) meta. A present-but-corrupt
    /// file is a hard error: silently resetting to `None` would re-introduce the
    /// very replay-of-purged-entries bug this durability layer prevents.
    fn load_meta(dir: &Path) -> ledge_core::Result<PersistedMeta> {
        let path = dir.join(META_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let (meta, _): (PersistedMeta, _) =
                    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                        .map_err(|e| {
                            ledge_core::LedgeError::Corruption(format!("applied-meta decode: {e}"))
                        })?;
                Ok(meta)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(PersistedMeta::default()),
            Err(e) => Err(ledge_core::LedgeError::Io(e)),
        }
    }

    /// Durably persist `last_applied_log` + `last_membership` to `dir/applied-meta`.
    ///
    /// Written via a temp-file + atomic rename so a crash mid-write never leaves a
    /// torn metadata file (which `load_meta` would reject as corrupt). No-op in the
    /// in-memory (`new_temp`) path. Bincode keeps the record tiny (tens of bytes).
    fn persist_meta(&self) -> ledge_core::Result<()> {
        let Some(dir) = self.durable_dir.as_ref() else {
            return Ok(());
        };
        let meta = PersistedMeta {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
        };
        let bytes = bincode::serde::encode_to_vec(&meta, bincode::config::standard())
            .map_err(|e| ledge_core::LedgeError::Corruption(format!("applied-meta encode: {e}")))?;
        Self::atomic_write(&dir.join(META_FILE), &bytes)
    }

    /// Durably persist a snapshot (its meta + payload) to `dir/snapshot`, so
    /// `get_current_snapshot()` can serve it after a restart even though the
    /// covered log prefix has been purged. No-op in the in-memory path.
    fn persist_snapshot(
        dir: Option<&Path>,
        last_log_id: Option<LogId<u64>>,
        last_membership: &StoredMembership<u64, BasicNode>,
        snapshot_id: &str,
        payload: &[u8],
    ) -> ledge_core::Result<()> {
        let Some(dir) = dir else {
            return Ok(());
        };
        let rec = PersistedSnapshot {
            last_log_id,
            last_membership: last_membership.clone(),
            snapshot_id: snapshot_id.to_string(),
            payload: payload.to_vec(),
        };
        let bytes = bincode::serde::encode_to_vec(&rec, bincode::config::standard())
            .map_err(|e| ledge_core::LedgeError::Corruption(format!("snapshot encode: {e}")))?;
        Self::atomic_write(&dir.join(SNAPSHOT_FILE), &bytes)
    }

    /// Load the persisted snapshot, if any (durable path only). A missing file
    /// yields `None`; a corrupt file is a hard error (callers must not silently
    /// serve a torn snapshot).
    fn load_snapshot(&self) -> ledge_core::Result<Option<PersistedSnapshot>> {
        let Some(dir) = self.durable_dir.as_ref() else {
            return Ok(None);
        };
        match std::fs::read(dir.join(SNAPSHOT_FILE)) {
            Ok(bytes) => {
                let (rec, _): (PersistedSnapshot, _) =
                    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                        .map_err(|e| {
                            ledge_core::LedgeError::Corruption(format!("snapshot decode: {e}"))
                        })?;
                Ok(Some(rec))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ledge_core::LedgeError::Io(e)),
        }
    }

    /// Atomically write `bytes` to `path` via a sibling temp file + rename, so a
    /// reader (or a crash) never observes a partially-written file.
    fn atomic_write(path: &Path, bytes: &[u8]) -> ledge_core::Result<()> {
        use std::io::Write;
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp).map_err(ledge_core::LedgeError::Io)?;
            f.write_all(bytes).map_err(ledge_core::LedgeError::Io)?;
            f.flush().map_err(ledge_core::LedgeError::Io)?;
            f.sync_all().map_err(ledge_core::LedgeError::Io)?;
        }
        std::fs::rename(&tmp, path).map_err(ledge_core::LedgeError::Io)?;
        Ok(())
    }

    /// A cloneable, read-only handle onto this SM's applied state. The handle
    /// shares the same `ArcSwap` cell as the SM, so reads observe every applied
    /// entry and survive a snapshot-install store swap. Used by cluster tests to
    /// assert per-replica convergence without routing through Raft.
    pub fn read_handle(&self) -> ReadHandle {
        ReadHandle {
            stores: self.stores.clone(),
            txn_records: self.txn_records.clone(),
        }
    }

    /// Current durable decision for `txn_id`, or `None` if no record exists
    /// (`None` ⟹ presumed-abort by recovery). Lock-free `ArcSwap` load.
    pub fn txn_decision(&self, txn_id: TxnId) -> Option<TxnDecision> {
        self.txn_records.load().get(&txn_id).map(|r| r.decision)
    }

    /// Snapshot the current stores (a cheap `ArcSwap` load + `Arc` clones).
    fn refs_arc(&self) -> Arc<RefStoreImpl> {
        self.stores.load().refs.clone()
    }

    fn leases_arc(&self) -> Arc<LeaseStore> {
        self.stores.load().leases.clone()
    }

    /// Test/read helper: current ref entry.
    pub async fn refs_get(&self, name: &RefName) -> Option<RefEntry> {
        use ledge_core::RefStore;
        self.refs_arc().get(name).await.unwrap()
    }

    /// Test/read helper: live leases (all non-expired-at-0, i.e. effectively all).
    pub async fn leases_all(&self) -> Vec<Lease> {
        self.leases_arc().live(0).await.unwrap_or_default()
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
                outcome_to_resp(self.refs_arc().apply_op(&applied).await)
            }
            LedgeOp::LeasePut { lease } => {
                self.leases_arc()
                    .put(lease.clone())
                    .await
                    .expect("lease put");
                LedgeResp::LeaseOk
            }
            LedgeOp::LeaseTombstone { id, hlc } => {
                // Use the op-carried hlc (leader-assigned at propose time), NOT a
                // local `self.hlc.tick()`, so every replica records the identical
                // tombstone frame — mirroring the ref path's explicit-hlc
                // determinism. (LeasePut above already applies the full Lease
                // verbatim, including its own leader-assigned hlc.)
                self.leases_arc()
                    .tombstone_with_hlc(*id, *hlc)
                    .await
                    .expect("lease tombstone");
                LedgeResp::LeaseOk
            }
            LedgeOp::TxnBegin {
                txn_id,
                participants,
            } => {
                // Copy-on-write insert: clone the map, add the Pending record,
                // publish atomically so a concurrent ReadHandle sees all-or-nothing.
                self.txn_records.rcu(|m| {
                    let mut m = (**m).clone();
                    m.insert(
                        *txn_id,
                        TxnRecord {
                            decision: TxnDecision::Pending,
                            participants: participants.clone(),
                        },
                    );
                    m
                });
                LedgeResp::TxnState(Some(TxnDecision::Pending))
            }
            LedgeOp::TxnDecide { txn_id, commit } => {
                let decision = if *commit {
                    TxnDecision::Commit
                } else {
                    TxnDecision::Abort
                };
                self.txn_records.rcu(|m| {
                    let mut m = (**m).clone();
                    // Commit point. If the record is missing (recovery/duplicate),
                    // create it with empty participants so the durable decision
                    // still lands.
                    m.entry(*txn_id)
                        .and_modify(|r| r.decision = decision)
                        .or_insert_with(|| TxnRecord {
                            decision,
                            participants: Vec::new(),
                        });
                    m
                });
                LedgeResp::TxnState(Some(decision))
            }
            LedgeOp::TxnEnd { txn_id } => {
                self.txn_records.rcu(|m| {
                    let mut m = (**m).clone();
                    m.remove(txn_id);
                    m
                });
                LedgeResp::TxnState(None)
            }
            // Ref-2PC routing is wired in the next step.
            LedgeOp::RefPrepare { .. }
            | LedgeOp::RefCommitPrepared { .. }
            | LedgeOp::RefAbortPrepared { .. }
            | LedgeOp::RefBatch { .. } => {
                unreachable!("ref-2PC op routing not yet wired into apply_one")
            }
        }
    }

    /// Serialize the full applied state for a snapshot.
    async fn dump(&self) -> Vec<u8> {
        use ledge_core::RefStore;
        let refs_arc = self.refs_arc();
        let snap = refs_arc.snapshot();
        let refs: Vec<(String, RefEntry)> = snap
            .list("")
            .into_iter()
            .map(|(n, e)| (n.as_str().to_string(), e))
            .collect();
        let leases = self.leases_arc().live(0).await.unwrap_or_default();
        // BTreeMap::iter yields keys in sorted order ⇒ deterministic dump bytes.
        let txn_records: Vec<(TxnId, TxnRecord)> = self
            .txn_records
            .load()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let dump = StateDump {
            refs,
            leases,
            txn_records,
        };
        bincode::serde::encode_to_vec(&dump, bincode::config::standard()).unwrap()
    }

    /// Rebuild the stores from a snapshot payload (fresh tempdir-backed stores).
    ///
    /// # Determinism
    /// Refs are installed via `RefStoreImpl::restore_from`, which inserts each
    /// `RefEntry` VERBATIM — preserving `target`, `hlc`, AND `version`. Replaying
    /// via `apply_op(Update, expected: None)` would reset `version` to 1, so a
    /// node that installed a snapshot then served a CAS update would diverge in
    /// `version` from a node that replayed the log. `restore_from` closes that
    /// gap: the snapshot install path is byte-identical to the log-replay path.
    async fn restore(&mut self, bytes: &[u8]) {
        let (dump, _): (StateDump, _) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let hlc = Arc::new(HLC::new());
        let refs = Arc::new(RefStoreImpl::open(dir.path().to_path_buf(), hlc.clone()).unwrap());
        let leases = Arc::new(LeaseStore::open(dir.path().to_path_buf(), hlc).unwrap());
        // Restore the FULL RefEntry set verbatim — version preserved.
        let entries: Vec<(RefName, RefEntry)> = dump
            .refs
            .into_iter()
            .map(|(name, entry)| (RefName::new(&name).expect("snapshot ref name valid"), entry))
            .collect();
        refs.restore_from(entries).await.expect("restore refs");
        // Leases carry their own hlc + generation; `put` stores them verbatim.
        for lease in dump.leases {
            leases.put(lease).await.expect("restore lease");
        }
        // Atomically swap the whole store set so any shared ReadHandle observes
        // the restored state as one consistent unit, never a torn pair.
        self.stores.store(Arc::new(Stores {
            refs,
            leases,
            _dir: Some(dir),
        }));
        // Rebuild the durable txn records and publish atomically. A durable
        // COMMIT/ABORT decision must survive snapshot install (recovery rolls
        // forward/back from it), so it rides in the snapshot payload.
        let mut map = BTreeMap::new();
        for (k, v) in dump.txn_records {
            map.insert(k, v);
        }
        self.txn_records.store(Arc::new(map));
    }
}

/// A cloneable, read-only view of a [`StateMachineStore`]'s applied state.
///
/// Shares the SM's `ArcSwap<Stores>` cell, so it always reflects the latest
/// applied refs/leases — including after a snapshot install swaps the backing
/// stores. Cluster tests hold one per replica to assert convergence directly.
#[derive(Clone)]
pub struct ReadHandle {
    stores: Arc<ArcSwap<Stores>>,
    /// Shares the SM's durable txn-record cell so coordinators/resolvers can
    /// read a decision without a Raft round-trip (lock-free `ArcSwap` load).
    txn_records: Arc<ArcSwap<BTreeMap<TxnId, TxnRecord>>>,
}

impl ReadHandle {
    /// Current durable decision for `txn_id`, or `None` if absent (presumed-abort).
    pub fn txn_decision(&self, txn_id: TxnId) -> Option<TxnDecision> {
        self.txn_records.load().get(&txn_id).map(|r| r.decision)
    }

    /// Current applied ref entry for `name`, or `None` if absent.
    pub async fn applied_ref(&self, name: &str) -> Option<RefEntry> {
        use ledge_core::RefStore;
        let refs = self.stores.load().refs.clone();
        let rn = RefName::new(name).ok()?;
        refs.get(&rn).await.unwrap()
    }

    /// All currently-live leases.
    pub async fn applied_leases(&self) -> Vec<Lease> {
        let leases = self.stores.load().leases.clone();
        leases.live(0).await.unwrap_or_default()
    }

    /// All applied refs whose name starts with `prefix`, as `(RefName, RefEntry)`
    /// pairs. Backs the clustered `list`/`snapshot` fan-out: a cheap single
    /// `ArcSwap` load + ART prefix scan over the local applied state.
    pub async fn applied_refs_with_prefix(&self, prefix: &str) -> Vec<(RefName, RefEntry)> {
        use ledge_core::RefStore;
        let refs = self.stores.load().refs.clone();
        refs.list(prefix).await.unwrap_or_default()
    }

    /// The full applied ref map (every ref → entry). Used by the clustered
    /// snapshot merge; equivalent to `applied_refs_with_prefix("")`.
    pub async fn applied_ref_map(&self) -> Vec<(RefName, RefEntry)> {
        self.applied_refs_with_prefix("").await
    }

    /// The full applied ref map, read SYNCHRONOUSLY via the ref store's
    /// lock-free `snapshot()` (an O(1) atomic load + sync prefix scan). This
    /// backs the trait's sync `RefStore::snapshot()`, which cannot `.await`.
    pub fn applied_ref_map_sync(&self) -> Vec<(RefName, RefEntry)> {
        use ledge_core::RefStore;
        let refs = self.stores.load().refs.clone();
        refs.snapshot().list("")
    }

    /// The applied lease for `id`, or `None` if absent/tombstoned. Reads the
    /// local applied lease index directly (no Raft round-trip).
    pub async fn applied_lease(&self, id: WorkspaceId) -> Option<Lease> {
        let leases = self.stores.load().leases.clone();
        leases.get(id).await.unwrap_or(None)
    }

    /// All applied leases live at `now_ms` (expiry strictly after `now_ms`).
    pub async fn applied_leases_live(&self, now_ms: u64) -> Vec<Lease> {
        let leases = self.stores.load().leases.clone();
        leases.live(now_ms).await.unwrap_or_default()
    }

    /// All applied leases expired at `now_ms` (expiry at or before `now_ms`).
    pub async fn applied_leases_expired(&self, now_ms: u64) -> Vec<Lease> {
        let leases = self.stores.load().leases.clone();
        leases.expired(now_ms).await.unwrap_or_default()
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
    /// Point-in-time snapshot of the durable txn records (cheap `ArcSwap` load),
    /// captured at build-request time so the builder is self-contained.
    txn_records: Arc<BTreeMap<TxnId, TxnRecord>>,
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    snapshot_id: String,
    /// Durable root dir (disk-backed SM only). When `Some`, `build_snapshot`
    /// persists the freshly-built snapshot to `dir/snapshot` so it survives a
    /// restart and can still be served once the covered log prefix is purged.
    durable_dir: Option<PathBuf>,
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
        let txn_records: Vec<(TxnId, TxnRecord)> = self
            .txn_records
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let dump = StateDump {
            refs,
            leases,
            txn_records,
        };
        bincode::serde::encode_to_vec(&dump, bincode::config::standard()).unwrap()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for LedgeSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let bytes = self.dump().await;
        // Persist the built snapshot durably (disk-backed SM only) so a restart
        // can still serve it after the covered Raft log prefix is purged.
        StateMachineStore::persist_snapshot(
            self.durable_dir.as_deref(),
            self.last_applied_log,
            &self.last_membership,
            &self.snapshot_id,
            &bytes,
        )
        .map_err(|e| StorageError::from(StorageIOError::write_snapshot(None, &e)))?;
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
                    // Leader's no-op heartbeat entry; no state change, no-op ack.
                    responses.push(LedgeResp::Noop);
                }
                EntryPayload::Normal(op) => {
                    responses.push(self.apply_one(&op).await);
                }
                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), m);
                    // Membership entries carry no application result; no-op ack.
                    responses.push(LedgeResp::Noop);
                }
            }
        }
        // Durably record the new resume point (disk-backed SM only; no-op for
        // `new_temp`). The wrapped stores already fsync'd their own WALs above;
        // this small atomic write keeps `applied_state()` correct across restart
        // so openraft never replays purged-then-snapshotted entries.
        self.persist_meta()
            .map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        let snapshot_id = match self.last_applied_log {
            Some(log_id) => format!("{}-{}-{}", log_id.leader_id, log_id.index, self.snapshot_idx),
            None => format!("--{}", self.snapshot_idx),
        };
        LedgeSnapshotBuilder {
            refs: self.refs_arc(),
            leases: self.leases_arc(),
            txn_records: self.txn_records.load_full(),
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id,
            durable_dir: self.durable_dir.clone(),
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
        // Persist the installed snapshot + the new resume point durably (disk-backed
        // SM only) so a subsequent restart serves this snapshot and resumes at the
        // right log id without replaying the now-superseded prefix.
        Self::persist_snapshot(
            self.durable_dir.as_deref(),
            meta.last_log_id,
            &meta.last_membership,
            &meta.snapshot_id,
            snapshot.get_ref(),
        )
        .map_err(|e| StorageError::from(StorageIOError::write_snapshot(Some(meta.signature()), &e)))?;
        self.persist_meta()
            .map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        // Disk-backed SM: if a snapshot was previously built/installed, serve THAT
        // persisted snapshot (with its own coverage meta), so a restart can satisfy
        // an InstallSnapshot for a lagging follower even though the covered log
        // prefix has been purged. Falls through to a fresh dump only when none was
        // ever persisted (or in the in-memory `new_temp` path).
        if let Some(rec) = self
            .load_snapshot()
            .map_err(|e| StorageError::from(StorageIOError::read_snapshot(None, &e)))?
        {
            let meta = SnapshotMeta {
                last_log_id: rec.last_log_id,
                last_membership: rec.last_membership,
                snapshot_id: rec.snapshot_id,
            };
            return Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(rec.payload)),
            }));
        }

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

    use crate::op::TxnDecision;
    use ledge_core::TxnId;

    #[tokio::test]
    async fn txn_record_lifecycle_begin_decide_end() {
        let mut sm = StateMachineStore::new_temp().await;
        let txn = TxnId::from_bytes([1u8; 16]);

        let r = sm
            .apply([entry(
                1,
                1,
                LedgeOp::TxnBegin {
                    txn_id: txn,
                    participants: vec![0, 1],
                },
            )])
            .await
            .unwrap();
        assert_eq!(r, vec![LedgeResp::TxnState(Some(TxnDecision::Pending))]);
        assert_eq!(sm.txn_decision(txn), Some(TxnDecision::Pending));
        assert_eq!(sm.read_handle().txn_decision(txn), Some(TxnDecision::Pending));

        let r = sm
            .apply([entry(
                1,
                2,
                LedgeOp::TxnDecide {
                    txn_id: txn,
                    commit: true,
                },
            )])
            .await
            .unwrap();
        assert_eq!(r, vec![LedgeResp::TxnState(Some(TxnDecision::Commit))]);
        assert_eq!(
            sm.txn_decision(txn),
            Some(TxnDecision::Commit),
            "TxnDecide is the commit point"
        );

        let r = sm
            .apply([entry(1, 3, LedgeOp::TxnEnd { txn_id: txn })])
            .await
            .unwrap();
        assert_eq!(r, vec![LedgeResp::TxnState(None)]);
        assert_eq!(sm.txn_decision(txn), None, "TxnEnd GCs the record");
    }

    #[tokio::test]
    async fn txn_decide_abort_records_abort() {
        let mut sm = StateMachineStore::new_temp().await;
        let txn = TxnId::from_bytes([2u8; 16]);
        sm.apply([entry(
            1,
            1,
            LedgeOp::TxnBegin {
                txn_id: txn,
                participants: vec![5],
            },
        )])
        .await
        .unwrap();
        let r = sm
            .apply([entry(
                1,
                2,
                LedgeOp::TxnDecide {
                    txn_id: txn,
                    commit: false,
                },
            )])
            .await
            .unwrap();
        assert_eq!(r, vec![LedgeResp::TxnState(Some(TxnDecision::Abort))]);
        assert_eq!(sm.txn_decision(txn), Some(TxnDecision::Abort));
    }

    #[tokio::test]
    async fn txn_decide_without_begin_creates_durable_decision() {
        // Recovery/duplicate: a TxnDecide with no prior record still lands the
        // durable decision (empty participants).
        let mut sm = StateMachineStore::new_temp().await;
        let txn = TxnId::from_bytes([3u8; 16]);
        let r = sm
            .apply([entry(
                1,
                1,
                LedgeOp::TxnDecide {
                    txn_id: txn,
                    commit: true,
                },
            )])
            .await
            .unwrap();
        assert_eq!(r, vec![LedgeResp::TxnState(Some(TxnDecision::Commit))]);
        assert_eq!(sm.txn_decision(txn), Some(TxnDecision::Commit));
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

    /// Regression guard: the snapshot install path must preserve `version`.
    /// Drive a ref to version=3, snapshot, install into a fresh SM, and assert
    /// the restored entry reproduces version AND hlc AND target exactly. A
    /// node that installs this snapshot must agree byte-for-byte with one that
    /// replayed the log — otherwise a subsequent CAS update diverges in version.
    #[tokio::test]
    async fn snapshot_install_preserves_full_ref_entry_including_version() {
        let mut src = StateMachineStore::new_temp().await;
        // Three successful CAS updates → version reaches 3.
        let seq = vec![
            LedgeOp::RefUpdate {
                name: "refs/heads/v".into(),
                target_bytes: [1u8; 32],
                expected_bytes: None,
                hlc: 10,
            },
            LedgeOp::RefUpdate {
                name: "refs/heads/v".into(),
                target_bytes: [2u8; 32],
                expected_bytes: Some([1u8; 32]),
                hlc: 20,
            },
            LedgeOp::RefUpdate {
                name: "refs/heads/v".into(),
                target_bytes: [3u8; 32],
                expected_bytes: Some([2u8; 32]),
                hlc: 30,
            },
        ];
        let _ = src.apply(entries_for(seq)).await.unwrap();

        let v = RefName::new("refs/heads/v").unwrap();
        let src_entry = src.refs_get(&v).await.unwrap();
        assert_eq!(src_entry.version, 3, "precondition: source reached version 3");

        let mut builder = src.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        let mut dst = StateMachineStore::new_temp().await;
        dst.install_snapshot(&snap.meta, snap.snapshot).await.unwrap();

        let dst_entry = dst.refs_get(&v).await.unwrap();
        assert_eq!(dst_entry.version, 3, "snapshot install must preserve version (not reset to 1)");
        assert_eq!(dst_entry.hlc, 30, "snapshot install preserves hlc");
        assert_eq!(dst_entry.target, ledge_core::ObjectId::from_bytes([3u8; 32]));
        assert_eq!(dst_entry, src_entry, "full RefEntry reproduced byte-for-byte");

        // And a subsequent CAS update on the restored node lands at version 4 —
        // exactly as a log-replay node would, proving no divergence.
        let next = entry(
            1,
            4,
            LedgeOp::RefUpdate {
                name: "refs/heads/v".into(),
                target_bytes: [4u8; 32],
                expected_bytes: Some([3u8; 32]),
                hlc: 40,
            },
        );
        let resp = dst.apply([next]).await.unwrap();
        match &resp[0] {
            LedgeResp::RefUpdated(e) => assert_eq!(e.version, 4, "CAS after install continues version"),
            other => panic!("expected RefUpdated, got {other:?}"),
        }
    }

    /// RESTART DURABILITY (the core fix). Open a disk-backed SM, apply RefUpdate
    /// entries, record the last-applied log id, drop the SM, then reopen at the
    /// SAME dir. After reopen: (1) the applied refs are present via ReadHandle,
    /// and (2) `applied_state()` returns the SAME last_applied_log (not None) —
    /// so openraft resumes at the right point instead of replaying purged entries.
    #[tokio::test]
    async fn open_persists_applied_state_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        let last_before;
        {
            let mut sm = StateMachineStore::open(data_dir.clone(), Arc::new(HLC::new()))
                .await
                .unwrap();
            // Fresh boot: no applied state yet.
            assert!(
                sm.applied_state().await.unwrap().0.is_none(),
                "fresh durable SM has no last_applied_log"
            );
            let _ = sm.apply(entries_for(ops())).await.unwrap();
            last_before = sm.applied_state().await.unwrap().0;
            assert_eq!(last_before.unwrap().index, 3, "three entries applied");
            // Read the applied ref through the SHARED handle (no Raft round-trip).
            let read = sm.read_handle();
            let main = read.applied_ref("refs/heads/main").await.expect("ref present");
            assert_eq!(main.target, ledge_core::ObjectId::from_bytes([2u8; 32]));
            assert_eq!(main.hlc, 200);
        } // drop ⇒ all files closed/flushed.

        // Reopen at the same dir: applied refs AND applied-state metadata survive.
        let mut sm2 = StateMachineStore::open(data_dir, Arc::new(HLC::new()))
            .await
            .unwrap();
        let main = sm2
            .read_handle()
            .applied_ref("refs/heads/main")
            .await
            .expect("applied ref durable across reopen");
        assert_eq!(main.target, ledge_core::ObjectId::from_bytes([2u8; 32]));
        assert_eq!(main.hlc, 200, "applied hlc durable");

        let last_after = sm2.applied_state().await.unwrap().0;
        assert_eq!(
            last_after, last_before,
            "applied_state().last_applied_log must survive restart (else openraft replays purged log)"
        );
        assert_eq!(last_after.unwrap().index, 3);
    }

    /// SNAPSHOT DURABILITY. Build a snapshot in a disk-backed SM, drop it, reopen
    /// at the same dir, and assert `get_current_snapshot()` returns that snapshot
    /// (correct meta + payload) — so openraft can serve it to a lagging follower
    /// after a restart even though the covered log prefix has been purged.
    #[tokio::test]
    async fn open_snapshot_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        let (built_id, built_last_log);
        {
            let mut sm = StateMachineStore::open(data_dir.clone(), Arc::new(HLC::new()))
                .await
                .unwrap();
            let _ = sm.apply(entries_for(ops())).await.unwrap();
            let mut builder = sm.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.unwrap();
            built_id = snap.meta.snapshot_id.clone();
            built_last_log = snap.meta.last_log_id;
            assert_eq!(built_last_log.unwrap().index, 3);
        } // drop ⇒ persisted snapshot file is closed.

        let mut sm2 = StateMachineStore::open(data_dir, Arc::new(HLC::new()))
            .await
            .unwrap();
        let got = sm2
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("persisted snapshot served after reopen");
        assert_eq!(
            got.meta.snapshot_id, built_id,
            "get_current_snapshot returns the persisted snapshot's id"
        );
        assert_eq!(
            got.meta.last_log_id, built_last_log,
            "persisted snapshot retains its coverage meta"
        );

        // The payload must reconstruct the applied refs: install it into a fresh
        // in-memory SM and verify the ref is reproduced byte-for-byte.
        let mut dst = StateMachineStore::new_temp().await;
        dst.install_snapshot(&got.meta, got.snapshot).await.unwrap();
        let main = RefName::new("refs/heads/main").unwrap();
        assert_eq!(
            dst.refs_get(&main).await.unwrap().target,
            ledge_core::ObjectId::from_bytes([2u8; 32]),
            "persisted snapshot payload reconstructs applied state"
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

    /// LEASE APPLY DETERMINISM. Two fresh replicas applying the IDENTICAL lease
    /// op sequence (LeasePut then LeaseTombstone, with fixed hlcs) must reach
    /// identical queryable lease state — proving no `self.hlc.tick()` leaks into
    /// the replicated lease apply path (which would make the recorded tombstone
    /// hlc replica-dependent). The op-carried hlc is the sole source of truth.
    #[tokio::test]
    async fn lease_tombstone_apply_is_deterministic_across_replicas() {
        use ledge_workspace::id::WorkspaceId;

        let id = WorkspaceId::from_bytes([3u8; 16]);
        let lease = Lease {
            id,
            source_refs: vec!["refs/heads/main".into()],
            created_at_ms: 1,
            expires_at_ms: 10_000,
            hlc: 5,
            generation: 1,
        };
        // Fixed sequence: identical for both replicas, identical hlcs.
        let seq = vec![
            LedgeOp::LeasePut {
                lease: lease.clone(),
            },
            LedgeOp::LeaseTombstone { id, hlc: 42 },
        ];

        let mut sm_a = StateMachineStore::new_temp().await;
        let mut sm_b = StateMachineStore::new_temp().await;

        let resp_a = sm_a.apply(entries_for(seq.clone())).await.unwrap();
        let resp_b = sm_b.apply(entries_for(seq)).await.unwrap();

        // Same log prefix → identical responses.
        assert_eq!(resp_a, resp_b);
        assert_eq!(resp_a, vec![LedgeResp::LeaseOk, LedgeResp::LeaseOk]);

        // Both replicas have the lease absent (tombstoned) and identical
        // queryable state — no replica-local hlc divergence.
        assert!(sm_a.read_handle().applied_lease(id).await.is_none());
        assert!(sm_b.read_handle().applied_lease(id).await.is_none());
        assert_eq!(sm_a.leases_all().await, sm_b.leases_all().await);
        assert_eq!(sm_a.leases_all().await.len(), 0);
    }

    /// Applying the SAME LeaseTombstone op (same id, same hlc) to two fresh SMs
    /// yields identical lease-absent state on both — the tombstone is a pure
    /// function of the op, with no internal clock tick.
    #[tokio::test]
    async fn identical_lease_tombstone_op_yields_identical_state() {
        use ledge_workspace::id::WorkspaceId;

        let id = WorkspaceId::from_bytes([8u8; 16]);
        let tomb = LedgeOp::LeaseTombstone { id, hlc: 7 };

        let mut sm_a = StateMachineStore::new_temp().await;
        let mut sm_b = StateMachineStore::new_temp().await;

        let r_a = sm_a.apply([entry(1, 1, tomb.clone())]).await.unwrap();
        let r_b = sm_b.apply([entry(1, 1, tomb)]).await.unwrap();

        assert_eq!(r_a, r_b);
        assert_eq!(r_a, vec![LedgeResp::LeaseOk]);
        assert!(sm_a.read_handle().applied_lease(id).await.is_none());
        assert!(sm_b.read_handle().applied_lease(id).await.is_none());
    }
}
