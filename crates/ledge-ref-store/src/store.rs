//! `RefStoreImpl` — the production ref store.
//!
//! # Architecture
//! The live ref namespace is held in an `ArcSwap<Option<Arc<ArtNode>>>`.
//! Every mutation executes a CAS loop:
//!
//! 1. Load the current root (snapshot).
//! 2. Validate optimistic-concurrency preconditions against the snapshot.
//! 3. Produce a new root via the ART's pure copy-on-write insert/delete helpers.
//! 4. Attempt to swap the root atom; on failure (concurrent writer), restart.
//! 5. Durably log the committed entry to the WAL.
//!
//! # O(1) snapshot
//! `snapshot()` calls `ArcSwap::load_full()` — one atomic load — and wraps
//! the root in an `ArtSnapshot`.  No copying, no locking.
//!
//! # WAL replay on open
//! `open()` reads the WAL, replays `Checkpoint` + subsequent `Update`/`Delete`
//! entries in order, and arrives at the correct in-memory state before
//! accepting any new writes.

use std::path::PathBuf;
use std::sync::{Arc, Weak};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use ledge_core::{
    LedgeError, ObjectId, RefEntry, RefName, RefSnapshot, RefStore, Result, TxnId, HLC,
};
use tracing::{debug, instrument, warn};

use crate::art::{art_delete, art_insert, art_lookup, art_prefix_iter, ArtNode};
use crate::slot::{PreparedIntent, RefSlot};
use crate::snapshot::ArtSnapshot;
use crate::wal::{Wal, WalEntry};

// ---------------------------------------------------------------------------
// RefStoreImpl
// ---------------------------------------------------------------------------

/// Production ref store implementation.
///
/// Thread-safe and lock-free for reads.  Writes serialise their WAL appends
/// through `Wal`'s internal `tokio::sync::Mutex`, but the ART swap itself is
/// always attempted lock-free via the CAS loop.
pub struct RefStoreImpl {
    /// Current ART root, wrapped in `Option` (None ⟺ empty namespace).
    root: ArcSwap<Option<Arc<ArtNode>>>,
    /// Shared hybrid logical clock for stamping new entries.
    hlc: Arc<HLC>,
    /// Write-ahead log for durability.
    wal: Arc<Wal>,
    /// WAL size threshold (bytes) above which background compaction fires.
    wal_compact_threshold_bytes: u64,
    /// Data directory (unused after open, kept for diagnostics).
    #[allow(dead_code)]
    data_dir: PathBuf,
}

impl RefStoreImpl {
    /// Open (or create) the ref store rooted at `data_dir`.
    ///
    /// Creates `data_dir/refs/` if absent, opens the WAL at
    /// `data_dir/refs/wal`, replays it to reconstruct in-memory state,
    /// and returns a ready-to-use store.
    ///
    /// Uses a default compaction threshold of 64 MiB.
    ///
    /// # Errors
    /// Propagates any WAL I/O or corruption error.
    pub fn open(data_dir: PathBuf, hlc: Arc<HLC>) -> Result<Self> {
        Self::open_with_compaction_threshold(data_dir, hlc, 64 * 1024 * 1024)
    }

    /// Open (or create) the ref store with an explicit WAL compaction threshold.
    ///
    /// The background compaction task (launched via `spawn_compaction_task`) will
    /// trigger `compact_wal()` whenever the WAL file exceeds `threshold_bytes`.
    ///
    /// # Errors
    /// Propagates any WAL I/O or corruption error.
    pub fn open_with_compaction_threshold(
        data_dir: PathBuf,
        hlc: Arc<HLC>,
        threshold_bytes: u64,
    ) -> Result<Self> {
        let refs_dir = data_dir.join("refs");
        std::fs::create_dir_all(&refs_dir).map_err(LedgeError::Io)?;

        let (wal, entries) = Wal::open(refs_dir.join("wal"))?;

        // Replay WAL entries to reconstruct the in-memory ART.
        let mut root: Option<Arc<ArtNode>> = None;
        for entry in entries {
            match entry {
                WalEntry::Checkpoint { leaves } => {
                    // Checkpoint replaces all prior state with its full snapshot.
                    root = None;
                    for (name, ref_entry) in leaves {
                        root = Some(art_insert(
                            root,
                            name.as_bytes(),
                            RefSlot::committed(ref_entry),
                            0,
                        ));
                    }
                }
                WalEntry::Update {
                    name,
                    entry: ref_entry,
                } => {
                    root = Some(art_insert(
                        root,
                        name.as_bytes(),
                        RefSlot::committed(ref_entry),
                        0,
                    ));
                }
                WalEntry::Delete { name, .. } => {
                    if let Some(r) = root.take() {
                        root = art_delete(r, name.as_bytes(), 0);
                    }
                }
                WalEntry::Batch { updates } => {
                    // Atomic multi-ref commit: apply every update in order. The
                    // frame is present in full or not at all (length+CRC guard),
                    // so this never applies a partial batch.
                    for (name, ref_entry) in updates {
                        root = Some(art_insert(
                            root,
                            name.as_bytes(),
                            RefSlot::committed(ref_entry),
                            0,
                        ));
                    }
                }
            }
        }

        Ok(RefStoreImpl {
            root: ArcSwap::new(Arc::new(root)),
            hlc,
            wal: Arc::new(wal),
            wal_compact_threshold_bytes: threshold_bytes,
            data_dir,
        })
    }

    /// The shared HLC source this store stamps committed entries with. Exposed so
    /// the single-node atomic-commit path can tick a per-op timestamp before
    /// calling [`commit_batch`](Self::commit_batch), which takes caller-supplied
    /// HLCs (the same contract the replicated apply path uses).
    pub fn hlc(&self) -> &Arc<HLC> {
        &self.hlc
    }

    /// Compact the WAL by snapshotting all current refs into a single
    /// `Checkpoint` frame, truncating everything before it.
    ///
    /// Subsequent `append()` calls land after the checkpoint, so on the next
    /// `open()` only the checkpoint plus subsequent entries are replayed.
    ///
    /// # Errors
    /// Propagates WAL encode or I/O errors.
    pub async fn compact_wal(&self) -> Result<()> {
        // Snapshot the leaves UNDER the WAL lock (compact_with runs this closure
        // while holding it). A concurrent update() publishes to the root before
        // it appends, so any writer is either already visible to this snapshot
        // or blocked to land after the new checkpoint — never erased by the
        // whole-file replacement. See Wal::compact_with for the full analysis.
        self.wal
            .compact_with(|| {
                let snap = self.snapshot();
                snap.list("")
                    .into_iter()
                    .map(|(name, entry)| (name.as_str().to_string(), entry))
                    .collect()
            })
            .await
    }

    /// Test-only: arm a one-shot WAL append failure (delegates to the WAL) so
    /// durability-failure paths — `CommitBatchError::NotDurable`, single-node
    /// propagation — can be driven deterministically from any in-crate test.
    #[cfg(test)]
    pub(crate) fn fail_next_wal_append(&self) {
        self.wal.fail_next_append();
    }

    /// Replace the entire ref set with exact entries (target, hlc, version
    /// preserved). Used by Raft snapshot install — versions must NOT be reset.
    /// Rebuilds the ART atomically (`ArcSwap`) and writes a WAL `Checkpoint` of
    /// the new state so the restored state is durable across reopen.
    ///
    /// Unlike `apply_op`/`update`, this does NOT recompute `version` or stamp a
    /// fresh `hlc`: each provided `RefEntry` is inserted verbatim. This is the
    /// determinism-preserving snapshot install path — a node that installs a
    /// snapshot then serves a CAS update must agree byte-for-byte (including
    /// `version`) with a node that replayed the log to the same point.
    ///
    /// # Atomicity
    /// The fresh root is built off to the side and swapped in with a single
    /// `ArcSwap::store`. The WAL checkpoint is written first so a crash between
    /// the checkpoint write and the in-memory swap recovers to the new state on
    /// the next `open()` (the checkpoint is the durable source of truth).
    ///
    /// # Errors
    /// Propagates WAL encode or I/O errors from the checkpoint write.
    pub async fn restore_from(&self, entries: Vec<(RefName, RefEntry)>) -> Result<()> {
        // Build the durable checkpoint payload (name -> exact RefEntry).
        let leaves: Vec<(String, RefEntry)> = entries
            .iter()
            .map(|(name, entry)| (name.as_str().to_string(), entry.clone()))
            .collect();

        // Write the checkpoint to the WAL first so the restored state is durable
        // even if we crash before the in-memory swap below.
        self.wal.compact(leaves).await?;

        // Build a fresh ART containing exactly the provided (name -> RefEntry)
        // pairs, inserting each RefEntry VERBATIM (no version recompute / hlc tick).
        let mut root: Option<Arc<ArtNode>> = None;
        for (name, entry) in entries {
            debug_assert!(
                entry.version >= 1,
                "restored version invariant: version >= 1"
            );
            root = Some(art_insert(
                root,
                name.as_str().as_bytes(),
                RefSlot::committed(entry),
                0,
            ));
        }

        // Atomically publish the new root.
        self.root.store(Arc::new(root));
        Ok(())
    }

    /// Spawn a background tokio task that periodically checks the WAL size
    /// and calls `compact_wal()` when it exceeds the configured threshold.
    ///
    /// Uses a `Weak` reference so the task exits automatically once the store
    /// is dropped — no explicit cancellation needed.
    ///
    /// The task polls every 100 ms.
    pub fn spawn_compaction_task(self: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let Some(store) = weak.upgrade() else {
                    // Store has been dropped; exit the task.
                    break;
                };
                if store.wal.file_size_bytes() > store.wal_compact_threshold_bytes {
                    if let Err(e) = store.compact_wal().await {
                        warn!("background WAL compaction failed: {e:?}");
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// AppliedOp / AppliedOutcome — deterministic, pre-resolved replicated ops
// ---------------------------------------------------------------------------

/// A pre-resolved, replicable ref operation.
///
/// Unlike `RefStore::update`/`delete`, the HLC is **caller-supplied** (assigned
/// by the Raft leader at propose time and carried in the log entry), so every
/// replica applying the same committed log prefix produces byte-identical state.
/// The ledge-raft `LedgeOp` converts into this at apply time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppliedOp {
    /// Create-or-update under CAS, stamping the entry with `hlc`.
    Update {
        name: RefName,
        target: ObjectId,
        expected: Option<ObjectId>,
        hlc: u64,
    },
    /// Delete under CAS; `hlc` records the tombstone time in the WAL.
    Delete {
        name: RefName,
        expected: ObjectId,
        hlc: u64,
    },

    /// Phase-1 2PC: vote-yes + take a no-wait lock iff the CAS precondition holds
    /// and the ref is not already prepared by another txn; else vote-no (no lock).
    Prepare {
        txn_id: TxnId,
        coord_shard: u32,
        name: RefName,
        target: ObjectId,
        expected: Option<ObjectId>,
        hlc: u64,
    },
    /// Roll a prepared intent forward: replace committed with the staged value
    /// (version+1, staged hlc) and release the lock. Idempotent.
    CommitPrepared { txn_id: TxnId, name: RefName },
    /// Release a prepared intent without applying it. Idempotent.
    AbortPrepared { txn_id: TxnId, name: RefName },
}

/// The deterministic result of applying an `AppliedOp`.
///
/// This is the canonical per-op outcome the Raft state machine maps to
/// `LedgeResp`. It mirrors the success/`Conflict`/`NotFound` cases of the
/// single-node `update`/`delete` but as a value (no `Result`) because the
/// state machine must surface every outcome to the client through consensus.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppliedOutcome {
    /// Ref was created or updated; carries the committed entry.
    Updated(RefEntry),
    /// CAS precondition failed; carries the current entry observed at apply.
    Conflict(RefEntry),
    /// Target ref did not exist for an update-with-expected or a delete.
    NotFound,
    /// Ref was deleted.
    Deleted,

    /// Prepare succeeded: lock taken, vote yes.
    VoteYes,
    /// Prepare refused: precondition failed or ref already locked. No lock taken.
    VoteNo,
    /// CommitPrepared applied (or idempotently re-applied) the staged value.
    CommitedPrepared(RefEntry),
    /// AbortPrepared released the lock (or was a no-op).
    AbortedPrepared,
}

/// Why a [`RefStoreImpl::commit_batch`] did not return a clean, durable `Ok`.
#[derive(Debug)]
pub enum CommitBatchError {
    /// One or more CAS/lock preconditions failed; NO ref advanced. Carries each
    /// blocking `(name, current_committed)` pair (a `version == 0` sentinel means
    /// the ref was absent for an update-with-`expected`).
    Conflicts(Vec<(RefName, RefEntry)>),
    /// The batch WAS published to the in-memory store (one atomic CoW swap) but
    /// its WAL frame failed to persist, so it is not durable on this node.
    ///
    /// `applied` holds the new committed entries in input order; `source` is the
    /// underlying I/O error. Two caller modes handle it differently:
    /// - single-node (the ref-WAL is the only durability layer): propagate
    ///   `source` and refuse to ack — the client must not believe a lost write
    ///   succeeded;
    /// - Raft-backed (the replicated log is authoritative and re-applies this
    ///   entry on restart, rebuilding the materialized ref-WAL): surface the
    ///   `applied` outcomes and log the failure — no data is lost.
    NotDurable {
        applied: Vec<RefEntry>,
        source: LedgeError,
    },
}

impl RefStoreImpl {
    /// Deterministically apply a pre-resolved op using the supplied `hlc`
    /// (no internal `hlc.tick()`).
    ///
    /// Same CAS semantics as `update`/`delete`, but the `hlc` is caller-supplied
    /// so all Raft replicas agree on every `RefEntry` byte-for-byte. This is the
    /// replicated apply path; single-node callers keep using `update`/`delete`.
    ///
    /// # Determinism
    /// The outcome is a pure function of `(applied_state, op)`. openraft applies
    /// committed entries in log order one at a time, so the CAS loop swaps on its
    /// first attempt in practice; the loop is retained only to share the proven
    /// lock-free shape and remain correct under hypothetical concurrent calls.
    ///
    /// # Side effects
    /// On success, appends to the WAL exactly like `update`/`delete` for crash
    /// durability of the applied state.
    #[instrument(skip(self, op))]
    pub async fn apply_op(&self, op: &AppliedOp) -> AppliedOutcome {
        match op {
            AppliedOp::Update {
                name,
                target,
                expected,
                hlc,
            } => {
                let key = name.as_str().as_bytes().to_vec();
                loop {
                    let current_arc = self.root.load_full();
                    let current_root = current_arc.as_ref();
                    let current_entry: Option<RefEntry> = match current_root {
                        None => None,
                        Some(root) => art_lookup(root, &key, 0).map(|s| s.committed.clone()),
                    };

                    // Same precondition checks as `update`, returned as outcomes.
                    match (&current_entry, expected) {
                        (Some(existing), None) => {
                            return AppliedOutcome::Conflict(existing.clone());
                        }
                        (None, Some(_)) => {
                            return AppliedOutcome::NotFound;
                        }
                        (Some(existing), Some(exp_oid)) if existing.target != *exp_oid => {
                            return AppliedOutcome::Conflict(existing.clone());
                        }
                        _ => {}
                    }

                    let new_version = current_entry.as_ref().map(|e| e.version + 1).unwrap_or(1);
                    debug_assert!(new_version >= 1, "version invariant: version >= 1");

                    // CRITICAL: use the SUPPLIED hlc, never self.hlc.tick().
                    let new_entry = RefEntry {
                        target: *target,
                        hlc: *hlc,
                        version: new_version,
                    };

                    let new_root = art_insert(
                        current_root.clone(),
                        &key,
                        RefSlot::committed(new_entry.clone()),
                        0,
                    );
                    let new_root_arc = Arc::new(Some(new_root));
                    let prev = self
                        .root
                        .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
                    if Arc::ptr_eq(&prev, &current_arc) {
                        // apply_op is the REPLICATED path: it runs only on entries
                        // already committed to the Raft log, which is authoritative
                        // and re-applies this entry on restart to rebuild the
                        // materialized ref-WAL. So a local WAL write failure loses
                        // no data — log it and still return the deterministic
                        // outcome (apply must be deterministic and total). This is
                        // the same reasoning as CommitBatchError::NotDurable on the
                        // Raft side; the single-node paths (update/delete/
                        // commit_batch) instead surface the error to the caller.
                        if let Err(e) = self
                            .wal
                            .append(&WalEntry::Update {
                                name: name.as_str().to_string(),
                                entry: new_entry.clone(),
                            })
                            .await
                        {
                            warn!("apply_op Update WAL append failed: {e:?}");
                        }
                        return AppliedOutcome::Updated(new_entry);
                    }
                    // CAS lost — reload and retry.
                }
            }
            AppliedOp::Delete {
                name,
                expected,
                hlc,
            } => {
                let key = name.as_str().as_bytes().to_vec();
                loop {
                    let current_arc = self.root.load_full();
                    let current_root = current_arc.as_ref();
                    let current_entry: RefEntry = match current_root {
                        None => return AppliedOutcome::NotFound,
                        Some(root) => match art_lookup(root, &key, 0) {
                            None => return AppliedOutcome::NotFound,
                            Some(s) => s.committed.clone(),
                        },
                    };
                    if current_entry.target != *expected {
                        return AppliedOutcome::Conflict(current_entry);
                    }

                    let new_root_opt = match current_root {
                        None => None,
                        Some(root) => art_delete(Arc::clone(root), &key, 0),
                    };
                    let new_root_arc = Arc::new(new_root_opt);
                    let prev = self
                        .root
                        .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
                    if Arc::ptr_eq(&prev, &current_arc) {
                        if let Err(e) = self
                            .wal
                            .append(&WalEntry::Delete {
                                name: name.as_str().to_string(),
                                hlc: *hlc, // supplied hlc, not tick()
                            })
                            .await
                        {
                            warn!("apply_op Delete WAL append failed: {e:?}");
                        }
                        return AppliedOutcome::Deleted;
                    }
                    // CAS lost — retry.
                }
            }
            AppliedOp::Prepare {
                txn_id,
                coord_shard,
                name,
                target,
                expected,
                hlc,
            } => {
                let key = name.as_str().as_bytes().to_vec();
                loop {
                    let current_arc = self.root.load_full();
                    let current_root = current_arc.as_ref();
                    let current_slot: Option<RefSlot> = match current_root {
                        None => None,
                        Some(root) => art_lookup(root, &key, 0).cloned(),
                    };

                    // (b) Already locked by another txn → vote NO (no-wait).
                    if let Some(slot) = &current_slot {
                        if slot.locked_by_other(txn_id) {
                            return AppliedOutcome::VoteNo;
                        }
                    }

                    // (a) CAS precondition against the COMMITTED value. A version-0
                    // sentinel committed (prepared-only, never created) is absent.
                    let committed: Option<RefEntry> = current_slot
                        .as_ref()
                        .map(|s| s.committed.clone())
                        .filter(|c| c.version != 0);
                    let precondition_ok = match (&committed, expected) {
                        (None, None) => true,                 // create absent ref
                        (Some(_), None) => false,             // create but present
                        (None, Some(_)) => false,             // update but absent
                        (Some(c), Some(x)) => c.target == *x, // update matches
                    };
                    if !precondition_ok {
                        return AppliedOutcome::VoteNo;
                    }

                    // Build the new slot: keep committed (or a version-0 sentinel
                    // for an absent ref) and attach the prepared intent. The
                    // sentinel is NEVER observed by reads — get/list filter
                    // version-0 — so the ref stays absent until CommitPrepared.
                    let intent = PreparedIntent {
                        txn_id: *txn_id,
                        coord_shard: *coord_shard,
                        staged_target: *target,
                        staged_hlc: *hlc,
                    };
                    let new_slot = match &current_slot {
                        Some(s) => RefSlot {
                            committed: s.committed.clone(),
                            prepared: Some(intent),
                        },
                        None => RefSlot {
                            committed: RefEntry {
                                target: *target,
                                hlc: 0,
                                version: 0,
                            },
                            prepared: Some(intent),
                        },
                    };

                    let new_root = art_insert(current_root.clone(), &key, new_slot, 0);
                    let new_root_arc = Arc::new(Some(new_root));
                    let prev = self
                        .root
                        .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
                    if Arc::ptr_eq(&prev, &current_arc) {
                        // Locks are NOT WAL-logged as committed state: they are
                        // volatile until CommitPrepared writes the durable entry.
                        // A crash before CommitPrepared = presumed abort; the lock
                        // simply vanishes on replay (correct presumed-abort).
                        return AppliedOutcome::VoteYes;
                    }
                    // CAS lost — retry.
                }
            }
            AppliedOp::CommitPrepared { txn_id, name } => {
                let key = name.as_str().as_bytes().to_vec();
                loop {
                    let current_arc = self.root.load_full();
                    let current_root = current_arc.as_ref();
                    let current_slot: Option<RefSlot> = match current_root {
                        None => None,
                        Some(root) => art_lookup(root, &key, 0).cloned(),
                    };

                    let Some(slot) = current_slot else {
                        // No slot at all → already GC'd/aborted. Nothing to commit.
                        return AppliedOutcome::AbortedPrepared;
                    };

                    match &slot.prepared {
                        Some(p) if &p.txn_id == txn_id => {
                            // Roll forward: committed := staged (version+1).
                            let new_version = slot.committed.version + 1;
                            debug_assert!(new_version >= 1, "version invariant: >= 1");
                            let new_entry = RefEntry {
                                target: p.staged_target,
                                hlc: p.staged_hlc,
                                version: new_version,
                            };
                            let new_slot = RefSlot::committed(new_entry.clone());
                            let new_root = art_insert(current_root.clone(), &key, new_slot, 0);
                            let new_root_arc = Arc::new(Some(new_root));
                            let prev = self
                                .root
                                .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
                            if Arc::ptr_eq(&prev, &current_arc) {
                                if let Err(e) = self
                                    .wal
                                    .append(&WalEntry::Update {
                                        name: name.as_str().to_string(),
                                        entry: new_entry.clone(),
                                    })
                                    .await
                                {
                                    warn!("CommitPrepared WAL append failed: {e:?}");
                                }
                                return AppliedOutcome::CommitedPrepared(new_entry);
                            }
                            // CAS lost — retry.
                        }
                        _ => {
                            // Lock already cleared / different txn → idempotent:
                            // return the current committed unchanged.
                            return AppliedOutcome::CommitedPrepared(slot.committed.clone());
                        }
                    }
                }
            }
            AppliedOp::AbortPrepared { txn_id, name } => {
                let key = name.as_str().as_bytes().to_vec();
                loop {
                    let current_arc = self.root.load_full();
                    let current_root = current_arc.as_ref();
                    let current_slot: Option<RefSlot> = match current_root {
                        None => None,
                        Some(root) => art_lookup(root, &key, 0).cloned(),
                    };

                    let Some(slot) = current_slot else {
                        return AppliedOutcome::AbortedPrepared; // nothing to release
                    };

                    match &slot.prepared {
                        Some(p) if &p.txn_id == txn_id => {
                            if slot.committed.version == 0 {
                                // Absent-ref prepare (sentinel): aborting removes
                                // the slot entirely so the ref stays absent.
                                let new_root_opt = match current_root {
                                    None => None,
                                    Some(root) => art_delete(Arc::clone(root), &key, 0),
                                };
                                let new_root_arc = Arc::new(new_root_opt);
                                let prev = self
                                    .root
                                    .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
                                if Arc::ptr_eq(&prev, &current_arc) {
                                    return AppliedOutcome::AbortedPrepared;
                                }
                            } else {
                                let new_slot = RefSlot::committed(slot.committed.clone());
                                let new_root = art_insert(current_root.clone(), &key, new_slot, 0);
                                let new_root_arc = Arc::new(Some(new_root));
                                let prev = self
                                    .root
                                    .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
                                if Arc::ptr_eq(&prev, &current_arc) {
                                    return AppliedOutcome::AbortedPrepared;
                                }
                            }
                            // CAS lost — retry.
                        }
                        _ => return AppliedOutcome::AbortedPrepared, // idempotent no-op
                    }
                }
            }
        }
    }

    /// Atomically apply a multi-ref CAS batch in a SINGLE `ArcSwap` root swap.
    ///
    /// This is the single-node + single-shard all-or-nothing primitive used by
    /// the atomic `commit` path. Semantics:
    /// 1. Snapshot ONE root.
    /// 2. Evaluate EVERY CAS precondition against that snapshot (locked refs and
    ///    failed CAS both count as conflicts).
    /// 3. If all hold, build a new root by inserting all updated `RefSlot`s, then
    ///    publish via ONE `compare_and_swap` + ONE WAL frame per applied ref.
    /// 4. If ANY fails, apply NONE and return the per-ref conflicts.
    ///
    /// On a lost CAS (concurrent writer) the whole evaluation restarts against a
    /// fresh snapshot — preconditions are re-checked, so atomicity holds across
    /// retries.
    ///
    /// # Returns
    /// - `Ok(applied)`: `applied[i]` is the new committed entry for `ops[i]`,
    ///   durably persisted.
    /// - `Err(CommitBatchError::Conflicts)`: `(name, current_committed)` for each
    ///   failing ref; no ref advanced. An update-with-`expected` on an absent ref
    ///   reports a `version == 0` sentinel current entry.
    /// - `Err(CommitBatchError::NotDurable)`: the batch was published in memory
    ///   but its WAL write failed — see the type docs for how each caller mode
    ///   handles it.
    ///
    /// # Complexity
    /// O(B·k) per attempt for B ops of key length k (B inserts on the CoW path).
    pub async fn commit_batch(
        &self,
        ops: Vec<(RefName, ObjectId, Option<ObjectId>, u64)>,
    ) -> std::result::Result<Vec<RefEntry>, CommitBatchError> {
        loop {
            let current_arc = self.root.load_full();
            let current_root = current_arc.as_ref();

            // Phase 1: evaluate ALL preconditions against this snapshot.
            let mut conflicts: Vec<(RefName, RefEntry)> = Vec::new();
            let mut planned: Vec<(RefName, RefEntry)> = Vec::with_capacity(ops.len());

            for (name, target, expected, hlc) in &ops {
                let key = name.as_str().as_bytes();
                let slot: Option<RefSlot> = match current_root {
                    None => None,
                    Some(root) => art_lookup(root, key, 0).cloned(),
                };

                // A locked ref is busy → conflict (surface its committed).
                if let Some(s) = &slot {
                    if s.prepared.is_some() {
                        conflicts.push((name.clone(), s.committed.clone()));
                        continue;
                    }
                }

                // Treat a version-0 sentinel committed as absent.
                let committed: Option<RefEntry> = slot
                    .as_ref()
                    .map(|s| s.committed.clone())
                    .filter(|c| c.version != 0);

                match (&committed, expected) {
                    (Some(existing), None) => {
                        conflicts.push((name.clone(), existing.clone())); // create but present
                    }
                    (None, Some(exp)) => {
                        // update but absent → version-0 sentinel as the "current".
                        conflicts.push((
                            name.clone(),
                            RefEntry {
                                target: *exp,
                                hlc: 0,
                                version: 0,
                            },
                        ));
                    }
                    (Some(existing), Some(exp)) if existing.target != *exp => {
                        conflicts.push((name.clone(), existing.clone())); // stale CAS
                    }
                    _ => {
                        // Precondition holds: plan the new committed entry.
                        let new_version = committed.as_ref().map(|e| e.version + 1).unwrap_or(1);
                        debug_assert!(new_version >= 1, "version invariant: >= 1");
                        planned.push((
                            name.clone(),
                            RefEntry {
                                target: *target,
                                hlc: *hlc,
                                version: new_version,
                            },
                        ));
                    }
                }
            }

            // All-or-nothing: any conflict ⇒ apply none.
            if !conflicts.is_empty() {
                return Err(CommitBatchError::Conflicts(conflicts));
            }

            // Phase 2: build the new root by inserting every planned RefSlot.
            let mut new_root: Option<Arc<ArtNode>> = current_root.clone();
            for (name, entry) in &planned {
                new_root = Some(art_insert(
                    new_root,
                    name.as_str().as_bytes(),
                    RefSlot::committed(entry.clone()),
                    0,
                ));
            }
            let new_root_arc = Arc::new(new_root);

            // Single atomic publish.
            let prev = self
                .root
                .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
            if Arc::ptr_eq(&prev, &current_arc) {
                // The batch was published to the store in one atomic CoW swap, so
                // it must recover all-or-nothing. Persist it as a SINGLE WAL frame
                // (not one frame per ref): a crash mid-write drops the whole frame
                // on the length+CRC check, never a partial prefix of the batch.
                let updates: Vec<(String, RefEntry)> = planned
                    .iter()
                    .map(|(name, entry)| (name.as_str().to_string(), entry.clone()))
                    .collect();
                let applied: Vec<RefEntry> = planned.into_iter().map(|(_, e)| e).collect();
                if let Err(source) = self.wal.append(&WalEntry::Batch { updates }).await {
                    // The batch is already published in memory (CAS done) but its
                    // WAL frame did not persist. Do NOT silently ack: hand the
                    // applied entries AND the I/O error back so the caller decides
                    // per its durability model (single-node: refuse the ack; Raft:
                    // the log is authoritative and replays this entry on restart).
                    return Err(CommitBatchError::NotDurable { applied, source });
                }
                return Ok(applied);
            }
            // CAS lost to a concurrent writer — restart full evaluation.
        }
    }

    /// Enumerate every ref that currently holds a prepared 2PC lock, as
    /// `(name, intent)` pairs in unspecified order. Backs the crash-recovery
    /// resolver (spec §3.4): it scans these locks and resolves each against its
    /// coordinator-shard decision. Lock-free single atomic load + ART scan.
    pub fn prepared_locks(&self) -> Vec<(String, PreparedIntent)> {
        let root_guard = self.root.load();
        match root_guard.as_ref() {
            None => Vec::new(),
            Some(root) => {
                let pairs = art_prefix_iter(root, b"", 0);
                let mut out = Vec::new();
                for (key_bytes, slot) in pairs {
                    if let Some(intent) = slot.prepared {
                        if let Ok(s) = std::str::from_utf8(&key_bytes) {
                            out.push((s.to_string(), intent));
                        }
                    }
                }
                out
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RefStore implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl RefStore for RefStoreImpl {
    /// Return the current entry for `name`, or `None` if it does not exist.
    ///
    /// Lock-free single atomic load + O(k) ART traversal.
    async fn get(&self, name: &RefName) -> Result<Option<RefEntry>> {
        let root_guard = self.root.load();
        Ok(match root_guard.as_ref() {
            None => None,
            // Project `.committed`. A `version == 0` committed is the absent-ref
            // sentinel left by a `Prepare` on a not-yet-created ref — treat it as
            // absent so reads never observe a prepared-but-uncommitted creation.
            Some(root) => art_lookup(root, name.as_str().as_bytes(), 0)
                .map(|s| s.committed.clone())
                .filter(|c| c.version != 0),
        })
    }

    /// Atomically create or update `name` under optimistic CAS.
    ///
    /// - `expected = None`: create new ref; errors with `Conflict` if it
    ///   already exists with any target.
    /// - `expected = Some(id)`: update only if current target equals `id`;
    ///   errors with `Conflict` or `NotFound` otherwise.
    ///
    /// Version starts at 1 for new refs and increments by 1 on each update.
    /// `debug_assert!` guards the version ≥ 1 invariant on every write path.
    #[instrument(skip(self), fields(ref_name = %name))]
    async fn update(
        &self,
        name: &RefName,
        new: ObjectId,
        expected: Option<ObjectId>,
    ) -> Result<RefEntry> {
        let key = name.as_str().as_bytes().to_vec();

        loop {
            // Snapshot the current root for this CAS attempt.
            let current_arc = self.root.load_full();
            let current_root = current_arc.as_ref();

            // Read the slot that exists right now (if any).
            let current_slot: Option<RefSlot> = match current_root {
                None => None,
                Some(root) => art_lookup(root, &key, 0).cloned(),
            };

            // A prepared lock makes the ref busy: a 2PC txn holds it and must be
            // resolved (commit/abort) before any single-ref write may proceed.
            if let Some(slot) = &current_slot {
                if slot.prepared.is_some() {
                    return Err(LedgeError::Conflict {
                        current: slot.committed.clone(),
                    });
                }
            }

            // Project committed, treating the version-0 sentinel as absent.
            let current_entry: Option<RefEntry> = current_slot
                .as_ref()
                .map(|s| s.committed.clone())
                .filter(|c| c.version != 0);

            // Validate optimistic preconditions.
            match (&current_entry, &expected) {
                // create (expected = None) but ref already exists → Conflict.
                (Some(existing), None) => {
                    return Err(LedgeError::Conflict {
                        current: existing.clone(),
                    });
                }
                // update (expected = Some) but ref does not exist → NotFound.
                (None, Some(_)) => {
                    return Err(LedgeError::NotFound(new));
                }
                // update but wrong target → Conflict.
                (Some(existing), Some(exp_oid)) if existing.target != *exp_oid => {
                    return Err(LedgeError::Conflict {
                        current: existing.clone(),
                    });
                }
                // All other cases are valid (create with None, update with matching id).
                _ => {}
            }

            // Compute the new version (1-based; new ref starts at 1).
            let new_version = current_entry.as_ref().map(|e| e.version + 1).unwrap_or(1);
            // Per spec: version must always be >= 1 on the write path.
            debug_assert!(
                new_version >= 1,
                "version invariant violated: version must be >= 1"
            );

            let new_entry = RefEntry {
                target: new,
                hlc: self.hlc.tick(),
                version: new_version,
            };

            // Produce the new root via CoW ART insert.
            let new_root = art_insert(
                current_root.clone(),
                &key,
                RefSlot::committed(new_entry.clone()),
                0,
            );
            let new_root_arc = Arc::new(Some(new_root));

            // Attempt the atomic swap.
            let prev = self
                .root
                .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
            if Arc::ptr_eq(&prev, &current_arc) {
                // CAS succeeded — commit to WAL then return.
                self.wal
                    .append(&WalEntry::Update {
                        name: name.as_str().to_string(),
                        entry: new_entry.clone(),
                    })
                    .await?;
                debug!(version = new_entry.version, "ref committed");
                return Ok(new_entry);
            }
            // CAS lost to a concurrent writer — reload and retry.
        }
    }

    /// Atomically delete `name`, verifying the current target equals `expected`.
    ///
    /// # Errors
    /// - `NotFound` if the ref does not exist.
    /// - `Conflict` if the current target differs from `expected`.
    #[instrument(skip(self), fields(ref_name = %name))]
    async fn delete(&self, name: &RefName, expected: ObjectId) -> Result<()> {
        let key = name.as_str().as_bytes().to_vec();

        loop {
            let current_arc = self.root.load_full();
            let current_root = current_arc.as_ref();

            // Read and validate the current slot.
            let current_slot: RefSlot = match current_root {
                None => return Err(LedgeError::NotFound(expected)),
                Some(root) => match art_lookup(root, &key, 0) {
                    None => return Err(LedgeError::NotFound(expected)),
                    Some(s) => s.clone(),
                },
            };

            // A prepared lock makes the ref busy: it must be resolved first.
            if current_slot.prepared.is_some() {
                return Err(LedgeError::Conflict {
                    current: current_slot.committed.clone(),
                });
            }

            // A version-0 sentinel committed means the ref is logically absent.
            if current_slot.committed.version == 0 {
                return Err(LedgeError::NotFound(expected));
            }
            let current_entry = current_slot.committed.clone();

            if current_entry.target != expected {
                return Err(LedgeError::Conflict {
                    current: current_entry,
                });
            }

            // Produce the new root without the deleted key.
            let new_root_opt = match current_root {
                None => None,
                Some(root) => art_delete(Arc::clone(root), &key, 0),
            };
            let new_root_arc = Arc::new(new_root_opt);

            let prev = self
                .root
                .compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
            if Arc::ptr_eq(&prev, &current_arc) {
                self.wal
                    .append(&WalEntry::Delete {
                        name: name.as_str().to_string(),
                        hlc: self.hlc.tick(),
                    })
                    .await?;
                return Ok(());
            }
            // CAS lost — retry.
        }
    }

    /// Return all refs whose name starts with `prefix`, in unspecified order.
    ///
    /// Lock-free single atomic load + O(n_matches * k) ART prefix scan.
    async fn list(&self, prefix: &str) -> Result<Vec<(RefName, RefEntry)>> {
        let root_guard = self.root.load();
        match root_guard.as_ref() {
            None => Ok(Vec::new()),
            Some(root) => {
                let pairs = art_prefix_iter(root, prefix.as_bytes(), 0);
                let mut results = Vec::with_capacity(pairs.len());
                for (key_bytes, slot) in pairs {
                    // Skip prepared-only refs (version-0 sentinel): never created.
                    if slot.committed.version == 0 {
                        continue;
                    }
                    if let Ok(s) = std::str::from_utf8(&key_bytes) {
                        if let Ok(n) = RefName::new(s) {
                            results.push((n, slot.committed));
                        }
                    }
                }
                Ok(results)
            }
        }
    }

    /// Capture a consistent, point-in-time snapshot of all refs.
    ///
    /// O(1): one atomic load, no copying, no locking.
    fn snapshot(&self) -> Arc<dyn RefSnapshot> {
        let root_arc = self.root.load_full();
        // Unwrap the `Arc<Option<…>>` to get `Option<Arc<ArtNode>>`.
        let inner: Option<Arc<ArtNode>> = root_arc.as_ref().clone();
        Arc::new(ArtSnapshot { root: inner })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::{ObjectId, RefName, TxnId, HLC};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_store() -> RefStoreImpl {
        let dir = tempdir().unwrap();
        // Keep the TempDir so the path survives for the duration of the test.
        let path = dir.keep();
        RefStoreImpl::open(path, Arc::new(HLC::new())).unwrap()
    }

    fn oid(byte: u8) -> ObjectId {
        ObjectId::from_bytes([byte; 32])
    }

    fn name(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }

    fn txn(byte: u8) -> TxnId {
        TxnId::from_bytes([byte; 16])
    }

    // ── 1. create then get ───────────────────────────────────────────────────

    #[tokio::test]
    async fn create_and_get() {
        let store = make_store();
        let n = name("refs/heads/main");
        let t = oid(1);
        let entry = store.update(&n, t, None).await.unwrap();
        assert_eq!(entry.target, t);
        assert_eq!(entry.version, 1, "first write must be version 1");
        let got = store.get(&n).await.unwrap().unwrap();
        assert_eq!(got, entry);
    }

    /// A ref name that is a strict prefix of existing ones — used to panic in the
    /// ART inner-node split (index out of bounds). Real scenario: tags v1.0.0 and
    /// v1.0.1 exist (building a compressed inner prefix), then create v1.0.
    #[tokio::test]
    async fn update_ref_that_is_strict_prefix_of_existing() {
        let store = make_store();
        store
            .update(&name("refs/tags/v1.0.0"), oid(1), None)
            .await
            .unwrap();
        store
            .update(&name("refs/tags/v1.0.1"), oid(2), None)
            .await
            .unwrap();
        // This update used to panic; it must succeed and be independently readable.
        store
            .update(&name("refs/tags/v1.0"), oid(3), None)
            .await
            .unwrap();
        for (n, o) in [
            ("refs/tags/v1.0.0", 1),
            ("refs/tags/v1.0.1", 2),
            ("refs/tags/v1.0", 3),
        ] {
            assert_eq!(
                store.get(&name(n)).await.unwrap().unwrap().target,
                oid(o),
                "{n} resolves to its own target"
            );
        }
        assert_eq!(store.list("refs/tags/").await.unwrap().len(), 3);
    }

    /// Heavy-concurrency guard for the compaction path: sustained interleaved ref
    /// creation across multiple concurrent compactors, then reopen from the WAL
    /// alone — every acked ref must survive. This exercises `compact_wal` under
    /// load (catching deadlocks, panics, or gross loss); the precise
    /// snapshot/append race it addresses has a sub-microsecond window that this
    /// test cannot reliably trigger, so the *correctness* of the fix rests on the
    /// snapshot-under-lock invariant asserted deterministically in the WAL test
    /// `compact_with_snapshot_runs_under_wal_lock`, not on this one failing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_updates_survive_wal_compaction() {
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 60;
        const COMPACTORS: usize = 4;
        let dir = tempdir().unwrap();
        let path = dir.keep();
        let store = Arc::new(RefStoreImpl::open(path.clone(), Arc::new(HLC::new())).unwrap());

        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let s = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                for k in 0..PER_WRITER {
                    let nm = RefName::new(&format!("refs/heads/w{w}-{k:03}")).unwrap();
                    s.update(&nm, oid(((w * PER_WRITER + k) % 251) as u8), None)
                        .await
                        .unwrap();
                    // Yield so compactions interleave between creations.
                    tokio::task::yield_now().await;
                }
            }));
        }
        let mut compactors = Vec::new();
        for _ in 0..COMPACTORS {
            let s = Arc::clone(&store);
            compactors.push(tokio::spawn(async move {
                // Loop compactions until the writers are (almost certainly) done.
                for _ in 0..(PER_WRITER * 2) {
                    s.compact_wal().await.unwrap();
                    tokio::task::yield_now().await;
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        for c in compactors {
            c.await.unwrap();
        }

        // Reopen from the WAL only — no in-memory state carries over.
        drop(store);
        let reopened = RefStoreImpl::open(path, Arc::new(HLC::new())).unwrap();
        for w in 0..WRITERS {
            for k in 0..PER_WRITER {
                let nm = RefName::new(&format!("refs/heads/w{w}-{k:03}")).unwrap();
                let got =
                    reopened.get(&nm).await.unwrap().unwrap_or_else(|| {
                        panic!("ref w{w}-{k:03} lost across compaction + reopen")
                    });
                assert_eq!(got.target, oid(((w * PER_WRITER + k) % 251) as u8));
            }
        }
    }

    /// A multi-ref batch is published atomically in memory, so it must recover
    /// atomically too. Persisting it as one WAL frame guarantees that: a torn
    /// tail write drops the entire batch on the length+CRC check, never a partial
    /// prefix. Seed a ref, commit a 3-ref batch, shear a few bytes off the WAL
    /// tail, reopen — the seed survives and none of the batch leaks. (The old
    /// one-frame-per-ref encoding would leave the first two refs and drop only
    /// the third, a partial batch.)
    #[tokio::test]
    async fn torn_batch_frame_recovers_all_or_nothing() {
        let dir = tempdir().unwrap();
        let path = dir.keep();
        let wal_path = path.join("refs").join("wal");
        {
            let store = RefStoreImpl::open(path.clone(), Arc::new(HLC::new())).unwrap();
            store
                .update(&name("refs/heads/seed"), oid(9), None)
                .await
                .unwrap();
            let ops = vec![
                (name("refs/heads/a"), oid(1), None, 100u64),
                (name("refs/heads/b"), oid(2), None, 101u64),
                (name("refs/heads/c"), oid(3), None, 102u64),
            ];
            store.commit_batch(ops).await.unwrap();
        }
        // Shear the last few bytes → the trailing batch frame is now torn.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            let len = f.metadata().unwrap().len();
            f.set_len(len - 4).unwrap();
        }
        let store = RefStoreImpl::open(path, Arc::new(HLC::new())).unwrap();
        assert!(
            store.get(&name("refs/heads/seed")).await.unwrap().is_some(),
            "the pre-batch ref must survive the torn tail"
        );
        for r in ["refs/heads/a", "refs/heads/b", "refs/heads/c"] {
            assert!(
                store.get(&name(r)).await.unwrap().is_none(),
                "torn batch must recover all-or-nothing: {r} leaked"
            );
        }
    }

    // ── 2. version increments on each update ────────────────────────────────

    #[tokio::test]
    async fn version_increments() {
        let store = make_store();
        let n = name("refs/heads/ver");
        let t1 = oid(1);
        let t2 = oid(2);
        let e1 = store.update(&n, t1, None).await.unwrap();
        let e2 = store.update(&n, t2, Some(t1)).await.unwrap();
        assert_eq!(e1.version, 1);
        assert_eq!(e2.version, 2);
    }

    // ── 3. create conflict (expected = None but ref exists) ─────────────────

    #[tokio::test]
    async fn create_conflict() {
        let store = make_store();
        let n = name("refs/heads/conflict");
        let t = oid(1);
        store.update(&n, t, None).await.unwrap();
        // Second create with expected = None must fail.
        let res = store.update(&n, oid(2), None).await;
        assert!(
            matches!(res, Err(LedgeError::Conflict { .. })),
            "expected Conflict, got {res:?}"
        );
    }

    // ── 4. update with wrong expected → Conflict ────────────────────────────

    #[tokio::test]
    async fn update_wrong_expected() {
        let store = make_store();
        let n = name("refs/heads/cas");
        store.update(&n, oid(1), None).await.unwrap();
        let res = store.update(&n, oid(2), Some(oid(99))).await;
        assert!(matches!(res, Err(LedgeError::Conflict { .. })));
    }

    // ── 5. delete then get returns None ─────────────────────────────────────

    #[tokio::test]
    async fn delete_and_get() {
        let store = make_store();
        let n = name("refs/heads/del");
        let t = oid(0xdd);
        store.update(&n, t, None).await.unwrap();
        store.delete(&n, t).await.unwrap();
        assert!(store.get(&n).await.unwrap().is_none());
    }

    // ── 6. delete with wrong expected → Conflict ────────────────────────────

    #[tokio::test]
    async fn delete_wrong_expected() {
        let store = make_store();
        let n = name("refs/heads/delwrong");
        let t = oid(0xaa);
        store.update(&n, t, None).await.unwrap();
        let res = store.delete(&n, oid(0xbb)).await;
        assert!(matches!(res, Err(LedgeError::Conflict { .. })));
        assert!(
            store.get(&n).await.unwrap().is_some(),
            "ref must survive a failed delete"
        );
    }

    // ── 7. list by prefix ───────────────────────────────────────────────────

    #[tokio::test]
    async fn list_prefix() {
        let store = make_store();
        for r in ["refs/heads/main", "refs/heads/dev", "refs/tags/v1"] {
            store.update(&name(r), oid(1), None).await.unwrap();
        }
        let heads = store.list("refs/heads/").await.unwrap();
        assert_eq!(heads.len(), 2);
        let tags = store.list("refs/tags/").await.unwrap();
        assert_eq!(tags.len(), 1);
        let all = store.list("refs/").await.unwrap();
        assert_eq!(all.len(), 3);
    }

    // ── 8. snapshot isolation ───────────────────────────────────────────────

    #[tokio::test]
    async fn snapshot_isolation() {
        let store = make_store();
        let n = name("refs/heads/snap");
        let t1 = oid(1);
        let t2 = oid(2);
        store.update(&n, t1, None).await.unwrap();
        let snap = store.snapshot();
        // Mutate the live store.
        store.update(&n, t2, Some(t1)).await.unwrap();
        // Snapshot must still reflect t1.
        assert_eq!(
            snap.get(&n).unwrap().target,
            t1,
            "snapshot must be isolated from writes"
        );
        // Live store reflects t2.
        assert_eq!(store.get(&n).await.unwrap().unwrap().target, t2);
    }

    // ── apply_op: deterministic explicit-HLC apply path ─────────────────────────

    #[tokio::test]
    async fn apply_op_update_creates_with_supplied_hlc() {
        let store = make_store();
        let n = name("refs/heads/applied");
        let t = oid(1);
        let outcome = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: t,
                expected: None,
                hlc: 777,
            })
            .await;
        match outcome {
            AppliedOutcome::Updated(entry) => {
                assert_eq!(entry.target, t);
                assert_eq!(entry.version, 1, "first write is version 1");
                assert_eq!(
                    entry.hlc, 777,
                    "apply_op must use the SUPPLIED hlc, not tick()"
                );
            }
            other => panic!("expected Updated, got {other:?}"),
        }
        // Visible through the normal read path.
        let got = store.get(&n).await.unwrap().unwrap();
        assert_eq!(got.hlc, 777);
        assert_eq!(got.target, t);
    }

    #[tokio::test]
    async fn apply_op_update_version_increments_with_supplied_hlc() {
        let store = make_store();
        let n = name("refs/heads/appliedv");
        let (t1, t2) = (oid(1), oid(2));
        let _ = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: t1,
                expected: None,
                hlc: 10,
            })
            .await;
        let outcome = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: t2,
                expected: Some(t1),
                hlc: 20,
            })
            .await;
        match outcome {
            AppliedOutcome::Updated(e) => {
                assert_eq!(e.version, 2);
                assert_eq!(e.hlc, 20);
                assert_eq!(e.target, t2);
            }
            other => panic!("expected Updated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_op_update_wrong_expected_is_conflict() {
        let store = make_store();
        let n = name("refs/heads/appliedcas");
        let _ = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: oid(1),
                expected: None,
                hlc: 1,
            })
            .await;
        let outcome = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(99)),
                hlc: 2,
            })
            .await;
        match outcome {
            AppliedOutcome::Conflict(current) => {
                assert_eq!(
                    current.target,
                    oid(1),
                    "conflict surfaces the current entry"
                );
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_op_create_existing_is_conflict() {
        let store = make_store();
        let n = name("refs/heads/applieddup");
        let _ = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: oid(1),
                expected: None,
                hlc: 1,
            })
            .await;
        let outcome = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: oid(2),
                expected: None,
                hlc: 2,
            })
            .await;
        assert!(matches!(outcome, AppliedOutcome::Conflict(_)));
    }

    #[tokio::test]
    async fn apply_op_delete_then_missing() {
        let store = make_store();
        let n = name("refs/heads/applieddel");
        let t = oid(0xdd);
        let _ = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: t,
                expected: None,
                hlc: 1,
            })
            .await;
        let outcome = store
            .apply_op(&AppliedOp::Delete {
                name: n.clone(),
                expected: t,
                hlc: 2,
            })
            .await;
        assert!(matches!(outcome, AppliedOutcome::Deleted));
        assert!(store.get(&n).await.unwrap().is_none());

        // Deleting a now-missing ref → NotFound.
        let outcome = store
            .apply_op(&AppliedOp::Delete {
                name: n.clone(),
                expected: t,
                hlc: 3,
            })
            .await;
        assert!(matches!(outcome, AppliedOutcome::NotFound));
    }

    #[tokio::test]
    async fn apply_op_delete_wrong_expected_is_conflict() {
        let store = make_store();
        let n = name("refs/heads/applieddelcas");
        let _ = store
            .apply_op(&AppliedOp::Update {
                name: n.clone(),
                target: oid(0xaa),
                expected: None,
                hlc: 1,
            })
            .await;
        let outcome = store
            .apply_op(&AppliedOp::Delete {
                name: n.clone(),
                expected: oid(0xbb),
                hlc: 2,
            })
            .await;
        assert!(matches!(outcome, AppliedOutcome::Conflict(_)));
        assert!(
            store.get(&n).await.unwrap().is_some(),
            "ref survives a failed delete"
        );
    }

    // ── restore_from: snapshot install preserves exact RefEntry ─────────────────

    #[tokio::test]
    async fn restore_from_preserves_exact_version_and_hlc() {
        let store = make_store();
        let n = name("refs/heads/restored");
        // A source entry that has been updated several times: version=5, hlc=999.
        let entry = RefEntry {
            target: oid(7),
            hlc: 999,
            version: 5,
        };
        store
            .restore_from(vec![(n.clone(), entry.clone())])
            .await
            .unwrap();

        let got = store.get(&n).await.unwrap().expect("restored ref present");
        assert_eq!(got.version, 5, "restore_from must NOT reset version to 1");
        assert_eq!(got.hlc, 999, "restore_from must preserve the source hlc");
        assert_eq!(got.target, oid(7), "restore_from must preserve the target");
        assert_eq!(got, entry, "restored entry is byte-identical to the source");
    }

    #[tokio::test]
    async fn restore_from_replaces_entire_set_and_is_durable() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let stale = name("refs/heads/stale");
        let fresh = name("refs/heads/fresh");

        {
            let store = RefStoreImpl::open(data_dir.clone(), Arc::new(HLC::new())).unwrap();
            // Pre-existing ref that must NOT survive the restore.
            store.update(&stale, oid(1), None).await.unwrap();
            // Restore wipes prior state and installs `fresh` at version=3.
            store
                .restore_from(vec![(
                    fresh.clone(),
                    RefEntry {
                        target: oid(2),
                        hlc: 555,
                        version: 3,
                    },
                )])
                .await
                .unwrap();
            assert!(
                store.get(&stale).await.unwrap().is_none(),
                "restore replaces the set"
            );
            assert_eq!(store.get(&fresh).await.unwrap().unwrap().version, 3);
        }

        // Reopen: the WAL checkpoint must reproduce the restored state exactly.
        let store2 = RefStoreImpl::open(data_dir, Arc::new(HLC::new())).unwrap();
        assert!(
            store2.get(&stale).await.unwrap().is_none(),
            "stale ref gone after reopen"
        );
        let got = store2
            .get(&fresh)
            .await
            .unwrap()
            .expect("fresh ref durable");
        assert_eq!(got.version, 3, "version durable across reopen");
        assert_eq!(got.hlc, 555);
        assert_eq!(got.target, oid(2));
    }

    // ── 9. WAL replay durability ─────────────────────────────────────────────

    #[tokio::test]
    async fn wal_replay() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let n = name("refs/heads/persist");
        let t = oid(0xfe);

        // Write to the first store instance, then drop it.
        {
            let store = RefStoreImpl::open(data_dir.clone(), Arc::new(HLC::new())).unwrap();
            store.update(&n, t, None).await.unwrap();
        }

        // Open a fresh instance from the same directory and verify state is restored.
        let store2 = RefStoreImpl::open(data_dir, Arc::new(HLC::new())).unwrap();
        let entry = store2
            .get(&n)
            .await
            .unwrap()
            .expect("ref must survive a store reopen");
        assert_eq!(entry.target, t);
        assert_eq!(entry.version, 1);
    }

    // ── Prepare locks + VoteYes ───────────────────────────────────────────────
    #[tokio::test]
    async fn prepare_locks_and_votes_yes() {
        let store = make_store();
        let n = name("refs/heads/prep");
        store.update(&n, oid(1), None).await.unwrap();
        let outcome = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(1)),
                hlc: 50,
            })
            .await;
        assert_eq!(outcome, AppliedOutcome::VoteYes);
        // Read still sees the COMMITTED value, never the staged one.
        assert_eq!(store.get(&n).await.unwrap().unwrap().target, oid(1));
    }

    // ── Prepare on a ref already locked by another txn → VoteNo ────────────────
    #[tokio::test]
    async fn prepare_on_locked_ref_votes_no() {
        let store = make_store();
        let n = name("refs/heads/prep2");
        store.update(&n, oid(1), None).await.unwrap();
        let _ = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(1)),
                hlc: 50,
            })
            .await;
        // A different txn cannot prepare the same ref (no-wait): votes NO.
        let outcome = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(2),
                coord_shard: 0,
                name: n.clone(),
                target: oid(3),
                expected: Some(oid(1)),
                hlc: 60,
            })
            .await;
        assert_eq!(outcome, AppliedOutcome::VoteNo);
    }

    // ── Prepare with a failing CAS precondition → VoteNo (no lock taken) ───────
    #[tokio::test]
    async fn prepare_failed_precondition_votes_no_no_lock() {
        let store = make_store();
        let n = name("refs/heads/prep3");
        store.update(&n, oid(1), None).await.unwrap();
        // expected oid(9) != committed oid(1).
        let outcome = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(9)),
                hlc: 50,
            })
            .await;
        assert_eq!(outcome, AppliedOutcome::VoteNo);
        // No lock taken → a normal update still works.
        store.update(&n, oid(2), Some(oid(1))).await.unwrap();
    }

    // ── CommitPrepared applies staged (version+1, staged hlc) + releases lock ──
    #[tokio::test]
    async fn commit_prepared_applies_staged_and_releases() {
        let store = make_store();
        let n = name("refs/heads/cp");
        store.update(&n, oid(1), None).await.unwrap(); // version 1
        let _ = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(1)),
                hlc: 777,
            })
            .await;
        let outcome = store
            .apply_op(&AppliedOp::CommitPrepared {
                txn_id: txn(1),
                name: n.clone(),
            })
            .await;
        match outcome {
            AppliedOutcome::CommitedPrepared(e) => {
                assert_eq!(e.target, oid(2));
                assert_eq!(e.version, 2, "committed staged is version+1");
                assert_eq!(e.hlc, 777, "uses the staged_hlc deterministically");
            }
            other => panic!("expected CommitedPrepared, got {other:?}"),
        }
        // Lock released: read sees the new committed value, and update works again.
        assert_eq!(store.get(&n).await.unwrap().unwrap().target, oid(2));
        store.update(&n, oid(3), Some(oid(2))).await.unwrap();
    }

    // ── CommitPrepared is idempotent (already resolved) ───────────────────────
    #[tokio::test]
    async fn commit_prepared_idempotent_after_resolution() {
        let store = make_store();
        let n = name("refs/heads/cpidem");
        store.update(&n, oid(1), None).await.unwrap();
        let _ = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(1)),
                hlc: 777,
            })
            .await;
        let _ = store
            .apply_op(&AppliedOp::CommitPrepared {
                txn_id: txn(1),
                name: n.clone(),
            })
            .await;
        // Re-applying CommitPrepared returns the CURRENT committed (idempotent, no double-bump).
        let again = store
            .apply_op(&AppliedOp::CommitPrepared {
                txn_id: txn(1),
                name: n.clone(),
            })
            .await;
        match again {
            AppliedOutcome::CommitedPrepared(e) => {
                assert_eq!(e.version, 2, "no second version bump on replay");
                assert_eq!(e.target, oid(2));
            }
            other => panic!("expected idempotent CommitedPrepared, got {other:?}"),
        }
    }

    // ── AbortPrepared releases; committed unchanged ───────────────────────────
    #[tokio::test]
    async fn abort_prepared_releases_lock_committed_unchanged() {
        let store = make_store();
        let n = name("refs/heads/ap");
        store.update(&n, oid(1), None).await.unwrap();
        let _ = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(1)),
                hlc: 777,
            })
            .await;
        let outcome = store
            .apply_op(&AppliedOp::AbortPrepared {
                txn_id: txn(1),
                name: n.clone(),
            })
            .await;
        assert_eq!(outcome, AppliedOutcome::AbortedPrepared);
        // Committed is still oid(1) version 1; lock gone so update works.
        let got = store.get(&n).await.unwrap().unwrap();
        assert_eq!(got.target, oid(1));
        assert_eq!(got.version, 1);
        store.update(&n, oid(5), Some(oid(1))).await.unwrap();
    }

    // ── AbortPrepared is idempotent (no matching lock) ────────────────────────
    #[tokio::test]
    async fn abort_prepared_idempotent_when_unlocked() {
        let store = make_store();
        let n = name("refs/heads/apidem");
        store.update(&n, oid(1), None).await.unwrap();
        let outcome = store
            .apply_op(&AppliedOp::AbortPrepared {
                txn_id: txn(1),
                name: n.clone(),
            })
            .await;
        assert_eq!(
            outcome,
            AppliedOutcome::AbortedPrepared,
            "abort of an unheld lock is a no-op"
        );
    }

    // ── Prepare to CREATE an absent ref (expected=None) ───────────────────────
    #[tokio::test]
    async fn prepare_create_absent_ref_votes_yes() {
        let store = make_store();
        let n = name("refs/heads/prepnew");
        let outcome = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: None,
                hlc: 50,
            })
            .await;
        assert_eq!(outcome, AppliedOutcome::VoteYes);
        // Still absent (staged-not-committed) until CommitPrepared.
        assert!(store.get(&n).await.unwrap().is_none());
        let cp = store
            .apply_op(&AppliedOp::CommitPrepared {
                txn_id: txn(1),
                name: n.clone(),
            })
            .await;
        match cp {
            AppliedOutcome::CommitedPrepared(e) => {
                assert_eq!(e.target, oid(2));
                assert_eq!(e.version, 1, "first commit of a created ref is version 1");
            }
            other => panic!("expected CommitedPrepared, got {other:?}"),
        }
    }

    // ── Prepare to create an EXISTING ref (expected=None but present) → VoteNo ─
    #[tokio::test]
    async fn prepare_create_existing_votes_no() {
        let store = make_store();
        let n = name("refs/heads/prepdup");
        store.update(&n, oid(1), None).await.unwrap();
        let outcome = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: None,
                hlc: 50,
            })
            .await;
        assert_eq!(outcome, AppliedOutcome::VoteNo);
    }

    // ── update on a locked ref → Conflict ─────────────────────────────────────
    #[tokio::test]
    async fn update_on_locked_ref_conflicts() {
        let store = make_store();
        let n = name("refs/heads/lockedw");
        store.update(&n, oid(1), None).await.unwrap();
        let _ = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: n.clone(),
                target: oid(2),
                expected: Some(oid(1)),
                hlc: 50,
            })
            .await;
        let res = store.update(&n, oid(9), Some(oid(1))).await;
        assert!(
            matches!(res, Err(LedgeError::Conflict { .. })),
            "a locked ref is busy"
        );
        let res2 = store.delete(&n, oid(1)).await;
        assert!(
            matches!(res2, Err(LedgeError::Conflict { .. })),
            "delete on a locked ref conflicts too"
        );
    }

    // ── commit_batch: all preconditions hold → all applied atomically ─────────
    #[tokio::test]
    async fn commit_batch_all_hold_applies_all() {
        let store = make_store();
        let a = name("refs/heads/a");
        let b = name("refs/heads/b");
        store.update(&a, oid(1), None).await.unwrap(); // v1
        store.update(&b, oid(1), None).await.unwrap(); // v1
        let res = store
            .commit_batch(vec![
                (a.clone(), oid(2), Some(oid(1)), 100),
                (b.clone(), oid(3), Some(oid(1)), 100),
            ])
            .await;
        let applied = res.expect("all preconditions hold");
        assert_eq!(applied.len(), 2);
        assert_eq!(applied[0].target, oid(2));
        assert_eq!(applied[0].version, 2);
        assert_eq!(applied[0].hlc, 100);
        assert_eq!(applied[1].target, oid(3));
        // Both visible through normal reads.
        assert_eq!(store.get(&a).await.unwrap().unwrap().target, oid(2));
        assert_eq!(store.get(&b).await.unwrap().unwrap().target, oid(3));
    }

    // ── commit_batch: create (expected=None) in a batch ───────────────────────
    #[tokio::test]
    async fn commit_batch_creates_new_refs() {
        let store = make_store();
        let a = name("refs/heads/newa");
        let b = name("refs/heads/newb");
        let res = store
            .commit_batch(vec![
                (a.clone(), oid(2), None, 100),
                (b.clone(), oid(3), None, 100),
            ])
            .await;
        let applied = res.expect("creates succeed");
        assert_eq!(applied[0].version, 1);
        assert_eq!(applied[1].version, 1);
        assert_eq!(store.get(&a).await.unwrap().unwrap().target, oid(2));
    }

    // ── commit_batch: one precondition fails → NONE applied (atomic) ──────────
    #[tokio::test]
    async fn commit_batch_one_fails_applies_none() {
        let store = make_store();
        let a = name("refs/heads/at");
        let b = name("refs/heads/bt");
        store.update(&a, oid(1), None).await.unwrap();
        store.update(&b, oid(1), None).await.unwrap();
        // Second op has a stale expected → whole batch must be a no-op.
        let res = store
            .commit_batch(vec![
                (a.clone(), oid(2), Some(oid(1)), 100), // would succeed
                (b.clone(), oid(3), Some(oid(9)), 100), // stale → conflict
            ])
            .await;
        let conflicts = match res.expect_err("one stale precondition aborts the batch") {
            CommitBatchError::Conflicts(c) => c,
            CommitBatchError::NotDurable { .. } => panic!("expected conflicts, not a WAL error"),
        };
        assert!(
            conflicts
                .iter()
                .any(|(n, cur)| n == &b && cur.target == oid(1)),
            "conflict reports b's current committed target"
        );
        // ATOMICITY: neither ref advanced.
        assert_eq!(
            store.get(&a).await.unwrap().unwrap().target,
            oid(1),
            "a untouched"
        );
        assert_eq!(
            store.get(&b).await.unwrap().unwrap().target,
            oid(1),
            "b untouched"
        );
        assert_eq!(store.get(&a).await.unwrap().unwrap().version, 1);
    }

    /// A WAL write failure during commit_batch must NOT be swallowed: the batch
    /// is published in memory (CAS done) but the store returns NotDurable, handing
    /// back the applied entries (for a log-authoritative caller) and the I/O error
    /// (so a single-node caller can refuse to ack).
    #[tokio::test]
    async fn commit_batch_wal_failure_reports_not_durable() {
        let store = make_store();
        store.fail_next_wal_append();
        let res = store
            .commit_batch(vec![
                (name("refs/heads/x"), oid(1), None, 100),
                (name("refs/heads/y"), oid(2), None, 101),
            ])
            .await;
        match res {
            Err(CommitBatchError::NotDurable { applied, source: _ }) => {
                assert_eq!(applied.len(), 2, "applied entries handed back in order");
                assert_eq!(applied[0].target, oid(1));
                assert_eq!(applied[1].target, oid(2));
            }
            other => panic!("expected NotDurable, got {other:?}"),
        }
        // The in-memory CAS did publish — durability, not application, is what failed.
        assert_eq!(
            store
                .get(&name("refs/heads/x"))
                .await
                .unwrap()
                .unwrap()
                .target,
            oid(1)
        );
    }

    // ── commit_batch: a locked ref in the batch → conflict, none applied ──────
    #[tokio::test]
    async fn commit_batch_locked_ref_aborts() {
        let store = make_store();
        let a = name("refs/heads/la");
        let b = name("refs/heads/lb");
        store.update(&a, oid(1), None).await.unwrap();
        store.update(&b, oid(1), None).await.unwrap();
        let _ = store
            .apply_op(&AppliedOp::Prepare {
                txn_id: txn(1),
                coord_shard: 0,
                name: b.clone(),
                target: oid(7),
                expected: Some(oid(1)),
                hlc: 50,
            })
            .await;
        let res = store
            .commit_batch(vec![
                (a.clone(), oid(2), Some(oid(1)), 100),
                (b.clone(), oid(3), Some(oid(1)), 100), // b is locked → conflict
            ])
            .await;
        assert!(res.is_err(), "a batch touching a locked ref aborts");
        assert_eq!(
            store.get(&a).await.unwrap().unwrap().target,
            oid(1),
            "a not advanced"
        );
    }
}
