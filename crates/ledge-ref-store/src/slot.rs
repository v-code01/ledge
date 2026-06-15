//! `RefSlot` — the ART value type: a committed ref plus an optional 2PC lock.
//!
//! External callers never see `RefSlot`; `get`/`list`/`snapshot` project
//! `.committed` to `RefEntry`. The `prepared` intent is a no-wait write lock
//! held between `Prepare` (vote-yes) and `CommitPrepared`/`AbortPrepared`.

use ledge_core::{ObjectId, RefEntry, TxnId};

/// A prepared-but-not-committed write intent (Phase-1 2PC lock on one ref).
///
/// Carries `coord_shard` so a conflicting writer or the background resolver can
/// look up the owning transaction's decision on its coordinator shard and
/// roll-forward (commit) or release (presumed-abort) the lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedIntent {
    /// Owning transaction.
    pub txn_id: TxnId,
    /// Shard whose Raft log holds the authoritative `TxnDecide` record.
    pub coord_shard: u32,
    /// Target the ref will point to if the txn commits.
    pub staged_target: ObjectId,
    /// HLC the committed entry will carry on commit (deterministic across replicas).
    pub staged_hlc: u64,
}

/// One ref's stored state: the durable committed entry plus an optional lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefSlot {
    /// The currently committed (externally visible) ref entry.
    pub committed: RefEntry,
    /// A prepared write intent, if a 2PC txn currently holds this ref.
    pub prepared: Option<PreparedIntent>,
}

impl RefSlot {
    /// Construct an unlocked slot wrapping a committed entry.
    #[inline]
    pub fn committed(entry: RefEntry) -> Self {
        RefSlot {
            committed: entry,
            prepared: None,
        }
    }

    /// True iff a prepared lock is held by a *different* transaction than `txn`.
    #[inline]
    pub fn locked_by_other(&self, txn: &TxnId) -> bool {
        matches!(&self.prepared, Some(p) if &p.txn_id != txn)
    }
}
