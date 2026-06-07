//! Phase 4g — live shard-membership reconfiguration. Drives openraft to change
//! the voter set of an EXISTING shard (num_shards unchanged ⇒ no key reshuffle).
use std::collections::{BTreeMap, BTreeSet};

use ledge_core::{LedgeError, Result};
use ledge_raft::{Node, NodeId, TypeConfig};
use openraft::{ChangeMembers, Raft};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiff {
    pub to_add: BTreeSet<NodeId>,
    pub to_remove: BTreeSet<NodeId>,
}

/// Pure diff: node ids to add / remove to move `current` voters → `target`.
pub fn diff_members(current: &BTreeSet<NodeId>, target: &BTreeSet<NodeId>) -> MemberDiff {
    MemberDiff {
        to_add: target.difference(current).copied().collect(),
        to_remove: current.difference(target).copied().collect(),
    }
}

#[derive(Debug, Clone)]
pub struct ReconfigOutcome {
    pub added: BTreeSet<NodeId>,
    pub removed: BTreeSet<NodeId>,
    pub final_voters: BTreeSet<NodeId>,
}

/// Reconfigure one shard's voter set to exactly `target` (node_id → addr).
///
/// 1. add_learner(blocking) each NEW node (catches up via Raft snapshot+log),
/// 2. change_membership(ReplaceAllVoters(target), retain=false) — promote
///    learners + drop removed voters in one joint-consensus transition.
///
/// Idempotent: target == current voters ⇒ a no-op replace.
pub async fn reconfigure_shard(
    raft: &Raft<TypeConfig>,
    target: BTreeMap<NodeId, String>,
) -> Result<ReconfigOutcome> {
    let current: BTreeSet<NodeId> = {
        let m = raft.metrics().borrow().clone();
        m.membership_config.membership().voter_ids().collect()
    };
    let target_ids: BTreeSet<NodeId> = target.keys().copied().collect();
    let diff = diff_members(&current, &target_ids);

    for id in &diff.to_add {
        let addr = target.get(id).cloned().ok_or_else(|| {
            LedgeError::Unavailable(format!("reconfigure: no addr for new node {id}"))
        })?;
        raft.add_learner(*id, Node::new(addr), true)
            .await
            .map_err(|e| LedgeError::Unavailable(format!("add_learner {id}: {e}")))?;
    }
    raft.change_membership(ChangeMembers::ReplaceAllVoters(target_ids.clone()), false)
        .await
        .map_err(|e| LedgeError::Unavailable(format!("change_membership: {e}")))?;

    Ok(ReconfigOutcome {
        added: diff.to_add,
        removed: diff.to_remove,
        final_voters: target_ids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_members_add_remove() {
        let current: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        let target: BTreeSet<NodeId> = [2, 3, 4].into_iter().collect();
        let d = diff_members(&current, &target);
        assert_eq!(d.to_add, BTreeSet::from([4]));
        assert_eq!(d.to_remove, BTreeSet::from([1]));
    }

    #[test]
    fn diff_members_noop_when_equal() {
        let s: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        let d = diff_members(&s, &s);
        assert!(d.to_add.is_empty() && d.to_remove.is_empty());
    }
}
