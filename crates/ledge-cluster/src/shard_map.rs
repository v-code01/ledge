//! `ShardMap` — the static, per-node-identical shard→replica-set placement.
//!
//! In Phase 4a the map is declared in config and deployed identically to every
//! node, so each node computes the same placement deterministically (no gossip,
//! no consensus on the map itself — see spec §3.1). The type is intentionally
//! shaped so Phase 4g (dynamic resharding) can later back it with a control Raft
//! group without changing the query surface used here.

use std::collections::BTreeMap;

use crate::router::ShardId;

/// A replica node and the base URL it serves its `/raft/*` + `/cluster/*`
/// endpoints on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replica {
    /// Raft node id of this replica (unique within a shard's member list).
    pub node_id: u64,
    /// Base URL (e.g. `http://node3:8403`) for Raft RPC and ref-op forwarding.
    pub addr: String,
}

/// Why a candidate shard map is invalid (rejected at construction so every later
/// query is total over a known-good map).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardMapError {
    /// A shard was declared with zero replicas — it could never elect a leader.
    EmptyShard(ShardId),
    /// A node id appeared twice in one shard's member list (ambiguous replica).
    DuplicateNode { shard: ShardId, node_id: u64 },
}

impl std::fmt::Display for ShardMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardMapError::EmptyShard(s) => write!(f, "shard {s:?} has no replicas"),
            ShardMapError::DuplicateNode { shard, node_id } => {
                write!(f, "node {node_id} appears twice in shard {shard:?}")
            }
        }
    }
}

impl std::error::Error for ShardMapError {}

/// Authoritative shard → replica-set mapping. Identical on every node in 4a.
///
/// `BTreeMap` keying gives a deterministic shard iteration order (so
/// `shards_hosted_by` and any status report are stable across nodes), and the
/// member `Vec` preserves the operator's declared order (so `pick_forward_target`
/// with no leader preference is deterministic — always the first declared member).
#[derive(Clone, Debug, Default)]
pub struct ShardMap {
    shards: BTreeMap<ShardId, Vec<Replica>>,
}

impl ShardMap {
    /// Build a validated map. Rejects empty shards and duplicate node ids within
    /// a shard (spec §4.1). The cluster's shard count and routing both derive
    /// from this map, so a bad map is a hard configuration error caught here.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (ShardId, Vec<Replica>)>,
    ) -> Result<Self, ShardMapError> {
        let mut shards: BTreeMap<ShardId, Vec<Replica>> = BTreeMap::new();
        for (shard, replicas) in entries {
            if replicas.is_empty() {
                return Err(ShardMapError::EmptyShard(shard));
            }
            // O(n^2) dup check; member lists are tiny (RF, typically 3–5).
            for (i, r) in replicas.iter().enumerate() {
                if replicas[..i].iter().any(|p| p.node_id == r.node_id) {
                    return Err(ShardMapError::DuplicateNode {
                        shard,
                        node_id: r.node_id,
                    });
                }
            }
            shards.insert(shard, replicas);
        }
        Ok(Self { shards })
    }

    /// Number of shards (= `ShardRouter::new` argument so routing and placement
    /// agree on the partition count).
    pub fn num_shards(&self) -> u32 {
        self.shards.len() as u32
    }

    /// The replica set for `shard` (empty slice for an unknown shard — total).
    pub fn members(&self, shard: ShardId) -> &[Replica] {
        self.shards.get(&shard).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Whether `node_id` is a member (and thus a host) of `shard`.
    pub fn hosts(&self, shard: ShardId, node_id: u64) -> bool {
        self.members(shard).iter().any(|r| r.node_id == node_id)
    }

    /// The shards `node_id` builds a Raft group for, in ascending shard order.
    pub fn shards_hosted_by(&self, node_id: u64) -> Vec<ShardId> {
        self.shards
            .iter()
            .filter(|(_, replicas)| replicas.iter().any(|r| r.node_id == node_id))
            .map(|(s, _)| *s)
            .collect()
    }

    /// The `id → addr` set for `shard`, for `Raft::initialize` and the per-shard
    /// `HttpRaftNetworkFactory` (only that shard's members — spec §3.3).
    pub fn member_map(&self, shard: ShardId) -> BTreeMap<u64, String> {
        self.members(shard)
            .iter()
            .map(|r| (r.node_id, r.addr.clone()))
            .collect()
    }

    /// All `(shard, replica-set)` entries in ascending shard order. Round-trips
    /// [`Self::from_entries`] — used to rebuild a map on live reconfiguration
    /// (swap one shard's replica set, keep the rest).
    pub fn entries(&self) -> Vec<(ShardId, Vec<Replica>)> {
        self.shards
            .iter()
            .map(|(s, reps)| (*s, reps.clone()))
            .collect()
    }

    /// The address of `node_id` within `shard`, or `None` if it is not a member.
    pub fn replica_addr(&self, shard: ShardId, node_id: u64) -> Option<&str> {
        self.members(shard)
            .iter()
            .find(|r| r.node_id == node_id)
            .map(|r| r.addr.as_str())
    }

    /// Pick a member to forward a remote ref-op to. Prefer `prefer_leader` if it
    /// is a current member of `shard` (saves a `ForwardToLeader` hop on the
    /// hosting node); otherwise fall back to the first declared member. Returns
    /// `None` only for an unknown shard.
    pub fn pick_forward_target(
        &self,
        shard: ShardId,
        prefer_leader: Option<u64>,
    ) -> Option<&Replica> {
        let members = self.members(shard);
        if members.is_empty() {
            return None;
        }
        if let Some(leader) = prefer_leader {
            if let Some(r) = members.iter().find(|r| r.node_id == leader) {
                return Some(r);
            }
        }
        members.first()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::ShardId;

    /// The canonical 5-node / 2-shard distinct-subset map from spec §3.2:
    /// shard 0 → {1,2,3}, shard 1 → {3,4,5}. Node 1 hosts only shard 0,
    /// node 3 hosts both, node 5 hosts only shard 1.
    fn map_5n2s() -> ShardMap {
        ShardMap::from_entries([
            (
                ShardId(0),
                vec![
                    Replica { node_id: 1, addr: "http://n1:8401".into() },
                    Replica { node_id: 2, addr: "http://n2:8402".into() },
                    Replica { node_id: 3, addr: "http://n3:8403".into() },
                ],
            ),
            (
                ShardId(1),
                vec![
                    Replica { node_id: 3, addr: "http://n3:8403".into() },
                    Replica { node_id: 4, addr: "http://n4:8404".into() },
                    Replica { node_id: 5, addr: "http://n5:8405".into() },
                ],
            ),
        ])
        .expect("valid distinct-subset map")
    }

    #[test]
    fn num_shards_counts_entries() {
        assert_eq!(map_5n2s().num_shards(), 2);
    }

    #[test]
    fn members_returns_replica_set() {
        let m = map_5n2s();
        let ids: Vec<u64> = m.members(ShardId(0)).iter().map(|r| r.node_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        // An unknown shard has an empty member slice (total, no panic).
        assert!(m.members(ShardId(9)).is_empty());
    }

    #[test]
    fn hosts_reflects_membership() {
        let m = map_5n2s();
        assert!(m.hosts(ShardId(0), 1));
        assert!(!m.hosts(ShardId(1), 1));
        assert!(m.hosts(ShardId(0), 3) && m.hosts(ShardId(1), 3));
        assert!(m.hosts(ShardId(1), 5) && !m.hosts(ShardId(0), 5));
        assert!(!m.hosts(ShardId(9), 1)); // unknown shard
    }

    #[test]
    fn shards_hosted_by_is_per_node_and_sorted() {
        let m = map_5n2s();
        assert_eq!(m.shards_hosted_by(1), vec![ShardId(0)]);
        assert_eq!(m.shards_hosted_by(3), vec![ShardId(0), ShardId(1)]);
        assert_eq!(m.shards_hosted_by(5), vec![ShardId(1)]);
        assert!(m.shards_hosted_by(99).is_empty());
    }

    #[test]
    fn entries_round_trips_from_entries() {
        let m = map_5n2s();
        // `entries()` reproduces exactly what `from_entries` accepts, so feeding
        // its output back yields an identical map (same shards, same members).
        let rebuilt = ShardMap::from_entries(m.entries()).expect("entries round-trip");
        assert_eq!(rebuilt.num_shards(), m.num_shards());
        for s in 0..m.num_shards() {
            assert_eq!(rebuilt.members(ShardId(s)), m.members(ShardId(s)));
        }
        // And entries are in ascending shard order.
        let shards: Vec<u32> = m.entries().iter().map(|(s, _)| s.0).collect();
        assert_eq!(shards, vec![0, 1]);
    }

    #[test]
    fn member_map_is_id_to_addr() {
        let m = map_5n2s();
        let mm = m.member_map(ShardId(1));
        assert_eq!(mm.len(), 3);
        assert_eq!(mm.get(&3).map(String::as_str), Some("http://n3:8403"));
        assert_eq!(mm.get(&5).map(String::as_str), Some("http://n5:8405"));
        assert!(!mm.contains_key(&1));
    }

    #[test]
    fn replica_addr_resolves_or_none() {
        let m = map_5n2s();
        assert_eq!(m.replica_addr(ShardId(0), 2), Some("http://n2:8402"));
        assert_eq!(m.replica_addr(ShardId(0), 5), None); // not a member of shard 0
        assert_eq!(m.replica_addr(ShardId(9), 1), None); // unknown shard
    }

    #[test]
    fn pick_forward_target_prefers_leader_when_member() {
        let m = map_5n2s();
        // prefer_leader=Some(4) and 4 is a member of shard 1 → pick 4.
        let t = m.pick_forward_target(ShardId(1), Some(4)).expect("a member");
        assert_eq!(t.node_id, 4);
        // prefer_leader names a non-member (1 is not in shard 1) → fall back to
        // the first member deterministically.
        let t = m.pick_forward_target(ShardId(1), Some(1)).expect("a member");
        assert_eq!(t.node_id, 3);
        // No preference → first member.
        let t = m.pick_forward_target(ShardId(0), None).expect("a member");
        assert_eq!(t.node_id, 1);
        // Unknown shard → None.
        assert!(m.pick_forward_target(ShardId(9), None).is_none());
    }

    #[test]
    fn validation_rejects_empty_shard() {
        let err = ShardMap::from_entries([(ShardId(0), vec![])]).unwrap_err();
        assert!(matches!(err, ShardMapError::EmptyShard(ShardId(0))));
    }

    #[test]
    fn validation_rejects_duplicate_node_in_shard() {
        let err = ShardMap::from_entries([(
            ShardId(0),
            vec![
                Replica { node_id: 7, addr: "http://a:1".into() },
                Replica { node_id: 7, addr: "http://b:2".into() },
            ],
        )])
        .unwrap_err();
        assert!(matches!(err, ShardMapError::DuplicateNode { shard: ShardId(0), node_id: 7 }));
    }
}
