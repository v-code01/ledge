//! In-process multi-node Raft harness for single-shard fault-tolerance tests.
#![cfg(any(test, feature = "testkit"))]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::{Config, ServerState, SnapshotPolicy};

use crate::net_mem::{MemNetworkFactory, Registry};
use crate::router::ShardId;
use ledge_raft::{LogStore, Node, NodeId, ReadHandle, StateMachineStore, TypeConfig};

type RaftHandle = openraft::Raft<TypeConfig>;

/// Fast-timer config for tests. `snapshot_logs = Some(n)` sets a small
/// `LogsSinceLast(n)` so the snapshot test can trigger a snapshot cheaply.
pub fn test_config(snapshot_logs: Option<u64>) -> Arc<Config> {
    let mut c = Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        // Purge logs eagerly so a lagging node is forced onto the snapshot path.
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };
    if let Some(n) = snapshot_logs {
        c.snapshot_policy = SnapshotPolicy::LogsSinceLast(n);
    }
    Arc::new(c.validate().expect("valid raft config"))
}

/// One replica in the test cluster. Holds a [`ReadHandle`] (captured before the
/// `StateMachineStore` is moved into `Raft::new`) so a test can read this
/// replica's applied state directly, bypassing Raft.
pub struct TestNode {
    pub id: NodeId,
    pub raft: RaftHandle,
    pub sm: ReadHandle,
}

pub struct TestCluster {
    pub shard: ShardId,
    pub registry: Registry,
    pub nodes: HashMap<NodeId, TestNode>,
    pub members: BTreeMap<NodeId, Node>,
    /// Snapshot threshold used to construct nodes (so `spawn_node` matches).
    snapshot_logs: Option<u64>,
}

impl TestCluster {
    /// Build (but do not initialize) a single-shard cluster of `node_ids`.
    pub async fn new_single_shard(node_ids: &[NodeId], snapshot_logs: Option<u64>) -> Self {
        let shard = ShardId(0);
        let registry = Registry::new();
        let mut nodes = HashMap::new();
        let mut members = BTreeMap::new();

        let mut c = Self {
            shard,
            registry,
            nodes: HashMap::new(),
            members: BTreeMap::new(),
            snapshot_logs,
        };
        for &id in node_ids {
            let node = c.build_node(id).await;
            members.insert(id, Node::new("inproc"));
            nodes.insert(id, node);
        }
        c.nodes = nodes;
        c.members = members;
        c
    }

    /// Construct one replica (fresh empty stores) and register its handle.
    async fn build_node(&self, id: NodeId) -> TestNode {
        let log = LogStore::default();
        let sm = StateMachineStore::new_temp().await;
        // Capture the read handle BEFORE the SM moves into Raft::new — the SM is
        // not Clone, but the handle shares its ArcSwap'd applied state.
        let read = sm.read_handle();
        let net = MemNetworkFactory::new(self.shard, self.registry.clone());
        let raft = openraft::Raft::new(id, test_config(self.snapshot_logs), net, log, sm)
            .await
            .expect("Raft::new");
        self.registry.register(self.shard, id, raft.clone());
        TestNode { id, raft, sm: read }
    }

    pub fn node(&self, id: NodeId) -> &TestNode {
        self.nodes.get(&id).expect("node exists")
    }

    /// Initialize the membership set on `seed`. Call once after construction.
    pub async fn initialize(&self, seed: NodeId) {
        self.node(seed)
            .raft
            .initialize(self.members.clone())
            .await
            .expect("initialize membership");
    }

    /// Build and register one additional node (empty stores) into this cluster,
    /// returning its id. Used to add a lagging learner mid-test.
    pub async fn spawn_node(&mut self, id: NodeId) -> NodeId {
        let node = self.build_node(id).await;
        self.nodes.insert(id, node);
        id
    }

    /// Poll until a stable leader is observed (and confirms its own Leader
    /// state), or panic after ~30s with the last metrics seen. The budget is
    /// deliberately generous (≈50x the 300-600ms election timeout) so heavy
    /// `cargo test --workspace` parallelism — dozens of independent Raft
    /// clusters contending for cores — cannot starve an election into a
    /// spurious timeout; a real hang still fails in bounded time.
    pub async fn wait_for_leader(&self) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            for n in self.nodes.values() {
                let m = n.raft.metrics().borrow().clone();
                if let Some(l) = m.current_leader {
                    if let Some(ln) = self.nodes.get(&l) {
                        let lm = ln.raft.metrics().borrow().clone();
                        if lm.state == ServerState::Leader {
                            return l;
                        }
                    }
                }
            }
            if Instant::now() >= deadline {
                let dump: Vec<_> = self
                    .nodes
                    .values()
                    .map(|n| {
                        let m = n.raft.metrics().borrow().clone();
                        (n.id, m.state, m.current_leader)
                    })
                    .collect();
                panic!("no leader within 30s; metrics = {dump:?}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// The `Raft` handle for node `id`.
    pub fn leader(&self, id: NodeId) -> &RaftHandle {
        &self.node(id).raft
    }
}

// ---------------------------------------------------------------------------
// Multi-shard harness (Task 4): N shards × M nodes over one shared network
// registry. Each (shard, node) is an independent Raft group; the registry keys
// on (ShardId, NodeId) so shards are network-isolated from one another while
// sharing the same in-process transport.
// ---------------------------------------------------------------------------

/// One replica of one shard: its Raft handle plus a [`ReadHandle`] onto its
/// applied state and the shard-local HLC source used to stamp proposals.
pub struct ShardReplica {
    pub shard: ShardId,
    pub node: NodeId,
    pub raft: RaftHandle,
    pub sm: ReadHandle,
    pub hlc: Arc<ledge_core::HLC>,
}

/// An in-process cluster of `num_shards` shards, each replicated across the
/// same `node_ids`. All shards share one [`Registry`] (network) but are keyed
/// independently, so a write to shard A is invisible to shard B's log.
pub struct MultiShardCluster {
    pub num_shards: u32,
    pub node_ids: Vec<NodeId>,
    pub registry: Registry,
    pub members: BTreeMap<NodeId, Node>,
    /// All replicas of every shard hosted in this process, keyed by shard.
    pub replicas: BTreeMap<ShardId, Vec<ShardReplica>>,
}

impl MultiShardCluster {
    /// Build, initialize, and elect leaders for `num_shards` shards × `node_ids`
    /// replicas. Returns only once every shard has a stable leader.
    pub async fn start(num_shards: u32, node_ids: &[NodeId]) -> Self {
        let registry = Registry::new();
        let mut members = BTreeMap::new();
        for &id in node_ids {
            members.insert(id, Node::new("inproc"));
        }

        let mut replicas: BTreeMap<ShardId, Vec<ShardReplica>> = BTreeMap::new();
        for s in 0..num_shards {
            let shard = ShardId(s);
            // One HLC per shard, shared by that shard's replicas in-process: the
            // leader ticks it at propose time (the value travels in the op).
            let hlc = Arc::new(ledge_core::HLC::new());
            let mut shard_replicas = Vec::with_capacity(node_ids.len());
            for &id in node_ids {
                let log = LogStore::default();
                let sm = StateMachineStore::new_temp().await;
                let read = sm.read_handle();
                let net = MemNetworkFactory::new(shard, registry.clone());
                let raft = openraft::Raft::new(id, test_config(None), net, log, sm)
                    .await
                    .expect("Raft::new");
                registry.register(shard, id, raft.clone());
                shard_replicas.push(ShardReplica {
                    shard,
                    node: id,
                    raft,
                    sm: read,
                    hlc: hlc.clone(),
                });
            }
            replicas.insert(shard, shard_replicas);
        }

        let cluster = Self {
            num_shards,
            node_ids: node_ids.to_vec(),
            registry,
            members,
            replicas,
        };

        // Initialize each shard on its first node, then await a leader.
        for s in 0..num_shards {
            let shard = ShardId(s);
            let seed = cluster.node_ids[0];
            let seed_raft = cluster
                .replicas
                .get(&shard)
                .unwrap()
                .iter()
                .find(|r| r.node == seed)
                .unwrap()
                .raft
                .clone();
            seed_raft
                .initialize(cluster.members.clone())
                .await
                .expect("initialize shard membership");
        }
        for s in 0..num_shards {
            cluster.wait_for_leader(ShardId(s)).await;
        }
        cluster
    }

    /// Build, initialize, and elect leaders for a cluster whose shards live on
    /// DISTINCT node subsets, per `map`. Mirrors `build_cluster_stack`'s
    /// placement decision over the in-memory transport: a (shard, node) Raft
    /// group is built IFF `map.hosts(shard, node)`, and each shard's membership
    /// is exactly `map.members(shard)` — so a node hosts only its assigned
    /// shards and each shard elects among only its own members.
    pub async fn start_placed(map: &crate::shard_map::ShardMap) -> Self {
        let registry = Registry::new();
        // The union of all node ids appearing in any shard (for `node_ids`).
        let mut all_nodes: std::collections::BTreeSet<NodeId> = Default::default();
        let mut replicas: BTreeMap<ShardId, Vec<ShardReplica>> = BTreeMap::new();
        // Per-shard membership (id→Node) for initialize: the shard's OWN members.
        let mut per_shard_members: BTreeMap<ShardId, BTreeMap<NodeId, Node>> = BTreeMap::new();

        for s in 0..map.num_shards() {
            let shard = ShardId(s);
            let members = map.members(shard);
            // One HLC per shard, shared by that shard's replicas in-process.
            let hlc = Arc::new(ledge_core::HLC::new());
            let mut shard_replicas = Vec::with_capacity(members.len());
            let mut members_map: BTreeMap<NodeId, Node> = BTreeMap::new();
            for rep in members {
                let id = rep.node_id;
                all_nodes.insert(id);
                members_map.insert(id, Node::new(rep.addr.clone()));
                let log = LogStore::default();
                let sm = StateMachineStore::new_temp().await;
                let read = sm.read_handle();
                // The in-mem network is keyed on (shard, node); registering only
                // this shard's members means a shard's RPCs only ever reach its
                // own members — the same isolation `member_map(shard)` gives the
                // HTTP factory in build_cluster_stack.
                let net = MemNetworkFactory::new(shard, registry.clone());
                let raft = openraft::Raft::new(id, test_config(None), net, log, sm)
                    .await
                    .expect("Raft::new");
                registry.register(shard, id, raft.clone());
                shard_replicas.push(ShardReplica {
                    shard,
                    node: id,
                    raft,
                    sm: read,
                    hlc: hlc.clone(),
                });
            }
            per_shard_members.insert(shard, members_map);
            replicas.insert(shard, shard_replicas);
        }

        // `members` (the struct field) is unused by the placed path's election
        // (each shard initializes with its OWN member set), but keep the field
        // populated as the union for any introspection callers.
        let members_union: BTreeMap<NodeId, Node> = all_nodes
            .iter()
            .map(|&id| (id, Node::new("inproc")))
            .collect();

        let cluster = Self {
            num_shards: map.num_shards(),
            node_ids: all_nodes.into_iter().collect(),
            registry,
            members: members_union,
            replicas,
        };

        // Initialize EACH shard with ITS OWN member set on the shard's first
        // member (NOT the global node 0, which may not host the shard).
        for s in 0..cluster.num_shards {
            let shard = ShardId(s);
            let members = per_shard_members.get(&shard).unwrap().clone();
            let seed = *members.keys().next().expect("shard has >=1 member");
            let seed_raft = cluster
                .replicas
                .get(&shard)
                .unwrap()
                .iter()
                .find(|r| r.node == seed)
                .unwrap()
                .raft
                .clone();
            seed_raft
                .initialize(members)
                .await
                .expect("initialize shard membership");
        }
        for s in 0..cluster.num_shards {
            cluster.wait_for_leader(ShardId(s)).await;
        }
        cluster
    }

    /// Does this cluster host a replica of `shard` on `node`? (Placement probe.)
    pub fn hosts(&self, shard: ShardId, node: NodeId) -> bool {
        self.replicas
            .get(&shard)
            .is_some_and(|reps| reps.iter().any(|r| r.node == node))
    }

    /// Sorted node ids that are members of `shard`.
    pub fn member_ids(&self, shard: ShardId) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .replicas
            .get(&shard)
            .map(|reps| reps.iter().map(|r| r.node).collect())
            .unwrap_or_default();
        ids.sort_unstable();
        ids
    }

    /// Poll until `shard` has a node confirming `ServerState::Leader`.
    pub async fn wait_for_leader(&self, shard: ShardId) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(30);
        let reps = self.replicas.get(&shard).expect("shard exists");
        loop {
            for r in reps {
                let m = r.raft.metrics().borrow().clone();
                if let Some(l) = m.current_leader {
                    if let Some(lr) = reps.iter().find(|x| x.node == l) {
                        if lr.raft.metrics().borrow().state == ServerState::Leader {
                            return l;
                        }
                    }
                }
            }
            if Instant::now() >= deadline {
                panic!("shard {shard:?}: no leader within 30s");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// All replicas of `shard`.
    pub fn replicas_of(&self, shard: ShardId) -> &[ShardReplica] {
        self.replicas.get(&shard).expect("shard exists")
    }

    /// Build the `ShardId → Vec<ShardHandle>` registry the cluster stores need:
    /// every replica of every shard, so `leader_of` can address the leader
    /// (in-process registry shape; see `ref_store` module docs for production).
    pub fn shard_handles(&self) -> BTreeMap<ShardId, Vec<crate::ref_store::ShardHandle>> {
        let mut out = BTreeMap::new();
        for (&shard, reps) in &self.replicas {
            let handles = reps
                .iter()
                .map(|r| crate::ref_store::ShardHandle {
                    shard: r.shard,
                    node_id: r.node,
                    raft: r.raft.clone(),
                    sm: r.sm.clone(),
                    hlc: r.hlc.clone(),
                })
                .collect();
            out.insert(shard, handles);
        }
        out
    }

    /// A [`ShardRouter`] over this cluster's shard count.
    pub fn router(&self) -> crate::router::ShardRouter {
        crate::router::ShardRouter::new(self.num_shards)
    }

    /// A `ClusterRefStore` built on `node`'s view of the cluster.
    pub fn cluster_ref_store(&self, node: NodeId) -> crate::ref_store::ClusterRefStore {
        crate::ref_store::ClusterRefStore::new(node, self.router(), self.shard_handles())
    }

    /// A `ClusterRefStore` for `node` whose LOCAL handle map contains ONLY the
    /// shards `map` says `node` hosts (the rest are forwarded via `forwarder`).
    /// For the in-memory forwarding test (spec §7): build the underlying Raft
    /// groups for all shards on all nodes, but expose each node as hosting only
    /// its mapped subset. Returned as `Arc` so it can also be registered as a
    /// `LocalApplier` in the forwarder registry.
    pub fn cluster_ref_store_hosting(
        &self,
        node: NodeId,
        map: &crate::shard_map::ShardMap,
        forwarder: std::sync::Arc<dyn crate::forward::RefOpForwarder>,
    ) -> std::sync::Arc<crate::ref_store::ClusterRefStore> {
        let hosted: BTreeMap<_, _> = self
            .shard_handles()
            .into_iter()
            .filter(|(shard, _)| map.hosts(*shard, node))
            .collect();
        std::sync::Arc::new(crate::ref_store::ClusterRefStore::with_placement(
            node,
            self.router(),
            hosted,
            map.clone(),
            forwarder,
        ))
    }

    /// A `ClusterLeaseStore` built on `node`'s view of the cluster.
    pub fn cluster_lease_store(&self, node: NodeId) -> crate::ref_store::ClusterLeaseStore {
        crate::ref_store::ClusterLeaseStore::new(node, self.router(), self.shard_handles())
    }

    /// Does any replica of `shard` have an applied ref `name`? Polls briefly so
    /// async follower application is observed deterministically (no fixed sleep
    /// for correctness — bounded poll on a metrics-equivalent signal).
    pub async fn shard_sm_has_ref(&self, shard: ShardId, name: &ledge_core::RefName) -> bool {
        let reps = self.replicas_of(shard);
        for _ in 0..200 {
            for r in reps {
                if r.sm.applied_ref(name.as_str()).await.is_some() {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // One final check after the deadline.
        for r in reps {
            if r.sm.applied_ref(name.as_str()).await.is_some() {
                return true;
            }
        }
        false
    }

    /// Two ref names the router places on DISTINCT shards (brute-force search).
    /// Panics if `num_shards < 2`.
    pub fn two_names_on_distinct_shards(&self) -> (ledge_core::RefName, ledge_core::RefName) {
        assert!(self.num_shards >= 2, "need >= 2 shards");
        let router = self.router();
        let mut first: Option<(ledge_core::RefName, ShardId)> = None;
        for i in 0..10_000u32 {
            let name = ledge_core::RefName::new(&format!("refs/workspaces/t{i}/main")).unwrap();
            let s = router.shard_for(name.as_str());
            match &first {
                None => first = Some((name, s)),
                Some((f, fs)) if *fs != s => return (f.clone(), name),
                _ => {}
            }
        }
        panic!("could not find two names on distinct shards");
    }

    /// Two DURABLE (`refs/heads/`) ref names the router places on DISTINCT shards
    /// (brute-force search). Unlike [`Self::two_names_on_distinct_shards`] (which
    /// yields `refs/workspaces/` names), these are unconditional GC roots — used
    /// to test `committed_targets_by_shard`, which deliberately excludes workspace
    /// refs (those are lease-gated). Panics if `num_shards < 2`.
    pub fn two_durable_names_on_distinct_shards(
        &self,
    ) -> (ledge_core::RefName, ledge_core::RefName) {
        assert!(self.num_shards >= 2, "need >= 2 shards");
        let router = self.router();
        let mut first: Option<(ledge_core::RefName, ShardId)> = None;
        for i in 0..10_000u32 {
            let name = ledge_core::RefName::new(&format!("refs/heads/b{i}")).unwrap();
            let s = router.shard_for(name.as_str());
            match &first {
                None => first = Some((name, s)),
                Some((f, fs)) if *fs != s => return (f.clone(), name),
                _ => {}
            }
        }
        panic!("could not find two durable names on distinct shards");
    }

    /// `count` ref names spread across BOTH shards (at least one per shard).
    /// Panics if `num_shards != 2` or it cannot satisfy the spread.
    pub fn names_spanning_both_shards(&self, count: usize) -> Vec<ledge_core::RefName> {
        assert_eq!(self.num_shards, 2, "helper assumes exactly 2 shards");
        let router = self.router();
        let mut names = Vec::with_capacity(count);
        let mut seen = [false; 2];
        let mut i = 0u32;
        while names.len() < count {
            let name = ledge_core::RefName::new(&format!("refs/workspaces/t{i}/main")).unwrap();
            let s = router.shard_for(name.as_str()).0 as usize;
            seen[s] = true;
            names.push(name);
            i += 1;
        }
        // Ensure both shards represented; if not, extend until they are.
        while !(seen[0] && seen[1]) {
            let name = ledge_core::RefName::new(&format!("refs/workspaces/t{i}/main")).unwrap();
            let s = router.shard_for(name.as_str()).0 as usize;
            if !seen[s] {
                seen[s] = true;
                names.push(name);
            }
            i += 1;
        }
        names
    }

    /// Crash one replica of `shard`: shut its Raft down AND deregister it from
    /// the network so peers observe it as `Unreachable` (a hard partition, the
    /// shape the failover path relies on). The dead `ShardReplica` is left in
    /// `replicas` so handle indexing stays stable; it simply never wins another
    /// election. Returns once the node's `Raft` has fully shut down.
    pub async fn kill_replica(&self, shard: ShardId, node: NodeId) {
        let reps = self.replicas.get(&shard).expect("shard exists");
        let rep = reps.iter().find(|r| r.node == node).expect("replica exists");
        rep.raft.shutdown().await.expect("raft shutdown");
        self.registry.deregister(shard, node);
    }

    /// Poll until `shard` elects a leader that is NOT `excluded` (used after
    /// killing the prior leader to confirm a *new* one emerged). Bounded poll on
    /// the metrics watch — no fixed sleep gates correctness.
    pub async fn wait_for_new_leader(&self, shard: ShardId, excluded: NodeId) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(30);
        let reps = self.replicas.get(&shard).expect("shard exists");
        loop {
            for r in reps {
                if r.node == excluded {
                    continue;
                }
                let m = r.raft.metrics().borrow().clone();
                if let Some(l) = m.current_leader {
                    if l != excluded {
                        if let Some(lr) = reps.iter().find(|x| x.node == l) {
                            if lr.raft.metrics().borrow().state == ServerState::Leader {
                                return l;
                            }
                        }
                    }
                }
            }
            if Instant::now() >= deadline {
                panic!("shard {shard:?}: no NEW leader (excluding {excluded}) within 30s");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Has any *live* replica of `shard` (excluding `dead`) applied `name`? Polls
    /// briefly so post-failover follower application is observed deterministically.
    pub async fn surviving_replica_has_ref(
        &self,
        shard: ShardId,
        dead: NodeId,
        name: &ledge_core::RefName,
    ) -> bool {
        let reps = self.replicas_of(shard);
        for _ in 0..300 {
            for r in reps {
                if r.node != dead && r.sm.applied_ref(name.as_str()).await.is_some() {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    /// Wait until every replica of `shard` has applied the same number of refs
    /// as the leader's applied ref count, i.e. replication has caught up.
    pub async fn await_applied(&self, shard: ShardId, name: &ledge_core::RefName) {
        let reps = self.replicas_of(shard);
        for _ in 0..300 {
            let mut all = true;
            for r in reps {
                if r.sm.applied_ref(name.as_str()).await.is_none() {
                    all = false;
                    break;
                }
            }
            if all {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("shard {shard:?}: ref {} not replicated to all replicas", name.as_str());
    }
}

#[cfg(test)]
mod placement_tests {
    use super::*;
    use crate::router::ShardId;
    use crate::shard_map::{Replica, ShardMap};

    /// 4 nodes, 2 shards on DISTINCT subsets:
    ///   shard 0 = {1,2,3}, shard 1 = {2,3,4}.
    /// node 1 hosts ONLY shard 0; node 4 hosts ONLY shard 1; nodes 2,3 host both.
    fn distinct_subset_map() -> ShardMap {
        ShardMap::from_entries([
            (
                ShardId(0),
                vec![
                    Replica { node_id: 1, addr: "inproc-1".into() },
                    Replica { node_id: 2, addr: "inproc-2".into() },
                    Replica { node_id: 3, addr: "inproc-3".into() },
                ],
            ),
            (
                ShardId(1),
                vec![
                    Replica { node_id: 2, addr: "inproc-2".into() },
                    Replica { node_id: 3, addr: "inproc-3".into() },
                    Replica { node_id: 4, addr: "inproc-4".into() },
                ],
            ),
        ])
        .expect("valid distinct-subset map")
    }

    #[tokio::test]
    async fn placement_hosts_only_assigned_shards_and_each_shard_elects() {
        let map = distinct_subset_map();
        let cluster = MultiShardCluster::start_placed(&map).await;

        // Hosting: node 1 builds shard 0 only; node 4 builds shard 1 only;
        // node 2 builds both. (start_placed records (shard,node) hosting.)
        assert!(cluster.hosts(ShardId(0), 1));
        assert!(!cluster.hosts(ShardId(1), 1), "node 1 must NOT build shard 1");
        assert!(cluster.hosts(ShardId(1), 4));
        assert!(!cluster.hosts(ShardId(0), 4), "node 4 must NOT build shard 0");
        assert!(cluster.hosts(ShardId(0), 2) && cluster.hosts(ShardId(1), 2));

        // Each shard built a Raft group spanning exactly its members.
        assert_eq!(cluster.member_ids(ShardId(0)), vec![1, 2, 3]);
        assert_eq!(cluster.member_ids(ShardId(1)), vec![2, 3, 4]);

        // Each shard elects a leader AMONG ITS OWN MEMBERS (not a foreign node).
        let l0 = cluster.wait_for_leader(ShardId(0)).await;
        let l1 = cluster.wait_for_leader(ShardId(1)).await;
        assert!([1, 2, 3].contains(&l0), "shard0 leader {l0} must be a shard0 member");
        assert!([2, 3, 4].contains(&l1), "shard1 leader {l1} must be a shard1 member");
    }
}
