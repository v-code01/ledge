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
    /// state), or panic after ~10s with the last metrics seen.
    pub async fn wait_for_leader(&self) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(10);
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
                panic!("no leader within 10s; metrics = {dump:?}");
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

    /// Poll until `shard` has a node confirming `ServerState::Leader`.
    pub async fn wait_for_leader(&self, shard: ShardId) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(10);
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
                panic!("shard {shard:?}: no leader within 10s");
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
