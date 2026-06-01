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
