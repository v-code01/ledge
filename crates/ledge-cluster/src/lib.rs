//! Ledge cluster: sharding router + Raft networking + multi-node test harness.
//!
//! Task 3 deliverables: the deterministic [`ShardRouter`], an in-process
//! [`net_mem`] Raft network for cluster tests, and a 3-node single-shard
//! [`testkit`] harness that proves per-shard linearizability and crash
//! fault-tolerance against `openraft 0.9.24`.
pub mod forward;
pub mod net_http;
pub mod net_mem;
pub mod object_store;
pub mod ref_store;
pub mod router;
pub mod shard_map;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

pub use forward::{
    ClusterOp, HttpForwarder, InMemoryForwarder, LocalApplier, RefOpForwarder, RefOpResponse,
};
pub use object_store::{LocalObjectPeer, ObjectPeer, ReplicatedObjectStore};
pub use ref_store::{ClusterLeaseStore, ClusterRefStore, ConsistencyMode, ShardHandle};
pub use router::{ShardId, ShardRouter, ShardSpan};
pub use shard_map::{Replica, ShardMap, ShardMapError};
