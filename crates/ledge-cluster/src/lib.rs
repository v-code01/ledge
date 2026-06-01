//! Ledge cluster: sharding router + Raft networking + multi-node test harness.
//!
//! Task 3 deliverables: the deterministic [`ShardRouter`], an in-process
//! [`net_mem`] Raft network for cluster tests, and a 3-node single-shard
//! [`testkit`] harness that proves per-shard linearizability and crash
//! fault-tolerance against `openraft 0.9.24`.
pub mod net_mem;
pub mod router;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

pub use router::{ShardId, ShardRouter};
