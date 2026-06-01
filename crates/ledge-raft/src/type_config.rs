//! openraft `TypeConfig` for Ledge.
//!
//! Verified against the resolved openraft 0.9.24 source
//! (`src/raft/declare_raft_types_test.rs`): the field order accepted by the
//! macro is `D, R, NodeId, Node, Entry, SnapshotData, AsyncRuntime, Responder`,
//! and `Entry = openraft::Entry<Self>` (the macro substitutes `Self` with the
//! declared config type). `Responder` is left at its default
//! (`OneshotResponder`) — it only matters once a real `Raft` instance proposes
//! writes (Task 3); the determinism work here drives the state machine directly.
//!
//! Intent (holds regardless of macro field syntax): `D = LedgeOp`,
//! `R = LedgeResp`, node id is `u64`, the node descriptor is an address
//! (`BasicNode` carries an addr string), entries are openraft's standard
//! `Entry<TypeConfig>`, snapshots are an in-memory `Cursor<Vec<u8>>`, the async
//! runtime is tokio.

use std::io::Cursor;

use crate::op::{LedgeOp, LedgeResp};

openraft::declare_raft_types!(
    /// Ledge Raft type configuration.
    pub TypeConfig:
        D = LedgeOp,
        R = LedgeResp,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
