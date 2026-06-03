//! Remote ref-op forwarding (spec §4.3/§4.4).
//!
//! When a [`crate::ref_store::ClusterRefStore`] receives a ref op whose target
//! shard it does NOT host, it forwards the op to a node that does, over
//! `POST /cluster/ref-op`. The hosting node applies it through its own local
//! `ClusterRefStore` (assigning the leader HLC there) and returns the result.
//! This module defines the wire op/response, the forwarder trait, an in-memory
//! impl for deterministic tests, and the HTTP impl.
//!
//! HLC NOTE: `Update`/`Delete` carry only `target`/`expected` — NOT an HLC. The
//! HLC is leader-assigned on the HOSTING node (the forwarding node has no leader
//! handle for a shard it does not host), matching the local-path semantics where
//! `leader.hlc.tick()` happens on the resolved leader.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use ledge_core::{RefEntry, Result};

use crate::router::ShardId;

/// A shard-targeted ref op for forwarding. Object ids are raw 32-byte arrays for
/// a serde-trivial, stable wire form (mirrors `LedgeOp`). No HLC field: the
/// hosting node's leader assigns it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterOp {
    /// Create-or-update under CAS.
    Update {
        /// The ref name being written.
        name: String,
        /// The object id the ref should point to after the write.
        target_bytes: [u8; 32],
        /// CAS precondition: the object id the ref must currently point to
        /// (`None` ⇒ create-only / unconditional create).
        expected_bytes: Option<[u8; 32]>,
    },
    /// Delete under CAS.
    Delete {
        /// The ref name being deleted.
        name: String,
        /// CAS precondition: the object id the ref must currently point to.
        expected_bytes: [u8; 32],
    },
    /// Linearizable single-ref read.
    Get {
        /// The ref name to read.
        name: String,
    },
    /// Prefix list (per the target shard only — the caller fans out per shard).
    List {
        /// The prefix to enumerate within the target shard.
        prefix: String,
    },
}

/// The applied result of a forwarded [`ClusterOp`]. Mirrors the `LedgeResp`
/// variants the ref-store path consumes, plus read/list payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefOpResponse {
    /// Ref created/updated; carries the committed entry.
    Updated(RefEntry),
    /// CAS precondition failed; carries the current entry.
    Conflict(RefEntry),
    /// Target ref did not exist for an update-with-expected or a delete.
    NotFound,
    /// Ref was deleted.
    Deleted,
    /// Read result (the entry, or `None` if absent).
    Entry(Option<RefEntry>),
    /// List result for one shard (name string + entry pairs).
    Refs(Vec<(String, RefEntry)>),
}

/// The forwarding seam: apply a shard-targeted op on a node that hosts `shard`.
///
/// Implementations: [`InMemoryForwarder`] (calls the target node's local
/// applier directly — deterministic tests) and [`HttpForwarder`] (POSTs a
/// bincode body to a hosting member). The op is ALREADY shard-targeted; the
/// hosting node does NOT re-route it.
#[async_trait]
pub trait RefOpForwarder: Send + Sync {
    /// Forward `op` for `shard` to a hosting node and return its applied result.
    async fn forward(&self, shard: ShardId, op: ClusterOp) -> Result<RefOpResponse>;
}

/// The local-apply entry point a hosting node exposes: apply an ALREADY
/// shard-targeted op via its local shard handle (the `/cluster/ref-op` handler
/// and the in-memory forwarder both call this). Boxed-future trait so it is
/// object-safe and `dyn`-shareable across the in-memory registry.
#[async_trait]
pub trait LocalApplier: Send + Sync {
    /// Apply `op` against the local handle for `shard`. Errors if this node does
    /// not host `shard` (the caller misdirected the op).
    async fn apply_local(&self, shard: ShardId, op: ClusterOp) -> Result<RefOpResponse>;
}

/// In-memory forwarder for tests: a `node_id → LocalApplier` registry that
/// invokes the target node's applier directly, mirroring the HTTP round-trip
/// without sockets (spec §7 "in-process, via an in-memory forwarder").
#[derive(Default)]
pub struct InMemoryForwarder {
    appliers: std::sync::Mutex<std::collections::BTreeMap<u64, std::sync::Arc<dyn LocalApplier>>>,
    map: std::sync::Mutex<Option<crate::shard_map::ShardMap>>,
}

impl InMemoryForwarder {
    /// Empty registry; populate with [`register`](Self::register) and the map
    /// with [`set_map`](Self::set_map).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a hosting node's local applier under its node id.
    pub fn register(&self, node_id: u64, applier: std::sync::Arc<dyn LocalApplier>) {
        self.appliers.lock().unwrap().insert(node_id, applier);
    }

    /// Set the shard map used to pick a forward target.
    pub fn set_map(&self, map: crate::shard_map::ShardMap) {
        *self.map.lock().unwrap() = Some(map);
    }
}

#[async_trait]
impl RefOpForwarder for InMemoryForwarder {
    async fn forward(&self, shard: ShardId, op: ClusterOp) -> Result<RefOpResponse> {
        // Pick any member of the target shard (no leader preference in-mem).
        let target = {
            let guard = self.map.lock().unwrap();
            let map = guard
                .as_ref()
                .ok_or_else(|| ledge_core::LedgeError::Unavailable("forwarder map unset".into()))?;
            map.pick_forward_target(shard, None)
                .map(|r| r.node_id)
                .ok_or_else(|| {
                    ledge_core::LedgeError::Unavailable(format!("no host for shard {shard:?}"))
                })?
        };
        let applier = {
            let guard = self.appliers.lock().unwrap();
            guard.get(&target).cloned().ok_or_else(|| {
                ledge_core::LedgeError::Unavailable(format!("no applier for node {target}"))
            })?
        };
        applier.apply_local(shard, op).await
    }
}

/// HTTP forwarder: POSTs a bincode `(ShardId, ClusterOp)` to a hosting member's
/// `/cluster/ref-op` and decodes a bincode `RefOpResponse`. The live round-trip
/// test is in section 2's endpoint task; the type is defined here so
/// `build_cluster_stack` can construct it.
pub struct HttpForwarder {
    map: crate::shard_map::ShardMap,
    client: reqwest::Client,
}

impl HttpForwarder {
    /// Construct over the shard map (for target selection) and a shared client.
    pub fn new(map: crate::shard_map::ShardMap, client: reqwest::Client) -> Self {
        Self { map, client }
    }
}

#[async_trait]
impl RefOpForwarder for HttpForwarder {
    async fn forward(&self, shard: ShardId, op: ClusterOp) -> Result<RefOpResponse> {
        let target = self.map.pick_forward_target(shard, None).ok_or_else(|| {
            ledge_core::LedgeError::Unavailable(format!("no host for shard {shard:?}"))
        })?;
        let url = format!("{}/cluster/ref-op", target.addr.trim_end_matches('/'));
        // Wire body: bincode `(ShardId, ClusterOp)` (spec §4.4). bincode 2.x
        // serde API with the crate-standard config (matches `ledge-raft`).
        let body = bincode::serde::encode_to_vec((shard, &op), bincode::config::standard())
            .map_err(|e| ledge_core::LedgeError::Unavailable(format!("encode ref-op: {e}")))?;
        let resp = self
            .client
            .post(&url)
            .body(body)
            .send()
            .await
            .map_err(|e| ledge_core::LedgeError::Unavailable(format!("forward POST {url}: {e}")))?;
        if !resp.status().is_success() {
            return Err(ledge_core::LedgeError::Unavailable(format!(
                "forward {url} -> HTTP {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ledge_core::LedgeError::Unavailable(format!("forward body: {e}")))?;
        bincode::serde::decode_from_slice::<RefOpResponse, _>(&bytes, bincode::config::standard())
            .map(|(resp, _)| resp)
            .map_err(|e| ledge_core::LedgeError::Unavailable(format!("decode ref-op resp: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use ledge_core::{ObjectId, RefStore};

    use crate::ref_store::StoreApplier;
    use crate::router::ShardId;
    use crate::shard_map::{Replica, ShardMap};
    use crate::testkit::MultiShardCluster;

    fn oid(b: u8) -> ObjectId {
        ObjectId::from_bytes([b; 32])
    }

    /// Two nodes, two shards. Node 1 hosts ONLY shard 0; node 2 hosts ONLY
    /// shard 1. A ref that routes to shard 1 written through node 1 must forward
    /// to node 2 and be readable from node 2 (and from the forwarding node 1).
    /// This mirrors the HTTP path deterministically (no sockets) via an
    /// in-memory forwarder that calls the target node's local applier directly.
    ///
    /// NOTE: `MultiShardCluster` builds every shard on every node (Phase 3) for
    /// the underlying Raft groups, but each `ClusterRefStore` is constructed with
    /// a LOCAL-ONLY handle map (the node's hosted shard) + a forwarder to the
    /// other node, exercising the forward path exactly as production will.
    #[tokio::test]
    async fn non_hosting_node_forwards_update_get_delete() {
        let cluster = MultiShardCluster::start(2, &[1, 2]).await;

        // Placement: shard 0 → node 1, shard 1 → node 2 (distinct subsets).
        let map = ShardMap::from_entries([
            (ShardId(0), vec![Replica { node_id: 1, addr: "mem://1".into() }]),
            (ShardId(1), vec![Replica { node_id: 2, addr: "mem://2".into() }]),
        ])
        .unwrap();

        // Build the forwarder, then the local-only stores wired to it, then
        // register each store's local applier. The registry is populated AFTER
        // the Arcs exist (no consume/clone tangle); there is no infinite
        // recursion because store1 forwards a shard-1 op to node 2's applier
        // (store2), which hosts shard 1 locally and only applies (never forwards).
        let fwd = Arc::new(InMemoryForwarder::new());
        fwd.set_map(map.clone());
        let store1 = cluster.cluster_ref_store_hosting(1, &map, fwd.clone());
        let store2 = cluster.cluster_ref_store_hosting(2, &map, fwd.clone());
        fwd.register(1, Arc::new(StoreApplier(store1.clone())));
        fwd.register(2, Arc::new(StoreApplier(store2.clone())));

        // Pick two names that route to distinct shards (testkit helper).
        let (name_a, name_b) = cluster.two_names_on_distinct_shards();
        // Identify which of the two names lands on shard 1 (the remote shard for
        // node 1) and which lands on shard 0 (local to node 1).
        let router = cluster.router();
        let (name_s0, name_s1) = if router.shard_for(name_a.as_str()) == ShardId(0) {
            (name_a, name_b)
        } else {
            (name_b, name_a)
        };

        // Write the shard-1 ref THROUGH node 1 (which does not host shard 1):
        let e = store1.update(&name_s1, oid(0xaa), None).await.unwrap();
        assert_eq!(e.target, oid(0xaa));
        assert_eq!(e.version, 1);

        // Readable from the hosting node (node 2) AND from the forwarding node.
        assert_eq!(store2.get(&name_s1).await.unwrap().unwrap().target, oid(0xaa));
        assert_eq!(store1.get(&name_s1).await.unwrap().unwrap().target, oid(0xaa));

        // CAS through the forwarder: correct expected succeeds, wrong conflicts.
        let e2 = store1
            .update(&name_s1, oid(0xbb), Some(oid(0xaa)))
            .await
            .unwrap();
        assert_eq!(e2.version, 2);
        let conflict = store1.update(&name_s1, oid(0xcc), Some(oid(0xaa))).await;
        assert!(matches!(
            conflict,
            Err(ledge_core::LedgeError::Conflict { .. })
        ));

        // Delete through the forwarder.
        store1.delete(&name_s1, oid(0xbb)).await.unwrap();
        assert!(store2.get(&name_s1).await.unwrap().is_none());

        // Sanity: a LOCAL shard-0 write through node 1 stays on the fast path.
        let el = store1.update(&name_s0, oid(0x11), None).await.unwrap();
        assert_eq!(el.target, oid(0x11));
    }

    /// `apply_local_op` applies a SHARD-TARGETED op directly to the local handle
    /// without re-routing, and rejects an op for a shard this node does not host.
    #[tokio::test]
    async fn apply_local_op_applies_directly_and_rejects_misdirected() {
        let cluster = MultiShardCluster::start(2, &[1, 2]).await;
        let map = ShardMap::from_entries([
            (ShardId(0), vec![Replica { node_id: 1, addr: "mem://1".into() }]),
            (ShardId(1), vec![Replica { node_id: 2, addr: "mem://2".into() }]),
        ])
        .unwrap();
        let fwd = Arc::new(InMemoryForwarder::new());
        fwd.set_map(map.clone());
        let store1 = cluster.cluster_ref_store_hosting(1, &map, fwd.clone());

        // Direct local apply of a shard-0 op (node 1 hosts shard 0). Pick a name
        // on shard 0.
        let (name_a, name_b) = cluster.two_names_on_distinct_shards();
        let router = cluster.router();
        let name_s0 = if router.shard_for(name_a.as_str()) == ShardId(0) {
            name_a
        } else {
            name_b
        };
        let resp = store1
            .apply_local_op(
                ShardId(0),
                ClusterOp::Update {
                    name: name_s0.as_str().to_string(),
                    target_bytes: *oid(0x42).as_bytes(),
                    expected_bytes: None,
                },
            )
            .await
            .unwrap();
        match resp {
            RefOpResponse::Updated(e) => assert_eq!(e.target, oid(0x42)),
            other => panic!("expected Updated, got {other:?}"),
        }

        // Misdirected: shard 1 is not hosted locally on node 1 → error (the
        // applier must NOT re-route).
        let err = store1
            .apply_local_op(ShardId(1), ClusterOp::Get { name: "x".into() })
            .await;
        assert!(matches!(err, Err(ledge_core::LedgeError::Unavailable(_))));
    }
}
