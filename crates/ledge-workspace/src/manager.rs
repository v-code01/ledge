//! Workspace lifecycle manager: fork / renew / commit / release / get / list.
//!
//! A [`WorkspaceManager`] is the orchestration layer over the Phase 1 ref store
//! and the Task 2 lease store. It owns no storage of its own; every mutation is
//! a ref-store CAS or a lease-store WAL append. The manager enforces the
//! workspace invariants documented in the Phase 2a plan §3.0:
//!
//! 1. **Rebase**: a workspace ref is the source ref with its leading `refs/`
//!    stripped and re-rooted under `refs/workspaces/<hex-id>/`.
//! 2. **Object sharing**: fork copies only the ref *delta* (target `ObjectId`s),
//!    never objects (content addressing makes the copy free).
//! 3. **Commit = CAS promotion**: commit reads the live durable entry and uses
//!    it as the `expected` precondition; a concurrent durable mutation surfaces
//!    as [`CommitOutcome::Conflict`], never silent data loss. Commit does not
//!    release the workspace.
//! 4. **Idempotent release**: release deletes every `refs/workspaces/<id>/*`
//!    ref and tombstones the lease; calling it twice is `Ok(())`.
//! 5. **Client-facing names**: every view presents client-facing ref names
//!    (`refs/heads/main`), never the storage form.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ledge_core::{LedgeError, RefEntry, RefName, RefStore, Result, HLC};
use ledge_ref_store::RefStoreImpl;

use crate::id::WorkspaceId;
use crate::lease::{Lease, LeaseStore};

/// Orchestrates the workspace lifecycle over a ref store and a lease store.
///
/// Holds the concrete [`RefStoreImpl`] (the only implementation; server routes
/// need the concrete type) but every method body calls only [`RefStore`] trait
/// methods, so the logic is implementation-agnostic.
pub struct WorkspaceManager {
    refs: Arc<RefStoreImpl>,
    leases: Arc<LeaseStore>,
    hlc: Arc<HLC>,
}

/// A point-in-time view of a workspace: its id, governing lease, and the set of
/// refs it carries, presented with **client-facing** names (`refs/heads/…`).
#[derive(Debug, Clone)]
pub struct WorkspaceView {
    pub id: WorkspaceId,
    pub lease: Lease,
    /// Client-facing ref names (`refs/heads/main`), never the storage form.
    pub refs: Vec<(String, RefEntry)>,
}

/// The result of promoting one workspace ref to a durable ref during `commit`.
#[derive(Debug, Clone)]
pub enum CommitOutcome {
    /// The durable ref was created or CAS-updated to the workspace's target.
    Ok { target: String, entry: RefEntry },
    /// The durable ref moved under the manager between read and write; the
    /// promotion was rejected and `current` holds the live durable entry the
    /// caller must reconcile against. The durable ref is never clobbered.
    Conflict { target: String, current: RefEntry },
}

/// Wall-clock milliseconds since the Unix epoch. Monotonic enough for TTLs;
/// a backward clock step only shortens a lease (fail-safe — never extends it).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Re-root a source ref under the workspace namespace (plan §3.2).
/// `refs/heads/main` → `refs/workspaces/<hex>/heads/main`.
fn workspace_ref(id: &WorkspaceId, source: &RefName) -> Result<RefName> {
    let suffix = source
        .as_str()
        .strip_prefix("refs/")
        .ok_or_else(|| LedgeError::InvalidRefName(source.as_str().to_string()))?;
    RefName::new(&format!("refs/workspaces/{}/{}", id.to_hex(), suffix))
}

/// Map a stored workspace ref back to its client-facing name.
/// `refs/workspaces/<hex>/heads/main` → `refs/heads/main`.
fn client_ref(id: &WorkspaceId, stored: &str) -> String {
    let prefix = format!("refs/workspaces/{}/", id.to_hex());
    match stored.strip_prefix(&prefix) {
        Some(rest) => format!("refs/{rest}"),
        None => stored.to_string(),
    }
}

impl WorkspaceManager {
    /// Construct a manager over a ref store, lease store, and shared clock.
    pub fn new(refs: Arc<RefStoreImpl>, leases: Arc<LeaseStore>, hlc: Arc<HLC>) -> Self {
        Self { refs, leases, hlc }
    }

    /// Fork a workspace from `source` refs with lifetime `ttl`.
    ///
    /// For each source ref: read its durable entry; if present, create the
    /// re-rooted workspace ref (`expected = None` => create-if-absent) sharing
    /// the same target `ObjectId` (objects are never copied — content
    /// addressing). A missing source ref is an error (Corruption naming it).
    ///
    /// Complexity: O(n) ref reads + O(n) ref creates for n source refs, plus one
    /// lease put. Side effects: n ref-store WAL appends + one lease WAL append.
    pub async fn fork(&self, source: &[RefName], ttl: Duration) -> Result<WorkspaceView> {
        let id = WorkspaceId::generate(&self.hlc);

        let mut view_refs: Vec<(String, RefEntry)> = Vec::with_capacity(source.len());
        let mut source_names: Vec<String> = Vec::with_capacity(source.len());

        for src in source {
            let entry = self.refs.get(src).await?.ok_or_else(|| {
                LedgeError::Corruption(format!("fork: source ref does not exist: {}", src.as_str()))
            })?;
            let ws = workspace_ref(&id, src)?;
            // create-if-absent: a brand-new workspace namespace must be empty.
            self.refs.update(&ws, entry.target, None).await?;
            view_refs.push((src.as_str().to_string(), entry));
            source_names.push(src.as_str().to_string());
        }

        let created = now_ms();
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        let expires = created.saturating_add(ttl_ms);

        let lease = Lease {
            id,
            source_refs: source_names,
            created_at_ms: created,
            expires_at_ms: expires,
            hlc: self.hlc.tick(),
            generation: 1,
        };
        self.leases.put(lease.clone()).await?;

        Ok(WorkspaceView {
            id,
            lease,
            refs: view_refs,
        })
    }

    /// Extend a workspace's lease by `ttl` from *now*. Bumps `generation` and the
    /// HLC stamp so a concurrent GC pass orders this mutation last-writer-wins.
    /// Errors if the lease is unknown (Corruption naming the id).
    ///
    /// Complexity: one lease get + one lease put. Side effect: one lease WAL append.
    pub async fn renew(&self, id: WorkspaceId, ttl: Duration) -> Result<Lease> {
        let mut lease = self.leases.get(id).await?.ok_or_else(|| {
            LedgeError::Corruption(format!("renew: unknown workspace {}", id.to_hex()))
        })?;
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        lease.expires_at_ms = now_ms().saturating_add(ttl_ms);
        lease.generation += 1;
        lease.hlc = self.hlc.tick();
        self.leases.put(lease.clone()).await?;
        Ok(lease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::ObjectId;
    use tempfile::TempDir;

    /// A deterministic, distinct ObjectId for seeding refs. `n` varies the last
    /// byte so each id is unique. `ObjectId` wraps a 32-byte BLAKE3 digest.
    fn oid(n: u8) -> ObjectId {
        let mut bytes = [0u8; 32];
        bytes[31] = n;
        ObjectId::from_bytes(bytes)
    }

    /// Build a manager backed by a real ref store + lease store over tempdirs.
    /// Returns the manager and the TempDir guard (drop = cleanup).
    fn setup() -> (WorkspaceManager, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let hlc = Arc::new(HLC::new());
        let refs =
            Arc::new(RefStoreImpl::open(dir.path().join("refs"), hlc.clone()).expect("ref store"));
        let leases =
            Arc::new(LeaseStore::open(dir.path().join("leases"), hlc.clone()).expect("lease store"));
        let mgr = WorkspaceManager::new(refs, leases, hlc);
        (mgr, dir)
    }

    /// Convenience: a RefName from a &str, panicking on invalid (test-only).
    fn r(s: &str) -> RefName {
        RefName::new(s).expect("valid ref name")
    }

    #[tokio::test]
    async fn fork_copies_source_refs_with_same_targets() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        let target = oid(1);
        // Seed a durable ref (create-if-absent uses expected = None).
        mgr.refs.update(&main, target, None).await.unwrap();

        let view = mgr
            .fork(&[main.clone()], Duration::from_secs(60))
            .await
            .unwrap();

        // View presents the CLIENT-facing source name with the SAME target.
        assert_eq!(view.refs.len(), 1);
        assert_eq!(view.refs[0].0, "refs/heads/main");
        assert_eq!(view.refs[0].1.target, target);

        // The stored workspace ref exists and shares the target ObjectId.
        let ws = workspace_ref(&view.id, &main).unwrap();
        let stored = mgr.refs.get(&ws).await.unwrap().expect("ws ref present");
        assert_eq!(stored.target, target);

        // The lease is recorded and live.
        let lease = mgr.leases.get(view.id).await.unwrap().expect("lease present");
        assert_eq!(lease.generation, 1);
        assert_eq!(lease.source_refs, vec!["refs/heads/main".to_string()]);
        assert!(lease.expires_at_ms > lease.created_at_ms);
    }

    #[tokio::test]
    async fn fork_missing_source_ref_errors() {
        let (mgr, _dir) = setup();
        let absent = r("refs/heads/nope");
        let err = mgr
            .fork(&[absent], Duration::from_secs(60))
            .await
            .unwrap_err();
        match err {
            LedgeError::Corruption(msg) => assert!(msg.contains("refs/heads/nope")),
            other => panic!("expected Corruption naming the ref, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fork_multiple_sources_copies_all() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        let dev = r("refs/heads/dev");
        let tag = r("refs/tags/v1");
        mgr.refs.update(&main, oid(1), None).await.unwrap();
        mgr.refs.update(&dev, oid(2), None).await.unwrap();
        mgr.refs.update(&tag, oid(3), None).await.unwrap();

        let view = mgr
            .fork(
                &[main.clone(), dev.clone(), tag.clone()],
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        assert_eq!(view.refs.len(), 3);
        // Each stored workspace ref shares the matching source target.
        for (src, want) in [(&main, oid(1)), (&dev, oid(2)), (&tag, oid(3))] {
            let ws = workspace_ref(&view.id, src).unwrap();
            let got = mgr.refs.get(&ws).await.unwrap().expect("ws ref");
            assert_eq!(got.target, want);
        }
        // tags/ segment is preserved under the workspace prefix (rebase rule).
        let ws_tag = workspace_ref(&view.id, &tag).unwrap();
        assert!(ws_tag.as_str().contains("/tags/v1"));
        assert!(ws_tag.as_str().starts_with("refs/workspaces/"));
    }

    #[tokio::test]
    async fn renew_bumps_expiry_and_generation() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        mgr.refs.update(&main, oid(1), None).await.unwrap();
        let view = mgr.fork(&[main], Duration::from_secs(1)).await.unwrap();
        let before = view.lease.clone();

        let renewed = mgr.renew(view.id, Duration::from_secs(3600)).await.unwrap();

        assert_eq!(renewed.id, before.id);
        assert_eq!(renewed.generation, before.generation + 1);
        assert!(
            renewed.expires_at_ms > before.expires_at_ms,
            "renew must extend expiry: {} !> {}",
            renewed.expires_at_ms,
            before.expires_at_ms
        );
        assert!(renewed.hlc > before.hlc, "renew must advance the HLC stamp");

        // The bump is persisted, not just returned.
        let persisted = mgr.leases.get(view.id).await.unwrap().expect("lease");
        assert_eq!(persisted.generation, renewed.generation);
        assert_eq!(persisted.expires_at_ms, renewed.expires_at_ms);
    }
}
