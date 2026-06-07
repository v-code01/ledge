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

use ledge_core::{LedgeError, ObjectId, RefEntry, RefName, RefStore, Result, HLC};
use ledge_ref_store::{AtomicCommit, AtomicCommitResult};

use crate::id::WorkspaceId;
use crate::lease::{Lease, LeaseStore};
use crate::quota::{QuotaLimits, UsageMap};

/// Orchestrates the workspace lifecycle over a ref store and a lease store.
///
/// Holds an `Arc<dyn RefStore>` (single-node [`ledge_ref_store::RefStoreImpl`]
/// or clustered `ledge_cluster::ClusterRefStore`); every method body calls only
/// [`RefStore`] trait methods, so the logic is implementation-agnostic and the
/// concrete backing store is injected by the server at assembly time.
pub struct WorkspaceManager {
    refs: Arc<dyn RefStore>,
    leases: Arc<LeaseStore>,
    hlc: Arc<HLC>,
    /// All-or-nothing durable-ref promotion seam. Single-node injects
    /// `ledge_ref_store::LocalAtomicCommit` (one ArcSwap root swap over the same
    /// `RefStoreImpl`); clustered injects `ledge_cluster::TxnCoordinator` (single-
    /// shard `RefBatch` fast path + multi-shard 2PC). Plugged in at assembly, so
    /// this crate never depends on `ledge-cluster` (which would close a cycle).
    coordinator: Arc<dyn AtomicCommit>,
    /// Per-tenant durable quota limits (Phase 4d-3, R Q1). Held by value (Copy);
    /// `QuotaLimits::default()` (enabled=false) ⇒ every gate is a no-op
    /// (byte-identical to Phase 4d-2). Read by `fork` (workspace count) and
    /// `commit` (durable bytes/objects).
    quotas: QuotaLimits,
    /// The shared last-GC-measured per-tenant usage (R Q4). `commit`'s SOFT
    /// storage gate reads this `ArcSwap` snapshot; the GC writes it. The SAME
    /// `Arc` the server creates and injects into the GC + `QuotaCtx`.
    usage: Arc<UsageMap>,
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

/// Normalize a tenant id: the empty string (a legacy lease decoded without a
/// `tenant_id`) is the synthetic `root` tenant. Applied to BOTH sides of every
/// ownership comparison and stamped onto every new lease, so `""` (legacy) and
/// `"root"` (synthetic) are one tenant — matching `ledge_core::tenant_prefix`'s
/// `root == ""` semantics (Phase 4d-2 R7).
fn tenant_norm(t: &str) -> &str {
    if t.is_empty() {
        "root"
    } else {
        t
    }
}

/// A cross-tenant ownership mismatch surfaces as `NotFound` (never `Conflict` or
/// a tenant-naming error) so the HTTP layer maps it to 404 — existence is never
/// revealed (Phase 4d-2 spec §5). The carried `ObjectId` is a zero sentinel; it
/// is never shown to the client (only the 404 status is).
fn cross_tenant_not_found() -> LedgeError {
    LedgeError::NotFound(ObjectId::from_bytes([0u8; 32]))
}

/// Re-root a CLIENT-facing durable ref into the tenant's physical namespace.
/// `refs/heads/main` + tenant `acme` → `refs/tenants/acme/heads/main`; root → unchanged.
/// (Wraps `ledge_core::tenant_prefix` as the `ledge-git` `store_ref` does.)
fn durable_ref(tenant: &str, client: &RefName) -> Result<RefName> {
    let prefix = ledge_core::tenant_prefix(tenant);
    if prefix.is_empty() {
        return Ok(client.clone());
    }
    let rest = client
        .as_str()
        .strip_prefix("refs/")
        .ok_or_else(|| LedgeError::InvalidRefName(client.as_str().to_string()))?;
    RefName::new(&format!("refs/{prefix}{rest}"))
}

impl WorkspaceManager {
    /// Construct a manager over a ref store, lease store, shared clock, and the
    /// atomic-commit seam used to promote workspace refs to durable refs.
    pub fn new(
        refs: Arc<dyn RefStore>,
        leases: Arc<LeaseStore>,
        hlc: Arc<HLC>,
        coordinator: Arc<dyn AtomicCommit>,
        quotas: QuotaLimits,
        usage: Arc<UsageMap>,
    ) -> Self {
        Self {
            refs,
            leases,
            hlc,
            coordinator,
            quotas,
            usage,
        }
    }

    /// The storage prefix for a workspace's refs.
    fn ws_prefix(id: &WorkspaceId) -> String {
        format!("refs/workspaces/{}/", id.to_hex())
    }

    /// Load a lease and verify it belongs to `tenant`. `Ok(Some(lease))` if owned,
    /// `Ok(None)` if the lease is absent/tombstoned, `Err(NotFound)` if it exists
    /// but belongs to a DIFFERENT tenant (404 — no existence leak, spec §5).
    async fn owned_lease(&self, id: WorkspaceId, tenant: &str) -> Result<Option<Lease>> {
        match self.leases.get(id).await? {
            None => Ok(None),
            Some(l) if tenant_norm(&l.tenant_id) == tenant_norm(tenant) => Ok(Some(l)),
            Some(_) => Err(cross_tenant_not_found()),
        }
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
    #[tracing::instrument(skip(self, source), fields(ttl_secs = ttl.as_secs()))]
    pub async fn fork(
        &self,
        source: &[RefName],
        ttl: Duration,
        tenant_id: &str,
    ) -> Result<WorkspaceView> {
        let tenant = tenant_norm(tenant_id);

        // Phase 4d-3: workspace-count quota (EXACT — the lease index is the
        // source of truth, R Q6). Enforced only when enabled AND tenant != root
        // (R Q7). Counts the tenant's LIVE workspaces; at/over the limit ⇒ 507.
        if self.quotas.enforced_for(tenant) {
            if let Some(max) = self.quotas.max_workspaces {
                let live = self.leases.live_for_tenant(now_ms(), tenant).await?.len() as u64;
                if live >= max {
                    return Err(LedgeError::QuotaExceeded(format!(
                        "workspaces: {max} limit reached"
                    )));
                }
            }
        }

        let id = WorkspaceId::generate(&self.hlc);

        let mut view_refs: Vec<(String, RefEntry)> = Vec::with_capacity(source.len());
        let mut source_names: Vec<String> = Vec::with_capacity(source.len());

        for src in source {
            // Read the tenant's OWN durable ref (root ⇒ identity, today's path).
            let durable = durable_ref(tenant, src)?;
            let entry = self.refs.get(&durable).await?.ok_or_else(|| {
                LedgeError::Corruption(format!("fork: source ref does not exist: {}", src.as_str()))
            })?;
            // The workspace ref is re-rooted from the CLIENT-facing source name,
            // so the view + workspace namespace are tenant-agnostic (isolation is
            // the lease.tenant_id + the unguessable id, not a physical prefix).
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
            tenant_id: tenant.to_string(),
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
    #[tracing::instrument(skip(self), fields(id = %id, ttl_secs = ttl.as_secs()))]
    pub async fn renew(&self, id: WorkspaceId, ttl: Duration, tenant_id: &str) -> Result<Lease> {
        let mut lease = self.owned_lease(id, tenant_id).await?.ok_or_else(|| {
            LedgeError::Corruption(format!("renew: unknown workspace {}", id.to_hex()))
        })?;
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        lease.expires_at_ms = now_ms().saturating_add(ttl_ms);
        lease.generation += 1;
        lease.hlc = self.hlc.tick();
        self.leases.put(lease.clone()).await?;
        Ok(lease)
    }

    /// Promote workspace refs to durable targets **atomically** (all-or-nothing).
    /// For each `(ws_ref, durable_ref)` mapping:
    ///   - read the workspace ref entry; skip the mapping if it is absent
    ///     (nothing to promote — not an error, mirrors git's silent no-op);
    ///   - read the current durable entry to form the CAS precondition:
    ///       * absent  => `expected = None`            (create-only)
    ///       * present => `expected = Some(cur.target)` (CAS)
    /// then hand the ENTIRE promotion set to the injected [`AtomicCommit`] seam in
    /// ONE call. Either every durable ref advances together ([`AtomicCommitResult::
    /// Committed`] ⇒ every mapping is [`CommitOutcome::Ok`]) or NONE does
    /// ([`AtomicCommitResult::Aborted`] ⇒ the conflicting refs surface as
    /// [`CommitOutcome::Conflict`] and no durable ref moved). This is the change
    /// from the old sequential per-ref loop, which could leave a partial commit
    /// (first ref advanced, later ref failed); the seam makes the batch atomic
    /// single-node (one `ArcSwap` swap) and cross-shard (2PC).
    ///
    /// The `expected` snapshot is read here at build time; the coordinator
    /// re-validates each precondition atomically at apply/prepare time, so a
    /// concurrent durable mutation between the read and the apply still aborts the
    /// whole batch (no lost update). Does NOT release the workspace.
    ///
    /// Complexity: O(m) ref reads to build the set + one atomic commit for m
    /// mappings.
    #[tracing::instrument(skip(self, mappings), fields(id = %id, mappings = mappings.len()))]
    pub async fn commit(
        &self,
        id: WorkspaceId,
        mappings: &[(RefName, RefName)],
        tenant_id: &str,
    ) -> Result<Vec<CommitOutcome>> {
        // Ownership: a foreign workspace id is a 404 (never reveal it exists).
        let tenant = tenant_norm(tenant_id);
        if self.owned_lease(id, tenant).await?.is_none() {
            return Err(LedgeError::Corruption(format!(
                "commit: unknown workspace {}",
                id.to_hex()
            )));
        }

        // Phase 4d-3: durable storage quota — SOFT, against the LAST GC-measured
        // usage (spec §3.4, R Q6/Q9). Enforced only when enabled AND tenant != root
        // (R Q7). At/over a limit ⇒ QuotaExceeded (→507). SOFT by construction: a
        // commit that crosses from under to over SUCCEEDS (the pre-gate saw under);
        // the NEXT commit after the GC re-measures is rejected (overshoot ≤ one
        // inter-GC burst — spec §2/§6). An unmeasured tenant reads {0,0} ⇒ passes
        // (fails OPEN until the first GC measurement, R Q9). Placed BEFORE
        // commit_atomic: a rejected commit mutates NO durable ref (atomicity intact).
        if self.quotas.enforced_for(tenant) {
            let snapshot = self.usage.load();
            let cur = snapshot.get(tenant).copied().unwrap_or_default();
            if let Some(max) = self.quotas.max_durable_bytes {
                if cur.bytes >= max {
                    return Err(LedgeError::QuotaExceeded(format!(
                        "durable_bytes: {max} limit reached"
                    )));
                }
            }
            if let Some(max) = self.quotas.max_object_count {
                if cur.objects >= max {
                    return Err(LedgeError::QuotaExceeded(format!(
                        "object_count: {max} limit reached"
                    )));
                }
            }
        }

        // Authorization (unchanged): every source ref MUST live under THIS
        // workspace's namespace. Promoting a ref from a *different* workspace
        // would let a caller leak another workspace's work into a durable ref.
        // Reject the whole batch (no partial promotion) before any write.
        let ws_prefix = Self::ws_prefix(&id);
        for mapping in mappings {
            if !mapping.0.as_str().starts_with(&ws_prefix) {
                return Err(LedgeError::Corruption(format!(
                    "commit: ref {} does not belong to workspace {}",
                    mapping.0.as_str(),
                    id.to_hex()
                )));
            }
        }

        // Build the (durable, target, expected) set. The durable TARGET is
        // rewritten into the tenant's physical namespace; we keep the CLIENT name
        // to report back so the API contract (refs/heads/feature) is stable. A
        // workspace ref that is absent is silently skipped (mirrors git's no-op).
        let mut promotions: Vec<(RefName, ObjectId, Option<ObjectId>)> =
            Vec::with_capacity(mappings.len());
        let mut client_names: std::collections::HashMap<String, String> =
            std::collections::HashMap::with_capacity(mappings.len());
        for (ws_ref, client_durable) in mappings {
            let ws_entry = match self.refs.get(ws_ref).await? {
                Some(e) => e,
                None => continue, // nothing to promote from this workspace ref
            };
            let phys_durable = durable_ref(tenant, client_durable)?;
            client_names.insert(
                phys_durable.as_str().to_string(),
                client_durable.as_str().to_string(),
            );
            let current = self.refs.get(&phys_durable).await?;
            let expected = current.as_ref().map(|c| c.target);
            promotions.push((phys_durable, ws_entry.target, expected));
        }

        if promotions.is_empty() {
            return Ok(Vec::new());
        }

        // Map a physical durable name back to the client-facing name for output.
        let to_client = |phys: &str| -> String {
            client_names
                .get(phys)
                .cloned()
                .unwrap_or_else(|| phys.to_string())
        };

        // ATOMIC all-or-nothing promotion through the injected seam.
        let result = self.coordinator.commit_atomic(promotions).await?;
        let outcomes = match result {
            // Every durable ref advanced together.
            AtomicCommitResult::Committed(committed) => committed
                .into_iter()
                .map(|(name, entry)| CommitOutcome::Ok {
                    target: to_client(name.as_str()),
                    entry,
                })
                .collect(),
            // No durable ref advanced. Surface each conflicting ref carrying its
            // LIVE durable entry (re-read for the state the caller must reconcile
            // against). A create-only conflict means the durable ref now EXISTS
            // (that is why the create lost), so the re-read returns `Some`; the
            // absent fallback is defensive only.
            AtomicCommitResult::Aborted { conflicts, .. } => {
                let mut out = Vec::with_capacity(conflicts.len());
                for name in conflicts {
                    let current = self.refs.get(&name).await?.unwrap_or(RefEntry {
                        target: ObjectId::from_bytes([0; 32]),
                        version: 0,
                        hlc: 0,
                    });
                    out.push(CommitOutcome::Conflict {
                        target: to_client(name.as_str()),
                        current,
                    });
                }
                out
            }
        };
        Ok(outcomes)
    }

    /// Release a workspace: delete every `refs/workspaces/<id>/*` ref, then
    /// tombstone the lease. Idempotent — deleting an already-gone ref or
    /// tombstoning an already-tombstoned lease is `Ok`. A `NotFound` from delete
    /// (ref vanished between list and delete) or a `Conflict` (the target moved
    /// under us) is swallowed: the workspace ref is gone either way.
    ///
    /// Complexity: O(k) deletes for k workspace refs + one lease tombstone.
    #[tracing::instrument(skip(self), fields(id = %id))]
    pub async fn release(&self, id: WorkspaceId, tenant_id: &str) -> Result<()> {
        // Ownership: releasing a FOREIGN live workspace is a 404 (owned_lease
        // returns Err(NotFound) → propagated, no existence leak). An ABSENT lease
        // is a no-op success (owned_lease returns Ok(None) ⇒ idempotent release).
        if self.owned_lease(id, tenant_id).await?.is_none() {
            // Reached here with Ok(None) ⇒ the lease is absent/tombstoned ⇒
            // idempotent success. Still tombstone defensively (no-op if gone).
            self.leases.tombstone(id).await?;
            return Ok(());
        }
        let prefix = Self::ws_prefix(&id);
        for (name, entry) in self.refs.list(&prefix).await? {
            match self.refs.delete(&name, entry.target).await {
                Ok(()) => {}
                // Idempotency: the ref already vanished (concurrent release / GC).
                Err(LedgeError::NotFound(_)) => {}
                // A stale CAS on delete means someone moved it; treat as gone.
                Err(LedgeError::Conflict { .. }) => {}
                Err(other) => return Err(other),
            }
        }
        self.leases.tombstone(id).await?;
        Ok(())
    }

    /// Resolve a workspace to a view, or `None` if its lease is gone/tombstoned.
    /// Maps stored workspace ref names back to client-facing names (§3.2 inverse).
    ///
    /// Complexity: one lease get + O(k) for k workspace refs (list + map).
    pub async fn get(&self, id: WorkspaceId, tenant_id: &str) -> Result<Option<WorkspaceView>> {
        // A foreign workspace is indistinguishable from a missing one (Ok(None)):
        // existence is never revealed cross-tenant (spec §5). owned_lease returns
        // Err(NotFound) for foreign, which we MAP to Ok(None) here so the read
        // path never 500s and the handler 404s uniformly.
        let lease = match self.owned_lease(id, tenant_id).await {
            Ok(Some(l)) => l,
            Ok(None) => return Ok(None),
            Err(LedgeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let prefix = Self::ws_prefix(&id);
        let refs = self
            .refs
            .list(&prefix)
            .await?
            .into_iter()
            .map(|(name, entry)| (client_ref(&id, name.as_str()), entry))
            .collect();
        Ok(Some(WorkspaceView { id, lease, refs }))
    }

    /// List all live workspaces. Drives off the lease store's `live(now_ms)`
    /// partition (unexpired, non-tombstoned), then resolves each to a view.
    /// A lease that vanished between `live` and `get` is skipped (race-safe).
    ///
    /// Complexity: one `live` scan + O(w) `get`s for w live workspaces.
    pub async fn list(&self, tenant_id: &str) -> Result<Vec<WorkspaceView>> {
        let live = self.leases.live_for_tenant(now_ms(), tenant_id).await?;
        let mut views = Vec::with_capacity(live.len());
        for lease in live {
            if let Some(view) = self.get(lease.id, tenant_id).await? {
                views.push(view);
            }
        }
        Ok(views)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::ObjectId;
    use ledge_ref_store::RefStoreImpl;
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
        let leases = Arc::new(
            LeaseStore::open(dir.path().join("leases"), hlc.clone()).expect("lease store"),
        );
        // Single-node atomic commit over the same concrete ref store: behavior is
        // byte-identical to the old sequential loop for the happy path, but now
        // genuinely all-or-nothing across every ref in one batch.
        let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
            Arc::new(ledge_ref_store::LocalAtomicCommit::new(refs.clone()));
        let mgr = WorkspaceManager::new(
            refs,
            leases,
            hlc,
            coordinator,
            crate::quota::QuotaLimits::default(),
            Arc::new(crate::quota::UsageMap::default()),
        );
        (mgr, dir)
    }

    /// Build a manager with explicit quota limits (and a fresh empty usage map),
    /// otherwise identical to `setup()`. Returns the manager + the TempDir guard.
    fn setup_quota(limits: crate::quota::QuotaLimits) -> (WorkspaceManager, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let hlc = Arc::new(HLC::new());
        let refs =
            Arc::new(RefStoreImpl::open(dir.path().join("refs"), hlc.clone()).expect("ref store"));
        let leases = Arc::new(
            LeaseStore::open(dir.path().join("leases"), hlc.clone()).expect("lease store"),
        );
        let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
            Arc::new(ledge_ref_store::LocalAtomicCommit::new(refs.clone()));
        let usage = Arc::new(crate::quota::UsageMap::default());
        let mgr = WorkspaceManager::new(refs, leases, hlc, coordinator, limits, usage);
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
            .fork(std::slice::from_ref(&main), Duration::from_secs(60), "root")
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
        let lease = mgr
            .leases
            .get(view.id)
            .await
            .unwrap()
            .expect("lease present");
        assert_eq!(lease.generation, 1);
        assert_eq!(lease.source_refs, vec!["refs/heads/main".to_string()]);
        assert!(lease.expires_at_ms > lease.created_at_ms);
    }

    #[tokio::test]
    async fn fork_missing_source_ref_errors() {
        let (mgr, _dir) = setup();
        let absent = r("refs/heads/nope");
        let err = mgr
            .fork(&[absent], Duration::from_secs(60), "root")
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
                "root",
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
        let view = mgr.fork(&[main], Duration::from_secs(1), "root").await.unwrap();
        let before = view.lease.clone();

        let renewed = mgr.renew(view.id, Duration::from_secs(3600), "root").await.unwrap();

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

    #[tokio::test]
    async fn commit_promotes_to_new_durable_ref() {
        let (mgr, _dir) = setup();
        // Seed a source; fork; the workspace ref target == source target.
        let src = r("refs/heads/main");
        let src_target = oid(1);
        mgr.refs.update(&src, src_target, None).await.unwrap();
        let view = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(60), "root")
            .await
            .unwrap();

        // Durable target ref does NOT exist yet.
        let durable = r("refs/heads/feature");
        assert!(mgr.refs.get(&durable).await.unwrap().is_none());

        let ws = workspace_ref(&view.id, &src).unwrap();
        let outcomes = mgr
            .commit(view.id, &[(ws.clone(), durable.clone())], "root")
            .await
            .unwrap();

        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            CommitOutcome::Ok { target, entry } => {
                assert_eq!(target, "refs/heads/feature");
                assert_eq!(entry.target, src_target);
            }
            other => panic!("expected Ok outcome, got {other:?}"),
        }

        // Durable ref now resolves to the promoted target.
        let now = mgr
            .refs
            .get(&durable)
            .await
            .unwrap()
            .expect("durable now present");
        assert_eq!(now.target, src_target);

        // Commit does NOT release the workspace: its refs and lease persist.
        assert!(mgr.refs.get(&ws).await.unwrap().is_some());
        assert!(mgr.leases.get(view.id).await.unwrap().is_some());
    }

    /// A test-only [`AtomicCommit`] decorator that races a concurrent durable
    /// mutation in BEFORE delegating, so exactly one mapping's CAS precondition
    /// (the one `commit` read a moment earlier) is now stale at apply time. This
    /// deterministically reproduces the "durable moved under us between read and
    /// apply" window WITHOUT a flaky timing race, and proves the batch is
    /// all-or-nothing: the stale ref aborts the WHOLE batch, so the other ref
    /// (which would otherwise have committed under the old sequential loop) does
    /// NOT advance.
    struct RacingCommit {
        inner: Arc<dyn AtomicCommit>,
        /// The store the race mutates (the same one `commit` reads + the seam
        /// writes), so the mutation is observed by the inner commit's preconditions.
        store: Arc<RefStoreImpl>,
        /// The durable ref to bump (to a value that invalidates `commit`'s snapshot).
        victim: RefName,
        /// What the victim currently is (the value `commit` will read), so we can
        /// CAS it forward to a different value that makes the snapshot stale.
        from: ObjectId,
        to: ObjectId,
    }

    #[async_trait::async_trait]
    impl AtomicCommit for RacingCommit {
        async fn commit_atomic(
            &self,
            mappings: Vec<(RefName, ObjectId, Option<ObjectId>)>,
        ) -> Result<AtomicCommitResult> {
            // Race: move the victim durable forward so the `expected` snapshot the
            // manager just read for it is now stale. The inner atomic commit will
            // re-evaluate ALL preconditions against THIS post-race root.
            self.store
                .update(&self.victim, self.to, Some(self.from))
                .await
                .unwrap();
            self.inner.commit_atomic(mappings).await
        }
    }

    #[tokio::test]
    async fn commit_two_durable_refs_is_atomic_on_conflict() {
        // Build a manager whose coordinator races a durable mutation in before the
        // atomic apply, so one mapping's precondition is stale at apply time.
        let dir = TempDir::new().expect("tempdir");
        let hlc = Arc::new(HLC::new());
        let refs =
            Arc::new(RefStoreImpl::open(dir.path().join("refs"), hlc.clone()).expect("ref store"));
        let leases =
            Arc::new(LeaseStore::open(dir.path().join("leases"), hlc.clone()).expect("leases"));

        // Sources + their forked workspace refs.
        let s1 = r("refs/heads/one");
        let s2 = r("refs/heads/two");
        refs.update(&s1, oid(1), None).await.unwrap();
        refs.update(&s2, oid(2), None).await.unwrap();

        // Durable d1 pre-exists at oid(5); d2 is absent. The race will move d1 to
        // oid(9) AFTER `commit` reads its expected (oid(5)), so d1's CAS aborts.
        let d1 = r("refs/heads/dst1");
        let d2 = r("refs/heads/dst2");
        refs.update(&d1, oid(5), None).await.unwrap();

        let inner: Arc<dyn AtomicCommit> = Arc::new(ledge_ref_store::LocalAtomicCommit::new(
            refs.clone(),
        ));
        let coordinator: Arc<dyn AtomicCommit> = Arc::new(RacingCommit {
            inner,
            store: refs.clone(),
            victim: d1.clone(),
            from: oid(5),
            to: oid(9),
        });
        let mgr = WorkspaceManager::new(
            refs.clone(),
            leases,
            hlc,
            coordinator,
            crate::quota::QuotaLimits::default(),
            Arc::new(crate::quota::UsageMap::default()),
        );

        let view = mgr
            .fork(&[s1.clone(), s2.clone()], Duration::from_secs(60), "root")
            .await
            .unwrap();
        let ws1 = workspace_ref(&view.id, &s1).unwrap();
        let ws2 = workspace_ref(&view.id, &s2).unwrap();

        // commit reads d1=oid(5) (expected Some(5)) and d2 absent (expected None).
        // The racing coordinator then bumps d1→oid(9), so d1's CAS is stale ⇒ the
        // WHOLE batch aborts. Under the OLD sequential loop ws2→d2 would have
        // committed first; the atomic seam guarantees it does NOT.
        let outcomes = mgr
            .commit(view.id, &[(ws1, d1.clone()), (ws2, d2.clone())], "root")
            .await
            .unwrap();
        assert!(
            outcomes
                .iter()
                .any(|o| matches!(o, CommitOutcome::Conflict { target, .. } if target == d1.as_str())),
            "d1 must be reported as the conflict, got {outcomes:?}"
        );
        // Atomicity: d1 stays at the raced value oid(9) (never advanced to oid(1));
        // d2 was NEVER created (the abort rolled back the whole batch).
        assert_eq!(mgr.refs.get(&d1).await.unwrap().unwrap().target, oid(9));
        assert!(
            mgr.refs.get(&d2).await.unwrap().is_none(),
            "d2 must NOT be created (atomic abort)"
        );
    }

    #[tokio::test]
    async fn commit_stale_durable_yields_conflict() {
        let (mgr, _dir) = setup();

        // Fork a workspace whose ref carries pushed work.
        let src = r("refs/heads/main");
        mgr.refs.update(&src, oid(1), None).await.unwrap();
        let view = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(60), "root")
            .await
            .unwrap();
        let ws = workspace_ref(&view.id, &src).unwrap();
        mgr.refs.update(&ws, oid(2), Some(oid(1))).await.unwrap();

        // A SECOND workspace forked from the same source, with DIFFERENT work.
        let view2 = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(60), "root")
            .await
            .unwrap();
        let ws2 = workspace_ref(&view2.id, &src).unwrap();
        mgr.refs.update(&ws2, oid(3), Some(oid(1))).await.unwrap();

        let durable = r("refs/heads/main"); // currently oid(1)

        // First workspace commits: reads durable oid(1), CAS oid(1)->oid(2). Ok.
        let first = mgr
            .commit(view.id, &[(ws.clone(), durable.clone())], "root")
            .await
            .unwrap();
        assert!(matches!(first[0], CommitOutcome::Ok { .. }));
        assert_eq!(
            mgr.refs.get(&durable).await.unwrap().unwrap().target,
            oid(2)
        );

        // The second workspace was created when durable was oid(1). To exercise
        // the Conflict arm deterministically we present a STALE expected (oid(1))
        // through the ref store — exactly what a client holding a pre-move read
        // does. The CAS must be rejected with the live durable entry (oid(2)).
        let stale_expected = oid(1);
        let direct = mgr
            .refs
            .update(&durable, oid(3), Some(stale_expected))
            .await;
        match direct {
            Err(LedgeError::Conflict { current }) => {
                // The ref store reports the live durable (oid(2)); this is the
                // exact shape commit maps into CommitOutcome::Conflict.
                assert_eq!(current.target, oid(2));
            }
            other => panic!("expected ref-store Conflict, got {other:?}"),
        }

        // No-clobber: the stale write never moved durable off oid(2).
        assert_eq!(
            mgr.refs.get(&durable).await.unwrap().unwrap().target,
            oid(2)
        );
        let _ = (ws2, view2); // second workspace intentionally left for release tests
    }

    #[tokio::test]
    async fn commit_rejects_foreign_workspace_ref() {
        let (mgr, _dir) = setup();
        let src = r("refs/heads/main");
        mgr.refs.update(&src, oid(1), None).await.unwrap();

        // Workspace A: the target of the commit call.
        let view_a = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(60), "root")
            .await
            .unwrap();
        // Workspace B: a DIFFERENT workspace whose ref we maliciously pass to A.
        let view_b = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(60), "root")
            .await
            .unwrap();

        // A ref that belongs to workspace B, not A.
        let b_ws_ref = r(&format!(
            "refs/workspaces/{}/heads/main",
            view_b.id.to_hex()
        ));
        let durable = r("refs/heads/feature");

        // Promoting B's ref through A's commit must be rejected.
        let err = mgr
            .commit(view_a.id, &[(b_ws_ref.clone(), durable.clone())], "root")
            .await
            .unwrap_err();
        match err {
            LedgeError::Corruption(msg) => {
                assert!(msg.contains(b_ws_ref.as_str()), "msg names the foreign ref: {msg}");
                assert!(msg.contains(&view_a.id.to_hex()), "msg names the target ws: {msg}");
            }
            other => panic!("expected Corruption rejecting the foreign ref, got {other:?}"),
        }

        // No clobber: the durable target was never created.
        assert!(mgr.refs.get(&durable).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn release_removes_refs_and_tombstones_lease() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        let tag = r("refs/tags/v1");
        mgr.refs.update(&main, oid(1), None).await.unwrap();
        mgr.refs.update(&tag, oid(2), None).await.unwrap();
        let view = mgr
            .fork(&[main.clone(), tag.clone()], Duration::from_secs(60), "root")
            .await
            .unwrap();

        // Pre-condition: workspace refs exist.
        let prefix = format!("refs/workspaces/{}/", view.id.to_hex());
        assert_eq!(mgr.refs.list(&prefix).await.unwrap().len(), 2);

        mgr.release(view.id, "root").await.unwrap();

        // Workspace refs gone.
        assert!(mgr.refs.list(&prefix).await.unwrap().is_empty());
        // get() returns None after release.
        assert!(mgr.get(view.id, "root").await.unwrap().is_none());
        // Durable source refs are UNTOUCHED.
        assert_eq!(mgr.refs.get(&main).await.unwrap().unwrap().target, oid(1));
        assert_eq!(mgr.refs.get(&tag).await.unwrap().unwrap().target, oid(2));
    }

    #[tokio::test]
    async fn double_release_is_idempotent() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        mgr.refs.update(&main, oid(1), None).await.unwrap();
        let view = mgr.fork(&[main], Duration::from_secs(60), "root").await.unwrap();

        mgr.release(view.id, "root").await.unwrap();
        // Second release on an already-released workspace must still be Ok.
        mgr.release(view.id, "root").await.unwrap();
        // Release on a never-existed workspace id is also Ok.
        let phantom = WorkspaceId::generate(&mgr.hlc);
        mgr.release(phantom, "root").await.unwrap();

        assert!(mgr.get(view.id, "root").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_returns_client_facing_ref_names() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        let tag = r("refs/tags/v1");
        mgr.refs.update(&main, oid(1), None).await.unwrap();
        mgr.refs.update(&tag, oid(2), None).await.unwrap();
        let view = mgr
            .fork(&[main, tag], Duration::from_secs(60), "root")
            .await
            .unwrap();

        let got = mgr.get(view.id, "root").await.unwrap().expect("present");
        let mut names: Vec<&str> = got.refs.iter().map(|(n, _)| n.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["refs/heads/main", "refs/tags/v1"]);

        // No stored/workspace-prefixed form leaks into the view.
        for (n, _) in &got.refs {
            assert!(!n.contains("workspaces"), "leaked storage name: {n}");
        }
    }

    #[tokio::test]
    async fn list_returns_only_live_workspaces() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        mgr.refs.update(&main, oid(1), None).await.unwrap();

        // Live workspace (long TTL).
        let live = mgr
            .fork(std::slice::from_ref(&main), Duration::from_secs(3600), "root")
            .await
            .unwrap();
        // Released workspace (tombstoned -> not live).
        let released = mgr
            .fork(std::slice::from_ref(&main), Duration::from_secs(3600), "root")
            .await
            .unwrap();
        mgr.release(released.id, "root").await.unwrap();
        // Expired workspace (TTL already elapsed). expires_at_ms == created_at_ms;
        // `live` uses `expires_at_ms > now_ms`, so a 0ms TTL is not live.
        let expired = mgr
            .fork(std::slice::from_ref(&main), Duration::from_millis(0), "root")
            .await
            .unwrap();

        let listed = mgr.list("root").await.unwrap();
        let ids: Vec<WorkspaceId> = listed.iter().map(|v| v.id).collect();

        assert!(ids.contains(&live.id), "live workspace must be listed");
        assert!(
            !ids.contains(&released.id),
            "released workspace must not be listed"
        );
        assert!(
            !ids.contains(&expired.id),
            "expired workspace must not be listed"
        );
    }

    #[tokio::test]
    async fn cross_tenant_get_is_none_renew_release_are_404() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        // acme forks from its OWN durable namespace (refs/tenants/acme/...).
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        // acme forks a workspace.
        let view = mgr
            .fork(std::slice::from_ref(&main), Duration::from_secs(60), "acme")
            .await
            .unwrap();

        // globex cannot SEE it (Ok(None), not an error, not the view).
        assert!(mgr.get(view.id, "globex").await.unwrap().is_none());

        // globex renew/release on it ⇒ NotFound (→ 404 at the HTTP layer).
        let renew_err = mgr
            .renew(view.id, Duration::from_secs(60), "globex")
            .await
            .unwrap_err();
        assert!(matches!(renew_err, LedgeError::NotFound(_)));

        let rel_err = mgr.release(view.id, "globex").await.unwrap_err();
        assert!(matches!(rel_err, LedgeError::NotFound(_)));

        // acme still owns it.
        assert!(mgr.get(view.id, "acme").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn list_is_tenant_scoped() {
        let (mgr, _dir) = setup();
        let main = r("refs/heads/main");
        // Each tenant forks from its OWN durable namespace.
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        mgr.refs
            .update(&r("refs/tenants/globex/heads/main"), oid(1), None)
            .await
            .unwrap();
        let acme = mgr
            .fork(std::slice::from_ref(&main), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
        let globex = mgr
            .fork(
                std::slice::from_ref(&main),
                Duration::from_secs(3600),
                "globex",
            )
            .await
            .unwrap();

        let acme_ids: Vec<_> = mgr
            .list("acme")
            .await
            .unwrap()
            .iter()
            .map(|v| v.id)
            .collect();
        assert!(acme_ids.contains(&acme.id));
        assert!(
            !acme_ids.contains(&globex.id),
            "globex's ws must not appear in acme's list"
        );

        let globex_ids: Vec<_> = mgr
            .list("globex")
            .await
            .unwrap()
            .iter()
            .map(|v| v.id)
            .collect();
        assert!(globex_ids.contains(&globex.id));
        assert!(!globex_ids.contains(&acme.id));
    }

    #[tokio::test]
    async fn fork_and_commit_land_in_the_tenant_namespace() {
        let (mgr, _dir) = setup();
        // acme's durable ref is physically refs/tenants/acme/heads/main.
        let phys = r("refs/tenants/acme/heads/main");
        mgr.refs.update(&phys, oid(1), None).await.unwrap();

        // acme forks from the CLIENT name refs/heads/main (resolves to its phys ref).
        let client = r("refs/heads/main");
        let view = mgr
            .fork(std::slice::from_ref(&client), Duration::from_secs(60), "acme")
            .await
            .unwrap();
        assert_eq!(view.refs[0].0, "refs/heads/main", "view shows client-facing name");
        assert_eq!(view.refs[0].1.target, oid(1));

        // Advance the workspace ref, then commit back to the CLIENT durable name.
        let ws = workspace_ref(&view.id, &client).unwrap();
        mgr.refs.update(&ws, oid(2), Some(oid(1))).await.unwrap();
        let outcomes = mgr
            .commit(view.id, &[(ws, client.clone())], "acme")
            .await
            .unwrap();
        match &outcomes[0] {
            CommitOutcome::Ok { target, entry } => {
                assert_eq!(target, "refs/heads/main", "reported name is client-facing");
                assert_eq!(entry.target, oid(2));
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        // It landed PHYSICALLY under the tenant namespace, not the global one.
        assert_eq!(mgr.refs.get(&phys).await.unwrap().unwrap().target, oid(2));
        assert!(
            mgr.refs.get(&r("refs/heads/main")).await.unwrap().is_none(),
            "global refs/heads/main must be untouched (it was never acme's)"
        );
    }

    #[tokio::test]
    async fn fork_workspace_count_quota_rejects_over_limit() {
        let limits = crate::quota::QuotaLimits {
            enabled: true,
            max_workspaces: Some(2),
            ..Default::default()
        };
        let (mgr, _dir) = setup_quota(limits);
        // acme forks from its OWN durable namespace (refs/tenants/acme/...).
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        let src = r("refs/heads/main");

        // First two forks succeed (live count 0, then 1 — both < 2).
        let w1 = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
        let _w2 = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(3600), "acme")
            .await
            .unwrap();

        // Third fork: live count is 2 ⇒ 2 >= 2 ⇒ QuotaExceeded (→507).
        let err = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(3600), "acme")
            .await
            .unwrap_err();
        match err {
            LedgeError::QuotaExceeded(m) => {
                assert!(m.starts_with("workspaces:"), "msg names the resource: {m}");
                assert!(m.contains("2 limit reached"), "msg names the limit: {m}");
            }
            other => panic!("expected QuotaExceeded, got {other:?}"),
        }

        // Release one ⇒ a slot frees ⇒ the next fork succeeds (count back to 1).
        mgr.release(w1.id, "acme").await.unwrap();
        let _w4 = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fork_workspace_count_quota_root_exempt_and_other_tenant_independent() {
        let limits = crate::quota::QuotaLimits {
            enabled: true,
            max_workspaces: Some(1),
            ..Default::default()
        };
        let (mgr, _dir) = setup_quota(limits);
        // Seed root + two tenants' durable refs.
        mgr.refs.update(&r("refs/heads/main"), oid(1), None).await.unwrap();
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        mgr.refs
            .update(&r("refs/tenants/globex/heads/main"), oid(1), None)
            .await
            .unwrap();
        let src = r("refs/heads/main");

        // root is EXEMPT: many forks despite max_workspaces=1.
        for _ in 0..3 {
            mgr.fork(std::slice::from_ref(&src), Duration::from_secs(3600), "root")
                .await
                .unwrap();
        }

        // acme: first ok, second rejected (limit 1).
        mgr.fork(std::slice::from_ref(&src), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
        let acme_err = mgr
            .fork(std::slice::from_ref(&src), Duration::from_secs(3600), "acme")
            .await
            .unwrap_err();
        assert!(matches!(acme_err, LedgeError::QuotaExceeded(_)));

        // globex is INDEPENDENT: its first fork still succeeds (acme's count does
        // not count against globex — per-tenant limits).
        mgr.fork(std::slice::from_ref(&src), Duration::from_secs(3600), "globex")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fork_count_quota_disabled_never_rejects() {
        // enabled=false (default) ⇒ the gate is a no-op even with a tiny limit set.
        let limits = crate::quota::QuotaLimits {
            enabled: false,
            max_workspaces: Some(1),
            ..Default::default()
        };
        let (mgr, _dir) = setup_quota(limits);
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        let src = r("refs/heads/main");
        for _ in 0..5 {
            mgr.fork(std::slice::from_ref(&src), Duration::from_secs(3600), "acme")
                .await
                .unwrap();
        }
    }

    // ── Task 6: SOFT durable-storage / object-count commit gate ────────────

    /// Like `setup_quota` but also returns the shared usage map so a test can
    /// simulate a GC measurement (store a `TenantUsage`) and then exercise the
    /// commit soft-gate against it.
    fn setup_quota_usage(
        limits: crate::quota::QuotaLimits,
    ) -> (WorkspaceManager, Arc<crate::quota::UsageMap>, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let hlc = Arc::new(HLC::new());
        let refs =
            Arc::new(RefStoreImpl::open(dir.path().join("refs"), hlc.clone()).expect("ref store"));
        let leases = Arc::new(
            LeaseStore::open(dir.path().join("leases"), hlc.clone()).expect("lease store"),
        );
        let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
            Arc::new(ledge_ref_store::LocalAtomicCommit::new(refs.clone()));
        let usage = Arc::new(crate::quota::UsageMap::default());
        let mgr = WorkspaceManager::new(refs, leases, hlc, coordinator, limits, usage.clone());
        (mgr, usage, dir)
    }

    /// Set the last-measured usage for a tenant (simulates a GC pass).
    fn set_usage(map: &crate::quota::UsageMap, tenant: &str, u: crate::quota::TenantUsage) {
        let mut m = std::collections::HashMap::new();
        m.insert(tenant.to_string(), u);
        map.store(Arc::new(m));
    }

    #[tokio::test]
    async fn commit_durable_bytes_quota_soft_overshoot_then_reject() {
        use crate::quota::TenantUsage;
        let limits = crate::quota::QuotaLimits {
            enabled: true,
            max_durable_bytes: Some(1000),
            ..Default::default()
        };
        let (mgr, usage, _dir) = setup_quota_usage(limits);
        // acme forks + advances a workspace ref so it has work to commit.
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        let client = r("refs/heads/main");
        let view = mgr
            .fork(std::slice::from_ref(&client), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
        let ws = workspace_ref(&view.id, &client).unwrap();
        mgr.refs.update(&ws, oid(2), Some(oid(1))).await.unwrap();

        // Last-measured usage is UNDER the limit (999 < 1000) ⇒ this commit (which
        // pushes over) SUCCEEDS — the pre-gate saw under (soft semantics, §2/§6).
        set_usage(&usage, "acme", TenantUsage { bytes: 999, objects: 1 });
        let out = mgr
            .commit(view.id, &[(ws.clone(), client.clone())], "acme")
            .await
            .unwrap();
        assert!(
            matches!(out[0], CommitOutcome::Ok { .. }),
            "crossing commit succeeds (soft)"
        );

        // Simulate the NEXT GC re-measuring acme AT/OVER the limit. The next commit
        // is now REJECTED (507) — the documented one-burst overshoot bound.
        set_usage(&usage, "acme", TenantUsage { bytes: 1000, objects: 2 });
        // Advance the ws ref again so there is something to promote.
        mgr.refs.update(&ws, oid(3), Some(oid(2))).await.unwrap();
        let err = mgr
            .commit(view.id, &[(ws.clone(), client.clone())], "acme")
            .await
            .unwrap_err();
        match err {
            LedgeError::QuotaExceeded(m) => {
                assert!(m.starts_with("durable_bytes:"), "msg: {m}")
            }
            other => panic!("expected QuotaExceeded(durable_bytes), got {other:?}"),
        }
        // No-clobber: the rejected commit never moved the durable ref off oid(2).
        assert_eq!(
            mgr.refs
                .get(&r("refs/tenants/acme/heads/main"))
                .await
                .unwrap()
                .unwrap()
                .target,
            oid(2),
        );
    }

    #[tokio::test]
    async fn commit_object_count_quota_rejects_at_limit() {
        use crate::quota::TenantUsage;
        let limits = crate::quota::QuotaLimits {
            enabled: true,
            max_object_count: Some(5),
            ..Default::default()
        };
        let (mgr, usage, _dir) = setup_quota_usage(limits);
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        let client = r("refs/heads/main");
        let view = mgr
            .fork(std::slice::from_ref(&client), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
        let ws = workspace_ref(&view.id, &client).unwrap();
        mgr.refs.update(&ws, oid(2), Some(oid(1))).await.unwrap();

        // acme measured AT the object-count limit (5 >= 5) ⇒ reject.
        set_usage(&usage, "acme", TenantUsage { bytes: 0, objects: 5 });
        let err = mgr.commit(view.id, &[(ws, client)], "acme").await.unwrap_err();
        match err {
            LedgeError::QuotaExceeded(m) => assert!(m.starts_with("object_count:"), "msg: {m}"),
            other => panic!("expected QuotaExceeded(object_count), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn commit_storage_quota_under_limit_succeeds() {
        use crate::quota::TenantUsage;
        let limits = crate::quota::QuotaLimits {
            enabled: true,
            max_durable_bytes: Some(1000),
            max_object_count: Some(10),
            ..Default::default()
        };
        let (mgr, usage, _dir) = setup_quota_usage(limits);
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        let client = r("refs/heads/main");
        let view = mgr
            .fork(std::slice::from_ref(&client), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
        let ws = workspace_ref(&view.id, &client).unwrap();
        mgr.refs.update(&ws, oid(2), Some(oid(1))).await.unwrap();

        // Strictly under both limits ⇒ commit succeeds.
        set_usage(&usage, "acme", TenantUsage { bytes: 500, objects: 3 });
        let out = mgr.commit(view.id, &[(ws, client)], "acme").await.unwrap();
        assert!(matches!(out[0], CommitOutcome::Ok { .. }), "under limit succeeds");
    }

    #[tokio::test]
    async fn commit_storage_quota_root_exempt_and_disabled_passes() {
        use crate::quota::TenantUsage;
        // root is exempt even with usage far over a tiny limit.
        let limits = crate::quota::QuotaLimits {
            enabled: true,
            max_durable_bytes: Some(1),
            max_object_count: Some(1),
            ..Default::default()
        };
        let (mgr, usage, _dir) = setup_quota_usage(limits);
        mgr.refs.update(&r("refs/heads/main"), oid(1), None).await.unwrap();
        let client = r("refs/heads/main");
        let view = mgr
            .fork(std::slice::from_ref(&client), Duration::from_secs(3600), "root")
            .await
            .unwrap();
        let ws = workspace_ref(&view.id, &client).unwrap();
        mgr.refs.update(&ws, oid(2), Some(oid(1))).await.unwrap();
        set_usage(&usage, "root", TenantUsage { bytes: 10_000, objects: 10_000 });
        // root is exempt ⇒ commit succeeds despite being far over.
        let out = mgr.commit(view.id, &[(ws, client)], "root").await.unwrap();
        assert!(matches!(out[0], CommitOutcome::Ok { .. }), "root exempt");
    }

    #[tokio::test]
    async fn commit_storage_quota_disabled_passes_far_over() {
        use crate::quota::TenantUsage;
        // enabled=false (default) ⇒ no gate even for a real tenant far over.
        let (mgr, usage, _dir) = setup_quota_usage(crate::quota::QuotaLimits::default());
        mgr.refs
            .update(&r("refs/tenants/acme/heads/main"), oid(1), None)
            .await
            .unwrap();
        let client = r("refs/heads/main");
        let view = mgr
            .fork(std::slice::from_ref(&client), Duration::from_secs(3600), "acme")
            .await
            .unwrap();
        let ws = workspace_ref(&view.id, &client).unwrap();
        mgr.refs.update(&ws, oid(2), Some(oid(1))).await.unwrap();
        set_usage(&usage, "acme", TenantUsage { bytes: 10_000, objects: 10_000 });
        let out = mgr.commit(view.id, &[(ws, client)], "acme").await.unwrap();
        assert!(matches!(out[0], CommitOutcome::Ok { .. }), "disabled ⇒ no gate");
    }
}
