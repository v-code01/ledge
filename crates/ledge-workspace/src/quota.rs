//! Per-tenant quota types shared by the manager (reader) and the GC (writer).
//!
//! Phase 4d-3. These live in `ledge-workspace` — NOT `ledge-server` — because the
//! manager (this crate) READS the last-measured usage on the `commit` soft-gate,
//! while the GC writers are `ledge_workspace::Gc` (this crate) AND
//! `ledge_cluster::gc::ClusterGc` (which already depends on this crate). Placing
//! `UsageMap` here lets every party name it without a crate cycle (spec §3.4, R Q4).
//!
//! - [`QuotaLimits`] — the Copy enforcement struct the manager holds as a field
//!   (`enabled` + the three durable limits; the RATE limit lives with the
//!   server's `TenantRateLimiter`, not here — R Q13).
//! - [`TenantUsage`] — one tenant's last GC-measured durable footprint.
//! - [`UsageMap`] — an `ArcSwap` of `tenant → TenantUsage`, atomically swapped by
//!   each GC pass and read lock-free on the commit gate.

use std::collections::HashMap;

use arc_swap::ArcSwap;

/// Per-tenant durable resource limits enforced by [`crate::WorkspaceManager`].
///
/// `Copy` (six small fields) so the manager holds it by value with no clone cost.
/// `None` ⇒ unlimited for that resource. `enabled=false` (the default) ⇒ NO quota
/// is enforced (byte-identical to Phase 4d-2). The RATE limit is intentionally
/// ABSENT — it is enforced in the auth middleware via the server's
/// `TenantRateLimiter`, never by the manager (R Q13).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QuotaLimits {
    /// Master switch. `false` (default) ⇒ every gate is a no-op.
    pub enabled: bool,
    /// Max LIVE workspaces per tenant (exact, gated at `fork`). `None` = unlimited.
    pub max_workspaces: Option<u64>,
    /// Max durable bytes per tenant (SOFT, gated at `commit` against the last GC
    /// measurement). `None` = unlimited.
    pub max_durable_bytes: Option<u64>,
    /// Max durable object count per tenant (SOFT, gated at `commit`). `None` = unlimited.
    pub max_object_count: Option<u64>,
}

impl QuotaLimits {
    /// True iff a quota should be enforced for `tenant`: enabled AND not root.
    /// `root` (and the legacy `""`, normalized here) is ALWAYS exempt regardless
    /// of `enabled` (spec §3.1, R Q7). The single rule every gate calls.
    pub fn enforced_for(&self, tenant: &str) -> bool {
        let norm = if tenant.is_empty() { "root" } else { tenant };
        self.enabled && norm != "root"
    }
}

/// One tenant's last GC-measured durable footprint (spec §3.4). `Copy` (two u64s)
/// so the commit gate reads it by value off the `ArcSwap` snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TenantUsage {
    /// Sum of on-disk object file sizes reachable from this tenant's durable refs.
    pub bytes: u64,
    /// Count of distinct objects reachable from this tenant's durable refs.
    pub objects: u64,
}

/// The shared, atomically-swappable per-tenant usage snapshot.
///
/// Written by each GC pass (`ArcSwap::store` a freshly-measured map); read
/// lock-free by the manager's `commit` gate (`ArcSwap::load`). One `Arc<UsageMap>`
/// is created in the server and injected into the manager, the single-node GC, the
/// cluster GC, and `QuotaCtx` (the usage gauges) — one store, many parties, no
/// cycle (R Q4). `Default` is an empty map ⇒ every tenant reads `TenantUsage`
/// default `{0,0}` until the first measurement (fails OPEN — spec §3.4, R Q9).
pub type UsageMap = ArcSwap<HashMap<String, TenantUsage>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_limits_enforce_nothing() {
        let q = QuotaLimits::default();
        assert!(!q.enabled);
        assert!(!q.enforced_for("acme"), "disabled ⇒ no enforcement");
        assert!(!q.enforced_for("root"));
    }

    #[test]
    fn root_is_always_exempt_even_enabled() {
        let q = QuotaLimits { enabled: true, max_workspaces: Some(1), ..Default::default() };
        assert!(!q.enforced_for("root"), "root exempt");
        assert!(!q.enforced_for(""), "legacy empty ⇒ root ⇒ exempt");
        assert!(q.enforced_for("acme"), "a real tenant is enforced when enabled");
    }

    #[test]
    fn usage_map_default_is_empty_and_reads_zero() {
        let m = UsageMap::default();
        let snap = m.load();
        assert!(snap.is_empty());
        let cur = snap.get("acme").copied().unwrap_or_default();
        assert_eq!(cur, TenantUsage { bytes: 0, objects: 0 });
    }

    #[test]
    fn usage_map_store_then_load_roundtrips() {
        let m = UsageMap::default();
        let mut fresh = HashMap::new();
        fresh.insert("acme".to_string(), TenantUsage { bytes: 100, objects: 3 });
        m.store(std::sync::Arc::new(fresh));
        let snap = m.load();
        assert_eq!(snap.get("acme").copied().unwrap(), TenantUsage { bytes: 100, objects: 3 });
        assert_eq!(snap.get("globex").copied().unwrap_or_default(), TenantUsage::default());
    }
}
