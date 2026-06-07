//! Tenant → physical ref-namespace mapping (Phase 4d-2 spec §3.1).
//!
//! [`tenant_prefix`] is the single, shared definition of how a tenant's DURABLE
//! refs are physically namespaced. It lives in `ledge-core` (the leaf crate) so
//! `ledge-workspace`, `ledge-rpc`, and `ledge-server` all use ONE copy without a
//! dependency cycle.
//!
//! The synthetic `root` tenant (auth-disabled / admin) keeps the LEGACY global
//! namespace (empty prefix) so existing data and every pre-4d-2 test are
//! byte-identical. The empty string is treated as `root` (a legacy lease decoded
//! without a `tenant_id` defaults to `""` — see `ledge_workspace::lease`).

/// The physical ref-namespace prefix for a tenant's DURABLE refs.
///
/// - `"root"` (and `""`, the legacy default) → `""` (the global namespace;
///   `refs/heads/main` stays `refs/heads/main`).
/// - any other `t` → `"tenants/<t>/"` (so `refs/heads/main` is physically
///   `refs/tenants/<t>/heads/main`).
///
/// Applied to durable ref names only (fork sources, commit targets, the
/// default-repo git segment). Workspace refs stay at `refs/workspaces/<wid>/…`
/// (spec §3.2). The returned string is a `segment` in the `ledge-git`
/// `present_ref`/`store_ref` sense: inserted immediately after `refs/`.
///
/// Pure, total, no allocation for the root path.
pub fn tenant_prefix(tenant_id: &str) -> String {
    if tenant_id.is_empty() || tenant_id == "root" {
        String::new()
    } else {
        format!("tenants/{tenant_id}/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_empty_prefix() {
        assert_eq!(tenant_prefix("root"), "");
    }

    #[test]
    fn empty_is_treated_as_root() {
        // A legacy lease decoded without tenant_id defaults to "" ⇒ root/global.
        assert_eq!(tenant_prefix(""), "");
    }

    #[test]
    fn named_tenant_is_prefixed() {
        assert_eq!(tenant_prefix("acme"), "tenants/acme/");
        assert_eq!(tenant_prefix("globex"), "tenants/globex/");
    }

    #[test]
    fn prefix_composes_as_a_git_segment() {
        // The segment is inserted after `refs/`: refs/<prefix>heads/main.
        let p = tenant_prefix("acme");
        assert_eq!(format!("refs/{p}heads/main"), "refs/tenants/acme/heads/main");
        // Root collapses to the legacy form.
        let r = tenant_prefix("root");
        assert_eq!(format!("refs/{r}heads/main"), "refs/heads/main");
    }
}
