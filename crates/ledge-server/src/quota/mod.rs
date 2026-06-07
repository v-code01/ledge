//! Server-side per-tenant quota context (Phase 4d-3).
//!
//! [`QuotaCtx`] bundles the three things the server needs to enforce quotas:
//! the [`QuotaLimits`](ledge_workspace::QuotaLimits) (SHARED by value with the
//! manager, which holds its own Copy), the shared [`UsageMap`] `Arc` (read by the
//! commit gate AND by the usage gauges), and the [`TenantRateLimiter`] (built from
//! the rate/burst config; enforced ONLY in the auth middleware). `QuotaCtx` is in
//! `AppState`; `QuotaCtx::disabled()` is the inert default wired into every test
//! `AppState` so the suite stays byte-identical with quotas off (R Q15).

pub mod rate;

use std::sync::Arc;

use ledge_workspace::{QuotaLimits, UsageMap};

use rate::TenantRateLimiter;

/// The server's quota enforcement context, carried in [`crate::AppState`].
#[derive(Clone)]
pub struct QuotaCtx {
    /// The manager-relevant durable limits (also held, by Copy, inside the
    /// `WorkspaceManager`). Carried here too so the middleware can call
    /// `limits.enforced_for(tenant)` (the root/disabled bypass — R Q7).
    pub limits: QuotaLimits,
    /// The shared per-tenant usage snapshot (the SAME `Arc` the manager + the GC
    /// hold). Read by the `ledge_quota_usage_*` gauges (Task 7).
    pub usage: Arc<UsageMap>,
    /// Per-tenant request-rate token bucket. Enforced in the auth middleware
    /// (Task 4) on CLIENT requests, post-principal. An inert limiter (no limit)
    /// never denies.
    pub rate: Arc<TenantRateLimiter>,
}

impl QuotaCtx {
    /// The inert context: quotas disabled, an empty usage map, a no-limit rate
    /// limiter. Used by every test `AppState` and by single-node dev when
    /// `[quotas] enabled=false` (the default). Byte-identical to Phase 4d-2.
    pub fn disabled() -> Self {
        Self {
            limits: QuotaLimits::default(),
            usage: Arc::new(UsageMap::default()),
            rate: Arc::new(TenantRateLimiter::unlimited()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_ctx_is_inert() {
        let ctx = QuotaCtx::disabled();
        assert!(!ctx.limits.enabled, "disabled() ⇒ quotas off");
        assert!(ctx.usage.load().is_empty(), "disabled() ⇒ empty usage map");
        assert!(
            ctx.rate.check("acme", std::time::Instant::now()),
            "unlimited limiter never denies"
        );
    }
}
