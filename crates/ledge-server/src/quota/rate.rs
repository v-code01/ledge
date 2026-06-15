//! Per-tenant request-rate token bucket (Phase 4d-3 spec §3.3).
//!
//! A CUSTOM sharded token bucket — NOT `governor` — to keep deps minimal (spec
//! §3.3, R Q3): a `Mutex<HashMap<tenant, Bucket>>` + `std`/`f64` arithmetic, NO
//! new external crate. The clock is INJECTED into [`TenantRateLimiter::check`] as
//! an `Instant`, so refill is deterministic in tests (no sleeps); production code
//! calls [`TenantRateLimiter::check_now`], which supplies `Instant::now()`.
//! Enforced in the auth middleware on CLIENT requests, post-principal,
//! pre-`next.run`: a deny is a 429. `root`/disabled bypass is the CALLER's concern
//! (the middleware checks `QuotaCtx.limits.enforced_for(tenant)` before calling
//! `check_now`).
//!
//! # Per-node (honest)
//! The limiter is per-node, in-memory: in a multi-node cluster a tenant's
//! effective rate is up to `nodes × limit` (no global coordination). Standard
//! pragmatic choice (clients hit one node); see spec §6.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// One tenant's bucket. `tokens` is a float so partial refills accumulate.
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// Per-tenant token-bucket rate limiter. `rate == None` ⇒ unlimited (every
/// `check` allows). Idle buckets accumulate in the map; they are pruned
/// opportunistically when the map grows past a soft cap (bounded memory).
pub struct TenantRateLimiter {
    /// Sustained refill rate (tokens/sec). `None` ⇒ unlimited (never denies).
    rate: Option<f64>,
    /// Burst capacity (max tokens). Defaults to `rate` when the config omits it.
    burst: f64,
    /// Per-tenant buckets. A `std::sync::Mutex` is correct here: the critical
    /// section is O(1) and never `.await`s (R Q3).
    buckets: Mutex<HashMap<String, Bucket>>,
}

/// Soft cap on the bucket map size before an opportunistic prune of buckets that
/// are full (idle long enough to have refilled to `burst`) — bounds memory under
/// a churn of distinct tenants without affecting an active tenant's accounting.
const PRUNE_SOFT_CAP: usize = 10_000;

impl TenantRateLimiter {
    /// Build a limiter from a sustained rate + optional burst (spec §3.1). A burst
    /// of `None` defaults to the rate (a one-second bucket). A `rate` of `None` (or
    /// 0) yields an unlimited limiter.
    pub fn new(rate_per_sec: Option<u32>, burst: Option<u32>) -> Self {
        let rate = rate_per_sec.filter(|&r| r > 0).map(|r| r as f64);
        let burst = match (rate, burst) {
            (None, _) => 0.0,
            (Some(r), None) => r, // burst defaults to the rate
            (Some(_), Some(b)) => (b.max(1)) as f64, // at least 1 so a single req can pass
        };
        Self {
            rate,
            burst,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// A limiter that NEVER denies (no configured rate). The `QuotaCtx::disabled()`
    /// default and the bypass for tenants without a rate limit.
    pub fn unlimited() -> Self {
        Self::new(None, None)
    }

    /// Allow (`true`) or deny (`false`) one request for `tenant` at `now`.
    ///
    /// Unlimited ⇒ always allows. Otherwise: refill the tenant's bucket by
    /// `elapsed_secs * rate` (capped at `burst`), then if `>= 1.0` token is
    /// available, consume one and allow; else deny. `now` is INJECTED so tests
    /// drive refill deterministically (R Q3); production calls [`Self::check_now`].
    pub fn check(&self, tenant: &str, now: Instant) -> bool {
        let Some(rate) = self.rate else {
            return true; // unlimited — no lock taken
        };
        let mut buckets = self.buckets.lock().unwrap();
        // Opportunistic prune when the map is large: drop buckets that have
        // refilled to full (idle), so memory stays bounded under tenant churn.
        if buckets.len() > PRUNE_SOFT_CAP {
            buckets.retain(|_, b| {
                let elapsed = now.saturating_duration_since(b.last_refill).as_secs_f64();
                (b.tokens + elapsed * rate) < self.burst
            });
        }
        let bucket = buckets.entry(tenant.to_string()).or_insert_with(|| Bucket {
            tokens: self.burst, // a new tenant starts with a full bucket
            last_refill: now,
        });
        // Refill: add elapsed * rate, cap at burst. saturating_duration_since
        // makes a non-monotone `now` (should not happen with Instant) yield 0.
        let elapsed = now
            .saturating_duration_since(bucket.last_refill)
            .as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(self.burst);
        bucket.last_refill = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Production convenience: [`Self::check`] with the real monotonic clock. The
    /// auth middleware calls this; tests call [`Self::check`] with an injected
    /// `Instant` so refill stays deterministic (no real-clock flake, no sleeps).
    pub fn check_now(&self, tenant: &str) -> bool {
        self.check(tenant, Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn unlimited_never_denies() {
        let rl = TenantRateLimiter::unlimited();
        let t0 = Instant::now();
        for _ in 0..1000 {
            assert!(rl.check("acme", t0), "unlimited must always allow");
        }
        assert!(rl.check_now("acme"), "check_now also always allows");
    }

    #[test]
    fn burst_then_deny_then_refill_allows_again() {
        // rate=1/sec, burst=3: first 3 pass at t0, the 4th denies, then after 1s
        // (one token refilled) the next passes. Deterministic via injected time.
        let rl = TenantRateLimiter::new(Some(1), Some(3));
        let t0 = Instant::now();
        assert!(rl.check("acme", t0), "1st (burst)");
        assert!(rl.check("acme", t0), "2nd (burst)");
        assert!(rl.check("acme", t0), "3rd (burst)");
        assert!(!rl.check("acme", t0), "4th denied (bucket empty)");
        // Advance 1s ⇒ exactly 1 token refilled ⇒ one more passes, the next denies.
        let t1 = t0 + Duration::from_secs(1);
        assert!(rl.check("acme", t1), "refilled token passes");
        assert!(!rl.check("acme", t1), "denied again (only 1 refilled)");
    }

    #[test]
    fn per_tenant_independence() {
        // acme exhausts its bucket; globex is unaffected (separate bucket).
        let rl = TenantRateLimiter::new(Some(1), Some(2));
        let t0 = Instant::now();
        assert!(rl.check("acme", t0));
        assert!(rl.check("acme", t0));
        assert!(!rl.check("acme", t0), "acme exhausted");
        assert!(rl.check("globex", t0), "globex independent");
        assert!(rl.check("globex", t0));
        assert!(!rl.check("globex", t0), "globex exhausted on its own");
    }

    #[test]
    fn burst_defaults_to_rate_when_omitted() {
        // rate=2, burst=None ⇒ burst=2: two pass, third denies at t0.
        let rl = TenantRateLimiter::new(Some(2), None);
        let t0 = Instant::now();
        assert!(rl.check("acme", t0));
        assert!(rl.check("acme", t0));
        assert!(!rl.check("acme", t0));
    }

    #[test]
    fn refill_caps_at_burst() {
        // rate=1, burst=2. Idle a long time, then a single check sees the bucket
        // capped at burst (2), NOT rate*elapsed — so exactly 2 pass, then deny.
        let rl = TenantRateLimiter::new(Some(1), Some(2));
        let t0 = Instant::now();
        assert!(rl.check("acme", t0)); // consume 1 → 1 left
        assert!(rl.check("acme", t0)); // consume 1 → 0 left
        assert!(!rl.check("acme", t0), "empty");
        // Idle 100s ⇒ would refill 100 tokens, but cap is burst=2.
        let far = t0 + Duration::from_secs(100);
        assert!(rl.check("acme", far)); // 2 - 1 = 1
        assert!(rl.check("acme", far)); // 1 - 1 = 0
        assert!(!rl.check("acme", far), "capped at burst, not 100");
    }
}
