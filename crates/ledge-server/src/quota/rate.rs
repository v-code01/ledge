//! Per-tenant token-bucket rate limiter (Phase 4d-3). FLESHED OUT in Task 4;
//! Task 2 needs only `unlimited()` + a `check` that always allows so `QuotaCtx`
//! compiles. Custom sharded bucket, NO new dep (R Q3).
use std::time::Instant;

/// Per-tenant request-rate token bucket. Task-2 stub: an inert limiter that never
/// denies. Task 4 REPLACES this whole file with a real sharded-bucket
/// implementation (`buckets: Mutex<HashMap<String, Bucket>>`, rate, burst).
pub struct TenantRateLimiter {
    // Task 4 REPLACES this whole file with: buckets: Mutex<HashMap<String, Bucket>>, rate, burst.
}

impl TenantRateLimiter {
    /// A limiter that never denies (no configured rate). The Task-2 default.
    pub fn unlimited() -> Self {
        Self {}
    }
    /// Allow/deny one request for `tenant` at `now`. Task-2 stub: always allows
    /// (Task 4 implements the bucket). `now` is INJECTED for deterministic tests.
    pub fn check(&self, _tenant: &str, _now: Instant) -> bool {
        true
    }
}
