use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Hybrid Logical Clock.
///
/// Bit layout: `[63..20]` = wall time in milliseconds since Unix epoch (44 bits,
/// overflows year 2527), `[19..0]` = logical counter per millisecond (20 bits,
/// up to 1 048 575 events per ms per node).
///
/// `tick()` is strictly monotonically increasing and lock-free (CAS loop).
/// Used by the ref store for snapshot isolation and causality ordering.
///
/// # Thread Safety
/// `HLC` wraps a single `AtomicU64` and adds no other state, so it is
/// trivially `Send + Sync`. The `unsafe impl` below is sound.
pub struct HLC(AtomicU64);

/// Returns the current wall-clock time in milliseconds since Unix epoch.
///
/// # Panics
/// Panics if the system clock is set before the Unix epoch (1970-01-01).
#[inline]
fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}

impl HLC {
    /// Create a new HLC seeded with the current wall-clock time.
    pub fn new() -> Self {
        Self(AtomicU64::new(wall_ms() << 20))
    }

    /// Advance and return a new unique timestamp.
    ///
    /// CAS loop: `candidate = max(wall_ms << 20, last + 1)`.
    /// This ensures:
    /// - Wall time advances when the clock ticks forward.
    /// - The logical counter increments within a millisecond.
    /// - The returned value is always strictly greater than any prior value.
    ///
    /// # Complexity
    /// O(1) amortised; contention causes bounded spin (lock-free, not wait-free).
    #[inline]
    pub fn tick(&self) -> u64 {
        loop {
            let last = self.0.load(Ordering::Acquire);
            let candidate = std::cmp::max(wall_ms() << 20, last.wrapping_add(1));
            match self
                .0
                .compare_exchange_weak(last, candidate, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return candidate,
                Err(_) => std::hint::spin_loop(),
            }
        }
    }

    /// Non-advancing read of the current stored timestamp value.
    ///
    /// Returns the last value written by `tick()` (or the seed if `tick()`
    /// has never been called). Does **not** advance the clock.
    #[inline]
    pub fn now(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

impl Default for HLC {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: `HLC` is a newtype over `AtomicU64`, which is `Send + Sync`.
// No additional state is added, so these impls are sound.
unsafe impl Send for HLC {}
unsafe impl Sync for HLC {}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn tick_increases() {
        let hlc = HLC::new();
        let t1 = hlc.tick();
        let t2 = hlc.tick();
        assert!(t2 > t1, "t2={t2} must be > t1={t1}");
    }

    #[test]
    fn now_does_not_advance() {
        let hlc = HLC::new();
        let t1 = hlc.tick();
        let n = hlc.now();
        let t2 = hlc.tick();
        assert!(n >= t1, "now={n} should be >= last tick t1={t1}");
        assert!(t2 > n || t2 == n + 1, "tick after now: t2={t2}, now={n}");
    }

    #[test]
    fn counter_bits_increment_within_same_ms() {
        let hlc = HLC::new();
        let ticks: Vec<u64> = (0..1024).map(|_| hlc.tick()).collect();
        for window in ticks.windows(2) {
            let (a, b) = (window[0], window[1]);
            assert!(
                b > a,
                "tick sequence must be strictly increasing: {a} then {b}"
            );
            if (a >> 20) == (b >> 20) {
                assert_eq!(
                    b & 0xFFFFF,
                    (a & 0xFFFFF) + 1,
                    "counter must increment by 1 within same ms. a={a:064b} b={b:064b}"
                );
            }
        }
    }

    #[test]
    fn wall_bits_plausible() {
        let hlc = HLC::new();
        let t = hlc.tick();
        let wall_ms = t >> 20;
        assert!(
            wall_ms >= 1_704_067_200_000,
            "wall_ms={wall_ms} implausibly old"
        );
    }

    proptest! {
        #[test]
        fn prop_tick_monotonic(n in 2usize..512) {
            let hlc = HLC::new();
            let mut prev = hlc.tick();
            for _ in 1..n {
                let next = hlc.tick();
                prop_assert!(next > prev, "monotonicity violated: prev={prev} next={next}");
                prev = next;
            }
        }
    }

    #[test]
    fn concurrent_monotonicity() {
        use std::sync::{Arc, Mutex};
        use std::thread;
        let hlc = Arc::new(HLC::new());
        let collected = Arc::new(Mutex::new(Vec::<u64>::new()));
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let hlc = Arc::clone(&hlc);
                let collected = Arc::clone(&collected);
                thread::spawn(move || {
                    for _ in 0..256 {
                        collected.lock().unwrap().push(hlc.tick());
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let mut all = collected.lock().unwrap().clone();
        all.sort_unstable();
        for window in all.windows(2) {
            assert_ne!(
                window[0], window[1],
                "duplicate tick: {} appeared twice",
                window[0]
            );
        }
    }
}
