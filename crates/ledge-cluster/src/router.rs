//! Deterministic, total ref-name → shard routing.
//!
//! The router is the partition function of the multi-raft cluster: every ref
//! name maps to exactly one shard (totality), the same name always maps to the
//! same shard on every node (determinism via a fixed FNV-1a hash, never
//! SipHash), and all refs under one tenant/repo prefix collapse to one shard
//! (co-location, so the common single-tenant operation is single-shard and
//! fully linearizable). These three properties are the routing invariant the
//! cluster relies on for per-shard linearizability.

use ledge_workspace::id::WorkspaceId;

/// Identifier of one shard (one independent Raft group). `0..num_shards`.
#[derive(
    Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct ShardId(pub u32);

/// Which shards a `list(prefix)` must consult, per [`ShardRouter::shards_for_prefix`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardSpan {
    /// The prefix pins exactly one shard.
    One(ShardId),
    /// The prefix may span shards; fan out to all of them.
    All,
}

/// Maps ref names to shards. Cheap to clone (one `u32`); construct once per node
/// from the cluster's static shard count and share by value.
#[derive(Copy, Clone, Debug)]
pub struct ShardRouter {
    num_shards: u32,
}

impl ShardRouter {
    /// Create a router over `num_shards` shards.
    ///
    /// # Panics
    /// If `num_shards == 0` — a cluster with zero shards cannot route anything;
    /// this is a configuration bug, caught at construction, not per-call.
    pub fn new(num_shards: u32) -> Self {
        assert!(num_shards >= 1, "ShardRouter requires num_shards >= 1");
        Self { num_shards }
    }

    /// Number of shards this router partitions into.
    #[inline]
    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    /// Map `ref_name` to its owning shard. Total and deterministic.
    ///
    /// Complexity: O(len(ref_name)) — one split pass + one FNV-1a hash.
    /// Side effects: none (pure).
    pub fn shard_for(&self, ref_name: &str) -> ShardId {
        let key = Self::namespace_key(ref_name);
        let h = fnv1a64(key.as_bytes());
        // num_shards >= 1 invariant makes the modulo well-defined.
        ShardId((h % self.num_shards as u64) as u32)
    }

    /// Map a workspace to its owning shard, co-locating it with the workspace's
    /// refs (D5). A workspace's refs live under `refs/workspaces/<hex>/...`,
    /// whose namespace key is `workspaces/<hex>`; this hashes the SAME key so a
    /// workspace's lease and its refs always land on one Raft group, keeping the
    /// workspace lifecycle single-shard linearizable.
    pub fn shard_for_workspace(&self, ws: &WorkspaceId) -> ShardId {
        let key = format!("workspaces/{}", ws.to_hex());
        let h = fnv1a64(key.as_bytes());
        ShardId((h % self.num_shards as u64) as u32)
    }

    /// Decide which shards a `list(prefix)` must consult (D3).
    ///
    /// - A prefix that pins a complete namespace key (e.g.
    ///   `refs/workspaces/<tenant>/`) maps to exactly one shard
    ///   ([`ShardSpan::One`]).
    /// - A shallower prefix (e.g. `refs/`, `refs/workspaces/`) may span shards
    ///   and must fan out to all of them ([`ShardSpan::All`]).
    ///
    /// The rule mirrors [`Self::namespace_key`]: the prefix is "pinned" iff it
    /// contains enough path segments to fully determine the namespace key that
    /// `shard_for` would hash. Broad cross-shard `list` is per-shard
    /// linearizable, not a single global atomic snapshot — acceptable because
    /// `list` never drives CAS decisions.
    pub fn shards_for_prefix(&self, prefix: &str) -> ShardSpan {
        // A single shard cannot fan out; everything lives on shard 0.
        if self.num_shards == 1 {
            return ShardSpan::One(ShardId(0));
        }
        let trimmed = prefix.trim_end_matches('/');
        let segs: Vec<&str> = trimmed.split('/').filter(|s| !s.is_empty()).collect();
        let pinned: Option<&str> = match segs.as_slice() {
            // refs/workspaces/<tenant>[/...] — key fully determined.
            ["refs", "workspaces", _tenant, ..] => Some(trimmed),
            // refs/heads|tags/<name>[/...] — key fully determined.
            ["refs", "heads" | "tags", _name, ..] => Some(trimmed),
            // refs/<a>/<b>[/...] — key fully determined (rule 4).
            ["refs", _a, _b, ..] => Some(trimmed),
            // refs/<a> exactly with a trailing slash means "everything under
            // <a>", which (rules 2/4) can still span shards, so do not pin.
            // Non-refs prefixes (whole-string key) pin only when complete; a
            // bare token may be a strict prefix of several distinct keys, so be
            // conservative and fan out.
            _ => None,
        };
        match pinned {
            Some(p) => ShardSpan::One(self.shard_for(p)),
            None => ShardSpan::All,
        }
    }

    /// Extract the namespace key per the documented rule. Returns an owned
    /// String because rules 2–4 synthesize a substring join; allocation is
    /// negligible relative to the Raft round-trip this feeds.
    fn namespace_key(ref_name: &str) -> String {
        let segs: Vec<&str> = ref_name.split('/').collect();
        // Rule 1: not a refs/* name → whole string is the key (still total).
        if segs.first() != Some(&"refs") {
            return ref_name.to_string();
        }
        match segs.as_slice() {
            // Rule 2: refs/workspaces/<tenant>/... → "workspaces/<tenant>"
            ["refs", "workspaces", tenant, ..] => format!("workspaces/{tenant}"),
            // Rule 3: refs/heads|tags/<name>/... → "refs/<kind>/<name>"
            ["refs", kind @ ("heads" | "tags"), name, ..] => format!("refs/{kind}/{name}"),
            // Rule 4: refs/<a>/<b>/... → "<a>/<b>"
            ["refs", a, b, ..] => format!("{a}/{b}"),
            // refs/<a> exactly → "<a>"
            ["refs", a] => a.to_string(),
            // "refs" alone, or "refs/" with trailing empties.
            _ => ref_name.to_string(),
        }
    }
}

/// FNV-1a 64-bit. Fixed algorithm → identical on every platform and Rust
/// version, which `DefaultHasher` (SipHash) does NOT guarantee. Cluster
/// correctness requires every node to compute the identical shard map, so the
/// hash must be specified, not implementation-defined.
#[inline]
fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    // A representative corpus spanning every ref shape Ledge emits.
    fn corpus() -> Vec<String> {
        let mut v = Vec::new();
        for t in 0..50u32 {
            v.push(format!("refs/workspaces/tenant{t}/heads/main"));
            v.push(format!("refs/workspaces/tenant{t}/heads/feature"));
            v.push(format!("refs/workspaces/tenant{t}/tags/v1"));
        }
        for r in 0..50u32 {
            v.push(format!("refs/heads/repo{r}"));
            v.push(format!("refs/heads/repo{r}/sub"));
            v.push(format!("refs/tags/repo{r}"));
        }
        v.push("refs/meta/config".into());
        v.push("HEAD".into()); // malformed (no refs/ prefix) — must still map
        v.push("garbage".into());
        v
    }

    #[test]
    fn totality_every_ref_maps_in_range() {
        for n in [1u32, 2, 4, 8, 16] {
            let r = ShardRouter::new(n);
            for name in corpus() {
                let s = r.shard_for(&name);
                assert!(
                    s.0 < n,
                    "ref {name} → shard {} out of range (num_shards={n})",
                    s.0
                );
            }
        }
    }

    #[test]
    fn determinism_same_ref_same_shard() {
        let r = ShardRouter::new(8);
        for name in corpus() {
            let a = r.shard_for(&name);
            let b = r.shard_for(&name);
            assert_eq!(a, b, "non-deterministic shard for {name}");
        }
        // Determinism across independent router instances (cross-node proxy).
        let r2 = ShardRouter::new(8);
        for name in corpus() {
            assert_eq!(
                r.shard_for(&name),
                r2.shard_for(&name),
                "two routers disagree on {name}"
            );
        }
    }

    #[test]
    fn colocation_same_tenant_prefix_same_shard() {
        let r = ShardRouter::new(8);
        let a = r.shard_for("refs/workspaces/acme/heads/main");
        let b = r.shard_for("refs/workspaces/acme/heads/feature");
        let c = r.shard_for("refs/workspaces/acme/tags/release-1");
        assert_eq!(a, b);
        assert_eq!(b, c, "all of one workspace's refs must share a shard");

        // repo co-location for heads
        let h1 = r.shard_for("refs/heads/myrepo");
        let h2 = r.shard_for("refs/heads/myrepo/wip");
        assert_eq!(h1, h2, "a repo's heads must share a shard");
    }

    #[test]
    fn distribution_spreads_tenants() {
        let n = 8u32;
        let r = ShardRouter::new(n);
        let mut buckets = std::collections::HashMap::<u32, usize>::new();
        for t in 0..200u32 {
            let s = r.shard_for(&format!("refs/workspaces/tenant{t}/heads/main"));
            *buckets.entry(s.0).or_default() += 1;
        }
        // Sanity, not a chi-squared test: at least half the shards are used,
        // and no shard hoards more than ~60% of keys.
        assert!(
            buckets.len() as u32 >= n / 2,
            "tenants clustered into too few shards: {buckets:?}"
        );
        let max = buckets.values().copied().max().unwrap();
        assert!(
            max < 200 * 6 / 10,
            "one shard hoards {max}/200 tenants: {buckets:?}"
        );
    }

    #[test]
    #[should_panic]
    fn zero_shards_rejected() {
        let _ = ShardRouter::new(0);
    }

    #[test]
    fn shards_for_prefix_pins_full_namespace_keys() {
        let r = ShardRouter::new(8);
        // A complete namespace key pins exactly one shard, and it is the same
        // shard `shard_for` would choose for a ref under that prefix.
        for (prefix, ref_under) in [
            ("refs/workspaces/acme/", "refs/workspaces/acme/heads/main"),
            ("refs/heads/myrepo/", "refs/heads/myrepo/wip"),
            ("refs/tags/v1/", "refs/tags/v1/notes"),
            ("refs/meta/config/", "refs/meta/config/x"),
        ] {
            match r.shards_for_prefix(prefix) {
                ShardSpan::One(s) => assert_eq!(
                    s,
                    r.shard_for(ref_under),
                    "pinned prefix {prefix} must select the ref's shard"
                ),
                ShardSpan::All => panic!("prefix {prefix} should pin one shard"),
            }
        }
    }

    #[test]
    fn shards_for_prefix_fans_out_on_broad_prefixes() {
        let r = ShardRouter::new(8);
        for prefix in ["refs/", "refs/workspaces/", "refs"] {
            assert_eq!(
                r.shards_for_prefix(prefix),
                ShardSpan::All,
                "broad prefix {prefix} must fan out to all shards"
            );
        }
    }

    #[test]
    fn single_shard_never_fans_out() {
        let r = ShardRouter::new(1);
        assert_eq!(r.shards_for_prefix("refs/"), ShardSpan::One(ShardId(0)));
        assert_eq!(
            r.shards_for_prefix("refs/workspaces/acme/"),
            ShardSpan::One(ShardId(0))
        );
    }

    #[test]
    fn workspace_colocates_with_its_refs() {
        use ledge_workspace::id::WorkspaceId;
        let r = ShardRouter::new(8);
        for seed in [[1u8; 16], [2u8; 16], [0xab; 16], [0u8; 16]] {
            let ws = WorkspaceId::from_bytes(seed);
            let ref_name = format!("refs/workspaces/{}/main", ws.to_hex());
            assert_eq!(
                r.shard_for_workspace(&ws),
                r.shard_for(&ref_name),
                "lease and workspace refs must co-locate (D5)"
            );
        }
    }
}
