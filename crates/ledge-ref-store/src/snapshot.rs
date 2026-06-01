//! Immutable, point-in-time snapshot of the ref namespace backed by a
//! copy-on-write ART root.
//!
//! `ArtSnapshot` holds a single `Option<Arc<ArtNode>>`.  Because the ART is
//! purely copy-on-write, the root captures the entire namespace at the moment
//! the snapshot was taken — subsequent mutations to `RefStoreImpl` produce
//! new roots and leave the snapshot root undisturbed.
//!
//! All reads are non-blocking and share unmodified subtrees with the live store
//! at zero extra allocation cost.

use std::sync::Arc;

use ledge_core::{RefEntry, RefName, RefSnapshot};

use crate::art::{art_lookup, art_prefix_iter, ArtNode};

/// Snapshot backed by a frozen ART root.
pub struct ArtSnapshot {
    /// The frozen root of the ART at snapshot time.  `None` means the
    /// namespace was empty when the snapshot was taken.
    pub root: Option<Arc<ArtNode>>,
}

impl RefSnapshot for ArtSnapshot {
    /// O(k) lookup where k is the key length; no locking.
    fn get(&self, name: &RefName) -> Option<RefEntry> {
        let root = self.root.as_ref()?;
        art_lookup(root, name.as_str().as_bytes(), 0).cloned()
    }

    /// O(n_matches * k) prefix scan; no locking.
    fn list(&self, prefix: &str) -> Vec<(RefName, RefEntry)> {
        let root = match &self.root {
            Some(r) => r,
            None => return Vec::new(),
        };
        art_prefix_iter(root, prefix.as_bytes(), 0)
            .into_iter()
            .filter_map(|(key_bytes, entry)| {
                let key_str = std::str::from_utf8(&key_bytes).ok()?;
                let name = RefName::new(key_str).ok()?;
                Some((name, entry))
            })
            .collect()
    }
}
