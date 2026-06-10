//! Offline repack: re-store cold objects as deltas against similar bases.
//! Heuristic: group by git type, sort by size descending, slide a window and
//! deltify each object against larger same-type neighbours. Every deltify is
//! self-verified by `DiskObjectStore::deltify`, so a bad delta never corrupts.
use crate::disk::DiskObjectStore;
use ledge_core::ObjectId;

/// Aggregate counters reported by a single [`repack`] pass.
#[derive(Debug, Default, Clone)]
pub struct RepackStats {
    /// Total object ids enumerated from the store at the start of the pass.
    pub objects_seen: usize,
    /// Objects successfully (and verifiably) re-stored as deltas this pass.
    pub objects_deltified: usize,
    /// On-disk bytes of repack candidates (non-delta objects) before the pass.
    pub bytes_before: u64,
    /// On-disk bytes of those same candidates after the pass.
    pub bytes_after: u64,
}

/// Number of larger same-type neighbours considered as a delta base per object.
const WINDOW: usize = 16;

/// Run one offline repack pass over `store`.
///
/// Enumerates every full (non-delta) object, groups by git type, sorts each
/// group by size descending, and for each object attempts to deltify it against
/// up to [`WINDOW`] larger same-type neighbours. Because
/// [`DiskObjectStore::deltify`] is self-verifying (it re-reads and byte-compares
/// the reconstructed object before committing), a delta that would corrupt or
/// fail to shrink is rejected and the full object is left untouched. Returns the
/// pass statistics; on-disk size never increases.
pub async fn repack(store: &DiskObjectStore) -> ledge_core::Result<RepackStats> {
    let ids = store.list_all_ids().await?;
    let mut stats = RepackStats { objects_seen: ids.len(), ..Default::default() };
    let mut items: Vec<(u8, u64, ObjectId)> = Vec::new();
    for id in ids {
        if store.delta_base_of(id).await?.is_some() {
            continue; // already a delta
        }
        let ty = match store.git_type_of(id).await {
            Ok(t) => t,
            Err(_) => continue,
        };
        let size = ledge_core::ObjectStore::read(store, id).await.map(|b| b.len() as u64).unwrap_or(0);
        stats.bytes_before += std::fs::metadata(store.object_path(&id)).map(|m| m.len()).unwrap_or(0);
        items.push((ty, size, id));
    }
    items.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1))); // type asc, size desc
    for i in 0..items.len() {
        let (ty_i, _sz, id_i) = items[i];
        let lo = i.saturating_sub(WINDOW);
        // Larger same-type neighbours in `[lo, i)` are candidate delta bases.
        for &(ty_j, _, base_j) in &items[lo..i] {
            if ty_j != ty_i {
                continue;
            }
            if store.deltify(id_i, base_j).await.unwrap_or(false) {
                stats.objects_deltified += 1;
                break;
            }
        }
    }
    for (_, _, id) in &items {
        stats.bytes_after += std::fs::metadata(store.object_path(id)).map(|m| m.len()).unwrap_or(0);
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ledge_core::ObjectStore;

    #[tokio::test]
    async fn repack_shrinks_similar_objects() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::disk::DiskObjectStore::new(dir.path().to_path_buf()).unwrap();
        let base: Vec<u8> = (0..600).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let mut ids = Vec::new();
        let mut contents = Vec::new();
        for v in 0..8 {
            let c = String::from_utf8(base.clone()).unwrap().replace("line 300\n", &format!("CHANGED v{v}\n")).into_bytes();
            ids.push(store.write_git_object(3, Bytes::from(c.clone())).await.unwrap());
            contents.push(c);
        }
        let before: u64 = ids.iter().map(|i| std::fs::metadata(store.object_path(i)).unwrap().len()).sum();
        let stats = repack(&store).await.unwrap();
        let after: u64 = ids.iter().map(|i| std::fs::metadata(store.object_path(i)).unwrap().len()).sum();
        assert!(stats.objects_deltified >= 1, "should deltify at least one ({stats:?})");
        assert!(after < before, "store shrank: {after} < {before}");
        for (i, c) in ids.iter().zip(&contents) {
            assert_eq!(ObjectStore::read(&store, *i).await.unwrap().as_ref(), c.as_slice(), "reads exact post-repack");
        }
    }
}
