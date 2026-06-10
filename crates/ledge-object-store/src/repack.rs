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
    /// Objects written into the single consolidated pack this pass.
    pub objects_packed: usize,
    /// Object-store files present (loose + pack) before the pack stage.
    pub files_before: usize,
    /// Pack-directory entries remaining after consolidation (one `.pack` + one
    /// `.idx` for a non-empty store).
    pub files_after: usize,
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

    // ---- PACK: consolidate every present object into one new pack ----
    // Real file count before the pack stage: every loose object file plus every
    // existing pack-dir entry. This is what the dogfood/admin stats compare
    // against `files_after`.
    stats.files_before = count_loose_files(store) + count_pack_dir(store);
    let all_ids = store.list_all_ids().await?;
    let mut records: Vec<(ledge_core::ObjectId, Vec<u8>)> = Vec::with_capacity(all_ids.len());
    for id in &all_ids {
        if let Some(bytes) = store.raw_record(*id).await? {
            records.push((*id, bytes));
        }
    }
    let old_packs = store.pack_paths(); // snapshot BEFORE swap
    let new_pack = crate::pack::PackWriter::write(&store.pack_dir(), records)?;
    let new_ids = new_pack.ids();
    store.swap_packs(vec![std::sync::Arc::new(new_pack)]); // register BEFORE any delete

    // verify every object reads back through the real (now pack-backed) path
    // BEFORE deleting anything. A failure here returns Err via `?` with loose +
    // old packs still intact (the freshly written pack is at most an orphan).
    for id in &new_ids {
        ledge_core::ObjectStore::read(store, *id).await
            .map_err(|e| ledge_core::LedgeError::Corruption(format!("repack verify {}: {e}", id.to_hex())))?;
    }
    // safe now: delete loose files that are packed + the OLD pack/idx files.
    for id in &new_ids {
        let lp = store.object_path(id);
        if lp.exists() { let _ = std::fs::remove_file(&lp); }
    }
    for op in &old_packs {
        let _ = std::fs::remove_file(op);
        let _ = std::fs::remove_file(op.with_extension("idx"));
    }
    // Prune the now-empty `objects/<XX>/<YY>` loose dirs left behind by the deletes:
    // each empty dir still costs a filesystem block, which would otherwise dominate
    // `du` after packing (an empty 2-level skeleton is ~thousands of wasted blocks).
    // `pack/` and `tmp/` are preserved (not removed even if momentarily empty).
    let objects_root = store.pack_dir().parent().map(|p| p.to_path_buf());
    if let Some(root) = objects_root {
        if let Ok(level1) = std::fs::read_dir(&root) {
            for d1 in level1.flatten() {
                let name = d1.file_name();
                if name == std::ffi::OsStr::new("pack") || name == std::ffi::OsStr::new("tmp") {
                    continue;
                }
                let p1 = d1.path();
                if !p1.is_dir() {
                    continue;
                }
                if let Ok(level2) = std::fs::read_dir(&p1) {
                    for d2 in level2.flatten() {
                        let _ = std::fs::remove_dir(d2.path()); // removes only if empty
                    }
                }
                let _ = std::fs::remove_dir(&p1); // removes only if now empty
            }
        }
    }
    stats.objects_packed = new_ids.len();
    stats.files_after = std::fs::read_dir(store.pack_dir()).map(|rd| rd.count()).unwrap_or(0);

    Ok(stats)
}

/// Count loose object files under `objects/<XX>/<YY>/`, skipping the `tmp/` and
/// `pack/` directories. Used to record [`RepackStats::files_before`].
fn count_loose_files(store: &DiskObjectStore) -> usize {
    let root = store.pack_dir().parent().map(|p| p.to_path_buf());
    let Some(root) = root else { return 0 };
    let mut n = 0;
    if let Ok(l1) = std::fs::read_dir(&root) {
        for d1 in l1.flatten() {
            let name = d1.file_name();
            if name == std::ffi::OsStr::new("tmp") || name == std::ffi::OsStr::new("pack") {
                continue;
            }
            if !d1.path().is_dir() {
                continue;
            }
            if let Ok(l2) = std::fs::read_dir(d1.path()) {
                for d2 in l2.flatten() {
                    if let Ok(l3) = std::fs::read_dir(d2.path()) {
                        n += l3.flatten().filter(|e| e.path().is_file()).count();
                    }
                }
            }
        }
    }
    n
}

/// Count entries currently in the pack directory (`.pack` + `.idx` files).
fn count_pack_dir(store: &DiskObjectStore) -> usize {
    std::fs::read_dir(store.pack_dir()).map(|rd| rd.count()).unwrap_or(0)
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
        // repack now consolidates the deltified objects into a single pack and
        // unlinks the loose files, so the post-pass on-disk footprint is the
        // pack directory's total byte size.
        let after: u64 = std::fs::read_dir(store.pack_dir()).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| std::fs::metadata(e.path()).map(|m| m.len()).unwrap_or(0))
            .sum();
        assert!(stats.objects_deltified >= 1, "should deltify at least one ({stats:?})");
        assert!(after < before, "store shrank: {after} < {before}");
        for (i, c) in ids.iter().zip(&contents) {
            assert_eq!(ObjectStore::read(&store, *i).await.unwrap().as_ref(), c.as_slice(), "reads exact post-repack");
        }
    }

    #[tokio::test]
    async fn repack_consolidates_to_one_pack() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::disk::DiskObjectStore::new(dir.path().to_path_buf()).unwrap();
        let base: Vec<u8> = (0..600).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let mut ids = Vec::new(); let mut contents = Vec::new();
        for v in 0..10 {
            let c = String::from_utf8(base.clone()).unwrap().replace("line 300\n", &format!("V{v}\n")).into_bytes();
            ids.push(store.write_git_object(3, bytes::Bytes::from(c.clone())).await.unwrap());
            contents.push(c);
        }
        let loose_before = count_loose(dir.path());
        assert!(loose_before >= 10);
        let stats = repack(&store).await.unwrap();
        let loose_after = count_loose(dir.path());
        assert_eq!(loose_after, 0, "all loose objects packed (was {loose_before})");
        assert!(stats.objects_packed >= ids.len(), "packed {} >= {}", stats.objects_packed, ids.len());
        // every object still reads byte-exact from the pack
        for (i, c) in ids.iter().zip(&contents) {
            assert_eq!(ledge_core::ObjectStore::read(&store, *i).await.unwrap().as_ref(), c.as_slice(), "exact post-pack");
        }
        // exactly one .pack + one .idx remain
        let packdir = dir.path().join("objects").join("pack");
        let packs = std::fs::read_dir(&packdir).unwrap().filter_map(|e| e.ok()).filter(|e| e.path().extension().is_some_and(|x| x=="pack")).count();
        assert_eq!(packs, 1, "one pack file");
        // empty loose dirs are pruned (else they dominate du after packing)
        let empty_loose_dirs = std::fs::read_dir(dir.path().join("objects")).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| { let n = e.file_name(); n != std::ffi::OsStr::new("pack") && n != std::ffi::OsStr::new("tmp") && e.path().is_dir() })
            .count();
        assert_eq!(empty_loose_dirs, 0, "repack prunes the empty objects/XX loose dirs");
    }

    // counts loose object files under objects/XX/YY, excluding the pack/ and tmp/ dirs
    fn count_loose(data_dir: &std::path::Path) -> usize {
        let root = data_dir.join("objects");
        let mut n = 0;
        if let Ok(l1) = std::fs::read_dir(&root) {
            for d1 in l1.flatten() {
                let name = d1.file_name();
                if name == std::ffi::OsStr::new("tmp") || name == std::ffi::OsStr::new("pack") { continue; }
                if !d1.path().is_dir() { continue; }
                if let Ok(l2) = std::fs::read_dir(d1.path()) {
                    for d2 in l2.flatten() {
                        if let Ok(l3) = std::fs::read_dir(d2.path()) {
                            n += l3.flatten().filter(|e| e.path().is_file()).count();
                        }
                    }
                }
            }
        }
        n
    }
}
