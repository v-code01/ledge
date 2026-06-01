//! Git-type-aware object reachability over a [`DiskObjectStore`].
//!
//! Shared by `ledge-git` (clone/fetch must pack the full reachable closure of a
//! wanted commit) and `ledge-workspace` (GC marks every object reachable from a
//! live root). One walk, one on-disk format, two callers.

use std::collections::{HashSet, VecDeque};

use bytes::Bytes;
use ledge_core::{ObjectId, ObjectStore};

use crate::DiskObjectStore;

/// Decode a 40-char lowercase hex SHA-1 into 20 raw bytes.
pub fn parse_hex_sha1(hex_str: &str) -> Option<[u8; 20]> {
    if hex_str.len() != 40 {
        return None;
    }
    let bytes = hex::decode(hex_str).ok()?;
    let mut arr = [0u8; 20];
    arr.copy_from_slice(&bytes);
    Some(arr)
}

/// Extract the `tree` SHA-1 from a git commit object body.
///
/// A commit body is a header block of `key SP value LF` lines (the first line
/// is always `tree <40-hex-sha1>`), a blank line, then the message.
pub fn commit_tree_sha1(content: &[u8]) -> Option<[u8; 20]> {
    let text = std::str::from_utf8(content).ok()?;
    for line in text.lines() {
        if line.is_empty() {
            break; // end of header block
        }
        if let Some(rest) = line.strip_prefix("tree ") {
            return parse_hex_sha1(rest.trim());
        }
    }
    None
}

/// Extract all `parent` SHA-1s from a git commit object body (0+ entries).
pub fn commit_parent_sha1s(content: &[u8]) -> Vec<[u8; 20]> {
    let mut parents = Vec::new();
    if let Ok(text) = std::str::from_utf8(content) {
        for line in text.lines() {
            if line.is_empty() {
                break; // end of header block
            }
            if let Some(rest) = line.strip_prefix("parent ") {
                if let Some(sha1) = parse_hex_sha1(rest.trim()) {
                    parents.push(sha1);
                }
            }
        }
    }
    parents
}

/// Extract all child SHA-1s referenced by a git tree object body.
///
/// A tree body is a packed sequence of entries:
/// ```text
/// <mode-ascii> SP <name> NUL <20-byte-raw-sha1>
/// ```
/// Both sub-trees and blobs are returned; the walker resolves each by SHA-1.
pub fn tree_child_sha1s(content: &[u8]) -> Vec<[u8; 20]> {
    let mut children = Vec::new();
    let mut pos = 0usize;
    while pos < content.len() {
        let nul = match content[pos..].iter().position(|&b| b == 0) {
            Some(n) => pos + n,
            None => break,
        };
        let sha1_start = nul + 1;
        let sha1_end = sha1_start + 20;
        if sha1_end > content.len() {
            break;
        }
        let mut sha1 = [0u8; 20];
        sha1.copy_from_slice(&content[sha1_start..sha1_end]);
        children.push(sha1);
        pos = sha1_end;
    }
    children
}

/// Compute the set of [`ObjectId`]s reachable from `roots`, walking the git
/// object graph (commit → tree + parents, tree → children, blob/tag → leaf).
///
/// The returned set **includes the roots themselves**. Children are referenced
/// by git SHA-1, so we build the store's `sha1 → ObjectId` index **once** at the
/// start and resolve each discovered child through it; a child whose object is
/// genuinely absent (no index entry) is skipped rather than erroring. A `read`
/// or `git_type_of` failure on an enqueued id is likewise skipped defensively so
/// a torn/partial object can never panic a GC pass.
///
/// **Tag simplification (Phase 2a):** tag objects are treated as **leaves**.
/// An annotated tag could point at a commit, but no current Ledge path writes
/// annotated-tag objects, and Phase 1 only advertises `refs/heads/*` /
/// `refs/tags/*` whose targets we created directly. When annotated tags are
/// introduced, add a `4 => parse tagged object id` arm here.
///
/// Termination: the object set is finite and content-addressed (immutable), and
/// `visited` dedups, so the BFS reaches a fixpoint and is cycle-safe and
/// shared-subtree-safe.
///
/// Complexity: one `sha1_index()` scan (O(N) opens) plus O(R) reads where R is
/// the size of the reachable closure.
pub async fn reachable_from(
    store: &DiskObjectStore,
    roots: impl IntoIterator<Item = ObjectId>,
) -> ledge_core::Result<HashSet<ObjectId>> {
    let mut visited: HashSet<ObjectId> = HashSet::new();
    let mut queue: VecDeque<ObjectId> = roots.into_iter().collect();
    if queue.is_empty() {
        return Ok(visited);
    }

    // Resolve child git-SHA-1s → ObjectIds once for the whole walk.
    let sha1_to_obj = store.sha1_index().await?;

    while let Some(id) = queue.pop_front() {
        if !visited.insert(id) {
            continue; // already processed (cycle / shared subtree)
        }
        // Read content + type; skip defensively on any error (torn object).
        let content: Bytes = match store.read(id).await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let git_type = match store.git_type_of(id).await {
            Ok(t) => t,
            Err(_) => continue,
        };

        // Collect child git-SHA-1s by object type.
        let child_sha1s: Vec<[u8; 20]> = match git_type {
            1 => {
                // commit → tree + parents
                let mut v = Vec::new();
                if let Some(tree) = commit_tree_sha1(&content) {
                    v.push(tree);
                }
                v.extend(commit_parent_sha1s(&content));
                v
            }
            2 => tree_child_sha1s(&content), // tree → children
            _ => Vec::new(),                 // blob / tag → leaf (see doc note)
        };

        for sha1 in child_sha1s {
            if let Some(child_id) = sha1_to_obj.get(&sha1).copied() {
                if !visited.contains(&child_id) {
                    queue.push_back(child_id);
                }
            }
            // else: object genuinely absent → skip (mirrors fetch behavior).
        }
    }

    Ok(visited)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DiskObjectStore;
    use bytes::Bytes;
    use ledge_core::ObjectId;
    use std::collections::HashSet;
    use tempfile::tempdir;

    fn make_store() -> (DiskObjectStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = DiskObjectStore::new(dir.path().to_path_buf()).unwrap();
        (store, dir)
    }

    /// Build commit → tree → blob in the store and return all three ids plus the
    /// commit id (the single root).
    ///
    /// The tree body is encoded in git's on-disk tree format
    /// (`<mode> SP <name> NUL <20-byte-raw-sha1>`) referencing the blob's git
    /// SHA-1; the commit body references the tree's git SHA-1 in its `tree`
    /// header line. We read each child's SHA-1 back from the store via
    /// `sha1_of` so the wiring uses the exact bytes the walker will resolve.
    async fn build_graph(store: &DiskObjectStore) -> (ObjectId, ObjectId, ObjectId) {
        // Blob.
        let blob_id = store
            .write_git_object(3, Bytes::from_static(b"file contents"))
            .await
            .unwrap();
        let blob_sha1 = store.sha1_of(blob_id).await.unwrap();

        // Tree: one entry "file" (mode 100644) → blob.
        let mut tree_body = Vec::new();
        tree_body.extend_from_slice(b"100644 file\0");
        tree_body.extend_from_slice(&blob_sha1);
        let tree_id = store
            .write_git_object(2, Bytes::from(tree_body))
            .await
            .unwrap();
        let tree_sha1 = store.sha1_of(tree_id).await.unwrap();

        // Commit referencing the tree (no parents).
        let commit_body = format!(
            "tree {}\nauthor a <a@x> 0 +0000\ncommitter a <a@x> 0 +0000\n\nmsg\n",
            hex::encode(tree_sha1)
        );
        let commit_id = store
            .write_git_object(1, Bytes::from(commit_body.into_bytes()))
            .await
            .unwrap();

        (commit_id, tree_id, blob_id)
    }

    #[tokio::test]
    async fn reachable_from_commit_includes_tree_and_blob() {
        let (store, _dir) = make_store();
        let (commit_id, tree_id, blob_id) = build_graph(&store).await;

        let reachable = reachable_from(&store, [commit_id]).await.unwrap();

        let expected: HashSet<ObjectId> = [commit_id, tree_id, blob_id].into_iter().collect();
        assert_eq!(reachable, expected, "closure must be commit + tree + blob");
    }

    #[tokio::test]
    async fn reachable_from_excludes_unreachable_orphan_blob() {
        let (store, _dir) = make_store();
        let (commit_id, tree_id, blob_id) = build_graph(&store).await;

        // An orphan blob no ref/commit/tree points at.
        let orphan_id = store
            .write_git_object(3, Bytes::from_static(b"unreferenced orphan"))
            .await
            .unwrap();

        let reachable = reachable_from(&store, [commit_id]).await.unwrap();

        assert!(reachable.contains(&commit_id));
        assert!(reachable.contains(&tree_id));
        assert!(reachable.contains(&blob_id));
        assert!(
            !reachable.contains(&orphan_id),
            "orphan blob must NOT be reachable from the commit root"
        );
    }

    #[tokio::test]
    async fn reachable_from_no_roots_is_empty() {
        let (store, _dir) = make_store();
        let _ = build_graph(&store).await;
        let reachable = reachable_from(&store, std::iter::empty()).await.unwrap();
        assert!(reachable.is_empty(), "no roots → nothing reachable");
    }
}
