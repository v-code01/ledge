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

/// Decode a 40-char ASCII-hex SHA-1 from raw header-value bytes, tolerating
/// surrounding ASCII whitespace (e.g. a stray CR).
///
/// Byte-oriented so it never requires the enclosing object to be valid UTF-8:
/// git commit/tag *messages* and author/tagger *names* may contain arbitrary
/// bytes, but the structural `key SP <40-hex> LF` header lines the reachability
/// walk cares about are pure ASCII. Requiring the whole object to be UTF-8 would
/// make these parsers silently return nothing for such objects — dropping their
/// tree/parent/tagged refs from the reachable set (GC data loss + broken clones).
fn parse_hex_sha1_bytes(value: &[u8]) -> Option<[u8; 20]> {
    let trimmed = value.trim_ascii();
    if trimmed.len() != 40 {
        return None;
    }
    let mut arr = [0u8; 20];
    hex::decode_to_slice(trimmed, &mut arr).ok()?;
    Some(arr)
}

/// Extract the `tree` SHA-1 from a git commit object body.
///
/// A commit body is a header block of `key SP value LF` lines (the first line
/// is always `tree <40-hex-sha1>`), a blank line, then the message.
pub fn commit_tree_sha1(content: &[u8]) -> Option<[u8; 20]> {
    // Byte-oriented: scan header lines only, stopping at the blank line. Never
    // requires UTF-8 (a non-UTF-8 message must not hide the tree — see
    // parse_hex_sha1_bytes).
    for line in content.split(|&b| b == b'\n') {
        if line.is_empty() {
            break; // end of header block
        }
        if let Some(rest) = line.strip_prefix(b"tree ") {
            return parse_hex_sha1_bytes(rest);
        }
    }
    None
}

/// Extract all `parent` SHA-1s from a git commit object body (0+ entries).
pub fn commit_parent_sha1s(content: &[u8]) -> Vec<[u8; 20]> {
    let mut parents = Vec::new();
    for line in content.split(|&b| b == b'\n') {
        if line.is_empty() {
            break; // end of header block
        }
        if let Some(rest) = line.strip_prefix(b"parent ") {
            if let Some(sha1) = parse_hex_sha1_bytes(rest) {
                parents.push(sha1);
            }
        }
    }
    parents
}

/// Extract the tagged `object` SHA-1 from a git annotated-tag object body.
///
/// A tag body is a header block (`key SP value LF` lines) whose first line is
/// `object <40-hex-sha1>` naming the object it tags (usually a commit), followed
/// by `type`/`tag`/`tagger` lines, a blank line, then the message. Returns the
/// tagged object's SHA-1 so a walker follows a tag through to what it references.
pub fn tag_object_sha1(content: &[u8]) -> Option<[u8; 20]> {
    // Byte-oriented (see commit_tree_sha1): a tag's tagger name or message may
    // contain arbitrary bytes, but the `object <40-hex>` line is ASCII.
    for line in content.split(|&b| b == b'\n') {
        if line.is_empty() {
            break; // end of header block
        }
        if let Some(rest) = line.strip_prefix(b"object ") {
            return parse_hex_sha1_bytes(rest);
        }
    }
    None
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

/// The tree's children that must exist in THIS repository.
///
/// Same scan as [`tree_child_sha1s`], but skips **gitlinks** (mode `160000`): a
/// submodule entry names a commit that lives in a *different* repository and is
/// deliberately absent from this one. Git's own connectivity check makes exactly
/// this exclusion, and a caller that enforces "everything a tree names must be
/// present" (the push connectivity check) MUST use this, or it would reject
/// every push of a repo that has a submodule.
///
/// A tree entry is `<mode-ascii> SP <name> NUL <20-byte-raw-sha1>`, so the mode
/// runs from the start of the entry to the first space.
pub fn tree_child_sha1s_in_repo(content: &[u8]) -> Vec<[u8; 20]> {
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
        let header = &content[pos..nul];
        let mode = match header.iter().position(|&b| b == b' ') {
            Some(sp) => &header[..sp],
            None => header,
        };
        if mode != b"160000" {
            let mut sha1 = [0u8; 20];
            sha1.copy_from_slice(&content[sha1_start..sha1_end]);
            children.push(sha1);
        }
        pos = sha1_end;
    }
    children
}

/// Extract `(name, child SHA-1)` pairs from a git tree object body.
///
/// Same wire layout as [`tree_child_sha1s`] (`<mode> SP <name> NUL <sha1>`), but
/// keeps the entry name — the repack uses it to compute git's name-hash so
/// same-named files cluster adjacently and delta against each other. `name` is
/// the raw bytes of the path component (git permits non-UTF-8 names).
pub fn tree_entries(content: &[u8]) -> Vec<(Vec<u8>, [u8; 20])> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < content.len() {
        // entry = "<mode> <name>\0<20 bytes>"; the name starts after the first SP.
        let nul = match content[pos..].iter().position(|&b| b == 0) {
            Some(n) => pos + n,
            None => break,
        };
        let sha1_start = nul + 1;
        let sha1_end = sha1_start + 20;
        if sha1_end > content.len() {
            break;
        }
        // The header up to NUL is "<mode> <name>"; split on the first space.
        let header = &content[pos..nul];
        let name = match header.iter().position(|&b| b == b' ') {
            Some(sp) => header[sp + 1..].to_vec(),
            None => header.to_vec(),
        };
        let mut sha1 = [0u8; 20];
        sha1.copy_from_slice(&content[sha1_start..sha1_end]);
        out.push((name, sha1));
        pos = sha1_end;
    }
    out
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
/// **Annotated tags** (type 4) are followed to the object they tag (`object
/// <sha1>`), so a commit reachable only via an annotated tag ref is correctly
/// marked reachable — GC must not sweep a tagged commit, and a clone must pack it.
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
            4 => tag_object_sha1(&content).into_iter().collect(), // annotated tag → tagged object
            _ => Vec::new(),                 // blob → leaf
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

/// Expand `set` to its closure under the delta-base relation: for every id stored
/// as a delta, transitively include its base.
///
/// A kept delta whose base is reclaimed becomes unreadable (the delta can no
/// longer be reconstructed), so any GC keep-set must be closed under this
/// relation before sweeping — objects are delta-encoded against bases chosen for
/// size, and a base need not itself be ref-reachable. Both the single-node
/// (`ledge-workspace`) and clustered (`ledge-cluster`) GC passes rely on this.
///
/// Header-only `delta_base_of` reads. Termination: each step either inserts a NEW
/// id (the set grows monotonically, bounded by the finite on-disk object
/// population) or stops; no id is enqueued onto the frontier twice.
pub async fn close_under_delta_bases(
    store: &DiskObjectStore,
    set: HashSet<ObjectId>,
) -> ledge_core::Result<HashSet<ObjectId>> {
    let mut keep = set;
    let mut frontier: Vec<ObjectId> = keep.iter().copied().collect();
    while let Some(id) = frontier.pop() {
        if let Some(base) = store.delta_base_of(id).await? {
            // `insert` returns true only the first time `base` is seen, so a base
            // is enqueued at most once even with diamond delta chains.
            if keep.insert(base) {
                frontier.push(base);
            }
        }
    }
    Ok(keep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DiskObjectStore;
    use bytes::Bytes;
    use ledge_core::ObjectId;
    use std::collections::HashSet;
    use tempfile::tempdir;

    /// A gitlink (mode 160000) names a commit in ANOTHER repository. The plain
    /// child walk returns it (the GC walker just fails to resolve it and moves
    /// on), but any caller that REQUIRES every child to be present — the push
    /// connectivity check — must not see it, or every repo with a submodule
    /// becomes unpushable.
    #[test]
    fn tree_child_sha1s_in_repo_skips_gitlinks() {
        let mut tree = Vec::new();
        tree.extend_from_slice(b"100644 file\0");
        tree.extend_from_slice(&[0x11u8; 20]);
        tree.extend_from_slice(b"160000 vendor/lib\0"); // submodule
        tree.extend_from_slice(&[0x22u8; 20]);
        tree.extend_from_slice(b"40000 dir\0");
        tree.extend_from_slice(&[0x33u8; 20]);

        assert_eq!(
            tree_child_sha1s(&tree),
            vec![[0x11u8; 20], [0x22u8; 20], [0x33u8; 20]],
            "the plain walk returns every entry"
        );
        assert_eq!(
            tree_child_sha1s_in_repo(&tree),
            vec![[0x11u8; 20], [0x33u8; 20]],
            "the in-repo walk drops the gitlink"
        );
    }

    #[test]
    fn tree_entries_parses_names_and_shas() {
        // Two entries: a blob "a.txt" and a subtree "dir".
        let mut tree = Vec::new();
        tree.extend_from_slice(b"100644 a.txt\0");
        tree.extend_from_slice(&[0x11u8; 20]);
        tree.extend_from_slice(b"40000 dir\0");
        tree.extend_from_slice(&[0x22u8; 20]);
        let entries = tree_entries(&tree);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"a.txt".to_vec(), [0x11u8; 20]));
        assert_eq!(entries[1], (b"dir".to_vec(), [0x22u8; 20]));
        // names line up with the sha-only parser
        assert_eq!(
            tree_child_sha1s(&tree),
            entries.iter().map(|(_, s)| *s).collect::<Vec<_>>()
        );
        // a truncated trailing entry is dropped, not panicked
        let mut torn = tree.clone();
        torn.extend_from_slice(b"100644 b.txt\0\x01\x02"); // sha cut short
        assert_eq!(tree_entries(&torn).len(), 2);
    }

    /// Git commit/tag headers are followed by author/tagger names and a message
    /// that may contain arbitrary (non-UTF-8) bytes. The reachability parsers must
    /// still extract the ASCII `tree`/`parent`/`object` header lines — a UTF-8
    /// requirement would silently drop them, so GC would delete the (reachable)
    /// tree/parents and a clone would ship an incomplete pack.
    #[test]
    fn header_parsers_tolerate_non_utf8_message_and_names() {
        let tree_hex = "00".repeat(20);
        let parent_hex = "11".repeat(20);
        let mut commit = Vec::new();
        commit.extend_from_slice(format!("tree {tree_hex}\n").as_bytes());
        commit.extend_from_slice(format!("parent {parent_hex}\n").as_bytes());
        // Non-UTF-8 bytes in the author/committer names (git permits them).
        commit.extend_from_slice(b"author N\xffme <n@x> 1 +0000\n");
        commit.extend_from_slice(b"committer N\xffme <n@x> 1 +0000\n\n");
        commit.extend_from_slice(b"message with raw bytes \xff\xfe and more\n");
        assert_eq!(
            commit_tree_sha1(&commit),
            Some([0x00u8; 20]),
            "tree must be found despite non-UTF-8 author/message"
        );
        assert_eq!(
            commit_parent_sha1s(&commit),
            vec![[0x11u8; 20]],
            "parent must be found despite non-UTF-8 author/message"
        );

        let obj_hex = "22".repeat(20);
        let mut tag = Vec::new();
        tag.extend_from_slice(format!("object {obj_hex}\n").as_bytes());
        tag.extend_from_slice(b"type commit\ntag v1\n");
        tag.extend_from_slice(b"tagger N\xffme <n@x> 1 +0000\n\n");
        tag.extend_from_slice(b"tag message \xff\n");
        assert_eq!(
            tag_object_sha1(&tag),
            Some([0x22u8; 20]),
            "tagged object must be found despite non-UTF-8 tagger/message"
        );
    }

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
    async fn reachable_from_annotated_tag_includes_tagged_commit_closure() {
        let (store, _dir) = make_store();
        let (commit_id, tree_id, blob_id) = build_graph(&store).await;
        let commit_sha1 = store.sha1_of(commit_id).await.unwrap();
        // An annotated tag object referencing the commit (as `refs/tags/v1` would).
        let tag_body = format!(
            "object {}\ntype commit\ntag v1\ntagger t <t@x> 0 +0000\n\nrelease\n",
            hex::encode(commit_sha1)
        );
        let tag_id = store
            .write_git_object(4, Bytes::from(tag_body.into_bytes()))
            .await
            .unwrap();

        // Rooting at the TAG must reach the commit + its tree + blob — otherwise GC
        // would sweep a commit held only by an annotated tag (data loss).
        let reachable = reachable_from(&store, [tag_id]).await.unwrap();
        for (id, what) in [
            (tag_id, "tag"),
            (commit_id, "commit"),
            (tree_id, "tree"),
            (blob_id, "blob"),
        ] {
            assert!(
                reachable.contains(&id),
                "closure of an annotated tag must include the {what}"
            );
        }
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
