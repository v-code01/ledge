//! Adaptive Radix Tree (ART) — pure copy-on-write implementation.
//!
//! ## Key invariants
//! - Every public function is pure: callers receive a new root Arc; the old tree
//!   is never mutated. Unmodified subtrees are shared via Arc::clone — O(depth)
//!   allocations per insert/delete, not O(tree_size).
//! - key bytes are the raw UTF-8 bytes of a RefName string.
//! - Inner-node prefix compression: each inner node stores a common byte prefix
//!   shared by all keys in its subtree, allowing O(k) traversal where k is key
//!   length regardless of tree size.
//! - Node type upgrades are one-way and happen only when a new child cannot fit
//!   in the current node type (4 → 16 → 48 → 256).
//! - Node4/Node16 keep `keys` sorted to enable binary search on lookup.

use std::sync::Arc;
use ledge_core::RefEntry;

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

/// The four internal node types plus the leaf. Discriminated by `count` at
/// insertion time; the node type chosen is the smallest that fits all children.
///
/// Node48 is boxed to keep the enum size proportional to the common cases
/// (Leaf, Node4, Node16); clippy::large_enum_variant would fire otherwise.
#[derive(Clone)]
pub enum ArtNode {
    Leaf(LeafNode),
    Node4(Node4),
    Node16(Node16),
    Node48(Box<Node48>),
    Node256(Node256),
}

/// Terminal node. `key` is the full original key (not just the suffix), stored
/// so that prefix scans can reconstruct full key → entry pairs.
#[derive(Clone)]
pub struct LeafNode {
    pub key: Box<[u8]>,
    pub entry: RefEntry,
}

/// Holds up to 4 children. `keys[i]` is the byte discriminant for
/// `children[i]`. Kept sorted ascending by key byte for binary-search lookup.
#[derive(Clone)]
pub struct Node4 {
    pub prefix: Vec<u8>,
    pub count: u8, // 0..=4
    pub keys: [u8; 4],
    pub children: [Option<Arc<ArtNode>>; 4],
}

/// Holds up to 16 children. Same sorted-key layout as Node4.
#[derive(Clone)]
pub struct Node16 {
    pub prefix: Vec<u8>,
    pub count: u8, // 0..=16
    pub keys: [u8; 16],
    pub children: [Option<Arc<ArtNode>>; 16],
}

/// Holds up to 48 children. `key_index[byte]` maps a child byte → slot index
/// into `children[48]`. 0xFF means no child for that byte.
#[derive(Clone)]
pub struct Node48 {
    pub prefix: Vec<u8>,
    pub count: u8, // 0..=48
    pub key_index: [u8; 256], // 0xFF = absent
    pub children: [Option<Arc<ArtNode>>; 48],
}

/// Holds up to 256 children indexed directly by byte. Maximum density; never
/// upgrades further.
#[derive(Clone)]
pub struct Node256 {
    pub prefix: Vec<u8>,
    pub count: u16, // 0..=256
    pub children: Box<[Option<Arc<ArtNode>>; 256]>,
}

// ---------------------------------------------------------------------------
// Constructor helpers
// ---------------------------------------------------------------------------

impl Node4 {
    fn new(prefix: Vec<u8>) -> Self {
        Node4 {
            prefix,
            count: 0,
            keys: [0u8; 4],
            children: [None, None, None, None],
        }
    }
}

impl Node16 {
    fn new(prefix: Vec<u8>) -> Self {
        Node16 {
            prefix,
            count: 0,
            keys: [0u8; 16],
            children: std::array::from_fn(|_| None),
        }
    }
}

impl Node48 {
    fn new(prefix: Vec<u8>) -> Self {
        Node48 {
            prefix,
            count: 0,
            key_index: [0xFF; 256],
            children: std::array::from_fn(|_| None),
        }
    }
}

impl Node256 {
    fn new(prefix: Vec<u8>) -> Self {
        Node256 {
            prefix,
            count: 0,
            children: Box::new(std::array::from_fn(|_| None)),
        }
    }
}

// ---------------------------------------------------------------------------
// Prefix utilities
// ---------------------------------------------------------------------------

/// Returns the length of the common prefix between `a` and `b`.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

/// Pure lookup. Returns `Some(&RefEntry)` if `key` is present, `None` otherwise.
/// `depth` tracks how many bytes of `key` have been consumed by ancestors.
pub fn art_lookup<'a>(node: &'a ArtNode, key: &[u8], depth: usize) -> Option<&'a RefEntry> {
    match node {
        ArtNode::Leaf(leaf) => {
            if &*leaf.key == key {
                Some(&leaf.entry)
            } else {
                None
            }
        }
        ArtNode::Node4(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return None;
            }
            let after = depth + pfx.len();
            // Key exhausted at inner-node boundary: look for the null-byte child
            // which holds the entry for this exact prefix key.
            let byte = if after >= key.len() { 0x00 } else { key[after] };
            let next_depth = if after >= key.len() { after } else { after + 1 };
            for i in 0..n.count as usize {
                if n.keys[i] == byte {
                    if let Some(child) = &n.children[i] {
                        return art_lookup(child, key, next_depth);
                    }
                }
            }
            None
        }
        ArtNode::Node16(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return None;
            }
            let after = depth + pfx.len();
            let byte = if after >= key.len() { 0x00 } else { key[after] };
            let next_depth = if after >= key.len() { after } else { after + 1 };
            for i in 0..n.count as usize {
                if n.keys[i] == byte {
                    if let Some(child) = &n.children[i] {
                        return art_lookup(child, key, next_depth);
                    }
                }
            }
            None
        }
        ArtNode::Node48(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return None;
            }
            let after = depth + pfx.len();
            let byte = if after >= key.len() { 0x00 } else { key[after] };
            let next_depth = if after >= key.len() { after } else { after + 1 };
            let idx = n.key_index[byte as usize];
            if idx == 0xFF {
                return None;
            }
            if let Some(child) = &n.children[idx as usize] {
                art_lookup(child, key, next_depth)
            } else {
                None
            }
        }
        ArtNode::Node256(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return None;
            }
            let after = depth + pfx.len();
            let byte = if after >= key.len() { 0x00u8 } else { key[after] };
            let next_depth = if after >= key.len() { after } else { after + 1 };
            if let Some(child) = &n.children[byte as usize] {
                art_lookup(child, key, next_depth)
            } else {
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Insert helpers — node-level child setters (CoW: clone inner node, set child)
// ---------------------------------------------------------------------------

/// Insert a child into a Node4, returning the updated node (cloned).
/// Upgrades to Node16 if at capacity.
fn node4_with_child(n: &Node4, byte: u8, child: Arc<ArtNode>) -> Arc<ArtNode> {
    // Check if byte already exists (update in place within cloned node).
    for i in 0..n.count as usize {
        if n.keys[i] == byte {
            let mut new_n = n.clone();
            new_n.children[i] = Some(child);
            return Arc::new(ArtNode::Node4(new_n));
        }
    }
    if n.count < 4 {
        let mut new_n = n.clone();
        // Insertion into sorted position.
        let pos = (0..n.count as usize)
            .find(|&i| n.keys[i] > byte)
            .unwrap_or(n.count as usize);
        // Shift right to make room.
        for i in (pos..n.count as usize).rev() {
            new_n.keys[i + 1] = new_n.keys[i];
            new_n.children[i + 1] = new_n.children[i].clone();
        }
        new_n.keys[pos] = byte;
        new_n.children[pos] = Some(child);
        new_n.count += 1;
        Arc::new(ArtNode::Node4(new_n))
    } else {
        // Upgrade to Node16: copy all 4 existing entries then insert the new one.
        let mut new16 = Node16::new(n.prefix.clone());
        for i in 0..4 {
            new16.keys[i] = n.keys[i];
            new16.children[i] = n.children[i].clone();
        }
        new16.count = 4;
        node16_with_child(&new16, byte, child)
    }
}

fn node16_with_child(n: &Node16, byte: u8, child: Arc<ArtNode>) -> Arc<ArtNode> {
    for i in 0..n.count as usize {
        if n.keys[i] == byte {
            let mut new_n = n.clone();
            new_n.children[i] = Some(child);
            return Arc::new(ArtNode::Node16(new_n));
        }
    }
    if n.count < 16 {
        let mut new_n = n.clone();
        let pos = (0..n.count as usize)
            .find(|&i| n.keys[i] > byte)
            .unwrap_or(n.count as usize);
        for i in (pos..n.count as usize).rev() {
            new_n.keys[i + 1] = new_n.keys[i];
            new_n.children[i + 1] = new_n.children[i].clone();
        }
        new_n.keys[pos] = byte;
        new_n.children[pos] = Some(child);
        new_n.count += 1;
        Arc::new(ArtNode::Node16(new_n))
    } else {
        // Upgrade to Node48.
        let mut new48 = Node48::new(n.prefix.clone());
        for i in 0..16 {
            let k = n.keys[i] as usize;
            new48.key_index[k] = i as u8;
            new48.children[i] = n.children[i].clone();
        }
        new48.count = 16;
        node48_with_child(&new48, byte, child)
    }
}

fn node48_with_child(n: &Node48, byte: u8, child: Arc<ArtNode>) -> Arc<ArtNode> {
    let idx = n.key_index[byte as usize];
    if idx != 0xFF {
        // Update existing slot.
        let mut new_n = n.clone();
        new_n.children[idx as usize] = Some(child);
        return Arc::new(ArtNode::Node48(Box::new(new_n)));
    }
    if n.count < 48 {
        let mut new_n = n.clone();
        // Find a free slot in children[0..48].
        let slot = (0..48)
            .find(|&i| new_n.children[i].is_none())
            .expect("Node48 count/slot invariant violated");
        new_n.key_index[byte as usize] = slot as u8;
        new_n.children[slot] = Some(child);
        new_n.count += 1;
        Arc::new(ArtNode::Node48(Box::new(new_n)))
    } else {
        // Upgrade to Node256.
        let mut new256 = Node256::new(n.prefix.clone());
        for b in 0usize..256 {
            let slot = n.key_index[b];
            if slot != 0xFF {
                new256.children[b] = n.children[slot as usize].clone();
                new256.count += 1;
            }
        }
        node256_with_child(&new256, byte, child)
    }
}

fn node256_with_child(n: &Node256, byte: u8, child: Arc<ArtNode>) -> Arc<ArtNode> {
    let mut new_n = n.clone();
    if new_n.children[byte as usize].is_none() {
        new_n.count += 1;
    }
    new_n.children[byte as usize] = Some(child);
    Arc::new(ArtNode::Node256(new_n))
}

// ---------------------------------------------------------------------------
// Insert (CoW)
// ---------------------------------------------------------------------------

/// Pure insert. Returns a new root Arc reflecting the insertion of `(key, entry)`.
/// The old tree (if any) is not mutated — only the path from root to the new/
/// updated leaf is re-allocated; all unmodified subtrees are shared via Arc::clone.
pub fn art_insert(
    node: Option<Arc<ArtNode>>,
    key: &[u8],
    entry: RefEntry,
    depth: usize,
) -> Arc<ArtNode> {
    let node = match node {
        None => {
            // Empty tree: create a lone leaf.
            return Arc::new(ArtNode::Leaf(LeafNode {
                key: key.into(),
                entry,
            }));
        }
        Some(n) => n,
    };

    match node.as_ref() {
        ArtNode::Leaf(leaf) => {
            if &*leaf.key == key {
                // Overwrite existing leaf (CoW: new Arc with updated entry).
                return Arc::new(ArtNode::Leaf(LeafNode {
                    key: key.into(),
                    entry,
                }));
            }
            // Split: find common prefix between existing leaf key and new key.
            let existing_key = &leaf.key;
            let cp = common_prefix_len(&existing_key[depth..], &key[depth..]);
            let common: Vec<u8> = key[depth..depth + cp].to_vec();
            let mut new_node4 = Node4::new(common);

            let existing_leaf = Arc::clone(&node);
            let new_leaf = Arc::new(ArtNode::Leaf(LeafNode {
                key: key.into(),
                entry,
            }));

            // Handle the case where one key is a strict prefix of the other.
            // The shorter key is stored under the null-byte discriminant (0x00)
            // to match the convention used in inner-node traversal.
            let existing_exhausted = depth + cp >= existing_key.len();
            let new_exhausted = depth + cp >= key.len();

            if existing_exhausted {
                // existing_key is a prefix of new key — existing leaf goes under 0x00,
                // new leaf is rooted one level deeper under new_byte.
                let new_byte = key[depth + cp];
                let (p0, p1) = if 0x00u8 < new_byte { (0, 1) } else { (1, 0) };
                new_node4.keys[p0] = 0x00;
                new_node4.children[p0] = Some(existing_leaf);
                new_node4.keys[p1] = new_byte;
                new_node4.children[p1] = Some(new_leaf);
                new_node4.count = 2;
            } else if new_exhausted {
                // new key is a prefix of existing_key — new leaf goes under 0x00.
                let existing_byte = existing_key[depth + cp];
                let (p0, p1) = if 0x00u8 < existing_byte { (0, 1) } else { (1, 0) };
                new_node4.keys[p0] = 0x00;
                new_node4.children[p0] = Some(new_leaf);
                new_node4.keys[p1] = existing_byte;
                new_node4.children[p1] = Some(existing_leaf);
                new_node4.count = 2;
            } else {
                let existing_byte = existing_key[depth + cp];
                let new_byte = key[depth + cp];
                // Place into sorted order.
                let (pos_existing, pos_new) = if existing_byte < new_byte { (0, 1) } else { (1, 0) };
                new_node4.keys[pos_existing] = existing_byte;
                new_node4.children[pos_existing] = Some(existing_leaf);
                new_node4.keys[pos_new] = new_byte;
                new_node4.children[pos_new] = Some(new_leaf);
                new_node4.count = 2;
            }
            Arc::new(ArtNode::Node4(new_node4))
        }

        ArtNode::Node4(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            let cp = common_prefix_len(pfx, remaining);
            if cp < pfx.len() {
                // Compressed prefix mismatch: split the inner node.
                return split_inner_node4(n, key, entry, depth, cp);
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                // Key terminates exactly at node boundary: store as null-byte child.
                let leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));
                return node4_with_child(n, 0x00, leaf);
            }
            let byte = key[after];
            // Recurse into matching child or create new leaf.
            let existing_child = (0..n.count as usize)
                .find(|&i| n.keys[i] == byte)
                .and_then(|i| n.children[i].clone());
            let new_child = art_insert(existing_child, key, entry, after + 1);
            node4_with_child(n, byte, new_child)
        }

        ArtNode::Node16(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            let cp = common_prefix_len(pfx, remaining);
            if cp < pfx.len() {
                return split_inner_node16(n, key, entry, depth, cp);
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                let leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));
                return node16_with_child(n, 0x00, leaf);
            }
            let byte = key[after];
            let existing_child = (0..n.count as usize)
                .find(|&i| n.keys[i] == byte)
                .and_then(|i| n.children[i].clone());
            let new_child = art_insert(existing_child, key, entry, after + 1);
            node16_with_child(n, byte, new_child)
        }

        ArtNode::Node48(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            let cp = common_prefix_len(pfx, remaining);
            if cp < pfx.len() {
                return split_inner_node48(n, key, entry, depth, cp);
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                let leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));
                return node48_with_child(n, 0x00, leaf);
            }
            let byte = key[after];
            let idx = n.key_index[byte as usize];
            let existing_child = if idx != 0xFF {
                n.children[idx as usize].clone()
            } else {
                None
            };
            let new_child = art_insert(existing_child, key, entry, after + 1);
            node48_with_child(n, byte, new_child)
        }

        ArtNode::Node256(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            let cp = common_prefix_len(pfx, remaining);
            if cp < pfx.len() {
                return split_inner_node256(n, key, entry, depth, cp);
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                let leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));
                return node256_with_child(n, 0x00, leaf);
            }
            let byte = key[after];
            let existing_child = n.children[byte as usize].clone();
            let new_child = art_insert(existing_child, key, entry, after + 1);
            node256_with_child(n, byte, new_child)
        }
    }
}

// ---------------------------------------------------------------------------
// Split helpers — when compressed prefix partially matches and we must create
// a new intermediate node above the existing inner node.
// ---------------------------------------------------------------------------

/// Creates a new Node4 with prefix `key[depth..depth+cp]`. The old inner node
/// (trimmed prefix) and the new leaf become its two children, keyed on their
/// differing byte.
fn split_inner_node4(
    n: &Node4,
    key: &[u8],
    entry: RefEntry,
    depth: usize,
    cp: usize,
) -> Arc<ArtNode> {
    let new_prefix = n.prefix[..cp].to_vec();
    let old_byte = n.prefix[cp];
    let new_byte = key[depth + cp];

    let mut old_n = n.clone();
    old_n.prefix = n.prefix[cp + 1..].to_vec();
    let old_child = Arc::new(ArtNode::Node4(old_n));
    let new_leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));

    let mut split_node = Node4::new(new_prefix);
    let (p0, p1) = if old_byte < new_byte { (0, 1) } else { (1, 0) };
    split_node.keys[p0] = old_byte;
    split_node.children[p0] = Some(old_child);
    split_node.keys[p1] = new_byte;
    split_node.children[p1] = Some(new_leaf);
    split_node.count = 2;
    Arc::new(ArtNode::Node4(split_node))
}

fn split_inner_node16(
    n: &Node16,
    key: &[u8],
    entry: RefEntry,
    depth: usize,
    cp: usize,
) -> Arc<ArtNode> {
    let new_prefix = n.prefix[..cp].to_vec();
    let old_byte = n.prefix[cp];
    let new_byte = key[depth + cp];

    let mut old_n = n.clone();
    old_n.prefix = n.prefix[cp + 1..].to_vec();
    let old_child = Arc::new(ArtNode::Node16(old_n));
    let new_leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));

    let mut split_node = Node4::new(new_prefix);
    let (p0, p1) = if old_byte < new_byte { (0, 1) } else { (1, 0) };
    split_node.keys[p0] = old_byte;
    split_node.children[p0] = Some(old_child);
    split_node.keys[p1] = new_byte;
    split_node.children[p1] = Some(new_leaf);
    split_node.count = 2;
    Arc::new(ArtNode::Node4(split_node))
}

fn split_inner_node48(
    n: &Node48,
    key: &[u8],
    entry: RefEntry,
    depth: usize,
    cp: usize,
) -> Arc<ArtNode> {
    let new_prefix = n.prefix[..cp].to_vec();
    let old_byte = n.prefix[cp];
    let new_byte = key[depth + cp];

    let mut old_n = n.clone();
    old_n.prefix = n.prefix[cp + 1..].to_vec();
    let old_child = Arc::new(ArtNode::Node48(Box::new(old_n)));
    let new_leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));

    let mut split_node = Node4::new(new_prefix);
    let (p0, p1) = if old_byte < new_byte { (0, 1) } else { (1, 0) };
    split_node.keys[p0] = old_byte;
    split_node.children[p0] = Some(old_child);
    split_node.keys[p1] = new_byte;
    split_node.children[p1] = Some(new_leaf);
    split_node.count = 2;
    Arc::new(ArtNode::Node4(split_node))
}

fn split_inner_node256(
    n: &Node256,
    key: &[u8],
    entry: RefEntry,
    depth: usize,
    cp: usize,
) -> Arc<ArtNode> {
    let new_prefix = n.prefix[..cp].to_vec();
    let old_byte = n.prefix[cp];
    let new_byte = key[depth + cp];

    let mut old_n = n.clone();
    old_n.prefix = n.prefix[cp + 1..].to_vec();
    let old_child = Arc::new(ArtNode::Node256(old_n));
    let new_leaf = Arc::new(ArtNode::Leaf(LeafNode { key: key.into(), entry }));

    let mut split_node = Node4::new(new_prefix);
    let (p0, p1) = if old_byte < new_byte { (0, 1) } else { (1, 0) };
    split_node.keys[p0] = old_byte;
    split_node.children[p0] = Some(old_child);
    split_node.keys[p1] = new_byte;
    split_node.children[p1] = Some(new_leaf);
    split_node.count = 2;
    Arc::new(ArtNode::Node4(split_node))
}

// ---------------------------------------------------------------------------
// Delete (CoW)
// ---------------------------------------------------------------------------

/// Pure delete. Returns `None` if the tree is empty after the deletion.
/// Returns `Some(new_root)` otherwise, sharing all unmodified subtrees.
pub fn art_delete(node: Arc<ArtNode>, key: &[u8], depth: usize) -> Option<Arc<ArtNode>> {
    match node.as_ref() {
        ArtNode::Leaf(leaf) => {
            if &*leaf.key == key {
                None // Deleted — subtree is now empty.
            } else {
                Some(node) // Not our key; return unchanged.
            }
        }
        ArtNode::Node4(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return Some(node); // Key not in this subtree.
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                return Some(node);
            }
            let byte = key[after];
            let pos = (0..n.count as usize).find(|&i| n.keys[i] == byte)?;
            let child = n.children[pos].as_ref()?;
            let new_child = art_delete(Arc::clone(child), key, after + 1);
            let mut new_n = n.clone();
            if let Some(nc) = new_child {
                new_n.children[pos] = Some(nc);
                Some(Arc::new(ArtNode::Node4(new_n)))
            } else {
                // Remove the slot by shifting remaining entries left.
                for i in pos..new_n.count as usize - 1 {
                    new_n.keys[i] = new_n.keys[i + 1];
                    new_n.children[i] = new_n.children[i + 1].clone();
                }
                let last = new_n.count as usize - 1;
                new_n.keys[last] = 0;
                new_n.children[last] = None;
                new_n.count -= 1;
                if new_n.count == 0 {
                    None
                } else {
                    Some(Arc::new(ArtNode::Node4(new_n)))
                }
            }
        }
        ArtNode::Node16(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return Some(node);
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                return Some(node);
            }
            let byte = key[after];
            let pos = (0..n.count as usize).find(|&i| n.keys[i] == byte)?;
            let child = n.children[pos].as_ref()?;
            let new_child = art_delete(Arc::clone(child), key, after + 1);
            let mut new_n = n.clone();
            if let Some(nc) = new_child {
                new_n.children[pos] = Some(nc);
                Some(Arc::new(ArtNode::Node16(new_n)))
            } else {
                for i in pos..new_n.count as usize - 1 {
                    new_n.keys[i] = new_n.keys[i + 1];
                    new_n.children[i] = new_n.children[i + 1].clone();
                }
                let last = new_n.count as usize - 1;
                new_n.keys[last] = 0;
                new_n.children[last] = None;
                new_n.count -= 1;
                if new_n.count == 0 {
                    None
                } else {
                    Some(Arc::new(ArtNode::Node16(new_n)))
                }
            }
        }
        ArtNode::Node48(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return Some(node);
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                return Some(node);
            }
            let byte = key[after];
            let idx = n.key_index[byte as usize];
            if idx == 0xFF {
                return Some(node);
            }
            let child = n.children[idx as usize].as_ref()?;
            let new_child = art_delete(Arc::clone(child), key, after + 1);
            // n is &Box<Node48>; clone the inner Node48 directly.
            let mut new_n: Node48 = (**n).clone();
            if let Some(nc) = new_child {
                new_n.children[idx as usize] = Some(nc);
                Some(Arc::new(ArtNode::Node48(Box::new(new_n))))
            } else {
                new_n.key_index[byte as usize] = 0xFF;
                new_n.children[idx as usize] = None;
                new_n.count -= 1;
                if new_n.count == 0 {
                    None
                } else {
                    Some(Arc::new(ArtNode::Node48(Box::new(new_n))))
                }
            }
        }
        ArtNode::Node256(n) => {
            let remaining = &key[depth..];
            let pfx = &n.prefix;
            if remaining.len() < pfx.len() || &remaining[..pfx.len()] != pfx.as_slice() {
                return Some(node);
            }
            let after = depth + pfx.len();
            if after >= key.len() {
                return Some(node);
            }
            let byte = key[after] as usize;
            let child = n.children[byte].as_ref()?;
            let new_child = art_delete(Arc::clone(child), key, after + 1);
            let mut new_n = n.clone();
            if let Some(nc) = new_child {
                new_n.children[byte] = Some(nc);
                Some(Arc::new(ArtNode::Node256(new_n)))
            } else {
                new_n.children[byte] = None;
                new_n.count -= 1;
                if new_n.count == 0 {
                    None
                } else {
                    Some(Arc::new(ArtNode::Node256(new_n)))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Prefix iteration
// ---------------------------------------------------------------------------

/// Returns all (key, entry) pairs whose keys start with `prefix`.
/// `depth` is the number of bytes already consumed by ancestor nodes.
/// Results are not guaranteed to be in sorted order; callers sort if needed.
pub fn art_prefix_iter(node: &ArtNode, prefix: &[u8], depth: usize) -> Vec<(Box<[u8]>, RefEntry)> {
    let mut results = Vec::new();
    collect_prefix(node, prefix, depth, &mut results);
    results
}

fn collect_prefix(
    node: &ArtNode,
    prefix: &[u8],
    depth: usize,
    out: &mut Vec<(Box<[u8]>, RefEntry)>,
) {
    match node {
        ArtNode::Leaf(leaf) => {
            if leaf.key.starts_with(prefix) {
                out.push((leaf.key.clone(), leaf.entry.clone()));
            }
        }
        ArtNode::Node4(n) => {
            if !node_prefix_compatible(&n.prefix, prefix, depth) {
                return;
            }
            let next_depth = depth + n.prefix.len();
            for i in 0..n.count as usize {
                if let Some(child) = &n.children[i] {
                    collect_prefix(child, prefix, next_depth + 1, out);
                }
            }
        }
        ArtNode::Node16(n) => {
            if !node_prefix_compatible(&n.prefix, prefix, depth) {
                return;
            }
            let next_depth = depth + n.prefix.len();
            for i in 0..n.count as usize {
                if let Some(child) = &n.children[i] {
                    collect_prefix(child, prefix, next_depth + 1, out);
                }
            }
        }
        ArtNode::Node48(n) => {
            if !node_prefix_compatible(&n.prefix, prefix, depth) {
                return;
            }
            let next_depth = depth + n.prefix.len();
            for b in 0usize..256 {
                let idx = n.key_index[b];
                if idx != 0xFF {
                    if let Some(child) = &n.children[idx as usize] {
                        collect_prefix(child, prefix, next_depth + 1, out);
                    }
                }
            }
        }
        ArtNode::Node256(n) => {
            if !node_prefix_compatible(&n.prefix, prefix, depth) {
                return;
            }
            let next_depth = depth + n.prefix.len();
            for b in 0usize..256 {
                if let Some(child) = &n.children[b] {
                    collect_prefix(child, prefix, next_depth + 1, out);
                }
            }
        }
    }
}

/// Returns true if this node's subtree can possibly contain keys matching `prefix`.
///
/// A node is compatible if:
/// - `depth >= prefix.len()` (we are already past the prefix — all leaves match), OR
/// - The bytes consumed so far (node prefix bytes + depth) are all consistent with prefix.
fn node_prefix_compatible(node_prefix: &[u8], search_prefix: &[u8], depth: usize) -> bool {
    if depth >= search_prefix.len() {
        return true; // Already fully inside the matching subtree.
    }
    // Check how many bytes of `node_prefix` overlap with the remaining search prefix.
    let remaining_prefix = &search_prefix[depth..];
    let overlap = node_prefix.len().min(remaining_prefix.len());
    node_prefix[..overlap] == remaining_prefix[..overlap]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use ledge_core::{ObjectId, RefEntry};

    fn make_entry(byte: u8, version: u64) -> RefEntry {
        RefEntry { target: ObjectId::from_bytes([byte; 32]), hlc: version, version }
    }

    #[test]
    fn insert_and_lookup_single() {
        let key = b"refs/heads/main";
        let entry = make_entry(1, 1);
        let root = art_insert(None, key, entry.clone(), 0);
        assert_eq!(art_lookup(&root, key, 0), Some(&entry));
    }

    #[test]
    fn lookup_missing_returns_none() {
        let root = art_insert(None, b"refs/heads/main", make_entry(2, 1), 0);
        assert_eq!(art_lookup(&root, b"refs/heads/other", 0), None);
    }

    #[test]
    fn insert_multiple_lookup_each() {
        let keys: &[&[u8]] = &[b"refs/heads/main", b"refs/heads/feature", b"refs/tags/v1.0", b"refs/agents/a1"];
        let mut root = None;
        for (i, k) in keys.iter().enumerate() {
            root = Some(art_insert(root, k, make_entry(i as u8, i as u64 + 1), 0));
        }
        let root = root.unwrap();
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(art_lookup(&root, k, 0), Some(&make_entry(i as u8, i as u64 + 1)));
        }
    }

    #[test]
    fn cow_old_root_unchanged() {
        let key = b"refs/heads/main";
        let e1 = make_entry(1, 1);
        let e2 = make_entry(2, 2);
        let root1 = art_insert(None, key, e1.clone(), 0);
        let root2 = art_insert(Some(Arc::clone(&root1)), key, e2.clone(), 0);
        assert_eq!(art_lookup(&root1, key, 0), Some(&e1));
        assert_eq!(art_lookup(&root2, key, 0), Some(&e2));
    }

    #[test]
    fn delete_sole_leaf_returns_none() {
        let root = art_insert(None, b"refs/heads/main", make_entry(3, 1), 0);
        assert!(art_delete(root, b"refs/heads/main", 0).is_none());
    }

    #[test]
    fn delete_one_of_many() {
        let keys: &[&[u8]] = &[b"refs/heads/a", b"refs/heads/b", b"refs/heads/c"];
        let mut root = None;
        for (i, k) in keys.iter().enumerate() {
            root = Some(art_insert(root, k, make_entry(i as u8, i as u64 + 1), 0));
        }
        let root = art_delete(root.unwrap(), b"refs/heads/b", 0).unwrap();
        assert!(art_lookup(&root, b"refs/heads/b", 0).is_none());
        assert!(art_lookup(&root, b"refs/heads/a", 0).is_some());
        assert!(art_lookup(&root, b"refs/heads/c", 0).is_some());
    }

    #[test]
    fn prefix_iter_subtree() {
        let keys: &[&[u8]] = &[b"refs/heads/main", b"refs/heads/dev", b"refs/tags/v1.0", b"refs/agents/a1"];
        let mut root = None;
        for (i, k) in keys.iter().enumerate() {
            root = Some(art_insert(root, k, make_entry(i as u8, i as u64 + 1), 0));
        }
        let results = art_prefix_iter(&root.unwrap(), b"refs/heads/", 0);
        assert_eq!(results.len(), 2);
        let mut found: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.to_vec()).collect();
        found.sort();
        assert_eq!(found[0], b"refs/heads/dev".to_vec());
        assert_eq!(found[1], b"refs/heads/main".to_vec());
    }

    #[test]
    fn node4_to_node16_upgrade() {
        let mut root = None;
        for c in b'a'..=b'e' {
            let key = [b'r', b'e', b'f', b's', b'/', c];
            root = Some(art_insert(root, &key, make_entry(c, c as u64), 0));
        }
        let root = root.unwrap();
        for c in b'a'..=b'e' {
            assert!(art_lookup(&root, &[b'r', b'e', b'f', b's', b'/', c], 0).is_some());
        }
    }

    #[test]
    fn node16_to_node48_upgrade() {
        let mut root = None;
        for c in 0u8..17 {
            root = Some(art_insert(root, &[b'r', b'e', b'f', b's', b'/', c], make_entry(c, c as u64), 0));
        }
        let root = root.unwrap();
        for c in 0u8..17 {
            assert!(art_lookup(&root, &[b'r', b'e', b'f', b's', b'/', c], 0).is_some());
        }
    }

    #[test]
    fn node48_to_node256_upgrade() {
        let mut root = None;
        for c in 0u8..49 {
            root = Some(art_insert(root, &[b'r', b'e', b'f', b's', b'/', c], make_entry(c, c as u64), 0));
        }
        let root = root.unwrap();
        for c in 0u8..49 {
            assert!(art_lookup(&root, &[b'r', b'e', b'f', b's', b'/', c], 0).is_some());
        }
    }

    #[test]
    fn prefix_iter_empty_prefix_returns_all() {
        let mut root = None;
        for (i, k) in [b"refs/heads/main" as &[u8], b"refs/tags/v1"].iter().enumerate() {
            root = Some(art_insert(root, k, make_entry(i as u8, i as u64 + 1), 0));
        }
        assert_eq!(art_prefix_iter(&root.unwrap(), b"", 0).len(), 2);
    }
}
