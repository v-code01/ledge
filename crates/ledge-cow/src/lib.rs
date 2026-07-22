//! Platform-native copy-on-write filesystem primitives (Phase 2d).
//!
//! Provides [`clone_file`] and [`clone_tree`]: thin, safe wrappers over the
//! [`reflink-copy`](https://docs.rs/reflink-copy) crate that issue the native
//! copy-on-write syscall for the host filesystem (APFS `clonefile(2)`, Linux
//! `FICLONE`/btrfs/XFS reflink, Windows ReFS) and transparently fall back to a
//! byte copy when the filesystem lacks CoW or the clone would cross devices.
//!
//! The win for Ledge is **instant, space-shared whole-repo snapshots**: a
//! large `data_dir` clones in O(metadata) time and O(0) extra disk until the
//! copies diverge. See `docs/.../ledge-phase2d-cow-design.md`.
//!
//! No `unsafe`: every native syscall is issued by `reflink-copy`.

use std::path::Path;

use ledge_core::{LedgeError, Result};

/// How a single-file clone was satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneMethod {
    /// The filesystem performed a copy-on-write reflink (no data copied).
    Reflink,
    /// The filesystem lacks CoW (or the clone crossed devices); the file's
    /// bytes were copied as a fallback.
    Copied,
}

/// Clone a single file from `src` to `dst`.
///
/// Attempts a CoW reflink via the native syscall; on a filesystem without CoW
/// support (or a cross-device clone) falls back to a plain byte copy. Returns
/// which path was taken.
///
/// # Errors
/// [`LedgeError::Io`] if `src` is missing, `dst` cannot be created/written, or
/// the underlying syscall fails for a reason other than missing CoW support.
pub fn clone_file(src: &Path, dst: &Path) -> Result<CloneMethod> {
    // `reflink_or_copy` returns `Ok(None)` on a successful reflink and
    // `Ok(Some(bytes_copied))` when it fell back to a byte copy.
    match reflink_copy::reflink_or_copy(src, dst) {
        Ok(None) => Ok(CloneMethod::Reflink),
        Ok(Some(_)) => Ok(CloneMethod::Copied),
        Err(e) => Err(LedgeError::Io(e)),
    }
}

/// Aggregate statistics from a recursive [`clone_tree`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloneStats {
    /// Regular files cloned.
    pub files: usize,
    /// Directories created in the mirror.
    pub dirs: usize,
    /// Files satisfied by a CoW reflink.
    pub reflinked: usize,
    /// Files that fell back to a byte copy.
    pub copied: usize,
    /// Total logical bytes of cloned files (sum of source file lengths).
    pub bytes: u64,
}

/// Recursively clone `src_dir` into `dst_dir`.
///
/// Recreates the directory structure, [`clone_file`]s every regular file, and
/// recreates symlinks as symlinks (the link target is copied verbatim, never
/// followed). `dst_dir` is created and **must not already exist** — clone_tree
/// refuses to clobber an existing destination.
///
/// # Errors
/// [`LedgeError::Io`] if `dst_dir` already exists, `src_dir` cannot be read, or
/// any per-entry clone/mkdir/symlink operation fails.
pub fn clone_tree(src_dir: &Path, dst_dir: &Path) -> Result<CloneStats> {
    if dst_dir.exists() {
        return Err(LedgeError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "clone_tree refuses to overwrite existing destination: {}",
                dst_dir.display()
            ),
        )));
    }
    // Refuse a destination inside the source tree. clone_tree creates `dst_dir`
    // then walks `src_dir`; if `dst_dir` is under `src_dir`, the walk re-enters
    // the destination it just created and clones it into itself, recursing
    // without bound (stack overflow / ENAMETOOLONG / disk fill). A snapshot into
    // the data dir is a plausible operator mistake, so we fail fast — before any
    // directory is created — rather than leave a deep partial tree behind.
    if dest_within_src(src_dir, dst_dir)? {
        return Err(LedgeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "clone_tree destination {} is inside the source tree {}",
                dst_dir.display(),
                src_dir.display()
            ),
        )));
    }
    let mut stats = CloneStats::default();
    // The destination root mirrors src_dir; create it then walk recursively.
    std::fs::create_dir_all(dst_dir).map_err(LedgeError::Io)?;
    stats.dirs += 1;
    clone_tree_into(src_dir, dst_dir, &mut stats)?;
    Ok(stats)
}

/// Is `dst` the same as, or nested inside, `src` — by real filesystem location
/// (symlinks resolved), not string prefix? `src` is resolved with
/// [`std::fs::canonicalize`]; `dst` does not exist yet, so its nearest existing
/// ancestor is canonicalized and the not-yet-created tail re-appended. Comparison
/// is component-wise via [`Path::starts_with`], so `/data-backups` is NOT inside
/// `/data`.
fn dest_within_src(src: &Path, dst: &Path) -> Result<bool> {
    let src_c = std::fs::canonicalize(src).map_err(LedgeError::Io)?;
    let dst_c = canonicalize_leaf(dst)?;
    Ok(dst_c.starts_with(&src_c))
}

/// Canonicalize a path whose leaf may not exist: resolve the deepest existing
/// ancestor (following symlinks there), then re-append the absent components.
/// Falls back to the path as-given if it has no existing ancestor at all.
fn canonicalize_leaf(p: &Path) -> Result<std::path::PathBuf> {
    let mut ancestor = p;
    let mut tail: Vec<&std::ffi::OsStr> = Vec::new();
    loop {
        if ancestor.exists() {
            let mut base = std::fs::canonicalize(ancestor).map_err(LedgeError::Io)?;
            base.extend(tail.iter().rev());
            return Ok(base);
        }
        match (ancestor.parent(), ancestor.file_name()) {
            (Some(parent), Some(name)) => {
                tail.push(name);
                ancestor = parent;
            }
            // Root, or a relative path with no existing prefix: nothing to resolve.
            _ => return Ok(p.to_path_buf()),
        }
    }
}

/// Walk one directory level, cloning entries into the corresponding `dst`
/// mirror directory (which already exists). Recurses for subdirectories.
fn clone_tree_into(src: &Path, dst: &Path, stats: &mut CloneStats) -> Result<()> {
    for entry in std::fs::read_dir(src).map_err(LedgeError::Io)? {
        let entry = entry.map_err(LedgeError::Io)?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        // `symlink_metadata` does not follow symlinks — we must classify the
        // link itself, not its target, to recreate links faithfully.
        let meta = std::fs::symlink_metadata(&src_path).map_err(LedgeError::Io)?;
        let ft = meta.file_type();
        if ft.is_symlink() {
            let target = std::fs::read_link(&src_path).map_err(LedgeError::Io)?;
            std::os::unix::fs::symlink(&target, &dst_path).map_err(LedgeError::Io)?;
        } else if ft.is_dir() {
            std::fs::create_dir(&dst_path).map_err(LedgeError::Io)?;
            stats.dirs += 1;
            clone_tree_into(&src_path, &dst_path, stats)?;
        } else if ft.is_file() {
            match clone_file(&src_path, &dst_path)? {
                CloneMethod::Reflink => stats.reflinked += 1,
                CloneMethod::Copied => stats.copied += 1,
            }
            stats.files += 1;
            stats.bytes += meta.len();
        }
        // Other node types (fifos, sockets, devices) are not part of a Ledge
        // data dir and are intentionally skipped.
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn clone_file_produces_independent_copy() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("dst.bin");
        let original = b"ledge-cow original contents";
        fs::write(&src, original).unwrap();

        clone_file(&src, &dst).unwrap();

        // Mutate the source after the clone; the destination must not observe it.
        fs::write(&src, b"mutated source contents that are longer").unwrap();

        let dst_bytes = fs::read(&dst).unwrap();
        assert_eq!(
            dst_bytes, original,
            "clone must be an independent copy frozen at clone time"
        );
    }

    #[test]
    fn clone_file_reports_method() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("dst.bin");
        let original = b"method observation payload";
        fs::write(&src, original).unwrap();

        // Correctness is asserted unconditionally; the method is observational
        // (APFS reflinks, a non-CoW CI fs copies — both are correct).
        let method = clone_file(&src, &dst).unwrap();
        eprintln!("clone_file_reports_method: observed {method:?}");

        let dst_bytes = fs::read(&dst).unwrap();
        assert_eq!(dst_bytes, original, "cloned bytes must match the source");
        assert!(matches!(method, CloneMethod::Reflink | CloneMethod::Copied));
    }

    #[test]
    fn clone_tree_replicates_nested_structure() {
        let root = TempDir::new().unwrap();
        let src = root.path().join("src");
        let dst = root.path().join("dst");

        // Build: src/top.txt, src/a/mid.txt, src/a/b/deep.txt + a symlink.
        fs::create_dir_all(src.join("a/b")).unwrap();
        fs::write(src.join("top.txt"), b"top").unwrap();
        fs::write(src.join("a/mid.txt"), b"middle file").unwrap();
        fs::write(src.join("a/b/deep.txt"), b"deeply nested").unwrap();
        // Relative symlink (target need not exist for the link to be recreated).
        std::os::unix::fs::symlink("top.txt", src.join("link-to-top")).unwrap();

        let stats = clone_tree(&src, &dst).unwrap();

        // 3 regular files; dirs = dst root + a + a/b = 3.
        assert_eq!(stats.files, 3, "expected 3 files, got {stats:?}");
        assert_eq!(stats.dirs, 3, "expected 3 dirs, got {stats:?}");
        assert_eq!(stats.reflinked + stats.copied, 3);
        assert_eq!(stats.bytes, (3 + 11 + 13) as u64);

        // Content identical at every level.
        assert_eq!(fs::read(dst.join("top.txt")).unwrap(), b"top");
        assert_eq!(fs::read(dst.join("a/mid.txt")).unwrap(), b"middle file");
        assert_eq!(
            fs::read(dst.join("a/b/deep.txt")).unwrap(),
            b"deeply nested"
        );

        // Symlink preserved as a symlink, with the same target.
        let link = dst.join("link-to-top");
        let lmeta = fs::symlink_metadata(&link).unwrap();
        assert!(lmeta.file_type().is_symlink(), "link must remain a symlink");
        assert_eq!(
            fs::read_link(&link).unwrap(),
            std::path::PathBuf::from("top.txt")
        );
    }

    /// A destination INSIDE the source tree must be refused. Otherwise the walk
    /// re-enters the freshly-created destination and clones it into itself,
    /// recursing without bound (stack overflow / disk fill / ENAMETOOLONG). A
    /// snapshot into a subdirectory of the data dir is a plausible operator
    /// mistake, so this is a real footgun that can crash the server.
    #[test]
    fn clone_tree_refuses_dest_inside_src() {
        let root = TempDir::new().unwrap();
        let src = root.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("f"), b"x").unwrap();
        let dst = src.join("snapshot"); // nested INSIDE src

        let err = clone_tree(&src, &dst).unwrap_err();
        assert!(
            matches!(err, LedgeError::Io(_)),
            "expected an Io refusal, got {err:?}"
        );
        // Crucially: it must refuse UP FRONT, never begin the runaway nesting.
        assert!(
            !dst.exists(),
            "clone_tree must not create the nested destination before refusing"
        );
    }

    /// The refusal is by real location, not string prefix: `/data-backups` is not
    /// inside `/data`, and a sibling destination clones normally.
    #[test]
    fn clone_tree_allows_sibling_dest_with_shared_prefix() {
        let root = TempDir::new().unwrap();
        let src = root.path().join("data");
        let dst = root.path().join("data-backups"); // shares the "data" prefix, NOT inside
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("f"), b"x").unwrap();

        let stats = clone_tree(&src, &dst).unwrap();
        assert_eq!(stats.files, 1);
        assert_eq!(fs::read(dst.join("f")).unwrap(), b"x");
    }

    #[test]
    fn clone_tree_refuses_existing_dest() {
        let root = TempDir::new().unwrap();
        let src = root.path().join("src");
        let dst = root.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("f"), b"x").unwrap();
        fs::create_dir_all(&dst).unwrap(); // pre-existing → must refuse

        let err = clone_tree(&src, &dst).unwrap_err();
        assert!(
            matches!(err, LedgeError::Io(_)),
            "expected Io error, got {err:?}"
        );
    }

    #[test]
    fn clone_tree_independence() {
        let root = TempDir::new().unwrap();
        let src = root.path().join("src");
        let dst = root.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        let original = b"frozen at clone time";
        fs::write(src.join("data.bin"), original).unwrap();

        clone_tree(&src, &dst).unwrap();

        // Mutate the source file post-clone; the cloned copy must not change.
        fs::write(src.join("data.bin"), b"mutated after the clone, longer now").unwrap();

        assert_eq!(
            fs::read(dst.join("data.bin")).unwrap(),
            original,
            "cloned tree must be independent of post-clone source mutations"
        );
    }
}
