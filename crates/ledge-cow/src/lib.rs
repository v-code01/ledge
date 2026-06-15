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
    let mut stats = CloneStats::default();
    // The destination root mirrors src_dir; create it then walk recursively.
    std::fs::create_dir_all(dst_dir).map_err(LedgeError::Io)?;
    stats.dirs += 1;
    clone_tree_into(src_dir, dst_dir, &mut stats)?;
    Ok(stats)
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
