//! Thin wrappers over the `git` binary for upstream sync. All non-interactive
//! (GIT_TERMINAL_PROMPT=0). Credentials, when present, are injected into the URL
//! (https://<auth>@host/...) — NEVER logged.
use std::path::Path;
use std::time::Duration;

use ledge_core::{LedgeError, Result};
use tokio::process::Command;

/// One delta-expanded git object: (raw 20-byte SHA-1, git type byte
/// [1=commit, 2=tree, 3=blob, 4=tag], full content). The type byte mirrors
/// git's loose-object type ordering so downstream code can map directly to the
/// object store without restringifying the type name.
pub type GitObject = ([u8; 20], u8, Vec<u8>);

fn err(ctx: &str, stderr: &[u8]) -> LedgeError {
    LedgeError::Unavailable(format!("git {ctx}: {}", String::from_utf8_lossy(stderr).trim()))
}

/// Inject `auth` into an https URL as userinfo. Non-https (file/http) unchanged.
fn with_auth(url: &str, auth: Option<&str>) -> String {
    match auth {
        Some(a) if url.starts_with("https://") => format!("https://{a}@{}", &url["https://".len()..]),
        _ => url.to_string(),
    }
}

async fn run(args: &[&str], cwd: &Path, timeout: Duration, ctx: &str) -> Result<Vec<u8>> {
    let out = tokio::time::timeout(
        timeout,
        Command::new("git").args(args).current_dir(cwd).env("GIT_TERMINAL_PROMPT", "0").output(),
    )
    .await
    .map_err(|_| LedgeError::Unavailable(format!("git {ctx}: timed out")))?
    .map_err(|e| LedgeError::Unavailable(format!("git {ctx}: spawn failed: {e}")))?;
    if !out.status.success() {
        return Err(err(ctx, &out.stderr));
    }
    Ok(out.stdout)
}

/// `git clone --bare --quiet <url> <dst>` (dst created by git).
pub async fn clone_bare(url: &str, auth: Option<&str>, dst: &Path) -> Result<()> {
    let full = with_auth(url, auth);
    run(
        &["clone", "--bare", "--quiet", &full, dst.to_str().unwrap()],
        Path::new("/"),
        Duration::from_secs(120),
        "clone",
    )
    .await
    .map(|_| ())
}

/// All objects in `repo` (delta-EXPANDED) as (sha1, git_type_byte, content).
pub async fn cat_all_objects(repo: &Path) -> Result<Vec<GitObject>> {
    let stdout = run(
        &["cat-file", "--batch-all-objects", "--batch", "--buffer"],
        repo,
        Duration::from_secs(120),
        "cat-file",
    )
    .await?;
    parse_cat_file_batch(&stdout)
}

/// Parse the `git cat-file --batch` stream: "<sha1> <type> <size>\n<content>\n"
/// repeated. Content is binary + size-delimited — cursor by the header size.
fn parse_cat_file_batch(buf: &[u8]) -> Result<Vec<GitObject>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < buf.len() {
        let nl = buf[i..]
            .iter()
            .position(|&b| b == b'\n')
            .ok_or_else(|| LedgeError::Corruption("cat-file: truncated header".into()))?
            + i;
        let header = std::str::from_utf8(&buf[i..nl])
            .map_err(|_| LedgeError::Corruption("cat-file: bad header utf8".into()))?;
        let mut parts = header.split(' ');
        let sha_hex = parts.next().unwrap_or("");
        let type_name = parts.next().unwrap_or("");
        let size: usize = parts
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| LedgeError::Corruption(format!("cat-file: bad header '{header}'")))?;
        let sha = parse_sha1(sha_hex)?;
        let ty = type_byte(type_name)?;
        let content_start = nl + 1;
        let content_end = content_start + size;
        if content_end + 1 > buf.len() {
            return Err(LedgeError::Corruption("cat-file: truncated content".into()));
        }
        out.push((sha, ty, buf[content_start..content_end].to_vec()));
        i = content_end + 1; // skip the '\n' after content
    }
    Ok(out)
}

fn type_byte(name: &str) -> Result<u8> {
    match name {
        "commit" => Ok(1),
        "tree" => Ok(2),
        "blob" => Ok(3),
        "tag" => Ok(4),
        other => Err(LedgeError::Corruption(format!("cat-file: unknown type {other}"))),
    }
}

fn parse_sha1(hex: &str) -> Result<[u8; 20]> {
    if hex.len() != 40 {
        return Err(LedgeError::Corruption("cat-file: bad sha1 len".into()));
    }
    let mut b = [0u8; 20];
    for (i, slot) in b.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| LedgeError::Corruption("cat-file: bad sha1 hex".into()))?;
    }
    Ok(b)
}

/// `refs/heads/*` + `refs/tags/*` as (refname, sha1).
pub async fn for_each_ref(repo: &Path) -> Result<Vec<(String, [u8; 20])>> {
    let stdout = run(
        &["for-each-ref", "--format=%(refname) %(objectname)", "refs/heads", "refs/tags"],
        repo,
        Duration::from_secs(30),
        "for-each-ref",
    )
    .await?;
    let mut out = Vec::new();
    for line in String::from_utf8_lossy(&stdout).lines() {
        if let Some((name, sha)) = line.split_once(' ') {
            out.push((name.to_string(), parse_sha1(sha.trim())?));
        }
    }
    Ok(out)
}

/// Default branch (`git symbolic-ref --short HEAD`), or None.
pub async fn default_branch(repo: &Path) -> Option<String> {
    let out = run(&["symbolic-ref", "--short", "HEAD"], repo, Duration::from_secs(10), "head")
        .await
        .ok()?;
    let s = String::from_utf8_lossy(&out).trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::process::Command as Cmd;

    async fn run(args: &[&str], cwd: &std::path::Path) {
        let out = Cmd::new("git").args(args).current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0").output().await.unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    }

    /// A local bare repo with 1 commit on main + an annotated tag.
    ///
    /// The tag is ANNOTATED (`git tag -a`) so a type-4 tag object exists in the
    /// object database; a lightweight tag would point straight at the commit and
    /// emit zero tag objects, making the `*t == 4` assertion below non-deterministic.
    async fn make_upstream() -> tempfile::TempDir {
        let work = tempfile::TempDir::new().unwrap();
        run(&["init", "--initial-branch=main", "."], work.path()).await;
        run(&["config", "user.email", "t@l"], work.path()).await;
        run(&["config", "user.name", "t"], work.path()).await;
        std::fs::write(work.path().join("a.txt"), b"hello\n").unwrap();
        run(&["add", "a.txt"], work.path()).await;
        run(&["commit", "-m", "c1"], work.path()).await;
        run(&["tag", "-a", "v1", "-m", "v1"], work.path()).await;
        let bare = tempfile::TempDir::new().unwrap();
        run(&["clone", "--bare", work.path().to_str().unwrap(), bare.path().to_str().unwrap()],
            std::path::Path::new("/")).await;
        bare
    }

    #[tokio::test]
    async fn clone_then_cat_and_refs() {
        let bare = make_upstream().await;
        let dstdir = tempfile::TempDir::new().unwrap();
        let dst = dstdir.path().join("up.git");
        let url = format!("file://{}", bare.path().display());
        clone_bare(&url, None, &dst).await.unwrap();
        let objs = cat_all_objects(&dst).await.unwrap();
        assert!(objs.iter().any(|(_, t, _)| *t == 1), "has a commit object");
        assert!(objs.iter().any(|(_, t, _)| *t == 3), "has a blob object");
        assert!(objs.iter().any(|(_, t, _)| *t == 4), "has the annotated tag object");
        // blob content round-trips
        assert!(objs.iter().any(|(_, t, c)| *t == 3 && c == b"hello\n"), "blob content matches");
        let refs = for_each_ref(&dst).await.unwrap();
        assert!(refs.iter().any(|(n, _)| n == "refs/heads/main"), "main ref present");
        assert!(refs.iter().any(|(n, _)| n == "refs/tags/v1"), "tag ref present");
        assert_eq!(default_branch(&dst).await.as_deref(), Some("main"));
    }
}
