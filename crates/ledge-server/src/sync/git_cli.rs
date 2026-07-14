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
    LedgeError::Unavailable(format!(
        "git {ctx}: {}",
        String::from_utf8_lossy(stderr).trim()
    ))
}

/// Inject `auth` into an https URL as userinfo. Non-https (file/http) unchanged.
fn with_auth(url: &str, auth: Option<&str>) -> String {
    match auth {
        Some(a) if url.starts_with("https://") => {
            format!("https://{a}@{}", &url["https://".len()..])
        }
        _ => url.to_string(),
    }
}

/// Transports git is permitted to use for a caller-influenced URL. Excludes the
/// command-executing transports — `ext::` (runs an arbitrary shell command) above
/// all — so an upstream URL from an untrusted tenant (POST /sync/import) cannot
/// turn a clone into code execution. `GIT_ALLOW_PROTOCOL` is an explicit
/// allow-list: anything not named here is refused, and it overrides any ambient
/// `protocol.*.allow` config, so the policy does not depend on the git version or
/// the host's global gitconfig. `file` stays in so local-mirror clones and the
/// test suite keep working; cross-host/internal-file SSRF is gated separately by
/// the caller's `allowed_hosts`.
const GIT_ALLOWED_PROTOCOLS: &str = "file:git:http:https:ssh";

/// A `git` command pre-hardened for untrusted input: no terminal prompt (never
/// block on credentials) and the transport allow-list pinned. Every git
/// invocation in this module goes through here so neither guard can be forgotten.
fn git_command() -> Command {
    let mut c = Command::new("git");
    c.env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ALLOW_PROTOCOL", GIT_ALLOWED_PROTOCOLS);
    c
}

async fn run(args: &[&str], cwd: &Path, timeout: Duration, ctx: &str) -> Result<Vec<u8>> {
    let out = tokio::time::timeout(timeout, git_command().args(args).current_dir(cwd).output())
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
    // `--` terminates option parsing: a URL beginning with `-` reaches git as the
    // repository positional, never as a flag (argument injection).
    run(
        &[
            "clone",
            "--bare",
            "--quiet",
            "--",
            &full,
            dst.to_str().unwrap(),
        ],
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
        other => Err(LedgeError::Corruption(format!(
            "cat-file: unknown type {other}"
        ))),
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
        &[
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads",
            "refs/tags",
        ],
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
    let out = run(
        &["symbolic-ref", "--short", "HEAD"],
        repo,
        Duration::from_secs(10),
        "head",
    )
    .await
    .ok()?;
    let s = String::from_utf8_lossy(&out).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

use std::io::Write;

/// `git init --bare --initial-branch=main <dir>`.
///
/// HEAD is pinned to `refs/heads/main` (rather than git's build-time default,
/// historically `master`) so that a repo which subsequently receives a `main`
/// push has a HEAD that resolves: a later `git clone` follows the remote HEAD
/// and checks `main` out instead of warning about a nonexistent ref.
pub async fn init_bare(dir: &std::path::Path) -> Result<()> {
    run(
        &[
            "init",
            "--bare",
            "--quiet",
            "--initial-branch=main",
            dir.to_str().unwrap(),
        ],
        std::path::Path::new("/"),
        Duration::from_secs(30),
        "init",
    )
    .await
    .map(|_| ())
}

/// `git -C <repo> update-ref <refname> <sha1hex>`.
pub async fn update_ref(repo: &std::path::Path, refname: &str, sha1_hex: &str) -> Result<()> {
    run(
        &[
            "-C",
            repo.to_str().unwrap(),
            "update-ref",
            refname,
            sha1_hex,
        ],
        std::path::Path::new("/"),
        Duration::from_secs(30),
        "update-ref",
    )
    .await
    .map(|_| ())
}

/// Write a loose git object (zlib of "<type> <len>\0"+content) into <repo>/objects.
/// Content-addressed ⇒ idempotent if the object already exists.
pub fn write_loose_object(
    repo: &std::path::Path,
    sha1: &[u8; 20],
    git_type: u8,
    content: &[u8],
) -> Result<()> {
    let type_name = match git_type {
        1 => "commit",
        2 => "tree",
        3 => "blob",
        4 => "tag",
        other => return Err(LedgeError::Corruption(format!("loose: bad type {other}"))),
    };
    let mut framed = format!("{type_name} {}\0", content.len()).into_bytes();
    framed.extend_from_slice(content);
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&framed)
        .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
    let compressed = enc
        .finish()
        .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
    let hex: String = sha1.iter().map(|b| format!("{b:02x}")).collect();
    let dir = repo.join("objects").join(&hex[0..2]);
    std::fs::create_dir_all(&dir)
        .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
    let path = dir.join(&hex[2..]);
    if path.exists() {
        return Ok(());
    }
    std::fs::write(&path, compressed)
        .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PushRef {
    pub reference: String,
    pub status: String, // "ok" | "rejected"
    pub summary: String,
}

/// `git -C <repo> push [--force] --porcelain <url> <refspecs...>`, parsed per-ref.
/// A connection/auth failure (no porcelain ref lines + nonzero exit) ⇒ Err; a
/// per-ref rejection is reported in the returned Vec (status="rejected").
pub async fn push(
    repo: &std::path::Path,
    url: &str,
    auth: Option<&str>,
    refspecs: &[String],
    force: bool,
) -> Result<Vec<PushRef>> {
    let full = with_auth(url, auth);
    let mut args: Vec<String> = vec![
        "-C".into(),
        repo.to_str().unwrap().into(),
        "push".into(),
        "--porcelain".into(),
    ];
    if force {
        args.push("--force".into());
    }
    // `--` before the URL: the remote and refspecs are positionals, so a URL
    // beginning with `-` can never be parsed as a push option.
    args.push("--".into());
    args.push(full);
    args.extend(refspecs.iter().cloned());
    let argref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let out = tokio::time::timeout(
        Duration::from_secs(120),
        git_command().args(&argref).output(),
    )
    .await
    .map_err(|_| LedgeError::Unavailable("git push: timed out".into()))?
    .map_err(|e| LedgeError::Unavailable(format!("git push: spawn: {e}")))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let refs = parse_porcelain_push(&stdout);
    if refs.is_empty() && !out.status.success() {
        return Err(err("push", &out.stderr));
    }
    Ok(refs)
}

/// Parse `git push --porcelain` lines: "<flag>\t<src>:<dst>\t<summary>". The "To
/// <url>" header + "Done" trailer have no single-char flag field and are skipped.
fn parse_porcelain_push(stdout: &str) -> Vec<PushRef> {
    let mut out = Vec::new();
    for line in stdout.lines() {
        let mut it = line.splitn(3, '\t');
        let Some(flag_field) = it.next() else {
            continue;
        };
        let Some(refpair) = it.next() else { continue };
        let summary = it.next().unwrap_or("");
        // the flag field must be exactly one char.
        let mut chars = flag_field.chars();
        let Some(flag) = chars.next() else { continue };
        if chars.next().is_some() {
            continue;
        }
        let dst = refpair.split_once(':').map(|(_, d)| d).unwrap_or(refpair);
        let status = if flag == '!' { "rejected" } else { "ok" };
        out.push(PushRef {
            reference: dst.to_string(),
            status: status.to_string(),
            summary: summary.to_string(),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::process::Command as Cmd;

    async fn run(args: &[&str], cwd: &std::path::Path) {
        let out = Cmd::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
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
        run(
            &[
                "clone",
                "--bare",
                work.path().to_str().unwrap(),
                bare.path().to_str().unwrap(),
            ],
            std::path::Path::new("/"),
        )
        .await;
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
        assert!(
            objs.iter().any(|(_, t, _)| *t == 4),
            "has the annotated tag object"
        );
        // blob content round-trips
        assert!(
            objs.iter().any(|(_, t, c)| *t == 3 && c == b"hello\n"),
            "blob content matches"
        );
        let refs = for_each_ref(&dst).await.unwrap();
        assert!(
            refs.iter().any(|(n, _)| n == "refs/heads/main"),
            "main ref present"
        );
        assert!(
            refs.iter().any(|(n, _)| n == "refs/tags/v1"),
            "tag ref present"
        );
        assert_eq!(default_branch(&dst).await.as_deref(), Some("main"));
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }
    /// git blob sha1 = sha1("blob <len>\0" + content).
    fn git_blob_sha1(content: &[u8]) -> [u8; 20] {
        use sha1::{Digest, Sha1};
        let mut h = Sha1::new();
        h.update(format!("blob {}\0", content.len()).as_bytes());
        h.update(content);
        h.finalize().into()
    }

    #[tokio::test]
    async fn loose_object_then_git_reads_it() {
        let repo = tempfile::TempDir::new().unwrap();
        init_bare(repo.path()).await.unwrap();
        let content = b"hi\n";
        let sha = git_blob_sha1(content);
        write_loose_object(repo.path(), &sha, 3, content).unwrap();
        let out = Cmd::new("git")
            .args(["cat-file", "-p", &hex(&sha)])
            .current_dir(repo.path())
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "cat-file: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.stdout, content);
    }

    #[tokio::test]
    async fn push_reports_accept() {
        let upstream = tempfile::TempDir::new().unwrap();
        run(
            &["init", "--bare", upstream.path().to_str().unwrap()],
            std::path::Path::new("/"),
        )
        .await;
        let src = tempfile::TempDir::new().unwrap();
        run(&["init", "--initial-branch=main", "."], src.path()).await;
        run(&["config", "user.email", "t@l"], src.path()).await;
        run(&["config", "user.name", "t"], src.path()).await;
        std::fs::write(src.path().join("f"), b"x").unwrap();
        run(&["add", "f"], src.path()).await;
        run(&["commit", "-m", "c"], src.path()).await;
        let url = format!("file://{}", upstream.path().display());
        let res = push(
            src.path(),
            &url,
            None,
            &["refs/heads/main:refs/heads/main".to_string()],
            false,
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].reference, "refs/heads/main");
        assert_eq!(res[0].status, "ok", "summary={}", res[0].summary);
    }

    /// Git's `ext::` transport runs an arbitrary command. Modern git already
    /// refuses `ext` by default, but that default is a moving target across git
    /// versions and can be re-enabled by ambient `protocol.ext.allow` config, so
    /// clone_bare pins `GIT_ALLOW_PROTOCOL` and does not depend on it. This is the
    /// regression guard for that pin: the upstream URL comes from any
    /// authenticated tenant (POST /sync/import), so a permissive transport policy
    /// would be RCE on the server host.
    #[tokio::test]
    async fn clone_refuses_the_ext_transport() {
        let tmp = tempfile::TempDir::new().unwrap();
        let marker = tmp.path().join("touched");
        let dst = tmp.path().join("dst.git");
        // ext:: runs `sh -c "<script>"`; the script would `touch <marker>` if the
        // transport were permitted, whether or not the clone then succeeds.
        let ext_url = format!("ext::sh -c \"touch {}\"", marker.display());

        let res = clone_bare(&ext_url, None, &dst).await;

        assert!(
            !marker.exists(),
            "the ext:: transport executed a command — protocol pin is not holding"
        );
        assert!(res.is_err(), "a clone over a forbidden transport must fail");
    }

    /// A URL beginning with `-` must reach git as a positional argument, never as
    /// an option (argument injection). It is not exploitable through this exact
    /// call today — git needs a separate repository argument the attacker cannot
    /// supply — but the `--` separator makes that independent of the argv layout,
    /// so a later change (an added directory arg, a reused URL position) cannot
    /// reopen it. Regression guard for the `--`.
    #[tokio::test]
    async fn clone_treats_a_dashed_url_as_a_positional_not_an_option() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dst = tmp.path().join("dst.git");
        // `--upload-pack=<cmd>` is the classic injection option. With `--` in
        // place git treats this whole string as a (bogus) repository URL and
        // fails to resolve it, rather than parsing it as a known flag.
        let inject = "--upload-pack=touch /tmp/should-never-run".to_string();

        let res = clone_bare(&inject, None, &dst).await;
        assert!(res.is_err(), "a bogus dashed URL must fail cleanly");
        let msg = format!("{}", res.unwrap_err());
        assert!(
            !msg.contains("unknown option") && !msg.contains("usage:"),
            "the dashed URL was parsed as an option, not a positional: {msg}"
        );
    }
}
