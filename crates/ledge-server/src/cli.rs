//! Provisioning CLI (Phase 4d-1 spec §4.6): `ledge auth create-key/revoke-key/
//! list-keys`, operating directly on the `AuthStore` at the configured data dir
//! (no running server). The server (`ledge start`) is the default launch path;
//! `auth` is an additional subcommand that never starts a server.

use std::path::PathBuf;
use std::sync::Arc;

use ledge_core::HLC;

use crate::auth::principal::{PrincipalKind, Scopes};
use crate::auth::store::AuthStore;

/// `ledge auth <...>` subcommands.
#[derive(clap::Subcommand, Debug)]
pub enum AuthCommand {
    /// Mint a key; prints the full `ledge_<id>_<secret>` token ONCE.
    CreateKey {
        #[arg(long)]
        tenant: String,
        /// Comma list of read,write,admin (default: read,write).
        #[arg(long, default_value = "read,write")]
        scopes: String,
        #[arg(long, default_value = "user")]
        kind: String,
        /// Optional TTL in seconds.
        #[arg(long)]
        ttl_secs: Option<u64>,
    },
    /// Revoke a key by its key_id.
    RevokeKey { key_id: String },
    /// List key metadata (never secrets).
    ListKeys,
}

/// Parse a `read,write,admin` comma list into `Scopes`.
fn parse_scopes(s: &str) -> Scopes {
    let mut sc = Scopes::default();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        match part {
            "read" => sc.read = true,
            "write" => sc.write = true,
            "admin" => sc.admin = true,
            _ => {}
        }
    }
    sc
}

/// Map a `--kind` string to a [`PrincipalKind`] (anything but `service` ⇒ user).
fn parse_kind(s: &str) -> PrincipalKind {
    match s {
        "service" => PrincipalKind::Service,
        _ => PrincipalKind::User,
    }
}

/// Wall-clock ms (CLI is interactive; deterministic clock not required).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Run an auth subcommand against the store at `data_dir`. Returns an optional
/// line to print to stdout (the minted token for `create-key`, a confirmation
/// for `revoke-key`); `list-keys` prints directly and returns `None`.
///
/// Factored to take the `data_dir` (not a parsed `Cli`) so unit tests drive it
/// directly without spawning a process. The full secret is returned ONLY by
/// `create-key` and never logged anywhere else.
pub async fn run_auth(cmd: AuthCommand, data_dir: PathBuf) -> anyhow::Result<Option<String>> {
    let hlc = Arc::new(HLC::new());
    let store = AuthStore::open(data_dir, hlc)?;
    match cmd {
        AuthCommand::CreateKey {
            tenant,
            scopes,
            kind,
            ttl_secs,
        } => {
            let token = store
                .mint(
                    &tenant,
                    parse_kind(&kind),
                    parse_scopes(&scopes),
                    ttl_secs.map(std::time::Duration::from_secs),
                    now_ms(),
                )
                .await?;
            Ok(Some(token))
        }
        AuthCommand::RevokeKey { key_id } => {
            let existed = store.revoke(&key_id).await?;
            Ok(Some(format!("revoked={existed} key_id={key_id}")))
        }
        AuthCommand::ListKeys => {
            for r in store.list() {
                println!(
                    "key_id={} tenant={} kind={:?} read={} write={} admin={} created_at_ms={} expires_at_ms={:?} revoked={}",
                    r.key_id,
                    r.tenant_id,
                    r.kind,
                    r.read,
                    r.write,
                    r.admin,
                    r.created_at_ms,
                    r.expires_at_ms,
                    r.revoked
                );
            }
            Ok(None)
        }
    }
}

/// First-boot bootstrap (spec §4.4): if the store has no keys, record the
/// operator-supplied `bootstrap_admin_token` as a `root` admin key so a fresh
/// enabled cluster is reachable. Returns the recorded `key_id` on the boot that
/// performs the bootstrap, or `None` when the store is already non-empty
/// (idempotent — a restart never re-bootstraps or duplicates the key). The
/// plaintext token is never logged; only `BLAKE3(secret)` is persisted.
///
/// Factored here (rather than inline in `main`) so it is unit-testable without
/// spawning a server; `main.rs` performs the equivalent steps with config wiring
/// + metrics + compaction around it.
pub async fn bootstrap_admin_if_empty(
    store: &AuthStore,
    bootstrap_admin_token: &str,
    now_ms: u64,
) -> anyhow::Result<Option<String>> {
    if !store.list().is_empty() {
        return Ok(None);
    }
    let kid = store
        .put_token(
            bootstrap_admin_token,
            "root",
            PrincipalKind::User,
            Scopes::ALL,
            now_ms,
        )
        .await?;
    Ok(Some(kid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn create_key_mints_verifiable_token() {
        let dir = tempdir().unwrap();
        let dd = dir.path().to_path_buf();
        let token = run_auth(
            AuthCommand::CreateKey {
                tenant: "acme".into(),
                scopes: "read,write,admin".into(),
                kind: "user".into(),
                ttl_secs: None,
            },
            dd.clone(),
        )
        .await
        .unwrap()
        .expect("create-key prints a token");
        // Reopen the store and verify the printed token.
        let store = AuthStore::open(dd, Arc::new(HLC::new())).unwrap();
        let p = store
            .verify(&token, now_ms())
            .expect("minted token verifies");
        assert_eq!(p.tenant_id, "acme");
        assert!(p.scopes.is_admin());
    }

    #[tokio::test]
    async fn revoke_then_verify_fails() {
        let dir = tempdir().unwrap();
        let dd = dir.path().to_path_buf();
        let token = run_auth(
            AuthCommand::CreateKey {
                tenant: "acme".into(),
                scopes: "read".into(),
                kind: "user".into(),
                ttl_secs: None,
            },
            dd.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let (kid, _) = token
            .strip_prefix("ledge_")
            .unwrap()
            .split_once('_')
            .unwrap();
        run_auth(
            AuthCommand::RevokeKey {
                key_id: kid.to_string(),
            },
            dd.clone(),
        )
        .await
        .unwrap();
        let store = AuthStore::open(dd, Arc::new(HLC::new())).unwrap();
        assert!(
            store.verify(&token, now_ms()).is_none(),
            "revoked token fails verify"
        );
    }

    #[tokio::test]
    async fn bootstrap_records_admin_on_empty_then_is_idempotent() {
        let dir = tempdir().unwrap();
        let dd = dir.path().to_path_buf();
        // Operator generated this token out-of-band (mint elsewhere for a
        // well-formed token).
        let scratch = AuthStore::in_memory(Arc::new(HLC::new()));
        let token = scratch
            .mint("root", PrincipalKind::User, Scopes::ALL, None, 0)
            .await
            .unwrap();

        // First boot: empty store ⇒ records the admin key.
        let store = AuthStore::open(dd.clone(), Arc::new(HLC::new())).unwrap();
        let kid = bootstrap_admin_if_empty(&store, &token, 0)
            .await
            .unwrap()
            .expect("first boot records the bootstrap admin");
        let p = store
            .verify(&token, 0)
            .expect("bootstrapped admin verifies");
        assert_eq!(p.tenant_id, "root");
        assert!(p.scopes.is_admin());
        assert_eq!(store.list().len(), 1, "exactly one key after bootstrap");

        // Second boot (reopen, replaying the WAL): store non-empty ⇒ no-op,
        // returns None, and does NOT duplicate the key.
        let store2 = AuthStore::open(dd, Arc::new(HLC::new())).unwrap();
        let again = bootstrap_admin_if_empty(&store2, &token, 0).await.unwrap();
        assert!(again.is_none(), "second boot does not re-bootstrap");
        assert_eq!(store2.list().len(), 1, "no duplicate admin key on restart");
        // The original key_id is the one that survived.
        assert!(
            store2.list().iter().any(|r| r.key_id == kid),
            "the original bootstrap key survives the restart"
        );
    }

    #[tokio::test]
    async fn list_keys_shows_metadata_not_secret() {
        let dir = tempdir().unwrap();
        let dd = dir.path().to_path_buf();
        let token = run_auth(
            AuthCommand::CreateKey {
                tenant: "acme".into(),
                scopes: "read".into(),
                kind: "user".into(),
                ttl_secs: None,
            },
            dd.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let store = AuthStore::open(dd, Arc::new(HLC::new())).unwrap();
        let list = store.list();
        assert_eq!(list.len(), 1);
        // The full token's secret never appears in any record field.
        let secret = token
            .strip_prefix("ledge_")
            .unwrap()
            .split_once('_')
            .unwrap()
            .1;
        let dump = format!("{list:?}");
        assert!(
            !dump.contains(secret),
            "list output must not contain the secret"
        );
    }
}
