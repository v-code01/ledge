//! SSH transport: a native embedded SSH server that serves `git-upload-pack`
//! (clone / fetch) and `git-receive-pack` (push) over the channel using the
//! interactive git protocol.
//!
//! Reuses the transport-agnostic [`ledge_git::fetch::upload_pack_stream`] and
//! [`ledge_git::push::receive_pack_stream`] — the channel is just an
//! `AsyncRead + AsyncWrite`. Default-off (`[ssh] enabled`).

use std::path::Path;
use std::sync::Arc;

use russh::keys::ssh_key;
use russh::server::{Auth, Config, Handler, Msg, Server, Session};
use russh::{Channel, ChannelId};

use crate::routes::AppState;

/// Shared SSH context handed to every connection.
#[derive(Clone)]
pub struct SshCtx {
    pub state: AppState,
    /// Allowed `(public key, tenant)` pairs. The tenant is the key's
    /// authorized_keys comment field ("" = root). EMPTY ⇒ accept any key as root
    /// (dev only).
    pub authorized: Arc<Vec<(ssh_key::PublicKey, String)>>,
}

/// Load the persistent host key, generating + persisting an Ed25519 key on first
/// boot. The seed comes from the OS RNG (rand 0.8), avoiding a keygen rng-version
/// dependency on ssh-key's rand_core.
pub fn load_or_create_host_key(path: &Path) -> anyhow::Result<ssh_key::PrivateKey> {
    if path.exists() {
        let pem = std::fs::read_to_string(path)?;
        return Ok(ssh_key::PrivateKey::from_openssh(&pem)?);
    }
    use rand::RngCore;
    let mut seed = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut seed);
    let kp = ssh_key::private::Ed25519Keypair::from_seed(&seed);
    let key = ssh_key::PrivateKey::from(kp);
    let pem = key.to_openssh(ssh_key::LineEnding::LF)?;
    std::fs::write(path, pem.as_bytes())?;
    Ok(key)
}

/// A fresh random public key that no client will ever present — used to force a
/// non-empty (fail-closed) allowlist when a configured `authorized_keys` file is
/// unreadable, so a misconfiguration rejects everyone rather than opening up.
pub fn unreachable_key() -> ssh_key::PublicKey {
    use rand::RngCore;
    let mut seed = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut seed);
    let kp = ssh_key::private::Ed25519Keypair::from_seed(&seed);
    ssh_key::PrivateKey::from(kp).public_key().clone()
}

/// Parse an `authorized_keys` file into `(public key, tenant)` pairs. The tenant
/// is the key's comment field — `ssh-ed25519 AAAA… acme` maps that key to tenant
/// `acme`; an empty/absent comment maps it to root (`""`).
pub fn parse_authorized_keys(text: &str) -> Vec<(ssh_key::PublicKey, String)> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            let pk = ssh_key::PublicKey::from_openssh(l).ok()?;
            let tenant = pk.comment().to_string();
            Some((pk, tenant))
        })
        .collect()
}

/// Run the SSH listener until the process exits. `host_key` is the server identity.
pub async fn serve(ctx: SshCtx, addr: &str, host_key: ssh_key::PrivateKey) -> std::io::Result<()> {
    let config = Arc::new(Config {
        keys: vec![host_key],
        inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
        auth_rejection_time: std::time::Duration::from_secs(1),
        ..Default::default()
    });
    let mut server = SshServer { ctx };
    server.run_on_address(config, addr).await
}

/// Like [`serve`] but on a pre-bound socket — lets a caller (e.g. tests) pick an
/// ephemeral port and learn it before serving.
pub async fn serve_on_socket(
    ctx: SshCtx,
    listener: tokio::net::TcpListener,
    host_key: ssh_key::PrivateKey,
) -> std::io::Result<()> {
    let config = Arc::new(Config {
        keys: vec![host_key],
        inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
        auth_rejection_time: std::time::Duration::from_secs(1),
        ..Default::default()
    });
    let mut server = SshServer { ctx };
    server.run_on_socket(config, &listener).await
}

struct SshServer {
    ctx: SshCtx,
}

impl Server for SshServer {
    type Handler = SshHandler;
    fn new_client(&mut self, _peer: Option<std::net::SocketAddr>) -> SshHandler {
        SshHandler {
            ctx: self.ctx.clone(),
            channel: None,
            tenant: String::new(),
        }
    }
}

struct SshHandler {
    ctx: SshCtx,
    channel: Option<Channel<Msg>>,
    /// The authenticated connection's tenant (from the matched key's comment;
    /// "" = root). Set on a successful `auth_publickey`.
    tenant: String,
}

impl Handler for SshHandler {
    type Error = russh::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        key: &ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        // Empty allowlist ⇒ accept any key as root (dev). Otherwise the key must
        // be listed; the connection acts as that key's tenant.
        if self.ctx.authorized.is_empty() {
            self.tenant = String::new();
            return Ok(Auth::Accept);
        }
        // Match on key MATERIAL only — the client offers the key without the
        // authorized_keys comment we use to carry the tenant, so comparing whole
        // `PublicKey`s (which include the comment) would never match.
        if let Some((_, tenant)) = self
            .ctx
            .authorized
            .iter()
            .find(|(k, _)| k.key_data() == key.key_data())
        {
            self.tenant = tenant.clone();
            Ok(Auth::Accept)
        } else {
            Ok(Auth::reject())
        }
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        self.channel = Some(channel);
        Ok(true)
    }

    async fn exec_request(
        &mut self,
        id: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let cmd = String::from_utf8_lossy(data).to_string();
        let Some(channel) = self.channel.take() else {
            let _ = session.channel_failure(id);
            return Ok(());
        };
        let parsed = parse_git_command(&cmd);
        session.channel_success(id)?;
        let handle = session.handle();
        let state = self.ctx.state.clone();
        let tenant = self.tenant.clone();

        tokio::spawn(async move {
            let mut stream = channel.into_stream();
            let code: u32 = match parsed {
                Some((GitService::Upload, path)) => {
                    let Some(segment) = resolve_segment(&state, &path, &tenant).await else {
                        tracing::warn!(%tenant, %path, "ssh: workspace not owned by tenant");
                        let _ = handle.exit_status_request(id, 1).await;
                        let _ = handle.eof(id).await;
                        let _ = handle.close(id).await;
                        return;
                    };
                    match ledge_git::fetch::upload_pack_stream(
                        &mut stream,
                        state.objects.clone(),
                        state.refs.clone(),
                        state.objects_disk.as_ref(),
                        &segment,
                    )
                    .await
                    {
                        Ok(()) => 0,
                        Err(e) => {
                            tracing::warn!(error = %e, "ssh upload-pack failed");
                            1
                        }
                    }
                }
                Some((GitService::Receive, path)) => {
                    let Some(segment) = resolve_segment(&state, &path, &tenant).await else {
                        tracing::warn!(%tenant, %path, "ssh: workspace not owned by tenant");
                        let _ = handle.exit_status_request(id, 1).await;
                        let _ = handle.eof(id).await;
                        let _ = handle.close(id).await;
                        return;
                    };
                    match ledge_git::push::receive_pack_stream(
                        &mut stream,
                        state.refs.clone(),
                        state.objects_disk.as_ref(),
                        &segment,
                    )
                    .await
                    {
                        Ok(()) => 0,
                        Err(e) => {
                            tracing::warn!(error = %e, "ssh receive-pack failed");
                            1
                        }
                    }
                }
                None => {
                    tracing::warn!(cmd = %cmd, "ssh: unsupported exec command");
                    1
                }
            };
            let _ = handle.exit_status_request(id, code).await;
            let _ = handle.eof(id).await;
            let _ = handle.close(id).await;
        });
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum GitService {
    Upload,
    Receive,
}

/// Parse `git-upload-pack '<path>'` / `git-receive-pack '<path>'` (the command an
/// SSH git client execs). Tolerates quotes and the `git upload-pack` spelling.
fn parse_git_command(cmd: &str) -> Option<(GitService, String)> {
    let cmd = cmd.trim();
    let (svc, rest) = if let Some(r) = cmd.strip_prefix("git-upload-pack") {
        (GitService::Upload, r)
    } else if let Some(r) = cmd.strip_prefix("git upload-pack") {
        (GitService::Upload, r)
    } else if let Some(r) = cmd.strip_prefix("git-receive-pack") {
        (GitService::Receive, r)
    } else if let Some(r) = cmd.strip_prefix("git receive-pack") {
        (GitService::Receive, r)
    } else {
        return None;
    };
    let path = rest.trim().trim_matches(['\'', '"']).to_string();
    Some((svc, path))
}

/// Map an SSH repo path + tenant to a Ledge git segment, returning the segment
/// and (for a workspace path) its id so the caller can gate ownership.
/// `'/ws/<id>'` → (`workspaces/<id>/`, Some(id)); anything else → the tenant's
/// durable namespace (`tenant_prefix`, `""` for root), with no workspace to gate.
fn path_to_segment(path: &str, tenant: &str) -> (String, Option<String>) {
    let p = path.trim_start_matches('/');
    if let Some(rest) = p.strip_prefix("ws/") {
        let id = rest.split('/').next().unwrap_or("").to_string();
        if !id.is_empty() {
            return (format!("workspaces/{id}/"), Some(id));
        }
    }
    (ledge_core::tenant_prefix(tenant), None)
}

/// Resolve the tenant-scoped git segment for an SSH command's path, or `None` if
/// the path is a workspace not owned by `tenant` (which must be rejected — no
/// cross-tenant SSH access).
async fn resolve_segment(state: &AppState, path: &str, tenant: &str) -> Option<String> {
    let (segment, ws) = path_to_segment(path, tenant);
    match ws {
        Some(id) if !workspace_owned(state, &id, tenant).await => None,
        _ => Some(segment),
    }
}

/// Whether workspace `id` is owned by `tenant` (root-normalized, mirroring the
/// HTTP `ws_tenant_ok` check). Unknown/foreign ⇒ false (no cross-tenant access).
async fn workspace_owned(state: &AppState, id: &str, tenant: &str) -> bool {
    let Ok(wid) = ledge_workspace::WorkspaceId::from_hex(id) else {
        return false;
    };
    let norm = |t: &str| if t.is_empty() { "root" } else { t }.to_string();
    match state.leases.get(wid).await {
        Ok(Some(lease)) => norm(&lease.tenant_id) == norm(tenant),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_commands() {
        assert_eq!(
            parse_git_command("git-upload-pack '/myrepo'"),
            Some((GitService::Upload, "/myrepo".into()))
        );
        assert_eq!(
            parse_git_command("git-receive-pack '/ws/abc'"),
            Some((GitService::Receive, "/ws/abc".into()))
        );
        assert_eq!(parse_git_command("scp -t /tmp"), None);
    }

    #[test]
    fn maps_paths_to_segments() {
        // Durable repo → the tenant's namespace; no workspace to gate.
        assert_eq!(path_to_segment("/myrepo", ""), (String::new(), None));
        assert_eq!(
            path_to_segment("/myrepo", "acme"),
            ("tenants/acme/".to_string(), None)
        );
        // Workspace path → workspaces/<id>/ + the id to gate by ownership.
        assert_eq!(
            path_to_segment("/ws/abc123", "acme"),
            ("workspaces/abc123/".to_string(), Some("abc123".to_string()))
        );
        assert_eq!(
            path_to_segment("ws/abc123/extra", ""),
            ("workspaces/abc123/".to_string(), Some("abc123".to_string()))
        );
    }

    #[test]
    fn authorized_keys_carry_tenant_from_comment() {
        let dir = tempfile::tempdir().unwrap();
        let pk = load_or_create_host_key(&dir.path().join("k")).unwrap();
        let mut with_tenant = pk.public_key().clone();
        with_tenant.set_comment("acme");
        let mut no_tenant = pk.public_key().clone();
        no_tenant.set_comment("");
        let text = format!(
            "{}\n# a comment line\n{}\n",
            with_tenant.to_openssh().unwrap(),
            no_tenant.to_openssh().unwrap()
        );
        let parsed = parse_authorized_keys(&text);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].1, "acme", "comment maps to tenant");
        assert_eq!(parsed[1].1, "", "no comment ⇒ root");
    }

    #[test]
    fn host_key_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("hostkey");
        let k1 = load_or_create_host_key(&p).unwrap();
        assert!(p.exists());
        let k2 = load_or_create_host_key(&p).unwrap(); // reloads the same key
        assert_eq!(
            k1.public_key().to_openssh().unwrap(),
            k2.public_key().to_openssh().unwrap()
        );
    }
}
