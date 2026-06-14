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
    /// Allowed client public keys. EMPTY ⇒ accept any key (dev only).
    pub authorized: Arc<Vec<ssh_key::PublicKey>>,
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

/// Parse an `authorized_keys` file into public keys (one per non-comment line).
pub fn parse_authorized_keys(text: &str) -> Vec<ssh_key::PublicKey> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| ssh_key::PublicKey::from_openssh(l).ok())
        .collect()
}

/// Run the SSH listener until the process exits. `host_key` is the server identity.
pub async fn serve(
    ctx: SshCtx,
    addr: &str,
    host_key: ssh_key::PrivateKey,
) -> std::io::Result<()> {
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
        }
    }
}

struct SshHandler {
    ctx: SshCtx,
    channel: Option<Channel<Msg>>,
}

impl Handler for SshHandler {
    type Error = russh::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        key: &ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        // Empty allowlist ⇒ accept any (dev). Otherwise the key must be listed.
        if self.ctx.authorized.is_empty() || self.ctx.authorized.iter().any(|k| k == key) {
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

        tokio::spawn(async move {
            let mut stream = channel.into_stream();
            let code: u32 = match parsed {
                Some((GitService::Upload, path)) => {
                    let segment = segment_from_path(&path);
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
                    let segment = segment_from_path(&path);
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

/// Map an SSH repo path to a Ledge git segment. `'/ws/<id>'` → `workspaces/<id>/`;
/// anything else → "" (the root durable repo). Mirrors the HTTP path mapping.
fn segment_from_path(path: &str) -> String {
    let p = path.trim_start_matches('/');
    if let Some(rest) = p.strip_prefix("ws/") {
        let id = rest.split('/').next().unwrap_or("");
        if !id.is_empty() {
            return format!("workspaces/{id}/");
        }
    }
    String::new()
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
        assert_eq!(segment_from_path("/myrepo"), "");
        assert_eq!(segment_from_path("/ws/abc123"), "workspaces/abc123/");
        assert_eq!(segment_from_path("ws/abc123/extra"), "workspaces/abc123/");
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
