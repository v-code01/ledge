//! TLS / mTLS transport assembly (Phase 4d-4). Server-only: PEM loading and
//! rustls Server/Client config construction. The crypto provider is aws-lc-rs
//! (the same backend reqwest's `rustls-tls` uses) so only one crypto crate links.

use std::fs;
use std::sync::Arc;

use ledge_core::{LedgeError, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

fn io_err(path: &str, what: &str) -> LedgeError {
    LedgeError::Io(std::io::Error::other(format!("tls: {what} at {path}")))
}

/// Load a PEM cert chain (leaf + any intermediates). Errors if the file is
/// missing/unreadable or contains zero certificates.
fn load_cert_chain(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let pem = fs::read(path).map_err(|e| io_err(path, &format!("read cert ({e})")))?;
    let chain: std::result::Result<Vec<_>, _> = rustls_pemfile::certs(&mut pem.as_slice()).collect();
    let chain = chain.map_err(|e| io_err(path, &format!("parse cert ({e})")))?;
    if chain.is_empty() {
        return Err(io_err(path, "no certificates in PEM"));
    }
    Ok(chain)
}

/// Load the FIRST PEM private key (PKCS#8 / PKCS#1 / SEC1).
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let pem = fs::read(path).map_err(|e| io_err(path, &format!("read key ({e})")))?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|e| io_err(path, &format!("parse key ({e})")))?
        .ok_or_else(|| io_err(path, "no private key in PEM"))
}

/// Build a RootCertStore from a PEM CA bundle (>=1 cert).
pub(crate) fn load_ca_roots(path: &str) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let mut n = 0usize;
    for c in load_cert_chain(path)? {
        roots.add(c).map_err(|e| io_err(path, &format!("add CA cert ({e})")))?;
        n += 1;
    }
    if n == 0 {
        return Err(io_err(path, "no CA certificates"));
    }
    Ok(roots)
}

/// Server config for the CLIENT listener: server cert+key, NO client-cert auth.
pub fn server_config_tls_only(cert: &str, key: &str) -> Result<Arc<ServerConfig>> {
    // Load PEM material BEFORE touching the rustls builder: the builder resolves
    // the process crypto provider eagerly (panics if none installed), so the
    // fallible file I/O must fail first to surface a clean error on bad paths.
    let chain = load_cert_chain(cert)?;
    let pk = load_private_key(key)?;
    let cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, pk)
        .map_err(|e| io_err(cert, &format!("server config ({e})")))?;
    Ok(Arc::new(cfg))
}

/// Server config for the PEER listener: server cert+key + REQUIRE a client cert
/// chaining to `ca`. Rejects no-cert and wrong-CA client certs at the handshake.
pub fn server_config_mtls(cert: &str, key: &str, ca: &str) -> Result<Arc<ServerConfig>> {
    // Load all PEM material before the builder (see `server_config_tls_only`).
    let chain = load_cert_chain(cert)?;
    let pk = load_private_key(key)?;
    let roots = Arc::new(load_ca_roots(ca)?);
    let verifier = WebPkiClientVerifier::builder(roots)
        .build()
        .map_err(|e| io_err(ca, &format!("client verifier ({e})")))?;
    let cfg = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, pk)
        .map_err(|e| io_err(cert, &format!("mtls server config ({e})")))?;
    Ok(Arc::new(cfg))
}

/// Outbound cluster client config: root store = ONLY the configured `ca` (built-in
/// roots are NOT added, so an unknown peer cert is rejected); plus a client
/// identity (cert+key) when `client_id = Some` (mTLS). Hand to reqwest via
/// `ClientBuilder::use_preconfigured_tls`.
pub fn client_config(ca: &str, client_id: Option<(&str, &str)>) -> Result<ClientConfig> {
    // Load all PEM material before the builder (see `server_config_tls_only`).
    let roots = load_ca_roots(ca)?;
    let identity = match client_id {
        Some((cert, key)) => Some((load_cert_chain(cert)?, load_private_key(key)?, cert.to_owned())),
        None => None,
    };
    let builder = ClientConfig::builder().with_root_certificates(roots);
    match identity {
        Some((chain, pk, cert)) => builder
            .with_client_auth_cert(chain, pk)
            .map_err(|e| io_err(&cert, &format!("client identity ({e})"))),
        None => Ok(builder.with_no_client_auth()),
    }
}

/// Install the process-default rustls crypto provider (aws-lc-rs). Idempotent:
/// a second call — or one after reqwest already installed it — is a harmless
/// no-op (the `Err` from a double install is ignored). Call once at boot,
/// unconditionally (cheap; harmless when TLS is off).
pub fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Test-only cert-minting helper shared across modules (Task 5 reuses `mint`).
#[cfg(test)]
pub(crate) mod tests_support {
    /// Mint a self-signed CA plus a server identity (SANs: localhost, 127.0.0.1)
    /// and a client identity (SAN: ledge-node), all signed by the CA. Returns
    /// `(ca_pem, server_cert_pem, server_key_pem, client_cert_pem, client_key_pem)`.
    pub fn mint() -> (String, String, String, String, String) {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let server_params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
        let server_cert = server_params.signed_by(&server_key, &ca_cert, &ca_key).unwrap();
        let client_key = KeyPair::generate().unwrap();
        let client_params = CertificateParams::new(vec!["ledge-node".to_string()]).unwrap();
        let client_cert = client_params.signed_by(&client_key, &ca_cert, &ca_key).unwrap();
        (
            ca_cert.pem(),
            server_cert.pem(),
            server_key.serialize_pem(),
            client_cert.pem(),
            client_key.serialize_pem(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mint() -> (String, String, String, String, String) {
        tests_support::mint()
    }
    fn write(dir: &tempfile::TempDir, name: &str, body: &str) -> String {
        let p = dir.path().join(name);
        std::fs::write(&p, body).unwrap();
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn server_config_tls_only_loads_valid_pem() {
        install_crypto_provider();
        let d = tempfile::TempDir::new().unwrap();
        let (_, sc, sk, _, _) = mint();
        let cert = write(&d, "s.crt", &sc);
        let key = write(&d, "s.key", &sk);
        assert!(server_config_tls_only(&cert, &key).is_ok());
    }
    #[test]
    fn server_config_mtls_loads_valid_pem() {
        install_crypto_provider();
        let d = tempfile::TempDir::new().unwrap();
        let (ca, sc, sk, _, _) = mint();
        let cert = write(&d, "s.crt", &sc);
        let key = write(&d, "s.key", &sk);
        let ca_p = write(&d, "ca.crt", &ca);
        assert!(server_config_mtls(&cert, &key, &ca_p).is_ok());
    }
    #[test]
    fn client_config_with_and_without_identity() {
        install_crypto_provider();
        let d = tempfile::TempDir::new().unwrap();
        let (ca, _, _, cc, ck) = mint();
        let ca_p = write(&d, "ca.crt", &ca);
        let cc_p = write(&d, "c.crt", &cc);
        let ck_p = write(&d, "c.key", &ck);
        assert!(client_config(&ca_p, None).is_ok());
        assert!(client_config(&ca_p, Some((&cc_p, &ck_p))).is_ok());
    }
    #[test]
    fn missing_file_is_err_naming_path() {
        let e = server_config_tls_only("/no/such/cert.pem", "/no/such/key.pem").unwrap_err();
        assert!(e.to_string().contains("/no/such/cert.pem"), "err names the path: {e}");
    }
    #[test]
    fn garbage_pem_is_err() {
        let d = tempfile::TempDir::new().unwrap();
        let bad = write(&d, "bad.pem", "not a pem file");
        assert!(load_ca_roots(&bad).is_err());
    }
    #[test]
    fn provider_install_is_idempotent() {
        install_crypto_provider();
        install_crypto_provider();
    }
}
