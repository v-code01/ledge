//! Phase 4d-4 TLS + mTLS handshake matrix (spec §4) over a real loopback
//! axum-server listener with rcgen-minted certs. Positive AND negative:
//! untrusted server cert rejected, plaintext-to-TLS-port rejected, mTLS accepts
//! a CA-signed client identity and rejects no-cert / wrong-CA-cert.

use std::sync::Arc;

use axum::{routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use ledge_server::tls;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};

fn mint() -> (String, String, String, String, String, String, String) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let server_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();
    let client_key = KeyPair::generate().unwrap();
    let client_params = CertificateParams::new(vec!["ledge-node".to_string()]).unwrap();
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .unwrap();
    let rogue_key = KeyPair::generate().unwrap();
    let rogue_params = CertificateParams::new(vec!["rogue".to_string()]).unwrap();
    let rogue_cert = rogue_params.self_signed(&rogue_key).unwrap();
    (
        ca_cert.pem(),
        server_cert.pem(),
        server_key.serialize_pem(),
        client_cert.pem(),
        client_key.serialize_pem(),
        rogue_cert.pem(),
        rogue_key.serialize_pem(),
    )
}
fn certs(pem: &str) -> Vec<CertificateDer<'static>> {
    rustls_pemfile::certs(&mut pem.as_bytes())
        .map(|c| c.unwrap())
        .collect()
}
fn key(pem: &str) -> PrivateKeyDer<'static> {
    rustls_pemfile::private_key(&mut pem.as_bytes())
        .unwrap()
        .unwrap()
}
fn write(d: &tempfile::TempDir, n: &str, b: &str) -> String {
    let p = d.path().join(n);
    std::fs::write(&p, b).unwrap();
    p.to_string_lossy().into_owned()
}
fn roots(ca: &str) -> RootCertStore {
    let mut r = RootCertStore::empty();
    for c in certs(ca) {
        r.add(c).unwrap();
    }
    r
}
async fn boot(server_cfg: Arc<rustls::ServerConfig>) -> u16 {
    let app = Router::new().route("/healthz", get(|| async { "ok" }));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let cfg = RustlsConfig::from_config(server_cfg);
    tokio::spawn(async move {
        axum_server::from_tcp_rustls(listener, cfg)
            .serve(app.into_make_service())
            .await
            .ok();
    });
    tokio::task::yield_now().await;
    port
}

#[tokio::test]
async fn server_tls_trusted_client_succeeds() {
    tls::install_crypto_provider();
    let d = tempfile::TempDir::new().unwrap();
    let (ca, sc, sk, ..) = mint();
    let (sc_p, sk_p) = (write(&d, "s.crt", &sc), write(&d, "s.key", &sk));
    let port = boot(tls::server_config_tls_only(&sc_p, &sk_p).unwrap()).await;
    let client = reqwest::Client::builder()
        .use_preconfigured_tls(
            ClientConfig::builder()
                .with_root_certificates(roots(&ca))
                .with_no_client_auth(),
        )
        .build()
        .unwrap();
    let body = client
        .get(format!("https://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn server_tls_untrusted_client_rejected() {
    tls::install_crypto_provider();
    let d = tempfile::TempDir::new().unwrap();
    let (_ca, sc, sk, ..) = mint();
    let (sc_p, sk_p) = (write(&d, "s.crt", &sc), write(&d, "s.key", &sk));
    let port = boot(tls::server_config_tls_only(&sc_p, &sk_p).unwrap()).await;
    // Built-in roots only ⇒ does NOT trust our CA ⇒ TLS error.
    let client = reqwest::Client::new();
    assert!(client
        .get(format!("https://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .is_err());
}

#[tokio::test]
async fn plaintext_to_tls_port_rejected() {
    tls::install_crypto_provider();
    let d = tempfile::TempDir::new().unwrap();
    let (_ca, sc, sk, ..) = mint();
    let (sc_p, sk_p) = (write(&d, "s.crt", &sc), write(&d, "s.key", &sk));
    let port = boot(tls::server_config_tls_only(&sc_p, &sk_p).unwrap()).await;
    let client = reqwest::Client::new();
    assert!(client
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .is_err());
}

#[tokio::test]
async fn mtls_signed_identity_succeeds_no_identity_and_rogue_rejected() {
    tls::install_crypto_provider();
    let d = tempfile::TempDir::new().unwrap();
    let (ca, sc, sk, cc, ck, rc, rk) = mint();
    let (sc_p, sk_p) = (write(&d, "s.crt", &sc), write(&d, "s.key", &sk));
    let ca_p = write(&d, "ca.crt", &ca);
    let port = boot(tls::server_config_mtls(&sc_p, &sk_p, &ca_p).unwrap()).await;

    // (a) CA-signed identity ⇒ handshake completes.
    let ok = reqwest::Client::builder()
        .use_preconfigured_tls(
            ClientConfig::builder()
                .with_root_certificates(roots(&ca))
                .with_client_auth_cert(certs(&cc), key(&ck))
                .unwrap(),
        )
        .build()
        .unwrap();
    assert!(ok
        .get(format!("https://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .is_ok());

    // (b) No identity ⇒ rejected.
    let none = reqwest::Client::builder()
        .use_preconfigured_tls(
            ClientConfig::builder()
                .with_root_certificates(roots(&ca))
                .with_no_client_auth(),
        )
        .build()
        .unwrap();
    assert!(none
        .get(format!("https://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .is_err());

    // (c) Wrong-CA (self-signed) identity ⇒ rejected by the server's CA verifier.
    // The rogue cert is a valid, well-formed identity from the CLIENT's POV
    // (with_client_auth_cert accepts it at build time — it only checks the key
    // matches the leaf, not the chain), so the rejection happens at CONNECT time:
    // the server's WebPkiClientVerifier finds the rogue leaf does not chain to the
    // configured CA and aborts the handshake. Assert the request errors.
    let rogue = reqwest::Client::builder()
        .use_preconfigured_tls(
            ClientConfig::builder()
                .with_root_certificates(roots(&ca))
                .with_client_auth_cert(certs(&rc), key(&rk))
                .unwrap(),
        )
        .build()
        .unwrap();
    assert!(rogue
        .get(format!("https://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .is_err());
}

/// Like `boot`, but returns the `RustlsConfig` handle so the test can hot-reload
/// the live listener's certificate (the same handle `spawn_cert_reloader` swaps).
async fn boot_reloadable(server_cfg: Arc<rustls::ServerConfig>) -> (u16, RustlsConfig) {
    let app = Router::new().route("/healthz", get(|| async { "ok" }));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let cfg = RustlsConfig::from_config(server_cfg);
    let serving = cfg.clone();
    tokio::spawn(async move {
        axum_server::from_tcp_rustls(listener, serving)
            .serve(app.into_make_service())
            .await
            .ok();
    });
    tokio::task::yield_now().await;
    (port, cfg)
}

/// GET `url` with a client that trusts ONLY `ca` (no built-in roots). `Some(body)`
/// on a completed TLS handshake + request, `None` if the handshake/request errors
/// (e.g. the served cert does not chain to `ca`).
async fn get_with_ca(url: &str, ca: &str) -> Option<String> {
    let client = reqwest::Client::builder()
        .use_preconfigured_tls(
            ClientConfig::builder()
                .with_root_certificates(roots(ca))
                .with_no_client_auth(),
        )
        .build()
        .unwrap();
    client.get(url).send().await.ok()?.text().await.ok()
}

/// Hot cert rotation: the LIVE listener serves a NEW certificate after
/// `reload_from_config` re-reads the same PEM paths — no restart. This is exactly
/// what the server's SIGHUP handler (`spawn_cert_reloader`) does.
#[tokio::test]
async fn tls_cert_hot_reload_swaps_served_cert() {
    tls::install_crypto_provider();
    let d = tempfile::TempDir::new().unwrap();
    // Two independent CAs + server identities. Identity A is written to the paths
    // the listener loads from.
    let (ca_a, sc_a, sk_a, ..) = mint();
    let (ca_b, sc_b, sk_b, ..) = mint();
    let cert_p = write(&d, "s.crt", &sc_a);
    let key_p = write(&d, "s.key", &sk_a);

    let (port, handle) =
        boot_reloadable(tls::server_config_tls_only(&cert_p, &key_p).unwrap()).await;
    let url = format!("https://127.0.0.1:{port}/healthz");

    // Serving identity A: a client trusting CA-A succeeds; CA-B is rejected.
    assert_eq!(get_with_ca(&url, &ca_a).await.as_deref(), Some("ok"));
    assert!(get_with_ca(&url, &ca_b).await.is_none());

    // Rotate the files in place to identity B and reload from the SAME paths.
    std::fs::write(&cert_p, &sc_b).unwrap();
    std::fs::write(&key_p, &sk_b).unwrap();
    handle.reload_from_config(tls::server_config_tls_only(&cert_p, &key_p).unwrap());

    // The live listener now serves identity B — CA-B succeeds, CA-A is rejected.
    assert_eq!(get_with_ca(&url, &ca_b).await.as_deref(), Some("ok"));
    assert!(get_with_ca(&url, &ca_a).await.is_none());
}
