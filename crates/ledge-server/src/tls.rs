//! TLS / mTLS transport assembly (Phase 4d-4). Server-only: PEM loading and
//! rustls Server/Client config construction. The crypto provider is aws-lc-rs
//! (the same backend reqwest's `rustls-tls` uses) so only one crypto crate links.

/// Install the process-default rustls crypto provider (aws-lc-rs). Idempotent:
/// a second call — or one after reqwest already installed it — is a harmless
/// no-op (the `Err` from a double install is ignored). Call once at boot,
/// unconditionally (cheap; harmless when TLS is off).
pub fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}
