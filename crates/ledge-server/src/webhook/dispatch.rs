//! Async, signed, best-effort webhook delivery (Phase: webhooks). Never blocks
//! the caller — each delivery is a spawned task with bounded retry.
use std::sync::Arc;
use std::time::Duration;

use super::{sign, store::WebhookStore, EventKind, WebhookConfig};

pub struct WebhookDispatcher {
    store: Arc<WebhookStore>,
    client: reqwest::Client,
    /// When false (the secure default), webhook targets that resolve to a
    /// non-public address are blocked (SSRF guard). Set true for single-tenant /
    /// dev where delivering to localhost/internal hosts is intentional.
    allow_private: bool,
}

/// Per-delivery timeout for a webhook POST.
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// Build the reqwest client used for webhook delivery, optionally pinning `host`
/// to a validated `addr` (closing the DNS-rebind window for a DNS-name target).
///
/// Redirects are NOT followed. The SSRF guard validates the ORIGINAL target only;
/// a redirect target is never checked and the DNS pin does not cover it, so a
/// webhook endpoint that answers `302 Location: http://169.254.169.254/…` would
/// otherwise walk the delivery straight to an internal address — defeating the
/// guard's whole purpose. A 3xx is treated as a (non-2xx) delivery failure.
fn webhook_client(pin: Option<(&str, std::net::SocketAddr)>) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .timeout(WEBHOOK_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none());
    if let Some((host, addr)) = pin {
        b = b.resolve(host, addr);
    }
    b.build().unwrap_or_default()
}

impl WebhookDispatcher {
    pub fn new(store: Arc<WebhookStore>) -> Self {
        Self {
            store,
            client: webhook_client(None),
            allow_private: false,
        }
    }

    /// Allow webhook delivery to private/loopback/internal targets (dev /
    /// single-tenant). Default is to block them as an SSRF guard.
    pub fn allow_private_targets(mut self, allow: bool) -> Self {
        self.allow_private = allow;
        self
    }

    pub fn store(&self) -> &Arc<WebhookStore> {
        &self.store
    }

    /// Deliver `payload` to every tenant webhook handling `kind`. Spawns each
    /// delivery — returns immediately; never blocks the commit path.
    pub fn dispatch(&self, tenant: &str, kind: EventKind, payload: serde_json::Value) {
        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(_) => return,
        };
        for wh in self.store.for_event(tenant, kind) {
            let client = self.client.clone();
            let body = body.clone();
            let evt = kind.wire();
            let allow_private = self.allow_private;
            tokio::spawn(async move { deliver(client, wh, evt, body, allow_private).await });
        }
    }
}

async fn deliver(
    client: reqwest::Client,
    wh: WebhookConfig,
    evt: &'static str,
    body: Vec<u8>,
    allow_private: bool,
) {
    // SSRF guard: never deliver to a tenant-supplied URL that resolves to a
    // non-public address (cloud metadata, loopback, private ranges).
    let pin = match crate::ssrf::guard_outbound(&wh.url, allow_private).await {
        Ok(pin) => pin,
        Err(reason) => {
            crate::metrics::record_webhook_delivery("blocked");
            tracing::warn!(webhook = %wh.id.to_hex(), url = %wh.url, %reason, "webhook target blocked (SSRF guard)");
            return;
        }
    };
    // For a DNS-name target, pin the connection to the validated address so it
    // can't be rebound to a private IP between the check and the connect.
    let client = match pin {
        Some(addr) => reqwest::Url::parse(&wh.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string))
            .map(|host| webhook_client(Some((&host, addr))))
            .unwrap_or(client),
        None => client,
    };
    let sig = sign(&wh.secret, &body);
    let delivery_id = blake3::hash(&body).to_hex().to_string();
    let start = std::time::Instant::now();
    let mut attempt = 0u32;
    loop {
        let res = client
            .post(&wh.url)
            .header("content-type", "application/json")
            .header("x-ledge-event", evt)
            .header("x-ledge-delivery", &delivery_id[..32])
            .header("x-ledge-signature", &sig)
            .body(body.clone())
            .send()
            .await;
        let ok = matches!(&res, Ok(r) if r.status().is_success());
        if ok {
            crate::metrics::record_webhook_delivery("ok");
            crate::metrics::record_webhook_delivery_duration(start.elapsed());
            return;
        }
        attempt += 1;
        if attempt >= 3 {
            crate::metrics::record_webhook_delivery("failed");
            crate::metrics::record_webhook_delivery_duration(start.elapsed());
            tracing::warn!(webhook = %wh.id.to_hex(), url = %wh.url, "webhook delivery failed after retries");
            return;
        }
        tokio::time::sleep(Duration::from_millis(500 * (1u64 << attempt))).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhook::{store::WebhookStore, EventKind};
    use std::sync::Arc;

    #[tokio::test]
    async fn dispatch_empty_store_is_noop() {
        let d = WebhookDispatcher::new(Arc::new(WebhookStore::in_memory()));
        // No webhooks registered ⇒ no delivery spawned, no panic.
        d.dispatch(
            "acme",
            EventKind::RefCommitted,
            serde_json::json!({"event":"ref.committed"}),
        );
        assert_eq!(d.store().count(), 0);
    }

    /// Spawn a loopback HTTP server; return its base URL and a shutdown-safe task.
    /// `handler` is an axum router.
    async fn spawn(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.ok();
        });
        format!("http://127.0.0.1:{}", addr.port())
    }

    /// The SSRF guard validates only the ORIGINAL webhook URL. If the delivery
    /// client followed HTTP redirects, a tenant webhook could point at their own
    /// public server, answer `302 Location: http://<internal>/`, and walk the
    /// delivery to an address the guard never saw — the exact SSRF pivot the guard
    /// exists to prevent. This drives a real redirect and asserts the target is
    /// never reached.
    ///
    /// Runs with `allow_private = true` so the guard is out of the way and the
    /// ONLY thing that can stop the redirect is the client's redirect policy —
    /// isolating exactly the behavior under test.
    #[tokio::test]
    async fn delivery_does_not_follow_redirects() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // Sentinel: flips a flag if it is ever hit (the "internal" target). It
        // answers ANY method — a 307 redirect preserves POST, a 302 downgrades to
        // GET, so the flag must trip regardless of which the client would use.
        let hit = Arc::new(AtomicBool::new(false));
        let hit2 = hit.clone();
        let sentinel = axum::Router::new().route(
            "/",
            axum::routing::any(move || {
                let h = hit2.clone();
                async move {
                    h.store(true, Ordering::SeqCst);
                    "reached"
                }
            }),
        );
        let sentinel_url = spawn(sentinel).await;

        // Redirector: 302s every request to the sentinel.
        let target = format!("{sentinel_url}/");
        let redirector = axum::Router::new().route(
            "/",
            axum::routing::post(move || {
                let t = target.clone();
                async move { axum::response::Redirect::temporary(&t) }
            }),
        );
        let redir_url = spawn(redirector).await;

        let wh = WebhookConfig {
            id: crate::webhook::WebhookId([9u8; 16]),
            tenant_id: "acme".into(),
            url: format!("{redir_url}/"),
            secret: [0u8; 32],
            events: vec![],
            created_at_ms: 0,
            active: true,
        };
        // allow_private = true: the guard passes, so only the redirect policy is
        // under test. pin = None ⇒ the delivery uses `webhook_client(None)`.
        deliver(
            webhook_client(None),
            wh,
            "ref.committed",
            b"{}".to_vec(),
            true,
        )
        .await;

        assert!(
            !hit.load(Ordering::SeqCst),
            "SSRF: the delivery followed a redirect to the internal target"
        );
    }
}
