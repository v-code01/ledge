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

impl WebhookDispatcher {
    pub fn new(store: Arc<WebhookStore>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        Self {
            store,
            client,
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
            .and_then(|host| {
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .resolve(&host, addr)
                    .build()
                    .ok()
            })
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
}
