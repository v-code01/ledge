//! Outbound SSRF guard for tenant-controlled URLs (webhook targets).
//!
//! A multi-tenant deploy must not let a tenant point Ledge at internal addresses
//! — loopback, RFC-1918 private ranges, link-local (incl. the cloud metadata
//! endpoint `169.254.169.254`), unique-local IPv6, etc. — and turn the server
//! into an SSRF pivot. [`guard_outbound`] resolves the URL's host and rejects any
//! non-public destination. `allow_private` opts out for single-tenant / dev use.
//!
//! Honest residual: a resolve-then-connect check is open to DNS rebinding (the
//! name resolves public here, private at connect time). Catching that needs a
//! pinned-IP connector; this v1 blocks the common cases (literal private IPs and
//! hostnames that resolve to them).

use std::net::IpAddr;

/// Is `ip` a non-public address that an outbound request must not target?
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local() // 169.254.0.0/16 — includes cloud metadata
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_documentation()
                // CGNAT shared space 100.64.0.0/10
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64)
        }
        IpAddr::V6(v6) => {
            // An IPv4-mapped address (::ffff:a.b.c.d) is really IPv4 — check it as such.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_blocked_ip(IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        }
    }
}

/// Reject an outbound URL whose host is (or resolves to) any non-public IP.
/// `allow_private` short-circuits to `Ok` for single-tenant / dev deployments.
pub async fn guard_outbound(url: &str, allow_private: bool) -> Result<(), String> {
    if allow_private {
        return Ok(());
    }
    let u = reqwest::Url::parse(url).map_err(|e| format!("invalid url: {e}"))?;
    let scheme = u.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(format!("scheme '{scheme}' not allowed"));
    }
    let host = u.host_str().ok_or_else(|| "url has no host".to_string())?;
    // A literal-IP host: check directly (no DNS).
    if let Ok(ip) = host.parse::<IpAddr>() {
        return if is_blocked_ip(ip) {
            Err(format!("blocked non-public address {ip}"))
        } else {
            Ok(())
        };
    }
    // Hostname: resolve and check every answer (an attacker may point one name at
    // a private IP). Async DNS so we never block the runtime.
    let port = u.port_or_known_default().unwrap_or(443);
    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("dns lookup failed: {e}"))?;
    let mut resolved = false;
    for a in addrs {
        resolved = true;
        if is_blocked_ip(a.ip()) {
            return Err(format!("{host} resolves to blocked address {}", a.ip()));
        }
    }
    if !resolved {
        return Err(format!("{host} did not resolve"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_addresses() {
        let blocked = [
            "127.0.0.1", "10.1.2.3", "192.168.0.1", "172.16.5.5",
            "169.254.169.254", // cloud metadata
            "0.0.0.0", "100.64.0.1", "::1", "fe80::1", "fc00::1",
            "::ffff:127.0.0.1", // IPv4-mapped loopback
        ];
        for s in blocked {
            assert!(is_blocked_ip(s.parse().unwrap()), "{s} must be blocked");
        }
        let public = ["1.1.1.1", "8.8.8.8", "93.184.216.34", "2606:4700:4700::1111"];
        for s in public {
            assert!(!is_blocked_ip(s.parse().unwrap()), "{s} must be allowed");
        }
    }

    #[tokio::test]
    async fn guard_blocks_private_allows_public_and_opts_out() {
        assert!(guard_outbound("http://169.254.169.254/latest/meta-data", false).await.is_err());
        assert!(guard_outbound("http://127.0.0.1:8080/hook", false).await.is_err());
        assert!(guard_outbound("ftp://example.com", false).await.is_err()); // scheme
        assert!(guard_outbound("http://1.1.1.1/hook", false).await.is_ok());
        // allow_private opts out entirely (single-tenant/dev).
        assert!(guard_outbound("http://127.0.0.1:8080/hook", true).await.is_ok());
    }
}
