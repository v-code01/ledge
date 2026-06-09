use std::path::PathBuf;
use ledge_core::LedgeError;
use ledge_cluster::{Replica, ShardId, ShardMap, ShardMapError};

#[derive(Debug, serde::Deserialize, Clone)]
pub struct LedgeConfig {
    pub server: ServerConfig,
    pub object_store: ObjectStoreConfig,
    pub ref_store: RefStoreConfig,
    pub metrics: MetricsConfig,
    pub workspace: WorkspaceConfig,
    pub cluster: ClusterConfig,
    pub auth: AuthConfig,
    pub quotas: QuotaConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub webhooks: WebhooksConfig,
    #[serde(default)]
    pub sync: SyncConfig,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct ServerConfig {
    pub addr: String,
    pub data_dir: String,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct ObjectStoreConfig {
    pub fanout_depth: u8,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct RefStoreConfig {
    pub wal_compact_threshold_mb: u64,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub addr: String,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct WorkspaceConfig {
    pub expiry_interval_secs: u64,
    pub gc_interval_secs: u64,
    pub default_ttl_secs: u64,
}

/// Cluster (sharded Raft) configuration. Disabled by default: a default-loaded
/// config is single-node and byte-identical to Phase 1/2 (the `/raft` and
/// `/cluster` handlers see no shards and report not-clustered).
#[derive(Debug, serde::Deserialize, Clone)]
pub struct ClusterConfig {
    /// When false (default), the server runs single-node; no Raft groups are
    /// built and the cluster endpoints are inert (503).
    pub enabled: bool,
    /// This node's Raft node id (must be unique across the cluster).
    pub node_id: u64,
    /// Number of shards (independent Raft groups) this cluster partitions into.
    pub num_shards: u32,
    /// Peer node table for the Raft HTTP network. Empty by default; populated
    /// from `[[cluster.peers]]` TOML blocks. `#[serde(default)]` so a config with
    /// no peers block deserializes to an empty Vec without a `config`-crate
    /// empty-Vec default footgun.
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    /// The static shard map (spec §5): each shard's replica set, identical on
    /// every node. `#[serde(default)]` so single-node configs omit it. When
    /// non-empty this SUPERSEDES the flat `num_shards`/`peers` fields, which are
    /// retained only for Phase 3 back-compat; `num_shards` is then derived from
    /// the map (`shard_map().num_shards()`).
    #[serde(default)]
    pub shards: Vec<ShardSpec>,
    /// Local bind address this node serves its `/raft/*` endpoints on.
    pub raft_bind: String,
}

/// One cluster peer: a Raft node id and the base URL it serves `/raft/*` on.
#[derive(Debug, serde::Deserialize, Clone)]
pub struct PeerConfig {
    pub id: u64,
    pub addr: String,
}

/// One shard's declared replica set in the static shard map (spec §5).
/// `#[serde(default)]` on `ClusterConfig.shards` lets a single-node config omit
/// this entirely.
#[derive(Debug, serde::Deserialize, Clone)]
pub struct ShardSpec {
    /// Shard id (`0..num_shards`); used as the `ShardId` key.
    pub id: u32,
    /// Ordered replica members of this shard (order is preserved into the map,
    /// so the first member is the deterministic no-preference forward target).
    pub members: Vec<ReplicaSpec>,
}

/// One replica entry inside a `[[cluster.shards]]` block (spec §5).
#[derive(Debug, serde::Deserialize, Clone)]
pub struct ReplicaSpec {
    /// Raft node id of the replica.
    pub id: u64,
    /// Base URL the replica serves `/raft/*` + `/cluster/*` on.
    pub addr: String,
}

/// Authentication (Phase 4d-1) configuration. Disabled by default: a
/// default-loaded config is unauthenticated and byte-identical to Phase 1-4c.
#[derive(Debug, serde::Deserialize, Clone)]
pub struct AuthConfig {
    /// When false (default), no credential is required: the middleware injects a
    /// synthetic root principal so every handler still extracts one.
    pub enabled: bool,
    /// Shared node-to-node bearer secret for INTERNAL routes (`/raft|/cluster|
    /// /objects`). Required iff `enabled && clustered`. Interim until 4d-4 mTLS.
    #[serde(default)]
    pub cluster_secret: Option<String>,
    /// Optional: on first boot, if `enabled` and the store is empty, mint an
    /// admin key from this token so a fresh cluster is reachable.
    #[serde(default)]
    pub bootstrap_admin_token: Option<String>,
}

/// Per-tenant quota (Phase 4d-3) configuration. Disabled by default: a
/// default-loaded config enforces NO quota and is byte-identical to Phase 4d-2.
/// `root` is always exempt regardless of `enabled`. `None` per-limit = unlimited.
#[derive(Debug, serde::Deserialize, Clone)]
pub struct QuotaConfig {
    /// When false (default), NO quota is enforced (back-compat: quotas off).
    pub enabled: bool,
    /// Max LIVE workspaces per non-root tenant (exact, at `fork`).
    #[serde(default)]
    pub max_workspaces: Option<u64>,
    /// Max durable bytes per non-root tenant (SOFT, at `commit`).
    #[serde(default)]
    pub max_durable_bytes: Option<u64>,
    /// Max durable object count per non-root tenant (SOFT, at `commit`).
    #[serde(default)]
    pub max_object_count: Option<u64>,
    /// Per-tenant token-bucket sustained rate (requests/sec). `None` = unlimited.
    #[serde(default)]
    pub max_requests_per_sec: Option<u32>,
    /// Token-bucket burst capacity. `None` ⇒ defaults to `max_requests_per_sec`.
    #[serde(default)]
    pub burst: Option<u32>,
}

/// TLS / mTLS (Phase 4d-4) configuration. Disabled by default: a default-loaded
/// config serves plaintext and is byte-identical to Phase 4d-3. `enabled` adds
/// server-TLS on the client listener; `mtls` adds a mutual-TLS peer listener.
#[derive(Debug, serde::Deserialize, Clone, Default)]
pub struct TlsConfig {
    /// When false (default), no TLS: the server binds plaintext (back-compat).
    pub enabled: bool,
    /// Server leaf+chain PEM (required when `enabled`).
    #[serde(default)] pub cert_path: Option<String>,
    /// Server private key PEM (required when `enabled`).
    #[serde(default)] pub key_path: Option<String>,
    /// CA bundle PEM: verifies peer server certs (outbound) and client certs
    /// (mTLS inbound). Required when `mtls`.
    #[serde(default)] pub ca_path: Option<String>,
    /// When true, require + verify a CA-signed client cert on the peer listener
    /// (mutual TLS). Requires `enabled` + `cluster.enabled` + ca/peer/client paths.
    #[serde(default)] pub mtls: bool,
    /// Bind address for the mTLS peer listener (required when `mtls`).
    #[serde(default)] pub peer_addr: Option<String>,
    /// THIS node's client identity cert PEM for outbound mTLS (required when `mtls`).
    #[serde(default)] pub client_cert_path: Option<String>,
    /// THIS node's client identity key PEM for outbound mTLS (required when `mtls`).
    #[serde(default)] pub client_key_path: Option<String>,
}

/// Webhooks / event surface configuration. Disabled by default (byte-identical
/// when off): no dispatcher, no events, the /webhooks routes report 503.
#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct WebhooksConfig {
    pub enabled: bool,
}

/// Git remote sync configuration. Disabled by default (byte-identical when off:
/// no engine, the /sync routes return 503, no `git` subprocess is ever spawned).
#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct SyncConfig {
    pub enabled: bool,
    /// Allowed upstream hosts (empty ⇒ any — dev only; set in prod to gate SSRF).
    #[serde(default)]
    pub allowed_upstream_hosts: Vec<String>,
}

impl QuotaConfig {
    /// Project the manager-relevant durable limits into a [`ledge_workspace::QuotaLimits`]
    /// (Copy). The rate/burst are NOT included — they build the `TenantRateLimiter`
    /// (R Q13).
    pub fn to_limits(&self) -> ledge_workspace::QuotaLimits {
        ledge_workspace::QuotaLimits {
            enabled: self.enabled,
            max_workspaces: self.max_workspaces,
            max_durable_bytes: self.max_durable_bytes,
            max_object_count: self.max_object_count,
        }
    }
}

impl ClusterConfig {
    /// Build the validated [`ShardMap`] from the declared `[[cluster.shards]]`.
    /// An empty `shards` (single-node / no-cluster) yields an empty, valid map
    /// (`num_shards() == 0`). Validation errors (empty shard, duplicate node)
    /// surface as `ShardMapError`.
    pub fn shard_map(&self) -> Result<ShardMap, ShardMapError> {
        ShardMap::from_entries(self.shards.iter().map(|s| {
            (
                ShardId(s.id),
                s.members
                    .iter()
                    .map(|m| Replica { node_id: m.id, addr: m.addr.clone() })
                    .collect::<Vec<_>>(),
            )
        }))
    }
}

impl LedgeConfig {
    pub fn load(config_path: Option<&PathBuf>) -> ledge_core::Result<Self> {
        use config::{Config, Environment, File};
        let mut builder = Config::builder()
            .set_default("server.addr",                       "0.0.0.0:3000").map_err(map_cfg)?
            .set_default("server.data_dir",                   "/var/lib/ledge").map_err(map_cfg)?
            .set_default("object_store.fanout_depth",          2i64).map_err(map_cfg)?
            .set_default("ref_store.wal_compact_threshold_mb", 64i64).map_err(map_cfg)?
            .set_default("metrics.enabled",                    true).map_err(map_cfg)?
            .set_default("metrics.addr",                       "0.0.0.0:9090").map_err(map_cfg)?
            .set_default("workspace.expiry_interval_secs",     30i64).map_err(map_cfg)?
            .set_default("workspace.gc_interval_secs",         300i64).map_err(map_cfg)?
            .set_default("workspace.default_ttl_secs",         3600i64).map_err(map_cfg)?
            .set_default("cluster.enabled",                    false).map_err(map_cfg)?
            .set_default("cluster.node_id",                    1i64).map_err(map_cfg)?
            .set_default("cluster.num_shards",                 1i64).map_err(map_cfg)?
            .set_default("cluster.raft_bind",                  "0.0.0.0:4001").map_err(map_cfg)?
            .set_default("auth.enabled",                       false).map_err(map_cfg)?
            .set_default("quotas.enabled",                     false).map_err(map_cfg)?
            .set_default("tls.enabled", false).map_err(map_cfg)?
            .set_default("tls.mtls",    false).map_err(map_cfg)?
            .set_default("webhooks.enabled", false).map_err(map_cfg)?
            .set_default("sync.enabled", false).map_err(map_cfg)?;
        if let Some(path) = config_path {
            builder = builder.add_source(
                File::from(path.as_ref())
                    .format(config::FileFormat::Toml)
                    .required(false),
            );
        }
        builder = builder.add_source(
            Environment::with_prefix("LEDGE").separator("__").try_parsing(true),
        );
        let cfg: LedgeConfig = builder
            .build()
            .map_err(map_cfg)?
            .try_deserialize()
            .map_err(map_cfg)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Fail-fast config invariants (Phase 4d-4). Called at the end of `load()` so
    /// the server refuses to boot half-configured rather than failing at the first
    /// handshake.
    fn validate(&self) -> ledge_core::Result<()> {
        if self.tls.enabled
            && (self.tls.cert_path.is_none() || self.tls.key_path.is_none())
        {
            return Err(invalid_config(
                "tls.enabled requires tls.cert_path and tls.key_path",
            ));
        }
        if self.tls.mtls {
            let ok = self.tls.enabled
                && self.cluster.enabled
                && self.tls.ca_path.is_some()
                && self.tls.peer_addr.is_some()
                && self.tls.client_cert_path.is_some()
                && self.tls.client_key_path.is_some();
            if !ok {
                return Err(invalid_config(
                    "tls.mtls requires tls.enabled + cluster.enabled + tls.{ca_path,peer_addr,client_cert_path,client_key_path}",
                ));
            }
        }
        Ok(())
    }
}

fn map_cfg(e: config::ConfigError) -> LedgeError {
    LedgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

fn invalid_config(msg: &str) -> LedgeError {
    LedgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    // Serialize all config tests to prevent env-var leakage between parallel tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn defaults_load_without_file() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config must load");
        assert_eq!(cfg.server.addr, "0.0.0.0:3000");
        assert_eq!(cfg.server.data_dir, "/var/lib/ledge");
        assert_eq!(cfg.object_store.fanout_depth, 2);
        assert_eq!(cfg.ref_store.wal_compact_threshold_mb, 64);
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.metrics.addr, "0.0.0.0:9090");
    }

    #[test]
    fn workspace_defaults_load() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config must load");
        assert_eq!(cfg.workspace.expiry_interval_secs, 30);
        assert_eq!(cfg.workspace.gc_interval_secs, 300);
        assert_eq!(cfg.workspace.default_ttl_secs, 3600);
    }

    #[test]
    fn toml_file_overrides_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[server]\naddr=\"127.0.0.1:4000\"\ndata_dir=\"/tmp/t\"\n[object_store]\nfanout_depth=2\n[ref_store]\nwal_compact_threshold_mb=128\n[metrics]\nenabled=false\naddr=\"127.0.0.1:9091\"").unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        assert_eq!(cfg.server.addr, "127.0.0.1:4000");
        assert_eq!(cfg.ref_store.wal_compact_threshold_mb, 128);
        assert!(!cfg.metrics.enabled);
    }

    #[test]
    fn cluster_config_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config must load");
        assert!(!cfg.cluster.enabled, "cluster disabled by default");
        assert_eq!(cfg.cluster.node_id, 1);
        assert_eq!(cfg.cluster.num_shards, 1);
        assert!(cfg.cluster.peers.is_empty(), "peers empty by default");
        assert_eq!(cfg.cluster.raft_bind, "0.0.0.0:4001");
    }

    #[test]
    fn cluster_config_toml_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[cluster]\nenabled=true\nnode_id=2\nnum_shards=4\nraft_bind=\"127.0.0.1:5001\"\n[[cluster.peers]]\nid=1\naddr=\"http://h1:4001\"\n[[cluster.peers]]\nid=3\naddr=\"http://h3:4001\""
        )
        .unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.node_id, 2);
        assert_eq!(cfg.cluster.num_shards, 4);
        assert_eq!(cfg.cluster.raft_bind, "127.0.0.1:5001");
        assert_eq!(cfg.cluster.peers.len(), 2);
        assert_eq!(cfg.cluster.peers[0].id, 1);
        assert_eq!(cfg.cluster.peers[0].addr, "http://h1:4001");
        assert_eq!(cfg.cluster.peers[1].id, 3);
    }

    #[test]
    fn cluster_shards_parse_from_toml() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[cluster]\nenabled=true\nnode_id=3\nraft_bind=\"0.0.0.0:8403\"\n\
             [[cluster.shards]]\nid=0\nmembers=[{{id=1,addr=\"http://n1:8401\"}},{{id=2,addr=\"http://n2:8402\"}},{{id=3,addr=\"http://n3:8403\"}}]\n\
             [[cluster.shards]]\nid=1\nmembers=[{{id=3,addr=\"http://n3:8403\"}},{{id=4,addr=\"http://n4:8404\"}},{{id=5,addr=\"http://n5:8405\"}}]"
        )
        .unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.node_id, 3);
        assert_eq!(cfg.cluster.shards.len(), 2);
        assert_eq!(cfg.cluster.shards[0].id, 0);
        assert_eq!(cfg.cluster.shards[0].members.len(), 3);
        assert_eq!(cfg.cluster.shards[1].members[2].id, 5);
        assert_eq!(cfg.cluster.shards[1].members[2].addr, "http://n5:8405");
    }

    #[test]
    fn cluster_shards_convert_to_shard_map() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[cluster]\nenabled=true\nnode_id=3\nraft_bind=\"0.0.0.0:8403\"\n\
             [[cluster.shards]]\nid=0\nmembers=[{{id=1,addr=\"http://n1:8401\"}},{{id=2,addr=\"http://n2:8402\"}},{{id=3,addr=\"http://n3:8403\"}}]\n\
             [[cluster.shards]]\nid=1\nmembers=[{{id=3,addr=\"http://n3:8403\"}},{{id=4,addr=\"http://n4:8404\"}},{{id=5,addr=\"http://n5:8405\"}}]"
        )
        .unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        let map = cfg.cluster.shard_map().expect("valid shard map");
        assert_eq!(map.num_shards(), 2);
        // Placement matches spec §3.2: node 3 hosts both, node 1 only shard 0.
        assert_eq!(
            map.shards_hosted_by(3),
            vec![ledge_cluster::ShardId(0), ledge_cluster::ShardId(1)]
        );
        assert_eq!(map.shards_hosted_by(1), vec![ledge_cluster::ShardId(0)]);
        assert_eq!(map.replica_addr(ledge_cluster::ShardId(1), 4), Some("http://n4:8404"));
    }

    #[test]
    fn empty_shards_still_parse_single_node() {
        // No [[cluster.shards]] blocks (single-node / no-cluster) must parse to
        // an empty Vec, and shard_map() yields an empty map (num_shards == 0).
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config must load");
        assert!(cfg.cluster.shards.is_empty());
        let map = cfg.cluster.shard_map().expect("empty map is valid");
        assert_eq!(map.num_shards(), 0);
    }

    #[test]
    fn auth_config_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config must load");
        assert!(!cfg.auth.enabled, "auth disabled by default (back-compat)");
        assert!(cfg.auth.cluster_secret.is_none());
        assert!(cfg.auth.bootstrap_admin_token.is_none());
    }

    #[test]
    fn auth_config_toml_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[auth]\nenabled=true\ncluster_secret=\"svc-secret\"\nbootstrap_admin_token=\"ledge_x_y\""
        )
        .unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        assert!(cfg.auth.enabled);
        assert_eq!(cfg.auth.cluster_secret.as_deref(), Some("svc-secret"));
        assert_eq!(cfg.auth.bootstrap_admin_token.as_deref(), Some("ledge_x_y"));
    }

    #[test]
    fn quota_config_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config must load");
        assert!(!cfg.quotas.enabled, "quotas disabled by default (back-compat)");
        assert!(cfg.quotas.max_workspaces.is_none());
        assert!(cfg.quotas.max_durable_bytes.is_none());
        assert!(cfg.quotas.max_object_count.is_none());
        assert!(cfg.quotas.max_requests_per_sec.is_none());
        assert!(cfg.quotas.burst.is_none());
    }

    #[test]
    fn quota_config_toml_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[quotas]\nenabled=true\nmax_workspaces=2\nmax_durable_bytes=1048576\nmax_object_count=100\nmax_requests_per_sec=10\nburst=20"
        )
        .unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        assert!(cfg.quotas.enabled);
        assert_eq!(cfg.quotas.max_workspaces, Some(2));
        assert_eq!(cfg.quotas.max_durable_bytes, Some(1_048_576));
        assert_eq!(cfg.quotas.max_object_count, Some(100));
        assert_eq!(cfg.quotas.max_requests_per_sec, Some(10));
        assert_eq!(cfg.quotas.burst, Some(20));
        // The projected limits carry only the manager-relevant fields (R Q13).
        let lim = cfg.quotas.to_limits();
        assert!(lim.enabled);
        assert_eq!(lim.max_workspaces, Some(2));
        assert_eq!(lim.max_durable_bytes, Some(1_048_576));
        assert_eq!(lim.max_object_count, Some(100));
    }

    #[test]
    fn env_var_overrides() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LEDGE__SERVER__ADDR", "10.0.0.1:5000");
        let cfg = LedgeConfig::load(None).unwrap();
        assert_eq!(cfg.server.addr, "10.0.0.1:5000");
        std::env::remove_var("LEDGE__SERVER__ADDR");
    }

    #[test]
    fn tls_config_defaults_disabled() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config must load");
        assert!(!cfg.tls.enabled);
        assert!(!cfg.tls.mtls);
        assert!(cfg.tls.cert_path.is_none());
    }

    #[test]
    fn tls_enabled_requires_cert_and_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[tls]\nenabled=true").unwrap();
        assert!(
            LedgeConfig::load(Some(&f.path().to_path_buf())).is_err(),
            "enabled without cert/key must fail validation"
        );
    }

    #[test]
    fn tls_mtls_requires_full_peer_identity_and_cluster() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[tls]\nenabled=true\ncert_path=\"/c.pem\"\nkey_path=\"/k.pem\"\nmtls=true"
        )
        .unwrap();
        assert!(
            LedgeConfig::load(Some(&f.path().to_path_buf())).is_err(),
            "mtls without ca/peer/client/cluster must fail"
        );
    }

    #[test]
    fn tls_full_valid_config_ok() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[cluster]\nenabled=true\nnode_id=1\nnum_shards=1\nraft_bind=\"0.0.0.0:4001\"\n\
             [tls]\nenabled=true\ncert_path=\"/c.pem\"\nkey_path=\"/k.pem\"\nca_path=\"/ca.pem\"\n\
             mtls=true\npeer_addr=\"0.0.0.0:4443\"\nclient_cert_path=\"/cc.pem\"\nclient_key_path=\"/ck.pem\""
        )
        .unwrap();
        assert!(LedgeConfig::load(Some(&f.path().to_path_buf())).is_ok());
    }

    #[test]
    fn sync_disabled_by_default() {
        let _g = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config");
        assert!(!cfg.sync.enabled);
        assert!(cfg.sync.allowed_upstream_hosts.is_empty());
    }

    #[test]
    fn sync_toml_override() {
        let _g = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[sync]\nenabled=true\nallowed_upstream_hosts=[\"github.com\"]").unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        assert!(cfg.sync.enabled);
        assert_eq!(cfg.sync.allowed_upstream_hosts, vec!["github.com".to_string()]);
    }

    #[test]
    fn webhooks_disabled_by_default() {
        let _g = ENV_LOCK.lock().unwrap();
        let cfg = LedgeConfig::load(None).expect("default config");
        assert!(!cfg.webhooks.enabled);
    }

    #[test]
    fn webhooks_toml_override() {
        let _g = ENV_LOCK.lock().unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[webhooks]\nenabled=true").unwrap();
        let cfg = LedgeConfig::load(Some(&f.path().to_path_buf())).unwrap();
        assert!(cfg.webhooks.enabled);
    }

    #[test]
    fn tls_enabled_env_parses_with_cert_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LEDGE__TLS__ENABLED", "true");
        std::env::set_var("LEDGE__TLS__CERT_PATH", "/c.pem");
        std::env::set_var("LEDGE__TLS__KEY_PATH", "/k.pem");
        let cfg = LedgeConfig::load(None).unwrap();
        assert!(cfg.tls.enabled);
        assert_eq!(cfg.tls.cert_path.as_deref(), Some("/c.pem"));
        std::env::remove_var("LEDGE__TLS__ENABLED");
        std::env::remove_var("LEDGE__TLS__CERT_PATH");
        std::env::remove_var("LEDGE__TLS__KEY_PATH");
    }
}
