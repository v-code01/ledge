use std::path::PathBuf;
use ledge_core::LedgeError;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct LedgeConfig {
    pub server: ServerConfig,
    pub object_store: ObjectStoreConfig,
    pub ref_store: RefStoreConfig,
    pub metrics: MetricsConfig,
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

impl LedgeConfig {
    pub fn load(config_path: Option<&PathBuf>) -> ledge_core::Result<Self> {
        use config::{Config, Environment, File};
        let mut builder = Config::builder()
            .set_default("server.addr",                       "0.0.0.0:3000").map_err(map_cfg)?
            .set_default("server.data_dir",                   "/var/lib/ledge").map_err(map_cfg)?
            .set_default("object_store.fanout_depth",          2i64).map_err(map_cfg)?
            .set_default("ref_store.wal_compact_threshold_mb", 64i64).map_err(map_cfg)?
            .set_default("metrics.enabled",                    true).map_err(map_cfg)?
            .set_default("metrics.addr",                       "0.0.0.0:9090").map_err(map_cfg)?;
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
        builder.build().map_err(map_cfg)?.try_deserialize::<LedgeConfig>().map_err(map_cfg)
    }
}

fn map_cfg(e: config::ConfigError) -> LedgeError {
    LedgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
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
    fn env_var_overrides() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LEDGE__SERVER__ADDR", "10.0.0.1:5000");
        let cfg = LedgeConfig::load(None).unwrap();
        assert_eq!(cfg.server.addr, "10.0.0.1:5000");
        std::env::remove_var("LEDGE__SERVER__ADDR");
    }
}
