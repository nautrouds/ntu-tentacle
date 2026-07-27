use anyhow::Result;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Target {
    pub addr: String,
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

impl Target {
    pub fn from(addr: String) -> Self {
        Self {
            addr,
            ca: None,
            cert: None,
            key: None,
        }
    }

    pub fn tls_pair_is_valid(&self) -> bool {
        self.cert.is_some() == self.key.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub service_name: String,
    pub targets: Vec<Target>,
    pub base_dir: PathBuf,
    pub max_connections: usize,
    pub metrics_interval_secs: u64,
}

pub mod env;
pub mod yml;

pub fn load() -> Result<Config> {
    let config = env::load().and_then(yml::load)?;

    if config.targets.is_empty() {
        tracing::error!(
            var = "NAUTROUDS_TARGET_ADDR",
            "no valid target addresses found"
        );
        anyhow::bail!("NAUTROUDS_TARGET_ADDR must contain at least one address");
    }

    Ok(config)
}
