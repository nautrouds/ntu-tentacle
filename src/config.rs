use anyhow::Result;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub service_name: String,
    pub targets: Vec<String>,
    pub base_dir: PathBuf,
    pub max_connections: usize,
    pub metrics_interval_secs: u64,
}

pub mod env;

pub fn load() -> Result<Config> {
    let config = env::load()?;

    if config.targets.is_empty() {
        tracing::error!(
            var = "NAUTROUDS_TARGET_ADDR",
            "no valid target addresses found"
        );
        anyhow::bail!("NAUTROUDS_TARGET_ADDR must contain at least one address");
    }

    Ok(config)
}
