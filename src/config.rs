use anyhow::Result;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Target {
    pub addr: String,
    pub weight: Option<u32>,
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

impl Target {
    pub const MAX_WEIGHT: u32 = 100;

    pub fn from(addr: String) -> Self {
        Self {
            addr,
            weight: None,
            ca: None,
            cert: None,
            key: None,
        }
    }

    pub fn tls_pair_is_valid(&self) -> bool {
        self.cert.is_some() == self.key.is_some()
    }

    pub fn weight_in_range(&self) -> bool {
        self.weight.is_none_or(|w| (1..=Self::MAX_WEIGHT).contains(&w))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn target_with_weight(weight: Option<u32>) -> Target {
        Target {
            weight,
            ..Target::from("localhost:8080".to_string())
        }
    }

    #[test]
    fn weight_in_range_accepts_none() {
        assert!(target_with_weight(None).weight_in_range());
    }

    #[test]
    fn weight_in_range_accepts_bounds() {
        assert!(target_with_weight(Some(1)).weight_in_range());
        assert!(target_with_weight(Some(Target::MAX_WEIGHT)).weight_in_range());
    }

    #[test]
    fn weight_in_range_rejects_zero() {
        assert!(!target_with_weight(Some(0)).weight_in_range());
    }

    #[test]
    fn weight_in_range_rejects_above_max() {
        assert!(!target_with_weight(Some(Target::MAX_WEIGHT + 1)).weight_in_range());
    }
}
