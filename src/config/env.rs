use super::Config;
use anyhow::{Result, anyhow};
use std::env;
use std::path::PathBuf;

pub fn load() -> Result<Config> {
    tracing::debug!("loading configuration from environment variables");

    let service_name_env_keys = [
        "NAUTROUDS_SERVICE_NAME",
        "NAUTROUDS_SERVICE_ID",
        "SERVICE_NAME",
        "SERVICE",
        "NAME",
    ];

    let service_name: String = service_name_env_keys
        .iter()
        .find_map(|key| env::var(key).ok())
        .ok_or_else(|| {
            tracing::error!(
                var = "NAUTROUDS_SERVICE_NAME",
                "missing required environment variable"
            );
            anyhow!("NAUTROUDS_SERVICE_NAME is required")
        })?;

    let base_dir: PathBuf = env::var("NAUTROUDS_SERVICES_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/run/nautrouds/services"));

    let max_connections: usize = env::var("NAUTROUDS_MAX_CONNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);

    let metrics_interval_secs: u64 = env::var("NAUTROUDS_METRICS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);

    let target_addr_env_keys = ["NAUTROUDS_TARGET_ADDR", "TARGET_ADDR", "TARGET", "ADDR"];

    let target_addrs_raw = target_addr_env_keys
        .iter()
        .find_map(|key| env::var(key).ok())
        .unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let targets: Vec<String> = target_addrs_raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .filter(|s| seen.insert(s.clone()))
        .collect();

    Ok(Config {
        service_name,
        targets,
        base_dir,
        max_connections,
        metrics_interval_secs,
    })
}
