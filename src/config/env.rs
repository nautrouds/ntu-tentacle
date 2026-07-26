use super::Config;
use anyhow::{Result, anyhow};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn load() -> Result<Vec<Config>> {
    tracing::debug!("loading configuration from environment variables");

    let service_name_env_keys = [
        "NAUTROUDS_SERVICE_NAME",
        "NAUTROUDS_SERVICE_ID",
        "SERVICE_NAME",
        "SERVICE",
        "NAME",
    ];

    let service_name: Arc<str> = service_name_env_keys
        .iter()
        .find_map(|key| env::var(key).ok())
        .ok_or_else(|| {
            tracing::error!(
                var = "NAUTROUDS_SERVICE_NAME",
                "missing required environment variable"
            );
            anyhow!("NAUTROUDS_SERVICE_NAME is required")
        })?
        .into();

    let base_dir: Arc<Path> = env::var("NAUTROUDS_SERVICES_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/run/nautrouds/services"))
        .into();

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
        .ok_or_else(|| {
            tracing::error!(
                var = "NAUTROUDS_TARGET_ADDR",
                "missing required environment variable"
            );
            anyhow!("NAUTROUDS_TARGET_ADDR is required")
        })?;

    let mut seen = std::collections::HashSet::new();
    let target_addrs: Vec<String> = target_addrs_raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .filter(|s| seen.insert(s.clone()))
        .collect();

    if target_addrs.is_empty() {
        tracing::error!(
            var = "NAUTROUDS_TARGET_ADDR",
            "no valid target addresses found"
        );
        anyhow::bail!("NAUTROUDS_TARGET_ADDR must contain at least one address");
    }

    Ok(target_addrs
        .into_iter()
        .map(|target_addr| {
            Config::new(
                service_name.clone(),
                target_addr,
                base_dir.clone(),
                max_connections,
                metrics_interval_secs,
            )
        })
        .collect())
}
