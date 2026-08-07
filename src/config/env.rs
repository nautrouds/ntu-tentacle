use super::{Config, Target};
use anyhow::{Result, anyhow};
use std::env;
use std::path::PathBuf;

pub fn service_name_from_env() -> Option<String> {
    let service_name_env_keys = [
        "NAUTROUDS_SERVICE_NAME",
        "NAUTROUDS_SERVICE_ID",
        "SERVICE_NAME",
        "SERVICE",
        "NAME",
    ];

    service_name_env_keys
        .iter()
        .find_map(|key| env::var(key).ok())
}

pub fn pid_dir_from_env() -> Option<String> {
    env::var("NAUTROUDS_PID_DIR").ok()
}

pub fn load() -> Result<Config> {
    tracing::debug!("loading configuration from environment variables");

    let service_name: String = service_name_from_env().ok_or_else(|| {
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

    let metrics_socket_path: Option<PathBuf> = match env::var("NAUTROUDS_METRICS_SOCKET") {
        Ok(v) if v == "-" => None,
        Ok(v) if !v.is_empty() => Some(base_dir.join(v)),
        _ => Some(base_dir.join("metrics.sock")),
    };

    let target_addr_env_keys = ["NAUTROUDS_TARGET_ADDR", "TARGET_ADDR", "TARGET", "ADDR"];

    let target_addrs_raw = target_addr_env_keys
        .iter()
        .find_map(|key| env::var(key).ok())
        .unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let targets: Vec<Target> = target_addrs_raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .filter(|s| seen.insert(s.clone()))
        .map(Target::from)
        .collect();

    Ok(Config {
        service_name,
        targets,
        base_dir,
        max_connections,
        metrics_interval_secs,
        metrics_socket_path,
    })
}
