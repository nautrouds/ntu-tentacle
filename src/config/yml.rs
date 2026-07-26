use super::{Config, Target};
use anyhow::{Context, Ok, Result};
use serde::Deserialize;
use serde_yaml::Value;
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Default)]
struct TargetTlsRaw {
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
}

pub fn load(cfg: Config) -> Result<Config> {
    let targets_file_env_keys = ["NAUTROUDS_TARGETS_FILE", "TARGETS_FILE", "TARGETS"];

    let targets_file_raw = targets_file_env_keys
        .iter()
        .find_map(|key| env::var(key).ok());

    if let Some(targets_file) = targets_file_raw {
        let content = fs::read_to_string(&targets_file)
            .with_context(|| format!("failed to read targets file: {targets_file}"))?;
        let value: Value = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse targets file as YAML: {targets_file}"))?;

        if let Some(m) = value.as_mapping() {
            let mut targets = Vec::new();

            for (k, v) in m {
                let Some(target) = k.as_str().map(str::trim).filter(|s| !s.is_empty()) else {
                    continue;
                };
                let addr = target.to_string();

                let TargetTlsRaw { ca, cert, key } =
                    serde_yaml::from_value::<TargetTlsRaw>(v.clone()).unwrap_or_default();

                targets.push(Target {
                    addr,
                    ca,
                    cert,
                    key,
                });
            }

            return Ok(Config { targets, ..cfg });
        }
    }

    Ok(cfg)
}
