use super::Config;
use anyhow::{Context, Ok, Result};
use serde_yaml::Value;
use std::env;
use std::fs;

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
            let targets = m
                .keys()
                .filter_map(|k| k.as_str().map(str::trim).filter(|s| !s.is_empty()))
                .map(String::from)
                .collect();

            return Ok(Config { targets, ..cfg });
        }
    }

    Ok(cfg)
}
