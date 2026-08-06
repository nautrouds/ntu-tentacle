use super::{Config, Target};
use anyhow::{Context, Ok, Result};
use serde::Deserialize;
use serde_yaml::Value;
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Default)]
struct TargetRaw {
    weight: Option<Value>,
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
}

fn parse_weight(addr: &str, raw: Option<Value>) -> Option<u32> {
    let value = raw?;

    let Some(n) = value.as_i64() else {
        tracing::warn!(target = addr, ?value, "weight is not an integer, ignoring");
        return None;
    };

    if n <= 0 {
        tracing::warn!(
            target = addr,
            weight = n,
            "weight must be positive, ignoring"
        );
        return None;
    }

    let max = Target::MAX_WEIGHT as i64;
    if n > max {
        tracing::warn!(
            target = addr,
            weight = n,
            max,
            "weight exceeds max, clamping"
        );
        return Some(Target::MAX_WEIGHT);
    }

    Some(n as u32)
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

                let TargetRaw {
                    weight,
                    ca,
                    cert,
                    key,
                } = serde_yaml::from_value::<TargetRaw>(v.clone()).unwrap_or_default();

                targets.push(Target {
                    weight: parse_weight(&addr, weight),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_weight_absent_is_none() {
        assert_eq!(parse_weight("t", None), None);
    }

    #[test]
    fn parse_weight_in_range() {
        assert_eq!(parse_weight("t", Some(Value::from(5i64))), Some(5));
    }

    #[test]
    fn parse_weight_zero_is_none() {
        assert_eq!(parse_weight("t", Some(Value::from(0i64))), None);
    }

    #[test]
    fn parse_weight_negative_is_none() {
        assert_eq!(parse_weight("t", Some(Value::from(-5i64))), None);
    }

    #[test]
    fn parse_weight_above_max_clamps() {
        assert_eq!(
            parse_weight("t", Some(Value::from(999i64))),
            Some(Target::MAX_WEIGHT)
        );
    }

    #[test]
    fn parse_weight_at_max_is_unclamped() {
        assert_eq!(
            parse_weight("t", Some(Value::from(Target::MAX_WEIGHT as i64))),
            Some(Target::MAX_WEIGHT)
        );
    }

    #[test]
    fn parse_weight_non_numeric_is_none() {
        assert_eq!(parse_weight("t", Some(Value::from("abc"))), None);
    }

    #[test]
    fn parse_weight_float_is_none() {
        assert_eq!(parse_weight("t", Some(Value::from(1.5f64))), None);
    }
}
