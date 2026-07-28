use std::path::PathBuf;
use std::sync::Arc;

use crate::tls;

#[derive(Debug, Clone)]
pub struct CommonInfo {
    pub max_connections: usize,
    pub service_name: String,
    pub metrics_interval_secs: u64,
}

#[derive(Clone)]
pub struct Metadata {
    pub common: Arc<CommonInfo>,
    pub socket_id: String,
    pub socket_path: PathBuf,
    pub target_addr: String,
    pub target_tls: tls::Tls,
}

impl Metadata {
    pub fn missing_targets<'a>(old: &'a [Metadata], new: &[Metadata]) -> Vec<&'a Metadata> {
        let new_addrs: std::collections::HashSet<&str> =
            new.iter().map(|m| m.target_addr.as_str()).collect();

        old.iter()
            .filter(|m| !new_addrs.contains(m.target_addr.as_str()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(target_addr: &str) -> Metadata {
        Metadata {
            common: Arc::new(CommonInfo {
                max_connections: 1,
                service_name: "test".to_string(),
                metrics_interval_secs: 1,
            }),
            socket_id: target_addr.replace([':', '/'], "_"),
            socket_path: PathBuf::from(format!("/tmp/{target_addr}.sock")),
            target_addr: target_addr.to_string(),
            target_tls: tls::Tls::default(),
        }
    }

    #[test]
    fn returns_targets_removed_from_new() {
        let old = vec![metadata("a:1"), metadata("b:2"), metadata("c:3")];
        let new = vec![metadata("a:1"), metadata("c:3")];

        let missing = Metadata::missing_targets(&old, &new);

        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].target_addr, "b:2");
    }

    #[test]
    fn returns_empty_when_targets_unchanged() {
        let old = vec![metadata("a:1"), metadata("b:2")];
        let new = vec![metadata("a:1"), metadata("b:2")];

        assert!(Metadata::missing_targets(&old, &new).is_empty());
    }

    #[test]
    fn returns_empty_when_old_is_empty() {
        let old: Vec<Metadata> = Vec::new();
        let new = vec![metadata("a:1")];

        assert!(Metadata::missing_targets(&old, &new).is_empty());
    }
}
