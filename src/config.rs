use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Config {
    pub service_name: Arc<str>,
    pub target_addr: String,
    pub base_dir: Arc<Path>,
    pub max_connections: usize,
    pub metrics_interval_secs: u64,
}

impl Config {
    pub fn new(
        service_name: Arc<str>,
        target_addr: String,
        base_dir: Arc<Path>,
        max_connections: usize,
        metrics_interval_secs: u64,
    ) -> Self {
        Self {
            service_name,
            target_addr,
            base_dir,
            max_connections,
            metrics_interval_secs,
        }
    }

    pub fn socket_id(&self) -> String {
        self.target_addr.replace([':', '/'], "_")
    }

    pub fn socket_path(&self) -> PathBuf {
        let socket_name = format!("{}.sock", self.socket_id());
        self.service_dir().join(socket_name)
    }

    pub fn service_dir(&self) -> PathBuf {
        self.base_dir.join(self.service_name.as_ref())
    }
}

pub mod env;
