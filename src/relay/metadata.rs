use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CommonInfo {
    pub max_connections: usize,
    pub service_name: String,
    pub metrics_interval_secs: u64,
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub common: Arc<CommonInfo>,
    pub socket_id: String,
    pub socket_path: PathBuf,
    pub target: String,
}
