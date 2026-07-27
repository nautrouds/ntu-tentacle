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
