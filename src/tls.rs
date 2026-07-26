use crate::config::Target;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell, RwLock};

pub type Bytes = Arc<Vec<u8>>;

#[derive(Debug, Clone, Default)]
pub struct Tls {
    pub ca: Option<Bytes>,
    pub cert: Option<Bytes>,
    pub key: Option<Bytes>,
}

type LoadResult = std::result::Result<Bytes, Arc<anyhow::Error>>;

#[derive(Default, Clone)]
struct Slot {
    cache: Arc<RwLock<HashMap<String, Bytes>>>,
    inflight: Arc<Mutex<HashMap<String, Arc<OnceCell<LoadResult>>>>>,
}

impl Slot {
    async fn get_cached(&self, path: &str) -> Option<Bytes> {
        self.cache.read().await.get(path).cloned()
    }

    async fn get_or_create_cell(&self, path: &str) -> Arc<OnceCell<LoadResult>> {
        let mut inflight = self.inflight.lock().await;
        inflight
            .entry(path.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    }

    async fn store(&self, path: &str, bytes: Bytes) {
        let mut cache = self.cache.write().await;
        cache.insert(path.to_string(), bytes);
    }

    async fn clear_inflight(&self, path: &str) {
        let mut inflight = self.inflight.lock().await;
        inflight.remove(path);
    }
}

#[derive(Default)]
pub struct TlsManager {
    ca: Slot,
    cert: Slot,
    key: Slot,
}

impl TlsManager {
    pub fn new() -> Self {
        Self::default()
    }

    async fn load(slot: &Slot, path: &str) -> Result<Bytes> {
        if let Some(cached) = slot.get_cached(path).await {
            return Ok(cached);
        }

        let cell = slot.get_or_create_cell(path).await;

        let init_result = cell
            .get_or_init(|| async move {
                let read_result = tokio::fs::read(path).await;
                let outcome = read_result
                    .with_context(|| format!("failed to read TLS file: {path}"))
                    .map(Arc::new);

                if let std::result::Result::Ok(bytes) = &outcome {
                    slot.store(path, bytes.clone()).await;
                }
                slot.clear_inflight(path).await;

                outcome.map_err(Arc::new)
            })
            .await;
        let result = init_result.clone();

        result.map_err(|e| anyhow::anyhow!("{e:#}"))
    }

    pub async fn fetch(&self, target: Target) -> Result<(String, Tls)> {
        let Target {
            addr,
            ca,
            cert,
            key,
        } = target;
        let mut tls = Tls::default();

        if let Some(ca_path) = ca.as_deref().and_then(|p| p.to_str()) {
            let ca_bytes = Self::load(&self.ca, ca_path).await?;
            tls.ca = Some(ca_bytes);
        }

        if let (Some(cert_path), Some(key_path)) = (
            cert.as_deref().and_then(|p| p.to_str()),
            key.as_deref().and_then(|p| p.to_str()),
        ) {
            let cert_bytes = Self::load(&self.cert, cert_path).await?;
            let key_bytes = Self::load(&self.key, key_path).await?;
            tls.cert = Some(cert_bytes);
            tls.key = Some(key_bytes);
        }

        Ok((addr, tls))
    }
}
