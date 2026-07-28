use crate::config::Target;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tracing::warn;

pub type Bytes = Arc<Vec<u8>>;

#[derive(Clone, Default)]
pub struct Tls {
    inner: Option<(
        tokio_rustls::TlsConnector,
        rustls_pki_types::ServerName<'static>,
    )>,
}

impl Tls {
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub async fn connect(
        &self,
        tcp_stream: tokio::net::TcpStream,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let (connector, sni) = self
            .inner
            .as_ref()
            .expect("connect called without a TLS connector configured");

        connector
            .connect(sni.clone(), tcp_stream)
            .await
            .context("TLS handshake failed")
    }
}

fn server_name(addr: &str) -> Result<rustls_pki_types::ServerName<'static>> {
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    rustls_pki_types::ServerName::try_from(host.to_string()).context("invalid TLS server name")
}

fn build_connector(
    ca: Option<Bytes>,
    cert: Option<Bytes>,
    key: Option<Bytes>,
) -> Result<Option<tokio_rustls::TlsConnector>> {
    if ca.is_none() && (cert.is_none() || key.is_none()) {
        return Ok(None);
    }

    let mut roots = rustls::RootCertStore::empty();
    if let Some(ca_bytes) = &ca {
        for cert in rustls_pemfile::certs(&mut ca_bytes.as_slice()) {
            let cert = cert.context("invalid CA cert in PEM")?;
            roots.add(cert).context("failed to add CA certificate")?;
        }
    } else {
        let loaded = rustls_native_certs::load_native_certs();
        for err in &loaded.errors {
            warn!(error = %err, "failed to load a native certificate");
        }
        for cert in loaded.certs {
            let _ = roots.add(cert);
        }
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);

    let mut config = if let (Some(cert_bytes), Some(key_bytes)) = (&cert, &key) {
        let chain: Vec<_> = rustls_pemfile::certs(&mut cert_bytes.as_slice())
            .collect::<std::result::Result<_, _>>()
            .context("invalid client cert PEM")?;
        let key = rustls_pemfile::private_key(&mut key_bytes.as_slice())
            .context("invalid client key PEM")?
            .context("no private key found in key PEM")?;
        builder
            .with_client_auth_cert(chain, key)
            .context("invalid client cert/key pair")?
    } else {
        builder.with_no_client_auth()
    };

    // Without ALPN, servers gating on it may pick http/1.1 for h2 traffic and choke on it.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Some(tokio_rustls::TlsConnector::from(Arc::new(config))))
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

        let ca_bytes = match ca.as_deref().and_then(|p| p.to_str()) {
            Some(ca_path) => Some(Self::load(&self.ca, ca_path).await?),
            None => None,
        };

        let (cert_bytes, key_bytes) = match (
            cert.as_deref().and_then(|p| p.to_str()),
            key.as_deref().and_then(|p| p.to_str()),
        ) {
            (Some(cert_path), Some(key_path)) => (
                Some(Self::load(&self.cert, cert_path).await?),
                Some(Self::load(&self.key, key_path).await?),
            ),
            _ => (None, None),
        };

        let inner = match build_connector(ca_bytes, cert_bytes, key_bytes)? {
            Some(connector) => Some((connector, server_name(&addr)?)),
            None => None,
        };

        Ok((addr, Tls { inner }))
    }
}
