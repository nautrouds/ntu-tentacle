use crate::metrics::MetricsManager;
use crate::metrics::MetricsSnapshot;
use anyhow::Context;
use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::{Semaphore, watch};
use tokio::time::interval;
use tracing::error;
use tracing::{debug, info, warn};
mod listener;
pub mod metadata;
mod probe;

use metadata::Metadata;

pub struct Relay {
    metadata: Arc<Metadata>,
    pool: Arc<Semaphore>,
    metrics: Arc<MetricsManager>,
}

impl Relay {
    pub fn new(metadata: Metadata) -> Self {
        let max_conn = metadata.common.max_connections;
        let metrics = Arc::new(MetricsManager::new());
        Self {
            metadata: Arc::new(metadata),
            pool: Arc::new(Semaphore::new(max_conn)),
            metrics,
        }
    }

    fn spawn_reporter(&self) {
        let metrics = self.metrics.clone();

        let socket_id = self.metadata.socket_id.clone();
        let socket_path = self.metadata.socket_path.clone();
        let service_name = self.metadata.common.service_name.clone();
        let metrics_interval_secs = self.metadata.common.metrics_interval_secs;
        let mut metrics_interval = interval(Duration::from_secs(metrics_interval_secs));

        tokio::spawn(async move {
            let mut snap = MetricsSnapshot::default(socket_id, service_name);
            let mut buf = Vec::new();
            let mut frame = Vec::new();
            loop {
                metrics_interval.tick().await;

                if let Err(e) =
                    Self::push_metrics_once(&metrics, &socket_path, &mut snap, &mut buf, &mut frame)
                        .await
                {
                    debug!(error = ?e, "metrics push skipped or failed, data will accumulate");
                }
            }
        });
    }

    async fn push_metrics_once(
        metrics: &Arc<MetricsManager>,
        path: &Path,
        snap: &mut MetricsSnapshot,
        buf: &mut Vec<u8>,
        frame: &mut Vec<u8>,
    ) -> Result<()> {
        metrics.take_snapshot(snap);
        MetricsManager::encode_to_binary(snap, buf, frame);

        match UnixStream::connect(path).await {
            Ok(mut stream) => {
                use tokio::io::AsyncWriteExt;
                stream
                    .write_all(frame)
                    .await
                    .context("failed to write to metrics socket")?;

                metrics.commit_sent_metrics(snap);
                debug!("metrics successfully pushed to nautrouds");
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!("metrics socket unavailable: {}", e)),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut check_interval = interval(Duration::from_secs(2));
        let mut active: Option<(tokio::task::JoinHandle<()>, watch::Sender<()>)> = None;

        self.spawn_reporter();

        info!(
            target = %self.metadata.target_addr,
            max_conns = self.metadata.common.max_connections,
            "relay loop initialized"
        );

        loop {
            check_interval.tick().await;

            let is_alive = probe::probe(&self.metadata.target_addr).await;
            debug!(target = %self.metadata.target_addr, alive = is_alive, "health probe result");

            if is_alive && active.is_none() {
                info!(target = %self.metadata.target_addr, "target online, starting listener");
                let metadata = self.metadata.clone();
                let pool = self.pool.clone();
                let metrics = self.metrics.clone();
                let (stop_tx, stop_rx) = watch::channel(());

                let handle = tokio::spawn(async move {
                    #[cfg(unix)]
                    if let Err(e) = listener::run(metadata, pool, metrics, stop_rx).await {
                        error!(error = ?e, "uds listener failure");
                    }
                });
                active = Some((handle, stop_tx));
            } else if !is_alive && active.is_some() {
                warn!(target = %self.metadata.target_addr, "target offline, stopping listener");
                if let Some((_handle, stop_tx)) = active.take() {
                    // Unlink first so new connect() attempts fail fast instead of racing the stop signal.
                    listener::unlink_socket(&self.metadata.socket_path);

                    // No abort(): dropping the handle detaches the task, letting it drain accept_loop itself.
                    let _ = stop_tx.send(());
                    debug!("stop signal sent, listener detached to drain");
                }
            }
        }
    }
}

