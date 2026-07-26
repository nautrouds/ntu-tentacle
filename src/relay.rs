use crate::metrics::MetricsManager;
use crate::metrics::MetricsSnapshot;
use crate::tracked_stream::TrackedStream;
use anyhow::Context;
use anyhow::Result;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::copy_bidirectional;
use tokio::net::TcpStream;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, watch};
use tokio::time::interval;
use tracing::error;
use tracing::{debug, info, warn};
pub mod metadata;

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
        let mut active_handle: Option<tokio::task::JoinHandle<()>> = None;
        let (stop_tx, _stop_rx) = watch::channel(());

        self.spawn_reporter();

        info!(
            target = %self.metadata.target,
            max_conns = self.metadata.common.max_connections,
            "relay loop initialized"
        );

        loop {
            check_interval.tick().await;

            let is_alive = self.probe().await;
            debug!(target = %self.metadata.target, alive = is_alive, "health probe result");

            if is_alive && active_handle.is_none() {
                info!(target = %self.metadata.target, "target online, starting listener");
                let metadata = self.metadata.clone();
                let pool = self.pool.clone();
                let _metrics = self.metrics.clone();
                let stop_rx = _stop_rx.clone();

                active_handle = Some(tokio::spawn(async move {
                    #[cfg(unix)]
                    if let Err(e) = run_uds_listener(metadata, pool, _metrics, stop_rx).await {
                        error!(error = ?e, "uds listener failure");
                    }
                }));
            } else if !is_alive && active_handle.is_some() {
                warn!(target = %self.metadata.target, "target offline, stopping listener");
                if let Some(handle) = active_handle.take() {
                    let _ = stop_tx.send(());
                    handle.abort();
                    debug!("listener task terminated");
                }
            }
        }
    }

    async fn probe(&self) -> bool {
        match tokio::time::timeout(
            Duration::from_secs(1),
            TcpStream::connect(&self.metadata.target),
        )
        .await
        {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                debug!(target = %self.metadata.target, error = %e, "probe connection failed");
                false
            }
            Err(_) => {
                debug!(target = %self.metadata.target, "probe timed out");
                false
            }
        }
    }
}

async fn run_uds_listener(
    metadata: Arc<Metadata>,
    pool: Arc<Semaphore>,
    metrics: Arc<MetricsManager>,
    mut stop_rx: watch::Receiver<()>,
) -> Result<()> {
    let socket_path = metadata.socket_path.clone();
    let temp_socket_path = socket_path.with_extension("tmp");

    debug!(path = ?socket_path, "binding unix domain socket");

    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent()
        && !parent.exists()
    {
        info!(dir = ?parent, "creating service directory");
        fs::create_dir_all(parent)?;
    }

    // Cleanup old sockets
    let _ = fs::remove_file(&temp_socket_path);
    let _ = fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&temp_socket_path).context("Failed to bind UDS")?;

    // Set permissions (equivalent to chmod 0666)
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp_socket_path, fs::Permissions::from_mode(0o666))?;
    }

    fs::rename(&temp_socket_path, &socket_path).context("Failed to rename UDS")?;

    info!(path = ?socket_path, "uds listener active");

    struct CleanupOnDrop {
        path: std::path::PathBuf,
    }
    impl Drop for CleanupOnDrop {
        fn drop(&mut self) {
            info!(path = ?self.path, "cleaning up socket file");
            let _ = fs::remove_file(&self.path);
        }
    }
    let _cleanup = CleanupOnDrop {
        path: socket_path.clone(),
    };

    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((uds_stream, addr)) => {
                        let pool = pool.clone();
                        let target_addr = metadata.target.clone();
                        let metrics = metrics.clone();

                        // Acquire permit from connection pool
                        // If pool is full, this will wait (queue) until a spot opens
                        let permit = match pool.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                error!("connection pool semaphore exhausted");
                                break;
                            }
                        };

                        debug!(
                            client = ?addr,
                            active_conns = metadata.common.max_connections - pool.available_permits(),
                            "connection accepted"
                        );

                        tokio::spawn(async move {
                            // Move permit into spawn to keep it alive for the duration of the connection
                            let _permit = permit;
                            let start = std::time::Instant::now();
                            metrics.add_active_connection();
                            metrics.add_attempts_total();

                            match TcpStream::connect(&target_addr).await {
                                Ok(mut tcp_stream) => {
                                    let mut tracked_uds_stream = TrackedStream::new(uds_stream, metrics.clone());

                                    debug!(target = %target_addr, "relaying stream");
                                    let latency_us = start.elapsed().as_micros() as u64;
                                    MetricsManager::observe_duration(&metrics.transport_latency_seconds, latency_us);

                                    if let Err(e) = copy_bidirectional(&mut tracked_uds_stream, &mut tcp_stream).await {
                                        debug!(error = ?e, "connection closed");
                                    }

                                }
                                Err(e) => {
                                    error!(target = %target_addr, error = ?e, "upstream connection failed");
                                    metrics.add_failures_total();
                                },
                            }
                            // Permit is automatically dropped here, returning to pool
                            metrics.remove_active_connection();
                        });
                    }
                    Err(e) => {
                        error!(error = ?e, "uds accept failure");
                        break;
                    }
                }
            }
            _ = stop_rx.changed() => {
                info!("shutdown signal received, stopping uds listener");
                break;
            }
        }
    }

    Ok(())
}
