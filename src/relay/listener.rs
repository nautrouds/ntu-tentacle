use crate::metrics::MetricsManager;
use crate::tls::Tls;
use crate::tracked_stream::TrackedStream;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, copy_bidirectional};
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, watch};
use tracing::{debug, error, info};

use super::Generation;

pub(super) async fn run(
    generation: Arc<ArcSwap<Generation>>,
    metrics: Arc<MetricsManager>,
    stop_rx: watch::Receiver<()>,
) -> Result<()> {
    let metadata = &generation.load().metadata;
    let (listener, _guard) = bind_socket(&metadata.socket_path)?;
    accept_loop(listener, generation, metrics, stop_rx).await
}

struct SocketGuard {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

impl SocketGuard {
    fn new(path: PathBuf) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let meta = fs::metadata(&path).context("failed to stat freshly bound UDS")?;
        Ok(Self {
            path,
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }

    fn owns_current_file(&self) -> bool {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(&self.path)
            .map(|m| m.dev() == self.dev && m.ino() == self.ino)
            .unwrap_or(false)
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if self.owns_current_file() {
            info!(path = ?self.path, "cleaning up socket file");
            unlink_socket(&self.path);
        } else {
            debug!(path = ?self.path, "socket no longer belongs to this listener, skip cleanup");
        }
    }
}

/// Best-effort unlink a UDS path; "already gone" is success, not failure.
pub(super) fn unlink_socket(path: &Path) {
    let _ = fs::remove_file(path);
}

fn bind_socket(socket_path: &Path) -> Result<(UnixListener, SocketGuard)> {
    let temp_socket_path = socket_path.with_extension("tmp");

    debug!(path = ?socket_path, "binding unix domain socket");

    if let Some(parent) = socket_path.parent()
        && !parent.exists()
    {
        info!(dir = ?parent, "creating service directory");
        fs::create_dir_all(parent)?;
    }

    unlink_socket(&temp_socket_path);
    unlink_socket(socket_path);

    let listener = UnixListener::bind(&temp_socket_path).context("Failed to bind UDS")?;

    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp_socket_path, fs::Permissions::from_mode(0o666))?;
    }

    fs::rename(&temp_socket_path, socket_path).context("Failed to rename UDS")?;

    info!(path = ?socket_path, "uds listener active");

    let guard = SocketGuard::new(socket_path.to_path_buf())?;
    Ok((listener, guard))
}

async fn accept_loop(
    listener: UnixListener,
    generation: Arc<ArcSwap<Generation>>,
    metrics: Arc<MetricsManager>,
    mut stop_rx: watch::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((uds_stream, addr)) => {
                        let snapshot = generation.load_full();
                        let pool = snapshot.pool.clone();
                        let metadata = &snapshot.metadata;
                        let metrics = metrics.clone();
                        let target_addr = metadata.target_addr.clone();
                        let target_tls = metadata.target_tls.clone();
                        let max_connections = metadata.common.max_connections;

                        // Acquired in the spawned task, not here, so a full pool can't stall accept_loop.
                        tokio::spawn(async move {
                            let permit = match pool.clone().acquire_owned().await {
                                Ok(p) => p,
                                Err(_) => {
                                    error!("connection pool semaphore closed");
                                    return;
                                }
                            };

                            debug!(
                                client = ?addr,
                                active_conns = max_connections - pool.available_permits(),
                                "connection accepted"
                            );

                            handle_connection(uds_stream, permit, metrics, target_addr, target_tls)
                                .await;
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

async fn handle_connection(
    uds_stream: UnixStream,
    permit: OwnedSemaphorePermit,
    metrics: Arc<MetricsManager>,
    target_addr: String,
    target_tls: Tls,
) {
    // Keep the permit alive for the connection's duration.
    let _permit = permit;
    let start = Instant::now();
    metrics.add_active_connection();
    metrics.add_attempts_total();

    match TcpStream::connect(&target_addr).await {
        Ok(tcp_stream) => {
            // Nagle's algorithm otherwise adds up to ~40ms per small gRPC frame.
            if let Err(e) = tcp_stream.set_nodelay(true) {
                debug!(target = %target_addr, error = ?e, "failed to set TCP_NODELAY");
            }

            if target_tls.is_enabled() {
                match target_tls.connect(tcp_stream).await {
                    Ok(tls_stream) => {
                        debug!(target = %target_addr, "relaying stream (tls)");
                        report_latency(&metrics, start);
                        relay(uds_stream, tls_stream, &metrics).await;
                    }
                    Err(e) => {
                        error!(target = %target_addr, error = ?e, "upstream TLS handshake failed");
                        metrics.add_failures_total();
                    }
                }
            } else {
                debug!(target = %target_addr, "relaying stream");
                report_latency(&metrics, start);
                relay(uds_stream, tcp_stream, &metrics).await;
            }
        }
        Err(e) => {
            error!(target = %target_addr, error = ?e, "upstream connection failed");
            metrics.add_failures_total();
        }
    }

    // Permit drops here, returning to the pool.
    metrics.remove_active_connection();
}

fn report_latency(metrics: &Arc<MetricsManager>, start: Instant) {
    let latency_us = start.elapsed().as_micros() as u64;
    MetricsManager::observe_duration(&metrics.transport_latency_seconds, latency_us);
}

async fn relay<S>(uds_stream: UnixStream, mut upstream: S, metrics: &Arc<MetricsManager>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut tracked_uds_stream = TrackedStream::new(uds_stream, metrics.clone());

    if let Err(e) = copy_bidirectional(&mut tracked_uds_stream, &mut upstream).await {
        debug!(error = ?e, "connection closed");
    }
}
