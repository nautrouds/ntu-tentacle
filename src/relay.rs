use crate::metrics::MetricsManager;
use crate::metrics::MetricsSnapshot;
use anyhow::Context;
use anyhow::Result;
use arc_swap::ArcSwap;
use std::fs;
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

struct Generation {
    metadata: Metadata,
    pool: Arc<Semaphore>,
}

impl Generation {
    fn new(metadata: Metadata) -> Self {
        let pool = Arc::new(Semaphore::new(metadata.common.max_connections));
        Self { metadata, pool }
    }
}

pub struct Relay {
    generation: Arc<ArcSwap<Generation>>,
    metrics: Arc<MetricsManager>,
    shutdown_tx: watch::Sender<()>,
    shutdown_rx: watch::Receiver<()>,
}

impl Relay {
    pub fn new(metadata: Metadata) -> Self {
        let metrics = Arc::new(MetricsManager::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        Self {
            generation: Arc::new(ArcSwap::from_pointee(Generation::new(metadata))),
            metrics,
            shutdown_tx,
            shutdown_rx,
        }
    }

    pub fn rotate_generation(&self, metadata: Metadata) {
        let current = self.generation.load();

        if metadata.socket_path != current.metadata.socket_path {
            match fs::rename(&current.metadata.socket_path, &metadata.socket_path) {
                Ok(()) => {
                    info!(
                        old_path = ?current.metadata.socket_path,
                        new_path = ?metadata.socket_path,
                        "renamed socket for updated path"
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug!(
                        old_path = ?current.metadata.socket_path,
                        new_path = ?metadata.socket_path,
                        "no socket at old path to rename, will bind fresh at new path"
                    );
                }
                Err(e) => {
                    warn!(
                        old_path = ?current.metadata.socket_path,
                        new_path = ?metadata.socket_path,
                        error = ?e,
                        "failed to rename socket for updated path, keeping previous generation"
                    );
                    return;
                }
            }
        }

        let pool = if metadata.common.max_connections == current.metadata.common.max_connections {
            current.pool.clone()
        } else {
            Arc::new(Semaphore::new(metadata.common.max_connections))
        };

        self.generation
            .store(Arc::new(Generation { metadata, pool }));
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Waits for in-flight connections to finish instead of exit cutting them off mid-stream.
    pub async fn drain(&self) {
        let mut poll_interval = interval(Duration::from_millis(100));
        loop {
            let active = self.metrics.active_connections();
            if active == 0 {
                break;
            }
            debug!(active, "waiting for in-flight connections to drain");
            poll_interval.tick().await;
        }
    }

    fn spawn_reporter(&self) {
        let metrics = self.metrics.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let generation = self.generation.clone();

        let metadata = &self.generation.load().metadata;
        let service_name = metadata.common.service_name.clone();
        let metrics_interval_secs = metadata.common.metrics_interval_secs;
        let mut metrics_interval = interval(Duration::from_secs(metrics_interval_secs));

        tokio::spawn(async move {
            let mut snap = MetricsSnapshot::default(String::new(), service_name);
            let mut buf = Vec::new();
            let mut frame = Vec::new();
            loop {
                tokio::select! {
                    _ = metrics_interval.tick() => {
                        let current = generation.load_full();
                        snap.tentacle_id.clone_from(&current.metadata.socket_id);
                        if let Err(e) = Self::push_metrics_once(
                            &metrics,
                            &current.metadata.socket_path,
                            &mut snap,
                            &mut buf,
                            &mut frame,
                        )
                        .await
                        {
                            debug!(error = ?e, "metrics push skipped or failed, data will accumulate");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        debug!("metrics reporter stopping");
                        break;
                    }
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

    fn stop_active_listener(
        &self,
        active: &mut Option<(tokio::task::JoinHandle<()>, watch::Sender<()>)>,
    ) {
        if let Some((_handle, stop_tx)) = active.take() {
            let metadata = &self.generation.load().metadata;

            // Unlink first so new connect() attempts fail fast instead of racing the stop signal.
            listener::unlink_socket(&metadata.socket_path);

            // No abort(): dropping the handle detaches the task, letting it drain accept_loop itself.
            let _ = stop_tx.send(());
            debug!("stop signal sent, listener detached to drain");
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut check_interval = interval(Duration::from_secs(2));
        let mut active: Option<(tokio::task::JoinHandle<()>, watch::Sender<()>)> = None;
        let mut shutdown_rx = self.shutdown_rx.clone();

        self.spawn_reporter();

        {
            let metadata = &self.generation.load().metadata;
            info!(
                target = %metadata.target_addr,
                max_conns = metadata.common.max_connections,
                "relay loop initialized"
            );
        }

        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    let generation = self.generation.load_full();
                    let metadata = &generation.metadata;
                    let is_alive = probe::probe(&metadata.target_addr).await;
                    debug!(target = %metadata.target_addr, alive = is_alive, "health probe result");

                    if is_alive && active.is_none() {
                        info!(target = %metadata.target_addr, "target online, starting listener");
                        let generation_handle = self.generation.clone();
                        let metrics = self.metrics.clone();
                        let (stop_tx, stop_rx) = watch::channel(());

                        let handle = tokio::spawn(async move {
                            #[cfg(unix)]
                            if let Err(e) = listener::run(generation_handle, metrics, stop_rx).await {
                                error!(error = ?e, "uds listener failure");
                            }
                        });
                        active = Some((handle, stop_tx));
                    } else if !is_alive && active.is_some() {
                        warn!(target = %metadata.target_addr, "target offline, stopping listener");
                        self.stop_active_listener(&mut active);
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("shutdown requested, stopping relay");
                    break;
                }
            }
        }

        self.stop_active_listener(&mut active);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metadata::CommonInfo;
    use std::path::PathBuf;

    fn metadata(max_connections: usize) -> Metadata {
        Metadata {
            common: Arc::new(CommonInfo {
                max_connections,
                service_name: "test".to_string(),
                metrics_interval_secs: 1,
            }),
            socket_id: "test".to_string(),
            socket_path: PathBuf::from("/tmp/test.sock"),
            target_addr: "127.0.0.1:1".to_string(),
            target_tls: crate::tls::Tls::default(),
        }
    }

    fn metadata_with_path(max_connections: usize, socket_path: PathBuf) -> Metadata {
        Metadata {
            socket_path,
            ..metadata(max_connections)
        }
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tentacle-test-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn rotate_generation_reuses_pool_when_max_connections_unchanged() {
        let relay = Relay::new(metadata(10));
        let pool_before = relay.generation.load().pool.clone();

        relay.rotate_generation(metadata(10));
        let pool_after = relay.generation.load().pool.clone();

        assert!(Arc::ptr_eq(&pool_before, &pool_after));
    }

    #[test]
    fn rotate_generation_replaces_pool_when_max_connections_changed() {
        let relay = Relay::new(metadata(10));
        let pool_before = relay.generation.load().pool.clone();

        relay.rotate_generation(metadata(20));
        let pool_after = relay.generation.load().pool.clone();

        assert!(!Arc::ptr_eq(&pool_before, &pool_after));
        assert_eq!(pool_after.available_permits(), 20);
    }

    #[test]
    fn rotate_generation_renames_socket_when_path_changes() {
        let dir = unique_temp_dir("rename-ok");
        let old_path = dir.join("old.sock");
        let new_path = dir.join("new.sock");
        std::fs::write(&old_path, b"").unwrap();

        let relay = Relay::new(metadata_with_path(10, old_path.clone()));
        relay.rotate_generation(metadata_with_path(10, new_path.clone()));

        assert!(!old_path.exists());
        assert!(new_path.exists());
        assert_eq!(relay.generation.load().metadata.socket_path, new_path);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotate_generation_applies_when_old_path_never_created() {
        let dir = unique_temp_dir("rename-missing");
        let old_path = dir.join("old.sock");
        let new_path = dir.join("new.sock");

        let relay = Relay::new(metadata_with_path(10, old_path));
        relay.rotate_generation(metadata_with_path(10, new_path.clone()));

        assert!(!new_path.exists());
        assert_eq!(relay.generation.load().metadata.socket_path, new_path);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotate_generation_cancels_when_rename_onto_existing_dir_fails() {
        let dir = unique_temp_dir("rename-fail");
        let old_path = dir.join("old.sock");
        let new_path = dir.join("new_dir");
        std::fs::write(&old_path, b"").unwrap();
        std::fs::create_dir(&new_path).unwrap();

        let relay = Relay::new(metadata_with_path(10, old_path.clone()));
        relay.rotate_generation(metadata_with_path(10, new_path.clone()));

        assert_eq!(relay.generation.load().metadata.socket_path, old_path);
        assert!(old_path.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
