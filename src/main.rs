#[cfg(not(unix))]
compile_error!("This project is only supported on Unix systems.");

mod config;
mod metrics;
mod relay;
mod tls;
mod tracked_stream;

use anyhow::Result;
use config::Config;
use relay::metadata::{CommonInfo, Metadata};
use std::collections::HashMap;
use std::sync::Arc;
use tls::TlsManager;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    tracing::info!("ntu-tentacle starting");

    let mut relays: HashMap<String, Arc<relay::Relay>> = HashMap::new();
    let mut tasks: JoinSet<()> = JoinSet::new();
    let mut current: Vec<Metadata> = Vec::new();
    let mut bootstrapped = false;

    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    let mut sighup = signal(SignalKind::hangup()).expect("failed to install SIGHUP handler");

    loop {
        match load_metadatas().await {
            Ok(metadatas) => {
                for stale in Metadata::missing_targets(&current, &metadatas) {
                    if let Some(r) = relays.remove(&stale.target_addr) {
                        r.shutdown();
                    }
                }

                for metadata in metadatas.iter().cloned() {
                    let target_addr = metadata.target_addr.clone();
                    match relays.get(&target_addr) {
                        Some(existing) => existing.rotate_generation(metadata),
                        None => {
                            let r = Arc::new(relay::Relay::new(metadata));
                            relays.insert(target_addr.clone(), r.clone());
                            tasks.spawn(async move {
                                if let Err(e) = r.run().await {
                                    tracing::error!(target = %target_addr, error = ?e, "runtime fatal error");
                                }
                            });
                        }
                    }
                }

                current = metadatas;
                bootstrapped = true;
            }
            Err(e) if !bootstrapped => return Err(e),
            Err(e) => {
                tracing::error!(error = ?e, "reload failed, keeping current configuration");
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received SIGINT, shutting down");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, shutting down");
                break;
            }
            _ = sighup.recv() => {
                tracing::info!("received SIGHUP, reloading configuration");
            }
        }
    }

    for relay in relays.values() {
        relay.shutdown();
    }

    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            tracing::error!(error = ?e, "relay task panicked");
        }
    }

    tracing::info!("ntu-tentacle stopped");

    Ok(())
}

async fn load_metadatas() -> Result<Vec<Metadata>> {
    let cfg = match config::load() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = ?e, "initialization failed: configuration error");
            return Err(e);
        }
    };

    let base_dir = cfg.base_dir.clone();
    if let Err(e) = std::fs::create_dir_all(&base_dir) {
        tracing::error!(error = ?e, path = ?base_dir, "failed to create base directory");
        return Err(e.into());
    }

    expand_targets(cfg).await
}

async fn expand_targets(config: Config) -> Result<Vec<Metadata>> {
    let Config {
        service_name,
        targets,
        base_dir,
        max_connections,
        metrics_interval_secs,
    } = config;

    let common = Arc::new(CommonInfo {
        max_connections,
        service_name,
        metrics_interval_secs,
    });

    let mut metadatas: Vec<Metadata> = Vec::new();
    let tls_manager = TlsManager::default();

    for target in targets {
        if !target.tls_pair_is_valid() {
            tracing::warn!(
                target = %target.addr,
                "skipping target: TLS cert and key must both be set or both omitted"
            );
            continue;
        }

        let socket_id = target.addr.replace([':', '/'], "_");
        let socket_name = format!("{}.sock", socket_id);
        let socket_path = base_dir.join(&common.service_name).join(socket_name);

        let (target_addr, target_tls) = tls_manager.fetch(target).await?;

        let metadata = Metadata {
            common: common.clone(),
            socket_id: socket_id.clone(),
            socket_path,
            target_addr,
            target_tls,
        };

        metadatas.push(metadata);
    }

    Ok(metadatas)
}
