#[cfg(not(unix))]
compile_error!("This project is only supported on Unix systems.");

mod config;
mod metrics;
mod relay;
mod tracked_stream;

use anyhow::Result;
use config::Config;
use relay::metadata::{CommonInfo, Metadata};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    tracing::info!("ntu-tentacle starting");

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

    let metadatas = expand_targets(cfg);

    let handles: Vec<_> = metadatas
        .into_iter()
        .map(|metadata| {
            let target_addr = metadata.target.clone();
            tokio::spawn(async move {
                let r = relay::Relay::new(metadata);
                if let Err(e) = r.run().await {
                    tracing::error!(target = %target_addr, error = ?e, "runtime fatal error");
                }
            })
        })
        .collect();

    futures::future::join_all(handles).await;

    Ok(())
}

fn expand_targets(config: Config) -> Vec<Metadata> {
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

    for target in targets {
        let socket_id = target.replace([':', '/'], "_");
        let socket_name = format!("{}.sock", socket_id);
        let socket_path = base_dir.join(&common.service_name).join(socket_name);

        let metadata = Metadata {
            common: common.clone(),
            socket_id: socket_id.clone(),
            socket_path,
            target,
        };

        metadatas.push(metadata);
    }

    metadatas
}
