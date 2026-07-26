#[cfg(not(unix))]
compile_error!("This project is only supported on Unix systems.");

mod config;
mod metrics;
mod relay;
mod tracked_stream;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    tracing::info!("ntu-tentacle starting");

    let cfg = match config::env::load() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = ?e, "initialization failed: configuration error");
            return Err(e);
        }
    };

    if let Some(base_dir) = cfg.first().map(|c| c.base_dir.clone())
        && let Err(e) = std::fs::create_dir_all(&base_dir)
    {
        tracing::error!(error = ?e, path = ?base_dir, "failed to create base directory");
        return Err(e.into());
    }

    let handles: Vec<_> = cfg
        .into_iter()
        .map(|c| {
            let target_addr = c.target_addr.clone();
            tokio::spawn(async move {
                let r = relay::Relay::new(c);
                if let Err(e) = r.run().await {
                    tracing::error!(target = %target_addr, error = ?e, "runtime fatal error");
                }
            })
        })
        .collect();

    futures::future::join_all(handles).await;

    Ok(())
}
