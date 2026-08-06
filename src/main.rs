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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tls::TlsManager;
use tokio::task::JoinSet;

const DEFAULT_PID_DIR: &str = "/usr/local/tentacle";

fn pid_dir() -> String {
    config::env::pid_dir_from_env().unwrap_or_else(|| DEFAULT_PID_DIR.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("-h") | Some("--help") => {
            print_help();
            return Ok(());
        }
        Some("-V") | Some("--version") => {
            println!("tentacle {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some("-r") | Some("--reload") => {
            let explicit_path = match args.get(2).map(String::as_str) {
                Some(s) if !s.starts_with('-') => Some(PathBuf::from(s)),
                _ => None,
            };
            let path = explicit_path
                .or_else(|| config::env::service_name_from_env().map(|name| pid_file_path(&name)));

            let path = match path {
                Some(p) => p,
                None => {
                    eprintln!(
                        "error: no pid file path given and no service name found in environment to derive one"
                    );
                    print_help();
                    std::process::exit(1);
                }
            };

            std::process::exit(reload(&path));
        }
        Some(unknown) => {
            eprintln!("error: unknown option '{unknown}'");
            print_help();
            std::process::exit(1);
        }
        None => {}
    }

    run_daemon().await
}

fn print_help() {
    println!(
        "tentacle {version}\n\
Usage:\n\
  tentacle                     Run the relay daemon (configured via environment variables)\n\
  tentacle -r, --reload [PATH] Send SIGHUP to the daemon whose pid file is at PATH\n\
                                (defaults to the current service's pid file, derived from\n\
                                the environment, if PATH is omitted)\n\
  tentacle -h, --help          Print this help message\n\
  tentacle -V, --version       Print version information",
        version = env!("CARGO_PKG_VERSION")
    );
}

fn pid_file_path(service_name: &str) -> PathBuf {
    Path::new(&pid_dir()).join(format!("{service_name}.pid"))
}

fn write_pid_file(service_name: &str) {
    let dir = pid_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        tracing::warn!(error = ?e, path = dir, "failed to create pid file directory");
        return;
    }

    let path = pid_file_path(service_name);
    if let Err(e) = std::fs::write(&path, std::process::id().to_string()) {
        tracing::warn!(error = ?e, path = ?path, "failed to write pid file");
    }
}

fn remove_pid_file_if_owned(service_name: &str) {
    let path = pid_file_path(service_name);
    let owned = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .is_some_and(|pid| pid == std::process::id());

    if owned {
        let _ = std::fs::remove_file(&path);
    }
}

fn reload(path: &Path) -> i32 {
    let pid_str = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to read pid file {}: {e}", path.display());
            return 1;
        }
    };

    let pid: i32 = match pid_str.trim().parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("error: invalid pid in {}: {pid_str:?}", path.display());
            return 1;
        }
    };

    // SAFETY: kill(2) with a valid pid and SIGHUP has no memory-safety implications.
    let ret = unsafe { libc::kill(pid, libc::SIGHUP) };
    if ret == 0 {
        println!(
            "sent SIGHUP to tentacle (pid {pid}, pid file {})",
            path.display()
        );
        0
    } else {
        let err = std::io::Error::last_os_error();
        eprintln!("error: failed to signal pid {pid}: {err}");
        1
    }
}

async fn run_daemon() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    tracing::info!("tentacle starting");

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

        while tasks.try_join_next().is_some() {}

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

    for relay in relays.values() {
        relay.drain().await;
    }

    if let Some(service_name) = current.first().map(|m| m.common.service_name.clone()) {
        remove_pid_file_if_owned(&service_name);
    }

    tracing::info!("tentacle stopped");

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

    write_pid_file(&cfg.service_name);

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

        if !target.weight_in_range() {
            tracing::warn!(
                target = %target.addr,
                weight = target.weight,
                max = config::Target::MAX_WEIGHT,
                "weight out of range [1, max], ignoring and falling back to default weight"
            );
        }

        let socket_id = target.addr.replace([':', '/'], "_");
        let socket_name = match target.weight.filter(|_| target.weight_in_range()) {
            Some(weight) if weight > 1 => format!("{socket_id}@{weight}.sock"),
            _ => format!("{socket_id}.sock"),
        };
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
