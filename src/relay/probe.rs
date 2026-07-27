use std::time::Duration;
use tokio::net::TcpStream;
use tracing::debug;

const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) async fn probe(target_addr: &str) -> bool {
    match tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect(target_addr)).await {
        Ok(Ok(_)) => true,
        Ok(Err(e)) => {
            debug!(target = %target_addr, error = %e, "probe connection failed");
            false
        }
        Err(_) => {
            debug!(target = %target_addr, "probe timed out");
            false
        }
    }
}
