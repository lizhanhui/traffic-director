//! traffic-director: MQTT reverse proxy with zero-downtime self-restart.

pub mod server;
pub mod session;

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpListener;

/// Accept loop: serve each accepted connection as an MQTT session toward
/// `broker_addr` until the listener ends.
pub async fn serve(listener: TcpListener, broker_addr: SocketAddr) -> io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        log::debug!("accepted connection from {peer}");
        tokio::spawn(async move {
            if let Err(e) = session::run_session(stream, broker_addr).await {
                log::debug!("session with {peer} ended: {e}");
            }
        });
    }
}
