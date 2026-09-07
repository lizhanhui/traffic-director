//! traffic-director: MQTT reverse proxy with zero-downtime self-restart.

pub mod migrate;
pub mod registry;
pub mod server;
pub mod session;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use registry::{FreezeBatch, SessionRegistry};

/// Accept loop: serve each accepted connection as an MQTT session toward
/// `broker_addr` until the listener ends.
pub async fn serve(listener: TcpListener, broker_addr: SocketAddr) -> io::Result<()> {
    let registry = SessionRegistry::new();
    loop {
        let (stream, peer) = listener.accept().await?;
        log::debug!("accepted connection from {peer}");
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = session::run_session(stream, broker_addr, registry).await {
                log::debug!("session with {peer} ended: {e}");
            }
        });
    }
}

/// In-process proxy handle exposing session freeze/resume — used by tests and
/// by the migrate shed.
pub struct Proxy {
    registry: Arc<SessionRegistry>,
}

impl Proxy {
    pub fn start(listener: TcpListener, broker_addr: SocketAddr) -> Self {
        let registry = SessionRegistry::new();
        let sessions = registry.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        log::debug!("accepted connection from {peer}");
                        let registry = sessions.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                session::run_session(stream, broker_addr, registry).await
                            {
                                log::debug!("session with {peer} ended: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        log::warn!("accept error: {e}");
                        break;
                    }
                }
            }
        });
        Self { registry }
    }

    /// Freeze all live sessions at a packet boundary.
    pub async fn freeze_all(&self) -> FreezeBatch {
        self.registry.freeze_all().await
    }
}
