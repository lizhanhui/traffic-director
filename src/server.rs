//! Server lifecycle: ecdysis-managed listener, accept loop, and drain
//! tracking for zero-downtime restarts (PoC 1: drain shed).

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ecdysis::tokio_ecdysis::{SignalKind, StopOnShutdown, TokioEcdysisBuilder};
use futures::StreamExt;
use tokio::sync::Notify;

use crate::session;

/// Counts live sessions so the process can exit once it has fully drained
/// after a shed. `Notify` wakes the waiter as soon as the count hits zero.
pub struct DrainTracker {
    sessions: AtomicUsize,
    drained: Notify,
}

impl DrainTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sessions: AtomicUsize::new(0),
            drained: Notify::new(),
        })
    }

    pub fn track(self: &Arc<Self>) -> SessionGuard {
        self.sessions.fetch_add(1, Ordering::SeqCst);
        SessionGuard {
            tracker: Arc::downgrade(self),
        }
    }

    /// Resolves true once the session count reaches zero; false on timeout.
    pub async fn wait_drained(&self, timeout: Duration) -> bool {
        let wait = async {
            loop {
                if self.sessions.load(Ordering::SeqCst) == 0 {
                    return;
                }
                self.drained.notified().await;
            }
        };
        tokio::time::timeout(timeout, wait).await.is_ok()
    }
}

pub struct SessionGuard {
    tracker: Weak<DrainTracker>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if let Some(tracker) = self.tracker.upgrade()
            && tracker.sessions.fetch_sub(1, Ordering::SeqCst) == 1
        {
            tracker.drained.notify_waiters();
        }
    }
}

/// Run the proxy until an upgrade (SIGUSR2) or stop (SIGTERM/SIGINT) signal
/// arrives, then drain and exit.
///
/// On upgrade, ecdysis spawns the child (which inherits the listen socket),
/// the accept stream ends, and this function returns after all pre-existing
/// sessions drain or `drain_timeout` expires.
pub async fn run(listen: SocketAddr, broker: SocketAddr, drain_timeout: Duration) -> io::Result<()> {
    let mut builder = TokioEcdysisBuilder::new(SignalKind::user_defined2())?;
    builder.stop_on_signal(SignalKind::terminate())?;
    builder.stop_on_signal(SignalKind::interrupt())?;

    let mut incoming = builder.listen_tcp(StopOnShutdown::Yes, listen)?;
    let (_ecdysis, upgrader) = builder.ready()?;
    log::info!("listening on {listen}, proxying to {broker}");

    let tracker = DrainTracker::new();
    let accept_tracker = tracker.clone();
    let acceptor = tokio::spawn(async move {
        while let Some(conn) = incoming.next().await {
            match conn {
                Ok(stream) => {
                    let guard = accept_tracker.track();
                    tokio::spawn(async move {
                        let _guard = guard;
                        if let Err(e) = session::run_session(stream, broker).await {
                            log::debug!("session ended: {e}");
                        }
                    });
                }
                Err(e) => log::warn!("accept error: {e}"),
            }
        }
    });

    let (mode, reason) = upgrader.await.map_err(io::Error::other)?;
    log::info!("ecdysis shutting down: {mode:?} (reason: {reason:?})");

    // Listeners are stopped now; existing sessions keep running until they
    // finish naturally (drain) or the timeout forces an exit.
    if !tracker.wait_drained(drain_timeout).await {
        log::warn!("drain timeout expired, exiting with sessions still active");
    }
    acceptor.abort();
    Ok(())
}
