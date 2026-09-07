//! Server lifecycle: ecdysis-managed listener, accept loop, and the two shed
//! modes — drain (PoC 1) and migrate (PoC 2).
//!
//! We drive the base [`Ecdysis`] API directly rather than `TokioEcdysis`
//! because migration needs a pre-upgrade hook: sessions must be frozen and
//! snapshotted *before* the child is spawned.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ecdysis::Ecdysis;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, spawn_blocking};

use crate::migrate;
use crate::registry::SessionRegistry;
use crate::session;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShedMode {
    /// Parent keeps serving existing sessions until they finish; the child
    /// takes over only the listener.
    Drain,
    /// Parent freezes sessions, hands client sockets + snapshots to the
    /// child, and exits immediately after the handoff.
    Migrate,
}

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
    tracker: std::sync::Weak<DrainTracker>,
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

fn spawn_acceptor(
    listener: Arc<TcpListener>,
    broker: SocketAddr,
    registry: Arc<SessionRegistry>,
    tracker: Arc<DrainTracker>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    log::debug!("accepted connection from {peer}");
                    let guard = tracker.track();
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        let _guard = guard;
                        if let Err(e) = session::run_session(stream, broker, registry).await {
                            log::debug!("session with {peer} ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    log::warn!("accept error: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
}

/// Run the proxy until SIGTERM/SIGINT (stop) or SIGUSR2 (shed). Returns when
/// this process generation is done — after draining (drain mode / stop) or
/// right after a successful session handoff (migrate mode).
pub async fn run(
    listen: SocketAddr,
    broker: SocketAddr,
    drain_timeout: Duration,
    mode: ShedMode,
) -> io::Result<()> {
    let mut ecdysis = Ecdysis::new();
    let std_listener = ecdysis.listen_tcp(listen)?;
    std_listener.set_nonblocking(true)?;
    let listener = Arc::new(TcpListener::from_std(std_listener)?);

    let registry = SessionRegistry::new();
    let tracker = DrainTracker::new();

    // The datagram pair is registered with ecdysis, so the child inherits its
    // end; the parent keeps the other to push state after spawning.
    let (from_parent, to_child) = ecdysis.unix_datagram_pair("td-state".into());
    let to_child = to_child?;
    migrate::parent_channel(&to_child)?;

    let is_child = ecdysis.is_child();
    ecdysis.ready()?;
    log::info!("listening on {listen}, proxying to {broker} (mode: {mode:?}, child: {is_child})");

    // Child in migrate mode: adopt the previous generation's sessions.
    if is_child
        && mode == ShedMode::Migrate
        && let Some(from_parent) = from_parent
    {
        migrate::child_channel(&from_parent)?;
        let registry = registry.clone();
        tokio::spawn(async move {
            let sessions = match spawn_blocking(move || migrate::recv_sessions(&from_parent)).await
            {
                Ok(Ok(sessions)) => sessions,
                Ok(Err(e)) => {
                    log::error!("failed to receive migrated sessions: {e}");
                    return;
                }
                Err(e) => {
                    log::error!("migration receiver panicked: {e}");
                    return;
                }
            };
            log::info!("adopting {} migrated session(s)", sessions.len());
            for (snapshot, fd) in sessions {
                let registry = registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = session::adopt_session(fd, snapshot, broker, registry).await {
                        log::warn!("failed to adopt session: {e}");
                    }
                });
            }
        });
    }

    let ecdysis = Arc::new(ecdysis);
    let mut acceptor = spawn_acceptor(listener.clone(), broker, registry.clone(), tracker.clone());

    let mut sig_upgrade = signal(SignalKind::user_defined2())?;
    let mut sig_term = signal(SignalKind::terminate())?;
    let mut sig_int = signal(SignalKind::interrupt())?;

    loop {
        tokio::select! {
            _ = sig_upgrade.recv() => {
                match mode {
                    ShedMode::Drain => {
                        let ec = ecdysis.clone();
                        let result = tokio::task::spawn_blocking(move || ec.upgrade())
                            .await
                            .map_err(|e| io::Error::other(format!("upgrade join: {e}")))?;
                        match result {
                            Ok(()) => {
                                log::info!("ecdysis shutting down: Upgrade (reason: Signal(SIGUSR2))");
                                acceptor.abort();
                                ecdysis.quit();
                                break;
                            }
                            Err(e) => {
                                log::warn!("upgrade failed, staying alive: {e}");
                            }
                        }
                    }
                    ShedMode::Migrate => {
                        // Stop accepting first: sessions accepted after the
                        // freeze couldn't be migrated anyway. The listen
                        // socket stays open, so connecting clients queue in
                        // the kernel backlog until the child accepts them.
                        acceptor.abort();
                        let batch = registry.freeze_all().await;
                        log::info!("froze {} session(s) for migration", batch.frozen.len());

                        let ec = ecdysis.clone();
                        let result = tokio::task::spawn_blocking(move || ec.upgrade())
                            .await
                            .map_err(|e| io::Error::other(format!("upgrade join: {e}")))?;
                        match result {
                            Ok(()) => {
                                let mut sent = 0usize;
                                let handoff = migrate::send_sessions(&to_child, &batch.frozen, &mut sent);
                                match handoff {
                                    Ok(()) => {
                                        log::info!(
                                            "ecdysis shutting down: Upgrade (reason: Signal(SIGUSR2)) — handed over {sent} session(s)"
                                        );
                                        ecdysis.quit();
                                        batch.finish();
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "handoff failed after {sent} session(s): {e} — draining the rest"
                                        );
                                        // Sessions already sent are owned by
                                        // the child; resume the rest and
                                        // drain them here.
                                        batch.resume_skipping(sent);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!("upgrade failed, rolling back freeze: {e}");
                                batch.resume_all();
                                acceptor = spawn_acceptor(
                                    listener.clone(),
                                    broker,
                                    registry.clone(),
                                    tracker.clone(),
                                );
                            }
                        }
                    }
                }
            }
            _ = sig_term.recv() => {
                log::info!("ecdysis shutting down: FullStop (reason: SIGTERM)");
                break;
            }
            _ = sig_int.recv() => {
                log::info!("ecdysis shutting down: FullStop (reason: SIGINT)");
                break;
            }
        }
    }

    acceptor.abort();
    ecdysis.quit();
    if !tracker.wait_drained(drain_timeout).await {
        log::warn!("drain timeout expired, exiting with sessions still active");
    }
    Ok(())
}
