//! Session registry: tracks live sessions so a shed can freeze them, collect
//! their snapshots, and either resume them (upgrade failed) or let them die
//! with the process after a successful handoff.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::window::WindowEntry;

/// Everything the next process generation needs to adopt a session.
/// `client_fd` travels out-of-band (SCM_RIGHTS) during a real shed.
#[derive(Debug)]
pub struct FrozenSession {
    pub id: u64,
    pub snapshot: SessionSnapshot,
    pub client_fd: RawFd,
}

/// One subscription the client made, retained for re-SUBSCRIBE on thaw.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEntry {
    pub topic_filter: String,
    /// SUBSCRIBE options byte: QoS in bits 0-1; v5 adds no_local (bit 2),
    /// retain-as-published (bit 3), retain-handling (bits 4-5).
    pub options: u8,
}

/// Serializable per-session state at a packet boundary.
/// `version` is the MQTT protocol level (4 = v3.1.1, 5 = v5.0).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub version: u8,
    /// Raw re-encoded CONNECT packet, replayed toward the broker on thaw
    /// (with the clean bit cleared so the broker resumes the session).
    pub connect_raw: Vec<u8>,
    /// Undelivered bytes from the client, still frame-aligned.
    pub client_buf: Vec<u8>,
    /// Undelivered bytes from the broker, still frame-aligned.
    pub broker_buf: Vec<u8>,
    /// QoS1/2 packets forwarded but not yet acknowledged end-to-end.
    pub windows: Vec<WindowEntry>,
    /// Active subscriptions, re-issued toward the broker on thaw.
    pub subscriptions: Vec<SubscriptionEntry>,
}

pub enum SessionControl {
    Freeze(FreezeRequest),
}

pub struct FreezeRequest {
    pub reply: oneshot::Sender<FrozenSession>,
    pub resume: oneshot::Receiver<ResumeAction>,
}

/// Returned to the session on rollback so it can restore its buffers.
pub enum ResumeAction {
    Resume(Box<FrozenSession>),
}

const FREEZE_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// The result of freezing all sessions: the collected snapshots plus the
/// channels needed to resume or terminate the frozen sessions.
pub struct FreezeBatch {
    pub frozen: Vec<FrozenSession>,
    resumes: HashMap<u64, oneshot::Sender<ResumeAction>>,
}

impl FreezeBatch {
    /// Roll back the freeze: each session gets its snapshot buffers back and
    /// resumes normal operation. Used when an upgrade fails.
    pub fn resume_all(self) {
        self.resume_skipping(0);
    }

    /// Resume all sessions except the first `sent`, which were already handed
    /// to the child (their resume channels are dropped so they terminate).
    pub fn resume_skipping(self, sent: usize) {
        let mut resumes = self.resumes;
        for session in self.frozen.into_iter().skip(sent) {
            if let Some(tx) = resumes.remove(&session.id) {
                let _ = tx.send(ResumeAction::Resume(Box::new(session)));
            }
        }
    }

    /// Drop the resume channels without resuming; frozen sessions observe the
    /// closed channel and terminate. Used right before process exit after a
    /// successful handoff.
    pub fn finish(self) {}
}

#[derive(Default)]
pub struct SessionRegistry {
    sessions: Mutex<HashMap<u64, mpsc::Sender<SessionControl>>>,
    next_id: AtomicU64,
}

impl SessionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self) -> (u64, mpsc::Receiver<SessionControl>) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(1);
        self.sessions.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    pub fn unregister(&self, id: u64) {
        self.sessions.lock().unwrap().remove(&id);
    }

    pub fn count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    /// Freeze every live session at a packet boundary and collect snapshots.
    /// Sessions that die mid-freeze or fail to freeze in time are skipped.
    pub async fn freeze_all(&self) -> FreezeBatch {
        let senders: Vec<(u64, mpsc::Sender<SessionControl>)> = {
            let sessions = self.sessions.lock().unwrap();
            sessions.iter().map(|(id, tx)| (*id, tx.clone())).collect()
        };

        let mut frozen = Vec::with_capacity(senders.len());
        let mut resumes = HashMap::with_capacity(senders.len());
        for (id, tx) in senders {
            let (reply_tx, reply_rx) = oneshot::channel();
            let (resume_tx, resume_rx) = oneshot::channel();
            let request = FreezeRequest {
                reply: reply_tx,
                resume: resume_rx,
            };
            if tx.send(SessionControl::Freeze(request)).await.is_err() {
                continue; // session ended concurrently
            }
            match tokio::time::timeout(FREEZE_REPLY_TIMEOUT, reply_rx).await {
                Ok(Ok(session)) => {
                    frozen.push(session);
                    resumes.insert(id, resume_tx);
                }
                Ok(Err(_)) => {} // session died while freezing
                Err(_) => log::warn!("session {id} did not freeze in time, skipping"),
            }
        }
        FreezeBatch { frozen, resumes }
    }
}
