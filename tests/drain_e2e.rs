//! End-to-end test of the PoC 1 drain shed against the real binary.
//!
//! Flow: start the proxy → connect client A → SIGUSR2 (upgrade) → assert A is
//! still served by the parent while new client B is served by the child →
//! disconnect A → assert the parent exits 0 once drained and the child keeps
//! serving.

mod common;

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::{next_packet, require_broker, v3_connect, v3_ping};
use futures::SinkExt;
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttPacket, v3};
use tokio::net::TcpStream;

const BIN: &str = env!("CARGO_BIN_EXE_traffic-director");

/// Kills any leftover traffic-director processes for this test's listen port,
/// even when the test panics.
struct Cleanup(String);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = Command::new("pkill")
            .args(["-TERM", "-f", &format!("traffic-director {}", self.0)])
            .status();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn signal(pid: u32, sig: &str) {
    let status = Command::new("kill")
        .args([sig, &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill {sig} {pid} failed");
}

async fn wait_connectable(addr: SocketAddr) {
    let deadline = Instant::now() + common::TIMEOUT;
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "proxy never started listening at {addr}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn wait_exit(child: &mut Child, within: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::test]
async fn drain_shed_keeps_existing_sessions_and_hands_over_listener() {
    require_broker().await;

    let port = free_port();
    let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let _cleanup = Cleanup(format!("--listen {listen}"));

    let mut parent = Command::new(BIN)
        .args([
            "--listen",
            &listen.to_string(),
            "--broker",
            common::BROKER,
            "--drain-timeout-secs",
            "10",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped()) // env_logger writes here; child inherits it too
        .spawn()
        .unwrap();

    // Stream the proxy's log lines so we can wait for the deterministic
    // "parent has handed over" marker instead of racing the upgrade.
    let (log_tx, log_rx) = std::sync::mpsc::channel::<String>();
    let stderr = parent.stderr.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            if log_tx.send(line.unwrap()).is_err() {
                break;
            }
        }
    });

    wait_connectable(listen).await;

    // Client A connects pre-upgrade and is pinned to the parent.
    let mut a = v3_connect(listen, "e2e-drain-a").await;
    v3_ping(&mut a).await;

    // Trigger the upgrade.
    signal(parent.id(), "-USR2");

    // Wait until the parent has handed the listener to the child: it logs
    // "ecdysis shutting down" only after the child signalled readiness and
    // the parent's accept loop stopped. Any client connecting afterwards is
    // guaranteed to be served by the child.
    let deadline = Instant::now() + common::TIMEOUT;
    loop {
        let line = log_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("parent never logged the hand-over marker");
        if line.contains("ecdysis shutting down") {
            break;
        }
    }

    // Client B connects post-handover → served by the child.
    let mut b = v3_connect(listen, "e2e-drain-b").await;

    // B is fully functional through the child.
    let publish = Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "poc1/e2e".into(),
        packet_id: NonZeroU16::new(1),
        payload: bytes::Bytes::from_static(b"via-child"),
        properties: None,
    };
    b.send(MqttPacket::V3(v3::Packet::Publish(Box::new(publish))))
        .await
        .unwrap();
    match next_packet(&mut b).await {
        MqttPacket::V3(v3::Packet::PublishAck { packet_id }) => {
            assert_eq!(packet_id.get(), 1);
        }
        other => panic!("expected PublishAck, got {other:?}"),
    }

    // The parent must still be alive, draining: it keeps serving A.
    assert!(
        parent.try_wait().unwrap().is_none(),
        "parent exited while client A was still connected"
    );
    v3_ping(&mut a).await;

    // A disconnects gracefully; the parent now has zero sessions and must
    // exit promptly — well before the 10s drain-timeout backstop.
    a.send(MqttPacket::V3(v3::Packet::Disconnect)).await.unwrap();
    drop(a);

    let status = wait_exit(&mut parent, Duration::from_secs(5))
        .expect("parent did not exit promptly after drain");
    assert!(status.success(), "parent exited with {status}");

    // The child keeps serving new clients after the parent is gone.
    let mut c = v3_connect(listen, "e2e-drain-c").await;
    v3_ping(&mut c).await;
}
