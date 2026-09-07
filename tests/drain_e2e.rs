//! End-to-end test of the PoC 1 drain shed against the real binary.
//!
//! Flow: start the proxy → connect client A → SIGUSR2 (upgrade) → assert A is
//! still served by the parent while new client B is served by the child →
//! disconnect A → assert the parent exits 0 once drained and the child keeps
//! serving.

mod common;

use std::io::{BufRead, BufReader};
use std::num::NonZeroU16;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{
    BIN, Cleanup, free_port, next_packet, require_broker, signal, v3_connect, v3_ping,
    wait_connectable, wait_exit,
};
use futures::SinkExt;
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttPacket, v3};

#[tokio::test]
async fn drain_shed_keeps_existing_sessions_and_hands_over_listener() {
    require_broker().await;

    let port = free_port();
    let listen: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
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
