//! Broker-down-during-migration: the child retries the broker connection
//! with backoff while the client socket stays alive — locally acking and
//! buffering QoS publishes — and flushes the buffer once the broker returns.
//!
//! Uses a dedicated mosquitto container owned by this test (the shared
//! mosquitto1/2 containers are left untouched).

mod common;

use std::num::NonZeroU16;
use std::process::Command;
use std::time::Duration;

use common::{
    BIN, Cleanup, free_port, next_packet, signal, v3_connect, v3_ping, wait_connectable, wait_exit,
};
use futures::SinkExt;
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttPacket, v3};

fn docker(args: &[&str]) {
    let status = Command::new("docker")
        .args(args)
        .status()
        .expect("failed to run docker — is it available?");
    assert!(status.success(), "docker {args:?} failed");
}

struct BrokerContainer(String);

impl BrokerContainer {
    fn start_fresh(port: u16) -> Self {
        let name = format!("td-broker-down-{port}");
        docker(&[
            "run", "-d", "--name", &name, "-p", &format!("127.0.0.1:{port}:1883"),
            "eclipse-mosquitto:latest",
        ]);
        Self(name)
    }

    fn kill(&self) {
        docker(&["kill", &self.0]);
    }

    fn start(&self) {
        docker(&["start", &self.0]);
    }
}

impl Drop for BrokerContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.0])
            .status();
    }
}

#[tokio::test]
async fn broker_down_during_migration_buffers_and_recovers() {
    let broker_port = free_port();
    let broker_addr: std::net::SocketAddr = format!("127.0.0.1:{broker_port}").parse().unwrap();
    let broker = BrokerContainer::start_fresh(broker_port);
    wait_connectable(broker_addr).await;

    let proxy_port = free_port();
    let listen: std::net::SocketAddr = format!("127.0.0.1:{proxy_port}").parse().unwrap();
    let _cleanup = Cleanup(format!("--listen {listen}"));
    let mut parent = Command::new(BIN)
        .args([
            "--listen",
            &listen.to_string(),
            "--broker",
            &broker_addr.to_string(),
            "--mode",
            "migrate",
        ])
        .spawn()
        .unwrap();
    wait_connectable(listen).await;

    // Client connects and subscribes to its own topic.
    let mut a = v3_connect(listen, "bd-client").await;
    a.send(MqttPacket::V3(v3::Packet::Subscribe {
        packet_id: NonZeroU16::new(1).unwrap(),
        topic_filters: vec![("bd/#".into(), QoS::AtLeastOnce)],
    }))
    .await
    .unwrap();
    match next_packet(&mut a).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { .. }) => {}
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
    let a_local_before = a.get_ref().local_addr().unwrap();

    // Kill the broker, then shed: the child must adopt A's session while the
    // broker is unreachable.
    broker.kill();
    tokio::time::sleep(Duration::from_millis(300)).await;
    signal(parent.id(), "-USR2");
    let status = wait_exit(&mut parent, Duration::from_secs(15))
        .expect("parent did not exit after migrate shed");
    assert!(status.success(), "parent exited with {status}");

    // The client socket survived.
    assert_eq!(a.get_ref().local_addr().unwrap(), a_local_before);

    // During the outage the proxy answers keepalives locally...
    v3_ping(&mut a).await;

    // ...and locally acks QoS1 publishes, buffering them for the broker.
    for i in 0..5u16 {
        let publish = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            topic: "bd/buffered".into(),
            packet_id: NonZeroU16::new(10 + i),
            payload: bytes::Bytes::from(format!("buffered-{i}")),
            properties: None,
        };
        a.send(MqttPacket::V3(v3::Packet::Publish(Box::new(publish))))
            .await
            .unwrap();
        match next_packet(&mut a).await {
            MqttPacket::V3(v3::Packet::PublishAck { packet_id }) => {
                assert_eq!(packet_id.get(), 10 + i);
            }
            other => panic!("expected local PublishAck during outage, got {other:?}"),
        }
    }

    // Bring the broker back; the child's backoff retry reconnects and
    // flushes the buffer. A is subscribed, so it receives its own buffered
    // messages back on the surviving connection.
    broker.start();
    wait_connectable(broker_addr).await;

    let mut received = std::collections::HashSet::new();
    while received.len() < 5 {
        match next_packet(&mut a).await {
            MqttPacket::V3(v3::Packet::Publish(p)) => {
                let payload = String::from_utf8(p.payload.to_vec()).unwrap();
                assert!(payload.starts_with("buffered-"), "unexpected publish: {payload}");
                received.insert(payload);
            }
            // Tolerate anything else (e.g. re-SUBSCRIBE artifacts) while
            // waiting.
            _ => continue,
        }
    }
}
