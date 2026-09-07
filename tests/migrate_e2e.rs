//! End-to-end test of the PoC 2 migrate shed against the real binary.
//!
//! Unlike drain mode, the parent must exit immediately after handing sessions
//! to the child — and pre-existing client connections must survive the
//! handoff without a TCP reconnect.

mod common;

use std::num::NonZeroU16;
use std::process::Command;
use std::time::Duration;

use common::{
    BIN, Cleanup, free_port, next_packet, require_broker, signal, v3_connect, v3_ping,
    v5_connect, wait_connectable, wait_exit,
};
use futures::SinkExt;
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttPacket, v3, v5};

#[tokio::test]
async fn migrate_shed_hands_sessions_to_new_process() {
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
            "--mode",
            "migrate",
        ])
        .spawn()
        .unwrap();

    wait_connectable(listen).await;

    // Client A connects pre-shed and subscribes.
    let mut a = v3_connect(listen, "e2e-mig-a").await;
    a.send(MqttPacket::V3(v3::Packet::Subscribe {
        packet_id: NonZeroU16::new(9).unwrap(),
        topic_filters: vec![("mig/route/#".into(), QoS::AtLeastOnce)],
    }))
    .await
    .unwrap();
    match next_packet(&mut a).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { .. }) => {}
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
    let a_local_before = a.get_ref().local_addr().unwrap();

    // Trigger the migrate shed. The parent hands A's socket + state to the
    // child and exits immediately — no drain window.
    signal(parent.id(), "-USR2");
    let status = wait_exit(&mut parent, Duration::from_secs(10))
        .expect("parent did not exit after migrate shed");
    assert!(status.success(), "parent exited with {status}");

    // A's TCP connection survived the process handoff: same local address,
    // still responsive — now served by the child.
    assert_eq!(
        a.get_ref().local_addr().unwrap(),
        a_local_before,
        "client socket changed across the shed"
    );
    v3_ping(&mut a).await;

    // A's session still works end-to-end through the child's fresh
    // broker-side connection.
    let publish = Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "poc1/mig".into(),
        packet_id: NonZeroU16::new(1),
        payload: bytes::Bytes::from_static(b"after-migration"),
        properties: None,
    };
    a.send(MqttPacket::V3(v3::Packet::Publish(Box::new(publish))))
        .await
        .unwrap();
    match next_packet(&mut a).await {
        MqttPacket::V3(v3::Packet::PublishAck { packet_id }) => {
            assert_eq!(packet_id.get(), 1);
        }
        other => panic!("expected PublishAck after migration, got {other:?}"),
    }

    // New clients connect to the child as usual.
    let mut b = v3_connect(listen, "e2e-mig-b").await;
    v3_ping(&mut b).await;

    // A's subscription was restored on the child's broker connection: a
    // publish from B routes back to A on its surviving connection.
    let routed = Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "mig/route/x".into(),
        packet_id: NonZeroU16::new(1),
        payload: bytes::Bytes::from_static(b"routed-after-migration"),
        properties: None,
    };
    b.send(MqttPacket::V3(v3::Packet::Publish(Box::new(routed))))
        .await
        .unwrap();
    match next_packet(&mut b).await {
        MqttPacket::V3(v3::Packet::PublishAck { .. }) => {}
        other => panic!("B expected PublishAck, got {other:?}"),
    }
    match next_packet(&mut a).await {
        MqttPacket::V3(v3::Packet::Publish(p)) => {
            assert_eq!(&p.topic[..], "mig/route/x");
            assert_eq!(p.payload.as_ref(), b"routed-after-migration");
        }
        other => panic!("A expected routed Publish, got {other:?}"),
    }
}

#[tokio::test]
async fn migrate_shed_supports_mqtt5_sessions() {
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
            "--mode",
            "migrate",
        ])
        .spawn()
        .unwrap();

    wait_connectable(listen).await;

    let mut a = v5_connect(listen, "e2e-mig-v5").await;
    let a_local_before = a.get_ref().local_addr().unwrap();

    signal(parent.id(), "-USR2");
    let status = wait_exit(&mut parent, Duration::from_secs(10))
        .expect("parent did not exit after migrate shed");
    assert!(status.success(), "parent exited with {status}");

    assert_eq!(a.get_ref().local_addr().unwrap(), a_local_before);

    // v5 PINGREQ/PINGRESP through the adopted session.
    a.send(MqttPacket::V5(v5::Packet::PingRequest)).await.unwrap();
    match next_packet(&mut a).await {
        MqttPacket::V5(v5::Packet::PingResponse) => {}
        other => panic!("expected v5 PingResponse after migration, got {other:?}"),
    }

    // v5 QoS1 publish through the child's broker connection.
    let publish = Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "poc1/mig-v5".into(),
        packet_id: NonZeroU16::new(7),
        payload: bytes::Bytes::from_static(b"v5-after-migration"),
        properties: None,
    };
    a.send(MqttPacket::V5(v5::Packet::Publish(Box::new(publish))))
        .await
        .unwrap();
    match next_packet(&mut a).await {
        MqttPacket::V5(v5::Packet::PublishAck(ack)) => {
            assert_eq!(ack.packet_id.get(), 7);
        }
        other => panic!("expected v5 PublishAck after migration, got {other:?}"),
    }
}

