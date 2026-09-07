//! Deterministic in-process proof that a non-empty client→broker QoS window
//! is captured in the snapshot and retransmitted (DUP=1) on thaw.
//!
//! Uses a fake in-test broker that never PUBACKs first deliveries — a real
//! broker acks too fast on loopback to ever catch a window mid-flight. The
//! fake broker does PUBACK DUP-flagged publishes, so the post-thaw
//! retransmission completes end-to-end.

mod common;

use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::os::unix::io::RawFd;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttCodec, MqttPacket, v3};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use traffic_director::registry::SessionRegistry;

/// Minimal MQTT v3.1.1 broker: CONNACKs every CONNECT, reports every other
/// packet into the channel, PUBACKs only DUP-flagged QoS1 publishes.
async fn start_fake_broker() -> (SocketAddr, mpsc::Receiver<MqttPacket>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (packet_tx, packet_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let packet_tx = packet_tx.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(stream, MqttCodec::V3(v3::Codec::new(1024 * 1024)));
                while let Some(packet) = framed.next().await {
                    let Ok((packet, _)) = packet else { return };
                    match &packet {
                        MqttPacket::V3(v3::Packet::Connect(_)) => {
                            framed
                                .send(MqttPacket::V3(v3::Packet::ConnectAck(v3::ConnectAck {
                                    return_code: v3::ConnectAckReason::ConnectionAccepted,
                                    session_present: false,
                                })))
                                .await
                                .unwrap();
                        }
                        MqttPacket::V3(v3::Packet::Publish(p)) => {
                            if p.dup
                                && p.qos == QoS::AtLeastOnce
                                && let Some(packet_id) = p.packet_id
                            {
                                framed
                                    .send(MqttPacket::V3(v3::Packet::PublishAck { packet_id }))
                                    .await
                                    .unwrap();
                            }
                        }
                        _ => {}
                    }
                    if packet_tx.send(packet).await.is_err() {
                        return;
                    }
                }
            });
        }
    });

    (addr, packet_rx)
}

async fn recv_packet(packets: &mut mpsc::Receiver<MqttPacket>) -> MqttPacket {
    tokio::time::timeout(Duration::from_secs(5), packets.recv())
        .await
        .expect("timed out waiting for fake broker packet")
        .expect("fake broker channel closed")
}

#[tokio::test]
async fn c2b_window_retransmits_with_dup_on_thaw() {
    let (broker_addr, mut packets) = start_fake_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let proxy = traffic_director::Proxy::start(listener, broker_addr);

    let mut client = common::v3_connect(proxy_addr, "thaw-c2b").await;

    // Publish QoS1; the fake broker never acks first deliveries, so the
    // window must hold the publish at freeze time.
    let publish = Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "thaw/x".into(),
        packet_id: NonZeroU16::new(1),
        payload: bytes::Bytes::from_static(b"inflight"),
        properties: None,
    };
    client
        .send(MqttPacket::V3(v3::Packet::Publish(Box::new(publish))))
        .await
        .unwrap();

    match recv_packet(&mut packets).await {
        MqttPacket::V3(v3::Packet::Connect(_)) => {}
        other => panic!("fake broker expected CONNECT, got {other:?}"),
    }
    match recv_packet(&mut packets).await {
        MqttPacket::V3(v3::Packet::Publish(p)) => assert!(!p.dup),
        other => panic!("fake broker got unexpected packet: {other:?}"),
    }

    // Freeze: the snapshot must contain the unacked publish.
    let batch = proxy.freeze_all().await;
    assert_eq!(batch.frozen.len(), 1);
    let frozen = &batch.frozen[0];
    assert_eq!(
        frozen.snapshot.windows.len(),
        1,
        "window missing from snapshot"
    );

    // Simulate the shed in-process: dup the client fd (SCM_RIGHTS does this
    // across processes), end the old session, and adopt from the snapshot.
    let dup_fd: RawFd = nix::unistd::dup(frozen.client_fd).unwrap();
    let snapshot = frozen.snapshot.clone();
    batch.finish();

    let registry = SessionRegistry::new();
    let adopt = tokio::spawn(traffic_director::session::adopt_session(
        dup_fd,
        snapshot,
        broker_addr,
        registry,
    ));

    // The thawed session replays CONNECT (fake broker CONNACKs), then must
    // retransmit the unacked PUBLISH with DUP=1.
    match recv_packet(&mut packets).await {
        MqttPacket::V3(v3::Packet::Connect(_)) => {}
        other => panic!("fake broker expected CONNECT replay, got {other:?}"),
    }
    match recv_packet(&mut packets).await {
        MqttPacket::V3(v3::Packet::Publish(p)) => {
            assert!(p.dup, "retransmitted publish must have DUP set");
            assert_eq!(p.packet_id, NonZeroU16::new(1));
            assert_eq!(p.payload.as_ref(), b"inflight");
        }
        other => panic!("expected retransmitted publish, got {other:?}"),
    }

    // The client connection survived: the broker PUBACKs the DUP, and the
    // ack chains back to the client through the adopted session.
    match common::next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::PublishAck { packet_id }) => {
            assert_eq!(packet_id.get(), 1);
        }
        other => panic!("client expected PublishAck, got {other:?}"),
    }

    adopt.abort();
}

/// Broker→client direction: publishes the client never acked must be
/// replayed to it with DUP=1 after thaw.
#[tokio::test]
async fn b2c_window_replays_unacked_publishes_on_thaw() {
    common::require_broker().await;
    let broker_addr = common::broker_addr();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let proxy = traffic_director::Proxy::start(listener, broker_addr);

    // Client subscribes through the proxy.
    let mut client = common::v3_connect(proxy_addr, "thaw-b2c").await;
    client
        .send(MqttPacket::V3(v3::Packet::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            topic_filters: vec![("thaw/b2c".into(), QoS::AtLeastOnce)],
        }))
        .await
        .unwrap();
    match common::next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { .. }) => {}
        other => panic!("expected SubscribeAck, got {other:?}"),
    }

    // A broker-direct publisher sends 3 QoS1 messages.
    let mut publisher = common::v3_connect(broker_addr, "thaw-b2c-pub").await;
    for i in 0..3u16 {
        publisher
            .send(MqttPacket::V3(v3::Packet::Publish(Box::new(Publish {
                dup: false,
                retain: false,
                qos: QoS::AtLeastOnce,
                topic: "thaw/b2c".into(),
                packet_id: NonZeroU16::new(10 + i),
                payload: bytes::Bytes::from(format!("b2c-{i}")),
                properties: None,
            }))))
            .await
            .unwrap();
        match common::next_packet(&mut publisher).await {
            MqttPacket::V3(v3::Packet::PublishAck { .. }) => {}
            other => panic!("publisher expected PublishAck, got {other:?}"),
        }
    }

    // The client receives all 3 but NEVER acks them.
    for i in 0..3u16 {
        match common::next_packet(&mut client).await {
            MqttPacket::V3(v3::Packet::Publish(p)) => {
                assert!(!p.dup);
                assert_eq!(p.payload.as_ref(), format!("b2c-{i}").as_bytes());
            }
            other => panic!("client expected Publish, got {other:?}"),
        }
    }

    // Freeze: all 3 must be in the b2c window.
    let batch = proxy.freeze_all().await;
    assert_eq!(batch.frozen.len(), 1);
    let frozen = &batch.frozen[0];
    assert_eq!(
        frozen.snapshot.windows.len(),
        3,
        "expected 3 b2c window entries in snapshot"
    );

    let dup_fd: RawFd = nix::unistd::dup(frozen.client_fd).unwrap();
    let snapshot = frozen.snapshot.clone();
    batch.finish();

    let registry = SessionRegistry::new();
    let adopt = tokio::spawn(traffic_director::session::adopt_session(
        dup_fd,
        snapshot,
        broker_addr,
        registry,
    ));

    // After thaw, the client is re-sent all 3 unacked publishes with DUP=1.
    for i in 0..3u16 {
        match common::next_packet(&mut client).await {
            MqttPacket::V3(v3::Packet::Publish(p)) => {
                assert!(p.dup, "replayed publish must have DUP set");
                assert_eq!(p.payload.as_ref(), format!("b2c-{i}").as_bytes());
            }
            other => panic!("client expected replayed Publish, got {other:?}"),
        }
    }

    // The adopted session is alive and responsive.
    common::v3_ping(&mut client).await;

    adopt.abort();
}
