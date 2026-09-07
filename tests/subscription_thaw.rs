//! Deterministic in-process proof that the subscription table survives in
//! the snapshot, the replayed CONNECT is patched to clean_session=false, and
//! the session re-SUBSCRIBEs on thaw.

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

/// Fake broker v3.1.1: CONNACKs CONNECTs, SUBACKs SUBSCRIBEs with granted
/// QoS, and reports every received packet into the channel for assertions.
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
                        MqttPacket::V3(v3::Packet::Subscribe {
                            packet_id,
                            topic_filters,
                        }) => {
                            framed
                                .send(MqttPacket::V3(v3::Packet::SubscribeAck {
                                    packet_id: *packet_id,
                                    status: topic_filters
                                        .iter()
                                        .map(|(_, qos)| v3::SubscribeReturnCode::Success(*qos))
                                        .collect(),
                                }))
                                .await
                                .unwrap();
                        }
                        MqttPacket::V3(v3::Packet::PingRequest) => {
                            framed
                                .send(MqttPacket::V3(v3::Packet::PingResponse))
                                .await
                                .unwrap();
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
async fn thaw_resubscribes_and_forces_clean_session_false() {
    let (broker_addr, mut packets) = start_fake_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let proxy = traffic_director::Proxy::start(listener, broker_addr);

    // Client connects (clean_session=true!) and subscribes via the proxy.
    let mut client = common::v3_connect(proxy_addr, "sub-thaw").await;
    client
        .send(MqttPacket::V3(v3::Packet::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            topic_filters: vec![("sub/#".into(), QoS::AtLeastOnce)],
        }))
        .await
        .unwrap();
    match common::next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { .. }) => {}
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
    // Drain CONNECT + SUBSCRIBE from the broker's packet log.
    recv_packet(&mut packets).await;
    recv_packet(&mut packets).await;

    // Freeze: the snapshot must carry the subscription.
    let batch = proxy.freeze_all().await;
    assert_eq!(batch.frozen.len(), 1);
    let frozen = &batch.frozen[0];
    assert_eq!(
        frozen.snapshot.subscriptions.len(),
        1,
        "subscription missing from snapshot"
    );
    assert_eq!(frozen.snapshot.subscriptions[0].topic_filter, "sub/#");

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

    // Thaw: the replayed CONNECT must be patched to clean_session=false so
    // the broker resumes the session.
    match recv_packet(&mut packets).await {
        MqttPacket::V3(v3::Packet::Connect(c)) => {
            assert!(
                !c.clean_session,
                "replayed CONNECT must force clean_session=false"
            );
        }
        other => panic!("expected CONNECT replay, got {other:?}"),
    }

    // ...followed by a re-SUBSCRIBE with the same filter and QoS.
    match recv_packet(&mut packets).await {
        MqttPacket::V3(v3::Packet::Subscribe { topic_filters, .. }) => {
            assert_eq!(topic_filters.len(), 1);
            assert_eq!(&topic_filters[0].0[..], "sub/#");
            assert_eq!(topic_filters[0].1, QoS::AtLeastOnce);
        }
        other => panic!("expected re-SUBSCRIBE, got {other:?}"),
    }

    // The re-SUBSCRIBE was proxy-internal: the client must NOT see a second
    // SUBACK — it sees only traffic from after the thaw. Prove liveness and
    // routing instead: ping works...
    common::v3_ping(&mut client).await;

    adopt.abort();
}

/// A routed publish after thaw must reach the client (subscription actually
/// re-established at the broker).
#[tokio::test]
async fn routed_publish_flows_after_thaw() {
    common::require_broker().await;
    let broker_addr = common::broker_addr();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let proxy = traffic_director::Proxy::start(listener, broker_addr);

    let mut client = common::v3_connect(proxy_addr, "sub-route").await;
    client
        .send(MqttPacket::V3(v3::Packet::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            topic_filters: vec![("route/#".into(), QoS::AtLeastOnce)],
        }))
        .await
        .unwrap();
    match common::next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { .. }) => {}
        other => panic!("expected SubscribeAck, got {other:?}"),
    }

    let batch = proxy.freeze_all().await;
    let frozen = &batch.frozen[0];
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

    // Give the thaw a moment to re-SUBSCRIBE, then publish from a direct
    // broker connection.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut publisher = common::v3_connect(broker_addr, "sub-route-pub").await;
    publisher
        .send(MqttPacket::V3(v3::Packet::Publish(Box::new(Publish {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            topic: "route/x".into(),
            packet_id: NonZeroU16::new(1),
            payload: bytes::Bytes::from_static(b"after-thaw"),
            properties: None,
        }))))
        .await
        .unwrap();

    // The client must receive it on its surviving connection.
    loop {
        match common::next_packet(&mut client).await {
            MqttPacket::V3(v3::Packet::Publish(p)) => {
                assert_eq!(&p.topic[..], "route/x");
                assert_eq!(p.payload.as_ref(), b"after-thaw");
                break;
            }
            _ => continue, // tolerate unrelated packets
        }
    }

    adopt.abort();
}
