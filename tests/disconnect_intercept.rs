//! Server-side DISCONNECT interception: when the upstream broker asks
//! clients to go away (rolling update, scale-in), the proxy must swallow the
//! administrative DISCONNECT and quietly reconnect with backoff — the
//! downstream client stays unaware.
//!
//! v3.1/v3.1.1 has no server-side DISCONNECT; there the same behavior is
//! driven by the transport close.

mod common;

use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttCodec, MqttPacket, v3, v5};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::Framed;
use traffic_director::registry::SessionRegistry;

fn id(n: u16) -> NonZeroU16 {
    NonZeroU16::new(n).unwrap()
}

fn raw_v5(packet: v5::Packet) -> Vec<u8> {
    let mut codec = v5::Codec::new(1024 * 1024, 1024 * 1024);
    let mut buf = bytes::BytesMut::new();
    tokio_util::codec::Encoder::encode(&mut codec, packet, &mut buf).unwrap();
    buf.to_vec()
}

fn raw_v3(packet: v3::Packet) -> Vec<u8> {
    let mut codec = v3::Codec::new(1024 * 1024);
    let mut buf = bytes::BytesMut::new();
    tokio_util::codec::Encoder::encode(&mut codec, packet, &mut buf).unwrap();
    buf.to_vec()
}

enum BrokerCommand {
    /// Write raw bytes to the current connection.
    Inject(Vec<u8>),
    /// Close the current connection.
    CloseConnection,
}

/// Scriptable fake broker, auto-detecting v3/v5 per connection. Automatic:
/// CONNACK, SUBACK, PINGRESP, PUBACK for QoS1. Everything else via commands.
struct FakeBroker {
    addr: SocketAddr,
    packets: mpsc::Receiver<MqttPacket>,
    command: broadcast::Sender<BrokerCommand>,
}

async fn start_fake_broker() -> FakeBroker {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (packet_tx, packet_rx) = mpsc::channel(64);
    let (command_tx, _) = broadcast::channel::<BrokerCommand>(16);

    let command = command_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(handle_conn(stream, packet_tx.clone(), command.subscribe()));
        }
    });

    FakeBroker {
        addr,
        packets: packet_rx,
        command: command_tx,
    }
}

impl Clone for BrokerCommand {
    fn clone(&self) -> Self {
        match self {
            BrokerCommand::Inject(b) => BrokerCommand::Inject(b.clone()),
            BrokerCommand::CloseConnection => BrokerCommand::CloseConnection,
        }
    }
}

async fn handle_conn(
    stream: TcpStream,
    packets: mpsc::Sender<MqttPacket>,
    mut command: broadcast::Receiver<BrokerCommand>,
) {
    // Version-detect first, then run the versioned loop.
    let mut framed = Framed::new(
        stream,
        MqttCodec::Version(rmqtt_codec::version::VersionCodec),
    );
    let version = match framed.next().await {
        Some(Ok((MqttPacket::Version(v), _))) => v,
        _ => return,
    };
    let mut parts = framed.into_parts();
    parts.codec = match version {
        rmqtt_codec::version::ProtocolVersion::MQTT3 => {
            MqttCodec::V3(v3::Codec::new(1024 * 1024))
        }
        rmqtt_codec::version::ProtocolVersion::MQTT5 => {
            MqttCodec::V5(v5::Codec::new(1024 * 1024, 1024 * 1024))
        }
    };
    let mut framed = Framed::from_parts(parts);

    loop {
        tokio::select! {
            next = framed.next() => {
                let Some(result) = next else { return };
                let Ok((packet, _)) = result else { return };
                match &packet {
                    MqttPacket::V3(v3::Packet::Connect(_)) => {
                        let ack = MqttPacket::V3(v3::Packet::ConnectAck(v3::ConnectAck {
                            return_code: v3::ConnectAckReason::ConnectionAccepted,
                            session_present: false,
                        }));
                        if framed.send(ack).await.is_err() { return; }
                    }
                    MqttPacket::V5(v5::Packet::Connect(_)) => {
                        let ack = MqttPacket::V5(v5::Packet::ConnectAck(Box::new(
                            v5::ConnectAck {
                                session_present: false,
                                reason_code: v5::ConnectAckReason::Success,
                                ..Default::default()
                            },
                        )));
                        if framed.send(ack).await.is_err() { return; }
                    }
                    MqttPacket::V3(v3::Packet::Subscribe { packet_id, topic_filters }) => {
                        let ack = MqttPacket::V3(v3::Packet::SubscribeAck {
                            packet_id: *packet_id,
                            status: topic_filters
                                .iter()
                                .map(|(_, qos)| v3::SubscribeReturnCode::Success(*qos))
                                .collect(),
                        });
                        if framed.send(ack).await.is_err() { return; }
                    }
                    MqttPacket::V5(v5::Packet::Subscribe(s)) => {
                        let ack = MqttPacket::V5(v5::Packet::SubscribeAck(v5::SubscribeAck {
                            packet_id: s.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status: s
                                .topic_filters
                                .iter()
                                .map(|(_, o)| match o.qos {
                                    QoS::AtMostOnce => v5::SubscribeAckReason::GrantedQos0,
                                    QoS::AtLeastOnce => v5::SubscribeAckReason::GrantedQos1,
                                    QoS::ExactlyOnce => v5::SubscribeAckReason::GrantedQos2,
                                })
                                .collect(),
                        }));
                        if framed.send(ack).await.is_err() { return; }
                    }
                    MqttPacket::V3(v3::Packet::PingRequest) => {
                        if framed.send(MqttPacket::V3(v3::Packet::PingResponse)).await.is_err() { return; }
                    }
                    MqttPacket::V5(v5::Packet::PingRequest) => {
                        if framed.send(MqttPacket::V5(v5::Packet::PingResponse)).await.is_err() { return; }
                    }
                    MqttPacket::V3(v3::Packet::Publish(p)) if p.qos == QoS::AtLeastOnce => {
                        let ack = MqttPacket::V3(v3::Packet::PublishAck {
                            packet_id: p.packet_id.unwrap(),
                        });
                        if framed.send(ack).await.is_err() { return; }
                    }
                    MqttPacket::V5(v5::Packet::Publish(p)) if p.qos == QoS::AtLeastOnce => {
                        let ack = MqttPacket::V5(v5::Packet::PublishAck(v5::PublishAck {
                            packet_id: p.packet_id.unwrap(),
                            reason_code: v5::PublishAckReason::Success,
                            properties: Vec::new(),
                            reason_string: None,
                        }));
                        if framed.send(ack).await.is_err() { return; }
                    }
                    _ => {}
                }
                if packets.send(packet).await.is_err() { return; }
            }
            cmd = command.recv() => {
                match cmd {
                    Ok(BrokerCommand::Inject(raw)) => {
                        use tokio::io::AsyncWriteExt;
                        if framed.get_mut().write_all(&raw).await.is_err() { return; }
                    }
                    Ok(BrokerCommand::CloseConnection) => return,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

impl FakeBroker {
    async fn recv(&mut self) -> MqttPacket {
        tokio::time::timeout(Duration::from_secs(5), self.packets.recv())
            .await
            .expect("timed out waiting for fake broker packet")
            .expect("fake broker channel closed")
    }

    async fn recv_connect(&mut self) -> MqttPacket {
        match self.recv().await {
            p @ (MqttPacket::V3(v3::Packet::Connect(_))
            | MqttPacket::V5(v5::Packet::Connect(_))) => p,
            other => panic!("expected CONNECT, got {other:?}"),
        }
    }

    fn inject(&self, raw: Vec<u8>) {
        let _ = self.command.send(BrokerCommand::Inject(raw));
    }

    fn close_connection(&self) {
        let _ = self.command.send(BrokerCommand::CloseConnection);
    }
}

async fn start_proxy(broker_addr: SocketAddr) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(traffic_director::serve(listener, broker_addr));
    addr
}

/// Assert the client receives nothing within the grace period.
async fn expect_client_quiet(client: &mut common::Client, grace: Duration) {
    assert!(
        tokio::time::timeout(grace, client.next()).await.is_err(),
        "client received an unexpected packet (DISCONNECT leaked?)"
    );
}

#[tokio::test]
async fn v5_server_shutting_down_disconnect_is_intercepted() {
    let mut broker = start_fake_broker().await;
    let proxy_addr = start_proxy(broker.addr).await;

    let mut client = common::v5_connect(proxy_addr, "di-v5").await;
    broker.recv_connect().await;

    client
        .send(MqttPacket::V5(v5::Packet::Subscribe(v5::Subscribe {
            packet_id: id(1),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![("di/#".into(), v5::SubscriptionOptions {
                qos: QoS::AtLeastOnce,
                ..Default::default()
            })],
        })))
        .await
        .unwrap();
    match common::next_packet(&mut client).await {
        MqttPacket::V5(v5::Packet::SubscribeAck(_)) => {}
        other => panic!("expected v5 SubscribeAck, got {other:?}"),
    }
    broker.recv().await; // SUBSCRIBE at broker

    // Broker demands clients go away, then closes — rolling update style.
    broker.inject(raw_v5(v5::Packet::Disconnect(v5::Disconnect {
        reason_code: v5::DisconnectReasonCode::ServerShuttingDown,
        session_expiry_interval_secs: None,
        server_reference: None,
        reason_string: None,
        user_properties: Vec::new(),
    })));
    broker.close_connection();

    // The DISCONNECT must NOT reach the client.
    expect_client_quiet(&mut client, Duration::from_millis(500)).await;

    // The proxy quietly reconnects: a second CONNECT appears, with
    // clean_start=false so the broker resumes the session...
    match broker.recv_connect().await {
        MqttPacket::V5(v5::Packet::Connect(c)) => {
            assert!(!c.clean_start, "reconnect must resume the session");
        }
        other => panic!("expected CONNECT replay, got {other:?}"),
    }

    // ...and the subscription is re-issued.
    match broker.recv().await {
        MqttPacket::V5(v5::Packet::Subscribe(s)) => {
            assert_eq!(s.topic_filters.len(), 1);
            assert_eq!(&s.topic_filters[0].0[..], "di/#");
        }
        other => panic!("expected re-SUBSCRIBE, got {other:?}"),
    }

    // The client still knows nothing; routing works again.
    client
        .send(MqttPacket::V5(v5::Packet::PingRequest))
        .await
        .unwrap();
    match common::next_packet(&mut client).await {
        MqttPacket::V5(v5::Packet::PingResponse) => {}
        other => panic!("expected v5 PingResponse, got {other:?}"),
    }

    broker.inject(raw_v5(v5::Packet::Publish(Box::new(Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "di/x".into(),
        packet_id: Some(id(77)),
        payload: bytes::Bytes::from_static(b"back-online"),
        properties: None,
    }))));
    match common::next_packet(&mut client).await {
        MqttPacket::V5(v5::Packet::Publish(p)) => {
            assert_eq!(p.payload.as_ref(), b"back-online");
        }
        other => panic!("expected routed v5 Publish, got {other:?}"),
    }
}

#[tokio::test]
async fn v5_client_fault_disconnect_is_forwarded() {
    let mut broker = start_fake_broker().await;
    let proxy_addr = start_proxy(broker.addr).await;

    let mut client = common::v5_connect(proxy_addr, "di-v5-fault").await;
    broker.recv_connect().await;

    // A client-fault DISCONNECT (protocol error) must NOT be swallowed.
    broker.inject(raw_v5(v5::Packet::Disconnect(v5::Disconnect {
        reason_code: v5::DisconnectReasonCode::ProtocolError,
        session_expiry_interval_secs: None,
        server_reference: None,
        reason_string: None,
        user_properties: Vec::new(),
    })));

    match common::next_packet(&mut client).await {
        MqttPacket::V5(v5::Packet::Disconnect(d)) => {
            assert_eq!(d.reason_code, v5::DisconnectReasonCode::ProtocolError);
        }
        other => panic!("client-fault DISCONNECT was swallowed: {other:?}"),
    }
}

#[tokio::test]
async fn v3_transport_close_triggers_quiet_reconnect() {
    let mut broker = start_fake_broker().await;
    let proxy_addr = start_proxy(broker.addr).await;

    let mut client = common::v3_connect(proxy_addr, "di-v3").await;
    broker.recv_connect().await;

    client
        .send(MqttPacket::V3(v3::Packet::Subscribe {
            packet_id: id(1),
            topic_filters: vec![("di3/#".into(), QoS::AtLeastOnce)],
        }))
        .await
        .unwrap();
    match common::next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { .. }) => {}
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
    broker.recv().await;

    // v3: the "go away" signal is the transport dropping.
    broker.close_connection();

    // Client unaware, proxy reconnects with clean_session=false...
    match broker.recv_connect().await {
        MqttPacket::V3(v3::Packet::Connect(c)) => {
            assert!(!c.clean_session, "reconnect must resume the session");
        }
        other => panic!("expected CONNECT replay, got {other:?}"),
    }
    // ...re-SUBSCRIBEs...
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::Subscribe { topic_filters, .. }) => {
            assert_eq!(&topic_filters[0].0[..], "di3/#");
        }
        other => panic!("expected re-SUBSCRIBE, got {other:?}"),
    }

    // ...and traffic flows again.
    common::v3_ping(&mut client).await;
    broker.inject(raw_v3(v3::Packet::Publish(Box::new(Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "di3/x".into(),
        packet_id: Some(id(88)),
        payload: bytes::Bytes::from_static(b"v3-back-online"),
        properties: None,
    }))));
    match common::next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::Publish(p)) => {
            assert_eq!(p.payload.as_ref(), b"v3-back-online");
        }
        other => panic!("expected routed v3 Publish, got {other:?}"),
    }
}

// SessionRegistry is referenced by tests in this crate family; keep the
// import honest even though this file uses serve() instead.
#[allow(dead_code)]
fn _unused(_: SessionRegistry) {}
