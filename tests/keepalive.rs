//! Keepalive termination: the proxy must answer client PINGREQ instantly,
//! regardless of the upstream connection state — including a broker that is
//! reachable but never responds (partition without a TCP reset). The proxy
//! originates its own broker keepalives and enforces the client-side
//! keepalive timeout itself.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::QoS;
use rmqtt_codec::{MqttCodec, MqttPacket, v3, v5};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

/// Fake broker: answers CONNACK, SUBACK and PINGREQ, records every packet.
/// Can be flipped to fully silent (still recording) to simulate a
/// partitioned broker — under chained keepalive that means no PINGRESP.
struct FakeBroker {
    addr: SocketAddr,
    packets: mpsc::Receiver<MqttPacket>,
    responsive: Arc<AtomicBool>,
}

async fn start_fake_broker() -> FakeBroker {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (packet_tx, packet_rx) = mpsc::channel(64);
    let responsive = Arc::new(AtomicBool::new(true));

    let flag = responsive.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(handle_conn(stream, packet_tx.clone(), flag.clone()));
        }
    });

    FakeBroker {
        addr,
        packets: packet_rx,
        responsive,
    }
}

async fn handle_conn(
    stream: TcpStream,
    packets: mpsc::Sender<MqttPacket>,
    responsive: Arc<AtomicBool>,
) {
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
        rmqtt_codec::version::ProtocolVersion::MQTT3 => MqttCodec::V3(v3::Codec::new(1024 * 1024)),
        rmqtt_codec::version::ProtocolVersion::MQTT5 => {
            MqttCodec::V5(v5::Codec::new(1024 * 1024, 1024 * 1024))
        }
    };
    let mut framed = Framed::from_parts(parts);

    while let Some(packet) = framed.next().await {
        let Ok((packet, _)) = packet else { return };
        if responsive.load(Ordering::SeqCst) {
            let response = match &packet {
                MqttPacket::V3(v3::Packet::Connect(_)) => Some(MqttPacket::V3(
                    v3::Packet::ConnectAck(v3::ConnectAck {
                        return_code: v3::ConnectAckReason::ConnectionAccepted,
                        session_present: false,
                    }),
                )),
                MqttPacket::V3(v3::Packet::Subscribe { packet_id, topic_filters }) => {
                    Some(MqttPacket::V3(v3::Packet::SubscribeAck {
                        packet_id: *packet_id,
                        status: topic_filters
                            .iter()
                            .map(|(_, qos)| v3::SubscribeReturnCode::Success(*qos))
                            .collect(),
                    }))
                }
                MqttPacket::V5(v5::Packet::Connect(_)) => Some(MqttPacket::V5(
                    v5::Packet::ConnectAck(Box::new(v5::ConnectAck {
                        session_present: false,
                        reason_code: v5::ConnectAckReason::Success,
                        ..Default::default()
                    })),
                )),
                MqttPacket::V5(v5::Packet::Subscribe(s)) => Some(MqttPacket::V5(
                    v5::Packet::SubscribeAck(v5::SubscribeAck {
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
                    }),
                )),
                MqttPacket::V3(v3::Packet::PingRequest) => {
                    Some(MqttPacket::V3(v3::Packet::PingResponse))
                }
                MqttPacket::V5(v5::Packet::PingRequest) => {
                    Some(MqttPacket::V5(v5::Packet::PingResponse))
                }
                _ => None,
            };
            if let Some(response) = response
                && framed.send(response).await.is_err()
            {
                return;
            }
        }
        if packets.send(packet).await.is_err() {
            return;
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

}

async fn start_proxy(broker_addr: SocketAddr) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(traffic_director::serve(listener, broker_addr));
    addr
}

async fn v3_connect_keepalive(addr: SocketAddr, client_id: &str, keep_alive: u16) -> common::Client {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut client = Framed::new(stream, MqttCodec::V3(v3::Codec::new(common::MAX_PACKET)));
    let connect = v3::Connect {
        clean_session: true,
        keep_alive,
        ..Default::default()
    }
    .client_id(client_id.to_owned());
    client
        .send(MqttPacket::V3(v3::Packet::Connect(Box::new(connect))))
        .await
        .unwrap();
    match common::next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::ConnectAck(ack)) => {
            assert_eq!(ack.return_code, v3::ConnectAckReason::ConnectionAccepted);
        }
        other => panic!("expected ConnectAck, got {other:?}"),
    }
    client
}

/// PINGRESP must arrive within this bound; anything longer is a chained
/// round-trip, not an instant local answer.
const INSTANT: Duration = Duration::from_millis(200);

#[tokio::test]
async fn pingreq_answered_instantly_while_connected() {
    let mut broker = start_fake_broker().await;
    let proxy_addr = start_proxy(broker.addr).await;
    let mut client = v3_connect_keepalive(proxy_addr, "ka-v3", 60).await;
    broker.recv().await; // CONNECT
    broker.responsive.store(false, Ordering::SeqCst); // broker never PONGs

    let started = Instant::now();
    client
        .send(MqttPacket::V3(v3::Packet::PingRequest))
        .await
        .unwrap();
    match tokio::time::timeout(INSTANT, client.next()).await {
        Ok(Some(Ok((MqttPacket::V3(v3::Packet::PingResponse), _)))) => {}
        other => panic!("no instant PingResponse: {other:?}"),
    }
    assert!(started.elapsed() < INSTANT);

    // ...and the PINGREQ was also forwarded to the broker (drives the
    // broker-side keepalive).
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::PingRequest) => {}
        other => panic!("broker did not receive forwarded PINGREQ: {other:?}"),
    }
}

#[tokio::test]
async fn pingreq_answered_instantly_with_partitioned_broker() {
    let mut broker = start_fake_broker().await;
    let proxy_addr = start_proxy(broker.addr).await;
    let mut client = v3_connect_keepalive(proxy_addr, "ka-partitioned", 60).await;
    broker.recv().await; // CONNECT

    // Broker becomes a black hole (no RST, no responses).
    broker.responsive.store(false, Ordering::SeqCst);

    client
        .send(MqttPacket::V3(v3::Packet::PingRequest))
        .await
        .unwrap();
    match tokio::time::timeout(INSTANT, client.next()).await {
        Ok(Some(Ok((MqttPacket::V3(v3::Packet::PingResponse), _)))) => {}
        other => panic!("no instant PingResponse with partitioned broker: {other:?}"),
    }
}

#[tokio::test]
async fn pingreq_answered_instantly_v5() {
    let mut broker = start_fake_broker().await;
    let proxy_addr = start_proxy(broker.addr).await;
    let mut client = common::v5_connect(proxy_addr, "ka-v5").await;
    broker.recv().await; // CONNECT
    broker.responsive.store(false, Ordering::SeqCst); // broker never PONGs

    client
        .send(MqttPacket::V5(v5::Packet::PingRequest))
        .await
        .unwrap();
    match tokio::time::timeout(INSTANT, client.next()).await {
        Ok(Some(Ok((MqttPacket::V5(v5::Packet::PingResponse), _)))) => {}
        other => panic!("no instant v5 PingResponse: {other:?}"),
    }

    match broker.recv().await {
        MqttPacket::V5(v5::Packet::PingRequest) => {}
        other => panic!("broker did not receive forwarded v5 PINGREQ: {other:?}"),
    }
}

#[tokio::test]
async fn upstream_pingresp_is_swallowed() {
    let mut broker = start_fake_broker().await;
    let proxy_addr = start_proxy(broker.addr).await;
    let mut client = v3_connect_keepalive(proxy_addr, "ka-swallow", 60).await;
    broker.recv().await; // CONNECT

    // Client pings: it gets exactly ONE PINGRESP (the instant local one).
    client
        .send(MqttPacket::V3(v3::Packet::PingRequest))
        .await
        .unwrap();
    match tokio::time::timeout(INSTANT, client.next()).await {
        Ok(Some(Ok((MqttPacket::V3(v3::Packet::PingResponse), _)))) => {}
        other => panic!("no instant PingResponse: {other:?}"),
    }

    // The broker received the forwarded PINGREQ and answers PINGRESP; that
    // upstream PINGRESP must be swallowed — the client already has its
    // answer and must not see a duplicate.
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::PingRequest) => {}
        other => panic!("broker did not receive forwarded PINGREQ: {other:?}"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), client.next())
            .await
            .is_err(),
        "client received a duplicate PINGRESP from upstream"
    );
}
