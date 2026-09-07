//! QoS2 four-way handshake interrupted by a freeze/thaw at each step, in
//! both directions. Uses a scriptable fake broker for determinism:
//! automatic only for CONNACK/SUBACK/PINGRESP/PUBCOMP — every broker-side
//! QoS2 sender packet (PUBREC, PUBREL, PUBLISH) is injected by the test.
//!
//! The fake broker also emulates broker session resumption: if a connection
//! dies with an incomplete QoS2 outflow (PUBLISH sent, PUBREC received,
//! PUBCOMP outstanding), the PUBREL is re-sent when the same client-id
//! reconnects with clean_session=false — which is exactly what real brokers
//! do and what the thaw's patched CONNECT triggers.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttCodec, MqttPacket, v3};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::Framed;
use traffic_director::registry::SessionRegistry;

fn id(n: u16) -> NonZeroU16 {
    NonZeroU16::new(n).unwrap()
}

fn raw(packet: MqttPacket) -> Vec<u8> {
    let mut codec = v3::Codec::new(1024 * 1024);
    let mut buf = bytes::BytesMut::new();
    let MqttPacket::V3(p) = packet else {
        panic!("raw() only supports v3 packets")
    };
    tokio_util::codec::Encoder::encode(&mut codec, p, &mut buf).unwrap();
    buf.to_vec()
}

fn publish_qos2(packet_id: u16, payload: &'static str) -> MqttPacket {
    MqttPacket::V3(v3::Packet::Publish(Box::new(Publish {
        dup: false,
        retain: false,
        qos: QoS::ExactlyOnce,
        topic: "q2/x".into(),
        packet_id: NonZeroU16::new(packet_id),
        payload: bytes::Bytes::from_static(payload.as_bytes()),
        properties: None,
    })))
}

/// Packets the broker re-sends when a client resumes (clean_session=false).
type ResumptionStore = Arc<Mutex<HashMap<String, Vec<MqttPacket>>>>;

struct FakeBroker {
    addr: SocketAddr,
    packets: mpsc::Receiver<MqttPacket>,
    inject: broadcast::Sender<Vec<u8>>,
}

async fn start_fake_broker() -> FakeBroker {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (packet_tx, packet_rx) = mpsc::channel(64);
    let (inject_tx, _) = broadcast::channel::<Vec<u8>>(16);
    let store: ResumptionStore = Arc::new(Mutex::new(HashMap::new()));

    let inject = inject_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(handle_conn(
                stream,
                packet_tx.clone(),
                inject.clone(),
                store.clone(),
            ));
        }
    });

    FakeBroker {
        addr,
        packets: packet_rx,
        inject: inject_tx,
    }
}

async fn handle_conn(
    stream: tokio::net::TcpStream,
    packets: mpsc::Sender<MqttPacket>,
    inject: broadcast::Sender<Vec<u8>>,
    store: ResumptionStore,
) {
    let mut framed = Framed::new(stream, MqttCodec::V3(v3::Codec::new(1024 * 1024)));
    let mut inject_rx = inject.subscribe();
    let mut client_id = String::new();
    // QoS2 outflow state: we sent PUBLISH, got PUBREC, await PUBCOMP.
    let mut pending_pubrel: Option<NonZeroU16> = None;

    loop {
        tokio::select! {
            next = framed.next() => {
                let Some(result) = next else { break };
                let Ok((packet, _)) = result else { break };
                match &packet {
                    MqttPacket::V3(v3::Packet::Connect(c)) => {
                        client_id = c.client_id.to_string();
                        let resumed = !c.clean_session
                            && store.lock().unwrap().contains_key(&client_id);
                        // Take any parked resumption packets (present only
                        // after a clean_session=false reconnect).
                        let pending: Vec<MqttPacket> =
                            store.lock().unwrap().remove(&client_id).unwrap_or_default();
                        framed
                            .send(MqttPacket::V3(v3::Packet::ConnectAck(v3::ConnectAck {
                                return_code: v3::ConnectAckReason::ConnectionAccepted,
                                session_present: resumed,
                            })))
                            .await
                            .unwrap();
                        for p in pending {
                            framed.send(p).await.unwrap();
                        }
                    }
                    MqttPacket::V3(v3::Packet::Subscribe { packet_id, topic_filters }) => {
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
                    // The client's PUBREC for our injected PUBLISH: the
                    // outflow is now awaiting PUBCOMP; on connection death
                    // the PUBREL must be re-sent on resumption.
                    MqttPacket::V3(v3::Packet::PublishReceived { packet_id }) => {
                        pending_pubrel = Some(*packet_id);
                    }
                    MqttPacket::V3(v3::Packet::PublishComplete { .. }) => {
                        pending_pubrel = None;
                    }
                    _ => {}
                }
                if packets.send(packet).await.is_err() {
                    return;
                }
            }
            inject = inject_rx.recv() => {
                match inject {
                    Ok(raw) => {
                        use tokio::io::AsyncWriteExt;
                        if framed.get_mut().write_all(&raw).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    // Connection died with an incomplete QoS2 outflow: park the PUBREL for
    // resumption, like a real broker's session store would.
    if let Some(packet_id) = pending_pubrel
        && !client_id.is_empty()
    {
        store
            .lock()
            .unwrap()
            .entry(client_id)
            .or_default()
            .push(MqttPacket::V3(v3::Packet::PublishRelease { packet_id }));
    }
}

impl FakeBroker {
    async fn recv(&mut self) -> MqttPacket {
        tokio::time::timeout(Duration::from_secs(5), self.packets.recv())
            .await
            .expect("timed out waiting for fake broker packet")
            .expect("fake broker channel closed")
    }

    async fn recv_connect(&mut self) {
        match self.recv().await {
            MqttPacket::V3(v3::Packet::Connect(_)) => {}
            other => panic!("expected CONNECT, got {other:?}"),
        }
    }

    fn inject(&self, packet: MqttPacket) {
        let _ = self.inject.send(raw(packet));
    }

    /// Assert no packet arrives within the grace period.
    async fn expect_quiet(&mut self, grace: Duration) {
        assert!(
            tokio::time::timeout(grace, self.packets.recv())
                .await
                .is_err(),
            "broker received an unexpected packet during quiet period"
        );
    }
}

type Client = Framed<tokio::net::TcpStream, MqttCodec>;

async fn client_connect(addr: SocketAddr, client_id: &str) -> Client {
    common::v3_connect(addr, client_id).await
}

async fn expect(client: &mut Client, what: &str, pred: impl Fn(&MqttPacket) -> bool) -> MqttPacket {
    let packet = common::next_packet(client).await;
    assert!(pred(&packet), "expected {what}, got {packet:?}");
    packet
}

/// Freeze the single proxy session and thaw it in-process (dup'd fd), like
/// the other thaw tests.
async fn freeze_and_thaw(
    proxy: &traffic_director::Proxy,
    broker_addr: SocketAddr,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    let batch = proxy.freeze_all().await;
    assert_eq!(batch.frozen.len(), 1);
    let frozen = &batch.frozen[0];
    let dup_fd: RawFd = nix::unistd::dup(frozen.client_fd).unwrap();
    let snapshot = frozen.snapshot.clone();
    batch.finish();
    tokio::spawn(traffic_director::session::adopt_session(
        dup_fd,
        snapshot,
        broker_addr,
        SessionRegistry::new(),
    ))
}

async fn start_proxy(broker_addr: SocketAddr) -> (traffic_director::Proxy, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (traffic_director::Proxy::start(listener, broker_addr), addr)
}

// ---------- client → broker ----------

#[tokio::test]
async fn qos2_c2b_interrupt_before_pubrec() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-c1").await;
    broker.recv_connect().await;

    client.send(publish_qos2(1, "c1")).await.unwrap();
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::Publish(_)) => {}
        other => panic!("broker expected PUBLISH, got {other:?}"),
    }

    // Freeze before PUBREC: window holds C2BPublishSent.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;

    // Thaw retransmits the PUBLISH with DUP=1.
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::Publish(p)) => assert!(p.dup),
        other => panic!("expected retransmitted PUBLISH, got {other:?}"),
    }

    // The handshake completes across the thaw.
    broker.inject(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(1) }));
    expect(&mut client, "PUBREC", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishReceived { packet_id }) if *packet_id == id(1))
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(1) }))
        .await
        .unwrap();
    broker.inject(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(1) }));
    expect(&mut client, "PUBCOMP", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishComplete { packet_id }) if *packet_id == id(1))
    })
    .await;
    adopt.abort();
}

#[tokio::test]
async fn qos2_c2b_interrupt_after_pubrec() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-c2").await;
    broker.recv_connect().await;

    client.send(publish_qos2(1, "c2")).await.unwrap();
    broker.recv().await;
    broker.inject(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(1) }));
    expect(&mut client, "PUBREC", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishReceived { .. })
        )
    })
    .await;

    // Freeze before the client's PUBREL: state is still C2BPublishSent.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;

    // Thaw retransmits the PUBLISH (DUP); broker re-PUBRECs; the client
    // (holding sender state) answers with PUBREL; flow completes.
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::Publish(p)) => assert!(p.dup),
        other => panic!("expected retransmitted PUBLISH, got {other:?}"),
    }
    broker.inject(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(1) }));
    expect(&mut client, "second PUBREC", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishReceived { .. })
        )
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(1) }))
        .await
        .unwrap();
    broker.inject(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(1) }));
    expect(&mut client, "PUBCOMP", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishComplete { .. })
        )
    })
    .await;
    adopt.abort();
}

#[tokio::test]
async fn qos2_c2b_interrupt_after_pubrel() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-c3").await;
    broker.recv_connect().await;

    client.send(publish_qos2(1, "c3")).await.unwrap();
    broker.recv().await;
    broker.inject(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(1) }));
    expect(&mut client, "PUBREC", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishReceived { .. })
        )
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(1) }))
        .await
        .unwrap();
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::PublishRelease { .. }) => {}
        other => panic!("broker expected PUBREL, got {other:?}"),
    }

    // Freeze before PUBCOMP: window holds C2BPubrelSent.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;

    // Thaw retransmits the PUBREL itself (not the PUBLISH).
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::PublishRelease { packet_id }) => {
            assert_eq!(packet_id, id(1));
        }
        other => panic!("expected retransmitted PUBREL, got {other:?}"),
    }
    broker.inject(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(1) }));
    expect(&mut client, "PUBCOMP", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishComplete { .. })
        )
    })
    .await;
    adopt.abort();
}

#[tokio::test]
async fn qos2_c2b_interrupt_after_pubcomp() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-c4").await;
    broker.recv_connect().await;

    client.send(publish_qos2(1, "c4")).await.unwrap();
    broker.recv().await;
    broker.inject(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(1) }));
    expect(&mut client, "PUBREC", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishReceived { .. })
        )
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(1) }))
        .await
        .unwrap();
    broker.recv().await; // PUBREL
    broker.inject(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(1) }));
    expect(&mut client, "PUBCOMP", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishComplete { .. })
        )
    })
    .await;

    // Handshake complete: the window is empty, thaw retransmits nothing.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;
    broker.expect_quiet(Duration::from_millis(300)).await;

    common::v3_ping(&mut client).await;
    adopt.abort();
}

// ---------- broker → client ----------

#[tokio::test]
async fn qos2_b2c_interrupt_before_pubrec() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-s1").await;
    broker.recv_connect().await;

    broker.inject(publish_qos2(50, "s1"));
    expect(&mut client, "PUBLISH", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::Publish(p)) if p.qos == QoS::ExactlyOnce)
    })
    .await;

    // Freeze before the client's PUBREC: window holds B2CPublishSent.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;

    // Thaw replays the PUBLISH to the client with DUP=1.
    expect(&mut client, "replayed PUBLISH", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::Publish(p)) if p.dup && p.packet_id == Some(id(50)))
    })
    .await;

    // The client answers the replay; the handshake completes.
    client
        .send(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(50) }))
        .await
        .unwrap();
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::PublishReceived { packet_id }) => {
            assert_eq!(packet_id, id(50));
        }
        other => panic!("broker expected PUBREC, got {other:?}"),
    }
    broker.inject(MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(50) }));
    expect(&mut client, "PUBREL", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishRelease { .. })
        )
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(50) }))
        .await
        .unwrap();
    adopt.abort();
}

#[tokio::test]
async fn qos2_b2c_interrupt_after_pubrec() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-s2").await;
    broker.recv_connect().await;

    broker.inject(publish_qos2(51, "s2"));
    expect(&mut client, "PUBLISH", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::Publish(_)))
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(51) }))
        .await
        .unwrap();
    match broker.recv().await {
        MqttPacket::V3(v3::Packet::PublishReceived { .. }) => {}
        other => panic!("broker expected PUBREC, got {other:?}"),
    }

    // Freeze before the broker's PUBREL: state B2CPubrecForwarded, nothing
    // to replay. The old broker connection dies with the outflow pending, so
    // on the (clean=false) replayed CONNECT the broker re-sends PUBREL.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;

    expect(&mut client, "re-sent PUBREL", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishRelease { packet_id }) if *packet_id == id(51))
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(51) }))
        .await
        .unwrap();
    adopt.abort();
}

#[tokio::test]
async fn qos2_b2c_interrupt_after_pubrel() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-s3").await;
    broker.recv_connect().await;

    broker.inject(publish_qos2(52, "s3"));
    expect(&mut client, "PUBLISH", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::Publish(_)))
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(52) }))
        .await
        .unwrap();
    broker.recv().await; // PUBREC at broker
    broker.inject(MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(52) }));
    expect(&mut client, "PUBREL", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishRelease { .. })
        )
    })
    .await;

    // Freeze before the client's PUBCOMP. Nothing to replay; broker
    // resumption re-sends PUBREL, client PUBCOMPs.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;

    expect(&mut client, "re-sent PUBREL", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishRelease { packet_id }) if *packet_id == id(52))
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(52) }))
        .await
        .unwrap();
    adopt.abort();
}

#[tokio::test]
async fn qos2_b2c_interrupt_after_pubcomp() {
    let mut broker = start_fake_broker().await;
    let (proxy, proxy_addr) = start_proxy(broker.addr).await;
    let mut client = client_connect(proxy_addr, "q2-s4").await;
    broker.recv_connect().await;

    broker.inject(publish_qos2(53, "s4"));
    expect(&mut client, "PUBLISH", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::Publish(_)))
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(53) }))
        .await
        .unwrap();
    broker.recv().await;
    broker.inject(MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(53) }));
    expect(&mut client, "PUBREL", |p| {
        matches!(p, MqttPacket::V3(v3::Packet::PublishRelease { .. })
        )
    })
    .await;
    client
        .send(MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(53) }))
        .await
        .unwrap();
    broker.recv().await; // PUBCOMP at broker

    // Complete: thaw must be quiet on both sides.
    let adopt = freeze_and_thaw(&proxy, broker.addr).await;
    broker.recv_connect().await;
    broker.expect_quiet(Duration::from_millis(300)).await;
    common::v3_ping(&mut client).await;
    adopt.abort();
}
