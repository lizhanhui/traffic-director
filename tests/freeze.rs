//! In-process test of session freeze/resume — the core mechanic that the
//! migrate shed (PoC 2) is built on: sessions must quiesce at a packet
//! boundary, yield a transferable snapshot, and resume on demand (the
//! resume path doubles as the upgrade-failure rollback).

mod common;

use std::time::Duration;

use common::{broker_addr, next_packet, require_broker, v3_connect, v3_ping};
use futures::SinkExt;
use rmqtt_codec::{MqttCodec, MqttPacket, v3};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::codec::Decoder;

#[tokio::test]
async fn frozen_sessions_hold_and_resume() {
    require_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let proxy = traffic_director::Proxy::start(listener, broker_addr());

    let mut client = v3_connect(addr, "freeze-1").await;
    v3_ping(&mut client).await;

    let batch = proxy.freeze_all().await;
    assert_eq!(batch.frozen.len(), 1, "expected exactly one frozen session");
    let snapshot = &batch.frozen[0].snapshot;
    assert_eq!(snapshot.version, 4, "expected MQTT 3.1.1 (protocol level 4)");

    // The snapshot's CONNECT bytes must decode back to the client's CONNECT.
    let mut codec = MqttCodec::V3(v3::Codec::new(common::MAX_PACKET));
    let mut buf = bytes::BytesMut::from(&snapshot.connect_raw[..]);
    let (packet, _) = codec.decode(&mut buf).unwrap().expect("CONNECT did not decode");
    match packet {
        MqttPacket::V3(v3::Packet::Connect(connect)) => {
            assert_eq!(&connect.client_id[..], "freeze-1");
        }
        other => panic!("expected Connect in snapshot, got {other:?}"),
    }

    // While frozen, the session must not answer: a PINGREQ goes unanswered.
    client
        .send(MqttPacket::V3(v3::Packet::PingRequest))
        .await
        .unwrap();
    let answered = timeout(Duration::from_millis(300), next_packet(&mut client)).await;
    assert!(answered.is_err(), "session answered while frozen");

    // Resume: the queued PINGREQ is processed, and the session keeps working.
    batch.resume_all();
    match next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::PingResponse) => {}
        other => panic!("expected PingResponse after resume, got {other:?}"),
    }
    v3_ping(&mut client).await;
}
