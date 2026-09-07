//! QoS in-flight window survival across a migrate shed.
//!
//! Test 1 (client→broker): a proxy client publishes 300 QoS1 messages while a
//! shed lands mid-stream; a broker-direct observer must receive every
//! sequence number (duplicates are protocol-normal for QoS1 retransmission).

mod common;

use std::collections::HashSet;
use std::num::NonZeroU16;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{
    BIN, Cleanup, broker_addr, free_port, next_packet, require_broker, signal, v3_connect,
    wait_connectable, wait_exit,
};
use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::{MqttPacket, v3};

const MESSAGE_COUNT: u16 = 300;

fn publish_packet(id: u16, seq: u16) -> MqttPacket {
    MqttPacket::V3(v3::Packet::Publish(Box::new(Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "win/c2b".into(),
        packet_id: NonZeroU16::new(id),
        payload: bytes::Bytes::from(format!("seq:{seq}")),
        properties: None,
    })))
}

fn parse_seq(packet: &MqttPacket) -> Option<u16> {
    let payload = match packet {
        MqttPacket::V3(v3::Packet::Publish(p)) => &p.payload,
        _ => return None,
    };
    std::str::from_utf8(payload).ok()?
        .strip_prefix("seq:")?
        .parse()
        .ok()
}

#[tokio::test]
async fn qos1_window_survives_shed_without_message_loss() {
    require_broker().await;

    // Observer subscribes directly at the broker, bypassing the proxy, so its
    // subscription is unaffected by the shed.
    let mut observer = v3_connect(broker_addr(), "win-observer").await;
    observer
        .send(MqttPacket::V3(v3::Packet::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            topic_filters: vec![("win/#".into(), QoS::AtLeastOnce)],
        }))
        .await
        .unwrap();
    match next_packet(&mut observer).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { .. }) => {}
        other => panic!("observer subscribe failed: {other:?}"),
    }

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

    let publisher = v3_connect(listen, "win-publisher").await;
    let (mut sink, mut stream) = publisher.split();

    // Count distinct PUBACKed packet ids until all are accounted for.
    let ack_reader = tokio::spawn(async move {
        let mut acked = HashSet::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        while acked.len() < MESSAGE_COUNT as usize && Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                Ok(Some(Ok((MqttPacket::V3(v3::Packet::PublishAck { packet_id }), _)))) => {
                    acked.insert(packet_id.get());
                }
                Ok(Some(Ok((_other, _)))) => {}
                Ok(Some(Err(e))) => panic!("publisher stream error: {e}"),
                Ok(None) => panic!("publisher stream closed with {} acks", acked.len()),
                Err(_) => break,
            }
        }
        acked
    });

    // Shed lands mid-stream.
    let shed = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        signal(parent.id(), "-USR2");
        wait_exit(&mut parent, Duration::from_secs(10))
            .expect("parent did not exit after migrate shed")
    });

    // Publish paced at 1ms so the shed (at 120ms) lands mid-stream with
    // ~180 messages still flowing.
    let sender = tokio::spawn(async move {
        for i in 1..=MESSAGE_COUNT {
            sink.send(publish_packet(i, i - 1))
                .await
                .expect("publish send failed");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    sender.await.unwrap();
    let acked = ack_reader.await.unwrap();
    let shed_status = shed.await.unwrap();
    assert!(shed_status.success(), "parent exited with {shed_status}");
    assert_eq!(
        acked.len(),
        MESSAGE_COUNT as usize,
        "client never received PUBACKs for all messages"
    );

    // Drain the observer until the stream goes quiet, PUBACKing each routed
    // message so the broker keeps delivering (default in-flight cap is 20).
    let mut received = HashSet::new();
    let mut duplicates = 0usize;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), observer.next()).await {
            Ok(Some(Ok((packet, _)))) => {
                if let Some(seq) = parse_seq(&packet)
                    && !received.insert(seq)
                {
                    duplicates += 1;
                }
                if let MqttPacket::V3(v3::Packet::Publish(p)) = &packet
                    && p.qos == QoS::AtLeastOnce
                    && let Some(packet_id) = p.packet_id
                {
                    observer
                        .send(MqttPacket::V3(v3::Packet::PublishAck { packet_id }))
                        .await
                        .unwrap();
                }
            }
            Ok(Some(Err(e))) => panic!("observer stream error: {e}"),
            Ok(None) => panic!("observer stream closed"),
            Err(_) => break, // quiet: all messages delivered
        }
    }

    let missing: Vec<u16> = (0..MESSAGE_COUNT)
        .filter(|seq| !received.contains(seq))
        .collect();
    assert!(
        missing.is_empty(),
        "observer missed {} message(s), first few: {missing:?}",
        missing.len()
    );
    eprintln!("observer received all {MESSAGE_COUNT} messages with {duplicates} duplicate(s)");
}
