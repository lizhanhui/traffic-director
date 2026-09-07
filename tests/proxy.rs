//! Integration tests for the proxy session core against a real mosquitto
//! broker (container `mosquitto1`, published at 127.0.0.1:15883).

mod common;

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::time::Duration;

use common::{broker_addr, next_packet, require_broker, v3_connect, v5_connect};
use futures::SinkExt;
use rmqtt_codec::types::{Publish, QoS};
use rmqtt_codec::v5::SubscriptionOptions;
use rmqtt_codec::{MqttPacket, v3, v5};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

async fn start_proxy() -> SocketAddr {
    require_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(traffic_director::serve(listener, broker_addr()));
    addr
}

#[tokio::test]
async fn v3_client_can_subscribe_and_publish_through_proxy() {
    let proxy = start_proxy().await;
    let mut client = v3_connect(proxy, "poc1-v3-sub-pub").await;

    // SUBSCRIBE and expect SUBACK chained back from the broker.
    client
        .send(MqttPacket::V3(v3::Packet::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            topic_filters: vec![("poc1/v3/#".into(), QoS::AtLeastOnce)],
        }))
        .await
        .unwrap();
    match next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::SubscribeAck { packet_id, .. }) => {
            assert_eq!(packet_id.get(), 1);
        }
        other => panic!("expected SubscribeAck, got {other:?}"),
    }

    // PUBLISH QoS1 and expect PUBACK chained back from the broker.
    let publish = Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "poc1/v3/x".into(),
        packet_id: NonZeroU16::new(2),
        payload: bytes::Bytes::from_static(b"hello-poc1"),
        properties: None,
    };
    client
        .send(MqttPacket::V3(v3::Packet::Publish(Box::new(publish))))
        .await
        .unwrap();

    // Since we are subscribed, the broker routes the message back to us;
    // the PUBACK for our publish arrives as well (order between them is
    // broker-defined, so accept both in any order).
    let mut got_puback = false;
    let mut got_routed_publish = false;
    for _ in 0..2 {
        match next_packet(&mut client).await {
            MqttPacket::V3(v3::Packet::PublishAck { packet_id }) => {
                assert_eq!(packet_id.get(), 2);
                got_puback = true;
            }
            MqttPacket::V3(v3::Packet::Publish(p)) => {
                assert_eq!(&p.topic[..], "poc1/v3/x");
                assert_eq!(p.payload.as_ref(), b"hello-poc1");
                got_routed_publish = true;
            }
            other => panic!("unexpected packet: {other:?}"),
        }
    }
    assert!(got_puback, "never received PUBACK");
    assert!(got_routed_publish, "never received routed PUBLISH");
}

#[tokio::test]
async fn v5_client_can_subscribe_and_publish_through_proxy() {
    let proxy = start_proxy().await;
    let mut client = v5_connect(proxy, "poc1-v5-sub-pub").await;

    client
        .send(MqttPacket::V5(v5::Packet::Subscribe(v5::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![("poc1/v5/#".into(), SubscriptionOptions {
                qos: QoS::AtLeastOnce,
                ..Default::default()
            })],
        })))
        .await
        .unwrap();
    match next_packet(&mut client).await {
        MqttPacket::V5(v5::Packet::SubscribeAck(ack)) => {
            assert_eq!(ack.packet_id.get(), 1);
        }
        other => panic!("expected v5 SubscribeAck, got {other:?}"),
    }

    let publish = Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: "poc1/v5/x".into(),
        packet_id: NonZeroU16::new(2),
        payload: bytes::Bytes::from_static(b"hello-poc1-v5"),
        properties: None,
    };
    client
        .send(MqttPacket::V5(v5::Packet::Publish(Box::new(publish))))
        .await
        .unwrap();

    let mut got_puback = false;
    let mut got_routed_publish = false;
    for _ in 0..2 {
        match next_packet(&mut client).await {
            MqttPacket::V5(v5::Packet::PublishAck(ack)) => {
                assert_eq!(ack.packet_id.get(), 2);
                got_puback = true;
            }
            MqttPacket::V5(v5::Packet::Publish(p)) => {
                assert_eq!(&p.topic[..], "poc1/v5/x");
                assert_eq!(p.payload.as_ref(), b"hello-poc1-v5");
                got_routed_publish = true;
            }
            other => panic!("unexpected packet: {other:?}"),
        }
    }
    assert!(got_puback, "never received v5 PUBACK");
    assert!(got_routed_publish, "never received routed v5 PUBLISH");
}

#[tokio::test]
async fn proxy_closes_session_when_client_disconnects() {
    let proxy = start_proxy().await;
    let client = v3_connect(proxy, "poc1-v3-disconnect").await;

    // Dropping the client closes the TCP connection; the proxy session must
    // notice and terminate instead of hanging. We observe this indirectly:
    // a second client with the SAME client_id must be able to connect and
    // take over the broker session without being kicked repeatedly.
    drop(client);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client2 = v3_connect(proxy, "poc1-v3-disconnect").await;
    common::v3_ping(&mut client2).await;
}

#[tokio::test]
async fn proxy_rejects_garbage_first_packet() {
    let proxy = start_proxy().await;
    let mut stream = TcpStream::connect(proxy).await.unwrap();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"this is not mqtt at all, padded out past thirty-two bytes...")
        .await
        .unwrap();

    // The proxy must close the connection without proxying anything.
    let mut buf = [0u8; 64];
    let result = timeout(common::TIMEOUT, async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break Ok(()),
                Ok(_) => continue,
                Err(e) if e.kind() == ErrorKind::ConnectionReset => break Ok(()),
                Err(e) => break Err(e),
            }
        }
    })
    .await;
    assert!(result.is_ok(), "proxy did not close the garbage connection");
}
