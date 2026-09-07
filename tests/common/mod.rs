//! Shared helpers for integration tests: minimal MQTT clients speaking
//! through the proxy to a real mosquitto broker.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use rmqtt_codec::{MqttCodec, MqttPacket, v3, v5};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

pub const BROKER: &str = "127.0.0.1:15883";
pub const MAX_PACKET: u32 = 1024 * 1024;
pub const TIMEOUT: Duration = Duration::from_secs(10);

pub const BIN: &str = env!("CARGO_BIN_EXE_traffic-director");

/// Kills any leftover traffic-director processes for a test's listen port,
/// even when the test panics.
pub struct Cleanup(pub String);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = Command::new("pkill")
            .args(["-TERM", "-f", &format!("traffic-director {}", self.0)])
            .status();
    }
}

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub fn signal(pid: u32, sig: &str) {
    let status = Command::new("kill")
        .args([sig, &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill {sig} {pid} failed");
}

pub async fn wait_connectable(addr: SocketAddr) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "proxy never started listening at {addr}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn wait_exit(child: &mut Child, within: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub fn broker_addr() -> SocketAddr {
    BROKER.parse().unwrap()
}

/// Fail fast with a clear message if the mosquitto container isn't running.
pub async fn require_broker() {
    TcpStream::connect(broker_addr()).await.unwrap_or_else(|e| {
        panic!("mosquitto not reachable at {BROKER}: {e} — start the container first")
    });
}

pub type Client = Framed<TcpStream, MqttCodec>;

pub async fn next_packet(client: &mut Client) -> MqttPacket {
    timeout(TIMEOUT, client.next())
        .await
        .expect("timed out waiting for packet")
        .expect("connection closed unexpectedly")
        .expect("codec error")
        .0
}

pub async fn v3_connect(addr: SocketAddr, client_id: &str) -> Client {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut client = Framed::new(stream, MqttCodec::V3(v3::Codec::new(MAX_PACKET)));

    let connect = v3::Connect {
        clean_session: true,
        ..Default::default()
    }
    .client_id(client_id.to_owned());
    client
        .send(MqttPacket::V3(v3::Packet::Connect(Box::new(connect))))
        .await
        .unwrap();

    match next_packet(&mut client).await {
        MqttPacket::V3(v3::Packet::ConnectAck(ack)) => {
            assert_eq!(ack.return_code, v3::ConnectAckReason::ConnectionAccepted);
        }
        other => panic!("expected ConnectAck, got {other:?}"),
    }
    client
}

pub async fn v3_ping(client: &mut Client) {
    client
        .send(MqttPacket::V3(v3::Packet::PingRequest))
        .await
        .unwrap();
    match next_packet(client).await {
        MqttPacket::V3(v3::Packet::PingResponse) => {}
        other => panic!("expected PingResponse, got {other:?}"),
    }
}

pub async fn v5_connect(addr: SocketAddr, client_id: &str) -> Client {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut client = Framed::new(
        stream,
        MqttCodec::V5(v5::Codec::new(MAX_PACKET, MAX_PACKET)),
    );

    let connect = v5::Connect {
        clean_start: true,
        client_id: client_id.into(),
        ..Default::default()
    };
    client
        .send(MqttPacket::V5(v5::Packet::Connect(Box::new(connect))))
        .await
        .unwrap();

    match next_packet(&mut client).await {
        MqttPacket::V5(v5::Packet::ConnectAck(ack)) => {
            assert_eq!(ack.reason_code, v5::ConnectAckReason::Success);
        }
        other => panic!("expected v5 ConnectAck, got {other:?}"),
    }
    client
}
