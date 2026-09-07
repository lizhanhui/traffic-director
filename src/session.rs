//! Per-connection MQTT session: terminates MQTT on the client side and
//! originates a corresponding session toward the backend broker.
//!
//! Skeleton scope (PoC 1): handshake forwarding plus transparent
//! decode/re-encode forwarding in both directions. Packet identifiers pass
//! through unchanged because each client session owns exactly one broker-side
//! connection, so the broker's QoS acknowledgements chain end-to-end.

use std::io;
use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};
use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::{MqttCodec, MqttPacket, v3, v5};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

const MAX_PACKET_SIZE: u32 = 1024 * 1024;

fn codec_for(version: ProtocolVersion) -> MqttCodec {
    match version {
        ProtocolVersion::MQTT3 => MqttCodec::V3(v3::Codec::new(MAX_PACKET_SIZE)),
        ProtocolVersion::MQTT5 => MqttCodec::V5(v5::Codec::new(MAX_PACKET_SIZE, MAX_PACKET_SIZE)),
    }
}

fn invalid_data(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// Read the client CONNECT, establish the broker-side connection with the same
/// parameters, then forward packets in both directions until either side
/// closes.
pub async fn run_session(client: TcpStream, broker_addr: SocketAddr) -> io::Result<()> {
    // Peek the protocol version without consuming the CONNECT bytes.
    let mut client = Framed::new(client, MqttCodec::Version(rmqtt_codec::version::VersionCodec));
    let version = match client.next().await {
        Some(Ok((MqttPacket::Version(v), _))) => v,
        Some(Ok(_)) => unreachable!("VersionCodec only yields Version packets"),
        Some(Err(e)) => return Err(invalid_data(e)),
        None => return Ok(()), // client went away before CONNECT
    };

    // Swap in the version-specific codec, preserving buffered bytes.
    let mut parts = client.into_parts();
    parts.codec = codec_for(version);
    let mut client = Framed::from_parts(parts);

    // The CONNECT packet is still in the read buffer: decode it fully.
    let connect = match client.next().await {
        Some(Ok((p, _))) => p,
        Some(Err(e)) => return Err(invalid_data(e)),
        None => return Ok(()),
    };

    // Originate the broker-side session and chain the acknowledgement.
    let broker = TcpStream::connect(broker_addr).await?;
    let mut broker = Framed::new(broker, codec_for(version));
    broker.send(connect).await.map_err(invalid_data)?;

    let ack = match broker.next().await {
        Some(Ok((p, _))) => p,
        Some(Err(e)) => return Err(invalid_data(e)),
        None => {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "broker closed during handshake",
            ));
        }
    };
    client.send(ack).await.map_err(invalid_data)?;

    // Full-duplex forwarding; either side closing ends the session.
    loop {
        tokio::select! {
            next = client.next() => match next {
                Some(Ok((p, _))) => broker.send(p).await.map_err(invalid_data)?,
                Some(Err(e)) => return Err(invalid_data(e)),
                None => return Ok(()),
            },
            next = broker.next() => match next {
                Some(Ok((p, _))) => client.send(p).await.map_err(invalid_data)?,
                Some(Err(e)) => return Err(invalid_data(e)),
                None => return Ok(()),
            },
        }
    }
}
