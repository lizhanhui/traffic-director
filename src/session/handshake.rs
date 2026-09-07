//! Client CONNECT handling: version detection, CONNECT validation, raw
//! byte capture for later replay, and the clean-bit patch used on replay.

use std::io;

use bytes::BytesMut;
use futures::StreamExt;
use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::{MqttCodec, MqttPacket, v3, v5};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use super::{MqttFramed, codec_for, invalid_data};

/// Clear the clean_session (v3) / clean_start (v5) bit — bit 1 of the CONNECT
/// flags byte — in a raw CONNECT packet, so the broker resumes the session
/// when the child replays it.
pub(super) fn force_session_resumption(connect_raw: &mut [u8]) -> io::Result<()> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "malformed CONNECT in snapshot");
    // Skip the fixed-header byte and the remaining-length varint.
    let mut i = 1;
    loop {
        let byte = *connect_raw.get(i).ok_or_else(invalid)?;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    // Protocol name length (2 bytes) + name + protocol level (1) → flags byte.
    let name_len = u16::from_be_bytes([
        *connect_raw.get(i).ok_or_else(invalid)?,
        *connect_raw.get(i + 1).ok_or_else(invalid)?,
    ]) as usize;
    let flags = connect_raw.get_mut(i + 2 + name_len + 1).ok_or_else(invalid)?;
    *flags &= !0x02;
    Ok(())
}

pub(super) struct Handshake {
    pub(super) version: ProtocolVersion,
    pub(super) connect_raw: Vec<u8>,
    /// Client-requested keepalive in seconds (0 = disabled). Since the proxy
    /// terminates keepalive locally, it must also enforce the 1.5× timeout.
    pub(super) keep_alive: u16,
}

fn keep_alive_of(packet: &MqttPacket) -> u16 {
    match packet {
        MqttPacket::V3(v3::Packet::Connect(c)) => c.keep_alive,
        MqttPacket::V5(v5::Packet::Connect(c)) => c.keep_alive,
        _ => 0,
    }
}

impl Handshake {
    //
    pub(super) fn version_byte(&self) -> u8 {
        match self.version {
            ProtocolVersion::MQTT3 => 4,
            ProtocolVersion::MQTT5 => 5,
        }
    }

    pub(super) fn from_snapshot_version(version: u8, connect_raw: Vec<u8>) -> io::Result<Self> {
        let version = match version {
            4 => ProtocolVersion::MQTT3,
            5 => ProtocolVersion::MQTT5,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown MQTT protocol level {other}"),
                ));
            }
        };
        // Recover the keepalive by decoding the CONNECT snapshot.
        let mut codec = codec_for(version);
        let mut buf = BytesMut::from(&connect_raw[..]);
        let (packet, _) = codec
            .decode(&mut buf)
            .map_err(invalid_data)?
            .ok_or_else(|| invalid_data("undecodable CONNECT in snapshot"))?;
        Ok(Self {
            version,
            keep_alive: keep_alive_of(&packet),
            connect_raw,
        })
    }
}

/// Read and validate the client CONNECT, preserving the raw bytes for
/// later replay toward a broker on thaw.
pub(super) async fn handshake(client: TcpStream) -> io::Result<(MqttFramed, Handshake)> {
    // Peek the protocol version without consuming the CONNECT bytes.
    let mut client = Framed::new(client, MqttCodec::Version(rmqtt_codec::version::VersionCodec));
    let version = match client.next().await {
        Some(Ok((MqttPacket::Version(v), _))) => v,
        Some(Ok(_)) => unreachable!("VersionCodec only yields Version packets"),
        Some(Err(e)) => return Err(invalid_data(e)),
        None => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before CONNECT",
            ));
        }
    };

    // Swap in the version-specific codec, preserving buffered bytes.
    let mut parts = client.into_parts();
    parts.codec = codec_for(version);
    let mut client = Framed::from_parts(parts);

    // The CONNECT packet is still in the read buffer: decode it fully.
    let connect = match client.next().await {
        Some(Ok((p, _))) => p,
        Some(Err(e)) => return Err(invalid_data(e)),
        None => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before CONNECT",
            ));
        }
    };

    // Re-encode CONNECT so a future generation can replay it verbatim.
    let keep_alive = keep_alive_of(&connect);
    let mut connect_raw = BytesMut::new();
    codec_for(version)
        .encode(connect, &mut connect_raw)
        .map_err(invalid_data)?;

    Ok((
        client,
        Handshake {
            version,
            keep_alive,
            connect_raw: connect_raw.to_vec(),
        },
    ))
}
