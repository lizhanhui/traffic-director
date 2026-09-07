//! Per-connection MQTT session: terminates MQTT on the client side and
//! originates a corresponding session toward the backend broker.
//!
//! PoC 1: handshake forwarding plus transparent decode/re-encode forwarding
//! in both directions. Packet identifiers pass through unchanged because each
//! client session owns exactly one broker-side connection, so the broker's
//! QoS acknowledgements chain end-to-end.
//!
//! PoC 2 (plumbing): sessions can freeze at a packet boundary, yielding a
//! transferable [`SessionSnapshot`] plus the client socket FD, and can resume
//! in place (the upgrade-failure rollback path).

use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::sync::Arc;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::{MqttCodec, MqttPacket, v3, v5};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{Decoder, Encoder, Framed, FramedParts};

use crate::registry::{
    FrozenSession, ResumeAction, SessionControl, SessionRegistry, SessionSnapshot,
};

const MAX_PACKET_SIZE: u32 = 1024 * 1024;

pub type MqttFramed = Framed<TcpStream, MqttCodec>;

pub fn codec_for(version: ProtocolVersion) -> MqttCodec {
    match version {
        ProtocolVersion::MQTT3 => MqttCodec::V3(v3::Codec::new(MAX_PACKET_SIZE)),
        ProtocolVersion::MQTT5 => MqttCodec::V5(v5::Codec::new(MAX_PACKET_SIZE, MAX_PACKET_SIZE)),
    }
}

fn invalid_data(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

struct Handshake {
    version: ProtocolVersion,
    connect_raw: Vec<u8>,
}

impl Handshake {
    fn version_byte(&self) -> u8 {
        match self.version {
            ProtocolVersion::MQTT3 => 4,
            ProtocolVersion::MQTT5 => 5,
        }
    }

    fn from_snapshot_version(version: u8, connect_raw: Vec<u8>) -> io::Result<Self> {
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
        Ok(Self {
            version,
            connect_raw,
        })
    }
}

/// Read and validate the client CONNECT, preserving the raw bytes for
/// later replay toward a broker on thaw.
async fn handshake(client: TcpStream) -> io::Result<(MqttFramed, Handshake)> {
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
    let mut connect_raw = BytesMut::new();
    codec_for(version)
        .encode(connect, &mut connect_raw)
        .map_err(invalid_data)?;

    Ok((
        client,
        Handshake {
            version,
            connect_raw: connect_raw.to_vec(),
        },
    ))
}

/// Removes the session from the registry when the task ends for any reason.
struct Registration {
    registry: Arc<SessionRegistry>,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry.unregister(self.id);
    }
}

/// Run one proxied session: handshake, broker-side session establishment,
/// then bidirectional forwarding until close or a terminating freeze.
pub async fn run_session(
    client: TcpStream,
    broker_addr: SocketAddr,
    registry: Arc<SessionRegistry>,
) -> io::Result<()> {
    let (mut client, hs) = match handshake(client).await {
        Ok(v) => v,
        Err(e) => return Err(e),
    };

    // Originate the broker-side session by replaying the CONNECT bytes, then
    // chain the acknowledgement back to the client.
    let broker = TcpStream::connect(broker_addr).await?;
    let mut broker = Framed::new(broker, codec_for(hs.version));
    {
        use tokio::io::AsyncWriteExt;
        broker.get_mut().write_all(&hs.connect_raw).await?;
        broker.get_mut().flush().await?;
    }

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

    // Only established sessions are registered (and thus migratable).
    let (id, control) = registry.register();
    let _registration = Registration {
        registry,
        id,
    };

    forward_loop(id, client, broker, hs, control).await
}

async fn forward_loop(
    id: u64,
    mut client: MqttFramed,
    mut broker: MqttFramed,
    hs: Handshake,
    mut control: mpsc::Receiver<SessionControl>,
) -> io::Result<()> {
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
            req = control.recv() => match req {
                Some(SessionControl::Freeze(req)) => {
                    let mut cp = client.into_parts();
                    let mut bp = broker.into_parts();
                    let snapshot = SessionSnapshot {
                        version: hs.version_byte(),
                        connect_raw: hs.connect_raw.clone(),
                        client_buf: cp.read_buf.split().to_vec(),
                        broker_buf: bp.read_buf.split().to_vec(),
                    };
                    let mut frozen = FrozenSession {
                        id,
                        snapshot,
                        client_fd: cp.io.as_raw_fd(),
                    };
                    if let Err(returned) = req.reply.send(frozen) {
                        // Coordinator gave up on us: restore and carry on.
                        frozen = returned;
                        cp.read_buf.extend_from_slice(&frozen.snapshot.client_buf);
                        bp.read_buf.extend_from_slice(&frozen.snapshot.broker_buf);
                        client = Framed::from_parts(cp);
                        broker = Framed::from_parts(bp);
                        continue;
                    }
                    match req.resume.await {
                        Ok(ResumeAction::Resume(frozen)) => {
                            // Rollback: restore buffers and continue.
                            cp.read_buf.extend_from_slice(&frozen.snapshot.client_buf);
                            bp.read_buf.extend_from_slice(&frozen.snapshot.broker_buf);
                            client = Framed::from_parts(cp);
                            broker = Framed::from_parts(bp);
                        }
                        Err(_) => return Ok(()), // handoff done; process is exiting
                    }
                }
                None => return Ok(()), // registry gone: process is shutting down
            },
        }
    }
}

/// Adopt a migrated session (child side of a shed): take ownership of the
/// client socket FD, re-establish the broker-side connection by replaying the
/// original CONNECT, deliver any buffered broker bytes to the client, then
/// resume normal forwarding.
///
/// TODO(next milestone): force clean_start/clean_session=false on the
/// replayed CONNECT and restore subscription/QoS state.
pub async fn adopt_session(
    client_fd: RawFd,
    snapshot: SessionSnapshot,
    broker_addr: SocketAddr,
    registry: Arc<SessionRegistry>,
) -> io::Result<()> {
    let hs = Handshake::from_snapshot_version(snapshot.version, snapshot.connect_raw.clone())?;

    // Adopt the client socket.
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(client_fd) };
    std_stream.set_nonblocking(true)?;
    let client_stream = TcpStream::from_std(std_stream)?;
    let mut parts = FramedParts::new(client_stream, codec_for(hs.version));
    parts.read_buf.extend_from_slice(&snapshot.client_buf);
    let mut client = Framed::from_parts(parts);

    // Re-establish the broker side; the CONNACK stays proxy-local since the
    // client never disconnected.
    let broker = TcpStream::connect(broker_addr).await?;
    let mut broker = Framed::new(broker, codec_for(hs.version));
    {
        use tokio::io::AsyncWriteExt;
        broker.get_mut().write_all(&snapshot.connect_raw).await?;
        broker.get_mut().flush().await?;
    }
    match broker.next().await {
        Some(Ok((MqttPacket::V3(v3::Packet::ConnectAck(_)), _)))
        | Some(Ok((MqttPacket::V5(v5::Packet::ConnectAck(_)), _))) => {}
        Some(Ok((other, _))) => {
            return Err(invalid_data(format!("broker answered CONNECT replay with {other:?}")));
        }
        Some(Err(e)) => return Err(invalid_data(e)),
        None => {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "broker closed during CONNECT replay",
            ));
        }
    }

    // Deliver packets the old broker connection had buffered but not yet
    // forwarded; a trailing partial frame is dropped (QoS0 only — QoS1/2
    // arrives via broker redelivery once session resumption lands).
    let mut leftover = BytesMut::from(&snapshot.broker_buf[..]);
    let mut codec = codec_for(hs.version);
    loop {
        match codec.decode(&mut leftover) {
            Ok(Some((packet, _))) => client.send(packet).await.map_err(invalid_data)?,
            Ok(None) => break,
            Err(e) => {
                log::warn!("dropping undecodable broker buffer on thaw: {e}");
                break;
            }
        }
    }
    if !leftover.is_empty() {
        log::warn!("dropping {} partial broker bytes on thaw", leftover.len());
    }

    // Register so the adopted session is migratable on the next shed.
    let (id, control) = registry.register();
    let _registration = Registration { registry, id };

    forward_loop(id, client, broker, hs, control).await
}
