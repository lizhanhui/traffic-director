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
    SubscriptionEntry,
};
use crate::window::{InflightWindows, encode_raw};

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

/// The session's active subscriptions, tracked from the packet flow and
/// re-issued toward the broker on thaw.
#[derive(Debug, Default)]
pub struct Subscriptions(Vec<SubscriptionEntry>);

impl Subscriptions {
    fn upsert(&mut self, topic_filter: &str, options: u8) {
        if let Some(entry) = self
            .0
            .iter_mut()
            .find(|e| e.topic_filter == topic_filter)
        {
            entry.options = options; // re-subscribing replaces the options
        } else {
            self.0.push(SubscriptionEntry {
                topic_filter: topic_filter.to_owned(),
                options,
            });
        }
    }

    fn remove(&mut self, topic_filter: &str) {
        self.0.retain(|e| e.topic_filter != topic_filter);
    }

    /// Track a client→broker packet.
    fn track(&mut self, packet: &MqttPacket) {
        match packet {
            MqttPacket::V3(v3::Packet::Subscribe { topic_filters, .. }) => {
                for (filter, qos) in topic_filters {
                    self.upsert(filter, *qos as u8);
                }
            }
            MqttPacket::V3(v3::Packet::Unsubscribe { topic_filters, .. }) => {
                for filter in topic_filters {
                    self.remove(filter);
                }
            }
            MqttPacket::V5(v5::Packet::Subscribe(subscribe)) => {
                for (filter, options) in &subscribe.topic_filters {
                    let byte = options.qos as u8
                        | (options.no_local as u8) << 2
                        | (options.retain_as_published as u8) << 3
                        | (options.retain_handling as u8) << 4;
                    self.upsert(filter, byte);
                }
            }
            MqttPacket::V5(v5::Packet::Unsubscribe(unsubscribe)) => {
                for filter in &unsubscribe.topic_filters {
                    self.remove(filter);
                }
            }
            _ => {}
        }
    }

    /// Build the re-SUBSCRIBE packet for thaw, or None if there are no
    /// subscriptions.
    fn resubscribe_packet(&self, version: ProtocolVersion) -> Option<MqttPacket> {
        if self.0.is_empty() {
            return None;
        }
        // Proxy-internal packet id; unrelated to any client-visible id.
        let packet_id = std::num::NonZeroU16::new(1).unwrap();
        let packet = match version {
            ProtocolVersion::MQTT3 => MqttPacket::V3(v3::Packet::Subscribe {
                packet_id,
                topic_filters: self
                    .0
                    .iter()
                    .map(|e| {
                        let qos = rmqtt_codec::types::QoS::try_from(e.options & 0x03)
                            .unwrap_or(rmqtt_codec::types::QoS::AtMostOnce);
                        (e.topic_filter.clone().into(), qos)
                    })
                    .collect(),
            }),
            ProtocolVersion::MQTT5 => MqttPacket::V5(v5::Packet::Subscribe(v5::Subscribe {
                packet_id,
                id: None,
                user_properties: Vec::new(),
                topic_filters: self
                    .0
                    .iter()
                    .map(|e| {
                        let options = v5::SubscriptionOptions {
                            qos: rmqtt_codec::types::QoS::try_from(e.options & 0x03)
                                .unwrap_or(rmqtt_codec::types::QoS::AtMostOnce),
                            no_local: e.options & 0x04 != 0,
                            retain_as_published: e.options & 0x08 != 0,
                            retain_handling: v5::RetainHandling::try_from(e.options >> 4)
                                .unwrap_or(v5::RetainHandling::AtSubscribe),
                        };
                        (e.topic_filter.clone().into(), options)
                    })
                    .collect(),
            })),
        };
        Some(packet)
    }
}

const BROKER_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_millis(100);
const BROKER_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(5);
const OUTAGE_BUFFER_MAX_BYTES: usize = 1024 * 1024;

/// Publishes received from the client while the broker was unreachable,
/// already acked locally and awaiting delivery after reconnect.
#[derive(Default)]
struct OutageBuffer {
    entries: Vec<crate::registry::BufferedPublish>,
    bytes: usize,
}

impl OutageBuffer {
    fn push(
        &mut self,
        packet_id: u16,
        raw: Vec<u8>,
        qos: rmqtt_codec::types::QoS,
    ) -> io::Result<()> {
        if self.bytes + raw.len() > OUTAGE_BUFFER_MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::QuotaExceeded,
                "outage buffer full, closing session",
            ));
        }
        self.bytes += raw.len();
        self.entries.push(crate::registry::BufferedPublish {
            packet_id,
            raw,
            qos: qos as u8,
        });
        Ok(())
    }
}

/// Broker ack packet ids that must NOT be forwarded to the client: the
/// client was already acked locally during the outage.
#[derive(Default)]
pub struct LocalAcks {
    qos1: std::collections::HashSet<u16>,
    qos2: std::collections::HashSet<u16>,
}

/// Answer and/or buffer one client packet received during a broker outage.
async fn service_outage_packet(
    client: &mut MqttFramed,
    packet: MqttPacket,
    buffer: &mut OutageBuffer,
    subscriptions: &mut Subscriptions,
) -> io::Result<()> {
    use rmqtt_codec::types::QoS;
    match packet {
        MqttPacket::V3(v3::Packet::Publish(p)) => {
                let packet_id = p.packet_id;
                let qos = p.qos;
                let raw = encode_raw(&MqttPacket::V3(v3::Packet::Publish(p)));
                match qos {
                    QoS::AtMostOnce => {} // at-most-once: may be dropped
                    QoS::AtLeastOnce => {
                        let packet_id = packet_id.expect("QoS1 PUBLISH has an id");
                        buffer.push(packet_id.get(), raw, qos)?;
                        client
                            .send(MqttPacket::V3(v3::Packet::PublishAck { packet_id }))
                            .await
                            .map_err(invalid_data)?;
                    }
                    QoS::ExactlyOnce => {
                        let packet_id = packet_id.expect("QoS2 PUBLISH has an id");
                        buffer.push(packet_id.get(), raw, qos)?;
                        client
                            .send(MqttPacket::V3(v3::Packet::PublishReceived { packet_id }))
                            .await
                            .map_err(invalid_data)?;
                    }
                }
        }
        MqttPacket::V3(v3::Packet::PublishRelease { packet_id }) => {
            client
                .send(MqttPacket::V3(v3::Packet::PublishComplete { packet_id }))
                .await
                .map_err(invalid_data)?;
        }
        MqttPacket::V3(v3::Packet::PingRequest) => {
            client
                .send(MqttPacket::V3(v3::Packet::PingResponse))
                .await
                .map_err(invalid_data)?;
        }
        p @ MqttPacket::V3(v3::Packet::Subscribe { .. }) => {
            subscriptions.track(&p);
            let MqttPacket::V3(v3::Packet::Subscribe {
                packet_id,
                topic_filters,
            }) = p
            else {
                unreachable!()
            };
            client
                .send(MqttPacket::V3(v3::Packet::SubscribeAck {
                    packet_id,
                    status: topic_filters
                        .iter()
                        .map(|(_, qos)| v3::SubscribeReturnCode::Success(*qos))
                        .collect(),
                }))
                .await
                .map_err(invalid_data)?;
        }
        p @ MqttPacket::V3(v3::Packet::Unsubscribe { .. }) => {
            subscriptions.track(&p);
            let MqttPacket::V3(v3::Packet::Unsubscribe { packet_id, .. }) = p else {
                unreachable!()
            };
            client
                .send(MqttPacket::V3(v3::Packet::UnsubscribeAck { packet_id }))
                .await
                .map_err(invalid_data)?;
        }
        MqttPacket::V3(v3::Packet::Disconnect) => {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "client disconnected"));
        }
        MqttPacket::V5(v5::Packet::Publish(p)) => {
                let packet_id = p.packet_id;
                let qos = p.qos;
                let raw = encode_raw(&MqttPacket::V5(v5::Packet::Publish(p)));
                match qos {
                    QoS::AtMostOnce => {}
                    QoS::AtLeastOnce => {
                        let packet_id = packet_id.expect("QoS1 PUBLISH has an id");
                        buffer.push(packet_id.get(), raw, qos)?;
                        client
                            .send(MqttPacket::V5(v5::Packet::PublishAck(v5::PublishAck {
                                packet_id,
                                reason_code: v5::PublishAckReason::Success,
                                properties: Vec::new(),
                                reason_string: None,
                            })))
                            .await
                            .map_err(invalid_data)?;
                    }
                    QoS::ExactlyOnce => {
                        let packet_id = packet_id.expect("QoS2 PUBLISH has an id");
                        buffer.push(packet_id.get(), raw, qos)?;
                        client
                            .send(MqttPacket::V5(v5::Packet::PublishReceived(v5::PublishAck {
                                packet_id,
                                reason_code: v5::PublishAckReason::Success,
                                properties: Vec::new(),
                                reason_string: None,
                            })))
                            .await
                            .map_err(invalid_data)?;
                    }
                }
        }
        MqttPacket::V5(v5::Packet::PublishRelease(ack)) => {
            client
                .send(MqttPacket::V5(v5::Packet::PublishComplete(v5::PublishAck2 {
                    packet_id: ack.packet_id,
                    reason_code: v5::PublishAck2Reason::Success,
                    properties: Vec::new(),
                    reason_string: None,
                })))
                .await
                .map_err(invalid_data)?;
        }
        MqttPacket::V5(v5::Packet::PingRequest) => {
            client
                .send(MqttPacket::V5(v5::Packet::PingResponse))
                .await
                .map_err(invalid_data)?;
        }
        p @ MqttPacket::V5(v5::Packet::Subscribe(_)) => {
            subscriptions.track(&p);
            let MqttPacket::V5(v5::Packet::Subscribe(subscribe)) = p else {
                unreachable!()
            };
            client
                .send(MqttPacket::V5(v5::Packet::SubscribeAck(v5::SubscribeAck {
                    packet_id: subscribe.packet_id,
                    properties: Vec::new(),
                    reason_string: None,
                    status: subscribe
                        .topic_filters
                        .iter()
                        .map(|(_, options)| match options.qos {
                            QoS::AtMostOnce => v5::SubscribeAckReason::GrantedQos0,
                            QoS::AtLeastOnce => v5::SubscribeAckReason::GrantedQos1,
                            QoS::ExactlyOnce => v5::SubscribeAckReason::GrantedQos2,
                        })
                        .collect(),
                })))
                .await
                .map_err(invalid_data)?;
        }
        p @ MqttPacket::V5(v5::Packet::Unsubscribe(_)) => {
            subscriptions.track(&p);
            let MqttPacket::V5(v5::Packet::Unsubscribe(unsubscribe)) = p else {
                unreachable!()
            };
            client
                .send(MqttPacket::V5(v5::Packet::UnsubscribeAck(v5::UnsubscribeAck {
                    packet_id: unsubscribe.packet_id,
                    properties: Vec::new(),
                    reason_string: None,
                    status: Vec::new(),
                })))
                .await
                .map_err(invalid_data)?;
        }
        MqttPacket::V5(v5::Packet::Disconnect(_)) => {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "client disconnected"));
        }
        _ => {} // client acks of broker publishes: nothing to do mid-outage
    }
    Ok(())
}

/// Replay the session onto a fresh broker connection: CONNECT with the clean
/// bit cleared, re-SUBSCRIBE, deliver leftover broker bytes to the client,
/// retransmit QoS windows, and flush the outage buffer (marking the flushed
/// packet ids as proxy-local acks to eat). Shared by mid-session reconnect
/// and thaw.
#[allow(clippy::too_many_arguments)]
async fn establish_broker(
    broker: &mut MqttFramed,
    hs: &Handshake,
    client: &mut MqttFramed,
    subscriptions: &Subscriptions,
    windows: &InflightWindows,
    local_acks: &mut LocalAcks,
    buffer: &mut OutageBuffer,
    broker_leftover: &mut BytesMut,
) -> io::Result<()> {
    // CONNECT replay with forced session resumption; CONNACK stays
    // proxy-local since the client never disconnected.
    let mut connect_raw = hs.connect_raw.clone();
    force_session_resumption(&mut connect_raw)?;
    {
        use tokio::io::AsyncWriteExt;
        broker.get_mut().write_all(&connect_raw).await?;
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

    // Re-SUBSCRIBE; proxy-internal, so the SUBACK is not forwarded.
    if let Some(packet) = subscriptions.resubscribe_packet(hs.version) {
        broker.send(packet).await.map_err(invalid_data)?;
        match broker.next().await {
            Some(Ok((MqttPacket::V3(v3::Packet::SubscribeAck { .. }), _)))
            | Some(Ok((MqttPacket::V5(v5::Packet::SubscribeAck(_)), _))) => {}
            Some(Ok((other, _))) => {
                log::warn!("re-SUBSCRIBE answered with {other:?}, continuing anyway");
            }
            Some(Err(e)) => return Err(invalid_data(e)),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "broker closed during re-SUBSCRIBE",
                ));
            }
        }
    }

    // Deliver complete packets the dead broker connection had buffered; a
    // trailing partial frame can never complete and is dropped (QoS0 only —
    // QoS1/2 is healed by window retransmits / broker redelivery).
    {
        let mut codec = codec_for(hs.version);
        loop {
            match codec.decode(broker_leftover) {
                Ok(Some((packet, _))) => client.send(packet).await.map_err(invalid_data)?,
                Ok(None) => break,
                Err(e) => {
                    log::warn!("dropping undecodable broker leftover: {e}");
                    break;
                }
            }
        }
        if !broker_leftover.is_empty() {
            log::warn!("dropping {} partial broker byte(s)", broker_leftover.len());
            broker_leftover.clear();
        }
    }

    // Retransmit QoS in-flight packets that never completed end-to-end, then
    // flush outage-buffered publishes in arrival order.
    {
        use tokio::io::AsyncWriteExt;
        for raw in windows.broker_retransmits() {
            broker.get_mut().write_all(&raw).await?;
        }
        for raw in windows.client_retransmits() {
            client.get_mut().write_all(&raw).await?;
        }
        if !buffer.entries.is_empty() {
            log::info!("flushing {} outage-buffered publish(es)", buffer.entries.len());
        }
        for entry in buffer.entries.drain(..) {
            match entry.qos {
                1 => {
                    local_acks.qos1.insert(entry.packet_id);
                }
                2 => {
                    local_acks.qos2.insert(entry.packet_id);
                }
                other => unreachable!("QoS{other} is never buffered"),
            }
            broker.get_mut().write_all(&entry.raw).await?;
        }
        buffer.bytes = 0;
        broker.get_mut().flush().await?;
        client.get_mut().flush().await?;
    }
    Ok(())
}

/// Outcome of servicing a freeze request.
#[allow(clippy::large_enum_variant)]
enum FreezeOutcome {
    Resume(MqttFramed, Option<MqttFramed>),
    Exit,
}

/// Freeze at a packet boundary: snapshot everything (broker side optional —
/// it may be down), reply to the coordinator, then either restore on
/// rollback or report exit after a successful handoff.
#[allow(clippy::too_many_arguments)]
async fn handle_freeze(
    req: crate::registry::FreezeRequest,
    id: u64,
    client: MqttFramed,
    broker: Option<MqttFramed>,
    hs: &Handshake,
    windows: &InflightWindows,
    subscriptions: &Subscriptions,
    buffer: &OutageBuffer,
) -> FreezeOutcome {
    let mut cp = client.into_parts();
    let mut bp = broker.map(Framed::into_parts);
    let snapshot = SessionSnapshot {
        version: hs.version_byte(),
        connect_raw: hs.connect_raw.clone(),
        client_buf: cp.read_buf.split().to_vec(),
        broker_buf: bp
            .as_mut()
            .map(|p| p.read_buf.split().to_vec())
            .unwrap_or_default(),
        windows: windows.snapshot(),
        subscriptions: subscriptions.0.clone(),
        buffered: buffer.entries.clone(),
    };
    let frozen = FrozenSession {
        id,
        snapshot,
        client_fd: cp.io.as_raw_fd(),
    };
    log::debug!(
        "session {id} frozen with {} window entr(ies), {} buffered",
        frozen.snapshot.windows.len(),
        frozen.snapshot.buffered.len()
    );

    let returned = match req.reply.send(frozen) {
        Err(returned) => returned, // coordinator gave up: restore below
        Ok(()) => match req.resume.await {
            Ok(ResumeAction::Resume(returned)) => *returned,
            Err(_) => return FreezeOutcome::Exit, // handoff done; exiting
        },
    };
    let frozen = returned;
    cp.read_buf.extend_from_slice(&frozen.snapshot.client_buf);
    if let Some(bp) = bp.as_mut() {
        bp.read_buf.extend_from_slice(&frozen.snapshot.broker_buf);
    }
    FreezeOutcome::Resume(
        Framed::from_parts(cp),
        bp.map(Framed::from_parts),
    )
}

/// Clear the clean_session (v3) / clean_start (v5) bit — bit 1 of the CONNECT
/// flags byte — in a raw CONNECT packet, so the broker resumes the session
/// when the child replays it.
fn force_session_resumption(connect_raw: &mut [u8]) -> io::Result<()> {
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

    forward_loop(
        id,
        client,
        Some(broker),
        broker_addr,
        hs,
        control,
        InflightWindows::new(),
        Subscriptions::default(),
        LocalAcks::default(),
        OutageBuffer::default(),
        BytesMut::new(),
    )
    .await
}

/// v5 server-side DISCONNECT reason codes that mean "go away, we're doing
/// maintenance" — the proxy intercepts these and reconnects quietly. All
/// other reasons (protocol errors, auth failures, …) are client faults and
/// must reach the client.
fn is_retryable_disconnect(reason: v5::DisconnectReasonCode) -> bool {
    use v5::DisconnectReasonCode as R;
    matches!(
        reason,
        R::NormalDisconnection
            | R::ServerShuttingDown
            | R::ServerBusy
            | R::UseAnotherServer
            | R::ServerMoved
    )
}

/// What the outage phase waits on next: a connect attempt, or a backoff
/// sleep after a failed one.
enum OutageStep {
    Connect,
    Backoff(std::time::Duration),
}

#[allow(clippy::too_many_arguments)]
async fn forward_loop(
    id: u64,
    mut client: MqttFramed,
    mut broker: Option<MqttFramed>,
    broker_addr: SocketAddr,
    hs: Handshake,
    mut control: mpsc::Receiver<SessionControl>,
    mut windows: InflightWindows,
    mut subscriptions: Subscriptions,
    mut local_acks: LocalAcks,
    mut buffer: OutageBuffer,
    mut broker_leftover: BytesMut,
) -> io::Result<()> {
    let mut backoff = BROKER_RETRY_INITIAL;
    let mut step = OutageStep::Connect;
    loop {
        if broker.is_some() {
            // ---- connected phase ----
            let mut b = broker.take().expect("checked above");
            tokio::select! {
                next = client.next() => match next {
                    Some(Ok((p, _))) => {
                        windows.track_c2b(&p);
                        subscriptions.track(&p);
                        if let Err(e) = b.send(p).await {
                            log::warn!("broker write failed ({e}), entering outage mode");
                            broker = None;
                            continue;
                        }
                        broker = Some(b);
                    }
                    Some(Err(e)) => return Err(invalid_data(e)),
                    None => return Ok(()),
                },
                next = b.next() => match next {
                    Some(Ok((p, _))) => {
                        // v5 administrative DISCONNECT (rolling update,
                        // scale-in): swallow it and treat as a transport
                        // loss — the client stays unaware.
                        if let MqttPacket::V5(v5::Packet::Disconnect(d)) = &p {
                            if is_retryable_disconnect(d.reason_code) {
                                log::info!(
                                    "intercepted broker DISCONNECT ({:?}), entering outage mode",
                                    d.reason_code
                                );
                                broker_leftover = b.into_parts().read_buf;
                                broker = None;
                                continue;
                            }
                            // Client-fault DISCONNECT: pass it through and
                            // end the session.
                            client.send(p).await.map_err(invalid_data)?;
                            return Ok(());
                        }
                        broker = Some(b);
                        // Broker acks for outage-buffered publishes belong
                        // to the proxy; the client was already acked locally.
                        match &p {
                            MqttPacket::V3(v3::Packet::PublishAck { packet_id })
                                if local_acks.qos1.remove(&packet_id.get()) => {}
                            MqttPacket::V3(v3::Packet::PublishReceived { packet_id })
                                if local_acks.qos2.contains(&packet_id.get()) =>
                            {
                                broker
                                    .as_mut()
                                    .unwrap()
                                    .send(MqttPacket::V3(v3::Packet::PublishRelease {
                                        packet_id: *packet_id,
                                    }))
                                    .await
                                    .map_err(invalid_data)?;
                            }
                            MqttPacket::V3(v3::Packet::PublishComplete { packet_id })
                                if local_acks.qos2.remove(&packet_id.get()) => {}
                            MqttPacket::V5(v5::Packet::PublishAck(ack))
                                if local_acks.qos1.remove(&ack.packet_id.get()) => {}
                            MqttPacket::V5(v5::Packet::PublishReceived(ack))
                                if local_acks.qos2.contains(&ack.packet_id.get()) =>
                            {
                                broker
                                    .as_mut()
                                    .unwrap()
                                    .send(MqttPacket::V5(v5::Packet::PublishRelease(
                                        v5::PublishAck2 {
                                            packet_id: ack.packet_id,
                                            reason_code: v5::PublishAck2Reason::Success,
                                            properties: Vec::new(),
                                            reason_string: None,
                                        },
                                    )))
                                    .await
                                    .map_err(invalid_data)?;
                            }
                            MqttPacket::V5(v5::Packet::PublishComplete(ack))
                                if local_acks.qos2.remove(&ack.packet_id.get()) => {}
                            _ => {
                                windows.track_b2c(&p);
                                client.send(p).await.map_err(invalid_data)?;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        log::warn!("broker read failed ({e}), entering outage mode");
                        broker_leftover = b.into_parts().read_buf;
                        broker = None;
                    }
                    None => {
                        log::warn!("broker closed the connection, entering outage mode");
                        broker_leftover = b.into_parts().read_buf;
                        broker = None;
                    }
                },
                req = control.recv() => match req {
                    Some(SessionControl::Freeze(req)) => {
                        match handle_freeze(
                            req, id, client, Some(b), &hs, &windows, &subscriptions, &buffer,
                        )
                        .await
                        {
                            FreezeOutcome::Resume(c, b) => {
                                client = c;
                                broker = b;
                            }
                            FreezeOutcome::Exit => return Ok(()),
                        }
                    }
                    None => return Ok(()),
                },
            }
        } else {
            // ---- outage phase: retry the broker while servicing the client ----
            let wait: std::pin::Pin<Box<dyn std::future::Future<Output = Option<TcpStream>> + Send>> =
                match step {
                    OutageStep::Connect => Box::pin(async move { TcpStream::connect(broker_addr).await.ok() }),
                    OutageStep::Backoff(d) => Box::pin(async move {
                        tokio::time::sleep(d).await;
                        None
                    }),
                };
            tokio::select! {
                next = client.next() => match next {
                    Some(Ok((p, _))) => {
                        service_outage_packet(&mut client, p, &mut buffer, &mut subscriptions).await?;
                    }
                    Some(Err(e)) => return Err(invalid_data(e)),
                    None => return Ok(()),
                },
                req = control.recv() => match req {
                    Some(SessionControl::Freeze(req)) => {
                        match handle_freeze(
                            req, id, client, None, &hs, &windows, &subscriptions, &buffer,
                        )
                        .await
                        {
                            FreezeOutcome::Resume(c, b) => {
                                client = c;
                                broker = b;
                            }
                            FreezeOutcome::Exit => return Ok(()),
                        }
                    }
                    None => return Ok(()),
                },
                result = wait => match result {
                    Some(stream) => {
                        log::info!("broker connection (re)established");
                        let mut b = Framed::new(stream, codec_for(hs.version));
                        establish_broker(
                            &mut b,
                            &hs,
                            &mut client,
                            &subscriptions,
                            &windows,
                            &mut local_acks,
                            &mut buffer,
                            &mut broker_leftover,
                        )
                        .await?;
                        broker = Some(b);
                        backoff = BROKER_RETRY_INITIAL;
                        step = OutageStep::Connect;
                    }
                    None => match step {
                        OutageStep::Connect => {
                            log::warn!("broker connect failed, retrying in {backoff:?}");
                            step = OutageStep::Backoff(backoff);
                            backoff = (backoff * 2).min(BROKER_RETRY_MAX);
                        }
                        OutageStep::Backoff(_) => {
                            step = OutageStep::Connect;
                        }
                    },
                },
            }
        }
    }
}

/// Adopt a migrated session (child side of a shed): take ownership of the
/// client socket FD and resume the session from the snapshot. The broker
/// side is established by the forward loop itself (with outage tolerance),
/// so a shed during a broker outage works too.
pub async fn adopt_session(
    client_fd: RawFd,
    snapshot: SessionSnapshot,
    broker_addr: SocketAddr,
    registry: Arc<SessionRegistry>,
) -> io::Result<()> {
    let hs = Handshake::from_snapshot_version(snapshot.version, snapshot.connect_raw.clone())?;

    // Adopt the client socket, seeding undelivered client bytes.
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(client_fd) };
    std_stream.set_nonblocking(true)?;
    let client_stream = TcpStream::from_std(std_stream)?;
    let mut parts = FramedParts::new(client_stream, codec_for(hs.version));
    parts.read_buf.extend_from_slice(&snapshot.client_buf);
    let client = Framed::from_parts(parts);

    let windows = InflightWindows::from_snapshot(snapshot.windows);
    let subscriptions = Subscriptions(snapshot.subscriptions);
    let bytes = snapshot.buffered.iter().map(|p| p.raw.len()).sum();
    let buffer = OutageBuffer {
        entries: snapshot.buffered,
        bytes,
    };
    let broker_leftover = BytesMut::from(&snapshot.broker_buf[..]);

    // Register so the adopted session is migratable on the next shed.
    let (id, control) = registry.register();
    let _registration = Registration { registry, id };

    forward_loop(
        id,
        client,
        None,
        broker_addr,
        hs,
        control,
        windows,
        subscriptions,
        LocalAcks::default(),
        buffer,
        broker_leftover,
    )
    .await
}
