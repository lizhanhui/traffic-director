//! The session's forward loop: a two-phase state machine (connected /
//! outage), the freeze/thaw machinery, and the public entry points
//! [`run_session`] and [`adopt_session`].

use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::sync::Arc;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::{MqttPacket, v3, v5};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{Decoder, Framed, FramedParts};

use crate::registry::{
    FrozenSession, ResumeAction, SessionControl, SessionRegistry, SessionSnapshot,
};
use crate::window::InflightWindows;

use super::handshake::{Handshake, force_session_resumption, handshake};
use super::keepalive::{client_deadline, close_for_keepalive};
use super::outage::{
    BROKER_RETRY_INITIAL, BROKER_RETRY_MAX, LocalAcks, OutageBuffer, service_outage_packet,
};
use super::subscriptions::Subscriptions;
use super::{MqttFramed, Registration, codec_for, invalid_data};

/// Replay the session onto a fresh broker connection: CONNECT with the clean
/// bit cleared, re-SUBSCRIBE, deliver leftover broker bytes to the client,
/// retransmit QoS windows, and flush the outage buffer (marking the flushed
/// packet ids as proxy-local acks to eat). Shared by mid-session reconnect
/// and thaw.
#[allow(clippy::too_many_arguments)]
async fn establish_broker(
    broker: &mut MqttFramed,
    version: ProtocolVersion,
    connect_raw: &[u8],
    client: &mut MqttFramed,
    subscriptions: &Subscriptions,
    windows: &InflightWindows,
    local_acks: &mut LocalAcks,
    buffer: &mut OutageBuffer,
    broker_leftover: &mut BytesMut,
    keep_alive: &mut u16,
) -> io::Result<()> {
    // CONNECT replay with forced session resumption; CONNACK stays
    // proxy-local since the client never disconnected. Note: the clean bit
    // is the only thing we rewrite — the proxy deliberately does NOT patch
    // v5 session_expiry (see design doc, "v5 session expiry: explicit
    // non-goal"), so a client asking for expiry=0 forfeits broker-queued
    // offline messages on broker restart; routing is restored via
    // re-SUBSCRIBE regardless.
    let mut connect_raw = connect_raw.to_vec();
    force_session_resumption(&mut connect_raw)?;
    {
        use tokio::io::AsyncWriteExt;
        broker.get_mut().write_all(&connect_raw).await?;
        broker.get_mut().flush().await?;
    }
    match broker.next().await {
        Some(Ok((MqttPacket::V3(v3::Packet::ConnectAck(_)), _))) => {}
        Some(Ok((MqttPacket::V5(v5::Packet::ConnectAck(ack)), _))) => {
            // A Server Keep Alive assigned on a *reconnect* CONNACK also
            // updates our enforcement timer.
            if let Some(secs) = ack.server_keepalive_sec {
                *keep_alive = secs;
            }
        }
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
    if let Some(packet) = subscriptions.resubscribe_packet(version) {
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
        let mut codec = codec_for(version);
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

/// Run one proxied session: handshake, broker-side session establishment,
/// then bidirectional forwarding until close or a terminating freeze.
pub async fn run_session(
    client: TcpStream,
    broker_addr: SocketAddr,
    registry: Arc<SessionRegistry>,
) -> io::Result<()> {
    let (mut client, mut hs) = match handshake(client).await {
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
    // v5: if the broker assigns a Server Keep Alive in CONNACK, that value
    // replaces the client's requested keepalive for our enforcement timer
    // (the CONNACK is forwarded verbatim, so the client adopts it too).
    if let MqttPacket::V5(v5::Packet::ConnectAck(ack)) = &ack
        && let Some(secs) = ack.server_keepalive_sec
    {
        hs.keep_alive = secs;
    }
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
    mut hs: Handshake,
    mut control: mpsc::Receiver<SessionControl>,
    mut windows: InflightWindows,
    mut subscriptions: Subscriptions,
    mut local_acks: LocalAcks,
    mut buffer: OutageBuffer,
    mut broker_leftover: BytesMut,
) -> io::Result<()> {
    let mut backoff = BROKER_RETRY_INITIAL;
    let mut step = OutageStep::Connect;
    let mut last_client_activity = std::time::Instant::now();
    loop {
        if broker.is_some() {
            // ---- connected phase ----
            let deadline = client_deadline(hs.keep_alive, last_client_activity);
            let mut b = broker.take().expect("checked above");
            tokio::select! {
                next = client.next() => match next {
                    Some(Ok((p, _))) => {
                        last_client_activity = std::time::Instant::now();
                        // Keepalive is terminated locally: the client gets an
                        // instant PINGRESP, and the PINGREQ is still forwarded
                        // so the broker-side keepalive holds.
                        match &p {
                            MqttPacket::V3(v3::Packet::PingRequest) => {
                                client
                                    .send(MqttPacket::V3(v3::Packet::PingResponse))
                                    .await
                                    .map_err(invalid_data)?;
                            }
                            MqttPacket::V5(v5::Packet::PingRequest) => {
                                client
                                    .send(MqttPacket::V5(v5::Packet::PingResponse))
                                    .await
                                    .map_err(invalid_data)?;
                            }
                            _ => {}
                        }
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
                            // Upstream PINGRESP: the client was already
                            // answered locally — swallow it.
                            MqttPacket::V3(v3::Packet::PingResponse)
                            | MqttPacket::V5(v5::Packet::PingResponse) => {}
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
                _ = tokio::time::sleep_until(deadline.into()) => {
                    log::info!("session {id}: client keepalive timeout, disconnecting");
                    close_for_keepalive(&mut client, hs.version).await;
                    return Ok(());
                }
            }
        } else {
            // ---- outage phase: retry the broker while servicing the client ----
            let deadline = client_deadline(hs.keep_alive, last_client_activity);
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
                        last_client_activity = std::time::Instant::now();
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
                            hs.version,
                            &hs.connect_raw,
                            &mut client,
                            &subscriptions,
                            &windows,
                            &mut local_acks,
                            &mut buffer,
                            &mut broker_leftover,
                            &mut hs.keep_alive,
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
                _ = tokio::time::sleep_until(deadline.into()) => {
                    log::info!("session {id}: client keepalive timeout during outage, disconnecting");
                    close_for_keepalive(&mut client, hs.version).await;
                    return Ok(());
                }
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
