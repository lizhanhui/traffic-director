//! Broker-outage handling: while the broker is unreachable the client stays
//! serviced — local acks with bounded buffering, local keepalives and sub
//! management — until the reconnect replay flushes everything.

use std::io;

use futures::SinkExt;
use rmqtt_codec::{MqttPacket, v3, v5};

use crate::window::encode_raw;

use super::subscriptions::Subscriptions;
use super::{MqttFramed, invalid_data};

pub(super) const BROKER_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_millis(100);
pub(super) const BROKER_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(5);
const OUTAGE_BUFFER_MAX_BYTES: usize = 1024 * 1024;

/// Publishes received from the client while the broker was unreachable,
/// already acked locally and awaiting delivery after reconnect.
#[derive(Default)]
pub(super) struct OutageBuffer {
    pub(super) entries: Vec<crate::registry::BufferedPublish>,
    pub(super) bytes: usize,
}

impl OutageBuffer {
    pub(super) fn push(
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

// Buffered client packet kinds are self-describing via the MQTT packet
// type nibble in `raw[0]`.

/// Service one client packet during a broker outage. Withholding policy:
/// nothing that implies broker acceptance is answered locally — QoS
/// guarantees would break if the proxy acked on behalf of a broker that
/// never saw the message. Only keepalive (connection liveness, not
/// delivery) is answered locally.
pub(super) async fn service_outage_packet(
    client: &mut MqttFramed,
    packet: MqttPacket,
    buffer: &mut OutageBuffer,
    subscriptions: &mut Subscriptions,
    windows: &mut crate::window::InflightWindows,
) -> io::Result<()> {
    use rmqtt_codec::types::QoS;

    // Client acks of broker publishes keep the b2c window accurate; the
    // broker never sees them while down — session-resumption redelivery
    // heals the handshake after reconnect.
    match &packet {
        MqttPacket::V3(
            v3::Packet::PublishAck { .. }
            | v3::Packet::PublishReceived { .. }
            | v3::Packet::PublishComplete { .. },
        )
        | MqttPacket::V5(
            v5::Packet::PublishAck(_)
            | v5::Packet::PublishReceived(_)
            | v5::Packet::PublishComplete(_),
        ) => {
            windows.track_c2b(&packet);
            return Ok(());
        }
        _ => {}
    }

    match packet {
        // Publishes with a delivery guarantee: buffer WITHOUT acking. The
        // client's in-flight window provides the backpressure; on overflow
        // the session closes and the client's reconnect retransmits.
        MqttPacket::V3(v3::Packet::Publish(p)) => {
            let packet_id = p.packet_id;
            let qos = p.qos;
            let raw = encode_raw(&MqttPacket::V3(v3::Packet::Publish(p)));
            match qos {
                QoS::AtMostOnce => {} // at-most-once: may be dropped
                _ => {
                    let packet_id = packet_id.expect("QoS>0 PUBLISH has an id");
                    buffer.push(packet_id.get(), raw, qos)?;
                }
            }
        }
        MqttPacket::V3(v3::Packet::PublishRelease { packet_id }) => {
            // Part of a QoS2 handshake whose PUBREC the client already got
            // from the broker; buffered and forwarded after reconnect, in
            // order behind the window retransmits.
            let raw = encode_raw(&MqttPacket::V3(v3::Packet::PublishRelease { packet_id }));
            buffer.push(packet_id.get(), raw, QoS::ExactlyOnce)?;
        }
        MqttPacket::V3(v3::Packet::PingRequest) => {
            client
                .send(MqttPacket::V3(v3::Packet::PingResponse))
                .await
                .map_err(invalid_data)?;
        }
        p @ MqttPacket::V3(v3::Packet::Subscribe { .. }) => {
            subscriptions.track(&p);
            let MqttPacket::V3(v3::Packet::Subscribe { packet_id, .. }) = &p else {
                unreachable!()
            };
            let packet_id = *packet_id;
            // Withheld: the SUBACK must come from the broker after
            // reconnect, or the client would believe in a subscription the
            // broker never granted.
            buffer.push(packet_id.get(), encode_raw(&p), QoS::AtLeastOnce)?;
        }
        p @ MqttPacket::V3(v3::Packet::Unsubscribe { .. }) => {
            subscriptions.track(&p);
            let MqttPacket::V3(v3::Packet::Unsubscribe { packet_id, .. }) = &p else {
                unreachable!()
            };
            let packet_id = *packet_id;
            buffer.push(packet_id.get(), encode_raw(&p), QoS::AtLeastOnce)?;
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
                _ => {
                    let packet_id = packet_id.expect("QoS>0 PUBLISH has an id");
                    buffer.push(packet_id.get(), raw, qos)?;
                }
            }
        }
        MqttPacket::V5(v5::Packet::PublishRelease(ack)) => {
            let packet_id = ack.packet_id;
            let raw = encode_raw(&MqttPacket::V5(v5::Packet::PublishRelease(ack)));
            buffer.push(packet_id.get(), raw, QoS::ExactlyOnce)?;
        }
        MqttPacket::V5(v5::Packet::PingRequest) => {
            client
                .send(MqttPacket::V5(v5::Packet::PingResponse))
                .await
                .map_err(invalid_data)?;
        }
        p @ MqttPacket::V5(v5::Packet::Subscribe(_)) => {
            subscriptions.track(&p);
            let MqttPacket::V5(v5::Packet::Subscribe(subscribe)) = &p else {
                unreachable!()
            };
            buffer.push(subscribe.packet_id.get(), encode_raw(&p), QoS::AtLeastOnce)?;
        }
        p @ MqttPacket::V5(v5::Packet::Unsubscribe(_)) => {
            subscriptions.track(&p);
            let MqttPacket::V5(v5::Packet::Unsubscribe(unsubscribe)) = &p else {
                unreachable!()
            };
            buffer.push(unsubscribe.packet_id.get(), encode_raw(&p), QoS::AtLeastOnce)?;
        }
        MqttPacket::V5(v5::Packet::Disconnect(_)) => {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "client disconnected"));
        }
        _ => {}
    }
    Ok(())
}
