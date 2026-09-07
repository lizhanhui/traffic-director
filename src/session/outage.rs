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

/// Broker ack packet ids that must NOT be forwarded to the client: the
/// client was already acked locally during the outage.
#[derive(Default)]
pub(super) struct LocalAcks {
    pub(super) qos1: std::collections::HashSet<u16>,
    pub(super) qos2: std::collections::HashSet<u16>,
}

/// Answer and/or buffer one client packet received during a broker outage.
pub(super) async fn service_outage_packet(
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
