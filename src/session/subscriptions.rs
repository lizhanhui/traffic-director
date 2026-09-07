//! The session's subscription table, tracked from the packet flow and
//! re-issued toward the broker on reconnect or thaw.

use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::{MqttPacket, v3, v5};

use crate::registry::SubscriptionEntry;

/// The session's active subscriptions, tracked from the packet flow and
/// re-issued toward the broker on thaw.
#[derive(Debug, Default)]
pub(super) struct Subscriptions(pub(super) Vec<SubscriptionEntry>);

impl Subscriptions {
    pub(super) fn upsert(&mut self, topic_filter: &str, options: u8) {
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

    pub(super) fn remove(&mut self, topic_filter: &str) {
        self.0.retain(|e| e.topic_filter != topic_filter);
    }

    /// Track a client→broker packet.
    pub(super) fn track(&mut self, packet: &MqttPacket) {
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
    pub(super) fn resubscribe_packet(&self, version: ProtocolVersion) -> Option<MqttPacket> {
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
