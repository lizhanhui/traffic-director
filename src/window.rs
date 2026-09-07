//! QoS in-flight windows: the per-session bookkeeping of QoS1/2 packets that
//! have been forwarded but not yet acknowledged end-to-end.
//!
//! Packet identifiers pass through 1:1 (each session owns its broker
//! connection), so entries are keyed by packet id per direction. Entries hold
//! the raw encoded packet: retransmitting with DUP set is a single bit flip,
//! and snapshots need no codec work.


use bytes::BytesMut;
use rmqtt_codec::types::QoS;
use rmqtt_codec::{MqttPacket, v3, v5};
use serde::{Deserialize, Serialize};
use tokio_util::codec::Encoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowState {
    /// We sent the client's PUBLISH to the broker; awaiting PUBACK/PUBREC.
    /// On thaw: retransmit PUBLISH with DUP=1 toward the broker.
    C2BPublishSent,
    /// We sent the client's PUBREL to the broker; awaiting PUBCOMP.
    /// On thaw: retransmit PUBREL toward the broker.
    C2BPubrelSent,
    /// We sent the broker's PUBLISH to the client; awaiting PUBACK/PUBREC.
    /// On thaw: retransmit PUBLISH with DUP=1 toward the client.
    B2CPublishSent,
    /// We forwarded the client's PUBREC to the broker; awaiting PUBREL.
    /// On thaw: nothing to retransmit (the broker re-sends PUBREL once
    /// session resumption lands; until then the entry just tracks state).
    B2CPubrecForwarded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowEntry {
    pub packet_id: u16,
    pub state: WindowState,
    pub raw: Vec<u8>,
}

const DUP_FLAG: u8 = 0x08;

#[derive(Debug, Default)]
pub struct InflightWindows {
    // Insertion-ordered: replay must preserve the original delivery order
    // (QoS ordering guarantee), so these are Vecs, not maps. Windows are
    // small (broker in-flight caps), linear scans are fine.
    c2b: Vec<(u16, (WindowState, Vec<u8>))>,
    b2c: Vec<(u16, (WindowState, Vec<u8>))>,
}

fn vec_insert(v: &mut Vec<(u16, (WindowState, Vec<u8>))>, id: u16, entry: (WindowState, Vec<u8>)) {
    if let Some(slot) = v.iter_mut().find(|(k, _)| *k == id) {
        slot.1 = entry;
    } else {
        v.push((id, entry));
    }
}

fn vec_remove(v: &mut Vec<(u16, (WindowState, Vec<u8>))>, id: u16) {
    v.retain(|(k, _)| *k != id);
}

fn vec_get_mut(
    v: &mut [(u16, (WindowState, Vec<u8>))],
    id: u16,
) -> Option<&mut (WindowState, Vec<u8>)> {
    v.iter_mut().find(|(k, _)| *k == id).map(|(_, e)| e)
}

fn vec_contains(v: &[(u16, (WindowState, Vec<u8>))], id: u16) -> bool {
    v.iter().any(|(k, _)| *k == id)
}

pub(crate) fn encode_raw(packet: &MqttPacket) -> Vec<u8> {
    let mut buf = BytesMut::new();
    match packet {
        MqttPacket::V3(p) => {
                v3::Codec::new(1024 * 1024).encode(p.clone(), &mut buf)
        }
        MqttPacket::V5(p) => {
                v5::Codec::new(1024 * 1024, 1024 * 1024).encode(p.clone(), &mut buf)
        }
        MqttPacket::Version(_) => unreachable!("version packets are never tracked"),
    }
    .expect("window packets must be encodable");
    buf.to_vec()
}

fn with_dup(mut raw: Vec<u8>) -> Vec<u8> {
    debug_assert!(!raw.is_empty());
    raw[0] |= DUP_FLAG;
    raw
}

impl InflightWindows {
    pub fn new() -> Self {
        Self::default()
    }

    /// Account for a packet about to be forwarded client→broker.
    pub fn track_c2b(&mut self, packet: &MqttPacket) {
        match packet {
            MqttPacket::V3(v3::Packet::Publish(p)) => self.c2b_insert(p.qos, p.packet_id, packet),
            MqttPacket::V3(v3::Packet::PublishRelease { packet_id }) => {
                self.c2b_transition(*packet_id, WindowState::C2BPubrelSent, packet);
            }
            MqttPacket::V3(v3::Packet::PublishAck { packet_id }) => {
                vec_remove(&mut self.b2c, packet_id.get());
            }
            MqttPacket::V3(v3::Packet::PublishReceived { packet_id }) => {
                self.b2c_transition(*packet_id, WindowState::B2CPubrecForwarded);
            }
            MqttPacket::V3(v3::Packet::PublishComplete { packet_id }) => {
                vec_remove(&mut self.b2c, packet_id.get());
            }
            MqttPacket::V5(v5::Packet::Publish(p)) => self.c2b_insert(p.qos, p.packet_id, packet),
            MqttPacket::V5(v5::Packet::PublishRelease(ack)) => {
                self.c2b_transition(ack.packet_id, WindowState::C2BPubrelSent, packet);
            }
            MqttPacket::V5(v5::Packet::PublishAck(ack)) => {
                vec_remove(&mut self.b2c, ack.packet_id.get());
            }
            MqttPacket::V5(v5::Packet::PublishReceived(ack)) => {
                self.b2c_transition(ack.packet_id, WindowState::B2CPubrecForwarded);
            }
            MqttPacket::V5(v5::Packet::PublishComplete(ack)) => {
                vec_remove(&mut self.b2c, ack.packet_id.get());
            }
            _ => {}
        }
    }

    /// Account for a packet about to be forwarded broker→client.
    pub fn track_b2c(&mut self, packet: &MqttPacket) {
        match packet {
            MqttPacket::V3(v3::Packet::Publish(p)) => self.b2c_insert(p.qos, p.packet_id, packet),
            MqttPacket::V3(v3::Packet::PublishAck { packet_id }) => {
                vec_remove(&mut self.c2b, packet_id.get());
            }
            MqttPacket::V3(v3::Packet::PublishComplete { packet_id }) => {
                vec_remove(&mut self.c2b, packet_id.get());
            }
            MqttPacket::V5(v5::Packet::Publish(p)) => self.b2c_insert(p.qos, p.packet_id, packet),
            MqttPacket::V5(v5::Packet::PublishAck(ack)) => {
                vec_remove(&mut self.c2b, ack.packet_id.get());
            }
            MqttPacket::V5(v5::Packet::PublishComplete(ack)) => {
                vec_remove(&mut self.c2b, ack.packet_id.get());
            }
            _ => {}
        }
    }

    fn c2b_insert(
        &mut self,
        qos: QoS,
        packet_id: Option<std::num::NonZeroU16>,
        packet: &MqttPacket,
    ) {
        if qos == QoS::AtMostOnce {
            return;
        }
        let id = packet_id.expect("QoS>0 PUBLISH must have a packet id").get();
        vec_insert(
            &mut self.c2b,
            id,
            (WindowState::C2BPublishSent, encode_raw(packet)),
        );
    }

    fn b2c_insert(
        &mut self,
        qos: QoS,
        packet_id: Option<std::num::NonZeroU16>,
        packet: &MqttPacket,
    ) {
        if qos == QoS::AtMostOnce {
            return;
        }
        let id = packet_id.expect("QoS>0 PUBLISH must have a packet id").get();
        vec_insert(
            &mut self.b2c,
            id,
            (WindowState::B2CPublishSent, encode_raw(packet)),
        );
    }

    fn c2b_transition(
        &mut self,
        packet_id: std::num::NonZeroU16,
        state: WindowState,
        packet: &MqttPacket,
    ) {
        if vec_contains(&self.c2b, packet_id.get()) {
            vec_insert(&mut self.c2b, packet_id.get(), (state, encode_raw(packet)));
        }
    }

    fn b2c_transition(&mut self, packet_id: std::num::NonZeroU16, state: WindowState) {
        if let Some(entry) = vec_get_mut(&mut self.b2c, packet_id.get()) {
            entry.0 = state;
        }
    }

    /// Packets to re-send toward the broker on thaw, in no particular order.
    pub fn broker_retransmits(&self) -> Vec<Vec<u8>> {
        self.c2b
            .iter()
            .map(|(_, entry)| entry)
            .map(|(state, raw)| match state {
                WindowState::C2BPublishSent => with_dup(raw.clone()),
                WindowState::C2BPubrelSent => raw.clone(),
                _ => unreachable!("c2b window holds only c2b states"),
            })
            .collect()
    }

    /// Packets to re-send toward the client on thaw, in no particular order.
    pub fn client_retransmits(&self) -> Vec<Vec<u8>> {
        self.b2c
            .iter()
            .map(|(_, entry)| entry)
            .filter_map(|(state, raw)| match state {
                WindowState::B2CPublishSent => Some(with_dup(raw.clone())),
                WindowState::B2CPubrecForwarded => None,
                _ => unreachable!("b2c window holds only b2c states"),
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.c2b.is_empty() && self.b2c.is_empty()
    }

    /// Serialize into the session snapshot.
    pub fn snapshot(&self) -> Vec<WindowEntry> {
        let mut entries = Vec::with_capacity(self.c2b.len() + self.b2c.len());
        for (id, (state, raw)) in &self.c2b {
            entries.push(WindowEntry {
                packet_id: *id,
                state: *state,
                raw: raw.clone(),
            });
        }
        for (id, (state, raw)) in &self.b2c {
            entries.push(WindowEntry {
                packet_id: *id,
                state: *state,
                raw: raw.clone(),
            });
        }
        entries
    }

    /// Restore from a session snapshot.
    pub fn from_snapshot(entries: Vec<WindowEntry>) -> Self {
        let mut windows = Self::default();
        for entry in entries {
            let target = match entry.state {
                WindowState::C2BPublishSent | WindowState::C2BPubrelSent => &mut windows.c2b,
                WindowState::B2CPublishSent | WindowState::B2CPubrecForwarded => {
                    &mut windows.b2c
                }
            };
            vec_insert(target, entry.packet_id, (entry.state, entry.raw));
        }
        windows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU16;

    fn v3_publish(id: u16, qos: QoS) -> MqttPacket {
        MqttPacket::V3(v3::Packet::Publish(Box::new(rmqtt_codec::types::Publish {
            dup: false,
            retain: false,
            qos,
            topic: "t/x".into(),
            packet_id: NonZeroU16::new(id),
            payload: bytes::Bytes::from_static(b"payload"),
            properties: None,
        })))
    }

    fn id(n: u16) -> NonZeroU16 {
        NonZeroU16::new(n).unwrap()
    }

    #[test]
    fn qos1_c2b_insert_then_remove_on_puback() {
        let mut w = InflightWindows::new();
        w.track_c2b(&v3_publish(1, QoS::AtLeastOnce));
        assert_eq!(w.c2b.len(), 1);

        // QoS0 is never tracked.
        w.track_c2b(&v3_publish(2, QoS::AtMostOnce));
        assert_eq!(w.c2b.len(), 1);

        // Retransmit before ack: PUBLISH with DUP set.
        let re = w.broker_retransmits();
        assert_eq!(re.len(), 1);
        assert_eq!(re[0][0] & DUP_FLAG, DUP_FLAG);
        assert_eq!(re[0][0] >> 4, 3, "expected a PUBLISH packet");

        w.track_b2c(&MqttPacket::V3(v3::Packet::PublishAck { packet_id: id(1) }));
        assert!(w.c2b.is_empty());
        assert!(w.broker_retransmits().is_empty());
    }

    #[test]
    fn qos2_c2b_full_handshake() {
        let mut w = InflightWindows::new();
        w.track_c2b(&v3_publish(7, QoS::ExactlyOnce));
        w.track_b2c(&MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(7) }));
        // Still PublishSent after PUBREC: thaw retransmits the PUBLISH.
        assert_eq!(w.broker_retransmits()[0][0] >> 4, 3);

        w.track_c2b(&MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(7) }));
        // After PUBREL: thaw retransmits the PUBREL (type 6, no DUP bit).
        let re = w.broker_retransmits();
        assert_eq!(re.len(), 1);
        assert_eq!(re[0][0], 0x62);

        w.track_b2c(&MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(7) }));
        assert!(w.c2b.is_empty());
    }

    #[test]
    fn qos1_b2c_insert_then_remove_on_client_puback() {
        let mut w = InflightWindows::new();
        w.track_b2c(&v3_publish(9, QoS::AtLeastOnce));
        assert_eq!(w.b2c.len(), 1);

        let re = w.client_retransmits();
        assert_eq!(re.len(), 1);
        assert_eq!(re[0][0] & DUP_FLAG, DUP_FLAG);

        w.track_c2b(&MqttPacket::V3(v3::Packet::PublishAck { packet_id: id(9) }));
        assert!(w.b2c.is_empty());
    }

    #[test]
    fn qos2_b2c_stops_retransmitting_after_pubrec() {
        let mut w = InflightWindows::new();
        w.track_b2c(&v3_publish(11, QoS::ExactlyOnce));
        assert_eq!(w.client_retransmits().len(), 1);

        w.track_c2b(&MqttPacket::V3(v3::Packet::PublishReceived { packet_id: id(11) }));
        assert!(w.client_retransmits().is_empty(), "nothing to replay once PUBREC forwarded");
        assert!(!w.b2c.is_empty(), "entry kept to track the handshake");

        w.track_c2b(&MqttPacket::V3(v3::Packet::PublishComplete { packet_id: id(11) }));
        assert!(w.b2c.is_empty());
    }

    #[test]
    fn snapshot_roundtrip_preserves_state() {
        let mut w = InflightWindows::new();
        w.track_c2b(&v3_publish(1, QoS::AtLeastOnce));
        w.track_c2b(&v3_publish(2, QoS::ExactlyOnce));
        w.track_c2b(&MqttPacket::V3(v3::Packet::PublishRelease { packet_id: id(2) }));
        w.track_b2c(&v3_publish(3, QoS::AtLeastOnce));

        let restored = InflightWindows::from_snapshot(w.snapshot());
        assert_eq!(restored.c2b.len(), 2);
        assert_eq!(restored.b2c.len(), 1);
        assert_eq!(
            restored
                .c2b
                .iter()
                .find(|(id, _)| *id == 2)
                .map(|(_, e)| e.0),
            Some(WindowState::C2BPubrelSent),
            "QoS2 mid-handshake state must survive the roundtrip"
        );
        assert_eq!(restored.broker_retransmits().len(), 2);
        assert_eq!(restored.client_retransmits().len(), 1);
    }

    #[test]
    fn v5_qos1_both_directions() {
        let mut w = InflightWindows::new();
        let publish = MqttPacket::V5(v5::Packet::Publish(Box::new(rmqtt_codec::types::Publish {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            topic: "t/v5".into(),
            packet_id: id(5).into(),
            payload: bytes::Bytes::from_static(b"v5"),
            properties: None,
        })));
        w.track_c2b(&publish);
        w.track_b2c(&publish);
        assert_eq!(w.c2b.len(), 1);
        assert_eq!(w.b2c.len(), 1);

        let ack = MqttPacket::V5(v5::Packet::PublishAck(v5::PublishAck {
            packet_id: id(5),
            reason_code: v5::PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        }));
        w.track_b2c(&ack);
        assert!(w.c2b.is_empty());
        w.track_c2b(&ack);
        assert!(w.b2c.is_empty());
    }
}
