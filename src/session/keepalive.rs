//! Local keepalive termination: instant PINGRESP for clients, and the
//! 1.5x silence enforcement the broker can no longer do end-to-end.

use futures::SinkExt;
use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::{MqttPacket, v5};

use super::MqttFramed;

/// The instant at which a silent client must be dropped: 1.5× its
/// keepalive after the last received packet (MQTT spec). keepalive=0
/// disables the timeout per spec — use a far-future deadline.
pub(super) fn client_deadline(keep_alive: u16, last_activity: std::time::Instant) -> std::time::Instant {
    if keep_alive == 0 {
        return last_activity + std::time::Duration::from_secs(3600 * 24 * 365);
    }
    last_activity + std::time::Duration::from_secs(u64::from(keep_alive)) * 3 / 2
}

/// End a session for keepalive timeout. v5 clients get a DISCONNECT with
/// reason KeepAliveTimeout (0x8D) first, per spec; v3 just closes.
pub(super) async fn close_for_keepalive(client: &mut MqttFramed, version: ProtocolVersion) {
    if version == ProtocolVersion::MQTT5 {
        let _ = client
            .send(MqttPacket::V5(v5::Packet::Disconnect(v5::Disconnect {
                reason_code: v5::DisconnectReasonCode::KeepAliveTimeout,
                session_expiry_interval_secs: None,
                server_reference: None,
                reason_string: None,
                user_properties: Vec::new(),
            })))
            .await;
    }
}
