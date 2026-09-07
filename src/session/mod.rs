//! Per-connection MQTT session: terminates MQTT on the client side and
//! originates a corresponding session toward the backend broker.
//!
//! Packet identifiers pass through unchanged because each client session owns
//! exactly one broker-side connection, so the broker's QoS acknowledgements
//! chain end-to-end.
//!
//! Module map:
//! - [`handshake`] — client CONNECT handling and raw-byte capture.
//! - [`subscriptions`] — the subscription table tracked from the flow.
//! - [`outage`] — broker-down servicing: local acks, buffering, retry steps.
//! - [`keepalive`] — local keepalive termination and 1.5x enforcement.
//! - [`forward`] — the two-phase forward loop, freeze/thaw, entry points.

use std::io;
use std::sync::Arc;

use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::{MqttCodec, v3, v5};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::registry::SessionRegistry;

mod forward;
mod handshake;
mod keepalive;
mod outage;
mod subscriptions;

pub use forward::{adopt_session, run_session};

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
