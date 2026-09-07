//! Migration channel: carries session snapshots (JSON) and client socket FDs
//! (SCM_RIGHTS) from parent to child over the ecdysis-inherited Unix datagram
//! pair. One datagram per session, acknowledged one at a time, then an END
//! marker.

use std::io::{self, IoSlice, IoSliceMut};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::UnixDatagram;
use std::time::Duration;

use nix::cmsg_space;
use nix::sys::socket::{
    ControlMessage, ControlMessageOwned, MsgFlags, SockaddrStorage, recvmsg, sendmsg, setsockopt,
    sockopt,
};
use nix::sys::time::TimeVal;

use crate::registry::{FrozenSession, SessionSnapshot};

const END_MARKER: &[u8] = b"TD-END";
const ACK: &[u8] = b"\x06";
const IO_TIMEOUT: Duration = Duration::from_secs(10);

fn set_timeouts(sock: &UnixDatagram) -> io::Result<()> {
    let tv = TimeVal::new(IO_TIMEOUT.as_secs() as i64, 0);
    setsockopt(sock.as_raw_fd(), sockopt::ReceiveTimeout, &tv)?;
    setsockopt(sock.as_raw_fd(), sockopt::SendTimeout, &tv)?;
    Ok(())
}

/// Prepare the parent side of the channel (blocking I/O in spawn_blocking).
pub fn parent_channel(sock: &UnixDatagram) -> io::Result<()> {
    set_timeouts(sock)
}

/// Prepare the child side of the channel.
pub fn child_channel(sock: &UnixDatagram) -> io::Result<()> {
    set_timeouts(sock)
}

fn send_with_fd(sock: &UnixDatagram, payload: &[u8], fd: RawFd) -> io::Result<()> {
    let iov = [IoSlice::new(payload)];
    let fds = [fd];
    sendmsg(
        sock.as_raw_fd(),
        &iov,
        &[ControlMessage::ScmRights(&fds)],
        MsgFlags::empty(),
        None::<&SockaddrStorage>,
    )?;
    Ok(())
}

fn recv_with_fd(sock: &UnixDatagram) -> io::Result<(Vec<u8>, Option<RawFd>)> {
    let mut buf = vec![0u8; 1024 * 1024];
    let mut cmsgspace = cmsg_space!([RawFd; 1]);
    let (n, fd) = {
        let mut iov = [IoSliceMut::new(&mut buf)];
        let msg = recvmsg::<()>(
            sock.as_raw_fd(),
            &mut iov,
            Some(&mut cmsgspace),
            MsgFlags::empty(),
        )?;
        let mut fd = None;
        for cmsg in msg.cmsgs() {
            if let ControlMessageOwned::ScmRights(fds) = cmsg {
                fd = fds.first().copied();
            }
        }
        (msg.bytes, fd)
    };
    buf.truncate(n);
    Ok((buf, fd))
}

/// Parent: send every frozen session, waiting for an ack after each so the
/// child's receive buffer can't overflow. On error, `sent` reports how many
/// sessions were successfully handed over.
pub fn send_sessions(
    sock: &UnixDatagram,
    frozen: &[FrozenSession],
    sent: &mut usize,
) -> io::Result<()> {
    for session in frozen {
        let payload = serde_json::to_vec(&session.snapshot)?;
        send_with_fd(sock, &payload, session.client_fd)?;

        let (ack, _) = recv_with_fd(sock)?;
        if ack != ACK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad handoff ack",
            ));
        }
        *sent += 1;
    }
    sock.send(END_MARKER)?;
    Ok(())
}

/// Child: receive sessions until the END marker. Each message is one
/// snapshot plus one client socket FD.
pub fn recv_sessions(sock: &UnixDatagram) -> io::Result<Vec<(SessionSnapshot, RawFd)>> {
    let mut sessions = Vec::new();
    loop {
        let (payload, fd) = recv_with_fd(sock)?;
        if payload == END_MARKER {
            break;
        }
        let snapshot: SessionSnapshot = serde_json::from_slice(&payload)?;
        let fd = fd.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "snapshot without client fd")
        })?;
        sessions.push((snapshot, fd));
        sock.send(ACK)?;
    }
    Ok(sessions)
}
