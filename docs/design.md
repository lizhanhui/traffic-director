# Traffic Director — Design

## Problem statement

Backend MQTT clusters need maintenance: version upgrades, configuration
changes, scaling events, and recovery from node failures. Today any of these
events disconnects every client attached to the affected node. For IoT and
messaging workloads this is disproportionately expensive:

- Reconnect storms hit the cluster and the clients (battery, bandwidth).
- In-flight QoS1/2 messages can be lost or duplicated uncontrolled.
- Subscriptions must be re-established by every client.
- Offline messages queued at the broker are lost whenever the client's
  session does not survive (notably MQTT v5 `session_expiry_interval = 0`).

The same problem exists one layer up: the proxy that would hide broker
maintenance must itself be upgradeable without dropping connections.

## Rationale

A reverse proxy that terminates MQTT on both sides can absorb all of these
events, because it holds the client connection independently of the broker
connection. Terminating the protocol (rather than splicing raw TCP) is what
makes the hard guarantees possible:

- **QoS acknowledgements chain end-to-end** across the two connections, so
  delivery semantics are preserved while the proxy re-establishes either
  side.
- Per-connection state (subscriptions, QoS windows) is explicitly known to
  the proxy, so it can be serialized and transferred between process
  generations or replayed toward a new broker connection.
- Server-initiated administrative DISCONNECTs (v5 rolling updates) can be
  intercepted and answered by quiet reconnection instead of being passed to
  clients.

Process-level zero downtime is built on
[`ecdysis`](https://docs.rs/ecdysis) (tableflip-style): the child process
inherits the listen socket, so there is never a moment without an acceptor,
and no `SO_REUSEPORT` routing races.

## Design goals

1. Traffic Director intends to serve as reverse proxy for backend MQTT
   clusters, supporting MQTT v3.1.1 and v5.0;
2. Traffic Director intends to achieve zero-downtime its upgrades and
   backend MQTT broker restarts, without disrupting existing connections;
3. Traffic Director, after integrating Istio via xDS protocol, is to direct
   traffic among multiple MQTT clusters to survive disasters;

Goal 3 (xDS/Istio multi-cluster routing) is **not yet implemented**; the
codebase currently proves goals 1 and 2.

## Architecture

```text
client ──MQTT──▶ traffic-director ──MQTT──▶ broker
                 [one Session per client:
                  client-side termination,
                  broker-side termination,
                  subscriptions + QoS windows]
```

- **Session core** (`src/session.rs`): one actor per client connection.
  Terminates MQTT both sides; packet ids pass through 1:1 (each session owns
  exactly one broker connection), so broker acks chain to the client
  unchanged.
- **Two-phase state machine**: *connected* or *outage*. Broker loss
  (transport close, or an intercepted administrative v5 DISCONNECT) moves
  the session to outage: retry with exponential backoff while the client
  stays serviced. Reconnect runs a full replay: CONNECT with the clean bit
  cleared → re-SUBSCRIBE → leftover decode → QoS window retransmit →
  outage-buffer flush.
- **Shed lifecycle** (`src/server.rs`): on `SIGUSR2` the parent spawns the
  child via ecdysis (listener inherited). Two modes:
  - **drain** — parent stops accepting, keeps serving existing sessions
    until they end (or a timeout backstop), then exits.
  - **migrate** — parent freezes every session at a packet boundary, ships
    snapshots (CONNECT bytes, codec buffers, QoS windows, subscriptions,
    outage buffer) plus client socket FDs over an inherited Unix datagram
  pair (JSON + SCM_RIGHTS, acked per session), and exits immediately. The
  child adopts the sockets and resumes sessions; clients observe only a
  brief stall.

## Technical details

### How ecdysis achieves a zero-gap process upgrade

The mechanism (inherited from Cloudflare's `tableflip`, and described in
their [ecdysis blog
post](https://blog.cloudflare.com/ecdysis-rust-graceful-restarts/)) solves
the hardest part of self-restart: handing the listen socket to a new
process without ever refusing a connection. ecdysis was built around four
goals: old code shuts down completely after upgrade; the new process gets
a grace period for initialization; a child crashing during initialization
is acceptable and must not affect the service; and only one upgrade runs
at a time.

1. **Socket registration.** At boot, every listener is created through
   ecdysis and recorded in a registry (fd + address). On first boot the
   socket is bound fresh; on every later generation it is *inherited*
   instead.
2. **Fork, then exec.** On `SIGUSR2` the parent `fork()`s and the child
   immediately `execve()`s the new binary — a clean address space with no
   inherited memory; only explicitly passed file descriptors cross the
   boundary (everything else stays `CLOEXEC`). The serialized socket
   registry and the listen FDs travel over a pipe shared with the parent
   (`SCM_RIGHTS`).
3. **Ready handshake.** The child boots, detects the inherited
   environment, and reclaims the same underlying socket — because both
   processes temporarily reference one kernel data structure, the accept
   queue never went away. The parent keeps accepting while the child
   initializes (there is even an intentional brief window where *both*
   accept concurrently; connections the parent picks up in that window are
   simply drained like any other). Once the child signals readiness over
   the pipe, the parent closes its copy of the listen socket and continues
   with existing connections only. No SYN is ever refused.
4. **Failure safety.** If the child crashes before signalling ready, the
   parent's `upgrade()` simply returns an error: the parent never stopped
   accepting and continues as the sole generation, and the upgrade can be
   retried. A crashed upgrade attempt is therefore indistinguishable from
   no upgrade at all. (Note for sandboxes: this model requires `fork()`
   and `execve()` to be permitted, e.g. under seccomp.)

What ecdysis deliberately does *not* provide is state transfer for
established connections — that is application territory, and where the two
shed modes diverge:

- **Drain mode** needs nothing more: the parent keeps its existing sessions
  running in old code until they end, then exits. Simple and robust; the
  cost is that old code lingers until the last long-lived MQTT connection
  closes (bounded by a drain timeout).
- **Migrate mode** adds our own state channel on top: the parent freezes
  each session at a packet boundary and serializes a snapshot (re-encoded
  CONNECT, codec buffer leftovers, QoS in-flight windows, subscription
  table, outage buffer) as one JSON datagram per session over an
  ecdysis-inherited Unix datagram pair — with the client socket fd attached
  to each datagram via `SCM_RIGHTS`, which dups the fd into the child's
  descriptor table while referring to the *same kernel socket*. The client
  4-tuple is preserved, so the client's TCP stack sees nothing but a pause.
  Ordering is important: snapshots are written and fds are marked
  inheritable *before* the child is spawned, and the parent waits for the
  child's ready signal before pushing the datagrams — so a parent crash
  either happens before the spawn (nothing was shed) or after the child is
  fully equipped (the parent is redundant). Each datagram is acknowledged
  before the next is sent, so a partial handoff is detectable and degrades
  to draining the un-sent remainder instead of losing sessions.

### How retry-with-backoff hides broker maintenance

The client connection and the broker connection are independent; the
session's job is to keep the client side healthy while the broker side is
rebuilt. The forward loop is a two-phase state machine:

**Connected phase.** Packets flow both ways with per-direction QoS
bookkeeping. Three events move the session to the outage phase: broker
transport error/close (the only signal v3 brokers give), a broker write
failure, or an intercepted v5 administrative DISCONNECT (rolling update).
The old connection's undecoded bytes are captured as a "leftover" so
nothing already received is dropped.

**Outage phase.** The client must not notice anything, so the proxy
becomes a temporary stand-in for the broker:

- **Keepalives are terminated locally.** On every client PINGREQ the proxy
  answers PINGRESP immediately and *also* forwards the PINGREQ upstream so
  the broker-side keepalive holds; upstream PINGRESPs are swallowed since
  the client already has its answer. The client therefore always sees an
  instant keepalive response — even while the broker is unreachable or
  partitioned — without the broker connection timing out. Since the proxy
  terminates keepalive, it also takes over the broker's enforcement duty:
  a client that sends nothing for 1.5× its keepalive interval is
  disconnected (v5 clients receive DISCONNECT/KeepAliveTimeout first, per
  spec). The enforced interval honors the v5 Server Keep Alive assigned in
  CONNACK, including on reconnects, while the CONNACK itself is forwarded
  verbatim so the client adopts the same value.
- **QoS1/2 publishes, PUBREL, SUBSCRIBE, and UNSUBSCRIBE are buffered
  without acking.** The proxy never acks on behalf of a broker that hasn't
  seen the packet — a local ack would tell the client the message was
  accepted when it wasn't, and a proxy crash would then lose it silently.
  Instead, end-to-end ack chaining is preserved: the client's own
  in-flight window (v5 receive-maximum, or the client's send window in v3)
  throttles publishing naturally. The buffer is bounded (1 MiB); on
  overflow the session closes, and the client's reconnect retransmits the
  unacked packets with DUP — the protocol heals itself. QoS0 is dropped,
  which at-most-once semantics permit.
- Meanwhile the proxy retries the broker with exponential backoff:
  100 ms initial, doubling, capped at 5 s, indefinitely. The client socket
  is serviced throughout — including freeze requests, so a proxy upgrade
  can happen *during* a broker outage and carries the buffer along in the
  snapshot.

**Replay on reconnect** is what makes the recovery exact rather than
"close enough":

1. CONNECT is replayed with the clean bit **cleared** — the broker treats
   it as session resumption and redelivers whatever its session store
   still holds.
2. The recorded subscriptions are re-issued (idempotent), restoring
   routing even when the broker dropped everything.
3. The dead connection's leftover bytes are decoded and delivered
   downstream.
4. QoS in-flight windows are retransmitted: unacked client→broker
   publishes go out with DUP=1 (or PUBREL for mid-handshake QoS2);
   unacked broker→client publishes are replayed downstream the same way.
5. The outage buffer is flushed in arrival order; flushed publishes and
   PUBRELs enter the c2b window, so the broker's PUBACKs/PUBRECs/PUBCOMPs
   chain to the client exactly as if the outage never happened.

The client-visible result of a broker rolling update is a brief pause in
broker-originated traffic; no DISCONNECT, no reconnect, no subscription
loss, and no message loss beyond what QoS0 permits.

## Features (implemented and tested)

| Feature | Guarantee |
| --- | --- |
| MQTT v3.1.1 + v5 proxying | Full termination both sides; end-to-end ack chaining |
| Drain shed (PoC 1) | Upgrades never refuse new connections; existing clients never disconnected; parent exits after drain |
| Migrate shed (PoC 2) | Parent exits immediately; client TCP socket survives (same 4-tuple); no MQTT reconnect |
| QoS1/2 in-flight windows | Unacked publishes retransmitted DUP=1 on thaw; QoS2 PUBREC/PUBREL state survives at every handshake step, both directions |
| Subscription restore | Tracked SUBSCRIBE/UNSUBSCRIBE re-issued on reconnect; replayed CONNECT forced `clean_start/clean_session=false` |
| Broker outage mode | Client stays connected; local PUBACK/PUBREC with bounded buffering (1 MiB); local keepalives and sub management; flush on recovery |
| Server DISCONNECT interception (v5) | Administrative reasons swallowed → quiet reconnect; client-fault reasons forwarded |
| Upgrade failure rollback | Failed child boot → sessions resume in place; parent keeps serving |

Test suite: 31 tests across unit, in-process thaw (deterministic fake
brokers), and real-binary/real-mosquitto e2e, all green with repeated runs.

## Pros and cons

### Pros

- **True zero-downtime on both hops**: client connections survive proxy
  upgrades (migrate) and broker restarts/rolling updates (outage mode).
- **Protocol-correct**: end-to-end ack chaining preserves at-least-once /
  exactly-once semantics; retransmissions follow the MQTT spec (DUP flag).
- **No old code after upgrade** (migrate mode): parent exits as soon as the
  handoff completes, matching ecdysis's design goal.
- **Failure-aware**: upgrade failure rolls back seamlessly; partial handoff
  degrades to draining; child boot failure never affects the listener.
- **Version-agnostic core**: one code path for v3.1.1 and v5 (unified
  codec), including mixed-version client populations.

### Cons

- **Full termination costs CPU**: decode/re-encode per packet (no
  zero-copy splicing). Acceptable for a PoC; a production deployment needs
  benchmarking before committing to it at scale.
- **One broker connection per client**: no connection pooling; broker sees
  the same connection count as clients. Large fleets need the broker tuned
  accordingly.
- **Outages stall publishers instead of losing messages**: while the broker
  is down, QoS1/2 publishes are buffered unacked, so clients stop sending
  once their in-flight window fills. Correct per MQTT, but a publisher
  expecting progress sees the outage as backpressure (which is honest, but
  changes observable behavior versus a direct connection that would break).
- **QoS1 duplicates possible at migration boundaries**: window replay plus
  broker redelivery can deliver a message twice. Protocol-normal for QoS1,
  but exactly-once *application* semantics still require idempotent
  consumers.
- **Operational complexity**: two shed modes, a state channel, and FD
  passing are more moving parts than "just restart the proxy".

## Risk analysis

| Risk | Impact | Mitigation in place | Residual |
| --- | --- | --- | --- |
| Child crashes after adopting FDs | Adopted client sockets die | Child boot path is minimal and fully test-covered; handoff is acked per session | Supervisor-level restart (systemd/k8s) recommended |
| Parent crashes mid-migration | Depends on phase | Snapshots and FD-clearing happen *before* spawn; pre-spawn crash = no shed at all | Crash between spawn and send: child has everything; parent dead is harmless |
| v5 `session_expiry = 0` | Broker-queued offline messages lost on broker restart | **Explicit non-goal** (proxy does not override client session semantics); re-SUBSCRIBE restores routing | Broker/cluster persistence + client config (see deployment advice) |
| Outage buffer exhaustion (1 MiB) | Session closed to protect the process | Bounded buffer, fail-closed | Tune per workload |
| Migration channel limits | Large snapshots as Unix datagrams (≤1 MiB each, one per session, acked) | Flow-controlled per-message acks; I/O timeouts | Very large in-flight state could need chunking |
| Freeze timeout (5 s) | Unfreezable session is skipped and dropped at shed | Logged loudly | Rare; only pathological sessions |
| Single static backend | No failover across broker nodes | — | Goal 3 (xDS) addresses this |
| Broker redelivery + window replay overlap | Duplicate delivery (QoS1) | — | Protocol-normal; idempotent consumers |
| Keepalive during long freezes | Client keepalive timeouts if freeze >> keepalive | Freezes are milliseconds; migration stall is sub-second | Very small keepalives (<5 s) could notice stalls |
| `server_reference` in DISCONNECT | Ignored (no redirection) | — | Relevant only with multi-backend (goal 3) |

## Deployment advice

**Choosing a shed mode.** Prefer **migrate** when old code must not linger
(security patches, memory-shape changes) and when connection survival
matters more than operational simplicity. Prefer **drain** when sessions are
short-lived anyway, or as a fallback if migrate misbehaves in your
environment — it is the simpler, more conservative mechanism. Both share
the same code path and signal handling.

**Signals and lifecycle.**

- `SIGUSR2` — zero-downtime upgrade (shed).
- `SIGTERM` / `SIGINT` — graceful stop (drains, then exits).
- Always replace the binary *before* signalling; the child re-executes
  `argv[0]` with identical arguments, so `--listen`, `--broker`, `--mode`
  carry over automatically.

**Broker configuration.**

- For offline-message retention across broker restarts, enable persistence
  (mosquitto: `persistence true`) or use a cluster with shared session
  state — and have clients connect with non-zero v5 `session_expiry_interval`.
  With `expiry = 0`, queued offline messages are lost on broker restart by
  protocol design; the proxy will still restore connections and
  subscriptions.
- Size the broker for one upstream connection per downstream client.
- Rolling updates: if the broker emits v5 DISCONNECT with an administrative
  reason, the proxy absorbs it; v3 clusters only need to close the
  connection.

**Client configuration.**

- Keepalives below ~5 s leave little slack for migration stalls; ≥15 s is
  comfortable.
- Consumers of QoS1 traffic should be idempotent, as anywhere in MQTT.

**Observability.** The proxy logs shed events (`froze N session(s)`,
`handed over N session(s)`), outage transitions (`entering outage mode`,
`broker connection (re)established`), intercepted DISCONNECTs, and buffer
flushes at `info` level (`RUST_LOG` overrides). Watch for repeated
`upgrade failed, rolling back freeze` and `outage buffer full` — both
indicate environment problems, not proxy bugs.

**Known limitations to plan around.**

- Static single backend; no TLS termination yet; no `foundations`
  integration yet.
- v5 server-reference redirection is ignored (single-backend scope).
- Migration currently moves established sessions only; a client mid-CONNECT
  during a shed retries as with any reconnect.
