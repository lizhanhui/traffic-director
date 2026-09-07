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
- **Local acking during outages changes semantics**: while the broker is
  down the proxy acks publishes itself (bounded). A client that would
  otherwise notice an outage within one round-trip now sees "acknowledged"
  messages that are only proxy-buffered.
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
