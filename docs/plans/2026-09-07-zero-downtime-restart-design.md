# Zero-Downtime Self-Restart — Design

Date: 2026-09-07
Status: Approved (PoC phase)
Scope: Proof-of-concepts for design goal 2 of `docs/design.md` — zero-downtime
self-restart of traffic-director without disrupting existing connections.

## Decisions

| Question | Decision |
| --- | --- |
| Fate of existing connections on restart | Both as PoCs: drain (old process keeps serving) and migrate (hand connections to new process), then compare |
| Proxy layer | L7 MQTT-aware from the start (MQTT v3.1.1 + v5.0) |
| L7 mode | Full termination on both sides: proxy is an MQTT endpoint toward client and toward broker |
| Migration state scope | Handshake params + subscription table + QoS1/2 in-flight windows (client-facing) |
| Restart mechanism | `ecdysis` crate (tableflip-style): listener socket inheritance + state pipe |
| Test environment | Real broker (mosquitto) in a container; Rust test harness in-repo |
| Out of scope for PoC | TLS, xDS/Istio multi-cluster routing, `foundations` crate, connection pooling, load testing |

## 1. Process model & restart lifecycle

Single `traffic-director` binary, three lifecycle states:
`Normal → Shedding → Exit`. `SIGUSR2` (or a CLI flag in the PoC) triggers an
ecdysis shed: the parent spawns the new binary and passes the listener via
ecdysis socket inheritance; the child detects the inherited FD and reclaims it
instead of binding port 1883 fresh. No listener gap, no `SO_REUSEPORT` races.

The PoCs diverge in what happens to `Session` tasks during `Shedding`:

- **PoC 1 (drain):** parent stops accepting, keeps its `Session` tasks alive
  until each client disconnects, exits on `sessions == 0` or a drain timeout
  (default 60s, then hard exit). Child runs fully independently.
- **PoC 2 (migrate):** before spawning, the parent freezes each session,
  serializes `Vec<SessionSnapshot>` over the ecdysis inheritance pipe, and
  passes the **client-side** socket FDs. The child reads snapshots, adopts the
  FDs, and re-creates broker-side connections with `clean_start=false` so the
  broker redelivers unacked QoS messages from its own session store. Only the
  client-facing QoS window crosses the migration boundary.

Rationale for migrating only the client side: passing both FDs would require
migrating broker-side protocol state as well; re-establishing with MQTT
session resumption is strictly simpler and exercises a feature real brokers
already implement.

`foundations` stays out of the PoCs — plain `tokio` + `ecdysis` + `rmqtt-codec`.

## 2. Session core (L7 full termination)

Each accepted client connection spawns one `Session` actor (tokio task):

- **Client side:** `rmqtt-codec` framed codec over the accepted TCP stream.
  First packet must be CONNECT; extract MQTT version, client-id,
  clean-start/session-expiry, keepalive, will message into `ClientParams`.
- **Broker side:** the session originates its own MQTT connection to the
  selected backend using the *same* client-id, forwarding clean-start /
  session-expiry semantics. One broker connection per proxied client.
- **Forwarding with end-to-end ack chaining:**
  - Client→broker QoS1/2: PUBACK/PUBREC is sent to the client only after the
    broker acknowledges.
  - Broker→client QoS1/2: outbound window tracks
    `packet_id → (packet bytes, ack-state)`; retransmitted per spec on resume.
- **Subscription table:** SUBSCRIBE/SUBACK intercepted and recorded
  (`topic_filter → max_qos`); the SUBSCRIBE is still forwarded to the broker.
- **Freeze/thaw (PoC 2):** `freeze() -> SessionSnapshot` stops I/O at a packet
  boundary, drains in-flight codec buffers into the QoS window (bytes of
  unacked packets are kept; nothing half-sent is lost), serializes via serde.
  `thaw()` adopts the client FD, replays the window, and re-SUBSCRIBEs on the
  new broker connection.

## 3. Data flow

Normal path (both PoCs):

```text
client ──TCP──▶ [accept loop] ──▶ Session actor ──decode──▶ routing
                                     ▲                        │ encode
                                     └──────── broker conn ◀──┘
```

PoC scope uses a single statically configured backend; xDS is out of scope.
Keepalive is end-to-end: the client's keepalive value is forwarded to the
broker-side CONNECT; PINGREQ/PINGRESP are chained on both hops.

### PoC 1 shed (drain)

1. `SIGUSR2` → parent sheds; child reclaims listener FD, starts accepting.
2. Parent's accept loop stops; existing sessions keep flowing through the old
   binary.
3. Parent exits when session count reaches zero or the drain timeout fires.

### PoC 2 shed (migrate)

1. `SIGUSR2` → parent broadcasts `freeze` to all sessions; each snapshots at a
   packet boundary.
2. Parent writes snapshots to the ecdysis pipe, passes client FDs
   (`CLOEXEC` cleared), spawns child.
3. Child reclaims listener, reads snapshots; for each: adopt client FD, open
   broker-side connection (`clean_start=false`, same client-id → broker
   redelivers unacked QoS1/2), re-SUBSCRIBE, thaw window (retransmit unacked
   client-facing PUBLISHs with DUP=1).
4. Child signals readiness via ecdysis; parent exits immediately after
   handoff — no old code keeps running.

Invariant: from the client's perspective a migration is a brief TCP stall —
the socket never closes, no MQTT reconnect occurs, and QoS ordering is
preserved because retransmission starts only after the window is fully
reloaded.

## 4. Error handling & failure modes

- **Child fails to boot (both):** ecdysis contract — crashing during
  initialisation is OK. Parent never stops accepting, continues as the sole
  generation; shed attempts are logged and rate-limited.
- **Parent crashes mid-migration (PoC 2):** parent writes snapshots and clears
  `CLOEXEC` *before* spawn; if it dies after spawn the child still has
  everything. If it dies before spawn, nothing was shed.
- **Child crashes after adopting FDs:** worst case — adopted sockets die with
  the child. PoC mitigation is observability + minimal, fully tested boot
  sequence; supervisor-level recovery is out of scope.
- **Broker unreachable during migration (PoC 2):** session is not dropped; it
  retries the broker-side connection with backoff while the client socket
  stays open. Inbound client PUBLISHs are answered per QoS and buffered in the
  window (bounded) until the broker link returns; on buffer overflow the
  client is disconnected.
- **No healthy backend at accept time:** reject CONNECT with reason code
  `0x88` (v5) / `0x03` (v3.1.1), close cleanly.
- **Codec/protocol errors:** malformed packet → log and close that session
  only. Never panic the process.
- **Drain timeout (PoC 1):** long-lived connections may outlive the timeout;
  on expiry the parent exits and remaining clients reconnect to the new
  generation via normal MQTT reconnect logic. Documented trade-off of drain
  mode.
- **Broker-initiated administrative DISCONNECT (v5):** intercepted
  (`NormalDisconnection`, `ServerShuttingDown`, `ServerBusy`,
  `UseAnotherServer`, `ServerMoved`) and treated as a transport loss — the
  session enters the outage path instead of telling the client. Client-fault
  reason codes are forwarded and end the session.

### v5 session expiry: explicit non-goal

When a v5 client connects with `session_expiry_interval = 0`, the broker
discards the session at transport loss, so a broker-node restart (or a
failover to another node) loses that session's queued offline QoS1/2
messages. **The proxy deliberately does not rewrite `session_expiry`**
(decision 2026-09-07): overriding a client's declared session semantics
would change what the client asked for and leak session state on brokers
for sessions that were meant to be transient.

Consequences and where the responsibility lies:

- What the proxy guarantees with `expiry = 0`: the client's connection and
  in-flight QoS windows survive proxy/broker restarts, subscriptions are
  restored via re-SUBSCRIBE, and publishes during a broker outage are
  locally acked and buffered by the proxy itself.
- What it cannot guarantee: messages the *broker* had queued for the client
  (offline messages). Preserving those requires broker-side session
  retention — e.g. mosquitto `persistence true`, or a cluster with shared
  session state (EMQX et al.) — plus clients connecting with a non-zero
  `session_expiry_interval`. That is a broker/cluster and client
  configuration concern, not a proxy one.

## 5. Testing & verification

Backend: mosquitto in a container (MQTT v3.1.1 + v5 on one listener).
Harness: in-repo Rust client (`rumqttc` or `rmqtt-codec`) for precise timing
control; sheds triggered programmatically by signalling the parent PID.

### PoC 1 (drain)

1. N clients (v3.1.1 + v5 mix) publishing QoS0/1/2 continuously; shed →
   assert new connections land on child, existing clients never disconnect,
   QoS1 arrives exactly-once, parent exits when drained.
2. Drain timeout: hold one client open past timeout → parent hard-exits →
   client reconnects to child, session resumed (`clean_start=false`).

### PoC 2 (migrate)

1. Same churn; shed mid-stream → assert zero TCP disconnects (client-side
   local port constant across shed), zero loss, zero unexpected duplicates.
2. QoS1 window stress: high-rate publish, shed injected at random offsets
   between PUBLISH and PUBACK → no gaps/dupes after thaw.
3. QoS2 handshake interrupted at each of the 4 steps → PUBREC/PUBREL state
   survives migration.
4. Broker killed during migration → child retries broker-side, client stays
   connected, buffered messages delivered on broker return.

### Harness assertions

Message-logging client records `(publisher, seq)` tuples; a post-run checker
verifies ordering, completeness, and duplication count.

### Non-goals

TLS, xDS/multi-cluster routing, load tests, broker clustering.
