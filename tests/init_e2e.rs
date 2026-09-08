//! td-init regression test (no containers needed): a shed must not make the
//! init exit while a descendant generation lives. Generic inits (tini,
//! dumb-init) exit when their direct child exits — which would tear down a
//! container on every shed.

mod common;

use std::process::Command;
use std::time::Duration;

use common::{
    free_port, require_broker, signal, v3_connect, v3_ping, wait_connectable, wait_exit,
};

const TD_INIT: &str = env!("CARGO_BIN_EXE_td-init");

#[tokio::test]
async fn init_survives_generational_exit_on_shed() {
    require_broker().await;

    let port = free_port();
    let listen: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut init = Command::new(TD_INIT)
        .args([
            "--",
            common::BIN,
            "--listen",
            &listen.to_string(),
            "--broker",
            common::BROKER,
            "--mode",
            "migrate",
        ])
        .spawn()
        .unwrap();
    let _cleanup = common::Cleanup(format!("--listen {listen}"));

    wait_connectable(listen).await;

    let mut a = v3_connect(listen, "init-e2e").await;
    let a_local_before = a.get_ref().local_addr().unwrap();

    // Shed via the init (as `docker kill -s USR2` would deliver to PID 1).
    signal(init.id(), "-USR2");

    // The init must NOT exit when the old generation exits: the new
    // generation (a descendant) is alive.
    assert!(
        wait_exit(&mut init, Duration::from_millis(1500)).is_none(),
        "td-init exited while a descendant generation was still alive"
    );

    // The shed worked end-to-end: A's socket survived on the new generation.
    assert_eq!(a.get_ref().local_addr().unwrap(), a_local_before);
    v3_ping(&mut a).await;

    // Graceful stop via the init: everything exits, init reports success.
    signal(init.id(), "-TERM");
    let status = wait_exit(&mut init, Duration::from_secs(10))
        .expect("td-init did not exit after graceful stop");
    assert!(status.success(), "td-init exited with {status}");
}
