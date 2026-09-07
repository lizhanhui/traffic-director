use std::net::SocketAddr;
use std::time::Duration;

use traffic_director::server::{self, ShedMode};

fn usage() -> ! {
    eprintln!(
        "usage: traffic-director --listen <addr> --broker <addr> [--mode drain|migrate] [--drain-timeout-secs <n>]\n\
         signals: SIGUSR2 = zero-downtime upgrade, SIGTERM/SIGINT = graceful stop"
    );
    std::process::exit(2);
}

fn parse_args() -> (SocketAddr, SocketAddr, Duration, ShedMode) {
    let mut listen = None;
    let mut broker = None;
    let mut drain_timeout = Duration::from_secs(60);
    let mut mode = ShedMode::Drain;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = Some(args.next().unwrap_or_else(|| usage())),
            "--broker" => broker = Some(args.next().unwrap_or_else(|| usage())),
            "--drain-timeout-secs" => {
                let secs = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
                drain_timeout = Duration::from_secs(secs);
            }
            "--mode" => {
                mode = match args.next().unwrap_or_else(|| usage()).as_str() {
                    "drain" => ShedMode::Drain,
                    "migrate" => ShedMode::Migrate,
                    _ => usage(),
                };
            }
            _ => usage(),
        }
    }

    let listen = listen.unwrap_or_else(|| usage()).parse().unwrap_or_else(|_| usage());
    let broker = broker.unwrap_or_else(|| usage()).parse().unwrap_or_else(|_| usage());
    (listen, broker, drain_timeout, mode)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let (listen, broker, drain_timeout, mode) = parse_args();
    server::run(listen, broker, drain_timeout, mode).await
}
