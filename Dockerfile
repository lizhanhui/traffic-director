# syntax=docker/dockerfile:1

# ---- build stage ----
FROM rust:bookworm AS build
# foundations' TLS bindings use bindgen → needs libclang
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# ---- runtime stage ----
FROM debian:bookworm-slim
COPY --from=build /src/target/release/traffic-director /usr/local/bin/traffic-director
COPY --from=build /src/target/release/td-init /usr/local/bin/td-init

ENV RUST_LOG=info
EXPOSE 1883

# td-init runs as PID 1. A generic init (tini/dumb-init) would kill the
# container on the first shed: the old generation exiting is normal here.
# td-init forwards signals (SIGUSR2 = shed, SIGTERM = graceful stop) to all
# generations, reaps orphaned generations, and exits only when no
# descendant remains. Absolute paths: ecdysis re-executes argv[0] on shed.
ENTRYPOINT ["/usr/local/bin/td-init", "--"]
CMD ["/usr/local/bin/traffic-director"]
