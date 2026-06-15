# syntax=docker/dockerfile:1
#
# Ledge production image (Phase 4e). Multi-stage: a Debian-based Rust builder with
# the real native build deps (capnproto for ledge-rpc codegen; cmake + clang for
# aws-lc-rs, the rustls crypto provider linked since 4d-4) → a minimal non-root
# debian-slim runtime. glibc matches across stages (both bookworm) so the
# dynamically-linked binary runs as-is.

# ---- builder ----------------------------------------------------------------
# Pinned to 1.89 (the locally-verified toolchain). The workspace declares
# rust-version = "1.78", but transitive deps (e.g. constant_time_eq 0.4.2) now
# require a newer Cargo manifest parser, so the effective MSRV is higher than the
# stale declaration. 1.89 is known-good (builds + passes the full suite).
FROM rust:1.89-bookworm AS builder

# capnproto: crates/ledge-rpc/build.rs runs capnpc at build time.
# cmake + clang: aws-lc-rs (rustls provider) compiles C at build time.
# lld: the workspace's .cargo/config.toml links the linux build with `-fuse-ld=lld`.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends capnproto cmake clang lld \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
# Copy the full workspace (the .dockerignore drops target/.git/docs/formal). A
# cargo-chef dependency-cache layer is a documented follow-on; correctness first.
COPY . .
RUN cargo build --release -p ledge-server

# ---- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: TLS hygiene. curl: container HEALTHCHECK. git: the [sync]
# import feature shells out to the git binary to clone upstream repos.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 ledge \
    && useradd --uid 10001 --gid 10001 --home-dir /var/lib/ledge --shell /usr/sbin/nologin ledge \
    && mkdir -p /var/lib/ledge \
    && chown -R ledge:ledge /var/lib/ledge

COPY --from=builder /build/target/release/ledge /usr/local/bin/ledge
COPY deploy/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER ledge
WORKDIR /var/lib/ledge

# 3000 = client port: git/REST/RPC/admin (+ /healthz + /metrics for back-compat) +
# /raft + /cluster (HTTP, or HTTPS under TLS). 9090 = dedicated plain-HTTP
# metrics/health listener ([metrics].addr) — Prometheus + probes scrape here
# TLS-agnostically even when the client port is TLS. NOTE: [cluster].raft_bind is
# NOT a separate listener — peers reach /raft on the client port (mTLS peers via
# tls.peer_addr; publish that port too). See deploy/README.md.
EXPOSE 3000 9090
VOLUME ["/var/lib/ledge"]

# Healthcheck the dedicated metrics/health port (plain HTTP, always, regardless of
# client TLS). Requires metrics.enabled=true (default); if disabled, override.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:9090/healthz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["start"]
