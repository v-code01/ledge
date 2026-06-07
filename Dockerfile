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
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends capnproto cmake clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
# Copy the full workspace (the .dockerignore drops target/.git/docs/formal). A
# cargo-chef dependency-cache layer is a documented follow-on; correctness first.
COPY . .
RUN cargo build --release -p ledge-server

# ---- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: TLS hygiene. curl: container HEALTHCHECK.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
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

# 3000 = the single client port: git/REST/RPC/admin + /healthz + /metrics + /raft
# + /cluster (HTTP, or HTTPS under TLS). NOTE: the server serves EVERYTHING on the
# client port — config.metrics.addr and config.cluster.raft_bind are currently NOT
# separate listeners, so peers reach /raft on the client port. Under mTLS a second
# listener is bound on tls.peer_addr (publish that port too). See deploy/README.md.
EXPOSE 3000
VOLUME ["/var/lib/ledge"]

# /healthz is on the client port. This default healthcheck assumes plaintext
# (TLS disabled). For a TLS deployment, override with an https + CA healthcheck
# (e.g. `curl -fsS --cacert /etc/ledge/tls/ca.crt https://localhost:3000/healthz`).
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:3000/healthz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["start"]
