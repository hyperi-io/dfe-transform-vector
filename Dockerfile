# Project:   dfe-transform-vector
# File:      Dockerfile
# Purpose:   Multi-stage build — Rust wrapper + Vector binary
# Language:  Dockerfile
#
# License:   FSL-1.1-ALv2
# Copyright: (c) 2026 HYPERI PTY LIMITED

# =============================================================================
# Stage 1: Rust build
# =============================================================================
FROM rust:1.85-bookworm AS builder

WORKDIR /build

# Cache dependencies by building a dummy project first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release 2>/dev/null || true \
    && rm -rf src

# Copy source and build for real
COPY src/ src/
RUN cargo build --release --bin dfe-transform-vector

# =============================================================================
# Stage 2: Vector binary (decoupled from Rust build)
# =============================================================================
FROM debian:bookworm-slim AS vector

ARG VECTOR_VERSION=0.48.0
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Map Docker TARGETARCH to Vector release naming
RUN ARCH=$(case "${TARGETARCH}" in \
        amd64) echo "x86_64" ;; \
        arm64) echo "aarch64" ;; \
        *) echo "${TARGETARCH}" ;; \
    esac) \
    && curl -fsSL "https://packages.timber.io/vector/${VECTOR_VERSION}/vector-${VECTOR_VERSION}-${ARCH}-unknown-linux-gnu.tar.gz" \
       | tar xz -C /tmp --strip-components=2 \
    && install -m 0755 /tmp/bin/vector /usr/local/bin/vector \
    && rm -rf /tmp/*

# =============================================================================
# Stage 3: Runtime
# =============================================================================
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries
COPY --from=builder /build/target/release/dfe-transform-vector /usr/local/bin/dfe-transform-vector
COPY --from=vector /usr/local/bin/vector /usr/local/bin/vector

# Create non-root user and data directories
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /var/lib/vector /var/run/vector/config /etc/dfe \
    && chown -R appuser:appuser /var/lib/vector /var/run/vector /etc/dfe

USER appuser

# Health (9000), Metrics (9090), Vector API (8686)
EXPOSE 9000 9090 8686

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:9000/health/live > /dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/dfe-transform-vector"]
CMD ["--config", "/etc/dfe/config.yaml"]
