# syntax=docker/dockerfile:1.7
#
# Multi-stage build for substreams-websocket.
#
# Build:
#   docker build -t substreams-websocket .
#
# Run (env-only config, suitable for Railway / Fly / Heroku):
#   docker run --rm -p 8080:8080 \
#     -e SUBSTREAMS_API_KEY=<your key> \
#     -e SUBSTREAMS_WEBSOCKET_LISTEN=0.0.0.0:8080 \
#     -e SUBSTREAMS_WEBSOCKET_STREAMS_TOML="$(cat streams.toml)" \
#     -v $(pwd)/cursors:/data/cursors \
#     -e SUBSTREAMS_WEBSOCKET_CURSORS_DIR=/data/cursors \
#     substreams-websocket

ARG RUST_VERSION=1.90

# -----------------------------------------------------------------------------
# Builder
# -----------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-bookworm AS builder

# protoc is required by the build script.
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy the full workspace. Layer caching is good enough without cargo-chef
# for a single binary crate.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build the release binary. Use BuildKit cache mounts so repeat builds reuse
# the registry, git, and target directories.
RUN --mount=type=cache,id=s/4c51fc52-0454-4c54-a13b-50a767997aca-/usr/local/cargo/registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=s/4c51fc52-0454-4c54-a13b-50a767997aca-/usr/local/cargo/git,target=/usr/local/cargo/git \
    --mount=type=cache,id=s/4c51fc52-0454-4c54-a13b-50a767997aca-/src/target,target=/src/target \
    cargo build --release --bin substreams-websocket \
 && cp /src/target/release/substreams-websocket /usr/local/bin/substreams-websocket

# -----------------------------------------------------------------------------
# Runtime
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# CA roots for HTTPS (manifest fetch, Pinax auth, Substreams TLS) + libssl.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --shell /sbin/nologin --uid 10001 app

WORKDIR /app

COPY --from=builder /usr/local/bin/substreams-websocket /usr/local/bin/substreams-websocket

# Default writable cursors dir; mount a persistent volume here in production.
RUN mkdir -p /data/cursors && chown -R app:app /data
ENV SUBSTREAMS_WEBSOCKET_CURSORS_DIR=/data/cursors \
    SUBSTREAMS_WEBSOCKET_LISTEN=0.0.0.0:8080

USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/substreams-websocket"]
CMD ["serve"]
