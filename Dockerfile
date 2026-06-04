FROM rust:1.93-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
# buf CLI: build.rs imports proto definitions from buf.build (see build.rs)
RUN BUF_VERSION=1.69.0 && \
    curl -fsSL "https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-$(uname -s)-$(uname -m)" \
      -o /usr/local/bin/buf && chmod +x /usr/local/bin/buf
COPY . .
RUN cargo build --release --bin substreams-websocket

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/substreams-websocket /usr/local/bin/
ENTRYPOINT ["substreams-websocket"]
CMD ["serve"]
