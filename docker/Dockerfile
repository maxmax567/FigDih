# ---- Compilación ----
FROM rust:slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build

COPY server/ ./server/
RUN cd server && cargo build --release

# ---- Imagen final mínima ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/server/target/release/figdih-server /usr/local/bin/figdih-server

WORKDIR /data
EXPOSE 3000
VOLUME ["/data"]
CMD ["figdih-server"]
