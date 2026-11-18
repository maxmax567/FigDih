# ---- Compilación ----
FROM rust:1.82-slim AS builder
WORKDIR /build

# Primero solo dependencias (cachea capas y acelera rebuilds)
COPY server/Cargo.toml server/Cargo.lock* ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs && cargo build --release

# Ahora el código real (solo recompila tu crate)
COPY server/src ./src
RUN touch src/main.rs && cargo build --release

# ---- Imagen final mínima ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/figdih-server /usr/local/bin/figdih-server

WORKDIR /data
EXPOSE 3000
VOLUME ["/data"]
CMD ["figdih-server"]
