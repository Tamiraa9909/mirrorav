# ---------- builder ----------
FROM rust:1-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake clang git perl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release

# ---------- runtime ----------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/mirrorav /usr/local/bin/mirrorav

ENV PORT=7860 \
    WORK_DIR=/data/tmp \
    SESSION_PATH=/data/mirrorav.session

VOLUME /data
EXPOSE 7860

CMD ["/usr/local/bin/mirrorav"]
