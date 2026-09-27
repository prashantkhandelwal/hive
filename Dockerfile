# syntax=docker/dockerfile:1.7

FROM rust:1.93-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

ARG HIVE_VERSION
ARG TARGETARCH
ARG TARGETVARIANT
ENV HIVE_VERSION=${HIVE_VERSION}

RUN --mount=type=cache,id=hive-cargo-registry-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry \
    --mount=type=cache,id=hive-cargo-git-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git \
    cargo build --locked --release

FROM debian:bookworm-slim AS runtime

RUN groupadd --system hive \
    && useradd --system --gid hive --home-dir /var/lib/hive --create-home hive \
    && install --directory --owner hive --group hive /data /etc/hive

COPY --from=builder /app/target/release/hive-tracker /usr/local/bin/hive-tracker
COPY docker/hive.toml /etc/hive/hive.toml

USER hive
WORKDIR /var/lib/hive

VOLUME ["/data"]
EXPOSE 3000/tcp
EXPOSE 6969/udp

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["hive-tracker", "--health-check"]

STOPSIGNAL SIGTERM

ENTRYPOINT ["hive-tracker"]
CMD ["--config", "/etc/hive/hive.toml"]
