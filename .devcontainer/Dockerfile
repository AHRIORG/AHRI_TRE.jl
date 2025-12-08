FROM --platform=${BUILDPLATFORM} julia:latest

ARG TARGETARCH

ENV DUCKDB_RELEASE_BASE="https://github.com/duckdb/duckdb/releases/latest/download"

# Basic tooling + Postgres client libraries + CIFS for Samba mounting
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        git-lfs \
        ca-certificates \
        curl \
        wget \
        unzip \
        postgresql-client \
        libpq-dev \
        cifs-utils \
    && rm -rf /var/lib/apt/lists/*

# Install the latest DuckDB CLI release that matches the target architecture
RUN set -eux; \
    case "$TARGETARCH" in \
        amd64) duckdb_asset="duckdb_cli-linux-amd64.zip" ;; \
        arm64) duckdb_asset="duckdb_cli-linux-arm64.zip" ;; \
        *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -Ls "$DUCKDB_RELEASE_BASE/$duckdb_asset" -o /tmp/duckdb_cli.zip && \
    unzip /tmp/duckdb_cli.zip -d /usr/local/bin && \
    chmod +x /usr/local/bin/duckdb && \
    rm /tmp/duckdb_cli.zip

WORKDIR /workspace

ENV JULIA_PROJECT=/workspace

# Copy only Project.toml to allow fresh dependency resolution
COPY Project.toml ./

CMD ["julia"]
