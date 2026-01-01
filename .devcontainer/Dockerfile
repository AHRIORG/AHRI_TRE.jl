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
        gnupg \
        apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        mssql-tools18 \
        unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Add mssql-tools to PATH
ENV PATH="$PATH:/opt/mssql-tools18/bin"

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

# Install Revise into the default global Julia environment and auto-load it for REPL sessions
RUN julia -e 'import Pkg; env = "@v$(VERSION.major).$(VERSION.minor)"; Pkg.activate(env); Pkg.add("Revise"); Pkg.precompile()'

RUN mkdir -p /root/.julia/config && \
    printf '%s\n' \
    'try' \
    '    @eval using Revise' \
    'catch err' \
    'end' \
    > /root/.julia/config/startup.jl

CMD ["julia"]
