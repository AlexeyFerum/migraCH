FROM ubuntu:22.04

# ── System packages ──────────────────────────────────────────────────────────
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        python3 \
        python3-pip \
        perl \
        apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# ── ClickHouse client (same version as server images) ────────────────────────
RUN curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
        | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] \
        https://packages.clickhouse.com/deb stable main" \
        > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends clickhouse-client \
    && rm -rf /var/lib/apt/lists/*

# ── Scripts ───────────────────────────────────────────────────────────────────
COPY scripts/ /scripts/
RUN chmod +x /scripts/*.sh

# Log and backup directories expected by the migration script
RUN mkdir -p /var/lib/clickhouse/migration_backup /var/log

CMD ["/scripts/entrypoint.sh"]
