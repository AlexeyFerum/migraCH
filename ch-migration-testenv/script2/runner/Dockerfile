FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        openssl \
        python3 \
        python3-pip \
        perl \
        apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# ── ClickHouse client ─────────────────────────────────────────────────────────
RUN curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
        | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] \
        https://packages.clickhouse.com/deb stable main" \
        > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends clickhouse-client \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/ /scripts/
RUN chmod +x /scripts/*.sh

RUN mkdir -p /var/lib/clickhouse/migration_backup /var/log

CMD ["/scripts/entrypoint.sh"]
