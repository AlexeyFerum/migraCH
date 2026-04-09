#!/bin/bash
# =============================================================================
# run_migration.sh  –  Patches host/port variables in ch_migration_asis.sh
# so they resolve to the Docker container hostnames, then executes it.
# =============================================================================

set -euo pipefail

SCRIPT_SRC="/scripts/ch_migration_asis.sh"
SCRIPT_RUN="/tmp/ch_migration_asis_run.sh"

OLD_SHARD1="${OLD_SHARD1_HOST:-old-shard1}"
NEW_S1R1="${NEW_S1R1_HOST:-new-s1-r1}"

echo "Patching migration script..."
sed \
    -e "s|OLD_CLUSTER_HOST=\"localhost\"|OLD_CLUSTER_HOST=\"${OLD_SHARD1}\"|" \
    -e "s|OLD_CLUSTER_PORT=\"19000\"|OLD_CLUSTER_PORT=\"9000\"|" \
    -e "s|NEW_CLUSTER_HOST=\"localhost\"|NEW_CLUSTER_HOST=\"${NEW_S1R1}\"|" \
    -e "s|NEW_CLUSTER_PORT=\"29000\"|NEW_CLUSTER_PORT=\"9000\"|" \
    -e "s|OLD_CLICKHOUSE_PASSWORD=\"changeme\"|OLD_CLICKHOUSE_PASSWORD=\"\"|" \
    -e "s|NEW_CLICKHOUSE_PASSWORD=\"changeme\"|NEW_CLICKHOUSE_PASSWORD=\"\"|" \
    -e "s|BACKUP_DIR=.*|BACKUP_DIR=\"/var/lib/clickhouse/migration_backup\"|" \
    -e "s|LOG_FILE=.*|LOG_FILE=\"/var/log/clickhouse-migration.log\"|" \
    "$SCRIPT_SRC" > "$SCRIPT_RUN"

chmod +x "$SCRIPT_RUN"

echo "Running ch_migration_asis.sh ..."
echo "  OLD: ${OLD_SHARD1}:9000"
echo "  NEW: ${NEW_S1R1}:9000  (entry point; ON CLUSTER will fan out to all replicas)"
echo ""

bash "$SCRIPT_RUN"
