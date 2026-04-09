#!/bin/bash
# =============================================================================
# run_migration.sh  –  Patches clickhouse_migration.sh for the Docker
# container hostnames and TLS settings, then executes it.
#
# The script uses --insecure because we use a self-signed test CA.
# In production you would use --verify-tls-cert (the default) together
# with a properly signed certificate.
# =============================================================================

set -euo pipefail

SCRIPT_SRC="/scripts/clickhouse_migration.sh"
SCRIPT_RUN="/tmp/clickhouse_migration_run.sh"

OLD_SHARD1="${OLD_SHARD1_HOST:-old-shard1}"
NEW_S1R1="${NEW_S1R1_HOST:-new-s1-r1}"

echo "Patching migration script for Docker hostnames..."

sed \
    -e "s|OLD_CLUSTER_HOST=\"old-clickhouse-server\"|OLD_CLUSTER_HOST=\"${OLD_SHARD1}\"|" \
    -e "s|NEW_CLUSTER_HOST=\"new-clickhouse-server\"|NEW_CLUSTER_HOST=\"${NEW_S1R1}\"|" \
    -e "s|OLD_CLUSTER_SECURE_PORT=\"9440\"|OLD_CLUSTER_SECURE_PORT=\"9440\"|" \
    -e "s|NEW_CLUSTER_SECURE_PORT=\"9440\"|NEW_CLUSTER_SECURE_PORT=\"9440\"|" \
    -e "s|CLICKHOUSE_PASSWORD=\"\"|CLICKHOUSE_PASSWORD=\"\"|" \
    -e "s|ENABLE_TLS=true|ENABLE_TLS=true|" \
    -e "s|VERIFY_TLS_CERT=true|VERIFY_TLS_CERT=false|" \
    -e "s|CLUSTER_NAME=\"main\"|CLUSTER_NAME=\"main\"|" \
    -e "s|BACKUP_DIR=.*|BACKUP_DIR=\"/var/lib/clickhouse/migration_backup\"|" \
    -e "s|LOG_FILE=.*|LOG_FILE=\"/var/log/clickhouse-migration.log\"|" \
    "$SCRIPT_SRC" > "$SCRIPT_RUN"

chmod +x "$SCRIPT_RUN"

echo "Running clickhouse_migration.sh ..."
echo "  OLD: ${OLD_SHARD1}:9440 (TLS, self-signed)"
echo "  NEW: ${NEW_S1R1}:9440  (TLS, self-signed; ON CLUSTER fans out to all replicas)"
echo ""

bash "$SCRIPT_RUN"
