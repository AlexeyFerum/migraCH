#!/bin/bash
# =============================================================================
# verify_result.sh  –  Post-migration checks for script2 (TLS cluster).
# =============================================================================

set -euo pipefail

OLD_SHARD1="${OLD_SHARD1_HOST:-old-shard1}"
OLD_SHARD2="${OLD_SHARD2_HOST:-old-shard2}"
NEW_S1R1="${NEW_S1R1_HOST:-new-s1-r1}"

qold1() {
    clickhouse-client --host="$OLD_SHARD1" --port=9440 \
        --secure --no-verify --multiquery -q "$1"
}
qold2() {
    clickhouse-client --host="$OLD_SHARD2" --port=9440 \
        --secure --no-verify --multiquery -q "$1"
}
qnew() {
    clickhouse-client --host="$NEW_S1R1" --port=9440 \
        --secure --no-verify --multiquery -q "$1"
}

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

pass() { echo -e "${GREEN}  ✓ $1${NC}"; }
fail() { echo -e "${RED}  ✗ $1${NC}"; }
info() { echo -e "${YELLOW}  → $1${NC}"; }

echo "================================================================"
echo "  Post-migration verification  (Script 2 – in-cluster TLS)"
echo "================================================================"

echo ""
echo "── 1. Engine transformations ───────────────────────────────────"
while IFS=$'\t' read -r db tbl old_eng; do
    new_eng=$(qnew \
        "SELECT engine FROM system.tables WHERE database='$db' AND name='$tbl'" \
        2>/dev/null | tr -d '[:space:]')

    if [ -z "$new_eng" ]; then
        fail "$db.$tbl: NOT FOUND in new cluster"
        continue
    fi

    if echo "$old_eng" | grep -qiP "MergeTree" && \
       ! echo "$old_eng" | grep -qi "Replicated"; then
        if echo "$new_eng" | grep -qi "Replicated"; then
            pass "$db.$tbl: $old_eng  →  $new_eng"
        else
            fail "$db.$tbl: expected Replicated*, got '$new_eng'"
        fi
    else
        info "$db.$tbl: $old_eng  →  $new_eng (no conversion expected)"
    fi
done < <(qold1 \
    "SELECT database, name, engine
     FROM system.tables
     WHERE database NOT IN ('system','information_schema','_temporary_and_external_tables')
       AND engine NOT LIKE '%View%'" \
    2>/dev/null)

echo ""
echo "── 2. ZooKeeper paths in engine_full (sample) ──────────────────"
qnew "
SELECT database, name,
       extractAll(engine_full, '\\'[^\\']+\\'')[1] AS zk_path,
       extractAll(engine_full, '\\'[^\\']+\\'')[2] AS replica_macro
FROM system.tables
WHERE database NOT IN ('system','information_schema')
  AND engine LIKE 'Replicated%'
ORDER BY database, name
LIMIT 12
FORMAT PrettyCompact
" 2>/dev/null

echo ""
echo "── 3. Row counts – old vs new ──────────────────────────────────"
for db_table in \
    analytics.events_local \
    analytics.metrics_local \
    analytics.page_views \
    analytics.audit_log \
    inventory.products \
    inventory.stock_movements
do
    c1=$(qold1 "SELECT count() FROM $db_table" 2>/dev/null | tr -d '[:space:]' || echo 0)
    c2=$(qold2 "SELECT count() FROM $db_table" 2>/dev/null | tr -d '[:space:]' || echo 0)
    old_total=$((c1 + c2))

    new_total=$(qnew "SELECT count() FROM $db_table" 2>/dev/null | tr -d '[:space:]' || echo 0)

    if [ "$old_total" -eq "$new_total" ] 2>/dev/null; then
        pass "$db_table: $old_total rows"
    else
        fail "$db_table: old_total=$old_total  new_entry_node=$new_total"
    fi
done

echo ""
echo "── 4. ON CLUSTER clause present in new DDL ─────────────────────"
# SHOW CREATE TABLE output should contain ON CLUSTER for all non-view tables
while IFS=$'\t' read -r db tbl; do
    ddl=$(qnew "SHOW CREATE TABLE \`$db\`.\`$tbl\`" 2>/dev/null)
    if echo "$ddl" | grep -qi "ON CLUSTER"; then
        pass "$db.$tbl has ON CLUSTER"
    else
        fail "$db.$tbl missing ON CLUSTER in DDL"
    fi
done < <(qnew \
    "SELECT database, name FROM system.tables
     WHERE database NOT IN ('system','information_schema')
       AND engine NOT LIKE '%View%'
       AND engine NOT LIKE 'Dictionary%'" \
    2>/dev/null)

echo ""
echo "── 5. Replica health ───────────────────────────────────────────"
unhealthy=$(qnew "
SELECT database, table, replica_name, is_readonly, is_session_expired
FROM system.replicas
WHERE is_readonly = 1 OR is_session_expired = 1
" 2>/dev/null)

if [ -z "$unhealthy" ]; then
    pass "All replicas healthy"
else
    fail "Unhealthy replicas:"
    echo "$unhealthy"
fi

under=$(qnew "
SELECT database, table, count() AS cnt
FROM system.replicas
GROUP BY database, table
HAVING cnt < 2
" 2>/dev/null)

if [ -z "$under" ]; then
    pass "All replicated tables have >= 2 replicas"
else
    fail "Under-replicated tables (< 2 replicas):"
    echo "$under"
fi

echo ""
echo "── 6. system.replicas overview ─────────────────────────────────"
qnew "
SELECT database, table, replica_name,
       zookeeper_path,
       is_leader,
       total_replicas,
       active_replicas
FROM system.replicas
ORDER BY database, table, replica_name
FORMAT PrettyCompact
" 2>/dev/null

echo ""
echo "── 7. Dry-run smoke test ───────────────────────────────────────"
echo "Running migration script in --dry-run mode against old cluster..."
PATCHED_SCRIPT="/tmp/clickhouse_migration_dryrun.sh"
sed \
    -e "s|OLD_CLUSTER_HOST=\"old-clickhouse-server\"|OLD_CLUSTER_HOST=\"${OLD_SHARD1}\"|" \
    -e "s|NEW_CLUSTER_HOST=\"new-clickhouse-server\"|NEW_CLUSTER_HOST=\"${NEW_S1R1}\"|" \
    -e "s|VERIFY_TLS_CERT=true|VERIFY_TLS_CERT=false|" \
    -e "s|BACKUP_DIR=.*|BACKUP_DIR=\"/var/lib/clickhouse/migration_backup_dryrun\"|" \
    -e "s|LOG_FILE=.*|LOG_FILE=\"/var/log/clickhouse-migration-dryrun.log\"|" \
    /scripts/clickhouse_migration.sh > "$PATCHED_SCRIPT"
chmod +x "$PATCHED_SCRIPT"
bash "$PATCHED_SCRIPT" --dry-run 2>&1 | tail -20
pass "Dry-run completed without error"

echo ""
echo "================================================================"
echo "  Verification complete"
echo "================================================================"
