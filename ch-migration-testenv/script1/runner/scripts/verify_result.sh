#!/bin/bash
# =============================================================================
# verify_result.sh  –  Manual post-migration checks for script1.
# Run inside the runner container after the migration completes.
# =============================================================================

set -euo pipefail

NEW_S1R1="${NEW_S1R1_HOST:-new-s1-r1}"
OLD_SHARD1="${OLD_SHARD1_HOST:-old-shard1}"

qnew() { clickhouse client --host="$NEW_S1R1" --port=9000 --multiquery -q "$1"; }
qold() { clickhouse client --host="$OLD_SHARD1" --port=9000 --multiquery -q "$1"; }

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

pass() { echo -e "${GREEN}  ✓ $1${NC}"; }
fail() { echo -e "${RED}  ✗ $1${NC}"; }
info() { echo -e "${YELLOW}  → $1${NC}"; }

echo "================================================================"
echo "  Post-migration verification  (Script 1 – cross-cluster)"
echo "================================================================"

echo ""
echo "── 1. Engine transformations ───────────────────────────────────"
while IFS=$'\t' read -r db tbl old_eng; do
    new_eng=$(qnew "SELECT engine FROM system.tables WHERE database='$db' AND name='$tbl'" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$new_eng" ]; then
        fail "$db.$tbl: NOT FOUND in new cluster"
        continue
    fi
    if echo "$old_eng" | grep -qiP "MergeTree" && ! echo "$old_eng" | grep -qi "Replicated"; then
        if echo "$new_eng" | grep -qi "Replicated"; then
            pass "$db.$tbl: $old_eng  →  $new_eng"
        else
            fail "$db.$tbl: expected Replicated*, got '$new_eng'"
        fi
    else
        info "$db.$tbl: $old_eng  →  $new_eng (no conversion expected)"
    fi
done < <(qold "SELECT database, name, engine FROM system.tables WHERE database NOT IN ('system','information_schema','INFORMATION_SCHEMA','default') AND engine NOT ILIKE '%view%' AND engine NOT ILIKE 'dictionary'" 2>/dev/null)

echo ""
echo "── 2. ZooKeeper paths contain {shard} and {replica} macros ────"
qnew "
SELECT database, name, engine_full
FROM system.tables
WHERE database NOT IN ('system','information_schema')
  AND engine_full ILIKE '%ReplicatedMergeTree%'
FORMAT Vertical
" 2>/dev/null | grep -E "(database|name|engine_full)" | head -40

echo ""
echo "── 3. Row counts – old vs new (distributed view) ──────────────"
for db_table in \
    analytics.events_local \
    analytics.metrics_local \
    analytics.page_views \
    analytics.audit_log \
    inventory.products \
    inventory.stock_movements
do
    old_total=0
    for shard_host in "$OLD_SHARD1" old-shard2; do
        c=$(clickhouse client --host="$shard_host" --port=9000 -q "SELECT count() FROM $db_table" 2>/dev/null || echo 0)
        old_total=$((old_total + c))
    done
    new_total=$(qnew "SELECT sum(count) FROM clusterAllReplicas('epm_cluster', system.tables) WHERE concat(database,'.', name) = '$db_table'" 2>/dev/null | tr -d '[:space:]' || echo 0)
    # fallback: simple count from entry node
    if [ -z "$new_total" ] || [ "$new_total" = "0" ]; then
        new_total=$(qnew "SELECT count() FROM $db_table" 2>/dev/null | tr -d '[:space:]' || echo 0)
    fi
    if [ "$old_total" -eq "$new_total" ] 2>/dev/null; then
        pass "$db_table: $old_total rows"
    else
        fail "$db_table: old=$old_total  new=$new_total"
    fi
done

echo ""
echo "── 4. Replica health ───────────────────────────────────────────"
unhealthy=$(qnew "
SELECT database, table, replica_name, is_readonly, is_session_expired
FROM system.replicas
WHERE is_readonly = 1 OR is_session_expired = 1
" 2>/dev/null)

if [ -z "$unhealthy" ]; then
    pass "All replicas healthy"
else
    fail "Unhealthy replicas found:"
    echo "$unhealthy"
fi

under=$(qnew "
SELECT database, table, count() AS cnt
FROM system.replicas
GROUP BY database, table
HAVING cnt < 2
" 2>/dev/null)

if [ -z "$under" ]; then
    pass "All replicated tables have ≥ 2 replicas"
else
    fail "Under-replicated tables:"
    echo "$under"
fi

echo ""
echo "── 5. ReplicatedMergeTree ZK paths (sample) ───────────────────"
qnew "
SELECT database, table, zookeeper_path, replica_name
FROM system.replicas
ORDER BY database, table
LIMIT 10
FORMAT PrettyCompact
" 2>/dev/null

echo ""
echo "================================================================"
echo "  Verification complete"
echo "================================================================"
