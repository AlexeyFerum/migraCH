#!/bin/bash

# ============================================================
# ClickHouse In-Cluster Topology Migration Script
# Migrates data within a single physical ClickHouse cluster to a
# new logical topology: shards-only -> shards + replicas.
#
# Key changes applied to all tables:
#   - All MergeTree-family engines are converted to their
#     Replicated* counterparts with correct ZooKeeper paths.
#   - CREATE statements receive ON CLUSTER clauses.
#   - Data is transferred via remoteSecure() INSERT … SELECT,
#     partition by partition for large tables.
#
# Use --dry-run to preview all DDL changes without applying them.
# ============================================================

# --------------- Configuration ---------------
OLD_CLUSTER_HOST="old-clickhouse-server"
OLD_CLUSTER_SECURE_PORT="9440"
NEW_CLUSTER_HOST="new-clickhouse-server"
NEW_CLUSTER_SECURE_PORT="9440"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""
BACKUP_DIR="/var/lib/clickhouse/migration_backup"
LOG_FILE="/var/log/clickhouse-migration.log"
ENABLE_TLS=true
VERIFY_TLS_CERT=true
CLUSTER_NAME="main"

# Expected number of replicas per shard after migration.
# Used in post-migration replica health verification.
EXPECTED_REPLICAS=2

# ZooKeeper path prefix template.
# {cluster}, {shard}, {replica} are resolved from ClickHouse macros.
ZK_PATH_PREFIX="/clickhouse/{cluster}"

# When true, print all DDL to stdout but do not execute anything
# on the new cluster and do not transfer any data.
DRY_RUN=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# -------- Helpers --------

get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S.%6N'
}

log() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${timestamp} - $1" | tee -a "$LOG_FILE"
}

success() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${GREEN}${timestamp} - $1${NC}" | tee -a "$LOG_FILE"
}

error() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${RED}${timestamp} - ERROR: $1${NC}" | tee -a "$LOG_FILE"
    exit 1
}

warning() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${YELLOW}${timestamp} - WARNING: $1${NC}" | tee -a "$LOG_FILE"
}

dry_run_log() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${CYAN}${timestamp} - [DRY-RUN] $1${NC}" | tee -a "$LOG_FILE"
}

# -------- ClickHouse query wrappers --------

clickhouse_query() {
    local host=$1
    local port=$2
    local query=$3
    local secure_flag=""

    if [ "$ENABLE_TLS" = true ]; then
        secure_flag="--secure"
        [ "$VERIFY_TLS_CERT" = false ] && secure_flag="$secure_flag --insecure"
    fi

    # shellcheck disable=SC2086
    clickhouse-client \
        --host="$host" \
        --port="$port" \
        $secure_flag \
        --user="$CLICKHOUSE_USER" \
        --password="$CLICKHOUSE_PASSWORD" \
        --multiquery \
        -q "$query" 2>> "$LOG_FILE"
}

clickhouse_query_old() {
    clickhouse_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_SECURE_PORT" "$1"
}

clickhouse_query_new() {
    local query=$1

    if [ "$DRY_RUN" = true ]; then
        dry_run_log "Would execute on new cluster:"
        echo "$query" | sed 's/^/    /' | tee -a "$LOG_FILE"
        return 0
    fi

    clickhouse_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_SECURE_PORT" "$query"
}

# -------- Pre-flight checks --------

check_connections() {
    log "Checking connection to old cluster..."
    if clickhouse_query_old "SELECT 1" >/dev/null 2>&1; then
        success "Connection to old cluster established"
    else
        error "Failed to connect to old cluster"
    fi

    if [ "$DRY_RUN" = true ]; then
        dry_run_log "Skipping new cluster connection check (dry-run mode)"
        return 0
    fi

    log "Checking connection to new cluster..."
    if clickhouse_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_SECURE_PORT" "SELECT 1" >/dev/null 2>&1; then
        success "Connection to new cluster established"
    else
        error "Failed to connect to new cluster"
    fi
}

# Verify that the ClickHouse macros required by ReplicatedMergeTree
# are configured on every node of the new cluster.
check_replication_macros() {
    if [ "$DRY_RUN" = true ]; then
        dry_run_log "Skipping macro check (dry-run mode)"
        return 0
    fi

    log "Checking replication macros on new cluster..."

    local shard_macro replica_macro cluster_macro missing=0

    shard_macro=$(clickhouse_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_SECURE_PORT" \
        "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')
    replica_macro=$(clickhouse_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_SECURE_PORT" \
        "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')
    cluster_macro=$(clickhouse_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_SECURE_PORT" \
        "SELECT getMacro('cluster')" 2>/dev/null | tr -d '[:space:]')

    if [ -z "$shard_macro" ] || [ "$shard_macro" = "shard" ]; then
        warning "  Macro 'shard' is not defined on the new cluster"
        missing=1
    else
        log "  Macro 'shard'   = $shard_macro"
    fi

    if [ -z "$replica_macro" ] || [ "$replica_macro" = "replica" ]; then
        warning "  Macro 'replica' is not defined on the new cluster"
        missing=1
    else
        log "  Macro 'replica' = $replica_macro"
    fi

    if [ -z "$cluster_macro" ] || [ "$cluster_macro" = "cluster" ]; then
        warning "  Macro 'cluster' is not defined – ZK paths using {cluster} will be literal strings"
    else
        log "  Macro 'cluster' = $cluster_macro"
    fi

    if [ $missing -eq 1 ]; then
        error "Required ClickHouse macros ('shard', 'replica') are missing on the new cluster. " \
              "Add them to /etc/clickhouse-server/config.d/macros.xml on every new-cluster node."
    fi

    success "Replication macros verified"
}

# -------- Backup directory --------

create_backup_dir() {
    log "Creating backup directory..."
    mkdir -p "$BACKUP_DIR/ddl" "$BACKUP_DIR/data" "$BACKUP_DIR/users"
    chown -R clickhouse:clickhouse "$BACKUP_DIR" 2>/dev/null || true
}

# -------- DDL export --------

export_tables() {
    local db=$1
    local tables
    tables=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db' AND engine NOT LIKE '%View%'")

    for table in $tables; do
        log "  Exporting table: $table"
        clickhouse_query_old \
            "SHOW CREATE TABLE \`$db\`.\`$table\`" \
            > "$BACKUP_DIR/ddl/$db/$table.sql"
    done
}

export_views() {
    local db=$1
    local views
    views=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db' AND engine LIKE '%View%'")

    for view in $views; do
        log "  Exporting view: $view"
        clickhouse_query_old \
            "SHOW CREATE TABLE \`$db\`.\`$view\`" \
            > "$BACKUP_DIR/ddl/$db/$view.view.sql"
    done
}

export_dictionaries() {
    local db=$1
    local dicts
    dicts=$(clickhouse_query_old \
        "SELECT name FROM system.dictionaries WHERE database = '$db'")

    for dict in $dicts; do
        log "  Exporting dictionary: $dict"
        clickhouse_query_old \
            "SHOW CREATE DICTIONARY \`$db\`.\`$dict\`" \
            > "$BACKUP_DIR/ddl/$db/$dict.dict.sql"
    done
}

export_users() {
    log "Exporting users and permissions..."

    clickhouse_query_old "SHOW USERS" | while read -r user; do
        clickhouse_query_old \
            "SHOW CREATE USER \`$user\`" \
            > "$BACKUP_DIR/users/$user.user.sql" 2>/dev/null || true
    done

    clickhouse_query_old "SHOW ROLES" | while read -r role; do
        clickhouse_query_old \
            "SHOW CREATE ROLE \`$role\`" \
            > "$BACKUP_DIR/users/$role.role.sql" 2>/dev/null || true
    done

    clickhouse_query_old "SHOW QUOTAS" | while read -r quota; do
        clickhouse_query_old \
            "SHOW CREATE QUOTA \`$quota\`" \
            > "$BACKUP_DIR/users/$quota.quota.sql" 2>/dev/null || true
    done

    clickhouse_query_old "SHOW SETTINGS PROFILES" | while read -r profile; do
        clickhouse_query_old \
            "SHOW CREATE SETTINGS PROFILE \`$profile\`" \
            > "$BACKUP_DIR/users/$profile.profile.sql" 2>/dev/null || true
    done

    success "User export completed"
}

# Step 1
export_ddl() {
    log "=== Step 1: Exporting DDL ==="

    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases
         WHERE name NOT IN ('system', 'information_schema', '_temporary_and_external_tables')")

    if [ -z "$databases" ]; then
        warning "No non-system databases found"
        return 0
    fi

    for db in $databases; do
        log "Exporting database: $db"
        mkdir -p "$BACKUP_DIR/ddl/$db"

        clickhouse_query_old "SHOW CREATE DATABASE \`$db\`" \
            > "$BACKUP_DIR/ddl/$db/database.sql"

        export_tables "$db"
        export_views "$db"
        export_dictionaries "$db"
    done

    success "DDL export completed"
}

# -------- DDL modification --------

# Add "ON CLUSTER <name>" to a CREATE statement.
add_on_cluster() {
    local content=$1
    local entity_type=$2  # DATABASE | TABLE | VIEW | MATERIALIZED VIEW | DICTIONARY

    if echo "$content" | grep -qi "ON CLUSTER"; then
        echo "$content"
        return 0
    fi

    # Match: CREATE [OR REPLACE] [MATERIALIZED] TABLE/VIEW/DATABASE/DICTIONARY `name`
    # and insert ON CLUSTER immediately after the object name.
    echo "$content" | perl -pe \
        "s|(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+\S+)|\$1 ON CLUSTER ${CLUSTER_NAME}|i; last"
}

# ---------------------------------------------------------------------------
# convert_engine_to_replicated
#
# Rewrites a MergeTree-family ENGINE clause to its Replicated* variant.
#
# Conversion map:
#   MergeTree                    -> ReplicatedMergeTree
#   SummingMergeTree             -> ReplicatedSummingMergeTree
#   AggregatingMergeTree         -> ReplicatedAggregatingMergeTree
#   CollapsingMergeTree          -> ReplicatedCollapsingMergeTree
#   ReplacingMergeTree           -> ReplicatedReplacingMergeTree
#   VersionedCollapsingMergeTree -> ReplicatedVersionedCollapsingMergeTree
#   GraphiteMergeTree            -> ReplicatedGraphiteMergeTree
#
# Already-Replicated* engines:   ZK path is rewritten; other args preserved.
# Non-MergeTree engines:         content returned unchanged.
# ---------------------------------------------------------------------------
convert_engine_to_replicated() {
    local content=$1
    local db_name=$2
    local table_name=$3

    local zk_path="${ZK_PATH_PREFIX}/${db_name}/${table_name}/{shard}"
    local zk_replica="{replica}"

    # Case 1: already Replicated* — just rewrite the ZK path.
    if echo "$content" | grep -qiP "ENGINE\s*=\s*Replicated\w*MergeTree"; then
        log "    Engine is already Replicated*MergeTree – rewriting ZK path to $zk_path"
        echo "$content" | perl -pe \
            "s|(ENGINE\s*=\s*Replicated\w*MergeTree\s*\()('[^']*'\s*,\s*'[^']*')|\${1}'${zk_path}', '${zk_replica}'|i"
        return 0
    fi

    # Case 2: plain MergeTree-family — detect the variant, then convert.
    local engine_variant
    engine_variant=$(echo "$content" | \
        grep -oiP "ENGINE\s*=\s*\K(Summing|Aggregating|Collapsing|Replacing|VersionedCollapsing|Graphite)?MergeTree" \
        | head -1)

    if [ -z "$engine_variant" ]; then
        # Not a MergeTree engine — return unchanged.
        echo "$content"
        return 0
    fi

    log "    Converting $engine_variant -> Replicated${engine_variant}"

    # Extract existing engine arguments (if any) using Python for safe
    # parenthesis matching. These must be appended after the ZK args.
    local extra_args
    extra_args=$(python3 - "$engine_variant" <<'PYEOF' <<< "$content"
import sys, re

variant = sys.argv[1]
content = sys.stdin.read()

pattern = re.compile(
    r'ENGINE\s*=\s*' + re.escape(variant) + r'MergeTree\s*\(([^)]*)\)',
    re.IGNORECASE
)
m = pattern.search(content)
if m:
    args = m.group(1).strip()
    print(args)
PYEOF
)

    local new_engine_call
    if [ -n "$extra_args" ]; then
        new_engine_call="Replicated${engine_variant}('${zk_path}', '${zk_replica}', ${extra_args})"
    else
        new_engine_call="Replicated${engine_variant}('${zk_path}', '${zk_replica}')"
    fi

    echo "$content" | perl -pe \
        "s|ENGINE\s*=\s*${engine_variant}MergeTree\s*\([^)]*\)|ENGINE = ${new_engine_call}|i"
}

# Master DDL modifier: applies ON CLUSTER injection and engine conversion.
modify_ddl_for_cluster() {
    local file_path=$1
    local entity_type=$2   # database | table | view | dictionary
    local db_name=$3
    local entity_name=$4

    [ -f "$file_path" ] || { warning "File $file_path does not exist"; return 1; }

    log "  Modifying DDL for $entity_type: $db_name.${entity_name:-<database>}"

    local content
    content=$(cat "$file_path")

    # Step A: inject ON CLUSTER
    content=$(add_on_cluster "$content" "$entity_type")

    # Step B: convert engine (tables only)
    if [ "$entity_type" = "table" ]; then
        content=$(convert_engine_to_replicated "$content" "$db_name" "$entity_name")
    fi

    echo "$content" > "$file_path"
}

# -------- Apply DDL --------

# Step 2
apply_ddl() {
    log "=== Step 2: Applying DDL to new cluster ==="

    # -- Databases --
    for db_sql in "$BACKUP_DIR/ddl"/*/database.sql; do
        [ -f "$db_sql" ] || continue
        local db_name
        db_name=$(basename "$(dirname "$db_sql")")

        modify_ddl_for_cluster "$db_sql" "database" "$db_name" ""

        log "Creating database: $db_name"
        if [ "$DRY_RUN" = true ]; then
            dry_run_log "DDL for database $db_name:"
            cat "$db_sql" | sed 's/^/    /'
        else
            clickhouse_query_new "$(cat "$db_sql")" || \
                warning "Failed to create database '$db_name' (may already exist)"
        fi
    done

    apply_tables
    apply_views
    apply_dictionaries

    success "DDL application completed"
}

apply_tables() {
    log "Applying tables..."

    # Glob all .sql files, then filter out database/view/dict files.
    for table_sql in "$BACKUP_DIR/ddl"/*/*.sql; do
        [ -f "$table_sql" ] || continue

        local filename
        filename=$(basename "$table_sql")

        # Skip non-table files (fixes the database.sql glob bug)
        [[ "$filename" == "database.sql" ]] && continue
        [[ "$filename" == *.view.sql     ]] && continue
        [[ "$filename" == *.dict.sql     ]] && continue

        local db_name table_name
        db_name=$(basename "$(dirname "$table_sql")")
        table_name=$(basename "$table_sql" .sql)

        modify_ddl_for_cluster "$table_sql" "table" "$db_name" "$table_name"

        log "  Creating table: $db_name.$table_name"
        if [ "$DRY_RUN" = true ]; then
            dry_run_log "DDL for $db_name.$table_name:"
            cat "$table_sql" | sed 's/^/    /'
        else
            clickhouse_query_new "$(cat "$table_sql")" || \
                warning "Failed to create table '$db_name.$table_name'"
        fi
    done
}

apply_views() {
    log "Applying views..."

    for view_sql in "$BACKUP_DIR/ddl"/*/*.view.sql; do
        [ -f "$view_sql" ] || continue

        local db_name view_name
        db_name=$(basename "$(dirname "$view_sql")")
        view_name=$(basename "$view_sql" .view.sql)

        modify_ddl_for_cluster "$view_sql" "view" "$db_name" "$view_name"

        log "  Creating view: $db_name.$view_name"
        if [ "$DRY_RUN" = true ]; then
            dry_run_log "DDL for view $db_name.$view_name:"
            cat "$view_sql" | sed 's/^/    /'
        else
            clickhouse_query_new "$(cat "$view_sql")" || \
                warning "Failed to create view '$db_name.$view_name'"
        fi
    done
}

apply_dictionaries() {
    log "Applying dictionaries..."

    for dict_sql in "$BACKUP_DIR/ddl"/*/*.dict.sql; do
        [ -f "$dict_sql" ] || continue

        local db_name dict_name
        db_name=$(basename "$(dirname "$dict_sql")")
        dict_name=$(basename "$dict_sql" .dict.sql)

        modify_ddl_for_cluster "$dict_sql" "dictionary" "$db_name" "$dict_name"

        log "  Creating dictionary: $db_name.$dict_name"
        if [ "$DRY_RUN" = true ]; then
            dry_run_log "DDL for dictionary $db_name.$dict_name:"
            cat "$dict_sql" | sed 's/^/    /'
        else
            clickhouse_query_new "$(cat "$dict_sql")" || \
                warning "Failed to create dictionary '$db_name.$dict_name'"
        fi
    done
}

# -------- Data migration --------

# Step 3: Transfer data from old cluster to new cluster.
migrate_data() {
    log "=== Step 3: Data migration ==="

    if [ "$DRY_RUN" = true ]; then
        dry_run_log "Skipping data migration (dry-run mode)"
        return 0
    fi

    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases
         WHERE name NOT IN ('system', 'information_schema', '_temporary_and_external_tables')")

    [ -z "$databases" ] && { warning "No databases to migrate"; return 0; }

    for db in $databases; do
        log "Migrating data from database: $db"

        local tables
        tables=$(clickhouse_query_old \
            "SELECT name FROM system.tables
             WHERE database = '$db' AND engine NOT LIKE '%View%'")

        for table in $tables; do
            migrate_table "$db" "$table"
        done
    done

    success "Data migration completed"
}

migrate_table() {
    local db=$1
    local table=$2

    log "  Migrating table: $db.$table"

    local engine
    engine=$(clickhouse_query_old \
        "SELECT engine FROM system.tables WHERE database = '$db' AND name = '$table'" \
        | tr -d '[:space:]')

    case "$engine" in
        *Distributed*)
            migrate_distributed_table "$db" "$table"
            ;;
        *MergeTree*)
            migrate_mergetree_table "$db" "$table"
            ;;
        *)
            migrate_simple_table "$db" "$table"
            ;;
    esac
}

migrate_mergetree_table() {
    local db=$1
    local table=$2

    # Fetch the list of active partition IDs from system.parts.
    # Using partition_id (the directory-level identifier) rather than the
    # human-readable "partition" expression avoids any column-level ambiguity.
    local partition_ids
    partition_ids=$(clickhouse_query_old \
        "SELECT DISTINCT partition_id
         FROM system.parts
         WHERE database = '$db' AND table = '$table' AND active
         ORDER BY partition_id")

    if [ -z "$partition_ids" ]; then
        log "    No active parts found – migrating entire table"
        migrate_simple_table "$db" "$table"
        return
    fi

    log "    Migrating by partition ($(echo "$partition_ids" | wc -l | tr -d ' ') partition(s))..."

    for partition_id in $partition_ids; do
        log "    Partition: $partition_id"
        # Filter on _partition_id (virtual column available in all MergeTree tables).
        clickhouse_query_new \
            "INSERT INTO \`$db\`.\`$table\`
             SELECT * FROM remoteSecure('${OLD_CLUSTER_HOST}:${OLD_CLUSTER_SECURE_PORT}', \`$db\`, \`$table\`,
                 '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')
             WHERE _partition_id = '$partition_id'" || \
            warning "    Failed to migrate partition $partition_id of $db.$table"
    done

    # Wait for replication to settle before reporting success.
    clickhouse_query_new "SYSTEM SYNC REPLICA \`$db\`.\`$table\`" >/dev/null 2>&1 || true
}

migrate_distributed_table() {
    local db=$1
    local table=$2

    # Attempt to retrieve underlying local tables from system.distributed_tables.
    # This view is available in ClickHouse 22.x+; fall back to simple migration
    # if the view does not exist or returns no rows.
    local underlying_tables
    underlying_tables=$(clickhouse_query_old \
        "SELECT underlying_table FROM system.distributed_tables
         WHERE database = '$db' AND name = '$table'" 2>/dev/null)

    if [ -z "$underlying_tables" ]; then
        log "    No underlying tables found – migrating distributed table directly"
        migrate_simple_table "$db" "$table"
        return
    fi

    for underlying_table in $underlying_tables; do
        log "    Migrating underlying table: $underlying_table"
        clickhouse_query_new \
            "INSERT INTO \`$db\`.\`$table\`
             SELECT * FROM remoteSecure('${OLD_CLUSTER_HOST}:${OLD_CLUSTER_SECURE_PORT}', \`$db\`, \`$underlying_table\`,
                 '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')" || \
            warning "    Failed to migrate underlying table $underlying_table"
    done
}

migrate_simple_table() {
    local db=$1
    local table=$2

    log "    Simple migration (full table scan)..."
    clickhouse_query_new \
        "INSERT INTO \`$db\`.\`$table\`
         SELECT * FROM remoteSecure('${OLD_CLUSTER_HOST}:${OLD_CLUSTER_SECURE_PORT}', \`$db\`, \`$table\`,
             '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')" || \
        warning "    Failed to migrate table $db.$table"
}

# -------- Apply users --------

# Step 4
apply_users() {
    log "=== Step 4: Applying users and permissions ==="

    if [ "$DRY_RUN" = true ]; then
        dry_run_log "Skipping user application (dry-run mode)"
        return 0
    fi

    for user_file in "$BACKUP_DIR/users"/*.user.sql; do
        [ -f "$user_file" ] || continue
        local user_name
        user_name=$(basename "$user_file" .user.sql)
        log "Creating user: $user_name"
        clickhouse_query_new "$(cat "$user_file")" || \
            warning "Failed to create user '$user_name'"
    done

    for role_file in "$BACKUP_DIR/users"/*.role.sql; do
        [ -f "$role_file" ] || continue
        local role_name
        role_name=$(basename "$role_file" .role.sql)
        log "Creating role: $role_name"
        clickhouse_query_new "$(cat "$role_file")" || \
            warning "Failed to create role '$role_name'"
    done

    success "Users and permissions applied"
}

# -------- Verification --------

# Step 5
verify_migration() {
    log "=== Step 5: Verifying migration integrity ==="

    if [ "$DRY_RUN" = true ]; then
        dry_run_log "Skipping verification (dry-run mode)"
        return 0
    fi

    # Table counts
    local old_count new_count
    old_count=$(clickhouse_query_old \
        "SELECT count() FROM system.tables
         WHERE database NOT IN ('system', 'information_schema')" | tr -d '[:space:]')
    new_count=$(clickhouse_query_new \
        "SELECT count() FROM system.tables
         WHERE database NOT IN ('system', 'information_schema')" | tr -d '[:space:]')

    log "Tables in old cluster: $old_count"
    log "Tables in new cluster: $new_count"
    [ "$old_count" -eq "$new_count" ] \
        && success "Table counts match" \
        || warning "Table counts differ!"

    # Engine transformation verification
    verify_engine_transformations

    # Replica health and count verification
    verify_replica_health

    # Sample row-count verification
    log "Sample row-count verification..."
    local sample_tables
    sample_tables=$(clickhouse_query_old \
        "SELECT concat(database, '.', name) FROM system.tables
         WHERE database NOT IN ('system', 'information_schema')
           AND engine NOT LIKE '%View%'
         LIMIT 5")

    [ -z "$sample_tables" ] && { warning "No tables found for row-count verification"; return 0; }

    for table in $sample_tables; do
        local old_rows new_rows
        old_rows=$(clickhouse_query_old "SELECT count() FROM $table" | tr -d '[:space:]')
        new_rows=$(clickhouse_query_new "SELECT count() FROM $table" | tr -d '[:space:]')
        if [ "$old_rows" -eq "$new_rows" ]; then
            success "  $table: $old_rows rows – OK"
        else
            warning "  $table: row count mismatch (old=$old_rows, new=$new_rows)"
        fi
    done

    success "Verification completed"
}

# Verify that every MergeTree-family table was correctly converted
# to its Replicated* counterpart on the new cluster.
verify_engine_transformations() {
    log "  Verifying engine transformations..."

    local old_tables
    old_tables=$(clickhouse_query_old \
        "SELECT database, name, engine FROM system.tables
         WHERE database NOT IN ('system', 'information_schema')
           AND engine NOT LIKE '%View%'")

    local ok=0 fail=0

    while IFS=$'\t' read -r db table old_engine; do
        [ -z "$db" ] && continue

        local new_engine
        new_engine=$(clickhouse_query_new \
            "SELECT engine FROM system.tables WHERE database = '$db' AND name = '$table'" \
            | tr -d '[:space:]')

        if [ -z "$new_engine" ]; then
            warning "    $db.$table: NOT FOUND in new cluster"
            fail=$((fail + 1))
            continue
        fi

        if echo "$old_engine" | grep -qi "MergeTree" && \
           ! echo "$old_engine" | grep -qi "^Replicated"; then
            # Was plain MergeTree — new engine must be Replicated*MergeTree
            if echo "$new_engine" | grep -qiP "^Replicated\w*MergeTree$"; then
                log "    ✓ $db.$table: $old_engine -> $new_engine"
                ok=$((ok + 1))
            else
                warning "    ✗ $db.$table: expected Replicated*MergeTree, got '$new_engine'"
                fail=$((fail + 1))
            fi
        elif echo "$old_engine" | grep -qi "^Replicated"; then
            # Was already Replicated — must still be Replicated on new cluster
            if echo "$new_engine" | grep -qi "^Replicated"; then
                log "    ✓ $db.$table: Replicated (ZK path updated)"
                ok=$((ok + 1))
            else
                warning "    ✗ $db.$table: was Replicated on old cluster but is '$new_engine' on new"
                fail=$((fail + 1))
            fi
        else
            log "    – $db.$table: non-MergeTree engine '$old_engine' – no conversion expected"
        fi
    done <<< "$old_tables"

    log "    Engine transformations: $ok OK, $fail failed"
    [ $fail -eq 0 ] \
        && success "  All engine transformations verified correctly" \
        || warning "  $fail table(s) have incorrect engines after migration"
}

# Query system.replicas on the new cluster to confirm:
#   1. No replicas are in read-only or session-expired state.
#   2. The actual replica count per table matches EXPECTED_REPLICAS.
verify_replica_health() {
    log "  Verifying replica health (expecting $EXPECTED_REPLICAS replica(s) per shard)..."

    # Unhealthy replicas
    local unhealthy
    unhealthy=$(clickhouse_query_new \
        "SELECT database, table, replica_name, is_readonly, is_session_expired
         FROM system.replicas
         WHERE is_readonly = 1 OR is_session_expired = 1" 2>/dev/null)

    if [ -z "$unhealthy" ]; then
        success "  No unhealthy replicas found"
    else
        warning "  Unhealthy replicas detected:"
        echo "$unhealthy" | while IFS=$'\t' read -r db tbl replica readonly expired; do
            warning "    $db.$tbl / $replica  readonly=$readonly  session_expired=$expired"
        done
    fi

    # Tables with fewer replicas than expected
    local under_replicated
    under_replicated=$(clickhouse_query_new \
        "SELECT database, table, count() AS replica_count
         FROM system.replicas
         GROUP BY database, table
         HAVING replica_count < $EXPECTED_REPLICAS" 2>/dev/null)

    if [ -z "$under_replicated" ]; then
        success "  All tables have at least $EXPECTED_REPLICAS replica(s)"
    else
        warning "  Under-replicated tables (fewer than $EXPECTED_REPLICAS replica(s)):"
        echo "$under_replicated" | while IFS=$'\t' read -r db tbl cnt; do
            warning "    $db.$tbl: $cnt replica(s)"
        done
    fi
}

# -------- Entry point --------

main() {
    if [ "$DRY_RUN" = true ]; then
        log "======================================================="
        log "  DRY-RUN MODE – no changes will be made to the cluster"
        log "======================================================="
    fi

    log "Starting ClickHouse in-cluster topology migration"
    log "  Old cluster     : $OLD_CLUSTER_HOST:$OLD_CLUSTER_SECURE_PORT"
    log "  New cluster     : $NEW_CLUSTER_HOST:$NEW_CLUSTER_SECURE_PORT"
    log "  Cluster name    : $CLUSTER_NAME"
    log "  ZK prefix       : $ZK_PATH_PREFIX"
    log "  Expected replicas: $EXPECTED_REPLICAS"
    log "  TLS enabled     : $ENABLE_TLS"
    log "  Verify TLS cert : $VERIFY_TLS_CERT"

    if ! command -v clickhouse-client &>/dev/null; then
        error "clickhouse-client not found. Please install the ClickHouse client."
    fi

    if ! command -v python3 &>/dev/null; then
        error "python3 not found. It is required for engine parameter parsing."
    fi

    check_connections
    check_replication_macros
    create_backup_dir

    export_ddl
    export_users
    apply_ddl
    migrate_data
    apply_users
    verify_migration

    success "Migration successfully completed!"
    log "Logs       : $LOG_FILE"
    log "DDL backup : $BACKUP_DIR"
}

# -------- CLI argument parsing --------

while [[ $# -gt 0 ]]; do
    case $1 in
        --old-host)      OLD_CLUSTER_HOST="$2";        shift 2 ;;
        --new-host)      NEW_CLUSTER_HOST="$2";        shift 2 ;;
        --old-port)      OLD_CLUSTER_SECURE_PORT="$2"; shift 2 ;;
        --new-port)      NEW_CLUSTER_SECURE_PORT="$2"; shift 2 ;;
        --user)          CLICKHOUSE_USER="$2";         shift 2 ;;
        --password)      CLICKHOUSE_PASSWORD="$2";     shift 2 ;;
        --backup-dir)    BACKUP_DIR="$2";              shift 2 ;;
        --cluster-name)  CLUSTER_NAME="$2";            shift 2 ;;
        --zk-prefix)     ZK_PATH_PREFIX="$2";          shift 2 ;;
        --expected-replicas) EXPECTED_REPLICAS="$2";   shift 2 ;;
        --disable-tls)   ENABLE_TLS=false;             shift ;;
        --insecure)      VERIFY_TLS_CERT=false;        shift ;;
        --dry-run)       DRY_RUN=true;                 shift ;;
        --help)
            cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --old-host HOST              Old cluster host (default: $OLD_CLUSTER_HOST)
  --new-host HOST              New cluster host (default: $NEW_CLUSTER_HOST)
  --old-port PORT              Old cluster TLS port (default: $OLD_CLUSTER_SECURE_PORT)
  --new-port PORT              New cluster TLS port (default: $NEW_CLUSTER_SECURE_PORT)
  --user USER                  ClickHouse user (default: $CLICKHOUSE_USER)
  --password PASSWORD          ClickHouse password
  --backup-dir DIR             Backup directory (default: $BACKUP_DIR)
  --cluster-name NAME          Target cluster name for ON CLUSTER (default: $CLUSTER_NAME)
  --zk-prefix PREFIX           ZooKeeper path prefix (default: $ZK_PATH_PREFIX)
  --expected-replicas N        Expected replica count per shard for health check (default: $EXPECTED_REPLICAS)
  --disable-tls                Use unencrypted connections
  --insecure                   Skip TLS certificate verification
  --dry-run                    Export and transform DDL but make no changes to the new cluster

Note: Both old and new clusters share the same credentials (--user / --password)
      because this is an in-cluster migration. Both hosts must be accessible with
      the same user account.
EOF
            exit 0
            ;;
        *)
            echo "Unknown parameter: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

main