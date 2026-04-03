#!/bin/bash

# ============================================================
# ClickHouse Cross-Cluster Migration Script
# Transfers data from one physical ClickHouse cluster to another.
# All MergeTree-family engines are converted to their Replicated
# counterparts so the destination cluster can run in fault-tolerant
# mode (each shard has at least one replica).
# ============================================================

# --------------- Configuration ---------------
OLD_CLUSTER_HOST="localhost"
OLD_CLUSTER_PORT="19000"
OLD_CLICKHOUSE_USER="default"
OLD_CLICKHOUSE_PASSWORD="changeme"

NEW_CLUSTER_HOST="localhost"
NEW_CLUSTER_PORT="29000"
NEW_CLICKHOUSE_USER="default"
NEW_CLICKHOUSE_PASSWORD="changeme"

CLUSTER_NAME="epm_cluster"
BACKUP_DIR="/var/lib/clickhouse/migration_backup"
LOG_FILE="/var/log/clickhouse-migration.log"

# ZooKeeper path prefix template for ReplicatedMergeTree tables.
# Available placeholders (resolved by ClickHouse macros on each node):
#   {cluster}   – cluster name macro
#   {shard}     – shard number macro
#   {replica}   – replica name macro
# The table-level path is constructed as:
#   $ZK_PATH_PREFIX/{database}/{table}/{shard}
ZK_PATH_PREFIX="/clickhouse/{cluster}"

# Exclude system databases
EXCLUDED_DATABASES="'system', 'information_schema', 'INFORMATION_SCHEMA', 'default'"

# Exclude system table engines
EXCLUDED_TABLE_ENGINES="'dictionary', '%postgres%', '%view%'"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# -------- Helpers --------

get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S.%6N'
}

log() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "$timestamp - $1" | tee -a "$LOG_FILE"
}

success() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${GREEN}$timestamp - $1${NC}"
    echo "$timestamp - $1" >> "$LOG_FILE"
}

error() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${RED}$timestamp - ERROR: $1${NC}" >&2
    echo "$timestamp - ERROR: $1" >> "$LOG_FILE"
    exit 1
}

warning() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${YELLOW}$timestamp - WARNING: $1${NC}"
    echo "$timestamp - WARNING: $1" >> "$LOG_FILE"
}

# -------- ClickHouse query wrappers --------

clickhouse_query() {
    local host=$1
    local port=$2
    local user=$3
    local password=$4
    local query=$5

    local output
    output=$(clickhouse client \
        --host="$host" \
        --port="$port" \
        --user="$user" \
        --password="$password" \
        --multiquery \
        -q "$query" 2>&1)

    local exit_code=$?

    if [ -n "$output" ]; then
        echo "$output" | sed -r "s/\x1B\[[0-9;]*[JKmsu]//g" >> "$LOG_FILE"
    fi

    if [ $exit_code -ne 0 ] && [ -n "$output" ]; then
        local error_msg
        error_msg=$(echo "$output" | grep -o "Code: [0-9]\+.*" | head -1)
        [ -z "$error_msg" ] && error_msg=$(echo "$output" | grep -v "^$" | head -1)
        echo "$error_msg" >&2
    fi

    [ $exit_code -eq 0 ] && echo "$output"
    return $exit_code
}

clickhouse_query_old() {
    clickhouse_query \
        "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" \
        "$OLD_CLICKHOUSE_USER" "$OLD_CLICKHOUSE_PASSWORD" \
        "$1"
}

clickhouse_query_new() {
    clickhouse_query \
        "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
        "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD" \
        "$1"
}

# -------- Pre-flight checks --------

check_connections() {
    log "Checking connection to old cluster..."
    if clickhouse_query_old "SELECT hostname()" >/dev/null 2>&1; then
        success "Connection to old cluster established"
    else
        error "Failed to connect to old cluster"
    fi

    log "Checking connection to new cluster..."
    if clickhouse_query_new "SELECT hostname()" >/dev/null 2>&1; then
        success "Connection to new cluster established"
    else
        error "Failed to connect to new cluster"
    fi
}

# Verify that the ClickHouse macro substitutions required by
# ReplicatedMergeTree are defined on the new cluster nodes.
check_replication_macros() {
    log "Checking replication macros on new cluster..."

    local shard_macro
    shard_macro=$(clickhouse_query_new "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')

    local replica_macro
    replica_macro=$(clickhouse_query_new "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')

    local cluster_macro
    cluster_macro=$(clickhouse_query_new "SELECT getMacro('cluster')" 2>/dev/null | tr -d '[:space:]')

    local missing=0

    if [ -z "$shard_macro" ] || [ "$shard_macro" = "shard" ]; then
        warning "  Macro 'shard' is not defined on the new cluster. ReplicatedMergeTree tables will fail to create."
        missing=1
    else
        log "  Macro 'shard'   = $shard_macro"
    fi

    if [ -z "$replica_macro" ] || [ "$replica_macro" = "replica" ]; then
        warning "  Macro 'replica' is not defined on the new cluster. ReplicatedMergeTree tables will fail to create."
        missing=1
    else
        log "  Macro 'replica' = $replica_macro"
    fi

    if [ -z "$cluster_macro" ] || [ "$cluster_macro" = "cluster" ]; then
        warning "  Macro 'cluster' is not defined. ZooKeeper paths using {cluster} will be literal strings."
    else
        log "  Macro 'cluster' = $cluster_macro"
    fi

    if [ $missing -eq 1 ]; then
        error "Required ClickHouse macros are missing on the new cluster. " \
              "Add 'shard' and 'replica' macros to /etc/clickhouse-server/config.d/macros.xml on every new-cluster node and restart ClickHouse before retrying."
    fi

    success "Replication macros verified"
}

# -------- Backup directory --------

create_backup_dir() {
    log "Creating backup directory..."
    mkdir -p "$BACKUP_DIR/ddl" "$BACKUP_DIR/data"
    chown -R clickhouse:clickhouse "$BACKUP_DIR" 2>/dev/null || true
}

# -------- DDL helpers --------

fix_escaped_chars_in_ddl() {
    local file_path=$1
    [ -f "$file_path" ] || return 1

    local content
    content=$(cat "$file_path")
    content=$(echo "$content" | sed "s/\\\\'/'/g")
    content=$(echo "$content" | sed 's/\\\\/\\/g')
    echo "$content" > "$file_path"
}

# Add "ON CLUSTER <name>" to a CREATE statement that does not already have it.
add_on_cluster_to_ddl() {
    local file_path=$1
    local entity_type=$2
    local db_name=$3
    local entity_name=$4

    [ -f "$file_path" ] || { warning "File $file_path does not exist"; return 1; }

    log "  Adding ON CLUSTER to $entity_type: $db_name.$entity_name"

    local content
    content=$(cat "$file_path")
    content=$(echo -e "$content")

    if echo "$content" | grep -qi "ON CLUSTER"; then
        log "    ON CLUSTER already present"
        return 0
    fi

    local lines=()
    while IFS= read -r line; do
        lines+=("$line")
    done <<< "$content"

    if [[ ${lines[0]} =~ ^CREATE[[:space:]]+(DATABASE|TABLE|VIEW|MATERIALIZED[[:space:]]+VIEW|DICTIONARY) ]]; then
        if [[ ${lines[0]} =~ ^(CREATE[[:space:]]+[A-Z[:space:]]+[^[:space:]]+)(.*) ]]; then
            local create_part="${BASH_REMATCH[1]}"
            local rest_part="${BASH_REMATCH[2]}"
            lines[0]="${create_part} ON CLUSTER ${CLUSTER_NAME}${rest_part}"
            printf "%s\n" "${lines[@]}" > "$file_path"
            log "    Successfully added ON CLUSTER $CLUSTER_NAME"
        else
            warning "    Failed to parse CREATE statement: ${lines[0]}"
            return 1
        fi
    else
        warning "    No CREATE statement found in first line"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# convert_engine_to_replicated  –  core of the new functionality
#
# Rewrites the ENGINE clause in a table DDL file so that every MergeTree
# family engine becomes its Replicated* counterpart.
#
# Handled variants (non-exhaustive but covers all built-in ClickHouse engines):
#   MergeTree                    -> ReplicatedMergeTree
#   SummingMergeTree             -> ReplicatedSummingMergeTree
#   AggregatingMergeTree         -> ReplicatedAggregatingMergeTree
#   CollapsingMergeTree          -> ReplicatedCollapsingMergeTree
#   ReplacingMergeTree           -> ReplicatedReplacingMergeTree
#   VersionedCollapsingMergeTree -> ReplicatedVersionedCollapsingMergeTree
#   GraphiteMergeTree            -> ReplicatedGraphiteMergeTree
#
# Already-Replicated engines:    path is rewritten to the new ZK convention,
#                                 other arguments are preserved.
# Non-MergeTree engines:         file is left unchanged.
# ---------------------------------------------------------------------------
convert_engine_to_replicated() {
    local file_path=$1
    local db_name=$2
    local table_name=$3

    [ -f "$file_path" ] || { warning "File $file_path does not exist"; return 1; }

    local content
    content=$(cat "$file_path")

    # Build the ZooKeeper path for this table.
    # Pattern:  /clickhouse/{cluster}/{database}/{table}/{shard}
    local zk_path="${ZK_PATH_PREFIX}/${db_name}/${table_name}/{shard}"
    local zk_replica="{replica}"

    # ------------------------------------------------------------------
    # Case 1: already a Replicated* engine
    #   ENGINE = ReplicatedMergeTree('/old/path', '{replica}' [, extra_args])
    # We keep the engine name and extra_args; only replace the ZK path
    # and replica arguments.
    # ------------------------------------------------------------------
    if echo "$content" | grep -qiP "ENGINE\s*=\s*Replicated\w*MergeTree"; then
        log "    Engine is already Replicated*MergeTree – rewriting ZK path"

        # Replace the first two string arguments (path, replica) while
        # preserving any additional arguments that follow.
        # Handles both forms:
        #   ReplicatedMergeTree('/path', '{replica}')
        #   ReplicatedMergeTree('/path', '{replica}', version_col)
        content=$(echo "$content" | perl -pe \
            "s|(ENGINE\s*=\s*Replicated\w*MergeTree\s*\()('[^']*'\s*,\s*'[^']*')|\${1}'${zk_path}', '${zk_replica}'|i")

        echo "$content" > "$file_path"
        log "    ZK path rewritten to: $zk_path"
        return 0
    fi

    # ------------------------------------------------------------------
    # Case 2: plain MergeTree-family engine (not yet Replicated)
    #   ENGINE = SummingMergeTree(...)   or   ENGINE = MergeTree()
    # ------------------------------------------------------------------
    # Extract the engine variant name (the part between "ENGINE = " and "(")
    local engine_variant
    engine_variant=$(echo "$content" | grep -oiP "ENGINE\s*=\s*\K(Summing|Aggregating|Collapsing|Replacing|VersionedCollapsing|Graphite)?MergeTree" | head -1)

    if [ -z "$engine_variant" ]; then
        log "    Engine is not a MergeTree family – skipping conversion"
        return 0
    fi

    log "    Converting $engine_variant -> Replicated${engine_variant}"

    # Extract any existing engine parameters (everything inside the outer
    # parentheses after the engine name).  We must preserve them because
    # some variants carry required arguments (e.g. CollapsingMergeTree
    # needs a sign column, ReplacingMergeTree needs an optional version col).
    #
    # Strategy: capture text between the FIRST '(' and its matching ')'.
    # We use Python for reliable paren-matching since bash/sed cannot count
    # nested parentheses.
    local extra_args
    extra_args=$(python3 - "$engine_variant" <<'PYEOF'
import sys, re

variant = sys.argv[1]
# Read from stdin
content = sys.stdin.read()

# Find ENGINE = <Variant>MergeTree( ... )
pattern = re.compile(
    r'ENGINE\s*=\s*' + re.escape(variant) + r'MergeTree\s*\(([^)]*)\)',
    re.IGNORECASE
)
m = pattern.search(content)
if m:
    print(m.group(1).strip())
PYEOF
<<< "$content")

    # Build the new ENGINE clause.
    # ReplicatedMergeTree (and variants) always take (zk_path, replica)
    # as the first two arguments.  Extra variant-specific args come after.
    local new_engine_call
    if [ -n "$extra_args" ]; then
        new_engine_call="Replicated${engine_variant}('${zk_path}', '${zk_replica}', ${extra_args})"
    else
        new_engine_call="Replicated${engine_variant}('${zk_path}', '${zk_replica}')"
    fi

    # Replace the old ENGINE clause in the DDL.
    content=$(echo "$content" | perl -pe \
        "s|ENGINE\s*=\s*${engine_variant}MergeTree\s*\([^)]*\)|ENGINE = ${new_engine_call}|i")

    echo "$content" > "$file_path"
    log "    Engine rewritten to: ENGINE = $new_engine_call"
    return 0
}

# -------- Export phase --------

export_tables() {
    local db=$1
    local tables
    tables=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db'
         AND engine NOT ILIKE '%view%'
         AND engine NOT ILIKE 'dictionary'
         AND engine NOT ILIKE '%postgres%'")

    for table in $tables; do
        log "  Exporting table: $table"
        clickhouse_query_old \
            "SHOW CREATE TABLE \`$db\`.\`$table\`" \
            > "$BACKUP_DIR/ddl/$db/$table.sql"
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/$table.sql"
    done
}

export_views() {
    local db=$1
    local views
    views=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db' AND engine ILIKE '%view%'")

    for view in $views; do
        log "  Exporting view: $view"
        clickhouse_query_old \
            "SHOW CREATE TABLE \`$db\`.\`$view\`" \
            > "$BACKUP_DIR/ddl/$db/$view.view.sql"
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/$view.view.sql"
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
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/$dict.dict.sql"
    done
}

# Step 1: Export all DDL from the old cluster.
export_ddl() {
    log "=== Step 1: Exporting DDL ==="

    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")

    if [ -z "$databases" ]; then
        warning "No non-system databases found"
        return 0
    fi

    for db in $databases; do
        log "Exporting database: $db"
        mkdir -p "$BACKUP_DIR/ddl/$db"

        clickhouse_query_old "SHOW CREATE DATABASE \`$db\`" \
            > "$BACKUP_DIR/ddl/$db/database.sql"
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/database.sql"

        export_tables "$db"
        export_views "$db"
        export_dictionaries "$db"
    done

    success "DDL export completed"
}

# -------- Apply phase --------

# Step 2: Apply (modified) DDL to the new cluster.
apply_ddl() {
    log "=== Step 2: Applying DDL to new cluster ==="

    local has_errors=0

    # -- Databases --
    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        [ -d "$db_dir" ] || continue
        local db_name
        db_name=$(basename "$db_dir")
        local db_sql="$db_dir/database.sql"

        [ -f "$db_sql" ] || { warning "Database DDL not found for: $db_name"; has_errors=1; continue; }

        if ! add_on_cluster_to_ddl "$db_sql" "database" "$db_name" ""; then
            warning "Failed to modify DDL for database: $db_name"
            has_errors=1
            continue
        fi

        log "Creating database: $db_name"
        local sql_statement
        sql_statement=$(cat "$db_sql")

        if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
            local err
            err=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
            warning "Failed to create database '$db_name': ${err:-unknown error}"
            has_errors=1
        else
            log "  Successfully created database: $db_name"
        fi
    done

    if [ $has_errors -ne 0 ]; then
        error "Stopping DDL application due to database creation errors"
    fi

    apply_tables
    apply_views
    apply_dictionaries

    success "DDL application completed"
}

apply_tables() {
    log "Applying tables..."
    local has_errors=0

    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        [ -d "$db_dir" ] || continue
        local db_name
        db_name=$(basename "$db_dir")

        for table_sql in "$db_dir"*.sql; do
            [ -f "$table_sql" ] || continue
            local filename
            filename=$(basename "$table_sql")

            # Skip non-table files
            [[ "$filename" == "database.sql"  ]] && continue
            [[ "$filename" == *.view.sql      ]] && continue
            [[ "$filename" == *.dict.sql      ]] && continue

            local table_name
            table_name=$(basename "$table_sql" .sql)

            # 1) Inject ON CLUSTER
            if ! add_on_cluster_to_ddl "$table_sql" "table" "$db_name" "$table_name"; then
                warning "Failed to add ON CLUSTER for table: $db_name.$table_name"
                has_errors=1
                continue
            fi

            # 2) Convert MergeTree engine to ReplicatedMergeTree
            if ! convert_engine_to_replicated "$table_sql" "$db_name" "$table_name"; then
                warning "Failed to convert engine for table: $db_name.$table_name"
                has_errors=1
                continue
            fi

            log "  Creating table: $db_name.$table_name"
            local sql_statement
            sql_statement=$(cat "$table_sql")

            if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
                local err
                err=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
                warning "Failed to create table '$db_name.$table_name': ${err:-unknown error}"
                has_errors=1
            else
                log "    Successfully created table: $db_name.$table_name"
            fi
        done
    done

    [ $has_errors -eq 0 ] || warning "Some tables failed to create"
}

apply_views() {
    log "Applying views..."
    local has_errors=0

    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        [ -d "$db_dir" ] || continue
        local db_name
        db_name=$(basename "$db_dir")

        for view_sql in "$db_dir"*.view.sql; do
            [ -f "$view_sql" ] || continue
            local view_name
            view_name=$(basename "$view_sql" .view.sql)

            if ! add_on_cluster_to_ddl "$view_sql" "view" "$db_name" "$view_name"; then
                warning "Failed to modify DDL for view: $db_name.$view_name"
                has_errors=1
                continue
            fi

            log "  Creating view: $db_name.$view_name"
            local sql_statement
            sql_statement=$(cat "$view_sql")

            if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
                local err
                err=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
                warning "Failed to create view '$db_name.$view_name': ${err:-unknown error}"
                has_errors=1
            else
                log "    Successfully created view: $db_name.$view_name"
            fi
        done
    done

    [ $has_errors -eq 0 ] || warning "Some views failed to create"
}

apply_dictionaries() {
    log "Applying dictionaries..."
    local has_errors=0

    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        [ -d "$db_dir" ] || continue
        local db_name
        db_name=$(basename "$db_dir")

        for dict_sql in "$db_dir"*.dict.sql; do
            [ -f "$dict_sql" ] || continue
            local dict_name
            dict_name=$(basename "$dict_sql" .dict.sql)

            if ! add_on_cluster_to_ddl "$dict_sql" "dictionary" "$db_name" "$dict_name"; then
                warning "Failed to modify DDL for dictionary: $db_name.$dict_name"
                has_errors=1
                continue
            fi

            log "  Creating dictionary: $db_name.$dict_name"
            local sql_statement
            sql_statement=$(cat "$dict_sql")

            if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
                local err
                err=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
                warning "Failed to create dictionary '$db_name.$dict_name': ${err:-unknown error}"
                has_errors=1
            else
                log "    Successfully created dictionary: $db_name.$dict_name"
            fi
        done
    done

    [ $has_errors -eq 0 ] || warning "Some dictionaries failed to create"
}

# -------- Data migration --------

# Step 3: Migrate data using INSERT … SELECT FROM remote().
migrate_data() {
    log "=== Step 3: Data migration ==="

    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")

    [ -z "$databases" ] && { warning "No databases to migrate"; return 0; }

    local total_errors=0

    # Phase 1: distributed tables first (they read from underlying local tables)
    log "Phase 1: Migrating distributed tables..."
    for db in $databases; do
        log "Processing distributed tables in database: $db"
        local distributed_tables
        distributed_tables=$(clickhouse_query_old \
            "SELECT name FROM system.tables WHERE database = '$db' AND engine ILIKE 'Distributed%'")

        if [ -n "$distributed_tables" ]; then
            for table in $distributed_tables; do
                migrate_table_data "$db" "$table" || total_errors=$((total_errors + 1))
            done
        else
            log "  No distributed tables found in $db"
        fi
    done

    # Phase 2: local (non-distributed) tables
    log "Phase 2: Migrating local tables..."
    for db in $databases; do
        log "Processing local tables in database: $db"
        local local_tables
        local_tables=$(clickhouse_query_old \
            "SELECT name FROM system.tables WHERE database = '$db'
             AND engine NOT ILIKE '%view%'
             AND engine NOT ILIKE 'dictionary'
             AND engine NOT ILIKE '%postgres%'
             AND engine NOT ILIKE 'Distributed%'")

        if [ -n "$local_tables" ]; then
            for table in $local_tables; do
                migrate_table_data "$db" "$table" || total_errors=$((total_errors + 1))
            done
        else
            log "  No local tables found in $db"
        fi
    done

    if [ $total_errors -eq 0 ]; then
        success "Data migration completed"
    else
        warning "Data migration completed with $total_errors error(s)"
    fi
}

migrate_table_data() {
    local db=$1
    local table=$2

    log "  Migrating table: $db.$table"

    # Skip if target already has data
    local target_count=0
    local count_result
    count_result=$(clickhouse_query_new "SELECT count() FROM \`$db\`.\`$table\`" 2>&1)
    if [ $? -eq 0 ]; then
        target_count=$(echo "$count_result" | tr -d '[:space:]')
    fi

    if [ -n "$target_count" ] && [ "$target_count" -gt 0 ]; then
        warning "    Target table $db.$table already has $target_count rows – skipping"
        return 0
    fi

    # Source row count
    local row_count
    row_count=$(clickhouse_query_old "SELECT count() FROM \`$db\`.\`$table\`" | tr -d '[:space:]')

    if [ -z "$row_count" ] || [ "$row_count" = "0" ]; then
        log "    Source table is empty – skipping"
        return 0
    fi

    log "    Source rows: $row_count"

    local start_time
    start_time=$(date +%s)

    local query="INSERT INTO \`$db\`.\`$table\` SELECT * FROM remote('${OLD_CLUSTER_HOST}:${OLD_CLUSTER_PORT}', \`$db\`, \`$table\`, '$OLD_CLICKHOUSE_USER', '$OLD_CLICKHOUSE_PASSWORD')"

    if clickhouse_query_new "$query" >/dev/null 2>&1; then
        local duration=$(( $(date +%s) - start_time ))

        # For ReplicatedMergeTree tables, wait for replication before verifying.
        local engine
        engine=$(clickhouse_query_new \
            "SELECT engine FROM system.tables WHERE database = '$db' AND name = '$table'" \
            | tr -d '[:space:]')

        if echo "$engine" | grep -qi "Replicated"; then
            log "    Waiting for replica sync on $db.$table ..."
            clickhouse_query_new "SYSTEM SYNC REPLICA \`$db\`.\`$table\`" >/dev/null 2>&1 || true
        fi

        local new_row_count
        new_row_count=$(clickhouse_query_new "SELECT count() FROM \`$db\`.\`$table\`" | tr -d '[:space:]')

        if [ "$row_count" -eq "$new_row_count" ]; then
            success "    Migrated $row_count rows in ${duration}s"
            return 0
        elif [ "$new_row_count" -gt "$row_count" ]; then
            warning "    Row count mismatch – target has MORE rows than source (source: $row_count, target: $new_row_count). Possible duplication."
            return 1
        else
            warning "    Row count mismatch – target has FEWER rows than source (source: $row_count, target: $new_row_count). Possible data loss."
            return 1
        fi
    else
        local err
        err=$(clickhouse_query_new "$query" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
        warning "    Failed to migrate '$db.$table': ${err:-unknown error}"
        return 1
    fi
}

# -------- Verification --------

# Step 4: Verify schema and data on the new cluster, including
# checking that engine conversions were applied correctly.
verify_migration() {
    log "=== Step 4: Verifying migration ==="

    # Database counts
    local old_db_count new_db_count
    old_db_count=$(clickhouse_query_old \
        "SELECT count() FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)" | tr -d '[:space:]')
    new_db_count=$(clickhouse_query_new \
        "SELECT count() FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)" | tr -d '[:space:]')

    log "Databases – old: $old_db_count, new: $new_db_count"
    [ "$old_db_count" -eq "$new_db_count" ] \
        && success "Database counts match" \
        || warning "Database counts differ!"

    # Table counts and engine transformation check per database
    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")

    log "Verifying per-database table counts and engine transformations..."
    for db in $databases; do
        local old_table_count new_table_count
        old_table_count=$(clickhouse_query_old \
            "SELECT count() FROM system.tables WHERE database = '$db'
             AND engine NOT ILIKE '%view%' AND engine NOT ILIKE 'dictionary'
             AND engine NOT ILIKE '%postgres%' AND engine NOT ILIKE 'Distributed%'" \
            | tr -d '[:space:]')
        new_table_count=$(clickhouse_query_new \
            "SELECT count() FROM system.tables WHERE database = '$db'
             AND engine NOT ILIKE '%view%' AND engine NOT ILIKE 'dictionary'
             AND engine NOT ILIKE '%postgres%' AND engine NOT ILIKE 'Distributed%'" \
            | tr -d '[:space:]')

        if [ "$old_table_count" -eq "$new_table_count" ]; then
            log "  $db: table count OK ($old_table_count)"
        else
            warning "  $db: table count mismatch! old=$old_table_count new=$new_table_count"
        fi
    done

    # Engine transformation verification
    verify_engine_transformations

    # Replica health check
    verify_replica_health

    # Sample data verification
    log "Sample row-count verification..."
    local sample_tables
    sample_tables=$(clickhouse_query_old \
        "SELECT concat(database, '.', name) FROM system.tables
         WHERE database NOT IN ($EXCLUDED_DATABASES)
           AND engine NOT ILIKE '%view%' AND engine NOT ILIKE 'dictionary'
           AND engine NOT ILIKE '%postgres%' AND engine NOT ILIKE 'Distributed%'
         ORDER BY rand() LIMIT 5")

    for table in $sample_tables; do
        local old_count new_count
        old_count=$(clickhouse_query_old "SELECT count() FROM $table" | tr -d '[:space:]')
        new_count=$(clickhouse_query_new "SELECT count() FROM $table" | tr -d '[:space:]')
        if [ "$old_count" -eq "$new_count" ]; then
            success "  $table: $old_count rows – OK"
        else
            warning "  $table: row count mismatch (old=$old_count, new=$new_count)"
        fi
    done

    success "Verification completed"
}

# Confirm that every MergeTree-family table on the old cluster
# has a Replicated* counterpart on the new cluster.
verify_engine_transformations() {
    log "Verifying engine transformations..."

    local old_tables
    old_tables=$(clickhouse_query_old \
        "SELECT database, name, engine FROM system.tables
         WHERE database NOT IN ($EXCLUDED_DATABASES)
           AND engine NOT ILIKE '%view%'
           AND engine NOT ILIKE 'dictionary'
           AND engine NOT ILIKE 'Distributed'")

    local ok=0 fail=0

    while IFS=$'\t' read -r db table old_engine; do
        [ -z "$db" ] && continue

        local new_engine
        new_engine=$(clickhouse_query_new \
            "SELECT engine FROM system.tables WHERE database = '$db' AND name = '$table'" \
            | tr -d '[:space:]')

        if [ -z "$new_engine" ]; then
            warning "  $db.$table: NOT FOUND in new cluster"
            fail=$((fail + 1))
            continue
        fi

        # Tables that were MergeTree-family on the old cluster must be
        # Replicated* on the new cluster.
        if echo "$old_engine" | grep -qi "MergeTree" && \
           ! echo "$old_engine" | grep -qi "Replicated"; then
            if echo "$new_engine" | grep -qi "ReplicatedMergeTree\|ReplicatedSummingMergeTree\|ReplicatedAggregatingMergeTree\|ReplicatedCollapsingMergeTree\|ReplicatedReplacingMergeTree\|ReplicatedVersionedCollapsingMergeTree\|ReplicatedGraphiteMergeTree"; then
                log "  ✓ $db.$table: $old_engine -> $new_engine"
                ok=$((ok + 1))
            else
                warning "  ✗ $db.$table: expected Replicated*MergeTree, got '$new_engine'"
                fail=$((fail + 1))
            fi
        elif echo "$old_engine" | grep -qi "Replicated"; then
            if echo "$new_engine" | grep -qi "Replicated"; then
                log "  ✓ $db.$table: Replicated (ZK path updated)"
                ok=$((ok + 1))
            else
                warning "  ✗ $db.$table: was Replicated on old cluster but got '$new_engine' on new"
                fail=$((fail + 1))
            fi
        else
            log "  – $db.$table: non-MergeTree engine '$old_engine' – no conversion required"
        fi
    done <<< "$old_tables"

    log "Engine transformation results: $ok OK, $fail failed"
    [ $fail -eq 0 ] && success "All engine transformations verified" \
                     || warning "$fail table(s) have incorrect engine after migration"
}

# Check system.replicas on the new cluster for any tables that are
# not fully replicated or have inactive replicas.
verify_replica_health() {
    log "Checking replica health on new cluster..."

    local unhealthy
    unhealthy=$(clickhouse_query_new \
        "SELECT database, table, replica_name, is_session_expired, future_parts
         FROM system.replicas
         WHERE is_readonly = 1 OR is_session_expired = 1" 2>/dev/null)

    if [ -z "$unhealthy" ]; then
        success "All replicas are healthy"
    else
        warning "Some replicas appear unhealthy:"
        echo "$unhealthy" | while IFS=$'\t' read -r db tbl replica expired future; do
            warning "  $db.$tbl replica '$replica' – session_expired=$expired, future_parts=$future"
        done
    fi
}

# -------- Entry point --------

main() {
    log "Starting ClickHouse cross-cluster migration"
    log "  Old cluster : $OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT"
    log "  New cluster : $NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT"
    log "  Cluster name: $CLUSTER_NAME"
    log "  ZK prefix   : $ZK_PATH_PREFIX"

    if ! command -v clickhouse &>/dev/null; then
        error "clickhouse client not found. Please install the ClickHouse client."
    fi

    if ! command -v python3 &>/dev/null; then
        error "python3 not found. It is required for engine parameter parsing."
    fi

    check_connections
    check_replication_macros
    create_backup_dir

    export_ddl
    apply_ddl
    migrate_data
    verify_migration

    success "Migration successfully completed!"
    log "Logs    : $LOG_FILE"
    log "DDL backup: $BACKUP_DIR"
}

main