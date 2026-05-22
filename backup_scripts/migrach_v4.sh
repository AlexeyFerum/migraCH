set -o pipefail

# =============================================================================
# ClickHouse Cross-Cluster Migration Script
# =============================================================================

OLD_CLUSTER_HOST="localhost"
OLD_CLUSTER_PORT="9000"

NEW_CLUSTER_HOST="shard1.new-cluster.internal"
NEW_CLUSTER_PORT="9000"

CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""

CLUSTER_NAME="epm_cluster"

BACKUP_DIR="/var/lib/clickhouse/migration_backup"
LOG_FILE="/var/log/clickhouse-migration.log"

EXCLUDED_DATABASES="'system', 'information_schema', 'INFORMATION_SCHEMA', 'default'"

SIZE_LIMIT_BYTES=$((100 * 1024 * 1024 * 1024))

DRY_RUN=false
TARGET_DB=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)
            TARGET_DB="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--db <database_name>] [--dry-run]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

RED='[0;31m'
GREEN='[0;32m'
YELLOW='[1;33m'
NC='[0m'

get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S.%3N'
}

log_file() {
    echo "$(get_timestamp) - $1" >> "$LOG_FILE"
}

log_step() {
    echo "$(get_timestamp) - $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}$(get_timestamp) - $1${NC}" | tee -a "$LOG_FILE"
}

warning() {
    echo -e "${YELLOW}$(get_timestamp) - WARNING: $1${NC}" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}$(get_timestamp) - ERROR: $1${NC}" | tee -a "$LOG_FILE"
    exit 1
}

ch_query() {
    local host="$1"
    local port="$2"
    local query="$3"

    local clean_query
    clean_query=$(printf '%s' "$query" \
        | tr -d '
' \
        | tr -s '[:space:]' ' ' \
        | sed 's/^ *//;s/ *$//')

    log_file "CH_EXEC [${host}:${port}] -> ${clean_query:0:150}$( [ ${#clean_query} -gt 150 ] && echo '...')"

    printf '%s
' "$clean_query" | clickhouse-client \
        --host="$host" \
        --port="$port" \
        --user="$CLICKHOUSE_USER" \
        --password="$CLICKHOUSE_PASSWORD" \
        --multiquery \
        2>>"$LOG_FILE"
}

ch_old() {
    ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "$1"
}

ch_new() {
    if $DRY_RUN; then
        log_file "[DRY-RUN] Пропускаем запрос на новом кластере"
        return 0
    fi

    if ! ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "$1"; then
        error "Критическая ошибка на новом кластере. Скрипт остановлен."
    fi
}

check_dependencies() {
    command -v clickhouse-client &>/dev/null \
        || error "clickhouse-client не найден"

    log_file "clickhouse-client: $(command -v clickhouse-client)"
}

check_connections() {
    log_step "Проверка подключений..."

    ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "SELECT 1" >/dev/null 2>&1 \
        || error "Нет связи со старым кластером"

    if ! $DRY_RUN; then
        ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT 1" >/dev/null 2>&1 \
            || error "Нет связи с новым кластером"
    fi

    success "Подключения успешны"
}

check_replication_macros() {
    $DRY_RUN && {
        log_step "[DRY-RUN] Пропускаем проверку макросов"
        return 0
    }

    log_step "Проверка макросов репликации..."

    local shard_macro replica_macro

    shard_macro=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
        "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')

    replica_macro=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
        "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')

    [ -z "$shard_macro" ] || [ "$shard_macro" = "shard" ] \
        && error "Макрос 'shard' не задан"

    [ -z "$replica_macro" ] || [ "$replica_macro" = "replica" ] \
        && error "Макрос 'replica' не задан"

    success "Макросы репликации проверены"
}

create_backup_dir() {
    mkdir -p \
        "$BACKUP_DIR/ddl/databases" \
        "$BACKUP_DIR/ddl/tables" \
        "$BACKUP_DIR/ddl/distributed" \
        "$BACKUP_DIR/ddl/dictionaries" \
        "$BACKUP_DIR/ddl/views" \
        "$BACKUP_DIR/ddl/matviews"
}

safe_modify() {
    local file="$1"
    local modifier_name="$2"
    local bak="${file}.safe_bak"

    [ -f "$file" ] || error "Файл не найден: $file"
    [ -s "$file" ] || error "Файл пустой перед $modifier_name: $file"

    cp -f "$file" "$bak"

    shift 2
    "$@"

    if [ ! -s "$file" ]; then
        mv -f "$bak" "$file"
        error "ФАТАЛЬНО: $file стал пустым после $modifier_name"
    fi

    rm -f "$bak"
}

export_ddl() {
    log_step "=== Шаг 1: Экспорт DDL ==="

    local databases

    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
        log_file "Выбрана БД: $TARGET_DB"
    else
        databases=$(ch_old \
            "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    fi

    for db in $databases; do
        log_step "Экспорт БД: $db"

        ch_old "SHOW CREATE DATABASE \`$db\`" \
            > "$BACKUP_DIR/ddl/databases/${db}.sql"

        local tables
        tables=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database='$db'
               AND engine NOT IN ('Distributed', 'Dictionary')
               AND engine NOT LIKE '%View%'")

        for table in $tables; do
            ch_old "SHOW CREATE TABLE \`$db\`.\`$table\`" \
                > "$BACKUP_DIR/ddl/tables/${db}___${table}.sql"
        done

        local dist_tables
        dist_tables=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database='$db'
               AND engine='Distributed'")

        for table in $dist_tables; do
            ch_old "SHOW CREATE TABLE \`$db\`.\`$table\`" \
                > "$BACKUP_DIR/ddl/distributed/${db}___${table}.sql"
        done

        local dicts
        dicts=$(ch_old \
            "SELECT name FROM system.dictionaries WHERE database='$db'")

        for dict in $dicts; do
            ch_old "SHOW CREATE DICTIONARY \`$db\`.\`$dict\`" \
                > "$BACKUP_DIR/ddl/dictionaries/${db}___${dict}.sql"
        done

        local views
        views=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database='$db'
               AND engine='View'")

        for view in $views; do
            ch_old "SHOW CREATE TABLE \`$db\`.\`$view\`" \
                > "$BACKUP_DIR/ddl/views/${db}___${view}.sql"
        done

        local matviews
        matviews=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database='$db'
               AND engine='MaterializedView'")

        for mv in $matviews; do
            ch_old "SHOW CREATE TABLE \`$db\`.\`$mv\`" \
                > "$BACKUP_DIR/ddl/matviews/${db}___${mv}.sql"
        done
    done

    success "Экспорт DDL завершён"
}

convert_engine() {
    local file="$1"

    if grep -qiP "ENGINE\s*=\s*ReplicatedMergeTree" "$file"; then
        log_file "ReplicatedMergeTree -> ReplicatedMergeTree"

        safe_modify "$file" "convert_engine" \
            perl -i -pe \
            's#(ENGINE\s*=\s*ReplicatedMergeTree\s*\(\s*)\x27[^\x27]*\x27\s*,\s*\x27[^\x27]*\x27#$1\x27/clickhouse/{database}/{table}/\x27, \x27{replica}\x27#i' \
            "$file"

        return 0
    fi

    if grep -qiP "ENGINE\s*=\s*MergeTree" "$file"; then
        log_file "MergeTree -> ReplicatedMergeTree"

        safe_modify "$file" "convert_engine" \
            perl -i -pe \
            's#ENGINE\s*=\s*MergeTree\s*\([^)]*\)#ENGINE = ReplicatedMergeTree(\x27/clickhouse/{database}/{table}/{shard}/\x27, \x27{replica}\x27)#i' \
            "$file"

        return 0
    fi
}

add_on_cluster() {
    local file="$1"

    [ -f "$file" ] || return 1

    grep -qi "ON CLUSTER" "$file" && return 0

    safe_modify "$file" "add_on_cluster" \
        perl -i -pe \
        "s|(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+\`[^\`]+\`)|\$1 ON CLUSTER ${CLUSTER_NAME}|i; last" \
        "$file"
}

apply_file() {
    local file="$1"
    local label="$2"

    log_file "APPLY: $label"

    ch_new "$(cat "$file")"

    log_step "  ✓ $label"
}

parse_filename() {
    local fname="$1"
    local -n _db="$2"
    local -n _name="$3"

    _db="${fname%%___*}"
    _name="${fname#*___}"
}

apply_ddl() {
    log_step "=== Шаг 2: Применение DDL ==="

    for f in "$BACKUP_DIR/ddl/databases/"*.sql; do
        [ -f "$f" ] || continue
        add_on_cluster "$f"
        apply_file "$f" "DATABASE"
    done

    for f in "$BACKUP_DIR/ddl/tables/"*.sql; do
        [ -f "$f" ] || continue
        convert_engine "$f"
        add_on_cluster "$f"
        apply_file "$f" "TABLE"
    done

    for f in "$BACKUP_DIR/ddl/distributed/"*.sql; do
        [ -f "$f" ] || continue
        add_on_cluster "$f"
        apply_file "$f" "DISTRIBUTED"
    done

    for f in "$BACKUP_DIR/ddl/dictionaries/"*.sql; do
        [ -f "$f" ] || continue
        add_on_cluster "$f"
        apply_file "$f" "DICTIONARY"
    done

    for f in "$BACKUP_DIR/ddl/views/"*.sql; do
        [ -f "$f" ] || continue
        add_on_cluster "$f"
        apply_file "$f" "VIEW"
    done

    success "DDL применён"
}

get_table_size() {
    local db="$1"
    local table="$2"
    local engine="$3"

    local size

    if [ "$engine" = "Distributed" ]; then
        size=$(ch_old \
            "SELECT sum(bytes_on_disk)
             FROM system.parts
             WHERE active
               AND database='$db'
               AND table='$table'" \
             2>/dev/null | tr -d '[:space:]')

        if [ -z "$size" ] || [ "$size" = "0" ]; then
            size=0
        fi
    else
        size=$(ch_old \
            "SELECT sum(bytes_on_disk)
             FROM system.parts
             WHERE active
               AND database='$db'
               AND table='$table'" \
            2>/dev/null | tr -d '[:space:]')
    fi

    echo "${size:-0}"
}

migrate_data() {
    $DRY_RUN && {
        log_step "[DRY-RUN] Пропускаем перенос данных"
        return 0
    }

    log_step "=== Шаг 3: Перенос данных ==="

    local databases

    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
    else
        databases=$(ch_old \
            "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    fi

    for db in $databases; do
        local tables

        tables=$(ch_old \
            "SELECT name, engine FROM system.tables
             WHERE database='$db'
               AND engine IN ('Distributed', 'ReplicatedMergeTree')")

        while IFS=$'	' read -r table engine; do
            [ -z "$table" ] && continue

            local size
            size=$(get_table_size "$db" "$table" "$engine")

            if (( size > SIZE_LIMIT_BYTES )); then
                warning "Пропущена большая таблица: $db.$table"
                LARGE_TABLES+=("$db.$table")
                continue
            fi

            log_step "Перенос: $db.$table [$engine]"

            ch_new "
                INSERT INTO \`$db\`.\`$table\`
                SELECT * FROM remote(
                    '$OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT',
                    \`$db\`,
                    \`$table\`,
                    '$CLICKHOUSE_USER',
                    '$CLICKHOUSE_PASSWORD'
                )
            "
        done <<< "$tables"
    done

    success "Перенос данных завершён"
}

apply_matviews() {
    log_step "=== Шаг 4: Materialized Views ==="

    for f in "$BACKUP_DIR/ddl/matviews/"*.sql; do
        [ -f "$f" ] || continue
        add_on_cluster "$f"
        apply_file "$f" "MATERIALIZED VIEW"
    done

    success "Materialized View созданы"
}

verify_migration() {
    $DRY_RUN && {
        log_step "[DRY-RUN] Пропускаем верификацию"
        return 0
    }

    log_step "=== Шаг 5: Верификация ==="

    local databases

    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
    else
        databases=$(ch_old \
            "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    fi

    local total_ok=0
    local total_fail=0

    for db in $databases; do
        local tables

        tables=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database='$db'
               AND engine IN ('Distributed', 'ReplicatedMergeTree')")

        while read -r table; do
            [ -z "$table" ] && continue

            local old_count
            local new_count

            old_count=$(ch_old \
                "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')

            new_count=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
                "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')

            old_count=${old_count:-0}
            new_count=${new_count:-0}

            if [ "$old_count" -eq "$new_count" ]; then
                success "✓ $db.$table : $old_count"
                total_ok=$(( total_ok + 1 ))
            else
                warning "$db.$table old=$old_count new=$new_count"
                total_fail=$(( total_fail + 1 ))
            fi
        done <<< "$tables"
    done

    log_step "Итого: OK=$total_ok FAIL=$total_fail"
}

LARGE_TABLES=()

main() {
    mkdir -p "$(dirname "$LOG_FILE")"
    : > "$LOG_FILE"

    log_step "=================================================="
    log_step " ClickHouse Migration"
    log_step " Mode: $([ "$DRY_RUN" = true ] && echo 'DRY-RUN' || echo 'LIVE')"
    log_step " Target DB: ${TARGET_DB:-ALL}"
    log_step "=================================================="

    check_dependencies
    check_connections
    check_replication_macros
    create_backup_dir

    export_ddl
    apply_ddl
    migrate_data
    apply_matviews
    verify_migration

    if [ ${#LARGE_TABLES[@]} -gt 0 ]; then
        warning "Таблицы для ручного переноса: ${LARGE_TABLES[*]}"
    fi

    success "Миграция завершена"
}

main
