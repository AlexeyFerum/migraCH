#!/bin/bash

# =============================================================================
# ClickHouse Cross-Cluster Migration Script
#
# Переносит схему и данные с одного физического кластера ClickHouse на другой.
#
# Топология:
#   Старый кластер: N шардов без реплик
#   Новый кластер:  M шардов × K реплик
#
# Конвертация движков:
#   MergeTree(...)           → ReplicatedMergeTree('/clickhouse/{database}/{table}/{shard}/', '{replica}')
#   ReplicatedMergeTree(...) → ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}')
#   Все остальные движки     — без изменений
#
# Перенос данных:
#   Distributed         → INSERT INTO <new_dist>    SELECT * FROM remote(<old>, <old_dist>)
#   ReplicatedMergeTree → INSERT INTO <local_table> SELECT * FROM remote(<old>, <old_table>)
#   Всё остальное       — данные не переносятся
#
# Порядок выполнения:
#   1. Экспорт DDL со старого кластера
#   2. Применение DDL на новом:
#        базы → локальные таблицы → Distributed → словари → View
#   3. Перенос данных (Distributed, ReplicatedMergeTree)
#   4. Создание Materialized View (после данных — исключаем дубли)
#   5. Верификация
#
# Использование:
#   bash ch_migration.sh            # боевой запуск
#   bash ch_migration.sh --dry-run  # без изменений на новом кластере
# =============================================================================

# ── Конфигурация ──────────────────────────────────────────────────────────────

# Хост старого кластера.
# Скрипт запускается на одной из нод старого кластера.
# Для чтения данных используется этот же хост:
#   - Distributed:          сама агрегирует данные со всех шардов
#   - ReplicatedMergeTree:  данные одинаковы на всех нодах
OLD_CLUSTER_HOST="localhost"
OLD_CLUSTER_PORT="9000"

# Точка входа на новый кластер.
# DDL применяется через ON CLUSTER — достаточно одной ноды.
NEW_CLUSTER_HOST="shard1.new-cluster.internal"
NEW_CLUSTER_PORT="9000"

# Credentials (одинаковые для старого и нового кластера)
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""

# Имя кластера — должно совпадать на старом и новом (сервисы не меняются)
CLUSTER_NAME="epm_cluster"

# Директория для хранения экспортированного DDL
BACKUP_DIR="/var/lib/clickhouse/migration_backup"

# Лог-файл
LOG_FILE="/var/log/clickhouse-migration.log"

# Системные базы данных — не мигрируем
EXCLUDED_DATABASES="'system', 'information_schema', 'INFORMATION_SCHEMA', 'default'"

# Таблицы крупнее этого порога пропускаются с предупреждением.
# Для таких таблиц требуется ручной перенос.
SIZE_LIMIT_BYTES=$((100 * 1024 * 1024 * 1024))  # 100 GB

# ── Аргументы командной строки ────────────────────────────────────────────────

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

# ── Цвета вывода ──────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ── Логирование ───────────────────────────────────────────────────────────────

get_timestamp() { date '+%Y-%m-%d %H:%M:%S.%3N'; }

log()     { echo -e "$(get_timestamp) - $1"              | tee -a "$LOG_FILE"; }
success() { echo -e "${GREEN}$(get_timestamp) - $1${NC}" | tee -a "$LOG_FILE"; }
error()   { echo -e "${RED}$(get_timestamp) - ERROR: $1${NC}" | tee -a "$LOG_FILE"; exit 1; }
warning() { echo -e "${YELLOW}$(get_timestamp) - WARNING: $1${NC}" | tee -a "$LOG_FILE"; }

# ── ClickHouse-запросы ────────────────────────────────────────────────────────

ch_query() {
    local host="$1"
    local port="$2"
    local query="$3"

    clickhouse-client \
        --host="$host" \
        --port="$port" \
        --user="$CLICKHOUSE_USER" \
        --password="$CLICKHOUSE_PASSWORD" \
        --multiquery \
        --query="$query" \
        2>>"$LOG_FILE"
}

# Запрос на старом кластере (всегда реальный — читаем DDL и данные)
ch_old() { ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "$1"; }

# Запрос на новом кластере (в dry-run только логируем, не выполняем)
ch_new() {
    if $DRY_RUN; then
        log "[DRY-RUN] Пропускаем запрос на новом кластере:"
        echo "$1" | sed 's/^/  /' | tee -a "$LOG_FILE"
        return 0
    fi
    ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "$1"
}

# ── Предварительные проверки ──────────────────────────────────────────────────

check_dependencies() {
    command -v clickhouse-client &>/dev/null \
        || error "clickhouse-client не найден. Установите ClickHouse client."
    log "clickhouse-client: $(command -v clickhouse-client)"
}

check_connections() {
    log "Проверка подключений..."

    ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "SELECT 1" >/dev/null 2>&1 \
        || error "Нет связи со старым кластером ($OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT)"
    log "  ✓ Старый кластер: $OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT"

    if $DRY_RUN; then
        log "  [DRY-RUN] Пропускаем проверку нового кластера"
    else
        ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT 1" >/dev/null 2>&1 \
            || error "Нет связи с новым кластером ($NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT)"
        log "  ✓ Новый кластер: $NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT"
    fi

    success "Подключения успешны"
}

check_replication_macros() {
    $DRY_RUN && { log "[DRY-RUN] Пропускаем проверку макросов"; return 0; }

    log "Проверка макросов репликации на новом кластере..."

    local shard_macro replica_macro ok=1

    shard_macro=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
        "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')
    replica_macro=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
        "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')

    if [ -z "$shard_macro" ] || [ "$shard_macro" = "shard" ]; then
        warning "  Макрос 'shard' не задан на новом кластере"; ok=0
    else
        log "  Макрос 'shard'   = $shard_macro"
    fi

    if [ -z "$replica_macro" ] || [ "$replica_macro" = "replica" ]; then
        warning "  Макрос 'replica' не задан на новом кластере"; ok=0
    else
        log "  Макрос 'replica' = $replica_macro"
    fi

    [ "$ok" -eq 0 ] && error "Макросы 'shard' и 'replica' должны быть прописаны в config.xml на каждой ноде нового кластера."

    success "Макросы репликации проверены"
}

# ── Директория бэкапа ─────────────────────────────────────────────────────────

create_backup_dir() {
    mkdir -p \
        "$BACKUP_DIR/ddl/databases" \
        "$BACKUP_DIR/ddl/tables" \
        "$BACKUP_DIR/ddl/distributed" \
        "$BACKUP_DIR/ddl/dictionaries" \
        "$BACKUP_DIR/ddl/views" \
        "$BACKUP_DIR/ddl/matviews"
    log "Директория бэкапа: $BACKUP_DIR"
}

# ── Шаг 1: Экспорт DDL ───────────────────────────────────────────────────────

export_ddl() {
    log "=== Шаг 1: Экспорт DDL ==="

    local databases
    databases=$(ch_old \
        "SELECT name FROM system.databases
         WHERE name NOT IN ($EXCLUDED_DATABASES)")

    if [ -z "$databases" ]; then
        warning "Пользовательских баз данных не найдено"
        return 0
    fi

    for db in $databases; do
        log "Экспорт базы: \`$db\`"

        # DDL базы данных
        ch_old "SHOW CREATE DATABASE \`$db\`" \
            > "$BACKUP_DIR/ddl/databases/${db}.sql"

        # Локальные таблицы (не Distributed, не View, не Dictionary)
        local tables
        tables=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database = '$db'
               AND engine NOT IN ('Distributed', 'Dictionary')
               AND engine NOT LIKE '%View%'")

        for table in $tables; do
            log "  Таблица: \`$db\`.\`$table\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$table\`" \
                > "$BACKUP_DIR/ddl/tables/${db}___${table}.sql"
        done

        # Distributed таблицы
        local dist_tables
        dist_tables=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database = '$db'
               AND engine = 'Distributed'")

        for table in $dist_tables; do
            log "  Distributed: \`$db\`.\`$table\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$table\`" \
                > "$BACKUP_DIR/ddl/distributed/${db}___${table}.sql"
        done

        # Словари
        local dicts
        dicts=$(ch_old \
            "SELECT name FROM system.dictionaries WHERE database = '$db'")

        for dict in $dicts; do
            log "  Словарь: \`$db\`.\`$dict\`"
            ch_old "SHOW CREATE DICTIONARY \`$db\`.\`$dict\`" \
                > "$BACKUP_DIR/ddl/dictionaries/${db}___${dict}.sql"
        done

        # Обычные View (не Materialized)
        local views
        views=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database = '$db'
               AND engine = 'View'")

        for view in $views; do
            log "  View: \`$db\`.\`$view\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$view\`" \
                > "$BACKUP_DIR/ddl/views/${db}___${view}.sql"
        done

        # Materialized View — применяем отдельно, после переноса данных
        local matviews
        matviews=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database = '$db'
               AND engine = 'MaterializedView'")

        for mv in $matviews; do
            log "  MaterializedView: \`$db\`.\`$mv\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$mv\`" \
                > "$BACKUP_DIR/ddl/matviews/${db}___${mv}.sql"
        done
    done

    success "Экспорт DDL завершён"
}

# ── Конвертация движков ───────────────────────────────────────────────────────
#
# Правила:
#   MergeTree(...)           → ReplicatedMergeTree('/clickhouse/{database}/{table}/{shard}/', '{replica}')
#   ReplicatedMergeTree(...) → ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}')
#   Всё остальное            — без изменений
#
# {database}, {table}, {shard}, {replica} — макросы ClickHouse,
# передаются как литеральные строки, bash их не подставляет.

convert_engine() {
    local file="$1"
    [ -f "$file" ] || return 1

    local content
    content=$(cat "$file")

    # Случай 1: ReplicatedMergeTree — обновляем ZK-путь, убираем {shard}
    if echo "$content" | grep -qiP "ENGINE\s*=\s*ReplicatedMergeTree"; then
        log "    ReplicatedMergeTree → '/clickhouse/{database}/{table}/'"
        content=$(echo "$content" | perl -pe \
            "s|ENGINE\s*=\s*ReplicatedMergeTree\s*\(\s*'[^']*'\s*,\s*'[^']*'|ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}')|i")
        echo "$content" > "$file"
        return 0
    fi

    # Случай 2: MergeTree — конвертируем, добавляем {shard} в путь
    if echo "$content" | grep -qiP "ENGINE\s*=\s*MergeTree"; then
        log "    MergeTree → '/clickhouse/{database}/{table}/{shard}/'"
        content=$(echo "$content" | perl -pe \
            "s|ENGINE\s*=\s*MergeTree\s*\([^)]*\)|ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{table}/{shard}/', '{replica}')|i")
        echo "$content" > "$file"
        return 0
    fi

    log "    Движок не требует конвертации"
}

# ── Добавление ON CLUSTER ─────────────────────────────────────────────────────

add_on_cluster() {
    local file="$1"
    [ -f "$file" ] || return 1

    local content
    content=$(cat "$file")

    # Если ON CLUSTER уже есть — ничего не делаем
    echo "$content" | grep -qi "ON CLUSTER" && return 0

    # Вставляем ON CLUSTER после имени сущности в backtick-кавычках.
    # Обрабатываем только первую строку CREATE.
    content=$(echo "$content" | perl -pe \
        "s|(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+\`[^\`]+\`)|\$1 ON CLUSTER ${CLUSTER_NAME}|i; last")

    echo "$content" > "$file"
}

# ── Применить DDL-файл на новом кластере ─────────────────────────────────────

apply_file() {
    local file="$1"
    local label="$2"

    if ch_new "$(cat "$file")" 2>>"$LOG_FILE"; then
        log "  ✓ $label"
    else
        warning "  ✗ $label — не создано (см. лог)"
    fi
}

# Извлечь имя базы и сущности из имени файла вида db___name
parse_filename() {
    local fname="$1"
    local -n _db="$2"
    local -n _name="$3"
    _db="${fname%%___*}"
    _name="${fname#*___}"
}

# ── Шаг 2: Применение DDL на новом кластере ──────────────────────────────────

apply_ddl() {
    log "=== Шаг 2: Применение DDL на новом кластере ==="

    # 2.1 Базы данных
    log "-- 2.1 Базы данных"
    for f in "$BACKUP_DIR/ddl/databases/"*.sql; do
        [ -f "$f" ] || continue
        local db
        db=$(basename "$f" .sql)
        add_on_cluster "$f"
        apply_file "$f" "DATABASE \`$db\`"
    done

    # 2.2 Локальные таблицы (с конвертацией движков)
    log "-- 2.2 Локальные таблицы"
    for f in "$BACKUP_DIR/ddl/tables/"*.sql; do
        [ -f "$f" ] || continue
        local fname db tname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db tname
        log "  Подготовка: \`$db\`.\`$tname\`"
        convert_engine "$f"
        add_on_cluster "$f"
        apply_file "$f" "TABLE \`$db\`.\`$tname\`"
    done

    # 2.3 Distributed таблицы (локальные уже созданы)
    log "-- 2.3 Distributed таблицы"
    for f in "$BACKUP_DIR/ddl/distributed/"*.sql; do
        [ -f "$f" ] || continue
        local fname db tname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db tname
        add_on_cluster "$f"
        apply_file "$f" "DISTRIBUTED \`$db\`.\`$tname\`"
    done

    # 2.4 Словари
    log "-- 2.4 Словари"
    for f in "$BACKUP_DIR/ddl/dictionaries/"*.sql; do
        [ -f "$f" ] || continue
        local fname db dname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db dname
        add_on_cluster "$f"
        apply_file "$f" "DICTIONARY \`$db\`.\`$dname\`"
    done

    # 2.5 View
    log "-- 2.5 View"
    for f in "$BACKUP_DIR/ddl/views/"*.sql; do
        [ -f "$f" ] || continue
        local fname db vname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db vname
        add_on_cluster "$f"
        apply_file "$f" "VIEW \`$db\`.\`$vname\`"
    done

    success "Применение DDL завершено"
}

# ── Шаг 3: Перенос данных ─────────────────────────────────────────────────────

# Проверить размер таблицы на старом кластере в байтах.
# Для Distributed — сумма по всем шардам через system.parts не работает напрямую,
# поэтому размер проверяем у underlying локальной таблицы через сам Distributed-движок.
get_table_size() {
    local db="$1"
    local table="$2"
    local engine="$3"

    local size
    if [ "$engine" = "Distributed" ]; then
        # Для Distributed берём размер через удалённый вызов на всех шардах кластера
        size=$(ch_old \
            "SELECT sum(bytes_on_disk)
             FROM clusterAllReplicas('$CLUSTER_NAME', system.parts)
             WHERE active
               AND database = '$db'
               AND table = (
                   SELECT storage_policy
                   FROM system.tables
                   WHERE database = '$db' AND name = '$table'
               )" 2>/dev/null | tr -d '[:space:]')
        # Fallback: если clusterAllReplicas недоступен — читаем локальные parts
        if [ -z "$size" ] || [ "$size" = "0" ]; then
            size=$(ch_old \
                "SELECT sum(bytes_on_disk) FROM system.parts
                 WHERE active AND database = '$db' AND table = '$table'" \
                2>/dev/null | tr -d '[:space:]')
        fi
    else
        size=$(ch_old \
            "SELECT sum(bytes_on_disk) FROM system.parts
             WHERE active AND database = '$db' AND table = '$table'" \
            2>/dev/null | tr -d '[:space:]')
    fi

    echo "${size:-0}"
}

migrate_data() {
    $DRY_RUN && { log "[DRY-RUN] Пропускаем Шаг 3 (перенос данных)"; return 0; }
    log "=== Шаг 3: Перенос данных ==="

    local databases
    databases=$(ch_old \
        "SELECT name FROM system.databases
         WHERE name NOT IN ($EXCLUDED_DATABASES)")

    [ -z "$databases" ] && { warning "Нет баз для миграции данных"; return 0; }

    for db in $databases; do
        log "База: \`$db\`"

        local tables
        tables=$(ch_old \
            "SELECT name, engine FROM system.tables
             WHERE database = '$db'
               AND engine IN ('Distributed', 'ReplicatedMergeTree')")

        while IFS=$'\t' read -r table engine; do
            [ -z "$table" ] && continue

            # Проверка размера — пропускаем слишком большие таблицы
            local size
            size=$(get_table_size "$db" "$table" "$engine")
            if (( size > SIZE_LIMIT_BYTES )); then
                local size_gb=$(( size / 1024 / 1024 / 1024 ))
                warning "  ⏭ \`$db\`.\`$table\` [${size_gb}GB > 100GB] — пропущено, требует ручного переноса"
                LARGE_TABLES+=("${db}.${table}")
                continue
            fi

            local count
            count=$(ch_old "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')

            if [ -z "$count" ] || [ "$count" = "0" ]; then
                log "  \`$db\`.\`$table\` [$engine] — пусто, пропускаем"
                continue
            fi

            log "  \`$db\`.\`$table\` [$engine] — $count строк, переносим..."

            # ── Distributed → Distributed ─────────────────────────────────────
            # Distributed сама агрегирует данные со всех шардов старого кластера.
            # Вставляем в одноимённую Distributed на новом.
            #
            # ── ReplicatedMergeTree → ReplicatedMergeTree ─────────────────────
            # Данные одинаковы на всех нодах — читаем с текущей.
            # Вставляем в локальную таблицу напрямую, репликация разойдётся сама.
            if ch_new "
                INSERT INTO \`$db\`.\`$table\`
                SELECT * FROM remote('$OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT',
                    \`$db\`, \`$table\`,
                    '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')
            " 2>>"$LOG_FILE"; then
                log "  ✓ \`$db\`.\`$table\` перенесена"
            else
                warning "  ✗ \`$db\`.\`$table\` — ошибка переноса (см. лог)"
            fi

        done <<< "$tables"
    done

    success "Перенос данных завершён"
}

# ── Шаг 4: Создание Materialized View ────────────────────────────────────────
# Создаём строго после переноса данных.
# Если создать раньше — INSERT в таблицы-источники триггернёт MV
# и данные окажутся продублированы в таблицах-приёмниках.

apply_matviews() {
    log "=== Шаг 4: Создание Materialized View ==="

    for f in "$BACKUP_DIR/ddl/matviews/"*.sql; do
        [ -f "$f" ] || continue
        local fname db mvname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db mvname
        add_on_cluster "$f"
        apply_file "$f" "MATERIALIZED VIEW \`$db\`.\`$mvname\`"
    done

    success "Materialized View созданы"
}

# ── Шаг 5: Верификация ────────────────────────────────────────────────────────

verify_migration() {
    $DRY_RUN && { log "[DRY-RUN] Пропускаем Шаг 5 (верификация)"; return 0; }
    log "=== Шаг 5: Верификация ==="

    local total_ok=0 total_fail=0

    local databases
    databases=$(ch_old \
        "SELECT name FROM system.databases
         WHERE name NOT IN ($EXCLUDED_DATABASES)")

    for db in $databases; do
        local tables
        tables=$(ch_old \
            "SELECT name, engine FROM system.tables
             WHERE database = '$db'
               AND engine IN ('Distributed', 'ReplicatedMergeTree')")

        while IFS=$'\t' read -r table engine; do
            [ -z "$table" ] && continue

            # Пропускаем таблицы, которые не переносились из-за размера
            local size
            size=$(get_table_size "$db" "$table" "$engine")
            if (( size > SIZE_LIMIT_BYTES )); then
                log "  ⏭ \`$db\`.\`$table\` — пропущено (>100GB)"
                continue
            fi

            local old_count new_count

            old_count=$(ch_old \
                "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')
            old_count=${old_count:-0}

            new_count=$(ch_new \
                "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')
            new_count=${new_count:-0}

            if [ "$old_count" -eq "$new_count" ]; then
                success "  ✓ [$engine] \`$db\`.\`$table\`: $old_count строк"
                total_ok=$(( total_ok + 1 ))
            else
                warning "  ✗ [$engine] \`$db\`.\`$table\`: старый=$old_count новый=$new_count"
                total_fail=$(( total_fail + 1 ))
            fi
        done <<< "$tables"
    done

    log ""
    log "Итого: $total_ok таблиц OK, $total_fail с расхождениями"

    if [ "$total_fail" -eq 0 ]; then
        success "Верификация пройдена успешно"
    else
        warning "Верификация завершена с расхождениями — см. лог: $LOG_FILE"
    fi
}

# ── Точка входа ───────────────────────────────────────────────────────────────

LARGE_TABLES=()

main() {
    mkdir -p "$(dirname "$LOG_FILE")"

    log "=================================================="
    log "  Миграция ClickHouse кластера"
    log "  Режим: $([ "$DRY_RUN" = true ] && echo 'DRY-RUN' || echo 'LIVE')"
    log "=================================================="
    log "  Старый кластер: $OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT"
    log "  Новый кластер:  $NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT"
    log "  Имя кластера:   $CLUSTER_NAME"
    log "  Бэкап DDL:      $BACKUP_DIR"
    log "  Лог:            $LOG_FILE"
    log "  Лимит размера:  $(( SIZE_LIMIT_BYTES / 1024 / 1024 / 1024 )) GB"
    log "=================================================="

    check_dependencies
    check_connections
    check_replication_macros
    create_backup_dir

    export_ddl        # Шаг 1: экспорт DDL со старого кластера
    apply_ddl         # Шаг 2: базы → локальные таблицы → Distributed → словари → View
    migrate_data      # Шаг 3: перенос данных (Distributed, ReplicatedMergeTree)
    apply_matviews    # Шаг 4: Materialized View — после данных, чтобы не было дублей
    verify_migration  # Шаг 5: верификация count() по всем перенесённым таблицам

    if [ ${#LARGE_TABLES[@]} -gt 0 ]; then
        warning ""
        warning "⚠️  Следующие таблицы пропущены (>$(( SIZE_LIMIT_BYTES / 1024 / 1024 / 1024 ))GB) и требуют ручного переноса:"
        for t in "${LARGE_TABLES[@]}"; do
            warning "    - $t"
        done
    fi

    success "=================================================="
    success "  Миграция завершена. Лог: $LOG_FILE"
    success "=================================================="
}

main