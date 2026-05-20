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
#   bash ch_migration.sh                      # полная миграция
#   bash ch_migration.sh --dry-run            # без изменений на новом кластере
#   bash ch_migration.sh --db mydb            # только одна база
#   bash ch_migration.sh --db mydb --dry-run  # комбинация
# =============================================================================
set -o pipefail

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

# Credentials для старого кластера
OLD_CLICKHOUSE_USER="migration_user"
OLD_CLICKHOUSE_PASSWORD=""

# Credentials для нового кластера
NEW_CLICKHOUSE_USER="migration_user"
NEW_CLICKHOUSE_PASSWORD=""

# Имя кластера — должно совпадать на старом и новом (сервисы не меняются)
CLUSTER_NAME="epm_cluster"

# Директория для хранения экспортированного DDL
BACKUP_DIR="/var/lib/clickhouse/migration_backup"

# Лог-файл
LOG_FILE="/var/log/clickhouse-migration.log"

# Системные базы данных — не мигрируем
EXCLUDED_DATABASES="'system', 'information_schema', 'INFORMATION_SCHEMA', 'default'"

# Таблицы крупнее этого порога пропускаются с предупреждением.
SIZE_LIMIT_BYTES=$((100 * 1024 * 1024 * 1024))  # 100 GB

# ── Аргументы командной строки ────────────────────────────────────────────────

DRY_RUN=false
TARGET_DB=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true;        shift ;;
        --db)       TARGET_DB="$2";      shift 2 ;;
        --help|-h)
            echo "Использование: $0 [--db <database>] [--dry-run]"
            echo ""
            echo "  --db <database>  мигрировать только указанную базу данных"
            echo "  --dry-run        экспортировать и трансформировать DDL,"
            echo "                   но не применять изменения на новом кластере"
            exit 0
            ;;
        *)
            echo "Неизвестный аргумент: $1"
            echo "Используйте --help для справки"
            exit 1
            ;;
    esac
done

# ── Логирование ───────────────────────────────────────────────────────────────

mkdir -p "$(dirname "$LOG_FILE")"
: > "$LOG_FILE"   # очищаем лог при каждом запуске

# log_file  — только в файл (детали, отладка)
# log_step  — в файл + stdout (важные шаги)
# log_success / log_warning / log_error — в файл + stdout с цветом

log_file()    { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1" >> "$LOG_FILE"; }
log_step()    { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1" | tee -a "$LOG_FILE"; }
log_success() { echo -e "\033[0;32m$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1\033[0m" | tee -a "$LOG_FILE"; }
log_warning() { echo -e "\033[1;33m$(date '+%Y-%m-%d %H:%M:%S.%3N') - WARNING: $1\033[0m" | tee -a "$LOG_FILE"; }
log_error()   { echo -e "\033[0;31m$(date '+%Y-%m-%d %H:%M:%S.%3N') - ERROR: $1\033[0m" | tee -a "$LOG_FILE"; exit 1; }

# ── ClickHouse-запросы ────────────────────────────────────────────────────────

ch_query() {
    local host="$1"
    local port="$2"
    local user="$3"
    local password="$4"
    local query="$5"

    # Нормализуем запрос: убираем \r, схлопываем пробелы — для читаемого лога
    local clean_query
    clean_query=$(printf '%s' "$query" | tr -d '\r' | tr -s '[:space:]' ' ' | sed 's/^ *//;s/ *$//')

    log_file "CH_EXEC [${host}:${port}] -> ${clean_query:0:200}$([ ${#clean_query} -gt 200 ] && echo '...')"

    printf '%s\n' "$clean_query" | clickhouse-client \
        --host="$host" \
        --port="$port" \
        --user="$user" \
        --password="$password" \
        --multiquery \
        2>>"$LOG_FILE"
}

# Запрос на старом кластере (всегда реальный)
ch_old() {
    ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" \
             "$OLD_CLICKHOUSE_USER" "$OLD_CLICKHOUSE_PASSWORD" "$1"
}

# Запрос на новом кластере.
# В dry-run — только логируем.
# При ошибке на критических шагах — останавливаем скрипт (передать fatal=true).
ch_new() {
    local query="$1"
    local fatal="${2:-false}"   # true = останавливать скрипт при ошибке

    if $DRY_RUN; then
        log_file "[DRY-RUN] Пропускаем запрос на новом кластере"
        return 0
    fi

    if ! ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
                  "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD" "$query"; then
        if [ "$fatal" = "true" ]; then
            log_error "Критическая ошибка на новом кластере. Скрипт остановлен."
        fi
        return 1
    fi
}

# ── Предварительные проверки ──────────────────────────────────────────────────

check_dependencies() {
    command -v clickhouse-client &>/dev/null \
        || log_error "clickhouse-client не найден. Установите ClickHouse client."
    log_file "clickhouse-client: $(command -v clickhouse-client)"
}

check_connections() {
    log_step "Проверка подключений..."

    ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" \
             "$OLD_CLICKHOUSE_USER" "$OLD_CLICKHOUSE_PASSWORD" "SELECT 1" \
             >/dev/null 2>&1 \
        || log_error "Нет связи со старым кластером ($OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT)"
    log_step "  ✓ Старый кластер: $OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT"

    if $DRY_RUN; then
        log_step "  [DRY-RUN] Пропускаем проверку нового кластера"
    else
        ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
                 "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD" "SELECT 1" \
                 >/dev/null 2>&1 \
            || log_error "Нет связи с новым кластером ($NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT)"
        log_step "  ✓ Новый кластер: $NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT"
    fi

    log_success "Подключения успешны"
}

check_replication_macros() {
    $DRY_RUN && { log_step "[DRY-RUN] Пропускаем проверку макросов"; return 0; }

    log_step "Проверка макросов репликации на новом кластере..."

    local shard_macro replica_macro
    shard_macro=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
        "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD" \
        "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')
    replica_macro=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
        "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD" \
        "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')

    { [ -z "$shard_macro" ]   || [ "$shard_macro" = "shard" ]; }   \
        && log_error "Макрос 'shard' не задан на новом кластере"
    { [ -z "$replica_macro" ] || [ "$replica_macro" = "replica" ]; } \
        && log_error "Макрос 'replica' не задан на новом кластере"

    log_file "  Макрос 'shard'   = $shard_macro"
    log_file "  Макрос 'replica' = $replica_macro"
    log_success "Макросы репликации проверены"
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
    log_file "Директория бэкапа: $BACKUP_DIR"
}

# ── Шаг 1: Экспорт DDL ───────────────────────────────────────────────────────

export_ddl() {
    log_step "=== Шаг 1: Экспорт DDL ==="

    local databases
    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
        log_step "Целевая база: $TARGET_DB"
    else
        databases=$(ch_old \
            "SELECT name FROM system.databases
             WHERE name NOT IN ($EXCLUDED_DATABASES)")
        log_step "Экспорт всех пользовательских баз"
    fi

    if [ -z "$databases" ]; then
        log_warning "Баз данных для экспорта не найдено"
        return 0
    fi

    for db in $databases; do
        log_step "  Экспорт базы: \`$db\`"

        ch_old "SHOW CREATE DATABASE \`$db\`" \
            > "$BACKUP_DIR/ddl/databases/${db}.sql"

        # Локальные таблицы (не Distributed, не View, не Dictionary)
        ch_old "SELECT name FROM system.tables
                WHERE database = '$db'
                  AND engine NOT IN ('Distributed', 'Dictionary')
                  AND engine NOT LIKE '%View%'" \
        | while read -r tbl; do
            [ -z "$tbl" ] && continue
            log_file "    Таблица: \`$db\`.\`$tbl\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$tbl\`" \
                > "$BACKUP_DIR/ddl/tables/${db}___${tbl}.sql"
        done

        # Distributed таблицы
        ch_old "SELECT name FROM system.tables
                WHERE database = '$db'
                  AND engine = 'Distributed'" \
        | while read -r tbl; do
            [ -z "$tbl" ] && continue
            log_file "    Distributed: \`$db\`.\`$tbl\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$tbl\`" \
                > "$BACKUP_DIR/ddl/distributed/${db}___${tbl}.sql"
        done

        # Словари
        ch_old "SELECT name FROM system.dictionaries
                WHERE database = '$db'" \
        | while read -r dict; do
            [ -z "$dict" ] && continue
            log_file "    Словарь: \`$db\`.\`$dict\`"
            ch_old "SHOW CREATE DICTIONARY \`$db\`.\`$dict\`" \
                > "$BACKUP_DIR/ddl/dictionaries/${db}___${dict}.sql"
        done

        # Обычные View (не Materialized)
        ch_old "SELECT name FROM system.tables
                WHERE database = '$db'
                  AND engine = 'View'" \
        | while read -r view; do
            [ -z "$view" ] && continue
            log_file "    View: \`$db\`.\`$view\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$view\`" \
                > "$BACKUP_DIR/ddl/views/${db}___${view}.sql"
        done

        # Materialized View — применяем отдельно, после переноса данных
        ch_old "SELECT name FROM system.tables
                WHERE database = '$db'
                  AND engine = 'MaterializedView'" \
        | while read -r mv; do
            [ -z "$mv" ] && continue
            log_file "    MaterializedView: \`$db\`.\`$mv\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$mv\`" \
                > "$BACKUP_DIR/ddl/matviews/${db}___${mv}.sql"
        done
    done

    log_success "Экспорт DDL завершён"
}

# ── Безопасная модификация файлов ─────────────────────────────────────────────
#
# Перед модификацией создаём .bak.
# После — проверяем что файл не стал пустым.
# При опустошении — восстанавливаем из бэкапа и останавливаем скрипт.
# Используем perl -i (правка файла напрямую) — без промежуточных переменных,
# это исключает потерю содержимого через echo "$var" > file.

safe_modify() {
    local file="$1"
    local modifier_name="$2"
    shift 2
    # $@ — команда perl -i ... которую нужно выполнить

    [ -f "$file" ]  || log_error "safe_modify: файл не найден: $file"
    [ -s "$file" ]  || log_error "safe_modify: файл пустой перед $modifier_name: $file"

    local bak="${file}.bak"
    cp -f "$file" "$bak"

    "$@"

    if [ ! -s "$file" ]; then
        log_error "ФАТАЛЬНО: $file опустел после $modifier_name — восстановлен из бэкапа"
        mv -f "$bak" "$file"
    else
        rm -f "$bak"
    fi
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

    if grep -qiP "ENGINE\s*=\s*ReplicatedMergeTree" "$file"; then
        log_file "    ReplicatedMergeTree → '/clickhouse/{database}/{table}/'"
        safe_modify "$file" "convert_engine" \
            perl -i -pe \
            's#ENGINE\s*=\s*ReplicatedMergeTree\s*\(\s*'"'"'[^'"'"']*'"'"'\s*,\s*'"'"'[^'"'"']*'"'"'#ENGINE = ReplicatedMergeTree('"'"'/clickhouse/{database}/{table}/'"'"', '"'"'{replica}'"'"'#i' \
            "$file"

    elif grep -qiP "ENGINE\s*=\s*MergeTree" "$file"; then
        log_file "    MergeTree → '/clickhouse/{database}/{table}/{shard}/'"
        safe_modify "$file" "convert_engine" \
            perl -i -pe \
            's#ENGINE\s*=\s*MergeTree\s*\([^)]*\)#ENGINE = ReplicatedMergeTree('"'"'/clickhouse/{database}/{table}/{shard}/'"'"', '"'"'{replica}'"'"')#i' \
            "$file"

    else
        log_file "    Движок не требует конвертации"
    fi
}

# ── Добавление ON CLUSTER ─────────────────────────────────────────────────────

add_on_cluster() {
    local file="$1"
    [ -f "$file" ] || return 1

    grep -qi "ON CLUSTER" "$file" && return 0

    log_file "    Добавление ON CLUSTER $CLUSTER_NAME"

    # Паттерн матчит имя сущности в backtick-кавычках — надёжно для любых имён
    safe_modify "$file" "add_on_cluster" \
        perl -i -pe \
        "s|(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+\`[^\`]+\`)|\$1 ON CLUSTER ${CLUSTER_NAME}|i; last" \
        "$file"
}

# ── Применить DDL-файл на новом кластере ─────────────────────────────────────

apply_file() {
    local file="$1"
    local label="$2"
    local fatal="${3:-false}"

    if ch_new "$(cat "$file")" "$fatal"; then
        log_file "  ✓ $label"
    else
        log_warning "  ✗ $label — не создано (см. лог)"
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
    log_step "=== Шаг 2: Применение DDL на новом кластере ==="

    # 2.1 Базы данных — критично, останавливаемся при ошибке
    log_step "-- 2.1 Базы данных"
    for f in "$BACKUP_DIR/ddl/databases/"*.sql; do
        [ -f "$f" ] || continue
        local db
        db=$(basename "$f" .sql)
        [ -n "$TARGET_DB" ] && [ "$db" != "$TARGET_DB" ] && continue

        add_on_cluster "$f"
        log_file "  CREATE DATABASE \`$db\`"
        apply_file "$f" "DATABASE \`$db\`" true
    done

    # 2.2 Локальные таблицы (с конвертацией движков) — критично
    log_step "-- 2.2 Локальные таблицы"
    for f in "$BACKUP_DIR/ddl/tables/"*.sql; do
        [ -f "$f" ] || continue
        local fname db tname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db tname
        [ -n "$TARGET_DB" ] && [ "$db" != "$TARGET_DB" ] && continue

        log_file "  Подготовка: \`$db\`.\`$tname\`"
        convert_engine "$f"
        add_on_cluster "$f"
        apply_file "$f" "TABLE \`$db\`.\`$tname\`" true
    done

    # 2.3 Distributed таблицы — критично (локальные уже созданы)
    log_step "-- 2.3 Distributed таблицы"
    for f in "$BACKUP_DIR/ddl/distributed/"*.sql; do
        [ -f "$f" ] || continue
        local fname db tname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db tname
        [ -n "$TARGET_DB" ] && [ "$db" != "$TARGET_DB" ] && continue

        add_on_cluster "$f"
        apply_file "$f" "DISTRIBUTED \`$db\`.\`$tname\`" true
    done

    # 2.4 Словари — некритично (могут зависеть от внешних источников)
    log_step "-- 2.4 Словари"
    for f in "$BACKUP_DIR/ddl/dictionaries/"*.sql; do
        [ -f "$f" ] || continue
        local fname db dname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db dname
        [ -n "$TARGET_DB" ] && [ "$db" != "$TARGET_DB" ] && continue

        add_on_cluster "$f"
        apply_file "$f" "DICTIONARY \`$db\`.\`$dname\`" false
    done

    # 2.5 View — некритично
    log_step "-- 2.5 View"
    for f in "$BACKUP_DIR/ddl/views/"*.sql; do
        [ -f "$f" ] || continue
        local fname db vname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db vname
        [ -n "$TARGET_DB" ] && [ "$db" != "$TARGET_DB" ] && continue

        add_on_cluster "$f"
        apply_file "$f" "VIEW \`$db\`.\`$vname\`" false
    done

    log_success "Применение DDL завершено"
}

# ── Шаг 3: Перенос данных ─────────────────────────────────────────────────────

# Для Distributed: system.parts не содержит данных (они в локальных таблицах),
# поэтому пробуем clusterAllReplicas, при неудаче — локальный system.parts.
# Для ReplicatedMergeTree: данные лежат локально, читаем напрямую.
get_table_size() {
    local db="$1"
    local table="$2"
    local engine="$3"
    local size=0

    if [ "$engine" = "Distributed" ]; then
        size=$(ch_old \
            "SELECT sum(bytes_on_disk)
             FROM clusterAllReplicas('$CLUSTER_NAME', system.parts)
             WHERE active AND database = '$db'
               AND table = (
                   SELECT local_table_name FROM system.tables
                   WHERE database = '$db' AND name = '$table'
               )" 2>/dev/null | tr -d '[:space:]')
        # Fallback: если clusterAllReplicas не сработал
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
    $DRY_RUN && { log_step "[DRY-RUN] Пропускаем Шаг 3 (перенос данных)"; return 0; }
    log_step "=== Шаг 3: Перенос данных ==="

    local databases
    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
    else
        databases=$(ch_old \
            "SELECT name FROM system.databases
             WHERE name NOT IN ($EXCLUDED_DATABASES)")
    fi

    [ -z "$databases" ] && { log_warning "Нет баз для миграции данных"; return 0; }

    for db in $databases; do
        log_step "  База: \`$db\`"

        local tables
        tables=$(ch_old \
            "SELECT name, engine FROM system.tables
             WHERE database = '$db'
               AND engine IN ('Distributed', 'ReplicatedMergeTree')")

        while IFS=$'\t' read -r table engine; do
            [ -z "$table" ] && continue

            # Проверка размера
            local size
            size=$(get_table_size "$db" "$table" "$engine")
            if (( size > SIZE_LIMIT_BYTES )); then
                local size_gb=$(( size / 1024 / 1024 / 1024 ))
                log_warning "  ⏭ \`$db\`.\`$table\` [${size_gb} GB > 100 GB] — пропущено, требует ручного переноса"
                LARGE_TABLES+=("${db}.${table}")
                continue
            fi

            local count
            count=$(ch_old \
                "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')

            if [ -z "$count" ] || [ "$count" = "0" ]; then
                log_file "  \`$db\`.\`$table\` [$engine] — пусто, пропускаем"
                continue
            fi

            log_step "  \`$db\`.\`$table\` [$engine] — $count строк, переносим..."

            if ch_new "
                INSERT INTO \`$db\`.\`$table\`
                SELECT * FROM remote(
                    '$OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT',
                    \`$db\`, \`$table\`,
                    '$OLD_CLICKHOUSE_USER', '$OLD_CLICKHOUSE_PASSWORD'
                )
            "; then
                log_file "  ✓ \`$db\`.\`$table\` перенесена"
            else
                log_warning "  ✗ \`$db\`.\`$table\` — ошибка переноса (см. лог)"
            fi

        done <<< "$tables"
    done

    log_success "Перенос данных завершён"
}

# ── Шаг 4: Создание Materialized View ────────────────────────────────────────
# Создаём строго после переноса данных.
# Если создать раньше — INSERT в таблицы-источники триггернёт MV
# и данные окажутся продублированы в таблицах-приёмниках.

apply_matviews() {
    log_step "=== Шаг 4: Создание Materialized View ==="

    for f in "$BACKUP_DIR/ddl/matviews/"*.sql; do
        [ -f "$f" ] || continue
        local fname db mvname
        fname=$(basename "$f" .sql)
        parse_filename "$fname" db mvname
        [ -n "$TARGET_DB" ] && [ "$db" != "$TARGET_DB" ] && continue

        add_on_cluster "$f"
        apply_file "$f" "MATERIALIZED VIEW \`$db\`.\`$mvname\`" false
    done

    log_success "Materialized View созданы"
}

# ── Шаг 5: Верификация ────────────────────────────────────────────────────────

verify_migration() {
    $DRY_RUN && { log_step "[DRY-RUN] Пропускаем Шаг 5 (верификация)"; return 0; }
    log_step "=== Шаг 5: Верификация ==="

    local total_ok=0 total_fail=0

    local databases
    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
    else
        databases=$(ch_old \
            "SELECT name FROM system.databases
             WHERE name NOT IN ($EXCLUDED_DATABASES)")
    fi

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
                log_file "  ⏭ \`$db\`.\`$table\` — пропущено (>100 GB)"
                continue
            fi

            local old_count new_count
            old_count=$(ch_old \
                "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')
            old_count=${old_count:-0}

            new_count=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" \
                "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD" \
                "SELECT count() FROM \`$db\`.\`$table\`" \
                2>/dev/null | tr -d '[:space:]')
            new_count=${new_count:-0}

            if [ "$old_count" -eq "$new_count" ]; then
                log_success "  ✓ [$engine] \`$db\`.\`$table\`: $old_count строк"
                total_ok=$(( total_ok + 1 ))
            else
                log_warning "  ✗ [$engine] \`$db\`.\`$table\`: старый=$old_count новый=$new_count"
                total_fail=$(( total_fail + 1 ))
            fi
        done <<< "$tables"
    done

    log_step ""
    log_step "Итого: $total_ok таблиц OK, $total_fail с расхождениями"

    if [ "$total_fail" -eq 0 ]; then
        log_success "Верификация пройдена успешно"
    else
        log_warning "Верификация завершена с расхождениями — см. лог: $LOG_FILE"
    fi
}

# ── Точка входа ───────────────────────────────────────────────────────────────

LARGE_TABLES=()

main() {
    log_step "=================================================="
    log_step "  Миграция ClickHouse кластера"
    log_step "  Режим:        $([ "$DRY_RUN" = true ] && echo 'DRY-RUN' || echo 'LIVE')"
    log_step "  Целевая база: ${TARGET_DB:-все (кроме системных)}"
    log_step "=================================================="
    log_step "  Старый кластер: $OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT (user: $OLD_CLICKHOUSE_USER)"
    log_step "  Новый кластер:  $NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT (user: $NEW_CLICKHOUSE_USER)"
    log_step "  Имя кластера:   $CLUSTER_NAME"
    log_step "  Бэкап DDL:      $BACKUP_DIR"
    log_step "  Лог:            $LOG_FILE"
    log_step "  Лимит размера:  $(( SIZE_LIMIT_BYTES / 1024 / 1024 / 1024 )) GB"
    log_step "=================================================="

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
        log_warning ""
        log_warning "⚠️  Следующие таблицы пропущены (>$(( SIZE_LIMIT_BYTES / 1024 / 1024 / 1024 )) GB) и требуют ручного переноса:"
        for t in "${LARGE_TABLES[@]}"; do
            log_warning "    - $t"
        done
    fi

    log_success "=================================================="
    log_success "  Миграция завершена. Лог: $LOG_FILE"
    log_success "=================================================="
}

main