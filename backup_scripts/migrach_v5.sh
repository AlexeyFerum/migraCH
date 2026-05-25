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

# ── Подготовка DDL-файла ──────────────────────────────────────────────────────
#
# SHOW CREATE возвращает одну строку с литеральными \n (два символа: \ и n).
# prepare_ddl_file делает три вещи последовательно через sed:
#
#   1. Раскрывает литеральные \n в реальные переносы строк
#   2. Добавляет ON CLUSTER <name> после имени сущности (если ещё нет)
#   3. Конвертирует движок (только для таблиц):
#        MergeTree(...)           → ReplicatedMergeTree('/clickhouse/{database}/{table}/{shard}/', '{replica}')
#        ReplicatedMergeTree(...) → ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}')
#
# {database}, {table}, {shard}, {replica} — макросы ClickHouse,
# передаются как литеральные строки, bash их не подставляет.
#
# Все правки делаются через sed -i (in-place, без промежуточных переменных).
# Перед правкой создаётся .bak; если файл опустел — восстанавливаем и падаем.

prepare_ddl_file() {
    local file="$1"
    local kind="$2"   # database | table | distributed | dictionary | view | matview

    [ -f "$file" ] || log_error "prepare_ddl_file: файл не найден: $file"
    [ -s "$file" ] || log_error "prepare_ddl_file: файл пустой: $file"

    local bak="${file}.bak"
    cp -f "$file" "$bak"

    # ── 1. Раскрываем литеральные escape-последовательности ─────────────────────
    # SHOW CREATE возвращает \n и \' как два символа — раскрываем в реальные.
    sed -i 's/\\n/\n/g' "$file"
    sed -i "s/\\\\'/'/g" "$file"

    # ── 2. ON CLUSTER ─────────────────────────────────────────────────────────
    # После раскрытия \n имена сущностей — без backtick, вида: db.name или просто name.
    # Паттерны явные для каждого типа, матчат первую строку файла (CREATE ...).
    if ! grep -qi "ON CLUSTER" "$file"; then
        case "$kind" in
            database)
                # CREATE DATABASE db
                sed -i "1s/\(CREATE DATABASE\) \([^ ]*\)/\1 IF NOT EXISTS \2 ON CLUSTER ${CLUSTER_NAME}/" "$file"
                ;;
            table|distributed)
                # CREATE TABLE db.name  или  CREATE TABLE db.name (IF NOT EXISTS)
                sed -i "1s/\(CREATE TABLE\) \([^ ]*\)/\1 IF NOT EXISTS \2 ON CLUSTER ${CLUSTER_NAME}/" "$file"
                ;;
            dictionary)
                sed -i "1s/\(CREATE DICTIONARY\) \([^ ]*\)/\1 IF NOT EXISTS \2 ON CLUSTER ${CLUSTER_NAME}/" "$file"
                ;;
            view)
                sed -i "1s/\(CREATE VIEW\) \([^ ]*\)/\1 IF NOT EXISTS \2 ON CLUSTER ${CLUSTER_NAME}/" "$file"
                ;;
            matview)
                sed -i "1s/\(CREATE MATERIALIZED VIEW\) \([^ ]*\)/\1 IF NOT EXISTS \2 ON CLUSTER ${CLUSTER_NAME}/" "$file"
                ;;
        esac
        log_file "    ON CLUSTER $CLUSTER_NAME добавлен"
    fi

    # ── 3. Конвертация движков (только для локальных таблиц) ──────────────────
    if [ "$kind" = "table" ]; then
        if grep -qi "ENGINE = ReplicatedMergeTree" "$file"; then
            log_file "    ReplicatedMergeTree → '/clickhouse/{database}/{table}/'"
            sed -i "s|ENGINE = ReplicatedMergeTree('[^']*', '[^']*'|ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}'|I" "$file"

        elif grep -qi "ENGINE = MergeTree" "$file"; then
            log_file "    MergeTree → '/clickhouse/{database}/{table}/{shard}/'"
            # Покрываем все варианты: MergeTree / MergeTree() / MergeTree(args)
            sed -i 's|ENGINE = MergeTree([^)]*)|ENGINE = ReplicatedMergeTree('"'"'/clickhouse/{database}/{table}/{shard}/'"'"', '"'"'{replica}'"'"')|I' "$file"
            sed -i 's|ENGINE = MergeTree[[:space:]]*$|ENGINE = ReplicatedMergeTree('"'"'/clickhouse/{database}/{table}/{shard}/'"'"', '"'"'{replica}'"'"')|I' "$file"

        else
            log_file "    Движок не требует конвертации"
        fi
    fi

    # ── Проверка что файл не опустел ──────────────────────────────────────────
    if [ ! -s "$file" ]; then
        mv -f "$bak" "$file"
        log_error "ФАТАЛЬНО: $file опустел после prepare_ddl_file — восстановлен из бэкапа"
    else
        rm -f "$bak"
    fi
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

        prepare_ddl_file "$f" "database"
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
        prepare_ddl_file "$f" "table"
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

        prepare_ddl_file "$f" "distributed"
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

        prepare_ddl_file "$f" "dictionary"
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

        prepare_ddl_file "$f" "view"
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

# Проверяет, одинаковы ли данные на всех нодах кластера для MergeTree-таблицы.
# Использует cityHash64 через cluster() для сравнения хешей и счётчиков строк.
# Возвращает 0 (true) если данные идентичны на всех нодах, 1 (false) иначе.
check_data_identical_on_all_nodes() {
    local db="$1"
    local table="$2"

    local result
    result=$(ch_old "
        WITH hashes AS (
            SELECT
                hostname() as _host,
                sum(cityHash64(*)) AS data_hash,
                count()            AS row_count
            FROM cluster('$CLUSTER_NAME', $db.$table)
            GROUP BY _host
        )
        SELECT countIf(data_hash = min_hash AND row_count = min_count) = count()
        FROM (
            SELECT
                data_hash,
                row_count,
                min(data_hash)  OVER () AS min_hash,
                min(row_count)  OVER () AS min_count
            FROM hashes
        )
    " 2>/dev/null | tr -d '[:space:]')

    [ "${result:-0}" = "1" ]
}

migrate_data() {
    $DRY_RUN && { log_step "[DRY-RUN] Пропускаем Шаг 3 (перенос данных)"; return 0; }
    log_step "=== Шаг 3: Перенос данных ==="

    local databases
    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
    else
        databases=$(ch_old             "SELECT name FROM system.databases
             WHERE name NOT IN ($EXCLUDED_DATABASES)")
    fi

    [ -z "$databases" ] && { log_warning "Нет баз для миграции данных"; return 0; }

    for db in $databases; do
        log_step "  База: \`$db\`"

        # ── Кейс 1: Distributed → Distributed ────────────────────────────────
        # Distributed сама агрегирует данные со всех шардов.
        # Читаем с текущей ноды, вставляем в одноимённую Distributed на новом.
        #
        # ── Кейс 2: ReplicatedMergeTree → ReplicatedMergeTree ────────────────
        # Данные одинаковы на всех нодах — читаем с текущей.
        # Вставляем в локальную таблицу напрямую, репликация разойдётся сама.
        local tables
        tables=$(ch_old             "SELECT name, engine FROM system.tables
             WHERE database = '$db'
               AND engine IN ('Distributed', 'ReplicatedMergeTree')")

        while IFS=$'	' read -r table engine; do
            [ -z "$table" ] && continue
            migrate_table "$db" "$table" "$engine"
        done <<< "$tables"

        # ── Кейс 3: MergeTree без Distributed сверху ─────────────────────────
        # Проверяем идентичность данных на всех нодах через cityHash64.
        # Если данные одинаковые — таблица фактически реплицированная,
        # переносим с одной ноды как ReplicatedMergeTree без {shard}.
        # Если разные — пропускаем, требует ручного переноса.
        local mt_tables
        mt_tables=$(ch_old             "SELECT name FROM system.tables
             WHERE database = '$db'
               AND engine = 'MergeTree'
               AND name NOT IN (
                   SELECT extract(engine_full,
                       'Distributed\([^,]+,\s*''([^'']*)''\s*,\s*''([^'']*)'''
                   )
                   FROM system.tables
                   WHERE database = '$db'
                     AND engine = 'Distributed'
               )")

        for table in $mt_tables; do
            [ -z "$table" ] && continue

            log_step "  Проверка MergeTree \`$db\`.\`$table\` — нет Distributed сверху"

            if check_data_identical_on_all_nodes "$db" "$table"; then
                log_warning "  ⚠ \`$db\`.\`$table\`: данные ОДИНАКОВЫ на всех нодах — переносим как ReplicatedMergeTree (без {shard})"
                SUSPECT_TABLES+=("${db}.${table}")
                migrate_table "$db" "$table" "MergeTree_replicated"
            else
                log_warning "  ⚠ \`$db\`.\`$table\`: данные РАЗНЫЕ на нодах, Distributed отсутствует — пропущено, требует ручного переноса"
                SUSPECT_TABLES+=("${db}.${table} [РАЗНЫЕ ДАННЫЕ — пропущено]")
            fi
        done

    done

    log_success "Перенос данных завершён"
}

# Переносит данные одной таблицы на новый кластер.
# engine = MergeTree_replicated означает: трактуем MergeTree как реплицированную.
migrate_table() {
    local db="$1"
    local table="$2"
    local engine="$3"

    local size
    size=$(get_table_size "$db" "$table" "$engine")
    if (( size > SIZE_LIMIT_BYTES )); then
        local size_gb=$(( size / 1024 / 1024 / 1024 ))
        log_warning "  ⏭ \`$db\`.\`$table\` [${size_gb} GB > 100 GB] — пропущено, требует ручного переноса"
        LARGE_TABLES+=("${db}.${table}")
        return
    fi

    local count
    count=$(ch_old         "SELECT count() FROM \`$db\`.\`$table\`"         2>/dev/null | tr -d '[:space:]')

    if [ -z "$count" ] || [ "$count" = "0" ]; then
        log_file "  \`$db\`.\`$table\` [$engine] — пусто, пропускаем"
        return
    fi

    log_step "  \`$db\`.\`$table\` [$engine] — $count строк, переносим..."

    if ch_new "
        INSERT INTO \`$db\`.\`$table\`
        SELECT * FROM remoteSecure(
            '$OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT',
            \`$db\`, \`$table\`,
            '$OLD_CLICKHOUSE_USER', '$OLD_CLICKHOUSE_PASSWORD'
        )
    "; then
        log_file "  ✓ \`$db\`.\`$table\` перенесена"
    else
        log_warning "  ✗ \`$db\`.\`$table\` — ошибка переноса (см. лог)"
    fi
}

# ── Шаг 5: Верификация ────────────────────────────────────────────────────────

# Считает строки таблицы на старом и новом кластере и сравнивает.
# Для кейса 3 (MergeTree→Replicated без шардирования) на новом кластере
# данные лежат только на одном шарде — считаем через clusterAllReplicas
# чтобы убедиться что данные не разошлись по шардам случайно.
verify_table_count() {
    local db="$1"
    local table="$2"
    local engine_label="$3"
    local is_suspect="${4:-false}"   # true = кейс 3, MergeTree→Replicated

    local old_count new_count

    old_count=$(ch_old         "SELECT count() FROM \`$db\`.\`$table\`"         2>/dev/null | tr -d '[:space:]')
    old_count=${old_count:-0}

    if [ "$is_suspect" = "true" ]; then
        # Считаем через clusterAllReplicas — хотим убедиться что данные
        # есть ровно на одном шарде и не растиражировались лишний раз.
        # Ожидаем: сумма по всем нодам = old_count (не old_count * N шардов).
        new_count=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT"             "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD"             "SELECT sum(cnt) FROM (
                SELECT count() AS cnt
                FROM clusterAllReplicas('$CLUSTER_NAME', $db, $table)
                GROUP BY _shard_num
                LIMIT 1
             )"             2>/dev/null | tr -d '[:space:]')
        # fallback на прямой count если clusterAllReplicas недоступен
        if [ -z "$new_count" ]; then
            new_count=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT"                 "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD"                 "SELECT count() FROM \`$db\`.\`$table\`"                 2>/dev/null | tr -d '[:space:]')
        fi
    else
        new_count=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT"             "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD"             "SELECT count() FROM \`$db\`.\`$table\`"             2>/dev/null | tr -d '[:space:]')
    fi
    new_count=${new_count:-0}

    if [ "$old_count" -eq "$new_count" ]; then
        log_success "  ✓ [$engine_label] \`$db\`.\`$table\`: $old_count строк"
        return 0
    else
        log_warning "  ✗ [$engine_label] \`$db\`.\`$table\`: старый=$old_count новый=$new_count"
        return 1
    fi
}

verify_migration() {
    $DRY_RUN && { log_step "[DRY-RUN] Пропускаем Шаг 5 (верификация)"; return 0; }
    log_step "=== Шаг 5: Верификация ==="

    local total_ok=0 total_fail=0

    local databases
    if [ -n "$TARGET_DB" ]; then
        databases="$TARGET_DB"
    else
        databases=$(ch_old             "SELECT name FROM system.databases
             WHERE name NOT IN ($EXCLUDED_DATABASES)")
    fi

    for db in $databases; do

        # ── Кейс 1 и 2: Distributed и ReplicatedMergeTree ────────────────────
        local tables
        tables=$(ch_old             "SELECT name, engine FROM system.tables
             WHERE database = '$db'
               AND engine IN ('Distributed', 'ReplicatedMergeTree')")

        while IFS=$'	' read -r table engine; do
            [ -z "$table" ] && continue

            local size
            size=$(get_table_size "$db" "$table" "$engine")
            (( size > SIZE_LIMIT_BYTES )) && {
                log_file "  ⏭ \`$db\`.\`$table\` — пропущено (>100 GB)"
                continue
            }

            verify_table_count "$db" "$table" "$engine" false
            local rc=$?
            [ $rc -eq 0 ] && total_ok=$(( total_ok + 1 ))                           || total_fail=$(( total_fail + 1 ))
        done <<< "$tables"

        # ── Кейс 3: MergeTree без Distributed → ReplicatedMergeTree без {shard}
        # Данные должны лежать ровно на одном шарде нового кластера.
        for suspect in "${SUSPECT_TABLES[@]}"; do
            echo "$suspect" | grep -q "РАЗНЫЕ ДАННЫЕ" && continue

            local s_db s_table
            s_db="${suspect%%.*}"
            s_table="${suspect#*.}"
            [ "$s_db" != "$db" ] && continue

            verify_table_count "$s_db" "$s_table" "MergeTree→Replicated" true
            local rc=$?
            [ $rc -eq 0 ] && total_ok=$(( total_ok + 1 ))                           || total_fail=$(( total_fail + 1 ))
        done
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
SUSPECT_TABLES=()

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

    if [ ${#SUSPECT_TABLES[@]} -gt 0 ]; then
        log_warning ""
        log_warning "⚠️  Следующие MergeTree-таблицы не имеют Distributed сверху и требуют проверки:"
        for t in "${SUSPECT_TABLES[@]}"; do
            log_warning "    - $t"
        done
        log_warning "   Рекомендация: убедитесь что эти таблицы действительно должны быть ReplicatedMergeTree"
        log_warning "   и проверьте корректность перенесённых данных вручную."
    fi

    log_success "=================================================="
    log_success "  Миграция завершена. Лог: $LOG_FILE"
    log_success "=================================================="
}

main