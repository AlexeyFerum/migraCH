#!/bin/bash

# =============================================================================
# ClickHouse Cross-Cluster Migration Script
#
# Переносит данные с одного физического кластера ClickHouse на другой.
#
# Топология:
#   Старый кластер: N шардов, без реплик (MergeTree)
#   Новый кластер:  M шардов × K реплик (ReplicatedMergeTree)
#
# Конвертация движков:
#   MergeTree           → ReplicatedMergeTree('/clickhouse/{database}/{table}/{shard}/', '{replica}')
#   ReplicatedMergeTree → ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}')
#   Все остальные движки переносятся без изменений.
#
# Порядок миграции:
#   1. Экспорт DDL со старого кластера
#   2. Применение DDL на новом кластере:
#      базы → таблицы (с конвертацией) → вью → словари
#   3. Перенос данных:
#      MergeTree           — читаем с каждой ноды старого кластера отдельно
#      ReplicatedMergeTree — читаем с одной ноды (данные идентичны на всех)
#      Distributed         — данные не переносим (только DDL)
#   4. Верификация
#
# Запуск: bash ch_migration.sh
# =============================================================================

# ── Конфигурация ──────────────────────────────────────────────────────────────

# Ноды старого кластера (все шарды).
# Скрипт запускается на одной из этих нод.
# Для MergeTree таблиц данные читаются с каждой ноды отдельно.
OLD_CLUSTER_SHARDS=(
    "shard1.old-cluster.internal"
    "shard2.old-cluster.internal"
    "shard3.old-cluster.internal"
    "shard4.old-cluster.internal"
)

# Порт native-протокола на старом кластере
OLD_CLUSTER_PORT="9000"

# Точка входа на новый кластер (любая нода — DDL применяется через ON CLUSTER)
NEW_CLUSTER_HOST="shard1.new-cluster.internal"
NEW_CLUSTER_PORT="9000"

# Общие credentials (одинаковые для старого и нового кластера)
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""

# Имя кластера — одинаковое на старом и новом (сервисы не должны его замечать)
CLUSTER_NAME="epm_cluster"

# Директория для хранения экспортированного DDL
BACKUP_DIR="/var/lib/clickhouse/migration_backup"

# Лог-файл
LOG_FILE="/var/log/clickhouse-migration.log"

# Системные базы данных — не мигрируем
EXCLUDED_DATABASES="'system', 'information_schema', 'INFORMATION_SCHEMA', 'default'"

# ── Цвета вывода ──────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ── Вспомогательные функции ───────────────────────────────────────────────────

get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S.%3N'
}

log() {
    local ts
    ts=$(get_timestamp)
    echo -e "$ts - $1" | tee -a "$LOG_FILE"
}

success() {
    local ts
    ts=$(get_timestamp)
    echo -e "${GREEN}$ts - $1${NC}" | tee -a "$LOG_FILE"
}

error() {
    local ts
    ts=$(get_timestamp)
    echo -e "${RED}$ts - ERROR: $1${NC}" | tee -a "$LOG_FILE"
    exit 1
}

warning() {
    local ts
    ts=$(get_timestamp)
    echo -e "${YELLOW}$ts - WARNING: $1${NC}" | tee -a "$LOG_FILE"
}

# Выполнить запрос на произвольном хосте
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

# Выполнить запрос на первой ноде старого кластера
ch_old() {
    ch_query "${OLD_CLUSTER_SHARDS[0]}" "$OLD_CLUSTER_PORT" "$1"
}

# Выполнить запрос на конкретной ноде старого кластера
ch_old_shard() {
    local host="$1"
    local query="$2"
    ch_query "$host" "$OLD_CLUSTER_PORT" "$query"
}

# Выполнить запрос на новом кластере
ch_new() {
    ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "$1"
}

# ── Предварительные проверки ──────────────────────────────────────────────────

check_dependencies() {
    if ! command -v clickhouse-client &>/dev/null; then
        error "clickhouse-client не найден. Установите ClickHouse client."
    fi
}

check_connections() {
    log "Проверка подключений..."

    for shard in "${OLD_CLUSTER_SHARDS[@]}"; do
        if ch_query "$shard" "$OLD_CLUSTER_PORT" "SELECT 1" >/dev/null 2>&1; then
            log "  ✓ Старый кластер, нода: $shard"
        else
            error "Не удалось подключиться к ноде старого кластера: $shard"
        fi
    done

    if ch_new "SELECT 1" >/dev/null 2>&1; then
        log "  ✓ Новый кластер: $NEW_CLUSTER_HOST"
    else
        error "Не удалось подключиться к новому кластеру: $NEW_CLUSTER_HOST"
    fi

    success "Все подключения успешны"
}

check_replication_macros() {
    log "Проверка макросов репликации на новом кластере..."

    local shard_macro replica_macro
    shard_macro=$(ch_new "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')
    replica_macro=$(ch_new "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')

    local ok=1

    if [ -z "$shard_macro" ] || [ "$shard_macro" = "shard" ]; then
        warning "  Макрос 'shard' не задан на новом кластере"
        ok=0
    else
        log "  Макрос 'shard'   = $shard_macro"
    fi

    if [ -z "$replica_macro" ] || [ "$replica_macro" = "replica" ]; then
        warning "  Макрос 'replica' не задан на новом кластере"
        ok=0
    else
        log "  Макрос 'replica' = $replica_macro"
    fi

    if [ "$ok" -eq 0 ]; then
        error "Макросы 'shard' и 'replica' должны быть прописаны в config.xml на каждой ноде нового кластера."
    fi

    success "Макросы репликации проверены"
}

# ── Директория бэкапа ─────────────────────────────────────────────────────────

create_backup_dir() {
    mkdir -p "$BACKUP_DIR/ddl"
    log "Директория бэкапа: $BACKUP_DIR"
}

# ── Шаг 1: Экспорт DDL ───────────────────────────────────────────────────────

export_ddl() {
    log "=== Шаг 1: Экспорт DDL ==="

    local databases
    databases=$(ch_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")

    if [ -z "$databases" ]; then
        warning "Пользовательских баз данных не найдено"
        return 0
    fi

    for db in $databases; do
        log "Экспорт базы: $db"
        mkdir -p "$BACKUP_DIR/ddl/$db"

        # DDL базы данных
        ch_old "SHOW CREATE DATABASE \`$db\`" \
            > "$BACKUP_DIR/ddl/$db/database.sql"

        # Таблицы (без вью и словарей)
        local tables
        tables=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database = '$db'
               AND engine NOT LIKE '%View%'
               AND engine != 'Dictionary'")

        for table in $tables; do
            log "  Таблица: $db.\`$table\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$table\`" \
                > "$BACKUP_DIR/ddl/$db/$table.sql"
        done

        # Вью
        local views
        views=$(ch_old \
            "SELECT name FROM system.tables
             WHERE database = '$db'
               AND engine LIKE '%View%'")

        for view in $views; do
            log "  Вью: $db.\`$view\`"
            ch_old "SHOW CREATE TABLE \`$db\`.\`$view\`" \
                > "$BACKUP_DIR/ddl/$db/$view.view.sql"
        done

        # Словари
        local dicts
        dicts=$(ch_old \
            "SELECT name FROM system.dictionaries WHERE database = '$db'")

        for dict in $dicts; do
            log "  Словарь: $db.\`$dict\`"
            ch_old "SHOW CREATE DICTIONARY \`$db\`.\`$dict\`" \
                > "$BACKUP_DIR/ddl/$db/$dict.dict.sql"
        done
    done

    success "Экспорт DDL завершён"
}

# ── Конвертация движков ───────────────────────────────────────────────────────

# Конвертирует ENGINE-клаузу в DDL-файле по следующим правилам:
#
#   MergeTree(...)
#     → ReplicatedMergeTree('/clickhouse/{database}/{table}/{shard}/', '{replica}')
#
#   ReplicatedMergeTree('/старый/путь', '{replica}' [, доп_аргументы])
#     → ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}')
#
#   Все остальные движки — без изменений.
#
# Важно: дополнительные аргументы MergeTree (например, ORDER BY и т.д.)
# находятся НЕ внутри ENGINE = ..., а отдельными клаузами DDL,
# поэтому они сохраняются автоматически.
convert_engine() {
    local file="$1"
    local db="$2"
    local table="$3"

    [ -f "$file" ] || return 1

    local content
    content=$(cat "$file")

    # ── Случай 1: уже ReplicatedMergeTree ────────────────────────────────────
    # Заменяем путь и реплику, остальное не трогаем.
    if echo "$content" | grep -qiP "ENGINE\s*=\s*ReplicatedMergeTree"; then
        log "    Движок ReplicatedMergeTree — обновляем ZK-путь (без {shard})"
        # Заменяем первые два строковых аргумента (путь + реплика),
        # любые последующие аргументы сохраняются.
        content=$(echo "$content" | perl -pe \
            "s|ENGINE\s*=\s*ReplicatedMergeTree\s*\(\s*'[^']*'\s*,\s*'[^']*'|ENGINE = ReplicatedMergeTree('/clickhouse/${database}/${table}/', '{replica}')|i")
        echo "$content" > "$file"
        return 0
    fi

    # ── Случай 2: plain MergeTree ─────────────────────────────────────────────
    # Все прочие варианты семейства (Summing, Replacing и т.д.) не трогаем —
    # они попадут сюда только если явно указаны в условии ниже.
    if echo "$content" | grep -qiP "ENGINE\s*=\s*MergeTree"; then
        log "    Движок MergeTree → ReplicatedMergeTree (с {shard})"
        # MergeTree() может иметь пустые скобки или устаревшие аргументы —
        # заменяем ENGINE = MergeTree(...) целиком.
        content=$(echo "$content" | perl -pe \
            "s|ENGINE\s*=\s*MergeTree\s*\([^)]*\)|ENGINE = ReplicatedMergeTree('/clickhouse/${database}/${table}/{shard}/', '{replica}')|i")
        echo "$content" > "$file"
        return 0
    fi

    # Всё остальное — не трогаем
    log "    Движок не требует конвертации"
}

# ── Добавление ON CLUSTER ─────────────────────────────────────────────────────

add_on_cluster() {
    local file="$1"

    [ -f "$file" ] || return 1

    local content
    content=$(cat "$file")

    # Если уже есть — ничего не делаем
    if echo "$content" | grep -qi "ON CLUSTER"; then
        return 0
    fi

    # Вставляем ON CLUSTER после имени сущности в первой строке CREATE.
    # Паттерн: CREATE [OR REPLACE] [MATERIALIZED] TABLE/VIEW/DATABASE/DICTIONARY `name`
    content=$(echo "$content" | perl -pe \
        's|(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+`[^`]+`)(?:\s+ON\s+CLUSTER\s+\S+)?|$1 ON CLUSTER '"$CLUSTER_NAME"'|i; last')

    echo "$content" > "$file"
}

# ── Шаг 2: Применение DDL на новом кластере ──────────────────────────────────

apply_ddl() {
    log "=== Шаг 2: Применение DDL на новом кластере ==="

    # 2.1 Базы данных
    log "Создание баз данных..."
    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        [ -d "$db_dir" ] || continue
        local db
        db=$(basename "$db_dir")
        local f="$db_dir/database.sql"
        [ -f "$f" ] || continue

        add_on_cluster "$f"
        log "  CREATE DATABASE \`$db\`"
        ch_new "$(cat "$f")" || warning "  Не удалось создать базу '$db' (возможно, уже существует)"
    done

    # 2.2 Таблицы (с конвертацией движков)
    log "Создание таблиц..."
    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        [ -d "$db_dir" ] || continue
        local db
        db=$(basename "$db_dir")

        for f in "$db_dir"*.sql; do
            [ -f "$f" ] || continue
            local fname
            fname=$(basename "$f")

            # Пропускаем database.sql, вью и словари
            [[ "$fname" == "database.sql" ]] && continue
            [[ "$fname" == *.view.sql     ]] && continue
            [[ "$fname" == *.dict.sql     ]] && continue

            local table
            table=$(basename "$f" .sql)

            convert_engine "$f" "$db" "$table"
            add_on_cluster "$f"

            log "  CREATE TABLE \`$db\`.\`$table\`"
            ch_new "$(cat "$f")" || warning "  Не удалось создать таблицу '$db'.'$table'"
        done
    done

    # 2.3 Вью
    log "Создание вью..."
    for f in "$BACKUP_DIR/ddl"/*/*.view.sql; do
        [ -f "$f" ] || continue
        local db
        db=$(basename "$(dirname "$f")")
        local view
        view=$(basename "$f" .view.sql)

        add_on_cluster "$f"

        log "  CREATE VIEW \`$db\`.\`$view\`"
        ch_new "$(cat "$f")" || warning "  Не удалось создать вью '$db'.'$view'"
    done

    # 2.4 Словари
    log "Создание словарей..."
    for f in "$BACKUP_DIR/ddl"/*/*.dict.sql; do
        [ -f "$f" ] || continue
        local db
        db=$(basename "$(dirname "$f")")
        local dict
        dict=$(basename "$f" .dict.sql)

        add_on_cluster "$f"

        log "  CREATE DICTIONARY \`$db\`.\`$dict\`"
        ch_new "$(cat "$f")" || warning "  Не удалось создать словарь '$db'.'$dict'"
    done

    success "Применение DDL завершено"
}

# ── Шаг 3: Перенос данных ─────────────────────────────────────────────────────

migrate_data() {
    log "=== Шаг 3: Перенос данных ==="

    local databases
    databases=$(ch_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")

    [ -z "$databases" ] && { warning "Нет баз для миграции данных"; return 0; }

    for db in $databases; do
        log "База: $db"

        # Только MergeTree и ReplicatedMergeTree — остальные не трогаем
        local tables
        tables=$(ch_old \
            "SELECT name, engine
             FROM system.tables
             WHERE database = '$db'
               AND (engine = 'MergeTree' OR engine = 'ReplicatedMergeTree')")

        while IFS=$'\t' read -r table engine; do
            [ -z "$table" ] && continue
            case "$engine" in
                MergeTree)
                    migrate_mergetree "$db" "$table"
                    ;;
                ReplicatedMergeTree)
                    migrate_replicated "$db" "$table"
                    ;;
            esac
        done <<< "$tables"
    done

    success "Перенос данных завершён"
}

# MergeTree: каждая нода хранит свой кусок данных — читаем с каждой отдельно
migrate_mergetree() {
    local db="$1"
    local table="$2"

    log "  MergeTree \`$db\`.\`$table\` — читаем с каждого шарда"

    for shard in "${OLD_CLUSTER_SHARDS[@]}"; do
        local count
        count=$(ch_old_shard "$shard" \
            "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null | tr -d '[:space:]')

        if [ -z "$count" ] || [ "$count" = "0" ]; then
            log "    $shard — пусто, пропускаем"
            continue
        fi

        log "    $shard — $count строк, переносим..."

        ch_new "
            INSERT INTO \`$db\`.\`$table\`
            SELECT * FROM remote('$shard:$OLD_CLUSTER_PORT', \`$db\`, \`$table\`,
                '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')
        " || warning "    Не удалось перенести данные с $shard для $db.$table"
    done
}

# ReplicatedMergeTree: данные одинаковы на всех нодах — читаем с первой
migrate_replicated() {
    local db="$1"
    local table="$2"
    local src="${OLD_CLUSTER_SHARDS[0]}"

    local count
    count=$(ch_old_shard "$src" \
        "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null | tr -d '[:space:]')

    if [ -z "$count" ] || [ "$count" = "0" ]; then
        log "  ReplicatedMergeTree \`$db\`.\`$table\` — пусто, пропускаем"
        return 0
    fi

    log "  ReplicatedMergeTree \`$db\`.\`$table\` — $count строк, читаем с $src"

    ch_new "
        INSERT INTO \`$db\`.\`$table\`
        SELECT * FROM remote('$src:$OLD_CLUSTER_PORT', \`$db\`, \`$table\`,
            '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')
    " || warning "  Не удалось перенести данные для $db.$table"
}

# ── Шаг 4: Верификация ────────────────────────────────────────────────────────

verify_migration() {
    log "=== Шаг 4: Верификация ==="

    local databases
    databases=$(ch_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")

    local total_ok=0 total_fail=0

    for db in $databases; do
        local tables
        tables=$(ch_old \
            "SELECT name, engine
             FROM system.tables
             WHERE database = '$db'
               AND (engine = 'MergeTree' OR engine = 'ReplicatedMergeTree')")

        while IFS=$'\t' read -r table engine; do
            [ -z "$table" ] && continue

            # Суммарное количество строк на старом кластере
            local old_total=0
            if [ "$engine" = "MergeTree" ]; then
                # Складываем со всех шардов
                for shard in "${OLD_CLUSTER_SHARDS[@]}"; do
                    local c
                    c=$(ch_old_shard "$shard" \
                        "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null \
                        | tr -d '[:space:]')
                    old_total=$(( old_total + ${c:-0} ))
                done
            else
                # ReplicatedMergeTree — берём с одной ноды
                old_total=$(ch_old_shard "${OLD_CLUSTER_SHARDS[0]}" \
                    "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null \
                    | tr -d '[:space:]')
                old_total=${old_total:-0}
            fi

            # Количество строк на новом кластере (читаем через entry-point)
            local new_total
            new_total=$(ch_new \
                "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null \
                | tr -d '[:space:]')
            new_total=${new_total:-0}

            if [ "$old_total" -eq "$new_total" ]; then
                success "  ✓ \`$db\`.\`$table\`: $old_total строк"
                total_ok=$(( total_ok + 1 ))
            else
                warning "  ✗ \`$db\`.\`$table\`: старый=$old_total новый=$new_total"
                total_fail=$(( total_fail + 1 ))
            fi
        done <<< "$tables"
    done

    echo ""
    log "Итого: $total_ok таблиц OK, $total_fail с расхождениями"
    [ "$total_fail" -eq 0 ] \
        && success "Верификация пройдена" \
        || warning "Верификация завершена с расхождениями — проверьте лог"
}

# ── Точка входа ───────────────────────────────────────────────────────────────

main() {
    log "=================================================="
    log "  Миграция ClickHouse кластера"
    log "=================================================="
    log "Старый кластер (шарды): ${OLD_CLUSTER_SHARDS[*]}"
    log "Новый кластер:          $NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT"
    log "Имя кластера:           $CLUSTER_NAME"
    log "=================================================="

    check_dependencies
    check_connections
    check_replication_macros
    create_backup_dir

    export_ddl
    apply_ddl
    migrate_data
    verify_migration

    success "Миграция завершена. Лог: $LOG_FILE"
}

main