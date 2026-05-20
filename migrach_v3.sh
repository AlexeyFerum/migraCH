#!/bin/bash
# =============================================================================
# ClickHouse Cross-Cluster Migration Script (v2 - Fixed & Optimized)
# =============================================================================

# ── Конфигурация ──────────────────────────────────────────────────────────────
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
SIZE_LIMIT_BYTES=$((100 * 1024 * 1024 * 1024))  # 100 GB

TARGET_DB=""
DRY_RUN=false

# ── Парсинг аргументов ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)      TARGET_DB="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true;   shift ;;
    --help|-h) echo "Usage: $0 [--db <database_name>] [--dry-run]"; exit 0 ;;
    *)         echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Логирование ───────────────────────────────────────────────────────────────
mkdir -p "$(dirname "$LOG_FILE")"
> "$LOG_FILE" # Очищаем лог при старте

log_file()  { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1" >> "$LOG_FILE"; }
log_step()  { echo -e "$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1" | tee -a "$LOG_FILE"; }
success()   { echo -e "\033[0;32m$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1\033[0m" | tee -a "$LOG_FILE"; }
error()     { echo -e "\033[0;31m$(date '+%Y-%m-%d %H:%M:%S.%3N') - ERROR: $1\033[0m" | tee -a "$LOG_FILE"; exit 1; }
warning()   { echo -e "\033[1;33m$(date '+%Y-%m-%d %H:%M:%S.%3N') - WARNING: $1\033[0m" | tee -a "$LOG_FILE"; }

# ── ClickHouse-запросы ────────────────────────────────────────────────────────
ch_query() {
  local host="$1" port="$2" query="$3"
  clickhouse-client \
    --host="$host" --port="$port" \
    --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" \
    --multiquery --query="$query" 2>>"$LOG_FILE"
}

ch_old() { ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "$1"; }

ch_new() {
  if $DRY_RUN; then
    log_file "[DRY-RUN] Пропускаем запрос на новом кластере:"
    echo "$1" >> "$LOG_FILE"
    return 0
  fi
  ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "$1"
}

# ── Предпроверки ──────────────────────────────────────────────────────────────
check_dependencies() {
  command -v clickhouse-client &>/dev/null || error "clickhouse-client не найден"
}

check_connections() {
  log_step "Проверка подключений..."
  ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "SELECT 1" &>/dev/null \
    || error "Нет связи со старым кластером ($OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT)"
  $DRY_RUN || {
    ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT 1" &>/dev/null \
      || error "Нет связи с новым кластером ($NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT)"
  }
  success "Подключения успешны"
}

check_macros() {
  $DRY_RUN && { log_step "[DRY-RUN] Пропускаем проверку макросов"; return 0; }
  log_step "Проверка макросов репликации..."
  local s=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')
  local r=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')
  if [ -z "$s" ] || [ "$s" = "shard" ]; then error "Макрос 'shard' не задан на новом кластере"; fi
  if [ -z "$r" ] || [ "$r" = "replica" ]; then error "Макрос 'replica' не задан на новом кластере"; fi
  success "Макросы OK"
}

# ── Шаг 1: Экспорт DDL ───────────────────────────────────────────────────────
export_ddl() {
  log_step "=== Шаг 1: Экспорт DDL ==="
  local databases
  if [ -n "$TARGET_DB" ]; then
    databases="$TARGET_DB"
    log_file "Выбрана конкретная БД: $databases"
  else
    databases=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    log_file "Экспорт всех пользовательских БД"
  fi

  [ -z "$databases" ] && { warning "Баз данных для экспорта не найдено"; return 0; }

  for db in $databases; do
    log_step "📦 Экспорт БД: $db"
    mkdir -p "$BACKUP_DIR/ddl/$db"
    ch_old "SHOW CREATE DATABASE \`$db\`" > "$BACKUP_DIR/ddl/$db/database.sql"

    # Таблицы
    ch_old "SELECT name FROM system.tables WHERE database='$db' AND engine NOT LIKE '%View%' AND engine!='Dictionary'" \
      | while read -r tbl; do
        [ -z "$tbl" ] && continue
        ch_old "SHOW CREATE TABLE \`$db\`.\`$tbl\`" > "$BACKUP_DIR/ddl/$db/$tbl.sql"
      done

    # Вью
    ch_old "SELECT name FROM system.tables WHERE database='$db' AND engine LIKE '%View%'" \
      | while read -r v; do
        [ -z "$v" ] && continue
        ch_old "SHOW CREATE TABLE \`$db\`.\`$v\`" > "$BACKUP_DIR/ddl/$db/$v.view.sql"
      done

    # Словари
    ch_old "SELECT name FROM system.dictionaries WHERE database='$db'" \
      | while read -r d; do
        [ -z "$d" ] && continue
        ch_old "SHOW CREATE DICTIONARY \`$db\`.\`$d\`" > "$BACKUP_DIR/ddl/$db/$d.dict.sql"
      done
  done
  success "Экспорт DDL завершён"
}

# ── Утилиты DDL (ИСПРАВЛЕННЫЕ: in-place редактирование) ───────────────────────
convert_engine() {
  local file="$1"
  [ -f "$file" ] || return 1

  if grep -qiP "ENGINE\s*=\s*ReplicatedMergeTree" "$file"; then
    log_file "    ReplicatedMergeTree → обновление ZK-пути (без {shard})"
    perl -i -pe "s|ENGINE\s*=\s*ReplicatedMergeTree\s*\(\s*'[^']*'\s*,\s*'[^']*'|ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{table}/', '{replica}')|i" "$file"
  elif grep -qiP "ENGINE\s*=\s*MergeTree" "$file"; then
    log_file "    MergeTree → ReplicatedMergeTree (с {shard})"
    perl -i -pe "s|ENGINE\s*=\s*MergeTree\s*\([^)]*\)|ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{table}/{shard}/', '{replica}')|i" "$file"
  else
    log_file "    Движок не требует конвертации"
  fi
}

add_on_cluster() {
  local file="$1"
  [ -f "$file" ] || return 1
  grep -qi "ON CLUSTER" "$file" && return 0

  log_file "    Добавление ON CLUSTER"
  # Безопасная замена: редактируем файл напрямую, не перезаписывая через переменную
  perl -i -pe 's|(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+[\`]?[^\s\`]+[\`]?)(?:\s+ON\s+CLUSTER\s+\S+)?|$1 ON CLUSTER '"$CLUSTER_NAME"'|i' "$file"
}

# ── Шаг 2: Применение DDL ─────────────────────────────────────────────────────
apply_ddl() {
  log_step "=== Шаг 2: Применение DDL на новом кластере ==="
  for db_dir in "$BACKUP_DIR/ddl"/*/; do
    [ -d "$db_dir" ] || continue
    local db=$(basename "$db_dir")
    [ -n "$TARGET_DB" ] && [[ "$db" != "$TARGET_DB" ]] && continue

    # БД
    add_on_cluster "$db_dir/database.sql"
    ch_new "$(cat "$db_dir/database.sql")" || log_file "  БД '$db' уже существует"

    # Таблицы
    for f in "$db_dir"*.sql; do
      [ -f "$f" ] || continue
      local fname=$(basename "$f")
      [[ "$fname" == "database.sql" || "$fname" == *.view.sql || "$fname" == *.dict.sql ]] && continue
      convert_engine "$f"
      add_on_cluster "$f"
      ch_new "$(cat "$f")" || warning "  Ошибка создания '$db'.'$(basename "$f" .sql)'"
    done

    # Вью
    for f in "$db_dir"*.view.sql; do [ -f "$f" ] || continue; add_on_cluster "$f"; ch_new "$(cat "$f")" || warning "  Ошибка вью"; done

    # Словари
    for f in "$db_dir"*.dict.sql; do [ -f "$f" ] || continue; add_on_cluster "$f"; ch_new "$(cat "$f")" || warning "  Ошибка словаря"; done
  done
  success "Применение DDL завершено"
}

# ── Шаг 3: Перенос данных ─────────────────────────────────────────────────────
migrate_data() {
  $DRY_RUN && { log_step "[DRY-RUN] Пропуск Шага 3"; return 0; }
  log_step "=== Шаг 3: Перенос данных ==="

  local databases
  if [ -n "$TARGET_DB" ]; then databases="$TARGET_DB";
  else databases=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)"); fi

  for db in $databases; do
    log_step "📤 Перенос данных из БД: $db"
    local tables=$(ch_old "SELECT name, engine FROM system.tables WHERE database='$db' AND engine IN ('Distributed', 'ReplicatedMergeTree')")
    while IFS=$'\t' read -r table engine; do
      [ -z "$table" ] && continue
      local size=$(ch_old "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='$db' AND table='$table'" 2>/dev/null | awk '{print int($1+0)}')
      if (( ${size:-0} > SIZE_LIMIT_BYTES )); then
        warning "  ⏭ \`$db\`.\`$table\` >100GB — пропущено"
        LARGE_TABLES+=("$db.$table")
        continue
      fi
      log_file "  Перенос $engine \`$db\`.\`$table\`"
      ch_new "INSERT INTO \`$db\`.\`$table\` SELECT * FROM remote('$OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT', \`$db\`, \`$table\`, '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')" \
        || warning "  Ошибка переноса $db.$table"
    done <<< "$tables"
  done
  success "Перенос данных завершён"
}

# ── Шаг 4: Верификация ────────────────────────────────────────────────────────
verify_migration() {
  $DRY_RUN && { log_step "[DRY-RUN] Пропуск Шага 4"; return 0; }
  log_step "=== Шаг 4: Верификация ==="
  local ok=0 fail=0
  local databases
  if [ -n "$TARGET_DB" ]; then databases="$TARGET_DB";
  else databases=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)"); fi

  for db in $databases; do
    local tables=$(ch_old "SELECT name FROM system.tables WHERE database='$db' AND engine IN ('Distributed', 'ReplicatedMergeTree')")
    while read -r table; do
      [ -z "$table" ] && continue
      local size=$(ch_old "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='$db' AND table='$table'" 2>/dev/null | awk '{print int($1+0)}')
      (( ${size:-0} > SIZE_LIMIT_BYTES )) && continue

      local c_old=$(ch_old "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null | tr -d '[:space:]')
      local c_new=$(ch_new "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null | tr -d '[:space:]')
      if [ "${c_old:-0}" -eq "${c_new:-0}" ]; then success "  ✓ $db.$table: ${c_old:-0}"; ((ok++))
      else warning "  ✗ $db.$table: old=${c_old:-0} new=${c_new:-0}"; ((fail++)); fi
    done <<< "$tables"
  done
  log_file "Итого: $ok OK, $fail расхождений"
  [ "$fail" -eq 0 ] && success "Верификация пройдена" || warning "Есть расхождения — проверьте лог"
}

# ── Точка входа ───────────────────────────────────────────────────────────────
LARGE_TABLES=()
main() {
  log_step "=================================================="
  log_step "  Миграция ClickHouse ($([ $DRY_RUN = true ] && echo 'DRY RUN' || echo 'LIVE'))"
  log_step "  Целевая БД: ${TARGET_DB:-все (кроме системных)}"
  log_step "=================================================="
  check_dependencies; check_connections; check_macros
  export_ddl; apply_ddl; migrate_data; verify_migration

  if [ ${#LARGE_TABLES[@]} -gt 0 ]; then
    warning "⚠️  Пропущены таблицы >100GB: ${LARGE_TABLES[*]}"
  fi
  success "Готово. Детальный лог: $LOG_FILE"
}
main