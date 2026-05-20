#!/bin/bash
# =============================================================================
# ClickHouse Cross-Cluster Migration Script (v8 - Bulletproof File Safety)
# =============================================================================
set -o pipefail  # Ошибка в конвейере не будет скрыта

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

# ── Аргументы ─────────────────────────────────────────────────────────────────
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
: > "$LOG_FILE"

log_file()  { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1" >> "$LOG_FILE"; }
log_step()  { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1" | tee -a "$LOG_FILE"; }
log_success(){ echo -e "\033[0;32m$(date '+%Y-%m-%d %H:%M:%S.%3N') - $1\033[0m" | tee -a "$LOG_FILE"; }
log_warning(){ echo -e "\033[1;33m$(date '+%Y-%m-%d %H:%M:%S.%3N') - WARNING: $1\033[0m" | tee -a "$LOG_FILE"; }
log_error()  { echo -e "\033[0;31m$(date '+%Y-%m-%d %H:%M:%S.%3N') - ERROR: $1\033[0m" | tee -a "$LOG_FILE"; exit 1; }

# ── ClickHouse-запросы (stdin, безопасное экранирование) ─────────────────────
ch_query() {
  local host="$1" port="$2" query="$3"
  local clean_query
  clean_query=$(printf '%s' "$query" | tr -d '\r' | tr -s '[:space:]' ' ' | sed 's/^ *//;s/ *$//')
  
  log_file "CH_EXEC [${host}:${port}] -> ${clean_query:0:150}$( [ ${#clean_query} -gt 150 ] && echo '...')"
  printf '%s\n' "$clean_query" | clickhouse-client \
    --host="$host" --port="$port" \
    --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" \
    --multiquery 2>>"$LOG_FILE"
}

ch_old() { ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "$1"; }

ch_new() {
  if $DRY_RUN; then
    log_file "[DRY-RUN] Пропускаем запрос на новом кластере"
    return 0
  fi
  if ! ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "$1"; then
    log_error "Критическая ошибка на новом кластере. Скрипт остановлен."
  fi
}

# ── Предпроверки ──────────────────────────────────────────────────────────────
check_dependencies() {
  command -v clickhouse-client &>/dev/null || log_error "clickhouse-client не найден"
  log_file "clickhouse-client: $(command -v clickhouse-client)"
}

check_connections() {
  log_step "Проверка подключений..."
  ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "SELECT 1" &>/dev/null \
    || log_error "Нет связи со старым кластером ($OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT)"
  $DRY_RUN || {
    ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT 1" &>/dev/null \
      || log_error "Нет связи с новым кластером ($NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT)"
  }
  log_success "Подключения успешны"
}

check_macros() {
  $DRY_RUN && { log_step "[DRY-RUN] Пропускаем проверку макросов"; return 0; }
  log_step "Проверка макросов репликации..."
  local s=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')
  local r=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')
  { [ -z "$s" ] || [ "$s" = "shard" ]; } && log_error "Макрос 'shard' не задан на новом кластере"
  { [ -z "$r" ] || [ "$r" = "replica" ]; } && log_error "Макрос 'replica' не задан на новом кластере"
  log_success "Макросы OK"
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
  [ -z "$databases" ] && { log_warning "Баз данных для экспорта не найдено"; return 0; }

  for db in $databases; do
    log_step "📦 Экспорт БД: $db"
    mkdir -p "$BACKUP_DIR/ddl/$db"
    ch_old "SHOW CREATE DATABASE \`$db\`" > "$BACKUP_DIR/ddl/$db/database.sql"

    ch_old "SELECT name FROM system.tables WHERE database='$db' AND engine NOT LIKE '%View%' AND engine!='Dictionary'" \
      | while read -r tbl; do
        [ -z "$tbl" ] && continue
        log_file "  Экспорт таблицы: $db.$tbl"
        ch_old "SHOW CREATE TABLE \`$db\`.\`$tbl\`" > "$BACKUP_DIR/ddl/$db/$tbl.sql"
      done

    ch_old "SELECT name FROM system.tables WHERE database='$db' AND engine LIKE '%View%'" \
      | while read -r v; do
        [ -z "$v" ] && continue
        log_file "  Экспорт вью: $db.$v"
        ch_old "SHOW CREATE TABLE \`$db\`.\`$v\`" > "$BACKUP_DIR/ddl/$db/$v.view.sql"
      done

    ch_old "SELECT name FROM system.dictionaries WHERE database='$db'" \
      | while read -r d; do
        [ -z "$d" ] && continue
        log_file "  Экспорт словаря: $db.$d"
        ch_old "SHOW CREATE DICTIONARY \`$db\`.\`$d\`" > "$BACKUP_DIR/ddl/$db/$d.dict.sql"
      done
  done
  log_success "Экспорт DDL завершён"
}

# ── Утилиты DDL (АТОМАРНЫЕ, С БЭККАПОМ, БЕЗ content=$(cat)) ──────────────────
safe_modify() {
  local file="$1" modifier_name="$2"
  local bak="${file}.safe_bak"
  
  [ -f "$file" ] || log_error "Файл не найден: $file"
  [ -s "$file" ] || log_error "Файл пустой перед $modifier_name: $file"
  
  cp -f "$file" "$bak"
  shift 2
  "$@"
  
  if [ ! -s "$file" ]; then
    log_error "ФАТАЛЬНО: $file стал пустым после $modifier_name! Восстановлен из бэкапа."
    mv -f "$bak" "$file"
  else
    rm -f "$bak"
  fi
}

convert_engine() {
  local file="$1"
  if grep -qiP "ENGINE\s*=\s*ReplicatedMergeTree" "$file"; then
    log_file "    ReplicatedMergeTree → '/clickhouse/{database}/{table}/'"
    safe_modify "$file" "convert_engine" perl -i -pe 's#(ENGINE\s*=\s*ReplicatedMergeTree\s*\(\s*)\x27[^\x27]*\x27\s*,\s*\x27[^\x27]*\x27#$1\x27/clickhouse/{database}/{table}/\x27, \x27{replica}\x27#i' "$file"
  elif grep -qiP "ENGINE\s*=\s*MergeTree" "$file"; then
    log_file "    MergeTree → '/clickhouse/{database}/{table}/{shard}/'"
    safe_modify "$file" "convert_engine" perl -i -pe 's#ENGINE\s*=\s*MergeTree\s*\([^)]*\)#ENGINE = ReplicatedMergeTree(\x27/clickhouse/{database}/{table}/{shard}/\x27, \x27{replica}\x27)#i' "$file"
  else
    log_file "    Движок не требует конвертации"
  fi
}

add_on_cluster() {
  local file="$1"
  [ -f "$file" ] || return 0
  grep -qi "ON CLUSTER" "$file" && return 0
  log_file "    Добавление ON CLUSTER $CLUSTER_NAME"
  # \$1 экранирован, чтобы Bash не подставил его. $CLUSTER_NAME подставляется корректно.
  safe_modify "$file" "add_on_cluster" perl -i -pe "s#^(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+\S+)#\$1 ON CLUSTER $CLUSTER_NAME#i; last" "$file"
}

# ── Шаг 2: Применение DDL ─────────────────────────────────────────────────────
apply_ddl() {
  log_step "=== Шаг 2: Применение DDL на новом кластере ==="
  for db_dir in "$BACKUP_DIR/ddl"/*/; do
    [ -d "$db_dir" ] || continue
    local db=$(basename "$db_dir")
    [ -n "$TARGET_DB" ] && [[ "$db" != "$TARGET_DB" ]] && continue

    add_on_cluster "$db_dir/database.sql"
    log_file "  CREATE DATABASE \`$db\`"
    ch_new "$(cat "$db_dir/database.sql")"

    for f in "$db_dir"*.sql; do
      [ -f "$f" ] || continue
      local fname=$(basename "$f")
      [[ "$fname" == "database.sql" || "$fname" == *.view.sql || "$fname" == *.dict.sql ]] && continue
      local table=$(basename "$f" .sql)
      convert_engine "$f"
      add_on_cluster "$f"
      log_file "  CREATE TABLE \`$db\`.\`$table\`"
      ch_new "$(cat "$f")"
    done

    for f in "$db_dir"*.view.sql; do
      [ -f "$f" ] || continue
      add_on_cluster "$f"
      log_file "  CREATE VIEW \`$db\`.$(basename "$f" .view.sql)"
      ch_new "$(cat "$f")"
    done

    for f in "$db_dir"*.dict.sql; do
      [ -f "$f" ] || continue
      add_on_cluster "$f"
      log_file "  CREATE DICTIONARY \`$db\`.$(basename "$f" .dict.sql)"
      ch_new "$(cat "$f")"
    done
  done
  log_success "Применение DDL завершено"
}

# ── Шаг 3: Перенос данных ─────────────────────────────────────────────────────
migrate_data() {
  $DRY_RUN && { log_step "[DRY-RUN] Пропуск Шага 3"; return 0; }
  log_step "=== Шаг 3: Перенос данных ==="

  local databases
  if [ -n "$TARGET_DB" ]; then databases="$TARGET_DB"
  else databases=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)"); fi

  for db in $databases; do
    log_step "📤 Перенос данных из БД: $db"
    local tables=$(ch_old "SELECT name, engine FROM system.tables WHERE database='$db' AND engine IN ('Distributed', 'ReplicatedMergeTree')")
    while IFS=$'\t' read -r table engine; do
      [ -z "$table" ] && continue
      local size=$(ch_old "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='$db' AND table='$table'" 2>/dev/null | awk '{print int($1+0)}')
      if (( ${size:-0} > SIZE_LIMIT_BYTES )); then
        log_warning "  ⏭ \`$db\`.\`$table\` >100GB — пропущено"
        LARGE_TABLES+=("$db.$table")
        continue
      fi
      log_file "  Перенос $engine \`$db\`.\`$table\`"
      ch_new "INSERT INTO \`$db\`.\`$table\` SELECT * FROM remote('$OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT', \`$db\`, \`$table\`, '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')"
    done <<< "$tables"
  done
  log_success "Перенос данных завершён"
}

# ── Шаг 4: Верификация ────────────────────────────────────────────────────────
verify_migration() {
  $DRY_RUN && { log_step "[DRY-RUN] Пропуск Шага 4"; return 0; }
  log_step "=== Шаг 4: Верификация ==="
  local ok=0 fail=0
  local databases
  if [ -n "$TARGET_DB" ]; then databases="$TARGET_DB"
  else databases=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)"); fi

  for db in $databases; do
    local tables=$(ch_old "SELECT name FROM system.tables WHERE database='$db' AND engine IN ('Distributed', 'ReplicatedMergeTree')")
    while read -r table; do
      [ -z "$table" ] && continue
      local size=$(ch_old "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='$db' AND table='$table'" 2>/dev/null | awk '{print int($1+0)}')
      (( ${size:-0} > SIZE_LIMIT_BYTES )) && continue

      local c_old=$(ch_old "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null | tr -d '[:space:]')
      local c_new=$(ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "SELECT count() FROM \`$db\`.\`$table\`" 2>/dev/null | tr -d '[:space:]')
      if [ "${c_old:-0}" -eq "${c_new:-0}" ]; then
        log_success "  ✓ $db.$table: ${c_old:-0}"
        ((ok++))
      else
        log_warning "  ✗ $db.$table: old=${c_old:-0} new=${c_new:-0}"
        ((fail++))
      fi
    done <<< "$tables"
  done
  log_file "Итого: $ok OK, $fail расхождений"
  [ "$fail" -eq 0 ] && log_success "Верификация пройдена" || log_warning "Есть расхождения — проверьте лог"
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
    log_warning "⚠️  Пропущены таблицы >100GB: ${LARGE_TABLES[*]}"
  fi
  log_success "Готово. Детальный лог: $LOG_FILE"
}
main