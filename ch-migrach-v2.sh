#!/bin/bash
# =============================================================================
# ClickHouse Cross-Cluster Migration Script (v4 - Final)
# =============================================================================

# ── Конфигурация ──────────────────────────────────────────────────────────────
# Скрипт запускается НА одной из нод старого кластера.
# Укажите IP/hostname этой ноды (или localhost).
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
SIZE_LIMIT_BYTES=$((100 * 1024 * 1024 * 1024)) # 100 GB

# ── Аргументы ─────────────────────────────────────────────────────────────────
DRY_RUN=false
[[ "$1" == "--dry-run" ]] && DRY_RUN=true

# ── Цвета и логирование ───────────────────────────────────────────────────────
RED='\033[0;31m' GREEN='\033[0;32m' YELLOW='\033[1;33m' NC='\033[0m'
get_timestamp() { date '+%Y-%m-%d %H:%M:%S.%3N'; }
log()       { echo -e "$(get_timestamp) - $1" | tee -a "$LOG_FILE"; }
success()   { echo -e "${GREEN}$(get_timestamp) - $1${NC}" | tee -a "$LOG_FILE"; }
error()     { echo -e "${RED}$(get_timestamp) - ERROR: $1${NC}" | tee -a "$LOG_FILE"; exit 1; }
warning()   { echo -e "${YELLOW}$(get_timestamp) - WARNING: $1${NC}" | tee -a "$LOG_FILE"; }

ch_query() {
  local host="$1" port="$2" query="$3"
  if $DRY_RUN; then log "[DRY RUN] $host:$port >> $query"; return 0; fi
  clickhouse-client --host="$host" --port="$port" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --multiquery --query="$query" 2>>"$LOG_FILE"
}
ch_old() { ch_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "$1"; }
ch_new() { ch_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "$1"; }

# ── Предпроверки ──────────────────────────────────────────────────────────────
check_dependencies() { command -v clickhouse-client &>/dev/null || error "clickhouse-client не найден"; }
check_connections() {
  log "Проверка подключений..."
  ch_old "SELECT 1" &>/dev/null || error "Нет связи со старым кластером ($OLD_CLUSTER_HOST)"
  ch_new "SELECT 1" &>/dev/null || error "Нет связи с новым кластером ($NEW_CLUSTER_HOST)"
  success "Подключения успешны"
}
check_macros() {
  log "Проверка макросов..."
  local s=$(ch_new "SELECT getMacro('shard')" 2>/dev/null | tr -d '[:space:]')
  local r=$(ch_new "SELECT getMacro('replica')" 2>/dev/null | tr -d '[:space:]')
  { [ -z "$s" ] || [ "$s" = "shard" ]; } && error "Макрос 'shard' не задан"
  { [ -z "$r" ] || [ "$r" = "replica" ]; } && error "Макрос 'replica' не задан"
  success "Макросы OK"
}

mkdir -p "$BACKUP_DIR/ddl"

# ── Шаг 1: Экспорт DDL ───────────────────────────────────────────────────────
export_ddl() {
  log "=== Шаг 1: Экспорт DDL ==="
  local dbs=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
  [ -z "$dbs" ] && return 0
  for db in $dbs; do
    mkdir -p "$BACKUP_DIR/ddl/$db"
    ch_old "SHOW CREATE DATABASE \`$db\`" > "$BACKUP_DIR/ddl/$db/database.sql"
    for type_suffix in "" ".view" ".dict"; do
      local query="" suffix=""
      case "$type_suffix" in
        ".view") query="SELECT name FROM system.tables WHERE database='$db' AND engine LIKE '%View%'"; suffix="view" ;;
        ".dict") query="SELECT name FROM system.dictionaries WHERE database='$db'"; suffix="dict" ;;
        *) query="SELECT name FROM system.tables WHERE database='$db' AND engine NOT LIKE '%View%' AND engine!='Dictionary'"; suffix="sql" ;;
      esac
      ch_old "$query" | while read -r obj; do
        [ -z "$obj" ] && continue
        local cmd="SHOW CREATE TABLE"
        [[ "$suffix" == "dict" ]] && cmd="SHOW CREATE DICTIONARY"
        $cmd "\`$db\`.\`$obj\`" > "$BACKUP_DIR/ddl/$db/${obj}.${suffix}.sql" 2>/dev/null || \
        $cmd "\`$db\`.\`$obj\`" > "$BACKUP_DIR/ddl/$db/${obj}.sql"
      done
    done
  done
  success "Экспорт DDL завершён"
}

# ── Утилиты DDL ───────────────────────────────────────────────────────────────
convert_engine() {
  local f="$1" db="$2" tbl="$3"; [ -f "$f" ] || return 1
  local c=$(cat "$f")
  if echo "$c" | grep -qiP "ENGINE\s*=\s*ReplicatedMergeTree"; then
    c=$(echo "$c" | perl -pe "s|ENGINE\s*=\s*ReplicatedMergeTree\s*\(\s*'[^']*'\s*,\s*'[^']*'|ENGINE = ReplicatedMergeTree('/clickhouse/\${database}/\${table}/', '{replica}')|i")
  elif echo "$c" | grep -qiP "ENGINE\s*=\s*MergeTree"; then
    c=$(echo "$c" | perl -pe "s|ENGINE\s*=\s*MergeTree\s*\([^)]*\)|ENGINE = ReplicatedMergeTree('/clickhouse/\${database}/\${table}/{shard}/', '{replica}')|i")
  else return 0; fi
  echo "$c" > "$f"
}
add_on_cluster() {
  local f="$1"; [ -f "$f" ] || return 1
  local c=$(cat "$f")
  echo "$c" | grep -qi "ON CLUSTER" && return 0
  c=$(echo "$c" | perl -pe "s|(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW|DATABASE|DICTIONARY)\s+\S+)(?:\s+ON\s+CLUSTER\s+\S+)?|\1 ON CLUSTER $CLUSTER_NAME|i")
  echo "$c" > "$f"
}

# ── Шаг 2: Применение DDL ─────────────────────────────────────────────────────
apply_ddl() {
  log "=== Шаг 2: Применение DDL ==="
  for db_dir in "$BACKUP_DIR/ddl"/*/; do
    [ -d "$db_dir" ] || continue
    local db=$(basename "$db_dir")
    add_on_cluster "$db_dir/database.sql"
    ch_new "$(cat "$db_dir/database.sql")" || warning "БД '$db' уже существует"

    for f in "$db_dir"*.sql "$db_dir"*.view.sql "$db_dir"*.dict.sql; do
      [ -f "$f" ] || continue
      local fname=$(basename "$f"); [[ "$fname" == "database.sql" ]] && continue
      local obj=$(basename "$f" | sed -E 's/(\.view|\.dict)?\.sql$//')
      if [[ "$fname" == *.view.sql || "$fname" == *.dict.sql ]]; then
        add_on_cluster "$f"
        ch_new "$(cat "$f")" || warning "Ошибка создания $obj"
      else
        convert_engine "$f" "$db" "$obj"
        add_on_cluster "$f"
        ch_new "$(cat "$f")" || warning "Ошибка создания таблицы $db.$obj"
      fi
    done
  done
  success "DDL применены"
}

# ── Шаг 3: Перенос данных + MV safe-guard ─────────────────────────────────────
transfer_with_mv_guard() {
  local db="$1" tbl="$2" engine="$3"
  local detached=()
  # 1. DETACH MVs
  local mvs=$(ch_new "SELECT name FROM system.tables WHERE engine LIKE '%MaterializedView%' AND target_database='$db' AND target_table='$tbl'" 2>/dev/null)
  for mv in $mvs; do
    ch_new "DETACH TABLE \`$db\`.\`$mv\`" && detached+=("$mv")
  done

  # Функция очистки (гарантирует ATTACH даже при ошибке)
  local cleanup() {
    for mv in "${detached[@]}"; do
      ch_new "ATTACH TABLE \`$db\`.\`$mv\`" || warning "Не удалось ATTACH MV $db.$mv"
    done
  }
  trap cleanup EXIT

  # 2. Перенос
  log "  Перенос \`$db\`.\`$tbl\` ($engine)..."
  clickhouse-client -h "$OLD_CLUSTER_HOST" -p "$OLD_CLUSTER_PORT" -u "$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" \
    --query "SELECT * FROM \`$db\`.\`$tbl\` FORMAT Native" 2>>"$LOG_FILE" \
  | clickhouse-client -h "$NEW_CLUSTER_HOST" -p "$NEW_CLUSTER_PORT" -u "$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" \
    --query "INSERT INTO \`$db\`.\`$tbl\` FORMAT Native" 2>>"$LOG_FILE"

  local rc=$?
  trap - EXIT
  cleanup
  [ $rc -ne 0 ] && warning "Ошибка переноса $db.$tbl (rc=$rc)" && return 1
  return 0
}

migrate_data() {
  $DRY_RUN && { log "[DRY RUN] Пропуск Шага 3"; return 0; }
  log "=== Шаг 3: Перенос данных ==="
  local dbs=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
  [ -z "$dbs" ] && return 0

  for db in $dbs; do
    local tables=$(ch_old "SELECT name, engine FROM system.tables WHERE database='$db' AND engine IN ('Distributed', 'ReplicatedMergeTree')")
    while IFS=$'\t' read -r tbl eng; do
      [ -z "$tbl" ] && continue
      # Проверка размера
      local size=$(ch_old "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='$db' AND table='$tbl'" 2>/dev/null | awk '{print int($1+0)}')
      size=${size:-0}
      if (( size > SIZE_LIMIT_BYTES )); then
        warning "  ⏭ \`$db\`.\`$tbl\`: >100GB — пропущено (используйте migrate_selective.sh)"
        LARGE_TABLES+=("$db.$tbl")
        continue
      fi
      transfer_with_mv_guard "$db" "$tbl" "$eng"
    done <<< "$tables"
  done
  success "Перенос данных завершён"
}

# ── Шаг 4: Верификация ────────────────────────────────────────────────────────
verify_migration() {
  $DRY_RUN && { log "[DRY RUN] Пропуск Шага 4"; return 0; }
  log "=== Шаг 4: Верификация ==="
  local ok=0 fail=0
  local dbs=$(ch_old "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
  for db in $dbs; do
    local tables=$(ch_old "SELECT name FROM system.tables WHERE database='$db' AND engine IN ('Distributed', 'ReplicatedMergeTree')")
    while read -r tbl; do
      [ -z "$tbl" ] && continue
      # Пропускаем большие
      local size=$(ch_old "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='$db' AND table='$tbl'" 2>/dev/null | awk '{print int($1+0)}')
      (( ${size:-0} > SIZE_LIMIT_BYTES )) && continue

      local c_old=$(ch_old "SELECT count() FROM \`$db\`.\`$tbl\`" 2>/dev/null | tr -d '[:space:]')
      local c_new=$(ch_new "SELECT count() FROM \`$db\`.\`$tbl\`" 2>/dev/null | tr -d '[:space:]')
      if [ "${c_old:-0}" -eq "${c_new:-0}" ]; then success "  ✓ $db.$tbl: ${c_old:-0}"; ((ok++))
      else warning "  ✗ $db.$tbl: old=${c_old:-0} new=${c_new:-0}"; ((fail++)); fi
    done <<< "$tables"
  done
  log "Итого: $ok OK, $fail расхождений"
}

# ── Точка входа ───────────────────────────────────────────────────────────────
LARGE_TABLES=()
main() {
  log "=================================================="
  log "  Миграция ClickHouse ($([ $DRY_RUN = true ] && echo 'DRY RUN' || echo 'LIVE'))"
  log "=================================================="
  check_dependencies; check_connections; check_macros
  export_ddl; apply_ddl; migrate_data; verify_migration
  [ ${#LARGE_TABLES[@]} -gt 0 ] && warning "⚠️ Пропущены большие таблицы (>100GB): ${LARGE_TABLES[*]}"
  success "Готово. Лог: $LOG_FILE"
}
main