#!/bin/bash
# =============================================================================
# migrate_selective.sh - Точечная миграция больших/выборочных таблиц
# Только bash + clickhouse-client
# =============================================================================

# ── Парсинг аргументов ────────────────────────────────────────────────────────
usage() {
  echo "Usage: $0 --db DB --table TABLE [--engine TYPE] --old-host HOST --new-host HOST [--user U] [--password P]"
  echo "         [--chunk-col COL] [--chunk-size ROWS] [--timeout SEC] [--max-mem BYTES]"
  exit 1
}

DB="" TABLES="" ENGINE="ReplicatedMergeTree" OLD_HOST="" NEW_HOST="" USER="default" PASS=""
CHUNK_COL="" CHUNK_SIZE=0 TIMEOUT=7200 MAX_MEM="30000000000" # 30GB default
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --db) DB="$2"; shift 2;; --table) TABLES="$2"; shift 2;; --engine) ENGINE="$2"; shift 2;;
    --old-host) OLD_HOST="$2"; shift 2;; --new-host) NEW_HOST="$2"; shift 2;;
    --user) USER="$2"; shift 2;; --password) PASS="$2"; shift 2;;
    --chunk-col) CHUNK_COL="$2"; shift 2;; --chunk-size) CHUNK_SIZE="$2"; shift 2;;
    --timeout) TIMEOUT="$2"; shift 2;; --max-mem) MAX_MEM="$2"; shift 2;;
    --dry-run) DRY_RUN=true; shift;; --help|-h) usage;; *) echo "Unknown: $1"; usage;;
  esac
done
[[ -z "$DB" || -z "$TABLES" || -z "$OLD_HOST" || -z "$NEW_HOST" ]] && usage

# ── Утилиты ───────────────────────────────────────────────────────────────────
LOG="/var/log/clickhouse-selective.log"
ch_q() {
  local h="$1" q="$2"
  clickhouse-client -h "$h" -p 9000 -u "$USER" --password="$PASS" \
    --receive_timeout="$TIMEOUT" --send_timeout="$TIMEOUT" \
    --max_memory_usage="$MAX_MEM" --multiquery --query="$q" 2>>"$LOG"
}
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG"; }

# ── Ядро миграции ─────────────────────────────────────────────────────────────
migrate_table() {
  local db="$1" tbl="$2" eng="$3"
  log "📥 Миграция: $db.$tbl (Engine: $eng)"

  # 1. DETACH MV
  local mvs=$(ch_q "$NEW_HOST" "SELECT name FROM system.tables WHERE engine LIKE '%MaterializedView%' AND target_database='$db' AND target_table='$tbl'")
  for mv in $mvs; do ch_q "$NEW_HOST" "DETACH TABLE \`$db\`.\`$mv\`" 2>>"$LOG"; done

  # 2. Перенос
  if $DRY_RUN; then log "  [DRY RUN] Пропуск передачи"; return 0; fi

  if [ -n "$CHUNK_COL" ] && [ "$CHUNK_SIZE" -gt 0 ]; then
    # Чанкингованный перенос по числовой колонке
    local min=$(ch_q "$OLD_HOST" "SELECT min($CHUNK_COL) FROM $db.$tbl" | tr -d '[:space:]')
    local max=$(ch_q "$OLD_HOST" "SELECT max($CHUNK_COL) FROM $db.$tbl" | tr -d '[:space:]')
    local cur=$min
    while (( $(echo "$cur <= $max" | bc -l) )); do
      local next=$(( cur + CHUNK_SIZE ))
      local where="$CHUNK_COL BETWEEN $cur AND $next"
      log "  Чанк: $where"
      clickhouse-client -h "$OLD_HOST" -u "$USER" --password="$PASS" --receive_timeout="$TIMEOUT" \
        --query "SELECT * FROM $db.$tbl WHERE $where FORMAT Native" \
      | clickhouse-client -h "$NEW_HOST" -u "$USER" --password="$PASS" --send_timeout="$TIMEOUT" \
        --query "INSERT INTO $db.$tbl FORMAT Native"
      cur=$next
    done
  else
    # Нативный стриминг (ClickHouse сам чанкует на TCP уровне)
    clickhouse-client -h "$OLD_HOST" -u "$USER" --password="$PASS" --receive_timeout="$TIMEOUT" \
      --query "SELECT * FROM $db.$tbl FORMAT Native" \
    | clickhouse-client -h "$NEW_HOST" -u "$USER" --password="$PASS" --send_timeout="$TIMEOUT" \
      --query "INSERT INTO $db.$tbl FORMAT Native"
  fi
  local rc=$?

  # 3. ATTACH MV (гарантированно)
  for mv in $mvs; do ch_q "$NEW_HOST" "ATTACH TABLE \`$db\`.\`$mv\`" 2>>"$LOG"; done
  [ $rc -ne 0 ] && log "  ❌ Ошибка переноса $db.$tbl (rc=$rc)" && return 1
  log "  ✅ $db.$tbl перенесена"
}

# ── Запуск ────────────────────────────────────────────────────────────────────
log "=================================================="
log "  Selective Migration ($([ $DRY_RUN = true ] && echo 'DRY RUN' || echo 'LIVE'))"
log "=================================================="

IFS=',' read -ra TBL_LIST <<< "$TABLES"
for t in "${TBL_LIST[@]}"; do
  t=$(echo "$t" | xargs) # trim
  migrate_table "$DB" "$t" "$ENGINE"
done
log "🏁 Завершено."