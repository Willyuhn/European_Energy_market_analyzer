#!/usr/bin/env bash
# Daily ENTSO-E import + summary rebuild for Hostinger.
# Safe to invoke concurrently (flock) and repeatedly (idempotent OK marker).
set -euo pipefail

cd "$(dirname "$0")"
export TZ="${TZ:-Europe/Berlin}"

LOG_DIR="$PWD/logs"
mkdir -p "$LOG_DIR"

DAY="$(date +%Y%m%d)"
LOG="$LOG_DIR/daily_${DAY}.log"
LOCK="$LOG_DIR/daily.lock"
OK="$LOG_DIR/daily_ok_${DAY}"

# Already succeeded today → no-op (Node catch-up / GHA / double cron).
if [[ -f "$OK" ]]; then
  echo "[daily] $(date -u -Iseconds) already OK for ${DAY} — skip" >>"$LOG"
  exit 0
fi

# Exclusive lock: only one import at a time across Node workers / SSH / cron.
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "[daily] $(date -u -Iseconds) another instance holds the lock — skip" >>"$LOG"
  exit 0
fi

exec >>"$LOG" 2>&1
echo "======== [daily] start $(date -u -Iseconds) day=${DAY} ========"

set -a
# shellcheck disable=SC1091
source .env
set +a
# shellcheck disable=SC1091
source venv/bin/activate

START="$(date -u -d '3 days ago' +%F)"
END="$(date -u -d 'yesterday' +%F)"
echo "[daily] import ${START}..${END}"

python etl/entsoe_import.py \
  --all-zones \
  --start "$START" \
  --end "$END" \
  --chunk-days 4 \
  --sleep-between-zones 0.2

echo "[daily] rebuild summaries"
python etl/build_summaries.py

# Mark success only after both steps completed.
date -u -Iseconds >"$OK"
echo "======== [daily] done $(date -u -Iseconds) ========"
