#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
. "$SCRIPT_DIR/bridge_env.sh"
load_bridge_runtime_env "$SCRIPT_DIR" || exit 1
export_bridge_runtime_env

PID_FILE="$SCRIPT_DIR/.bridge.pid"
GUARD_PID_FILE="$SCRIPT_DIR/.bridge.guard.pid"
LOG_FILE="$SCRIPT_DIR/bridge.log"
RESTART_STATE_FILE="$SCRIPT_DIR/.bridge.watchdog.restarts"
SHUTDOWN_REASON_FILE="$SCRIPT_DIR/.bridge.shutdown-reason.json"

: "${BRIDGE_WATCHDOG_POLL_SEC:=5}"
: "${BRIDGE_WATCHDOG_HEALTH_TIMEOUT_SEC:=5}"
: "${BRIDGE_WATCHDOG_STARTUP_GRACE_SEC:=20}"
: "${BRIDGE_WATCHDOG_FAIL_THRESHOLD:=3}"
: "${BRIDGE_WATCHDOG_RESTART_BACKOFF_SEC:=3}"
: "${BRIDGE_WATCHDOG_RESTART_WINDOW_SEC:=300}"
: "${BRIDGE_WATCHDOG_MAX_RESTART_STREAK:=8}"
: "${BRIDGE_WATCHDOG_COOLDOWN_SEC:=60}"

log() {
  printf '[%s] [WATCHDOG] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >> "$LOG_FILE"
}

write_bridge_pid() {
  target_pid="$1"
  printf '%s\n' "$target_pid" > "$PID_FILE"
}

pid_is_running() {
  target_pid="$1"
  [ -n "$target_pid" ] || return 1
  kill -0 "$target_pid" 2>/dev/null
}

read_bridge_pid() {
  cat "$PID_FILE" 2>/dev/null || true
}

wait_for_bridge_exit() {
  target_pid="$1"
  tries="${2:-50}"
  while [ "$tries" -gt 0 ]; do
    if ! pid_is_running "$target_pid"; then
      return 0
    fi
    sleep 0.2
    tries=$((tries - 1))
  done
  return 1
}

bridge_health_ok() {
  python3 - "$HOST" "$PORT" "$BRIDGE_WATCHDOG_HEALTH_TIMEOUT_SEC" "$BRIDGE_BASIC_AUTH" "$BRIDGE_TOKEN" <<'PY'
import base64
import json
import sys
import urllib.request

host = sys.argv[1]
port = int(sys.argv[2])
timeout_sec = int(sys.argv[3])
basic_auth = sys.argv[4]
token = sys.argv[5]

base_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
url = f"http://{base_host}:{port}/healthz"
headers = {}
if basic_auth:
    raw = base64.b64encode(basic_auth.encode("utf-8")).decode("ascii")
    headers["Authorization"] = f"Basic {raw}"
elif token:
    headers["Authorization"] = f"Bearer {token}"

req = urllib.request.Request(url, headers=headers)
with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
    payload = json.loads(resp.read().decode("utf-8"))
if payload.get("ok") is not True:
    raise SystemExit(1)
PY
}

start_bridge_process() {
  load_bridge_runtime_env "$SCRIPT_DIR" || exit 1
  export_bridge_runtime_env
  if command -v setsid >/dev/null 2>&1; then
    setsid python3 "$SCRIPT_DIR/bridge.py" </dev/null >> "$LOG_FILE" 2>&1 &
  else
    python3 "$SCRIPT_DIR/bridge.py" </dev/null >> "$LOG_FILE" 2>&1 &
  fi
  child_pid=$!
  write_bridge_pid "$child_pid"
  log "started bridge pid=$child_pid"
}

stop_bridge_process() {
  target_pid="$1"
  [ -n "$target_pid" ] || return 0
  if ! pid_is_running "$target_pid"; then
    return 0
  fi
  printf '{"reason":"%s","at":%s}\n' "watchdog_restart" "$(date +%s%3N)" > "$SHUTDOWN_REASON_FILE"
  kill "$target_pid" 2>/dev/null || true
  wait_for_bridge_exit "$target_pid" 50 || {
    log "bridge pid=$target_pid did not exit after SIGTERM, sending SIGKILL"
    kill -9 "$target_pid" 2>/dev/null || true
    wait_for_bridge_exit "$target_pid" 25 || true
  }
}

record_restart() {
  now_ts=$(date +%s)
  tmp_file="${RESTART_STATE_FILE}.$$"
  count=0
  if [ -f "$RESTART_STATE_FILE" ]; then
    while IFS= read -r ts; do
      [ -n "$ts" ] || continue
      if [ $((now_ts - ts)) -le "$BRIDGE_WATCHDOG_RESTART_WINDOW_SEC" ]; then
        printf '%s\n' "$ts" >> "$tmp_file"
        count=$((count + 1))
      fi
    done < "$RESTART_STATE_FILE"
  fi
  printf '%s\n' "$now_ts" >> "$tmp_file"
  count=$((count + 1))
  mv "$tmp_file" "$RESTART_STATE_FILE"
  printf '%s\n' "$count"
}

cleanup() {
  rm -f "$GUARD_PID_FILE"
}

trap 'cleanup; exit 0' INT TERM
trap cleanup EXIT

printf '%s\n' "$$" > "$GUARD_PID_FILE"
rm -f "$RESTART_STATE_FILE"

fail_count=0
startup_grace_until=0

start_bridge_process
startup_grace_until=$(( $(date +%s) + BRIDGE_WATCHDOG_STARTUP_GRACE_SEC ))

while true; do
  sleep "$BRIDGE_WATCHDOG_POLL_SEC"
  now_ts=$(date +%s)
  bridge_pid=$(read_bridge_pid)

  if ! pid_is_running "$bridge_pid"; then
    fail_count=$((fail_count + 1))
    log "bridge pid missing; fail_count=$fail_count threshold=$BRIDGE_WATCHDOG_FAIL_THRESHOLD"
  elif [ "$now_ts" -lt "$startup_grace_until" ]; then
    fail_count=0
    continue
  elif bridge_health_ok; then
    fail_count=0
    continue
  else
    fail_count=$((fail_count + 1))
    log "bridge health check failed; fail_count=$fail_count threshold=$BRIDGE_WATCHDOG_FAIL_THRESHOLD pid=$bridge_pid"
  fi

  if [ "$fail_count" -lt "$BRIDGE_WATCHDOG_FAIL_THRESHOLD" ]; then
    continue
  fi

  restart_count=$(record_restart)
  log "restart triggered; recent_restart_count=$restart_count"
  if [ "$restart_count" -gt "$BRIDGE_WATCHDOG_MAX_RESTART_STREAK" ]; then
    log "restart streak exceeded limit=$BRIDGE_WATCHDOG_MAX_RESTART_STREAK, cooling down ${BRIDGE_WATCHDOG_COOLDOWN_SEC}s"
    sleep "$BRIDGE_WATCHDOG_COOLDOWN_SEC"
  fi

  bridge_pid=$(read_bridge_pid)
  if pid_is_running "$bridge_pid" && bridge_health_ok; then
    fail_count=0
    log "bridge recovered before restart; skip restart pid=$bridge_pid"
    continue
  fi

  stop_bridge_process "$bridge_pid"
  sleep "$BRIDGE_WATCHDOG_RESTART_BACKOFF_SEC"
  start_bridge_process
  startup_grace_until=$(( $(date +%s) + BRIDGE_WATCHDOG_STARTUP_GRACE_SEC ))
  fail_count=0
done
