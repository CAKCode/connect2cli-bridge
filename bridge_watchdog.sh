#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
. "$SCRIPT_DIR/bridge_env.sh"
load_bridge_runtime_env "$SCRIPT_DIR" || exit 1
export_bridge_runtime_env

CONTROL_STATE_ROOT="$(prepare_bridge_state_root "$SCRIPT_DIR" "${BRIDGE_CONTROL_ROOT:-}" "BRIDGE_CONTROL_ROOT")" || exit 1
BRIDGE_CONTROL_ROOT="$CONTROL_STATE_ROOT"
export BRIDGE_CONTROL_ROOT

PID_FILE="$CONTROL_STATE_ROOT/.bridge.pid"
GUARD_PID_FILE="$CONTROL_STATE_ROOT/.bridge.guard.pid"
LOG_FILE="$CONTROL_STATE_ROOT/bridge.log"
RESTART_STATE_FILE="$CONTROL_STATE_ROOT/.bridge.watchdog.restarts"
INSTANCE_STATE_ROOT="$(prepare_bridge_state_root "$SCRIPT_DIR" "${BRIDGE_RUNTIME_ROOT:-}" "BRIDGE_RUNTIME_ROOT")" || exit 1
SHUTDOWN_REASON_FILE="$INSTANCE_STATE_ROOT/.bridge.shutdown-reason.json"

if [ -L "$LOG_FILE" ] || { [ -e "$LOG_FILE" ] && [ ! -f "$LOG_FILE" ]; }; then
  echo "watchdog failed: bridge log is not a regular file: $LOG_FILE" >&2
  exit 1
fi
if [ ! -e "$LOG_FILE" ] && ! (umask 077 && : > "$LOG_FILE"); then
  echo "watchdog failed: cannot create bridge log: $LOG_FILE" >&2
  exit 1
fi
chmod 600 "$LOG_FILE" 2>/dev/null || true

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
  write_bridge_pid_file "$PID_FILE" "$target_pid"
}

pid_is_running() {
  target_pid="$1"
  bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" false bridge "$target_pid"
}

read_bridge_pid() {
  read_bridge_pid_file "$PID_FILE" 2>/dev/null || true
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
  BRIDGE_HEALTHCHECK_BASIC_AUTH="${BRIDGE_BASIC_AUTH:-}" \
  BRIDGE_HEALTHCHECK_TOKEN="${BRIDGE_TOKEN:-}" \
  python3 - "$HOST" "$PORT" "$BRIDGE_WATCHDOG_HEALTH_TIMEOUT_SEC" <<'PY'
import base64
import json
import os
import sys
import urllib.request

host = sys.argv[1]
port = int(sys.argv[2])
timeout_sec = int(sys.argv[3])
basic_auth = os.environ.get("BRIDGE_HEALTHCHECK_BASIC_AUTH", "")
token = os.environ.get("BRIDGE_HEALTHCHECK_TOKEN", "")

base_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
url = f"http://{base_host}:{port}/livez"
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
  reloaded_control_root="$(resolve_bridge_root_path "$SCRIPT_DIR" "${BRIDGE_CONTROL_ROOT:-}")" || return 1
  reloaded_instance_root="$(resolve_bridge_root_path "$SCRIPT_DIR" "${BRIDGE_RUNTIME_ROOT:-}")" || return 1
  if [ "$reloaded_control_root" != "$CONTROL_STATE_ROOT" ] || [ "$reloaded_instance_root" != "$INSTANCE_STATE_ROOT" ]; then
    log "runtime root configuration changed; run start.sh to apply it"
    return 1
  fi
  if command -v setsid >/dev/null 2>&1; then
    setsid "$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py" </dev/null >> "$LOG_FILE" 2>&1 &
  else
    "$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py" </dev/null >> "$LOG_FILE" 2>&1 &
  fi
  child_pid=$!
  if ! write_bridge_pid "$child_pid"; then
    kill "$child_pid" 2>/dev/null || true
    wait_for_bridge_exit "$child_pid" 50 || true
    log "failed to write bridge PID file; child stopped pid=$child_pid"
    return 1
  fi
  log "started bridge pid=$child_pid"
}

stop_bridge_process() {
  target_pid="$1"
  stop_reason="${2:-watchdog_restart}"
  [ -n "$target_pid" ] || return 0
  if ! kill -0 "$target_pid" 2>/dev/null; then
    return 0
  fi
  if ! pid_is_running "$target_pid"; then
    log "refusing to stop unrelated process pid=$target_pid"
    return 1
  fi
  write_bridge_shutdown_reason_file "$SHUTDOWN_REASON_FILE" "$stop_reason" || {
    log "failed to write shutdown reason: $SHUTDOWN_REASON_FILE"
  }
  kill "$target_pid" 2>/dev/null || true
  wait_for_bridge_exit "$target_pid" 50 || {
    log "bridge pid=$target_pid did not exit after SIGTERM, sending SIGKILL"
    kill -9 "$target_pid" 2>/dev/null || true
    wait_for_bridge_exit "$target_pid" 25 || true
  }
}

record_restart() {
  now_ts=$(date +%s)
  tmp_file="$(mktemp "${RESTART_STATE_FILE}.tmp.XXXXXX")" || return 1
  chmod 600 "$tmp_file" 2>/dev/null || {
    rm -f "$tmp_file"
    return 1
  }
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
  mv -f "$tmp_file" "$RESTART_STATE_FILE"
  printf '%s\n' "$count"
}

cleanup() {
  current_guard_pid="$(read_bridge_pid_file "$GUARD_PID_FILE" 2>/dev/null || true)"
  if [ "$current_guard_pid" = "$$" ]; then
    rm -f "$GUARD_PID_FILE"
  fi
}

shutdown_watchdog() {
  shutdown_status="${1:-0}"
  trap - EXIT HUP INT TERM
  bridge_pid="$(read_bridge_pid)"
  if pid_is_running "$bridge_pid"; then
    stop_bridge_process "$bridge_pid" "watchdog_exit" || true
  fi
  cleanup
  exit "$shutdown_status"
}

trap 'shutdown_watchdog "$?"' EXIT
trap 'shutdown_watchdog 129' HUP
trap 'shutdown_watchdog 130' INT
trap 'shutdown_watchdog 143' TERM

if ! write_bridge_pid_file "$GUARD_PID_FILE" "$$"; then
  log "failed to write watchdog PID file"
  exit 1
fi
rm -f "$RESTART_STATE_FILE"

fail_count=0
startup_grace_until=0

start_bridge_process || exit 1
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

  if ! stop_bridge_process "$bridge_pid"; then
    log "restart aborted because bridge PID ownership could not be verified"
    exit 1
  fi
  sleep "$BRIDGE_WATCHDOG_RESTART_BACKOFF_SEC"
  start_bridge_process || exit 1
  startup_grace_until=$(( $(date +%s) + BRIDGE_WATCHDOG_STARTUP_GRACE_SEC ))
  fail_count=0
done
