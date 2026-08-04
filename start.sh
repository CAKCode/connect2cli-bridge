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

# Linux flock locks follow the shared open-file description, so the lock set by
# this short Python child remains held by this shell's fd 9 until start.sh exits.
if ! exec 9< "$CONTROL_STATE_ROOT"; then
  echo "start failed: cannot open BRIDGE_CONTROL_ROOT for locking: $CONTROL_STATE_ROOT" >&2
  exit 1
fi
if ! python3 - 9 <<'PY'
import fcntl
import sys

fd = int(sys.argv[1])
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except (BlockingIOError, OSError):
    raise SystemExit(1)
PY
then
  echo "start failed: another start.sh is already running for BRIDGE_CONTROL_ROOT=$CONTROL_STATE_ROOT" >&2
  exit 1
fi

CONTROL_STATE_IS_CUSTOM="false"
if [ "$CONTROL_STATE_ROOT" != "$SCRIPT_DIR" ]; then
  CONTROL_STATE_IS_CUSTOM="true"
fi
ALLOW_LEGACY_PROCESS_MATCH="false"
if [ "$CONTROL_STATE_IS_CUSTOM" = "false" ]; then
  ALLOW_LEGACY_PROCESS_MATCH="true"
fi

PID_FILE="$CONTROL_STATE_ROOT/.bridge.pid"
GUARD_PID_FILE="$CONTROL_STATE_ROOT/.bridge.guard.pid"
LOG_FILE="$CONTROL_STATE_ROOT/bridge.log"
PREV_LOG_FILE="$CONTROL_STATE_ROOT/bridge.log.prev"
INSTANCE_STATE_ROOT="$(prepare_bridge_state_root "$SCRIPT_DIR" "${BRIDGE_RUNTIME_ROOT:-}" "BRIDGE_RUNTIME_ROOT")" || exit 1
SHUTDOWN_REASON_FILE="$INSTANCE_STATE_ROOT/.bridge.shutdown-reason.json"
STOPPED_PIDS=""
LAUNCHED_PID=""
LAUNCHED_KIND=""

: "${BRIDGE_WATCHDOG_ENABLED:=true}"

is_truthy() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

wait_for_pid_exit() {
  target_pid="$1"
  [ -n "$target_pid" ] || return 0
  tries="${2:-50}"
  while [ "$tries" -gt 0 ]; do
    if ! kill -0 "$target_pid" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
    tries=$((tries - 1))
  done
  return 1
}

wait_for_started_bridge_pid() {
  tries="${1:-50}"
  while [ "$tries" -gt 0 ]; do
    target_pid=$(read_bridge_pid_file "$PID_FILE" 2>/dev/null || true)
    if bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" "$ALLOW_LEGACY_PROCESS_MATCH" bridge "$target_pid"; then
      printf '%s\n' "$target_pid"
      return 0
    fi
    sleep 0.2
    tries=$((tries - 1))
  done
  return 1
}

port_is_available() {
  python3 - "$HOST" "$PORT" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

family = socket.AF_INET6 if ":" in host and host != "localhost" else socket.AF_INET
sock = socket.socket(family, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    sock.bind((host, port))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
}

wait_for_port_release() {
  tries="${1:-50}"
  while [ "$tries" -gt 0 ]; do
    if port_is_available; then
      return 0
    fi
    sleep 0.2
    tries=$((tries - 1))
  done
  return 1
}

stop_pid_from_file() {
  target_file="$1"
  target_kind="$2"
  stop_reason="$3"
  if [ ! -f "$target_file" ]; then
    return
  fi
  target_pid=$(read_bridge_pid_file "$target_file" 2>/dev/null || true)
  if [ -z "$target_pid" ]; then
    echo "ignoring invalid PID file: $target_file" >&2
    rm -f "$target_file"
    return
  fi
  if kill -0 "$target_pid" 2>/dev/null; then
    if ! bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" "$ALLOW_LEGACY_PROCESS_MATCH" "$target_kind" "$target_pid"; then
      echo "refusing to stop unrelated process pid=$target_pid from $target_file" >&2
      rm -f "$target_file"
      return
    fi
    if [ "$target_kind" = "bridge" ]; then
      write_bridge_shutdown_reason_file "$SHUTDOWN_REASON_FILE" "$stop_reason" || {
        echo "failed to write shutdown reason: $SHUTDOWN_REASON_FILE" >&2
      }
    fi
    kill "$target_pid" 2>/dev/null || true
    STOPPED_PIDS="$STOPPED_PIDS $target_pid"
  fi
  rm -f "$target_file"
}

stop_existing_watchdogs() {
  if is_truthy "$CONTROL_STATE_IS_CUSTOM"; then
    return
  fi
  if ! command -v pgrep >/dev/null 2>&1; then
    return
  fi
  pgrep -f 'bridge_watchdog\.sh' 2>/dev/null | while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    [ "$pid" != "$$" ] || continue
    if bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" true watchdog "$pid"; then
      kill "$pid" 2>/dev/null || true
      echo "$pid"
    fi
  done
}

stop_existing_bridges() {
  if is_truthy "$CONTROL_STATE_IS_CUSTOM"; then
    return
  fi
  if ! command -v pgrep >/dev/null 2>&1; then
    return
  fi
  pgrep -f 'bridge\.py' 2>/dev/null | while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    [ "$pid" != "$$" ] || continue
    if bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" true bridge "$pid"; then
      kill "$pid" 2>/dev/null || true
      echo "$pid"
    fi
  done
}

stop_pid_from_file "$GUARD_PID_FILE" watchdog "start.sh_restart"
stop_pid_from_file "$PID_FILE" bridge "start.sh_restart"

MORE_GUARD_PIDS=$(stop_existing_watchdogs)
STOPPED_PIDS="$STOPPED_PIDS $MORE_GUARD_PIDS"

MORE_PIDS=$(stop_existing_bridges)
STOPPED_PIDS="$STOPPED_PIDS $MORE_PIDS"

for pid in $STOPPED_PIDS; do
  wait_for_pid_exit "$pid" 50 || true
done

if [ -L "$LOG_FILE" ] || { [ -e "$LOG_FILE" ] && [ ! -f "$LOG_FILE" ]; }; then
  echo "start failed: bridge log is not a regular file: $LOG_FILE" >&2
  exit 1
fi
if [ -L "$PREV_LOG_FILE" ] || { [ -e "$PREV_LOG_FILE" ] && [ ! -f "$PREV_LOG_FILE" ]; }; then
  echo "start failed: previous bridge log is not a regular file: $PREV_LOG_FILE" >&2
  exit 1
fi
if [ -f "$LOG_FILE" ]; then
  mv "$LOG_FILE" "$PREV_LOG_FILE" 2>/dev/null || cp "$LOG_FILE" "$PREV_LOG_FILE" 2>/dev/null || true
fi

wait_for_port_release 50 || {
  echo "start failed: port ${HOST}:${PORT} is still in use"
  exit 1
}

if ! (umask 077 && : > "$LOG_FILE"); then
  echo "start failed: cannot create bridge log: $LOG_FILE" >&2
  exit 1
fi
chmod 600 "$LOG_FILE" 2>/dev/null || true

if is_truthy "$BRIDGE_WATCHDOG_ENABLED"; then
  if command -v setsid >/dev/null 2>&1; then
    nohup setsid sh "$SCRIPT_DIR/bridge_watchdog.sh" </dev/null 9<&- >> "$LOG_FILE" 2>&1 &
  else
    nohup sh "$SCRIPT_DIR/bridge_watchdog.sh" </dev/null 9<&- >> "$LOG_FILE" 2>&1 &
  fi
  LAUNCHED_PID=$!
  LAUNCHED_KIND="watchdog"
  if ! write_bridge_pid_file "$GUARD_PID_FILE" "$LAUNCHED_PID"; then
    kill "$LAUNCHED_PID" 2>/dev/null || true
    wait_for_pid_exit "$LAUNCHED_PID" 50 || true
    echo "start failed: cannot write watchdog PID file: $GUARD_PID_FILE" >&2
    exit 1
  fi
else
  rm -f "$GUARD_PID_FILE"
  if command -v setsid >/dev/null 2>&1; then
    nohup setsid "$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py" </dev/null 9<&- > "$LOG_FILE" 2>&1 &
  else
    nohup "$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py" </dev/null 9<&- > "$LOG_FILE" 2>&1 &
  fi
  LAUNCHED_PID=$!
  LAUNCHED_KIND="bridge"
  if ! write_bridge_pid_file "$PID_FILE" "$LAUNCHED_PID"; then
    kill "$LAUNCHED_PID" 2>/dev/null || true
    wait_for_pid_exit "$LAUNCHED_PID" 50 || true
    echo "start failed: cannot write bridge PID file: $PID_FILE" >&2
    exit 1
  fi
fi

sleep 2
PID=$(wait_for_started_bridge_pid 50 || true)
GUARD_PID=$(read_bridge_pid_file "$GUARD_PID_FILE" 2>/dev/null || true)
API_BASE=$(python3 "$SCRIPT_DIR/bridge_runtime_config.py" 2>/dev/null || printf 'http://%s:%s' "$HOST" "$PORT")

STARTED_PROCESSES_VALID="false"
if bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" "$ALLOW_LEGACY_PROCESS_MATCH" bridge "$PID"; then
  if ! is_truthy "$BRIDGE_WATCHDOG_ENABLED" || bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" "$ALLOW_LEGACY_PROCESS_MATCH" watchdog "$GUARD_PID"; then
    STARTED_PROCESSES_VALID="true"
  fi
fi

if is_truthy "$STARTED_PROCESSES_VALID"; then
  echo "started (PID: $PID)"
  if is_truthy "$BRIDGE_WATCHDOG_ENABLED"; then
    echo "watchdog: $GUARD_PID"
  fi
  echo "api: ${API_BASE}"
  if [ -n "${BRIDGE_TOKEN:-}" ] && [ -n "${BRIDGE_BASIC_AUTH:-}" ]; then
    echo "api auth: token or basic auth enabled"
  elif [ -n "${BRIDGE_BASIC_AUTH:-}" ]; then
    echo "api auth: basic auth enabled"
  elif [ -n "${BRIDGE_TOKEN:-}" ]; then
    echo "api auth: token enabled"
  else
    echo "api auth: localhost only"
  fi
  tail -5 "$LOG_FILE"
else
  echo "start failed"
  if [ -n "$LAUNCHED_PID" ] && bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" false "$LAUNCHED_KIND" "$LAUNCHED_PID"; then
    kill "$LAUNCHED_PID" 2>/dev/null || true
    wait_for_pid_exit "$LAUNCHED_PID" 50 || true
  fi
  FAILED_BRIDGE_PID=$(read_bridge_pid_file "$PID_FILE" 2>/dev/null || true)
  if bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" false bridge "$FAILED_BRIDGE_PID"; then
    kill "$FAILED_BRIDGE_PID" 2>/dev/null || true
    wait_for_pid_exit "$FAILED_BRIDGE_PID" 50 || true
  fi
  rm -f "$GUARD_PID_FILE" "$PID_FILE"
  cat "$LOG_FILE"
  exit 1
fi
