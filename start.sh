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
PREV_LOG_FILE="$SCRIPT_DIR/bridge.log.prev"
STOPPED_PIDS=""

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
    target_pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$target_pid" ] && kill -0 "$target_pid" 2>/dev/null; then
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
  if [ ! -f "$target_file" ]; then
    return
  fi
  target_pid=$(cat "$target_file" 2>/dev/null || true)
  if [ -n "$target_pid" ] && kill -0 "$target_pid" 2>/dev/null; then
    kill "$target_pid" 2>/dev/null || true
    STOPPED_PIDS="$STOPPED_PIDS $target_pid"
  fi
  rm -f "$target_file"
}

stop_existing_watchdogs() {
  if ! command -v pgrep >/dev/null 2>&1; then
    return
  fi
  pgrep -f 'bridge_watchdog\.sh' 2>/dev/null | while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    [ "$pid" != "$$" ] || continue
    cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)
    cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)

    case "$cmdline" in
      *"$SCRIPT_DIR/bridge_watchdog.sh"*)
        kill "$pid" 2>/dev/null || true
        echo "$pid"
        continue
        ;;
    esac

    if [ "$cwd" = "$SCRIPT_DIR" ]; then
      case "$cmdline" in
        *"bridge_watchdog.sh"*)
          kill "$pid" 2>/dev/null || true
          echo "$pid"
          ;;
      esac
    fi
  done
}

stop_existing_bridges() {
  if ! command -v pgrep >/dev/null 2>&1; then
    return
  fi
  pgrep -f 'python3( .*)?bridge\.py' 2>/dev/null | while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    [ "$pid" != "$$" ] || continue
    cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)
    cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)

    case "$cmdline" in
      *"$SCRIPT_DIR/bridge.py"*)
        kill "$pid" 2>/dev/null || true
        echo "$pid"
        continue
        ;;
    esac

    if [ "$cwd" = "$SCRIPT_DIR" ]; then
      case "$cmdline" in
        *"python3 bridge.py"*)
          kill "$pid" 2>/dev/null || true
          echo "$pid"
          ;;
      esac
    fi
  done
}

stop_pid_from_file "$GUARD_PID_FILE"
stop_pid_from_file "$PID_FILE"

MORE_GUARD_PIDS=$(stop_existing_watchdogs)
STOPPED_PIDS="$STOPPED_PIDS $MORE_GUARD_PIDS"

MORE_PIDS=$(stop_existing_bridges)
STOPPED_PIDS="$STOPPED_PIDS $MORE_PIDS"

for pid in $STOPPED_PIDS; do
  wait_for_pid_exit "$pid" 50 || true
done

if [ -f "$LOG_FILE" ]; then
  mv "$LOG_FILE" "$PREV_LOG_FILE" 2>/dev/null || cp "$LOG_FILE" "$PREV_LOG_FILE" 2>/dev/null || true
fi

wait_for_port_release 50 || {
  echo "start failed: port ${HOST}:${PORT} is still in use"
  exit 1
}

: > "$LOG_FILE"

if is_truthy "$BRIDGE_WATCHDOG_ENABLED"; then
  if command -v setsid >/dev/null 2>&1; then
    nohup setsid sh "$SCRIPT_DIR/bridge_watchdog.sh" </dev/null >> "$LOG_FILE" 2>&1 &
  else
    nohup sh "$SCRIPT_DIR/bridge_watchdog.sh" </dev/null >> "$LOG_FILE" 2>&1 &
  fi
  echo $! > "$GUARD_PID_FILE"
else
  rm -f "$GUARD_PID_FILE"
  if command -v setsid >/dev/null 2>&1; then
    nohup setsid python3 "$SCRIPT_DIR/bridge.py" </dev/null > "$LOG_FILE" 2>&1 &
  else
    nohup python3 "$SCRIPT_DIR/bridge.py" </dev/null > "$LOG_FILE" 2>&1 &
  fi
  echo $! > "$PID_FILE"
fi

sleep 2
PID=$(wait_for_started_bridge_pid 50 || true)
GUARD_PID=$(cat "$GUARD_PID_FILE" 2>/dev/null || true)
API_BASE=$(python3 "$SCRIPT_DIR/bridge_runtime_config.py" 2>/dev/null || printf 'http://%s:%s' "$HOST" "$PORT")

if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
  echo "started (PID: $PID)"
  if is_truthy "$BRIDGE_WATCHDOG_ENABLED" && [ -n "$GUARD_PID" ] && kill -0 "$GUARD_PID" 2>/dev/null; then
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
  if is_truthy "$BRIDGE_WATCHDOG_ENABLED" && [ -n "$GUARD_PID" ] && kill -0 "$GUARD_PID" 2>/dev/null; then
    kill "$GUARD_PID" 2>/dev/null || true
    rm -f "$GUARD_PID_FILE"
  fi
  cat "$LOG_FILE"
  exit 1
fi
