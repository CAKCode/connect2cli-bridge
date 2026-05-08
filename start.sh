#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  . "$SCRIPT_DIR/.env"
  set +a
fi

BRIDGE_BIND="${BRIDGE_BIND:-127.0.0.1:6288}"
HOST="${BRIDGE_BIND%:*}"
PORT="${BRIDGE_BIND##*:}"

PID_FILE="$SCRIPT_DIR/.bridge.pid"
LOG_FILE="$SCRIPT_DIR/bridge.log"
PREV_LOG_FILE="$SCRIPT_DIR/bridge.log.prev"
STOPPED_PIDS=""

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

port_probe() {
  python3 -m workspace_bridge.port_check "$HOST" "$PORT" >/dev/null 2>&1
}

describe_port_probe() {
  python3 -m workspace_bridge.port_check --describe "$HOST" "$PORT" 2>&1
}

wait_for_port_release() {
  tries="${1:-50}"
  while [ "$tries" -gt 0 ]; do
    port_probe
    status=$?
    if [ "$status" -eq 0 ]; then
      return 0
    fi
    if [ "$status" -ne 1 ]; then
      return 2
    fi
    sleep 0.2
    tries=$((tries - 1))
  done
  port_probe
  status=$?
  if [ "$status" -eq 0 ]; then
    return 0
  fi
  if [ "$status" -eq 1 ]; then
    return 1
  fi
  return 2
}

stop_existing_bridges() {
  if ! command -v pgrep >/dev/null 2>&1; then
    return
  fi
  pgrep -f 'python3( .*)?-m aiohttp\.web .*workspace_bridge\.service:load_app' 2>/dev/null | while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    [ "$pid" != "$$" ] || continue
    cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)
    cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)

    case "$cmdline" in
      *"workspace_bridge.service:load_app"*)
        if [ "$cwd" = "$SCRIPT_DIR" ]; then
          kill "$pid" 2>/dev/null || true
          echo "$pid"
        fi
        ;;
    esac
  done
}

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE" 2>/dev/null)
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
    kill "$OLD_PID" 2>/dev/null
    STOPPED_PIDS="$STOPPED_PIDS $OLD_PID"
  fi
  rm -f "$PID_FILE"
fi

if [ -f "$LOG_FILE" ]; then
  mv "$LOG_FILE" "$PREV_LOG_FILE" 2>/dev/null || cp "$LOG_FILE" "$PREV_LOG_FILE" 2>/dev/null || true
fi

MORE_PIDS=$(stop_existing_bridges)
STOPPED_PIDS="$STOPPED_PIDS $MORE_PIDS"

for pid in $STOPPED_PIDS; do
  wait_for_pid_exit "$pid" 50 || true
done

wait_for_port_release 50
PORT_STATUS=$?
if [ "$PORT_STATUS" -eq 1 ]; then
  echo "start failed: port ${HOST}:${PORT} is still in use"
  describe_port_probe
  exit 1
fi
if [ "$PORT_STATUS" -ne 0 ]; then
  echo "start failed: unable to probe port ${HOST}:${PORT}"
  describe_port_probe
  exit 1
fi

: > "$LOG_FILE"

if command -v setsid >/dev/null 2>&1; then
  nohup setsid python3 -m aiohttp.web -H "$HOST" -P "$PORT" workspace_bridge.service:load_app </dev/null > "$LOG_FILE" 2>&1 &
else
  nohup python3 -m aiohttp.web -H "$HOST" -P "$PORT" workspace_bridge.service:load_app </dev/null > "$LOG_FILE" 2>&1 &
fi

echo $! > "$PID_FILE"
sleep 2
PID=$(cat "$PID_FILE" 2>/dev/null)
API_BASE="http://${HOST}:${PORT}"

if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
  echo "started (PID: $PID)"
  echo "api: ${API_BASE}"
  echo "api auth: localhost only"
  tail -5 "$LOG_FILE"
else
  echo "start failed"
  cat "$LOG_FILE"
  exit 1
fi
