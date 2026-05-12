#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
. "$SCRIPT_DIR/bridge_env.sh"
load_bridge_runtime_env "$SCRIPT_DIR" || exit 1

export HOST PORT BRIDGE_TOKEN BRIDGE_BASIC_AUTH WORK_DIR MAX_JSON_BODY MAX_UPLOAD_SIZE FILE_SEND_ROOTS
export BRIDGE_BIND BRIDGE_HOST BRIDGE_PORT
export BRIDGE_SHARED_RUNTIME_ROOT
export BRIDGE_RUNTIME_ROOT
export MAX_INBOUND_IMAGE_SIZE MAX_INBOUND_FILE_SIZE
export SESSION_LEASE_TTL HTTP_PROXY HTTPS_PROXY NO_PROXY
export MEDIA_CONNECT_TIMEOUT MEDIA_TOTAL_TIMEOUT LOCAL_FILE_SEND_QUEUE_ROOT LOCAL_FILE_SEND_POLL_MS
export LOCAL_FILE_SEND_RESULT_TIMEOUT_MS
export CODEX_EXEC_MODE MAX_CONCURRENT_CODEX_RUNS
export SUBPROCESS_STREAM_LIMIT SUBPROCESS_STREAM_READ_SIZE SUBPROCESS_STREAM_MAX_LINE
export WEBSOCKET_SEND_TIMEOUT_SEC STATUS_STREAM_INTERVAL_SEC STATUS_SEND_TIMEOUT_SEC STATUS_SEND_LOCK_TIMEOUT_SEC
export REPLY_IDLE_FALLBACK_SEC REPLY_MAX_AGE_FALLBACK_SEC PROACTIVE_STATUS_INTERVAL_SEC
export SCHEDULE_POLL_MS
export SCHEDULE_DEFINITION_POLL_MS SCHEDULE_DEFINITION_LEASE_TTL_MS
export PROACTIVE_SEND_ACK_TIMEOUT_SEC
export SCHEDULE_PROCESSING_RETRY_MS
export SCHEDULE_ORPHAN_TTL_MS
export BOTS_FILE WECOM_BOOTSTRAP_BOTS_JSON WECOM_BOOTSTRAP_BOTS_JSON_FILE
export WECOM_BOT_CONFIG_ID WECOM_BOT_NAME WECOM_BOT_ID WECOM_BOT_SECRET_FILE
export WECOM_BOT_WORK_DIR WECOM_BOT_WELCOME WECOM_BOT_GROUP_SESSION_MODE WECOM_BOT_ENABLED

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

wait_for_port_release 50 || {
  echo "start failed: port ${HOST}:${PORT} is still in use"
  exit 1
}

: > "$LOG_FILE"

if command -v setsid >/dev/null 2>&1; then
  nohup setsid python3 "$SCRIPT_DIR/bridge.py" </dev/null > "$LOG_FILE" 2>&1 &
else
  nohup python3 "$SCRIPT_DIR/bridge.py" </dev/null > "$LOG_FILE" 2>&1 &
fi

echo $! > "$PID_FILE"
sleep 2
PID=$(cat "$PID_FILE" 2>/dev/null)
API_BASE=$(python3 "$SCRIPT_DIR/bridge_runtime_config.py" 2>/dev/null || printf 'http://%s:%s' "$HOST" "$PORT")

if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
  echo "started (PID: $PID)"
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
  cat "$LOG_FILE"
  exit 1
fi
