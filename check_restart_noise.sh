#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
. "$SCRIPT_DIR/bridge_env.sh"
load_bridge_runtime_env "$SCRIPT_DIR" || exit 1

: "${HEALTH_TIMEOUT_SEC:=10}"
: "${CHECK_TAIL_LINES:=200}"

LOG_FILE="$SCRIPT_DIR/bridge.log"
PREV_LOG_FILE="$SCRIPT_DIR/bridge.log.prev"
NOISE_REGEX='RuntimeError: Event loop is closed|BaseSubprocessTransport.__del__'

echo "[1/3] Restart bridge"
sh "$SCRIPT_DIR/start.sh" || exit 1

echo "[2/3] Wait for health check"
python3 - "$HOST" "$PORT" "$HEALTH_TIMEOUT_SEC" <<'PY'
import json
import sys
import time
import urllib.request

host = sys.argv[1]
port = int(sys.argv[2])
timeout_sec = int(sys.argv[3])
base_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
url = f"http://{base_host}:{port}/"
deadline = time.time() + timeout_sec
last_error = None

while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            if payload.get("ok") is True:
                print(f"health ok: {url}")
                raise SystemExit(0)
            last_error = f"unexpected payload: {payload}"
    except Exception as exc:
        last_error = str(exc)
        time.sleep(1)

print(f"health check failed: {last_error}", file=sys.stderr)
raise SystemExit(1)
PY

echo "[3/3] Scan restart logs for subprocess/event-loop noise"
noise_found=0

check_noise_file() {
  target="$1"
  if [ ! -f "$target" ]; then
    return 0
  fi
  matches=$(tail -n "$CHECK_TAIL_LINES" "$target" | grep -En "$NOISE_REGEX" || true)
  if [ -n "$matches" ]; then
    echo "noise detected in $(basename "$target"):"
    echo "$matches"
    noise_found=1
  fi
}

check_noise_file "$PREV_LOG_FILE"
check_noise_file "$LOG_FILE"

if [ "$noise_found" -eq 0 ]; then
  echo "restart noise check passed: no matching patterns found"
  exit 0
fi

echo "restart noise check failed"
exit 1
