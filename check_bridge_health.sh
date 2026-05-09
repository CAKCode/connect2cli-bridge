#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
. "$SCRIPT_DIR/bridge_env.sh"
load_bridge_runtime_env "$SCRIPT_DIR" || exit 1

: "${HEALTH_TIMEOUT_SEC:=10}"

python3 - "$HOST" "$PORT" "$HEALTH_TIMEOUT_SEC" "$BRIDGE_BASIC_AUTH" "$BRIDGE_TOKEN" <<'PY'
import base64
import json
import sys
import time
import urllib.request

host = sys.argv[1]
port = int(sys.argv[2])
timeout_sec = int(sys.argv[3])
basic_auth = sys.argv[4]
token = sys.argv[5]

def api_base():
    base_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return f"http://{base_host}:{port}"

def headers():
    out = {}
    if basic_auth:
        raw = base64.b64encode(basic_auth.encode("utf-8")).decode("ascii")
        out["Authorization"] = f"Basic {raw}"
    elif token:
        out["Authorization"] = f"Bearer {token}"
    return out

def fetch_json(path: str):
    req = urllib.request.Request(f"{api_base()}{path}", headers=headers())
    with urllib.request.urlopen(req, timeout=3) as resp:
        return json.loads(resp.read().decode("utf-8"))

deadline = time.time() + timeout_sec
last_error = None
while time.time() < deadline:
    try:
        root = fetch_json("/")
        if root.get("ok") is True:
            break
        last_error = f"unexpected root payload: {root}"
    except Exception as exc:
        last_error = str(exc)
        time.sleep(1)
else:
    print(f"bridge health failed: {last_error}", file=sys.stderr)
    raise SystemExit(1)

bots = fetch_json("/api/bots")
processing = 0
pending = 0
for folder in (".scheduled-messages/processing", ".scheduled-messages/pending"):
    try:
        import pathlib
        count = sum(1 for item in pathlib.Path(folder).glob("*.json"))
        if folder.endswith("processing"):
            processing = count
        else:
            pending = count
    except Exception:
        pass

print("bridge root: ok")
print(f"bots: {len(bots)}")
for bot in bots:
    print(
        "bot"
        f" name={bot.get('name')}"
        f" id={bot.get('id')}"
        f" status={bot.get('status')}"
        f" enabled={bot.get('enabled')}"
        f" sessions={len(bot.get('sessions') or [])}"
    )
print(f"scheduled processing={processing} pending={pending}")
PY
