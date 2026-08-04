#!/bin/sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

QUALITY_PYTHON="${BRIDGE_PYTHON:-}"
if [ -z "$QUALITY_PYTHON" ] && [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
  QUALITY_PYTHON="$SCRIPT_DIR/.venv/bin/python"
fi
QUALITY_PYTHON="${QUALITY_PYTHON:-python3}"

"$QUALITY_PYTHON" -m compileall -q \
  bridge.py \
  workspace_bridge \
  tests
"$QUALITY_PYTHON" -m ruff check --select E9,F63,F7,F82 .

for script in ./*.sh; do
  case "$(head -n 1 "$script")" in
    *bash*) bash -n "$script" ;;
    *) sh -n "$script" ;;
  esac
done
