#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

LOG_FILE="${BRIDGE_LOG_FILE:-$SCRIPT_DIR/bridge.log}"

if [ ! -f "$LOG_FILE" ]; then
  echo "bridge log not found: $LOG_FILE" >&2
  exit 1
fi

echo "[check] recent template-card smoke signals"
grep -n "template_card_event\|template_card.updated\|template_card.click_rejected_non_owner\|template_card.click_no_auto_update\|message.send_accepted\|template card update skipped" "$LOG_FILE" | tail -n 80 || true
