#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

CHAT_KEY="$1"
BOT_CONFIG_ID="${2:-}"
BOT_NAME="${3:-default}"

if [ -z "$CHAT_KEY" ]; then
  echo "usage: sh ./smoke_template_card.sh <chat-key> [bot-config-id] [bot-name]" >&2
  exit 2
fi

CARD_FILE="$(mktemp /tmp/wecom-smoke-button-card.XXXXXX.json)"
STAMP="$(date +%Y%m%d%H%M%S)"

cleanup() {
  rm -f "$CARD_FILE"
}

trap cleanup EXIT INT TERM

cat > "$CARD_FILE" <<EOF
{
  "card_type": "button_interaction",
  "source": {
    "desc": "Codex Smoke",
    "desc_color": 0
  },
  "main_title": {
    "title": "模板卡片 smoke",
    "desc": "点击任意按钮后观察 owner / 非 owner 行为"
  },
  "sub_title_text": "发送时间：$STAMP",
  "button_list": [
    {
      "text": "已收到",
      "style": 1,
      "key": "ack_received"
    },
    {
      "text": "稍后处理",
      "style": 0,
      "key": "handle_later"
    }
  ]
}
EOF

echo "[smoke] sending owner-aware button card..."
if [ -n "$BOT_CONFIG_ID" ]; then
  python3 "$SCRIPT_DIR/send_message.py" \
    --chat-key "$CHAT_KEY" \
    --bot-config-id "$BOT_CONFIG_ID" \
    --bot-name "$BOT_NAME" \
    --msgtype template_card \
    --template-card-file "$CARD_FILE" || exit 1
else
  python3 "$SCRIPT_DIR/send_message.py" \
    --chat-key "$CHAT_KEY" \
    --bot-name "$BOT_NAME" \
    --msgtype template_card \
    --template-card-file "$CARD_FILE" || exit 1
fi

echo "[smoke] sent. now verify in WeCom:"
echo "1. owner clicks once -> card should update"
echo "2. another member clicks once -> card should not update, only receive a hint"
echo
echo "[smoke] useful log filters:"
echo "grep -n 'template_card_event\\|message request accepted via api\\|template card update skipped\\|这是 .* 的卡片' '$SCRIPT_DIR/bridge.log' | tail -n 50"
