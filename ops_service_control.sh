#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <status|restart> <service-name>" >&2
  exit 2
fi

ACTION="$1"
SERVICE_NAME="$2"
SYSTEMCTL_BIN="${OPS_SYSTEMCTL_BIN:-systemctl}"

case "$ACTION" in
  status)
    SYSTEMCTL_ACTION="is-active"
    ;;
  restart)
    SYSTEMCTL_ACTION="restart"
    ;;
  *)
    echo "unsupported action: $ACTION" >&2
    exit 2
    ;;
esac

if [[ -z "${OPS_ALLOWED_SERVICE_NAMES:-}" ]]; then
  echo "OPS_ALLOWED_SERVICE_NAMES is required" >&2
  exit 2
fi

ALLOWED=0
IFS=',' read -r -a SERVICE_LIST <<< "${OPS_ALLOWED_SERVICE_NAMES}"
for item in "${SERVICE_LIST[@]}"; do
  if [[ "${item// /}" == "$SERVICE_NAME" ]]; then
    ALLOWED=1
    break
  fi
done

if [[ "$ALLOWED" -ne 1 ]]; then
  echo "service not allowed: $SERVICE_NAME" >&2
  exit 2
fi

exec "$SYSTEMCTL_BIN" "$SYSTEMCTL_ACTION" -- "$SERVICE_NAME"
