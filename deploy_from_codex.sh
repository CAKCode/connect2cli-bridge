#!/bin/sh

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

BOT_ID=""
BOT_SECRET=""
GITHUB_REPO=""
BOT_NAME=""
BRIDGE_BIND="127.0.0.1:9299"
WORK_ROOT=""
SKIP_INSTALL="false"
SKIP_START="false"
DRY_RUN="false"

usage() {
  cat <<'EOF'
Usage:
  sh ./deploy_from_codex.sh \
    --bot-id BOT_ID \
    --bot-secret BOT_SECRET \
    --github-repo GITHUB_REPO_URL \
    [--bot-name BOT_NAME] \
    [--bridge-bind HOST:PORT] \
    [--work-root DIR] \
    [--skip-install] \
    [--skip-start] \
    [--dry-run]

Example:
  sh ./deploy_from_codex.sh \
    --bot-id "your-bot-id" \
    --bot-secret "your-secret" \
    --github-repo "git@github.com:your-org/your-repo.git"
EOF
}

is_truthy() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

run_cmd() {
  if is_truthy "$DRY_RUN"; then
    printf '[dry-run] %s\n' "$*"
    return 0
  fi
  "$@"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bot-id)
      BOT_ID="${2:-}"
      shift 2
      ;;
    --bot-secret)
      BOT_SECRET="${2:-}"
      shift 2
      ;;
    --github-repo)
      GITHUB_REPO="${2:-}"
      shift 2
      ;;
    --bot-name)
      BOT_NAME="${2:-}"
      shift 2
      ;;
    --bridge-bind)
      BRIDGE_BIND="${2:-}"
      shift 2
      ;;
    --work-root)
      WORK_ROOT="${2:-}"
      shift 2
      ;;
    --skip-install)
      SKIP_INSTALL="true"
      shift
      ;;
    --skip-start)
      SKIP_START="true"
      shift
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

[ -n "$BOT_ID" ] || { echo "--bot-id is required" >&2; exit 1; }
[ -n "$BOT_SECRET" ] || { echo "--bot-secret is required" >&2; exit 1; }
[ -n "$GITHUB_REPO" ] || { echo "--github-repo is required" >&2; exit 1; }

require_command git
require_command python3
require_command sh

REPO_SLUG="$(basename "$GITHUB_REPO")"
REPO_SLUG="${REPO_SLUG%.git}"
[ -n "$REPO_SLUG" ] || { echo "failed to derive repository name from: $GITHUB_REPO" >&2; exit 1; }

if [ -z "$BOT_NAME" ]; then
  BOT_NAME="$(printf '%s' "$REPO_SLUG" | tr ' /:@' '_' | tr -cd 'A-Za-z0-9._-')"
fi
[ -n "$BOT_NAME" ] || BOT_NAME="default"

if [ -z "$WORK_ROOT" ]; then
  WORK_ROOT="$SCRIPT_DIR/bot-workdirs"
fi

TARGET_REPO_DIR="$WORK_ROOT/$REPO_SLUG"
SECRET_FILE="$SCRIPT_DIR/.secrets/$BOT_NAME.secret"
ENV_FILE="$SCRIPT_DIR/.env"
ENV_BACKUP_FILE="$SCRIPT_DIR/.env.backup.$(date +%Y%m%d%H%M%S)"

echo "Bridge repo: $SCRIPT_DIR"
echo "Target work repo: $TARGET_REPO_DIR"
echo "Bot name: $BOT_NAME"
echo "Bridge bind: $BRIDGE_BIND"
echo "Dry run: $DRY_RUN"

if [ ! -d "$WORK_ROOT" ]; then
  run_cmd mkdir -p "$WORK_ROOT"
fi

if [ ! -d "$SCRIPT_DIR/.secrets" ]; then
  run_cmd mkdir -p "$SCRIPT_DIR/.secrets"
fi

if [ -d "$TARGET_REPO_DIR/.git" ]; then
  echo "Reusing existing git repo: $TARGET_REPO_DIR"
elif [ -e "$TARGET_REPO_DIR" ]; then
  echo "target path exists but is not a git repo: $TARGET_REPO_DIR" >&2
  exit 1
else
  run_cmd git clone "$GITHUB_REPO" "$TARGET_REPO_DIR"
fi

if ! is_truthy "$SKIP_INSTALL"; then
  run_cmd python3 -m pip install -r "$SCRIPT_DIR/requirements.txt"
fi

if is_truthy "$DRY_RUN"; then
  printf '[dry-run] write secret file: %s\n' "$SECRET_FILE"
else
  umask 077
  printf '%s\n' "$BOT_SECRET" > "$SECRET_FILE"
fi

if [ -f "$ENV_FILE" ]; then
  if is_truthy "$DRY_RUN"; then
    printf '[dry-run] backup %s -> %s\n' "$ENV_FILE" "$ENV_BACKUP_FILE"
  else
    cp "$ENV_FILE" "$ENV_BACKUP_FILE"
  fi
fi

if is_truthy "$DRY_RUN"; then
  printf '[dry-run] write .env: %s\n' "$ENV_FILE"
else
  cat > "$ENV_FILE" <<EOF
BRIDGE_BIND=$BRIDGE_BIND
WORK_DIR=$TARGET_REPO_DIR
CODEX_EXEC_MODE=host
BRIDGE_WATCHDOG_ENABLED=true

WECOM_BOT_CONFIG_ID=$BOT_NAME
WECOM_BOT_NAME=$BOT_NAME
WECOM_BOT_ID=$BOT_ID
WECOM_BOT_SECRET_FILE=$SECRET_FILE
WECOM_BOT_WORK_DIR=$TARGET_REPO_DIR
WECOM_BOT_GROUP_SESSION_MODE=per-user
WECOM_BOT_ENABLED=true
EOF
fi

if ! is_truthy "$SKIP_START"; then
  run_cmd sh "$SCRIPT_DIR/start.sh"
fi

cat <<EOF

Done.

Summary:
- bridge repo: $SCRIPT_DIR
- target repo: $TARGET_REPO_DIR
- secret file: $SECRET_FILE
- env file: $ENV_FILE

Recommended checks:
- codex login status
- sh ./check_bridge_health.sh
- tail -f ./bridge.log
EOF
