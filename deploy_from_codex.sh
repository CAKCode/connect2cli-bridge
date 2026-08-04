#!/bin/sh

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

BOT_ID=""
BOT_SECRET=""
BOT_SECRET_SOURCE_FILE=""
BOT_SECRET_ARG_SET="false"
BOT_SECRET_FILE_ARG_SET="false"
SECRET_TMP_FILE=""
ENV_TMP_FILE=""
GITHUB_REPO=""
BOT_NAME=""
BRIDGE_BIND="127.0.0.1:9299"
WORK_ROOT=""
SKIP_INSTALL="false"
SKIP_START="false"
DRY_RUN="false"
BRIDGE_PYTHON="${BRIDGE_PYTHON:-}"

usage() {
  cat <<'EOF'
Usage:
  sh ./deploy_from_codex.sh \
    --bot-id BOT_ID \
    --bot-secret-file SECRET_FILE \
    --github-repo GITHUB_REPO_URL \
    [--bot-name BOT_NAME] \
    [--bridge-bind HOST:PORT] \
    [--work-root DIR] \
    [--skip-install] \
    [--skip-start] \
    [--dry-run]

Exactly one secret input is required:
  --bot-secret-file SECRET_FILE  Read the secret from a protected file (recommended).
  --bot-secret BOT_SECRET        Inline compatibility option (deprecated and insecure).

Example:
  sh ./deploy_from_codex.sh \
    --bot-id "your-bot-id" \
    --bot-secret-file "/secure/path/wecom-bot.secret" \
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

require_safe_name_component() {
  value="$1"
  label="$2"
  case "$value" in
    ""|"."|"..")
      echo "$label must not be empty, '.' or '..'" >&2
      exit 1
      ;;
    */*)
      echo "$label must not contain '/': $value" >&2
      exit 1
      ;;
  esac
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

cleanup_secret_tmp() {
  if [ -n "$SECRET_TMP_FILE" ]; then
    rm -f "$SECRET_TMP_FILE"
  fi
  if [ -n "$ENV_TMP_FILE" ]; then
    rm -f "$ENV_TMP_FILE"
  fi
}

trap cleanup_secret_tmp EXIT
trap 'exit 1' HUP INT TERM

shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

expand_user_path() {
  python3 - "$1" <<'PY'
import pathlib
import sys

print(pathlib.Path(sys.argv[1]).expanduser())
PY
}

validate_secret_file() {
  python3 - "$1" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1]).expanduser()
if not path.exists():
    print(f"--bot-secret-file does not exist: {path}", file=sys.stderr)
    raise SystemExit(1)
if not path.is_file():
    print(f"--bot-secret-file is not a regular file: {path}", file=sys.stderr)
    raise SystemExit(1)
try:
    mode = path.stat().st_mode
    if mode & 0o444 == 0:
        raise PermissionError
    value = path.read_text(encoding="utf-8")
except (OSError, UnicodeError):
    print(f"--bot-secret-file is not readable UTF-8 text: {path}", file=sys.stderr)
    raise SystemExit(1)
if not value.strip():
    print(f"--bot-secret-file is empty: {path}", file=sys.stderr)
    raise SystemExit(1)
print(path.resolve())
PY
}

normalize_repo_url() {
  python3 - "$1" <<'PY'
import sys
from urllib.parse import urlparse

raw = sys.argv[1].strip().rstrip("/")
if raw.endswith(".git"):
    raw = raw[:-4]

if "://" in raw:
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower().strip()
    path = parsed.path.lstrip("/").strip()
    if host and path:
        print(f"{host}/{path}")
    else:
        print(raw)
elif "@" in raw and ":" in raw.split("@", 1)[1]:
    user_host, path = raw.split(":", 1)
    host = user_host.split("@", 1)[1].lower().strip()
    path = path.lstrip("/").strip()
    if host and path:
        print(f"{host}/{path}")
    else:
        print(raw)
else:
    print(raw)
PY
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bot-id)
      BOT_ID="${2:-}"
      shift 2
      ;;
    --bot-secret)
      BOT_SECRET="${2:-}"
      BOT_SECRET_ARG_SET="true"
      shift 2
      ;;
    --bot-secret-file)
      BOT_SECRET_SOURCE_FILE="${2:-}"
      BOT_SECRET_FILE_ARG_SET="true"
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
[ -n "$GITHUB_REPO" ] || { echo "--github-repo is required" >&2; exit 1; }

if is_truthy "$BOT_SECRET_ARG_SET" && is_truthy "$BOT_SECRET_FILE_ARG_SET"; then
  echo "--bot-secret-file and --bot-secret are mutually exclusive" >&2
  exit 1
fi
if ! is_truthy "$BOT_SECRET_ARG_SET" && ! is_truthy "$BOT_SECRET_FILE_ARG_SET"; then
  echo "exactly one of --bot-secret-file or --bot-secret is required" >&2
  exit 1
fi

require_command git
require_command mktemp
require_command python3
require_command sh

if is_truthy "$BOT_SECRET_FILE_ARG_SET"; then
  [ -n "$BOT_SECRET_SOURCE_FILE" ] || { echo "--bot-secret-file must not be empty" >&2; exit 1; }
  BOT_SECRET_SOURCE_FILE="$(validate_secret_file "$BOT_SECRET_SOURCE_FILE")"
else
  [ -n "$BOT_SECRET" ] || { echo "--bot-secret must not be empty" >&2; exit 1; }
  echo "warning: --bot-secret exposes the secret in shell history and process listings; use --bot-secret-file instead" >&2
fi

REPO_SLUG="$(basename "$GITHUB_REPO")"
REPO_SLUG="${REPO_SLUG%.git}"
[ -n "$REPO_SLUG" ] || { echo "failed to derive repository name from: $GITHUB_REPO" >&2; exit 1; }
require_safe_name_component "$REPO_SLUG" "--github-repo repository name"

if [ -z "$BOT_NAME" ]; then
  BOT_NAME="$(printf '%s' "$REPO_SLUG" | tr ' /:@' '_' | tr -cd 'A-Za-z0-9._-')"
fi
[ -n "$BOT_NAME" ] || BOT_NAME="default"
require_safe_name_component "$BOT_NAME" "--bot-name"

if [ -z "$WORK_ROOT" ]; then
  WORK_ROOT="$SCRIPT_DIR/bot-workdirs"
fi
WORK_ROOT="$(expand_user_path "$WORK_ROOT")"

TARGET_REPO_DIR="$WORK_ROOT/$REPO_SLUG"
SECRET_FILE="$SCRIPT_DIR/.secrets/$BOT_NAME.secret"
ENV_FILE="$SCRIPT_DIR/.env"
ENV_BACKUP_FILE="$SCRIPT_DIR/.env.backup.$(date +%Y%m%d%H%M%S)"
VENV_DIR="$SCRIPT_DIR/.venv"
VENV_PYTHON="$VENV_DIR/bin/python"

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
if ! is_truthy "$DRY_RUN"; then
  chmod 700 "$SCRIPT_DIR/.secrets"
fi

if [ -d "$TARGET_REPO_DIR/.git" ]; then
  existing_origin="$(git -C "$TARGET_REPO_DIR" remote get-url origin 2>/dev/null || true)"
  if [ -z "$existing_origin" ]; then
    echo "existing git repo has no origin remote: $TARGET_REPO_DIR" >&2
    exit 1
  fi
  if [ "$(normalize_repo_url "$existing_origin")" != "$(normalize_repo_url "$GITHUB_REPO")" ]; then
    echo "existing git repo origin mismatch: $existing_origin != $GITHUB_REPO" >&2
    exit 1
  fi
  echo "Reusing existing git repo: $TARGET_REPO_DIR"
elif [ -e "$TARGET_REPO_DIR" ]; then
  echo "target path exists but is not a git repo: $TARGET_REPO_DIR" >&2
  exit 1
else
  run_cmd git clone "$GITHUB_REPO" "$TARGET_REPO_DIR"
fi

if ! is_truthy "$SKIP_INSTALL"; then
  if [ ! -x "$VENV_PYTHON" ]; then
    run_cmd python3 -m venv "$VENV_DIR"
  fi
  run_cmd "$VENV_PYTHON" -m pip install -r "$SCRIPT_DIR/requirements.txt"
  BRIDGE_PYTHON="$VENV_PYTHON"
elif [ -z "$BRIDGE_PYTHON" ]; then
  if [ -x "$VENV_PYTHON" ]; then
    BRIDGE_PYTHON="$VENV_PYTHON"
  else
    BRIDGE_PYTHON="python3"
  fi
fi

if is_truthy "$DRY_RUN"; then
  printf '[dry-run] write secret file: %s\n' "$SECRET_FILE"
else
  umask 077
  SECRET_TMP_FILE="$(mktemp "$SCRIPT_DIR/.secrets/.${BOT_NAME}.secret.tmp.XXXXXX")"
  if is_truthy "$BOT_SECRET_FILE_ARG_SET"; then
    cp "$BOT_SECRET_SOURCE_FILE" "$SECRET_TMP_FILE"
  else
    printf '%s\n' "$BOT_SECRET" > "$SECRET_TMP_FILE"
  fi
  chmod 600 "$SECRET_TMP_FILE"
  mv -f "$SECRET_TMP_FILE" "$SECRET_FILE"
  SECRET_TMP_FILE=""
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
  ENV_TMP_FILE="$(mktemp "$SCRIPT_DIR/.env.tmp.XXXXXX")"
  chmod 600 "$ENV_TMP_FILE"
  cat > "$ENV_TMP_FILE" <<EOF
BRIDGE_BIND=$(shell_quote "$BRIDGE_BIND")
BRIDGE_PYTHON=$(shell_quote "$BRIDGE_PYTHON")
WORK_DIR=$(shell_quote "$TARGET_REPO_DIR")
CODEX_EXEC_MODE=$(shell_quote "host")
BRIDGE_WATCHDOG_ENABLED=$(shell_quote "true")

WECOM_BOT_CONFIG_ID=$(shell_quote "$BOT_NAME")
WECOM_BOT_NAME=$(shell_quote "$BOT_NAME")
WECOM_BOT_ID=$(shell_quote "$BOT_ID")
WECOM_BOT_SECRET_FILE=$(shell_quote "$SECRET_FILE")
WECOM_BOT_WORK_DIR=$(shell_quote "$TARGET_REPO_DIR")
WECOM_BOT_GROUP_SESSION_MODE=$(shell_quote "per-user")
WECOM_BOT_ENABLED=$(shell_quote "true")
EOF
  mv -f "$ENV_TMP_FILE" "$ENV_FILE"
  ENV_TMP_FILE=""
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
