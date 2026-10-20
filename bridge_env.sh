#!/bin/sh

resolve_bridge_root_path() {
  bridge_root_base="$1"
  bridge_root_raw="$2"
  python3 - "$bridge_root_base" "$bridge_root_raw" <<'PY'
import sys
from pathlib import Path

base = Path(sys.argv[1]).resolve()
raw = sys.argv[2].strip()
path = Path(raw).expanduser() if raw else base
if not path.is_absolute():
    path = base / path
print(path.resolve())
PY
}

prepare_bridge_state_root() {
  bridge_state_base="$1"
  bridge_state_raw="$2"
  bridge_state_label="$3"
  bridge_state_root="$(resolve_bridge_root_path "$bridge_state_base" "$bridge_state_raw")" || return 1
  if [ -e "$bridge_state_root" ] && [ ! -d "$bridge_state_root" ]; then
    echo "$bridge_state_label is not a directory: $bridge_state_root" >&2
    return 1
  fi
  if ! mkdir -p "$bridge_state_root"; then
    echo "$bridge_state_label cannot be created: $bridge_state_root" >&2
    return 1
  fi
  if ! python3 - "$bridge_state_root" <<'PY'
import os
import stat
import sys
import tempfile

fd = None
path = None
try:
    root_stat = os.stat(sys.argv[1], follow_symlinks=False)
    if not stat.S_ISDIR(root_stat.st_mode) or root_stat.st_mode & 0o002:
        raise PermissionError("state root must not be world-writable")
    fd, path = tempfile.mkstemp(prefix=".bridge-write-test.", dir=sys.argv[1])
    os.close(fd)
    fd = None
    os.unlink(path)
    path = None
except OSError:
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass
    if path is not None:
        try:
            os.unlink(path)
        except OSError:
            pass
    raise
PY
  then
    echo "$bridge_state_label must not be world-writable and must be writable: $bridge_state_root" >&2
    return 1
  fi
  printf '%s\n' "$bridge_state_root"
}

write_bridge_shutdown_reason_file() {
  bridge_shutdown_file="$1"
  bridge_shutdown_reason="$2"
  python3 - "$bridge_shutdown_file" "$bridge_shutdown_reason" <<'PY'
import sys
import time
from pathlib import Path

from workspace_bridge.atomic_io import atomic_write_json

atomic_write_json(
    Path(sys.argv[1]),
    {
        "reason": str(sys.argv[2] or "").strip() or "unknown",
        "at": int(time.time() * 1000),
    },
)
PY
}

bridge_pid_is_positive_integer() {
  case "$1" in
    ""|0|*[!0-9]*)
      return 1
      ;;
  esac
  return 0
}

read_bridge_pid_file() {
  bridge_pid_file="$1"
  [ -f "$bridge_pid_file" ] && [ ! -L "$bridge_pid_file" ] || return 1
  IFS= read -r bridge_pid_value < "$bridge_pid_file" || return 1
  bridge_pid_is_positive_integer "$bridge_pid_value" || return 1
  printf '%s\n' "$bridge_pid_value"
}

write_bridge_pid_file() {
  bridge_pid_file="$1"
  bridge_pid_value="$2"
  bridge_pid_is_positive_integer "$bridge_pid_value" || return 1
  python3 - "$bridge_pid_file" "$bridge_pid_value" "$$" <<'PY'
import os
import stat
import sys
from pathlib import Path

path = Path(sys.argv[1])
value = sys.argv[2]
writer_pid = sys.argv[3]
try:
    target_stat = os.lstat(path)
except FileNotFoundError:
    pass
except OSError:
    raise SystemExit(1)
else:
    if not stat.S_ISREG(target_stat.st_mode) or stat.S_ISLNK(target_stat.st_mode):
        raise SystemExit(1)

temp_path = path.with_name(f"{path.name}.{writer_pid}.{value}.tmp")
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
if hasattr(os, "O_NOFOLLOW"):
    flags |= os.O_NOFOLLOW
try:
    fd = os.open(temp_path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="ascii", closefd=False) as handle:
            handle.write(f"{value}\n")
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(fd)
    os.replace(temp_path, path)
except OSError:
    try:
        temp_path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass
    raise SystemExit(1)
PY
}

bridge_process_env_value() {
  bridge_process_pid="$1"
  bridge_process_env_key="$2"
  python3 - "$bridge_process_pid" "$bridge_process_env_key" <<'PY'
import sys
from pathlib import Path

pid, key = sys.argv[1:]
try:
    entries = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
except OSError:
    raise SystemExit(1)
prefix = key.encode("utf-8") + b"="
for entry in entries:
    if entry.startswith(prefix):
        print(entry[len(prefix):].decode("utf-8", errors="replace"))
        break
PY
}

bridge_pid_matches_instance_process() {
  bridge_match_script_dir="$1"
  bridge_match_control_root="$2"
  bridge_match_allow_legacy="$3"
  bridge_match_kind="$4"
  bridge_match_pid="$5"
  bridge_pid_is_positive_integer "$bridge_match_pid" || return 1
  kill -0 "$bridge_match_pid" 2>/dev/null || return 1
  bridge_match_cmdline="$(tr '\0' ' ' < "/proc/$bridge_match_pid/cmdline" 2>/dev/null || true)"
  case "$bridge_match_kind" in
    bridge)
      case "$bridge_match_cmdline" in
        *"$bridge_match_script_dir/bridge.py"*) ;;
        *) return 1 ;;
      esac
      ;;
    watchdog)
      case "$bridge_match_cmdline" in
        *"$bridge_match_script_dir/bridge_watchdog.sh"*) ;;
        *) return 1 ;;
      esac
      ;;
    *)
      return 1
      ;;
  esac
  bridge_match_process_root="$(bridge_process_env_value "$bridge_match_pid" BRIDGE_CONTROL_ROOT 2>/dev/null || true)"
  if [ -z "$bridge_match_process_root" ]; then
    [ "$bridge_match_allow_legacy" = "true" ] || return 1
    return 0
  fi
  bridge_match_process_root="$(resolve_bridge_root_path "$bridge_match_script_dir" "$bridge_match_process_root")" || return 1
  [ "$bridge_match_process_root" = "$bridge_match_control_root" ]
}

load_bridge_runtime_env() {
  bridge_script_dir="$1"
  if [ -z "$bridge_script_dir" ]; then
    echo "load_bridge_runtime_env: script dir is required" >&2
    return 1
  fi

  if [ "${BRIDGE_ENV_FILE_REQUIRED+x}" = "x" ]; then
    bridge_env_file_required="$BRIDGE_ENV_FILE_REQUIRED"
  elif [ "${BRIDGE_ENV_FILE+x}" = "x" ] && [ -n "${BRIDGE_ENV_FILE:-}" ]; then
    bridge_env_file_required="true"
  else
    bridge_env_file_required="false"
  fi
  bridge_env_file="$(resolve_bridge_root_path "$bridge_script_dir" "${BRIDGE_ENV_FILE:-.env}")" || return 1
  if [ "$bridge_env_file_required" = "true" ] && [ ! -e "$bridge_env_file" ]; then
    echo "BRIDGE_ENV_FILE does not exist: $bridge_env_file" >&2
    return 1
  fi
  if [ -e "$bridge_env_file" ] && [ ! -f "$bridge_env_file" ]; then
    echo "BRIDGE_ENV_FILE is not a file: $bridge_env_file" >&2
    return 1
  fi
  if [ -f "$bridge_env_file" ]; then
    # shellcheck disable=SC1090
    . "$bridge_env_file"
  fi
  BRIDGE_ENV_FILE="$bridge_env_file"
  BRIDGE_ENV_FILE_REQUIRED="$bridge_env_file_required"

  if [ -n "${WECOM_BOT_SECRET:-}" ]; then
    echo "WECOM_BOT_SECRET is no longer supported; use WECOM_BOT_SECRET_FILE" >&2
    return 1
  fi

  BRIDGE_PYTHON="${BRIDGE_PYTHON:-python3}"
  if ! command -v "$BRIDGE_PYTHON" >/dev/null 2>&1; then
    echo "BRIDGE_PYTHON is not executable: $BRIDGE_PYTHON" >&2
    return 1
  fi

  export BRIDGE_ENV_FILE BRIDGE_ENV_FILE_REQUIRED BRIDGE_CONTROL_ROOT BRIDGE_PYTHON
  export BRIDGE_BIND BRIDGE_HOST BRIDGE_PORT HOST PORT
  set -- $(python3 "$bridge_script_dir/bridge_runtime_config.py" --print-host-port) || return 1
  HOST="$1"
  PORT="$2"
  export HOST PORT BRIDGE_BIND BRIDGE_HOST BRIDGE_PORT
}

export_bridge_runtime_env() {
  export BRIDGE_ENV_FILE BRIDGE_ENV_FILE_REQUIRED BRIDGE_CONTROL_ROOT BRIDGE_PYTHON
  export HOST PORT BRIDGE_TOKEN BRIDGE_BASIC_AUTH WORK_DIR MAX_JSON_BODY MAX_UPLOAD_SIZE FILE_SEND_ROOTS
  export BRIDGE_BIND BRIDGE_HOST BRIDGE_PORT
  export BRIDGE_SHARED_RUNTIME_ROOT
  export BRIDGE_RUNTIME_ROOT
  export MAX_INBOUND_IMAGE_SIZE MAX_INBOUND_FILE_SIZE
  export SESSION_TTL SESSION_LEASE_TTL HTTP_PROXY HTTPS_PROXY NO_PROXY
  export MEDIA_CONNECT_TIMEOUT MEDIA_TOTAL_TIMEOUT LOCAL_FILE_SEND_QUEUE_ROOT LOCAL_FILE_SEND_POLL_MS
  export LOCAL_FILE_SEND_RESULT_TIMEOUT_MS LOCAL_FILE_SEND_RESULT_RETENTION_MS
  export LOCAL_SEND_MESSAGE_QUEUE_ROOT LOCAL_SEND_MESSAGE_POLL_MS LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS LOCAL_SEND_MESSAGE_RESULT_RETENTION_MS
  export CODEX_EXEC_MODE MAX_CONCURRENT_CODEX_RUNS CODEX_COMMAND CODEX_HOME
  export SUBPROCESS_STREAM_LIMIT SUBPROCESS_STREAM_READ_SIZE SUBPROCESS_STREAM_MAX_LINE
  export WEBSOCKET_SEND_TIMEOUT_SEC STATUS_STREAM_INTERVAL_SEC STATUS_STREAM_PROGRESS_ENABLED STATUS_SEND_TIMEOUT_SEC STATUS_SEND_LOCK_TIMEOUT_SEC
  export REPLY_SEND_ACK_TIMEOUT_SEC UNCERTAIN_DELIVERY_RETRY_SEC STREAM_TEXT_MAX_CHARS
  export REPLY_IDLE_FALLBACK_SEC REPLY_MAX_AGE_FALLBACK_SEC PROACTIVE_STATUS_INTERVAL_SEC
  export SCHEDULE_POLL_MS
  export SCHEDULE_DEFINITION_POLL_MS SCHEDULE_DEFINITION_LEASE_TTL_MS
  export PROACTIVE_SEND_ACK_TIMEOUT_SEC
  export PROACTIVE_TEXT_MAX_CHARS
  export SCHEDULE_PROCESSING_RETRY_MS
  export SCHEDULE_ORPHAN_TTL_MS
  export BOTS_FILE WECOM_BOOTSTRAP_BOTS_JSON WECOM_BOOTSTRAP_BOTS_JSON_FILE
  export WECOM_BOT_CONFIG_ID WECOM_BOT_NAME WECOM_BOT_ID WECOM_BOT_SECRET_FILE
  export WECOM_BOT_WORK_DIR WECOM_BOT_WORKSPACE_MODE WECOM_BOT_WORKSPACE_NAMESPACE WECOM_WORKSPACE_MODE WECOM_WORKSPACE_NAMESPACE
  export WECOM_BOT_WELCOME WECOM_BOT_GROUP_SESSION_MODE WECOM_BOT_LOCAL_RESUME_OWNER_USER_ID WECOM_BOT_ENABLED
  export LOCAL_RESUME_OWNER_USER_ID
  export WECOM_BOT_AGENT_BACKEND WECOM_BOT_AGENT_COMMAND
  export WECOM_BOT_AGENT_RUN_AS_USER WECOM_BOT_AGENT_RUN_AS_GROUP WECOM_BOT_AGENT_RUNTIME_ROOT
  export AGENT_BACKEND AGENT_COMMAND AGENT_RUN_AS_USER AGENT_RUN_AS_GROUP AGENT_RUNTIME_ROOT
  export WECOM_INBOUND_IDLE_TIMEOUT_MS WECOM_HEARTBEAT_ACK_TIMEOUT_MS WECOM_DISCONNECT_GRACE_MS WECOM_RAW_INBOUND_LOG_ENABLED
  export BRIDGE_WATCHDOG_ENABLED BRIDGE_WATCHDOG_POLL_SEC BRIDGE_WATCHDOG_HEALTH_TIMEOUT_SEC
  export BRIDGE_WATCHDOG_STARTUP_GRACE_SEC BRIDGE_WATCHDOG_FAIL_THRESHOLD
  export BRIDGE_WATCHDOG_RESTART_BACKOFF_SEC BRIDGE_WATCHDOG_RESTART_WINDOW_SEC
  export BRIDGE_WATCHDOG_MAX_RESTART_STREAK BRIDGE_WATCHDOG_COOLDOWN_SEC
}
