#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

python3 -m py_compile bridge.py tests/test_bridge_core.py || exit 1

python3 -m pytest -q -p no:rerunfailures tests/test_bridge_core.py \
  -k "resolve_file_send_request or resolve_schedule_target or process_scheduled_messages or run_codex_sends_running_status_before_final or send_session_status_times_out_when_send_lock_is_busy"
