#!/bin/sh

load_bridge_runtime_env() {
  bridge_script_dir="$1"
  if [ -z "$bridge_script_dir" ]; then
    echo "load_bridge_runtime_env: script dir is required" >&2
    return 1
  fi

  if [ -f "$bridge_script_dir/.env" ]; then
    # shellcheck disable=SC1090
    . "$bridge_script_dir/.env"
  fi

  export BRIDGE_BIND BRIDGE_HOST BRIDGE_PORT HOST PORT
  set -- $(python3 "$bridge_script_dir/bridge_runtime_config.py" --print-host-port) || return 1
  HOST="$1"
  PORT="$2"
  export HOST PORT BRIDGE_BIND BRIDGE_HOST BRIDGE_PORT
}
