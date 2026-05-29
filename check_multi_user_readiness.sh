#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1

exec pytest -q -p pytest_asyncio.plugin \
  tests/test_bridge_core.py \
  tests/test_delivery_readiness.py \
  tests/test_execution_and_service_runtime.py \
  tests/test_wecom_runtime_subscribe.py \
  tests/test_runtime.py \
  tests/test_wecom_runtime_reliability.py \
  tests/test_schedule_runtime_and_service.py
