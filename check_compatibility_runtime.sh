#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

exec sh ./test.sh \
  tests/test_config_and_service.py \
  tests/test_delivery_readiness.py \
  tests/test_execution_and_service_runtime.py \
  tests/test_prompting_and_runner.py \
  tests/test_wecom_runtime_subscribe.py \
  tests/test_runtime.py \
  tests/test_wecom_runtime_reliability.py \
  tests/test_schedule_runtime_and_service.py
