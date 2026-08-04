#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

exec sh ./test.sh \
  tests/test_api_security.py \
  tests/test_atomic_io.py \
  tests/test_bridge_core.py \
  tests/test_production_readiness_probe.py
