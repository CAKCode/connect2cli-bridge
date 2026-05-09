#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
exec pytest -q -p pytest_asyncio.plugin "$@"
