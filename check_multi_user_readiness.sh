#!/bin/sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

sh ./check_production_runtime.sh
sh ./check_compatibility_runtime.sh
