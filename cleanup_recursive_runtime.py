#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from workspace_bridge.cleanup import cleanup_nested_runtime_dirs, find_nested_runtime_dirs


def main() -> int:
    parser = argparse.ArgumentParser(description="Remove nested .workspace-bridge-runtime directories copied into workspace project roots.")
    parser.add_argument("--runtime-root", default=".workspace-bridge-runtime")
    parser.add_argument("--apply", action="store_true", help="Delete the matched nested runtime directories.")
    args = parser.parse_args()

    runtime_root = Path(args.runtime_root).expanduser().resolve()
    matches = find_nested_runtime_dirs(runtime_root)
    print(f"runtime_root={runtime_root}")
    print(f"matches={len(matches)}")
    for path in matches[:20]:
        print(path)
    if len(matches) > 20:
        print(f"... {len(matches) - 20} more")

    if not args.apply:
        return 0

    removed = cleanup_nested_runtime_dirs(runtime_root)
    print(f"removed={len(removed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
