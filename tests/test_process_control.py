from __future__ import annotations

import asyncio
import os
import sys

import pytest

from workspace_bridge.process_control import (
    isolated_subprocess_kwargs,
    remember_isolated_process,
    terminate_process_tree,
)


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix" or not os.path.isdir("/proc"), reason="requires Linux process sessions")
async def test_terminate_process_tree_stops_descendant_in_separate_process_group() -> None:
    script = """
import os
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import os, time; os.setpgid(0, 0); time.sleep(60)"],
    stdout=sys.stdout,
    stderr=sys.stderr,
)
deadline = time.monotonic() + 2
while os.getpgid(child.pid) != child.pid:
    if child.poll() is not None:
        raise RuntimeError(f"child exited with status {child.returncode}")
    if time.monotonic() >= deadline:
        raise RuntimeError("child did not enter its process group")
    time.sleep(0.01)
print(child.pid, flush=True)
time.sleep(60)
"""
    # A legacy bridge test replaces asyncio.create_subprocess_exec globally.
    process = await asyncio.subprocess.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **isolated_subprocess_kwargs(),
    )
    remember_isolated_process(process)
    try:
        assert process.stdout is not None
        assert process.stderr is not None
        child_line = await asyncio.wait_for(process.stdout.readline(), timeout=5)
        if not child_line:
            stderr = (await process.stderr.read()).decode("utf-8", "replace").strip()
            pytest.fail(f"process fixture exited before reporting its child PID: {stderr}")
        child_pid = int(child_line.decode().strip())
        assert os.getsid(child_pid) == process.pid
        assert os.getpgid(child_pid) == child_pid

        assert await terminate_process_tree(process, timeout_sec=1) is True
        assert process.returncode is not None
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
    finally:
        await terminate_process_tree(process, timeout_sec=1)
