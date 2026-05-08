from __future__ import annotations

import os
import subprocess
from pathlib import Path

from .models import CodexLaunchSpec, RunnerInvocation


def build_codex_argv(output_file: Path, *, resume: bool = False, image_paths: list[str] | None = None) -> tuple[str, ...]:
    argv: list[str] = ["codex", "exec"]
    if resume:
        argv.append("resume")
    argv.extend(["--skip-git-repo-check", "--json", "-o", str(output_file)])
    argv.append("--full-auto")
    for image_path in image_paths or []:
        argv.extend(["-i", image_path])
    return tuple(argv)


def build_runner_invocation(
    launch: CodexLaunchSpec,
    *,
    prompt: str,
    output_file: Path,
    resume: bool = False,
    image_paths: list[str] | None = None,
    argv_override: tuple[str, ...] | None = None,
) -> RunnerInvocation:
    argv = argv_override or build_codex_argv(output_file, resume=resume, image_paths=image_paths)
    env = dict(os.environ)
    env.update(launch.env)
    return RunnerInvocation(argv=tuple(argv), cwd=launch.cwd, env=env, prompt=prompt)


def run_invocation(invocation: RunnerInvocation) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        invocation.argv,
        cwd=invocation.cwd,
        env=invocation.env,
        input=invocation.prompt,
        capture_output=True,
        text=True,
        check=False,
    )
