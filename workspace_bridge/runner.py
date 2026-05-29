from __future__ import annotations

import os
import subprocess
from pathlib import Path

from . import codex_runtime
from .models import CodexLaunchSpec, RunnerInvocation


def parse_command_to_argv(command: str) -> tuple[str, ...]:
    return tuple(__import__("shlex").split(command))


def resolve_codex_command() -> tuple[str, ...]:
    raw = str(os.environ.get("CODEX_COMMAND") or "").strip()
    if raw:
        return parse_command_to_argv(raw)
    return ("codex",)


def build_codex_argv(
    output_file: Path,
    *,
    resume: bool = False,
    image_paths: list[str] | None = None,
    exec_mode: str = "host",
) -> tuple[str, ...]:
    argv: list[str] = [*resolve_codex_command(), "exec"]
    if resume:
        argv.append("resume")
    argv.extend(["--skip-git-repo-check", "--json", "-o", str(output_file)])
    if str(exec_mode).strip().lower() == "host":
        argv.append("--dangerously-bypass-approvals-and-sandbox")
    else:
        argv.append("--full-auto")
    for image_path in image_paths or []:
        argv.extend(["-i", image_path])
    argv.append("-")
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
    argv = argv_override or build_codex_argv(output_file, resume=resume, image_paths=image_paths, exec_mode=launch.runtime_context.codex_exec_mode)
    env = dict(os.environ)
    env.update(launch.env)
    env = codex_runtime.prepare_codex_subprocess_env(env, command=argv[0] if argv else None)
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
