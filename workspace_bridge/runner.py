from __future__ import annotations

import os
import subprocess
from pathlib import Path

from . import codex_runtime
from .agent_backends import build_agent_argv, prepare_subprocess_env
from .agent_runtime import build_setpriv_prefix, resolve_posix_identity
from .models import CodexLaunchSpec, RunnerInvocation

def build_codex_argv(
    output_file: Path,
    *,
    resume: bool = False,
    resume_thread_id: str | None = None,
    image_paths: list[str] | None = None,
    exec_mode: str = "host",
    agent_backend: str = "codex",
    agent_command: str | None = None,
) -> tuple[str, ...]:
    return build_agent_argv(
        agent_backend,  # type: ignore[arg-type]
        agent_command,
        output_file,
        resume=resume,
        resume_thread_id=resume_thread_id,
        image_paths=image_paths,
        exec_mode=exec_mode,
    )


def build_runner_invocation(
    launch: CodexLaunchSpec,
    *,
    prompt: str,
    output_file: Path,
    resume: bool = False,
    resume_thread_id: str | None = None,
    image_paths: list[str] | None = None,
    argv_override: tuple[str, ...] | None = None,
) -> RunnerInvocation:
    agent_backend = str(launch.env.get("WECOM_BRIDGE_AGENT_BACKEND") or "codex").strip().lower() or "codex"
    raw_agent_command = launch.env.get("WECOM_BRIDGE_AGENT_COMMAND")
    agent_command = None if raw_agent_command in (None, "") else (str(raw_agent_command).strip() or None)
    raw_run_as_user = launch.env.get("WECOM_BRIDGE_AGENT_RUN_AS_USER")
    run_as_user = None if raw_run_as_user in (None, "") else (str(raw_run_as_user).strip() or None)
    raw_run_as_group = launch.env.get("WECOM_BRIDGE_AGENT_RUN_AS_GROUP")
    run_as_group = None if raw_run_as_group in (None, "") else (str(raw_run_as_group).strip() or None)
    argv = argv_override or build_codex_argv(
        output_file,
        resume=resume,
        resume_thread_id=resume_thread_id,
        image_paths=image_paths,
        exec_mode=launch.runtime_context.codex_exec_mode,
        agent_backend=agent_backend,
        agent_command=agent_command,
    )
    env = dict(os.environ)
    env.update(launch.env)
    env = codex_runtime.prepare_codex_subprocess_env(env, command=argv[0] if argv else None)
    env = prepare_subprocess_env(env, backend=agent_backend, command=argv[0] if argv else None)  # type: ignore[arg-type]
    if agent_backend != "claude":
        run_as_user = None
        run_as_group = None
    return RunnerInvocation(
        argv=tuple(argv),
        cwd=launch.cwd,
        env=env,
        prompt=prompt,
        run_as_user=run_as_user,
        run_as_group=run_as_group,
    )


def _wrap_with_setpriv(invocation: RunnerInvocation) -> tuple[tuple[str, ...], dict[str, str]]:
    user = str(invocation.run_as_user or "").strip()
    if not user:
        return invocation.argv, invocation.env
    uid, gid = resolve_posix_identity(user, invocation.run_as_group)
    wrapped = (*build_setpriv_prefix(uid, gid), *invocation.argv)
    env = dict(invocation.env)
    return wrapped, env


def run_invocation(invocation: RunnerInvocation) -> subprocess.CompletedProcess[str]:
    argv, env = _wrap_with_setpriv(invocation)
    return subprocess.run(
        argv,
        cwd=invocation.cwd,
        env=env,
        input=invocation.prompt,
        capture_output=True,
        text=True,
        check=False,
    )
