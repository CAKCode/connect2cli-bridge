from __future__ import annotations

import json
import os
import shlex
import shutil
from pathlib import Path
from typing import Literal


AgentBackend = Literal["codex", "claude"]


def normalize_agent_backend(value: str | None) -> AgentBackend:
    text = str(value or "").strip().lower()
    if not text or text == "codex":
        return "codex"
    if text in {"claude", "claude-code", "claudecode"}:
        return "claude"
    raise ValueError(f"unsupported agent backend: {value}")


def parse_command_to_argv(command: str) -> tuple[str, ...]:
    return tuple(shlex.split(command))


def _resolve_command(raw: str | None, env_var: str, default_argv: tuple[str, ...]) -> tuple[str, ...]:
    text = str(raw or "").strip()
    if text.lower() in {"none", "null"}:
        text = ""
    if text:
        return parse_command_to_argv(text)
    env_text = str(os.environ.get(env_var) or "").strip()
    if env_text.lower() in {"none", "null"}:
        env_text = ""
    if env_text:
        return parse_command_to_argv(env_text)
    return default_argv


def resolve_agent_command(backend: AgentBackend, command: str | None = None) -> tuple[str, ...]:
    if backend == "claude":
        return _resolve_command(command, "CLAUDE_COMMAND", ("claude",))
    return _resolve_command(command, "CODEX_COMMAND", ("codex",))


def build_agent_argv(
    backend: AgentBackend,
    command: str | None,
    output_file: Path,
    *,
    resume: bool = False,
    resume_thread_id: str | None = None,
    image_paths: list[str] | None = None,
    exec_mode: str = "host",
) -> tuple[str, ...]:
    base = list(resolve_agent_command(backend, command))
    images = list(image_paths or [])
    mode = str(exec_mode).strip().lower() or "host"

    if backend == "claude":
        argv = [*base, "-p", "--verbose"]
        if resume and resume_thread_id:
            argv.extend(["--resume", resume_thread_id])
        argv.extend(["--output-format", "stream-json"])
        return tuple(argv)

    argv = [*base, "exec"]
    if resume:
        argv.append("resume")
    argv.extend(["--skip-git-repo-check", "--json", "-o", str(output_file)])
    if mode == "host":
        argv.append("--dangerously-bypass-approvals-and-sandbox")
    else:
        argv.append("--full-auto")
    for image_path in images:
        argv.extend(["-i", image_path])
    argv.append("-")
    if resume and resume_thread_id:
        argv.insert(len(base) + 2, resume_thread_id)
    return tuple(argv)


def extract_agent_reply(backend: AgentBackend, stdout: str) -> str:
    if backend == "claude":
        latest = ""
        for line in str(stdout or "").splitlines():
            try:
                payload = json.loads(line)
            except Exception:
                continue
            payload_type = str(payload.get("type") or "").strip()
            if payload_type == "result":
                result_text = str(payload.get("result") or "").strip()
                if result_text:
                    latest = result_text
            message = payload.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, list):
                    text_parts = [
                        str(item.get("text") or "").strip()
                        for item in content
                        if isinstance(item, dict) and str(item.get("type") or "").strip() == "text"
                    ]
                    joined = "\n".join(part for part in text_parts if part).strip()
                    if joined:
                        latest = joined
        return latest

    latest = ""
    for line in str(stdout or "").splitlines():
        try:
            payload = json.loads(line)
        except Exception:
            continue
        item = payload.get("item") or {}
        if payload.get("type") == "item.completed" and item.get("type") in {"agent_message", "agentmessage"}:
            latest = str(item.get("text") or "").strip() or latest
    return latest


def extract_agent_thread_id(backend: AgentBackend, stdout: str) -> str | None:
    for line in str(stdout or "").splitlines():
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if backend == "claude":
            if str(payload.get("type") or "").strip() == "system":
                session_id = str(payload.get("session_id") or payload.get("sessionId") or "").strip()
                if session_id:
                    return session_id
                subtype = str(payload.get("subtype") or "").strip()
                if subtype == "init":
                    nested = payload.get("data")
                    if isinstance(nested, dict):
                        session_id = str(nested.get("session_id") or nested.get("sessionId") or "").strip()
                        if session_id:
                            return session_id
            continue
        if payload.get("type") == "thread.started":
            thread_id = str(payload.get("thread_id") or "").strip()
            if thread_id:
                return thread_id
    return None


def prepare_subprocess_env(
    env: dict[str, str] | None = None,
    *,
    backend: AgentBackend,
    command: str | None = None,
) -> dict[str, str]:
    prepared = dict(os.environ if env is None else env)
    if backend != "codex":
        return prepared

    raw = str(command or "").strip()
    if raw:
        command_name = Path(raw).name.lower()
        if command_name not in {"codex", "codex.js"} and "codex" not in command_name:
            return prepared

    path_value = prepared.get("PATH") or os.defpath
    if shutil.which("bwrap", path=path_value):
        return prepared

    executable = resolve_executable(command or "codex")
    if executable is None:
        return prepared

    patterns = (
        "node_modules/@openai/codex-*/vendor/*/codex-resources/bwrap",
        "@openai/codex-*/vendor/*/codex-resources/bwrap",
        "vendor/*/codex-resources/bwrap",
        "codex-resources/bwrap",
    )
    search_roots: list[Path] = []
    for root in (executable.parent, *executable.parents[:6]):
        resolved_root = root.resolve()
        if resolved_root in search_roots:
            continue
        search_roots.append(resolved_root)
    for root in search_roots:
        for pattern in patterns:
            for candidate in root.glob(pattern):
                if candidate.is_file():
                    prepared["PATH"] = _prepend_path(path_value, str(candidate.parent))
                    return prepared
    return prepared


def resolve_executable(command: str | None) -> Path | None:
    raw = str(command or "").strip()
    if not raw:
        return None
    if os.path.sep in raw or (os.path.altsep and os.path.altsep in raw):
        candidate = Path(raw).expanduser()
        if candidate.exists():
            return candidate.resolve()
        return None
    resolved = shutil.which(raw)
    if not resolved:
        return None
    return Path(resolved).expanduser().resolve()


def _prepend_path(path_value: str, entry: str) -> str:
    items: list[str] = []
    seen: set[str] = set()
    for value in [entry, *(item for item in str(path_value or "").split(os.pathsep) if item)]:
        normalized = value.rstrip("/\\") or value
        if not value or normalized in seen:
            continue
        seen.add(normalized)
        items.append(value)
    return os.pathsep.join(items)
