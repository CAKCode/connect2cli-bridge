from __future__ import annotations

import json
import os
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


AgentBackend = Literal["codex", "claude"]
PROGRESS_SUMMARY_MAX_CHARS = 480


@dataclass(frozen=True)
class AgentProgress:
    status: str
    summary: str = ""

    def updated(self, update: AgentProgress) -> AgentProgress:
        return AgentProgress(update.status, update.summary or self.summary)


def _progress_summary(value: object) -> str:
    """Normalize an explicit agent-provided stage summary for display."""
    if isinstance(value, list):
        value = " ".join(_progress_summary(part) for part in value)
    elif isinstance(value, dict):
        value = value.get("text")
    if not isinstance(value, str):
        return ""
    text = " ".join(value.split())
    if len(text) <= PROGRESS_SUMMARY_MAX_CHARS:
        return text
    return text[: PROGRESS_SUMMARY_MAX_CHARS - 1].rstrip() + "…"


def _codex_item_progress(item: object, event_type: str) -> AgentProgress | None:
    """Keep public descriptions separate from tool and execution stages."""
    if not isinstance(item, dict):
        return None
    item_type = str(item.get("type") or "").strip().lower()
    if item_type in {"agent_message", "agentmessage"}:
        if event_type != "item.completed":
            return None
        summary = _progress_summary(item.get("text"))
        return AgentProgress("处理中", summary) if summary else None
    if item_type == "command_execution":
        return AgentProgress("执行中")
    if item_type in {"mcp_tool_call", "function_call"}:
        return AgentProgress("处理中")
    if item_type == "file_change":
        return AgentProgress("修改中")
    if item_type == "web_search":
        return AgentProgress("检索中")
    if item_type == "reasoning":
        return AgentProgress("分析中", _progress_summary(item.get("summary")))
    return None


def extract_agent_progress(backend: AgentBackend, stdout: str) -> AgentProgress | None:
    """Extract a stage and any public description, excluding raw tool output."""
    latest = None
    for line in str(stdout or "").splitlines():
        try:
            payload = json.loads(line)
        except (ValueError, TypeError):
            continue
        if not isinstance(payload, dict):
            continue
        progress = None
        if backend == "claude":
            payload_type = str(payload.get("type") or "").strip().lower()
            message = payload.get("message")
            content = message.get("content") if isinstance(message, dict) else None
            if payload_type == "assistant" and isinstance(content, list):
                summary_parts = []
                status = ""
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    block_type = str(block.get("type") or "").strip().lower()
                    if block_type == "tool_use":
                        status = "处理中"
                    elif block_type == "text":
                        text = _progress_summary(block.get("text"))
                        if text:
                            summary_parts.append(text)
                            status = status or "整理中"
                if status:
                    progress = AgentProgress(status, _progress_summary(" ".join(summary_parts)))
            elif payload_type in {"tool_use", "tool_result"}:
                progress = AgentProgress("处理中")
            if progress is not None:
                latest = latest.updated(progress) if latest is not None else progress
            continue

        event_type = str(payload.get("type") or "").strip().replace("/", ".")
        item = payload.get("item")
        if event_type in {"item.started", "item.updated", "item.completed"}:
            progress = _codex_item_progress(item, event_type)
        elif event_type in {
            "item.reasoning.summaryPartAdded",
            "item.reasoning.summaryTextDelta",
            "item.reasoning.summaryTextDone",
        }:
            summary = _progress_summary(
                payload.get("summary") or payload.get("text") or payload.get("content") or payload.get("delta")
            )
            if summary:
                progress = AgentProgress("分析中", summary)
        elif event_type == "turn.started":
            progress = AgentProgress("分析中")
        elif event_type == "error":
            progress = AgentProgress("重试中")
        # Raw reasoning, command output and unfinished answer deltas do not
        # replace the last complete public description.
        if progress is not None:
            latest = latest.updated(progress) if latest is not None else progress
    return latest


def normalize_agent_backend(value: str | None) -> AgentBackend:
    text = str(value or "").strip().lower()
    if text in {"none", "null"}:
        text = ""
    if not text or text == "codex":
        return "codex"
    if text in {"claude", "claude-code", "claudecode"}:
        return "claude"
    raise ValueError(f"unsupported agent backend: {value}")


def parse_command_to_argv(command: str) -> tuple[str, ...]:
    try:
        return tuple(shlex.split(command))
    except ValueError:
        return (command,)


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
        has_permission_mode = any(
            item == "--permission-mode" or item.startswith("--permission-mode=")
            for item in base
        )
        if mode == "host" and not has_permission_mode:
            argv.extend(["--permission-mode", "bypassPermissions"])
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
        if command_name not in {"codex", "codex.js"} and not command_name.startswith("codex-"):
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
        if candidate.is_file():
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
