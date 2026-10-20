from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


@dataclass(frozen=True)
class CodexStatusSnapshot:
    available: bool
    version: str | None
    model: str | None
    reasoning_effort: str | None
    reasoning_summary: str | None
    model_provider: str | None
    directory: Path
    permissions: str | None
    agents_files: tuple[Path, ...]
    account: str | None
    collaboration_mode: str | None
    thread_id: str | None
    total_tokens: int | None
    input_tokens: int | None
    output_tokens: int | None
    context_tokens: int | None
    context_window: int | None
    limits: str | None
    model_source: str
    unavailable_reason: str | None = None


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            payload = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalized_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalized_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _find_thread_rollout(codex_home: Path, thread_id: str | None) -> Path | None:
    normalized_thread_id = _normalized_text(thread_id)
    sessions_root = codex_home / "sessions"
    if not normalized_thread_id or not sessions_root.exists() or not sessions_root.is_dir():
        return None
    latest: tuple[int, Path] | None = None
    for path in sessions_root.rglob("*.jsonl"):
        if normalized_thread_id not in path.name:
            continue
        try:
            modified_at = path.stat().st_mtime_ns
        except OSError:
            continue
        if latest is None or modified_at > latest[0]:
            latest = (modified_at, path)
    return latest[1] if latest is not None else None


def _read_rollout_status(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    status: dict[str, Any] = {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not any(marker in line for marker in ("session_meta", "turn_context", "token_count")):
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("type") == "session_meta":
                    metadata = payload.get("payload")
                    if isinstance(metadata, dict):
                        status["version"] = _normalized_text(metadata.get("cli_version"))
                    continue
                if payload.get("type") == "turn_context":
                    context = payload.get("payload")
                    if isinstance(context, dict):
                        status["turn_context"] = context
                    continue
                event = payload.get("payload")
                if isinstance(event, dict) and event.get("type") == "token_count":
                    status["token_count"] = event
    except OSError:
        return {}
    return status


def _sanitize_provider_url(value: Any) -> str | None:
    text = _normalized_text(value)
    if not text:
        return None
    try:
        parsed = urlsplit(text)
    except ValueError:
        return None
    if not parsed.scheme or not parsed.hostname:
        return None
    port = f":{parsed.port}" if parsed.port is not None else ""
    return urlunsplit((parsed.scheme, f"{parsed.hostname}{port}", parsed.path.rstrip("/"), "", ""))


def _format_provider(config: dict[str, Any]) -> str | None:
    provider_id = _normalized_text(config.get("model_provider"))
    providers = config.get("model_providers")
    provider_config = providers.get(provider_id) if provider_id and isinstance(providers, dict) else None
    if not isinstance(provider_config, dict):
        return provider_id
    name = _normalized_text(provider_config.get("name")) or provider_id
    base_url = _sanitize_provider_url(provider_config.get("base_url"))
    return " - ".join(part for part in (name, base_url) if part) or None


def _format_permissions(turn_context: dict[str, Any], exec_mode: str) -> str:
    sandbox = turn_context.get("sandbox_policy")
    sandbox_type = _normalized_text(sandbox.get("type")) if isinstance(sandbox, dict) else None
    approval = _normalized_text(turn_context.get("approval_policy"))
    if sandbox_type == "danger-full-access" or (not sandbox_type and exec_mode == "host"):
        return "Full access"
    if sandbox_type == "read-only":
        return "Read only"
    if approval == "never":
        return "Workspace (no approvals)"
    return "Workspace (ask for approval)"


def _find_agents_files(default_codex_home: Path, directory: Path) -> tuple[Path, ...]:
    candidates = [default_codex_home / "AGENTS.md"]
    candidates.extend(parent / "AGENTS.md" for parent in (directory, *directory.parents))
    found: list[Path] = []
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved.is_file() and resolved not in found:
            found.append(resolved)
    return tuple(found)


def _account_label(default_codex_home: Path) -> str:
    auth = _read_json(default_codex_home / "auth.json")
    auth_mode = str(auth.get("auth_mode") or "").strip().lower()
    if _normalized_text(auth.get("OPENAI_API_KEY")) or auth_mode in {"api", "api_key", "apikey"}:
        return "API key configured"
    if isinstance(auth.get("tokens"), dict) or auth_mode in {"chatgpt", "oauth"}:
        return "ChatGPT"
    return "Not logged in"


def _format_reset_time(value: Any) -> str | None:
    timestamp = _normalized_int(value)
    if timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except (OSError, OverflowError, ValueError):
        return None


def _format_limit(name: str, payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    try:
        used_percent = float(payload.get("used_percent"))
    except (TypeError, ValueError):
        used_percent = None
    parts = [name]
    if used_percent is not None:
        parts.append(f"{max(0.0, 100.0 - used_percent):g}% left")
    reset_at = _format_reset_time(payload.get("resets_at"))
    if reset_at:
        parts.append(f"resets {reset_at}")
    return ": ".join(parts[:2]) + (f", {parts[2]}" if len(parts) > 2 else "") if len(parts) > 1 else None


def _format_limits(token_count: dict[str, Any]) -> str:
    rate_limits = token_count.get("rate_limits")
    if not isinstance(rate_limits, dict):
        return "data not available"
    limits = [
        item
        for item in (
            _format_limit("primary", rate_limits.get("primary")),
            _format_limit("secondary", rate_limits.get("secondary")),
        )
        if item
    ]
    return "; ".join(limits) if limits else "data not available"


def _usage_values(token_count: dict[str, Any]) -> tuple[int | None, int | None, int | None, int | None, int | None]:
    info = token_count.get("info")
    if not isinstance(info, dict):
        return None, None, None, None, None
    total = info.get("total_token_usage")
    last = info.get("last_token_usage")
    total = total if isinstance(total, dict) else {}
    last = last if isinstance(last, dict) else {}
    return (
        _normalized_int(total.get("total_tokens")),
        _normalized_int(total.get("input_tokens")),
        _normalized_int(total.get("output_tokens")),
        _normalized_int(last.get("total_tokens")),
        _normalized_int(info.get("model_context_window")),
    )


def collect_codex_status(
    *,
    default_codex_home: Path,
    session_codex_home: Path,
    directory: Path,
    thread_id: str | None,
    exec_mode: str,
    agent_backend: str,
) -> CodexStatusSnapshot:
    if str(agent_backend or "").strip().lower() != "codex":
        return CodexStatusSnapshot(
            available=False,
            version=None,
            model=None,
            reasoning_effort=None,
            reasoning_summary=None,
            model_provider=None,
            directory=directory,
            permissions=None,
            agents_files=(),
            account=None,
            collaboration_mode=None,
            thread_id=_normalized_text(thread_id),
            total_tokens=None,
            input_tokens=None,
            output_tokens=None,
            context_tokens=None,
            context_window=None,
            limits=None,
            model_source="unavailable",
            unavailable_reason=f"agent backend: {agent_backend}",
        )

    config_path = default_codex_home / "config.toml"
    if not config_path.is_file():
        config_path = session_codex_home / "config.toml"
    config = _read_toml(config_path)
    rollout_path = _find_thread_rollout(session_codex_home, thread_id)
    if rollout_path is None:
        rollout_path = _find_thread_rollout(default_codex_home, thread_id)
    rollout = _read_rollout_status(rollout_path)
    turn_context = rollout.get("turn_context")
    turn_context = turn_context if isinstance(turn_context, dict) else {}
    token_count = rollout.get("token_count")
    token_count = token_count if isinstance(token_count, dict) else {}
    total_tokens, input_tokens, output_tokens, context_tokens, context_window = _usage_values(token_count)
    rollout_model = _normalized_text(turn_context.get("model"))
    configured_model = _normalized_text(config.get("model"))
    collaboration = turn_context.get("collaboration_mode")
    collaboration_mode = _normalized_text(collaboration.get("mode")) if isinstance(collaboration, dict) else None

    version = _normalized_text(rollout.get("version"))
    if not version:
        version = _normalized_text(_read_json(default_codex_home / "version.json").get("latest_version"))

    return CodexStatusSnapshot(
        available=True,
        version=version,
        model=rollout_model or configured_model,
        reasoning_effort=_normalized_text(turn_context.get("effort")) or _normalized_text(config.get("model_reasoning_effort")),
        reasoning_summary=_normalized_text(turn_context.get("summary")) or _normalized_text(config.get("model_reasoning_summary")),
        model_provider=_format_provider(config),
        directory=directory,
        permissions=_format_permissions(turn_context, str(exec_mode or "").strip().lower()),
        agents_files=_find_agents_files(default_codex_home, directory),
        account=_account_label(default_codex_home),
        collaboration_mode=collaboration_mode or "default",
        thread_id=_normalized_text(thread_id),
        total_tokens=total_tokens,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        context_tokens=context_tokens,
        context_window=context_window,
        limits=_format_limits(token_count),
        model_source="thread" if rollout_model else "config",
    )


def _display_name(value: str | None) -> str:
    text = _normalized_text(value)
    return text[:1].upper() + text[1:] if text else "-"


def format_codex_status(snapshot: CodexStatusSnapshot) -> str:
    if not snapshot.available:
        return f"Codex /status: unavailable ({snapshot.unavailable_reason or 'unknown reason'})"

    model = snapshot.model or "-"
    model_details = [
        detail
        for detail in (
            f"reasoning {snapshot.reasoning_effort}" if snapshot.reasoning_effort else None,
            f"summaries {snapshot.reasoning_summary}" if snapshot.reasoning_summary else None,
        )
        if detail
    ]
    if model_details:
        model = f"{model} ({', '.join(model_details)})"
    if snapshot.model_source == "config" and not snapshot.thread_id:
        model = f"{model} [next fresh exec]"

    if snapshot.total_tokens is None:
        token_usage = "unavailable"
    else:
        token_usage = (
            f"{snapshot.total_tokens} total "
            f"({snapshot.input_tokens or 0} input + {snapshot.output_tokens or 0} output)"
        )
    if snapshot.context_tokens is None or snapshot.context_window is None or snapshot.context_window <= 0:
        context_window = "unavailable"
    else:
        percent_left = max(0, min(100, round(100 * (1 - snapshot.context_tokens / snapshot.context_window))))
        context_window = f"{percent_left}% left ({snapshot.context_tokens} used / {snapshot.context_window})"

    agents = ", ".join(str(path) for path in snapshot.agents_files) or "<none>"
    return "\n".join(
        [
            f"Codex /status (v{snapshot.version or '-'})",
            f"Model: {model}",
            f"Model provider: {snapshot.model_provider or '-'}",
            f"Directory: {snapshot.directory}",
            f"Permissions: {snapshot.permissions or '-'}",
            f"Agents.md: {agents}",
            f"Account: {snapshot.account or '-'}",
            f"Collaboration mode: {_display_name(snapshot.collaboration_mode)}",
            f"Session: {snapshot.thread_id or '<not started>'}",
            f"Token usage: {token_usage}",
            f"Context window: {context_window}",
            f"Limits: {snapshot.limits or 'data not available'}",
        ]
    )
