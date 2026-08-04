from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .agent_backends import normalize_agent_backend
from .api_security import api_auth_enabled, is_loopback_address
from .runtime import VALID_CODEX_EXEC_MODES, build_bot_config


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    if not path.is_file():
        raise ValueError(f"env file is not a file: {path}")
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized = value.strip()
        if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
            normalized = normalized[1:-1]
        values[key.strip()] = normalized
    return values


def merge_env(environ: dict[str, str] | None = None, *, env_file: Path | None = None) -> dict[str, str]:
    merged = dict(read_env_file(env_file)) if env_file else {}
    merged.update(environ or os.environ)
    return merged


def resolve_bind(value: str) -> tuple[str, int]:
    text = str(value or "").strip() or "127.0.0.1:6288"
    if text.startswith("["):
        end = text.find("]")
        if end == -1 or end + 1 >= len(text) or text[end + 1] != ":":
            raise ValueError(f"invalid bind address: {text}")
        host = text[1:end].strip()
        port = text[end + 2 :].strip()
    else:
        host, sep, port = text.rpartition(":")
        if not sep or not host or not port:
            raise ValueError(f"invalid bind address: {text}")
        if ":" in host:
            raise ValueError(f"invalid bind address: {text}")
        host = host.strip()
        port = port.strip()
    if not host:
        raise ValueError(f"invalid bind address: {text}")
    try:
        port_value = int(port)
    except ValueError as exc:
        raise ValueError(f"invalid bind port: {port}") from exc
    if not 1 <= port_value <= 65535:
        raise ValueError(f"invalid bind port: {port_value}")
    return host, port_value


def require_env(values: dict[str, str], key: str) -> str:
    value = _normalize_optional_text(values.get(key))
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _normalize_optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null"}:
        return None
    return text


def read_secret_file(secret_file: Path) -> str:
    if not secret_file.exists():
        raise ValueError(f"secret file not found: {secret_file}")
    if not secret_file.is_file():
        raise ValueError(f"secret file is not a file: {secret_file}")
    value = secret_file.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"secret file is empty: {secret_file}")
    return value


@dataclass(frozen=True)
class AppConfig:
    bind_host: str
    bind_port: int
    api_token: str | None
    api_basic_auth: str | None
    runtime_root: Path
    source_dir: Path
    workspace_namespace: str
    chatfile_root: Path
    codex_output_root: Path
    workspace_mode: str
    codex_exec_mode: str
    agent_backend: str
    agent_command: str | None
    agent_run_as_user: str | None
    agent_run_as_group: str | None
    agent_runtime_root: Path | None
    file_send_roots: tuple[Path, ...]
    max_upload_size: int
    wecom_enabled: bool
    schedule_poll_ms: int
    wecom_subscribe_timeout_sec: int
    bot_secret_file: Path
    bot_secret: str
    bot_id: str
    bot_name: str


def load_app_config(environ: dict[str, str] | None = None, *, env_file: Path | None = None) -> AppConfig:
    values = merge_env(environ, env_file=env_file)
    bind_host, bind_port = resolve_bind(values.get("BRIDGE_BIND", "127.0.0.1:6288"))
    api_token = _normalize_optional_text(values.get("BRIDGE_TOKEN"))
    api_basic_auth = _normalize_optional_text(values.get("BRIDGE_BASIC_AUTH"))
    if not is_loopback_address(bind_host) and not api_auth_enabled(api_token or "", api_basic_auth or ""):
        raise ValueError("non-loopback BRIDGE_BIND requires BRIDGE_TOKEN or BRIDGE_BASIC_AUTH")
    runtime_root = Path(values.get("RUNTIME_ROOT") or ".workspace-bridge-runtime").expanduser().resolve()
    if runtime_root.exists() and not runtime_root.is_dir():
        raise ValueError(f"runtime root is not a directory: {runtime_root}")
    source_dir = Path(require_env(values, "WECOM_BOT_SOURCE_DIR")).expanduser().resolve()
    if not source_dir.exists():
        raise ValueError(f"WECOM_BOT_SOURCE_DIR does not exist: {source_dir}")
    if not source_dir.is_dir():
        raise ValueError(f"WECOM_BOT_SOURCE_DIR is not a directory: {source_dir}")
    chatfile_root = Path(values.get("CHATFILE_ROOT") or (runtime_root / "chatfiles")).expanduser().resolve()
    if chatfile_root.exists() and not chatfile_root.is_dir():
        raise ValueError(f"chatfile root is not a directory: {chatfile_root}")
    codex_output_root = Path(values.get("CODEX_OUTPUT_ROOT") or (runtime_root / "codex-output")).expanduser().resolve()
    if codex_output_root.exists() and not codex_output_root.is_dir():
        raise ValueError(f"codex output root is not a directory: {codex_output_root}")
    file_send_roots = tuple(
        Path(item.strip()).expanduser().resolve()
        for item in str(values.get("FILE_SEND_ROOTS") or "").split(",")
        if item.strip()
    )
    for root in file_send_roots:
        if root.exists() and not root.is_dir():
            raise ValueError(f"file send root is not a directory: {root}")
    max_upload_size = max(1, int(str(values.get("MAX_UPLOAD_SIZE") or str(100 * 1024 * 1024)).strip()))
    bot_secret_file = Path(require_env(values, "WECOM_BOT_SECRET_FILE")).expanduser().resolve()
    bot_secret = read_secret_file(bot_secret_file)
    workspace_mode = (str(values.get("WECOM_BOT_WORKSPACE_MODE") or values.get("WECOM_WORKSPACE_MODE") or "").strip().lower() or "")
    if workspace_mode in {"none", "null"}:
        workspace_mode = ""
    workspace_namespace = _normalize_optional_text(
        values.get("WORKSPACE_NAMESPACE")
        or values.get("WECOM_BOT_WORKSPACE_NAMESPACE")
        or values.get("WECOM_WORKSPACE_NAMESPACE")
    ) or require_env(values, "WECOM_BOT_ID")
    codex_exec_mode = (_normalize_optional_text(values.get("CODEX_EXEC_MODE")) or "host").lower()
    if codex_exec_mode not in VALID_CODEX_EXEC_MODES:
        raise ValueError(f"invalid codex_exec_mode: {values.get('CODEX_EXEC_MODE')}")
    return AppConfig(
        bind_host=bind_host,
        bind_port=bind_port,
        api_token=api_token,
        api_basic_auth=api_basic_auth,
        runtime_root=runtime_root,
        source_dir=source_dir,
        workspace_namespace=workspace_namespace,
        chatfile_root=chatfile_root,
        codex_output_root=codex_output_root,
        workspace_mode=workspace_mode,
        codex_exec_mode=codex_exec_mode,
        agent_backend=normalize_agent_backend(values.get("WECOM_AGENT_BACKEND") or values.get("AGENT_BACKEND") or "codex"),
        agent_command=_normalize_optional_text(values.get("WECOM_AGENT_COMMAND") or values.get("AGENT_COMMAND")),
        agent_run_as_user=None,
        agent_run_as_group=None,
        agent_runtime_root=None,
        file_send_roots=file_send_roots,
        max_upload_size=max_upload_size,
        wecom_enabled=str(values.get("WECOM_ENABLED") or "false").strip().lower() in {"1", "true", "yes", "on"},
        schedule_poll_ms=max(1000, int(str(values.get("SCHEDULE_POLL_MS") or "5000").strip())),
        wecom_subscribe_timeout_sec=max(5, int(str(values.get("WECOM_SUBSCRIBE_TIMEOUT_SEC") or "30").strip())),
        bot_secret_file=bot_secret_file,
        bot_secret=bot_secret,
        bot_id=require_env(values, "WECOM_BOT_ID"),
        bot_name=_normalize_optional_text(values.get("WECOM_BOT_NAME")) or "default",
    )


def build_bot_from_app_config(config: AppConfig):
    bot = build_bot_config(
        bot_id=config.bot_id,
        bot_name=config.bot_name,
        source_dir=config.source_dir,
        runtime_root=config.runtime_root,
        workspace_namespace=config.workspace_namespace,
        chatfile_root=config.chatfile_root,
        workspace_mode=config.workspace_mode,
        codex_exec_mode=config.codex_exec_mode,
        agent_backend=config.agent_backend,
        agent_command=config.agent_command,
        agent_run_as_user=config.agent_run_as_user,
        agent_run_as_group=config.agent_run_as_group,
        agent_runtime_root=config.agent_runtime_root,
        file_send_roots=config.file_send_roots,
        max_upload_size=config.max_upload_size,
    )
    return type(bot)(
        bot_id=bot.bot_id,
        bot_name=bot.bot_name,
        bot_secret=config.bot_secret,
        source=bot.source,
        runtime_root=bot.runtime_root,
        workspace_namespace=bot.workspace_namespace,
        chatfile_root=bot.chatfile_root,
        workspace_mode=bot.workspace_mode,
        codex_exec_mode=bot.codex_exec_mode,
        agent_backend=bot.agent_backend,
        agent_command=bot.agent_command,
        agent_run_as_user=bot.agent_run_as_user,
        agent_run_as_group=bot.agent_run_as_group,
        agent_runtime_root=bot.agent_runtime_root,
        file_send_roots=bot.file_send_roots,
        max_upload_size=bot.max_upload_size,
        platform=bot.platform,
    )
