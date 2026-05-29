from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .runtime import build_bot_config


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def merge_env(environ: dict[str, str] | None = None, *, env_file: Path | None = None) -> dict[str, str]:
    merged = dict(read_env_file(env_file)) if env_file else {}
    merged.update(environ or os.environ)
    return merged


def resolve_bind(value: str) -> tuple[str, int]:
    text = str(value or "").strip() or "127.0.0.1:6288"
    host, sep, port = text.rpartition(":")
    if not sep or not host or not port:
        raise ValueError(f"invalid bind address: {text}")
    return host, int(port)


def require_env(values: dict[str, str], key: str) -> str:
    value = str(values.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def read_secret_file(secret_file: Path) -> str:
    value = secret_file.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"secret file is empty: {secret_file}")
    return value


@dataclass(frozen=True)
class AppConfig:
    bind_host: str
    bind_port: int
    runtime_root: Path
    source_dir: Path
    global_skill_dir: Path
    chatfile_root: Path
    codex_output_root: Path
    codex_exec_mode: str
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
    runtime_root = Path(values.get("RUNTIME_ROOT") or ".workspace-bridge-runtime").expanduser().resolve()
    source_dir = Path(require_env(values, "WECOM_BOT_SOURCE_DIR")).expanduser().resolve()
    global_skill_dir = Path(values.get("GLOBAL_SKILL_DIR") or (runtime_root / "skills" / "global")).expanduser().resolve()
    chatfile_root = Path(values.get("CHATFILE_ROOT") or (runtime_root / "chatfiles")).expanduser().resolve()
    codex_output_root = Path(values.get("CODEX_OUTPUT_ROOT") or (runtime_root / "codex-output")).expanduser().resolve()
    file_send_roots = tuple(
        Path(item.strip()).expanduser().resolve()
        for item in str(values.get("FILE_SEND_ROOTS") or "").split(",")
        if item.strip()
    )
    max_upload_size = max(1, int(str(values.get("MAX_UPLOAD_SIZE") or str(100 * 1024 * 1024)).strip()))
    bot_secret_file = Path(require_env(values, "WECOM_BOT_SECRET_FILE")).expanduser().resolve()
    bot_secret = read_secret_file(bot_secret_file)
    return AppConfig(
        bind_host=bind_host,
        bind_port=bind_port,
        runtime_root=runtime_root,
        source_dir=source_dir,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
        codex_output_root=codex_output_root,
        codex_exec_mode=(str(values.get("CODEX_EXEC_MODE") or "host").strip().lower() or "host"),
        file_send_roots=file_send_roots,
        max_upload_size=max_upload_size,
        wecom_enabled=str(values.get("WECOM_ENABLED") or "false").strip().lower() in {"1", "true", "yes", "on"},
        schedule_poll_ms=max(1000, int(str(values.get("SCHEDULE_POLL_MS") or "5000").strip())),
        wecom_subscribe_timeout_sec=max(5, int(str(values.get("WECOM_SUBSCRIBE_TIMEOUT_SEC") or "30").strip())),
        bot_secret_file=bot_secret_file,
        bot_secret=bot_secret,
        bot_id=require_env(values, "WECOM_BOT_ID"),
        bot_name=str(values.get("WECOM_BOT_NAME") or "default").strip() or "default",
    )


def build_bot_from_app_config(config: AppConfig):
    bot = build_bot_config(
        bot_id=config.bot_id,
        bot_name=config.bot_name,
        source_dir=config.source_dir,
        runtime_root=config.runtime_root,
        global_skill_dir=config.global_skill_dir,
        chatfile_root=config.chatfile_root,
        codex_exec_mode=config.codex_exec_mode,
        file_send_roots=config.file_send_roots,
        max_upload_size=config.max_upload_size,
    )
    return type(bot)(
        bot_id=bot.bot_id,
        bot_name=bot.bot_name,
        bot_secret=config.bot_secret,
        source=bot.source,
        runtime_root=bot.runtime_root,
        global_skill_dir=bot.global_skill_dir,
        chatfile_root=bot.chatfile_root,
        codex_exec_mode=bot.codex_exec_mode,
        file_send_roots=bot.file_send_roots,
        max_upload_size=bot.max_upload_size,
    )
