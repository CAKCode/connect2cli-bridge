from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


WorkspaceScope = Literal["user", "room"]
SkillLayerName = Literal["global", "workspace"]


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    source_dir: Path


@dataclass(frozen=True)
class BotConfig:
    bot_id: str
    bot_name: str
    bot_secret: str | None
    source: SourceConfig
    runtime_root: Path
    global_skill_dir: Path
    chatfile_root: Path
    file_send_roots: tuple[Path, ...] = ()
    max_upload_size: int = 100 * 1024 * 1024


@dataclass
class WeComBotRuntime:
    config: BotConfig
    ws: object | None = None
    ws_send_lock: object | None = None
    pending_requests: dict[str, object] | None = None
    pending_streams: dict[str, dict | list[dict]] | None = None
    pending_finals: dict[str, dict | list[dict]] | None = None
    connected: bool = False
    reply_states: dict[str, "ReplyState"] = field(default_factory=dict)
    active_processes: dict[str, object] = field(default_factory=dict)
    message_tasks: set[object] = field(default_factory=set)
    active_message_tasks: dict[str, object] = field(default_factory=dict)
    last_error: str | None = None
    last_status: str | None = None
    resume_candidates: dict[str, list[dict[str, str | int]]] = field(default_factory=dict)
    resume_selection_expires_at: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkspaceRef:
    workspace_id: str
    scope: WorkspaceScope
    owner_user_id: str | None
    owner_room_id: str | None
    chat_key: str
    source_dir: Path
    source_key: str
    root_dir: Path
    project_dir: Path
    skill_dir: Path
    state_dir: Path
    workfile_dir: Path | None
    roomfile_dir: Path | None
    lock_file: Path
    metadata_file: Path


@dataclass(frozen=True)
class ProvisionedWorkspace:
    workspace: WorkspaceRef
    source_mode: Literal["git", "copy"]
    source_revision: str | None
    initialized_at: int
    updated_at: int
    project_ready: bool


@dataclass(frozen=True)
class SkillDefinition:
    name: str
    layer: SkillLayerName
    root_dir: Path
    skill_file: Path


@dataclass(frozen=True)
class SkillLayer:
    name: SkillLayerName
    root_dir: Path
    skills: dict[str, SkillDefinition]


@dataclass(frozen=True)
class ResolvedSkillSpace:
    layers: tuple[SkillLayer, ...]
    effective_skills: dict[str, SkillDefinition]


@dataclass(frozen=True)
class WorkspaceRuntimeContext:
    workspace: WorkspaceRef
    project_dir: Path
    chatfile_dir: Path
    export_dir: Path
    workfile_dir: Path | None
    roomfile_dir: Path | None
    allowed_file_roots: tuple[Path, ...]
    max_upload_size: int
    global_skill_dir: Path
    effective_skill_names: tuple[str, ...]
    env: dict[str, str]


@dataclass(frozen=True)
class SessionRecord:
    session_id: str
    bot_id: str
    bot_name: str
    chat_key: str
    workspace_id: str
    workspace_scope: WorkspaceScope
    project_dir: Path
    chatfile_dir: Path
    workfile_dir: Path | None
    roomfile_dir: Path | None
    created_at: int
    updated_at: int
    thread_id: str | None = None
    last_run_at: int | None = None


@dataclass(frozen=True)
class CodexLaunchSpec:
    session: SessionRecord
    workspace: ProvisionedWorkspace
    runtime_context: WorkspaceRuntimeContext
    cwd: Path
    env: dict[str, str]


@dataclass(frozen=True)
class RunnerInvocation:
    argv: tuple[str, ...]
    cwd: Path
    env: dict[str, str]
    prompt: str


@dataclass(frozen=True)
class WeComTextMessage:
    req_id: str
    chat_key: str
    content: str
    raw_payload: dict


@dataclass(frozen=True)
class FileSendRequest:
    session_id: str
    chat_key: str
    workspace_id: str
    file_path: Path
    file_name: str


@dataclass
class ReplyState:
    req_id: str
    session_id: str
    chat_key: str
    started_at: float
    last_sent_at: float
    proactive: bool = False
    proactive_notice_sent: bool = False
    pending_stream_payload: dict | None = None
    pending_final_payload: dict | None = None
    pending_stream_payloads: list[dict] | None = None
    pending_final_payloads: list[dict] | None = None
    proactive_status_sent_at: float = 0.0


@dataclass(frozen=True)
class ScheduleDefinition:
    schedule_id: str
    chat_key: str
    message: str
    cron: str | None
    timezone_name: str | None
    next_run_at: int
    enabled: bool
    max_runs: int | None
    run_count: int
    misfire_policy: str
    concurrency_policy: str
    run_at_ms: int | None = None


@dataclass(frozen=True)
class ScheduledJob:
    request_id: str
    schedule_id: str
    chat_key: str
    message: str
    run_at: int
    created_at: int
