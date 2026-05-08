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


@dataclass
class WeComBotRuntime:
    config: BotConfig
    ws: object | None = None
    pending_requests: dict[str, object] | None = None
    pending_streams: dict[str, dict] | None = None
    pending_finals: dict[str, dict] | None = None
    connected: bool = False
    reply_states: dict[str, "ReplyState"] = field(default_factory=dict)
    last_error: str | None = None
    last_status: str | None = None


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
    allowed_file_roots: tuple[Path, ...]
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
    created_at: int
    updated_at: int


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
    proactive_status_sent_at: float = 0.0
