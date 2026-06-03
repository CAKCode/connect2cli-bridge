from __future__ import annotations

from .models import BotConfig, CodexLaunchSpec


def build_bridge_context(bot: BotConfig, launch: CodexLaunchSpec) -> str:
    workspace = launch.workspace.workspace
    chat_type = "group" if workspace.scope == "room" or workspace.owner_room_id else "single"
    user_id = workspace.owner_user_id or "-"
    room_id = workspace.owner_room_id or "-"
    skill_names = ", ".join(launch.runtime_context.effective_skill_names) or "-"

    return "\n".join(
        [
            "[BridgeContext]",
            f"botName: {bot.bot_name}",
            f"botId: {bot.bot_id}",
            f"chatKey: {launch.session.chat_key}",
            f"chatType: {chat_type}",
            f"userId: {user_id}",
            f"roomId: {room_id}",
            f"sessionId: {launch.session.session_id}",
            f"workspaceId: {workspace.workspace_id}",
            f"workspaceScope: {workspace.scope}",
            f"SOURCE_DIR: {workspace.source_dir}",
            f"PROJECT_DIR: {launch.runtime_context.project_dir}",
            f"WORKSPACE_SKILL_DIR: {workspace.skill_dir}",
            "HOME_CODEX_SKILLS_DIR: ~/.codex/skills",
            f"CHATFILE_DIR: {launch.runtime_context.chatfile_dir}",
            f"WORKFILE_DIR: {launch.runtime_context.workfile_dir or '-'}",
            f"ROOMFILE_DIR: {launch.runtime_context.roomfile_dir or '-'}",
            f"effectiveSkills: {skill_names}",
            "Run in PROJECT_DIR.",
            "WORKSPACE_SKILL_DIR overrides HOME_CODEX_SKILLS_DIR on name conflicts.",
            "Export user-visible files under CHATFILE_DIR.",
            "[/BridgeContext]",
        ]
    )


def build_prompt(bot: BotConfig, launch: CodexLaunchSpec, user_text: str) -> str:
    body = str(user_text or "").strip()
    return f"{build_bridge_context(bot, launch)}\n\nUser request:\n{body}"
