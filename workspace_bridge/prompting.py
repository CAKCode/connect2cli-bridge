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
            f"PROJECT_DIR: {launch.cwd}",
            f"WORKSPACE_ROOT: {workspace.root_dir}",
            f"WORKSPACE_SKILL_DIR: {workspace.skill_dir}",
            f"GLOBAL_SKILL_DIR: {bot.global_skill_dir}",
            f"CHATFILE_DIR: {launch.runtime_context.chatfile_dir}",
            f"EXPORT_DIR: {launch.runtime_context.export_dir}",
            f"effectiveSkills: {skill_names}",
            "Codex must run with cwd=PROJECT_DIR, not SOURCE_DIR.",
            "SOURCE_DIR is the shared upstream source root for this workspace.",
            "WORKSPACE_SKILL_DIR overrides GLOBAL_SKILL_DIR when the same skill name exists.",
            "Create user-visible exported files under EXPORT_DIR or CHATFILE_DIR.",
            "Only CHATFILE_DIR is guaranteed to be suitable for send-back/export flows.",
            "[/BridgeContext]",
        ]
    )


def build_prompt(bot: BotConfig, launch: CodexLaunchSpec, user_text: str) -> str:
    body = str(user_text or "").strip()
    return f"{build_bridge_context(bot, launch)}\n\nUser request:\n{body}"
