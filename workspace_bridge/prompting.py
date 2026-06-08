from __future__ import annotations

from pathlib import Path

from .models import BotConfig, CodexLaunchSpec


REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_FILE_SEND_COMMAND = REPO_ROOT / "send_file.py"
LOCAL_SEND_MESSAGE_COMMAND = REPO_ROOT / "send_message.py"
LOCAL_SCHEDULE_MESSAGE_COMMAND = REPO_ROOT / "schedule_message.py"


def build_bridge_context(bot: BotConfig, launch: CodexLaunchSpec) -> str:
    workspace = launch.workspace.workspace
    chat_type = "group" if workspace.scope == "room" or workspace.owner_room_id else "single"
    user_id = workspace.owner_user_id or "-"
    room_id = workspace.owner_room_id or "-"
    skill_names = ", ".join(launch.runtime_context.effective_skill_names) or "-"
    allowed_file_roots = ", ".join(str(root) for root in launch.runtime_context.allowed_file_roots)
    execution_mode = launch.runtime_context.codex_exec_mode
    execution_mode_note = (
        "Local network access is blocked inside the Codex sandbox for this bridge. "
        "Never probe localhost ports or use curl/python sockets to send files."
        if execution_mode == "sandboxed"
        else "This bridge is running Codex in host mode without the built-in sandbox. "
        "Use shell and network access carefully, and still prefer the localSendFileCommand when sending files back."
    )

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
            f"executionMode: {execution_mode}",
            f"workspaceId: {workspace.workspace_id}",
            f"workspaceScope: {workspace.scope}",
            f"SOURCE_DIR: {workspace.source_dir}",
            f"CWD_DIR: {launch.cwd}",
            f"PROJECT_DIR: {launch.runtime_context.project_dir}",
            f"WORKSPACE_SKILL_DIR: {workspace.skill_dir}",
            "HOME_CODEX_SKILLS_DIR: ~/.codex/skills",
            f"CHATFILE_DIR: {launch.runtime_context.chatfile_dir}",
            f"WORKFILE_DIR: {launch.runtime_context.workfile_dir or '-'}",
            f"ROOMFILE_DIR: {launch.runtime_context.roomfile_dir or '-'}",
            f"effectiveSkills: {skill_names}",
            (
                "localSendFileCommand: "
                f"python3 {LOCAL_FILE_SEND_COMMAND} --chat-key '{launch.session.chat_key}' "
                f"--bot-config-id '{bot.bot_id}' --bot-name '{bot.bot_name}' --file-path ABSOLUTE_FILE_PATH "
                f"(fallback: --session-id {launch.session.session_id})"
            ),
            (
                "localSendMessageCommand: "
                f"python3 {LOCAL_SEND_MESSAGE_COMMAND} --chat-key '{launch.session.chat_key}' "
                f"--bot-config-id '{bot.bot_id}' --bot-name '{bot.bot_name}' "
                f"(fallback: --session-id {launch.session.session_id}) --msgtype template_card "
                "--template-card-file ABSOLUTE_JSON_FILE"
            ),
            (
                "localScheduleMessageCommand: "
                f"python3 {LOCAL_SCHEDULE_MESSAGE_COMMAND} --chat-key '{launch.session.chat_key}' "
                f"--bot-config-id '{bot.bot_id}' --bot-name '{bot.bot_name}' "
                f"(fallback: --session-id {launch.session.session_id}) --run-at RFC3339_OR_EPOCH_MS "
                '--message MESSAGE or --cron "0 9 * * *" --timezone TZ --message MESSAGE'
            ),
            f"allowedFileSendRoots: {allowed_file_roots}",
            execution_mode_note,
            "Run in PROJECT_DIR.",
            "WORKSPACE_SKILL_DIR overrides HOME_CODEX_SKILLS_DIR on name conflicts.",
            "Use localSendFileCommand for file replies.",
            "Use localSendMessageCommand for proactive template-card sends.",
            "Use localScheduleMessageCommand for follow-up messages.",
            "Export user-visible files under CHATFILE_DIR.",
            "[/BridgeContext]",
        ]
    )


def build_prompt(bot: BotConfig, launch: CodexLaunchSpec, user_text: str) -> str:
    body = str(user_text or "").strip()
    return f"{build_bridge_context(bot, launch)}\n\nUser request:\n{body}"
