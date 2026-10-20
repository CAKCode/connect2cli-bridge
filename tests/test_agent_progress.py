import json

import pytest

from workspace_bridge.agent_backends import AgentProgress, extract_agent_progress


def test_stage_only_event_preserves_last_description():
    events = [
        {"type": "item.completed", "item": {"type": "agent_message", "text": "已定位到配置问题，正在修复。"}},
        {"type": "item.started", "item": {"type": "file_change", "changes": [{"path": "/private/config"}]}},
    ]
    progress = extract_agent_progress("codex", "\n".join(json.dumps(event) for event in events))
    assert progress == AgentProgress("修改中", "已定位到配置问题，正在修复。")


def test_claude_text_and_tool_share_description_without_exposing_arguments():
    event = {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "正在检查刷新和中断后的显示效果。"},
        {"type": "tool_use", "name": "Bash", "input": {"command": "private command"}},
    ]}}
    assert extract_agent_progress("claude", json.dumps(event)) == AgentProgress(
        "处理中", "正在检查刷新和中断后的显示效果。"
    )


@pytest.mark.parametrize("event", [
    {"type": "item.reasoning.contentDelta", "delta": "private reasoning"},
    {"type": "item.commandExecution.outputDelta", "delta": "raw output"},
    {"type": "item.agentMessage.delta", "delta": "unfinished answer"},
    {"type": "item.outputText.delta", "delta": "unfinished answer"},
    {"type": "item.completed", "item": {"type": "reasoning", "text": "private reasoning", "encrypted_content": "private"}},
])
def test_internal_content_never_replaces_public_summary(event):
    current = AgentProgress("分析中", "正在核对配置。")
    update = extract_agent_progress("codex", json.dumps(event))
    if update is not None:
        current = current.updated(update)
    assert current.summary == "正在核对配置。"


def test_structured_public_summary_extracts_text_only():
    event = {"type": "item.completed", "item": {
        "type": "reasoning", "summary": [{"type": "summary_text", "text": "已核对配置，正在整理结果。"}],
        "content": "private reasoning",
    }}
    assert extract_agent_progress("codex", json.dumps(event)) == AgentProgress("分析中", "已核对配置，正在整理结果。")


@pytest.mark.parametrize("event", ["[]", "null", "invalid json", '{"type": "item.updated", "item": []}'])
def test_invalid_event_does_not_erase_progress(event):
    assert extract_agent_progress("codex", event) is None
