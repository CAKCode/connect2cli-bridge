import asyncio

import pytest

from workspace_bridge.wecom_context import AssistantCallAssembler, decorate_assistant_replies, feedback_event, text_with_quote
from workspace_bridge.wecom_limits import WeComRateLimitError, WeComRateLimiter, validate_media_upload, wait_for_send_slot


class Clock:
    def __init__(self):
        self.now = 0.0
        self.waits = []

    async def sleep(self, delay):
        self.waits.append(delay)
        self.now += delay

    def limiter(self):
        return WeComRateLimiter(clock=lambda: self.now, sleep=self.sleep)


async def test_group_members_share_one_conversation_quota():
    clock = Clock()
    limiter = clock.limiter()
    for member in range(31):
        payload = {"cmd": "aibot_send_msg", "body": {"chat_type": 2, "chatid": "room", "markdown": {"content": f"<@member-{member}>"}}}
        await wait_for_send_slot(limiter, payload)
    assert clock.now == 60
    assert clock.waits == [60]


async def test_hourly_quota_is_enforced_after_many_minute_windows():
    clock = Clock()
    limiter = clock.limiter()
    for _ in range(1001):
        await limiter.acquire("chat:2:room")
    assert clock.now == 3600


async def test_card_deadline_does_not_wait_past_five_seconds():
    clock = Clock()
    limiter = clock.limiter()
    for _ in range(30):
        await limiter.acquire("chat:2:room")
    with pytest.raises(WeComRateLimitError):
        await wait_for_send_slot(limiter, {"cmd": "aibot_respond_update_msg", "body": {}}, callback_target=(2, "room"), received_at=0)
    assert clock.waits == []


async def test_waiting_chat_does_not_block_a_different_chat():
    entered = asyncio.Event()
    hold = asyncio.Event()

    async def sleep(_delay):
        entered.set()
        await hold.wait()

    limiter = WeComRateLimiter(clock=lambda: 0, sleep=sleep)
    for _ in range(30):
        await limiter.acquire("room-a")
    waiting = asyncio.create_task(limiter.acquire("room-a"))
    await entered.wait()
    await asyncio.wait_for(limiter.acquire("room-b"), timeout=1)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting


@pytest.mark.parametrize("kind,extension,limit", [("file", ".txt", 20), ("image", ".png", 10), ("voice", ".amr", 2), ("video", ".mp4", 10)])
def test_media_limits_apply_before_upload(kind, extension, limit):
    maximum = limit * 1024 * 1024
    assert validate_media_upload("report" + extension, maximum, kind) <= 100
    with pytest.raises(ValueError, match="too large"):
        validate_media_upload("report" + extension, maximum + 1, kind)
    with pytest.raises(ValueError, match="at least 5"):
        validate_media_upload("report" + extension, 4, kind)


def assistant_frame(part, *, count=3, kind="text", task="call-1"):
    return {
        "cmd": "aibot_msg_callback", "headers": {"req_id": f"req-{part}"},
        "body": {"chatid": "room", "from": {"userid": "alice"}, "msgtype": kind, kind: {"content": f"part-{part}"},
                 "compat_info": {"source": "ai_assistant", "from_ai_assistant": {"task_id": task, "sub_id": part, "total_sub_count": count}}},
    }


def test_assistant_parts_are_aggregated_out_of_order_once():
    assembler = AssistantCallAssembler()
    assert assembler.add(assistant_frame(3, kind="file")) is None
    assert assembler.add(assistant_frame(1)) is None
    assert assembler.add(assistant_frame(1)) is None
    result = assembler.add(assistant_frame(2, kind="image"))
    assert [part["headers"]["req_id"] for part in result] == ["req-1", "req-2", "req-3"]
    assert assembler.add(assistant_frame(2)) is None


def test_assistant_parts_cannot_mix_different_sender_identities():
    assembler = AssistantCallAssembler()
    assembler.add(assistant_frame(1))
    other = assistant_frame(2)
    other["body"]["from"]["userid"] = "bob"
    with pytest.raises(ValueError, match="identity"):
        assembler.add(other)


def test_assistant_result_finishes_only_on_last_message():
    original = [{"cmd": "aibot_respond_msg", "body": {"msgtype": "stream", "stream": {"finish": True}}},
                {"cmd": "aibot_send_msg", "body": {"msgtype": "file", "file": {"media_id": "media"}}}]
    result = decorate_assistant_replies(original, {"assistant_task_id": "call-1"}, final=True)
    assert result[0]["body"]["to_ai_assistant"] == {"reply_status": "waiting"}
    assert result[1]["body"]["to_ai_assistant"] == {"task_id": "call-1", "reply_status": "finished"}
    assert "to_ai_assistant" not in original[0]["body"]


def test_quoted_control_command_is_preserved_as_context():
    result = text_with_quote("重跑这个", {"quote": {"msgtype": "text", "text": {"content": "/bridge-reset"}}})
    assert result.startswith("用户请求：\n重跑这个")
    assert "引用消息：\n/bridge-reset" in result


def test_feedback_cancellation_and_negative_reasons_are_preserved():
    result = feedback_event({"event": {"eventtype": "feedback_event", "feedback_event": {"id": "reply-1", "type": 2, "content": "少了失败原因", "inaccurate_reason_list": [2]}}})
    assert result["type"] == 2
    assert result["inaccurate_reason_list"] == [2]
    with pytest.raises(ValueError, match="feedback type"):
        feedback_event({"event": {"eventtype": "feedback_event", "feedback_event": {"id": "reply-1", "type": 99}}})
