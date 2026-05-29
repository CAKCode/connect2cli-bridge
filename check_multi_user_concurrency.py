#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import shutil
import tempfile
import time
from pathlib import Path

from workspace_bridge.config import build_bot_from_app_config, load_app_config
from workspace_bridge import execution as execution_module
from workspace_bridge.execution import stream_text_message_once
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.wecom_protocol import WeComTextMessage


class RecordingWS:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


class _FakeProcess:
    def __init__(self, *, thread_id: str, final_text: str, delay_sec: float) -> None:
        self.stdin = type(
            "FakeStdin",
            (),
            {
                "write": lambda self, _data: None,
                "drain": lambda self: asyncio.sleep(0),
                "close": lambda self: None,
            },
        )()
        self.stdout = None
        self.stderr = None
        self.returncode = None
        self._delay_sec = delay_sec
        self._stdout = (
            f'{{"type":"thread.started","thread_id":"{thread_id}"}}\n'
            f'{{"type":"item.completed","item":{{"type":"agentmessage","text":"{final_text}"}}}}\n'
        ).encode("utf-8")
        self._stderr = b""

    async def communicate(self):
        await asyncio.sleep(self._delay_sec)
        self.returncode = 0
        return self._stdout, self._stderr


def _build_summary(results: list[dict], *, ws_payload_count: int, elapsed_ms: int, mode: str) -> dict:
    session_ids = [item["sessionId"] for item in results]
    thread_ids = [item["threadId"] for item in results]
    passed = len(set(session_ids)) == len(session_ids) and len(set(thread_ids)) == len(thread_ids)
    return {
        "mode": mode,
        "chatCount": len(results),
        "elapsedMs": elapsed_ms,
        "wsPayloadCount": ws_payload_count,
        "uniqueSessionCount": len(set(session_ids)),
        "uniqueThreadCount": len(set(thread_ids)),
        "allSessionsUnique": len(set(session_ids)) == len(session_ids),
        "allThreadsUnique": len(set(thread_ids)) == len(thread_ids),
        "pass": passed,
        "results": results,
    }


def _real_mode_preflight(env_file: Path) -> list[str]:
    errors: list[str] = []
    if not env_file.exists():
        errors.append("--mode real requires an existing --env-file")
        return errors
    values = {}
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    for key in ("WECOM_BOT_SOURCE_DIR", "WECOM_BOT_SECRET_FILE", "WECOM_BOT_ID"):
        if not values.get(key):
            errors.append(f"missing required setting for real mode: {key}")
    source_dir = values.get("WECOM_BOT_SOURCE_DIR")
    if source_dir and not Path(source_dir).expanduser().exists():
        errors.append(f"WECOM_BOT_SOURCE_DIR does not exist: {source_dir}")
    secret_file = values.get("WECOM_BOT_SECRET_FILE")
    if secret_file and not Path(secret_file).expanduser().exists():
        errors.append(f"WECOM_BOT_SECRET_FILE does not exist: {secret_file}")
    if not shutil.which("codex"):
        errors.append("codex executable not found in PATH")
    return errors


async def run_once(env_file: Path, chat_count: int, message: str, *, mode: str, delay_ms: int) -> dict:
    raw_values = {}
    if env_file.exists():
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            raw_values[key.strip()] = value.strip()
    if not raw_values.get("WECOM_BOT_SOURCE_DIR"):
        raw_values["WECOM_BOT_SOURCE_DIR"] = str(Path.cwd())
    if not raw_values.get("RUNTIME_ROOT"):
        raw_values["RUNTIME_ROOT"] = tempfile.mkdtemp(prefix="bridge-concurrency-runtime-")
    config = load_app_config(raw_values, env_file=env_file if env_file.exists() else None)
    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = RecordingWS()
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*_args, **kwargs):
        chat_key = str((kwargs.get("env") or {}).get("WECOM_BRIDGE_CHAT_KEY") or "")
        suffix = chat_key.rsplit("-", 1)[-1] if "-" in chat_key else chat_key.replace(":", "_")
        return _FakeProcess(
            thread_id=f"thread-{suffix}",
            final_text=f"done-{suffix}",
            delay_sec=max(0, delay_ms) / 1000,
        )

    if mode == "mock":
        execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec

    async def run_chat(index: int) -> dict:
        chat_key = f"single:load-user-{index}"
        req_id = f"req-{index}"
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            WeComTextMessage(req_id=req_id, chat_key=chat_key, content=message, raw_payload={}),
        )
        thread_id = runtime.session_threads.get(chat_key)
        return {
            "chatKey": chat_key,
            "sessionId": session_id,
            "threadId": thread_id,
            "reply": reply,
        }

    started_at = time.monotonic()
    try:
        results = await asyncio.gather(*(run_chat(i) for i in range(chat_count)))
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec
    elapsed_ms = int((time.monotonic() - started_at) * 1000)
    return _build_summary(results, ws_payload_count=len(runtime.ws.sent), elapsed_ms=elapsed_ms, mode=mode)


async def run_many(env_file: Path, chat_count: int, message: str, *, mode: str, delay_ms: int, rounds: int) -> dict:
    rounds_payload: list[dict] = []
    started_at = time.monotonic()
    for _ in range(max(1, rounds)):
        try:
            rounds_payload.append(
                await run_once(
                    env_file,
                    chat_count,
                    message,
                    mode=mode,
                    delay_ms=delay_ms,
                )
            )
        except Exception as exc:
            rounds_payload.append(
                {
                    "mode": mode,
                    "chatCount": chat_count,
                    "elapsedMs": 0,
                    "wsPayloadCount": 0,
                    "uniqueSessionCount": 0,
                    "uniqueThreadCount": 0,
                    "allSessionsUnique": False,
                    "allThreadsUnique": False,
                    "pass": False,
                    "error": str(exc),
                    "results": [],
                }
            )
    total_elapsed_ms = int((time.monotonic() - started_at) * 1000)
    pass_count = sum(1 for item in rounds_payload if item.get("pass") is True)
    return {
        "mode": mode,
        "rounds": len(rounds_payload),
        "chatCount": chat_count,
        "passCount": pass_count,
        "allPass": pass_count == len(rounds_payload),
        "totalElapsedMs": total_elapsed_ms,
        "roundsPayload": rounds_payload,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Local multi-chat concurrency validation for the WeCom bridge workspace runtime.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--chat-count", type=int, default=10)
    parser.add_argument("--message", default="health check")
    parser.add_argument("--mode", choices=("mock", "real"), default="mock")
    parser.add_argument("--delay-ms", type=int, default=25)
    parser.add_argument("--rounds", type=int, default=1)
    args = parser.parse_args()
    if args.mode == "real":
        errors = _real_mode_preflight(Path(args.env_file).expanduser().resolve())
        if errors:
            print(json.dumps({"mode": "real", "pass": False, "errors": errors}, ensure_ascii=False, indent=2))
            return 2

    payload = asyncio.run(
        run_many(
            Path(args.env_file).expanduser().resolve(),
            args.chat_count,
            args.message,
            mode=args.mode,
            delay_ms=args.delay_ms,
            rounds=args.rounds,
        )
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["allPass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
