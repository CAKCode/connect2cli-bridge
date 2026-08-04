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

from workspace_bridge.agent_backends import normalize_agent_backend, parse_command_to_argv
from workspace_bridge.config import build_bot_from_app_config, load_app_config, read_env_file
from workspace_bridge import execution as execution_module
from workspace_bridge.execution import stream_text_message_once
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.wecom_protocol import WeComTextMessage


class RecordingWS:
    def __init__(self, runtime: WeComBotRuntime) -> None:
        self.runtime = runtime
        self.sent: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)
        req_id = ((payload.get("headers") or {}).get("req_id"))
        future = self.runtime.pending_requests.get(req_id) if req_id else None
        if future is not None and not future.done():
            future.set_result({"headers": {"req_id": req_id}, "errcode": 0, "body": {}})


class _ConcurrencyTracker:
    def __init__(self, expected_processes: int) -> None:
        self.active = 0
        self.maximum = 0
        self.expected_processes = max(1, int(expected_processes))
        self.overlap_reached = asyncio.Event()

    async def enter(self) -> None:
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            if self.maximum >= 2:
                self.overlap_reached.set()
            if self.expected_processes > 1 and not self.overlap_reached.is_set():
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.overlap_reached.wait(), timeout=2)
        except BaseException:
            self.active -= 1
            raise


class _FakeProcess:
    def __init__(
        self,
        *,
        thread_id: str,
        final_text: str,
        delay_sec: float,
        concurrency_tracker: _ConcurrencyTracker,
    ) -> None:
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
        self._concurrency_tracker = concurrency_tracker
        self._stdout = (
            f'{{"type":"thread.started","thread_id":"{thread_id}"}}\n'
            f'{{"type":"item.completed","item":{{"type":"agentmessage","text":"{final_text}"}}}}\n'
        ).encode("utf-8")
        self._stderr = b""

    async def communicate(self):
        await self._concurrency_tracker.enter()
        try:
            await asyncio.sleep(self._delay_sec)
            self.returncode = 0
            return self._stdout, self._stderr
        finally:
            self._concurrency_tracker.active -= 1


def _build_summary(
    results: list[dict],
    *,
    ws_payload_count: int,
    elapsed_ms: int,
    mode: str,
    max_concurrent_processes: int | None = None,
) -> dict:
    session_ids = [str(item.get("sessionId") or "").strip() for item in results]
    thread_ids = [str(item.get("threadId") or "").strip() for item in results]
    all_session_ids_present = bool(session_ids) and all(session_ids)
    all_thread_ids_present = bool(thread_ids) and all(thread_ids)
    all_sessions_unique = len(set(session_ids)) == len(session_ids)
    all_threads_unique = len(set(thread_ids)) == len(thread_ids)
    concurrency_proven = None if mode != "mock" else len(results) <= 1 or int(max_concurrent_processes or 0) >= 2
    concurrency_requirement_met = concurrency_proven is not False
    passed = (
        all_session_ids_present
        and all_thread_ids_present
        and all_sessions_unique
        and all_threads_unique
        and concurrency_requirement_met
    )
    return {
        "mode": mode,
        "chatCount": len(results),
        "elapsedMs": elapsed_ms,
        "wsPayloadCount": ws_payload_count,
        "uniqueSessionCount": len(set(session_ids)),
        "uniqueThreadCount": len(set(thread_ids)),
        "allSessionIdsPresent": all_session_ids_present,
        "allThreadIdsPresent": all_thread_ids_present,
        "allSessionsUnique": all_sessions_unique,
        "allThreadsUnique": all_threads_unique,
        "maxConcurrentProcesses": max_concurrent_processes,
        "concurrencyProven": concurrency_proven,
        "pass": passed,
        "results": results,
    }


def _real_mode_preflight(env_file: Path) -> list[str]:
    errors: list[str] = []
    if not env_file.exists():
        errors.append("--mode real requires an existing --env-file")
        return errors
    try:
        values = read_env_file(env_file)
    except ValueError as exc:
        errors.append(str(exc))
        return errors
    for key in ("WECOM_BOT_SOURCE_DIR", "WECOM_BOT_SECRET_FILE", "WECOM_BOT_ID"):
        if not values.get(key):
            errors.append(f"missing required setting for real mode: {key}")
    source_dir = values.get("WECOM_BOT_SOURCE_DIR")
    if source_dir:
        resolved_source_dir = Path(source_dir).expanduser()
        if not resolved_source_dir.exists():
            errors.append(f"WECOM_BOT_SOURCE_DIR does not exist: {source_dir}")
        elif not resolved_source_dir.is_dir():
            errors.append(f"WECOM_BOT_SOURCE_DIR is not a directory: {source_dir}")
    secret_file = values.get("WECOM_BOT_SECRET_FILE")
    if secret_file:
        resolved_secret_file = Path(secret_file).expanduser()
        if not resolved_secret_file.exists():
            errors.append(f"WECOM_BOT_SECRET_FILE does not exist: {secret_file}")
        elif not resolved_secret_file.is_file():
            errors.append(f"WECOM_BOT_SECRET_FILE is not a file: {secret_file}")
    try:
        backend = normalize_agent_backend(values.get("WECOM_BOT_AGENT_BACKEND") or values.get("AGENT_BACKEND") or "codex")
    except ValueError as exc:
        errors.append(str(exc))
        return errors
    command_text = str(values.get("WECOM_BOT_AGENT_COMMAND") or values.get("AGENT_COMMAND") or "").strip()
    executable = parse_command_to_argv(command_text)[0] if command_text else ("claude" if backend == "claude" else "codex")
    if not shutil.which(executable):
        errors.append(f"{executable} executable not found in PATH")
    return errors


async def run_once(env_file: Path, chat_count: int, message: str, *, mode: str, delay_ms: int) -> dict:
    if int(chat_count) < 1:
        raise ValueError("chat_count must be >= 1")
    raw_values = read_env_file(env_file) if mode != "mock" and env_file.exists() else {}
    temp_runtime_root: Path | None = None
    if mode != "mock" and not raw_values.get("WECOM_BOT_SOURCE_DIR"):
        raw_values["WECOM_BOT_SOURCE_DIR"] = str(Path.cwd())
    try:
        if mode == "mock":
            # Keep the repeatable probe isolated from production credentials and state,
            # even when the caller points it at the service's real .env file.
            temp_runtime_root = Path(tempfile.mkdtemp(prefix="bridge-concurrency-runtime-")).resolve()
            mock_runtime_root = temp_runtime_root / "runtime"
            mock_source_dir = temp_runtime_root / "source"
            mock_runtime_root.mkdir()
            mock_source_dir.mkdir()
            raw_values["RUNTIME_ROOT"] = str(mock_runtime_root)
            raw_values["WECOM_BOT_SOURCE_DIR"] = str(mock_source_dir)
            mock_secret_file = temp_runtime_root / "mock-bot.secret"
            mock_secret_file.write_text("mock-concurrency-secret\n", encoding="utf-8")
            mock_secret_file.chmod(0o600)
            raw_values = {
                "RUNTIME_ROOT": str(mock_runtime_root),
                "CHATFILE_ROOT": str(temp_runtime_root / "chatfiles"),
                "CODEX_OUTPUT_ROOT": str(temp_runtime_root / "codex-output"),
                "FILE_SEND_ROOTS": str(temp_runtime_root / "file-send"),
                "WECOM_BOT_SOURCE_DIR": str(mock_source_dir),
                "WECOM_BOT_SECRET_FILE": str(mock_secret_file),
                "WECOM_BOT_ID": "mock-concurrency-bot",
                "WECOM_BOT_NAME": "mock-concurrency",
                "WECOM_ENABLED": "false",
                "CODEX_EXEC_MODE": "host",
                "AGENT_BACKEND": "codex",
                "AGENT_RUNTIME_ROOT": str(temp_runtime_root / "agent-runtime"),
            }
        elif not raw_values.get("RUNTIME_ROOT"):
            temp_runtime_root = Path(tempfile.mkdtemp(prefix="bridge-concurrency-runtime-")).resolve()
            raw_values["RUNTIME_ROOT"] = str(temp_runtime_root)
        config = load_app_config(
            raw_values,
            env_file=env_file if mode != "mock" and env_file.exists() else None,
        )
        bot = build_bot_from_app_config(config)
        runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
        runtime.ws = RecordingWS(runtime)
        original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
        concurrency_tracker = _ConcurrencyTracker(chat_count)

        async def fake_create_subprocess_exec(*_args, **kwargs):
            chat_key = str((kwargs.get("env") or {}).get("WECOM_BRIDGE_CHAT_KEY") or "")
            suffix = chat_key.rsplit("-", 1)[-1] if "-" in chat_key else chat_key.replace(":", "_")
            return _FakeProcess(
                thread_id=f"thread-{suffix}",
                final_text=f"done-{suffix}",
                delay_sec=max(0, delay_ms) / 1000,
                concurrency_tracker=concurrency_tracker,
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
        results = await asyncio.gather(*(run_chat(i) for i in range(chat_count)))
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
    finally:
        if 'original_create_subprocess_exec' in locals():
            execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec
        if temp_runtime_root is not None:
            shutil.rmtree(temp_runtime_root, ignore_errors=True)
    return _build_summary(
        results,
        ws_payload_count=len(runtime.ws.sent),
        elapsed_ms=elapsed_ms,
        mode=mode,
        max_concurrent_processes=concurrency_tracker.maximum if mode == "mock" else None,
    )


async def run_many(env_file: Path, chat_count: int, message: str, *, mode: str, delay_ms: int, rounds: int) -> dict:
    if int(chat_count) < 1:
        raise ValueError("chat_count must be >= 1")
    if int(rounds) < 1:
        raise ValueError("rounds must be >= 1")
    rounds_payload: list[dict] = []
    started_at = time.monotonic()
    for _ in range(int(rounds)):
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
                    "allSessionIdsPresent": False,
                    "allThreadIdsPresent": False,
                    "allSessionsUnique": False,
                    "allThreadsUnique": False,
                    "maxConcurrentProcesses": None,
                    "concurrencyProven": False,
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
    env_file = Path(args.env_file).expanduser().resolve()
    if env_file.exists() and not env_file.is_file():
        print(json.dumps({"pass": False, "errors": [f"env file is not a file: {env_file}"]}, ensure_ascii=False, indent=2))
        return 2
    if args.chat_count < 1:
        print(json.dumps({"pass": False, "errors": ["--chat-count must be >= 1"]}, ensure_ascii=False, indent=2))
        return 2
    if args.rounds < 1:
        print(json.dumps({"pass": False, "errors": ["--rounds must be >= 1"]}, ensure_ascii=False, indent=2))
        return 2
    if args.mode == "real":
        errors = _real_mode_preflight(env_file)
        if errors:
            print(json.dumps({"mode": "real", "pass": False, "errors": errors}, ensure_ascii=False, indent=2))
            return 2

    payload = asyncio.run(
        run_many(
            env_file,
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
