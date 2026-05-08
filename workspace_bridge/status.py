from __future__ import annotations


def build_session_status_text(status: str, detail: str | None = None) -> str:
    text = f"运行状态：{status}"
    if detail:
        text = f"{text}，{detail}"
    return f"{text}。"


def build_thinking_status_text(elapsed_sec: int, detail: str | None = None) -> str:
    suffix = f"已运行 {max(0, int(elapsed_sec))}s"
    if detail:
        suffix = f"{suffix}，{detail}"
    return build_session_status_text("思考中", suffix)


def build_working_status_text(elapsed_sec: int, detail: str | None = None) -> str:
    elapsed = max(0, int(elapsed_sec))
    dots = "." * ((elapsed // 5) % 3 + 1)
    text = f"运行状态：整理回复中{dots} 已运行 {elapsed}s"
    if detail:
        text = f"{text}，{detail}"
    return f"{text}。"


def build_status_stream_content(status_text: str, summary: str | None = None) -> str:
    cleaned = str(summary or "").strip()
    if not cleaned:
        return status_text
    return f"{status_text}\n\n{cleaned}"


def build_reply_window_expired_notice(interval_sec: int = 120) -> str:
    return f"流式回复窗口即将到期，任务仍在后台运行，后续会约每 {interval_sec}s 主动发送阶段摘要和最终结果。"
