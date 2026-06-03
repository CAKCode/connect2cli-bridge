from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
from typing import Callable, TypeVar


T = TypeVar("T")
_POLL_INTERVAL_SEC = 0.01

async def run_blocking(func: Callable[..., T], /, *args, **kwargs) -> T:
    future: concurrent.futures.Future[T] = concurrent.futures.Future()
    context = contextvars.copy_context()

    def runner() -> None:
        try:
            result = context.run(func, *args, **kwargs)
        except BaseException as exc:
            future.set_exception(exc)
            return
        future.set_result(result)

    thread = threading.Thread(
        target=runner,
        name=f"bridge-blocking-{getattr(func, '__name__', 'call')}",
        daemon=True,
    )
    thread.start()
    while not future.done():
        await asyncio.sleep(_POLL_INTERVAL_SEC)
    return future.result()
