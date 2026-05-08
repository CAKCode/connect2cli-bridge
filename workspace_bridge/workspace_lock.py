from __future__ import annotations

import fcntl
from contextlib import contextmanager
from typing import Iterator

from .models import WorkspaceRef


@contextmanager
def workspace_lock(workspace: WorkspaceRef) -> Iterator[None]:
    workspace.lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(workspace.lock_file, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
