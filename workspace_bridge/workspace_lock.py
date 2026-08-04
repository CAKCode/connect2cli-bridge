from __future__ import annotations

import fcntl
from contextlib import contextmanager
from typing import Iterator

from .atomic_io import ensure_state_directory, open_state_lock_file
from .models import WorkspaceRef


@contextmanager
def workspace_lock(workspace: WorkspaceRef) -> Iterator[None]:
    ensure_state_directory(workspace.lock_file.parent)
    with open_state_lock_file(workspace.lock_file) as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
