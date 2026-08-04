from __future__ import annotations

import errno
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any, TextIO


OWNER_DIR_MODE = 0o700
GROUP_DIR_MODE = 0o2770
OWNER_FILE_MODE = 0o600
GROUP_FILE_MODE = 0o660


def state_directory_mode(parent: Path | str) -> int:
    parent_mode = Path(parent).stat().st_mode
    return GROUP_DIR_MODE if parent_mode & stat.S_IWGRP else OWNER_DIR_MODE


def state_file_mode(parent: Path | str) -> int:
    parent_mode = Path(parent).stat().st_mode
    return GROUP_FILE_MODE if parent_mode & stat.S_IWGRP else OWNER_FILE_MODE


def ensure_state_directory(path: Path | str) -> Path:
    directory = Path(path)
    if directory.is_symlink():
        raise OSError(f"state directory must not be a symlink: {directory}")
    if directory.exists():
        if not directory.is_dir():
            raise OSError(f"state directory is not a directory: {directory}")
        return directory

    parent = directory.parent
    if parent == directory:
        raise OSError(f"state directory cannot be created: {directory}")
    ensure_state_directory(parent)
    if parent.is_symlink():
        raise OSError(f"state directory parent must not be a symlink: {parent}")
    mode = state_directory_mode(parent)
    try:
        directory.mkdir(mode=mode)
    except FileExistsError:
        pass
    if directory.is_symlink() or not directory.is_dir():
        raise OSError(f"state directory is not a directory: {directory}")
    if stat.S_IMODE(directory.stat().st_mode) != mode:
        os.chmod(directory, mode)
    return directory


def open_state_lock_file(path: Path | str) -> TextIO:
    """Open a regular state lock file without following a final symlink."""
    lock_path = Path(path)
    parent = lock_path.parent
    if parent.is_symlink():
        raise OSError(f"lock file parent must not be a symlink: {parent}")
    ensure_state_directory(parent)
    if parent.is_symlink() or not parent.is_dir():
        raise OSError(f"lock file parent is not a directory: {parent}")
    if not hasattr(os, "O_NOFOLLOW"):
        raise OSError("secure lock files require O_NOFOLLOW support")

    mode = state_file_mode(parent)
    flags = os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    for _attempt in range(8):
        try:
            fd = os.open(lock_path, flags, mode)
        except OSError as exc:
            if exc.errno not in {errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP)}:
                raise
            try:
                link_mode = lock_path.lstat().st_mode
            except FileNotFoundError:
                continue
            if not stat.S_ISLNK(link_mode):
                raise OSError(f"lock file is not a regular file: {lock_path}") from exc
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
            continue

        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError(f"lock file is not a regular file: {lock_path}")
            os.fchmod(fd, mode)
            os.set_blocking(fd, True)
            return os.fdopen(fd, "a+", encoding="utf-8")
        except Exception:
            os.close(fd)
            raise
    raise OSError(f"lock file changed too often while opening: {lock_path}")


def atomic_write_text(
    path: Path | str,
    payload: str,
    *,
    encoding: str = "utf-8",
    mode: int | None = None,
) -> None:
    target = Path(path)
    parent = target.parent
    if parent.is_symlink():
        raise OSError(f"atomic write parent must not be a symlink: {parent}")
    ensure_state_directory(parent)
    if parent.is_symlink() or not parent.is_dir():
        raise OSError(f"atomic write parent is not a directory: {parent}")
    target_mode = state_file_mode(parent) if mode is None else mode

    fd = -1
    temp_path: Path | None = None
    try:
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=parent)
        temp_path = Path(temp_name)
        os.fchmod(fd, target_mode)
        with os.fdopen(fd, "w", encoding=encoding, closefd=True) as handle:
            fd = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
        temp_path = None

        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(parent, directory_flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass


def atomic_write_json(path: Path | str, payload: Any, *, mode: int | None = None) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2),
        mode=mode,
    )


def exclusive_create_json(path: Path | str, payload: Any, *, mode: int | None = None) -> bool:
    target = Path(path)
    parent = target.parent
    if parent.is_symlink():
        raise OSError(f"exclusive write parent must not be a symlink: {parent}")
    ensure_state_directory(parent)
    if parent.is_symlink() or not parent.is_dir():
        raise OSError(f"exclusive write parent is not a directory: {parent}")
    if not hasattr(os, "O_NOFOLLOW"):
        raise OSError("secure exclusive writes require O_NOFOLLOW support")

    target_mode = state_file_mode(parent) if mode is None else mode
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(target, flags, target_mode)
    except FileExistsError:
        return False
    opened_stat = os.fstat(fd)
    try:
        if not stat.S_ISREG(opened_stat.st_mode):
            raise OSError(f"exclusive write target is not a regular file: {target}")
        os.fchmod(fd, target_mode)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=True) as handle:
            fd = -1
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
            handle.flush()
            os.fsync(handle.fileno())
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True
    except Exception:
        if fd >= 0:
            os.close(fd)
        try:
            current_stat = target.lstat()
            if (current_stat.st_dev, current_stat.st_ino) == (opened_stat.st_dev, opened_stat.st_ino):
                target.unlink()
        except FileNotFoundError:
            pass
        raise
