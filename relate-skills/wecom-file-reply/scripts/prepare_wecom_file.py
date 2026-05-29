#!/usr/bin/env python3
"""Prepare files for WeCom replies with size-aware fallbacks.

Behavior:
- Send directly if the file is within the configured limit.
- Otherwise try a zip archive.
- If the archive is still too large, generate a download link when a
  publisher command or public base URL is configured.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import urllib.parse
import zipfile
from pathlib import Path
from typing import Any


DEFAULT_MAX_BYTES = 20 * 1024 * 1024
DEFAULT_CLOUDFLARED_SHARE_CMD = (
    "python3 /home/jenkins/.codex/skills/wecom-file-reply/scripts/share_via_cloudflared.py {file}"
)
DEFAULT_TEMP_PUBLISH_CMD = (
    "python3 /home/jenkins/.codex/skills/wecom-file-reply/scripts/publish_temp_link.py {file}"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+", help="Files or directories to prepare")
    parser.add_argument("--export-dir", required=True, help="Directory allowed for send-back")
    parser.add_argument("--max-bytes", type=int, default=None, help="Direct-send size limit")
    parser.add_argument("--max-mb", type=int, default=None, help="Direct-send size limit in MB")
    parser.add_argument("--public-base-url", default=None, help="Public download base URL")
    parser.add_argument("--publish-cmd", default=None, help="Publisher command that prints a URL")
    return parser.parse_args()


def env_max_bytes() -> int:
    raw_bytes = os.getenv("WECOM_FILE_MAX_BYTES")
    if raw_bytes:
        try:
            value = int(raw_bytes)
        except ValueError as exc:
            raise ValueError("WECOM_FILE_MAX_BYTES must be an integer") from exc
        if value <= 0:
            raise ValueError("WECOM_FILE_MAX_BYTES must be positive")
        return value

    raw_mb = os.getenv("WECOM_FILE_MAX_MB")
    if raw_mb:
        try:
            value = int(raw_mb)
        except ValueError as exc:
            raise ValueError("WECOM_FILE_MAX_MB must be an integer") from exc
        if value <= 0:
            raise ValueError("WECOM_FILE_MAX_MB must be positive")
        return value * 1024 * 1024

    return DEFAULT_MAX_BYTES


def resolve_limit(args: argparse.Namespace) -> int:
    if args.max_bytes is not None:
        if args.max_bytes <= 0:
            raise ValueError("--max-bytes must be positive")
        return args.max_bytes
    if args.max_mb is not None:
        if args.max_mb <= 0:
            raise ValueError("--max-mb must be positive")
        return args.max_mb * 1024 * 1024
    return env_max_bytes()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def unique_path(directory: Path, filename: str) -> Path:
    candidate = directory / filename
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    index = 1
    while True:
        numbered = directory / f"{stem}-{index}{suffix}"
        if not numbered.exists():
            return numbered
        index += 1


def stage_file(src: Path, export_dir: Path) -> Path:
    if export_dir in src.parents:
        return src
    ensure_dir(export_dir)
    dst = unique_path(export_dir, src.name)
    shutil.copy2(src, dst)
    return dst


def archive_name(src: Path) -> str:
    if src.is_dir():
        return f"{src.name}.zip"
    return f"{src.stem}.zip"


def zip_source(src: Path, export_dir: Path) -> Path:
    ensure_dir(export_dir)
    dst = unique_path(export_dir, archive_name(src))
    with zipfile.ZipFile(dst, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        if src.is_dir():
            for child in sorted(src.rglob("*")):
                if child.is_dir():
                    continue
                arcname = Path(src.name) / child.relative_to(src)
                zf.write(child, arcname=str(arcname))
        else:
            zf.write(src, arcname=src.name)
    return dst


def build_public_url(base_url: str, staged_path: Path) -> str:
    quoted = urllib.parse.quote(staged_path.name)
    return f"{base_url.rstrip('/')}/{quoted}"


def run_publish_cmd(command_template: str, file_path: Path) -> str:
    parts = shlex.split(command_template)
    replaced = False
    resolved: list[str] = []
    for part in parts:
        if "{file}" in part:
            resolved.append(part.replace("{file}", str(file_path)))
            replaced = True
        else:
            resolved.append(part)
    if not replaced:
        resolved.append(str(file_path))

    completed = subprocess.run(
        resolved,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip() or "publisher failed"
        raise RuntimeError(stderr)

    for line in completed.stdout.splitlines():
        url = line.strip()
        if url:
            return url
    raise RuntimeError("publisher did not print a URL")


def link_result(
    src_for_link: Path,
    export_dir: Path,
    public_base_url: str | None,
    publish_cmd: str | None,
    attempted_archive: Path | None,
) -> dict[str, Any]:
    if publish_cmd:
        url = run_publish_cmd(publish_cmd, src_for_link)
        return {
            "action": "link",
            "url": url,
            "path": str(src_for_link),
            "attempted_archive": str(attempted_archive) if attempted_archive else None,
        }

    if public_base_url:
        staged = src_for_link if export_dir in src_for_link.parents else stage_file(src_for_link, export_dir)
        return {
            "action": "link",
            "url": build_public_url(public_base_url, staged),
            "path": str(staged),
            "attempted_archive": str(attempted_archive) if attempted_archive else None,
        }

    return {
        "action": "error",
        "reason": "file exceeds send limit and no download-link publisher is configured",
        "attempted_archive": str(attempted_archive) if attempted_archive else None,
    }


def prepare_single(
    raw_path: str,
    export_dir: Path,
    max_bytes: int,
    public_base_url: str | None,
    publish_cmd: str | None,
) -> dict[str, Any]:
    src = Path(raw_path).expanduser().resolve()
    if not src.exists():
        return {"action": "error", "source": str(src), "reason": "source path does not exist"}

    if src.is_file() and src.stat().st_size <= max_bytes:
        staged = stage_file(src, export_dir)
        return {
            "action": "send",
            "source": str(src),
            "path": str(staged),
            "size_bytes": staged.stat().st_size,
            "compressed": False,
        }

    archive = zip_source(src, export_dir)
    archive_size = archive.stat().st_size
    if archive_size <= max_bytes:
        return {
            "action": "send",
            "source": str(src),
            "path": str(archive),
            "size_bytes": archive_size,
            "compressed": True,
        }

    link_target = archive if archive_size <= src.stat().st_size or src.is_dir() else src
    result = link_result(
        src_for_link=link_target,
        export_dir=export_dir,
        public_base_url=public_base_url,
        publish_cmd=publish_cmd,
        attempted_archive=archive,
    )
    result["source"] = str(src)
    result["size_bytes"] = archive_size if src.is_dir() else src.stat().st_size
    result["compressed"] = False
    return result


def main() -> int:
    try:
        args = parse_args()
        export_dir = Path(args.export_dir).expanduser().resolve()
        ensure_dir(export_dir)
        max_bytes = resolve_limit(args)
        public_base_url = args.public_base_url or os.getenv("WECOM_FILE_PUBLIC_BASE_URL")
        publish_cmd = args.publish_cmd or os.getenv("WECOM_FILE_PUBLISH_CMD")
        enable_cloudflared_share = os.getenv("WECOM_FILE_ENABLE_CLOUDFLARED_SHARE", "1").strip().lower()
        if (
            not publish_cmd
            and not public_base_url
            and enable_cloudflared_share not in {"0", "false", "no"}
        ):
            publish_cmd = os.getenv("WECOM_FILE_CLOUDFLARED_SHARE_CMD", DEFAULT_CLOUDFLARED_SHARE_CMD)
        enable_temp_publish = os.getenv("WECOM_FILE_ENABLE_TEMP_PUBLISH", "1").strip().lower()
        if not publish_cmd and not public_base_url and enable_temp_publish not in {"0", "false", "no"}:
            publish_cmd = os.getenv("WECOM_FILE_TEMP_PUBLISH_CMD", DEFAULT_TEMP_PUBLISH_CMD)

        results = [
            prepare_single(
                raw_path=raw_path,
                export_dir=export_dir,
                max_bytes=max_bytes,
                public_base_url=public_base_url,
                publish_cmd=publish_cmd,
            )
            for raw_path in args.paths
        ]
        payload = {
            "max_bytes": max_bytes,
            "export_dir": str(export_dir),
            "results": results,
        }
        json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        return 0
    except Exception as exc:
        payload = {"action": "error", "reason": str(exc)}
        json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
