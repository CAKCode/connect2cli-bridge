#!/usr/bin/env python3
"""Publish a temporary public download link for a local file.

Default backend: Litterbox (catbox temporary file hosting).
Useful for personal-computer environments without object storage or CDN.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests


DEFAULT_ENDPOINT = "https://litterbox.catbox.moe/resources/internals/api.php"
DEFAULT_DURATION = "24h"
ALLOWED_DURATIONS = {"1h", "12h", "24h", "72h"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="Local file to publish")
    parser.add_argument(
        "--duration",
        default=DEFAULT_DURATION,
        help="Temporary link retention: 1h, 12h, 24h, or 72h",
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help="Upload endpoint for the temporary hosting service",
    )
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()
        duration = args.duration.strip()
        if duration not in ALLOWED_DURATIONS:
            raise ValueError("duration must be one of: 1h, 12h, 24h, 72h")

        file_path = Path(args.file).expanduser().resolve()
        if not file_path.is_file():
            raise ValueError("file does not exist or is not a regular file")

        with file_path.open("rb") as fh:
            response = requests.post(
                args.endpoint,
                data={"reqtype": "fileupload", "time": duration},
                files={"fileToUpload": (file_path.name, fh)},
                timeout=120,
            )

        response.raise_for_status()
        url = response.text.strip()
        if not url.startswith("http://") and not url.startswith("https://"):
            raise RuntimeError(url or "temporary upload failed")

        print(url)
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
