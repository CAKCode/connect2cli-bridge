#!/usr/bin/env python3
"""Upload a file to S3-compatible storage and print a download URL.

This script is designed to be used by wecom-file-reply as a publish command.
It supports either:
- Returning a URL built from a public base URL
- Returning a presigned GET URL
"""

from __future__ import annotations

import argparse
import mimetypes
import os
import sys
import uuid
from pathlib import Path
from urllib.parse import quote

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="Local file to publish")
    parser.add_argument("--bucket", default=os.getenv("WECOM_FILE_S3_BUCKET"))
    parser.add_argument("--region", default=os.getenv("WECOM_FILE_S3_REGION"))
    parser.add_argument("--endpoint-url", default=os.getenv("WECOM_FILE_S3_ENDPOINT_URL"))
    parser.add_argument("--key-prefix", default=os.getenv("WECOM_FILE_S3_KEY_PREFIX", "wecom"))
    parser.add_argument("--public-base-url", default=os.getenv("WECOM_FILE_PUBLIC_BASE_URL"))
    parser.add_argument(
        "--expires-in",
        type=int,
        default=int(os.getenv("WECOM_FILE_LINK_EXPIRES_IN", "604800")),
        help="Presigned URL expiration in seconds",
    )
    parser.add_argument(
        "--content-disposition",
        default=os.getenv("WECOM_FILE_CONTENT_DISPOSITION", "attachment"),
        choices=["attachment", "inline"],
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not upload. Print the URL that would be used.",
    )
    return parser.parse_args()


def require(value: str | None, name: str) -> str:
    if value:
        return value
    raise ValueError(f"missing required setting: {name}")


def build_object_key(prefix: str, file_path: Path) -> str:
    prefix = prefix.strip("/")
    token = uuid.uuid4().hex[:12]
    name = file_path.name
    if prefix:
        return f"{prefix}/{token}/{name}"
    return f"{token}/{name}"


def build_public_url(base_url: str, key: str) -> str:
    encoded_key = "/".join(quote(part) for part in key.split("/"))
    return f"{base_url.rstrip('/')}/{encoded_key}"


def create_client(region: str | None, endpoint_url: str | None):
    kwargs = {}
    if region:
        kwargs["region_name"] = region
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)


def upload_file(client, bucket: str, key: str, file_path: Path, content_disposition: str) -> None:
    mime_type, _ = mimetypes.guess_type(file_path.name)
    extra_args = {
        "ContentDisposition": f'{content_disposition}; filename="{file_path.name}"',
    }
    if mime_type:
        extra_args["ContentType"] = mime_type
    client.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args)


def generate_presigned_url(
    client,
    bucket: str,
    key: str,
    expires_in: int,
    content_disposition: str,
    filename: str,
) -> str:
    params = {
        "Bucket": bucket,
        "Key": key,
        "ResponseContentDisposition": f'{content_disposition}; filename="{filename}"',
    }
    return client.generate_presigned_url(
        "get_object",
        Params=params,
        ExpiresIn=expires_in,
    )


def main() -> int:
    try:
        args = parse_args()
        file_path = Path(args.file).expanduser().resolve()
        if not file_path.is_file():
            raise ValueError("file does not exist or is not a regular file")

        bucket = require(args.bucket, "WECOM_FILE_S3_BUCKET")
        key = build_object_key(args.key_prefix, file_path)

        if args.public_base_url:
            url = build_public_url(args.public_base_url, key)
            if args.dry_run:
                print(url)
                return 0

        client = create_client(args.region, args.endpoint_url)

        if not args.dry_run:
            upload_file(client, bucket, key, file_path, args.content_disposition)

        if args.public_base_url:
            print(build_public_url(args.public_base_url, key))
            return 0

        print(
            generate_presigned_url(
                client=client,
                bucket=bucket,
                key=key,
                expires_in=args.expires_in,
                content_disposition=args.content_disposition,
                filename=file_path.name,
            )
        )
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
