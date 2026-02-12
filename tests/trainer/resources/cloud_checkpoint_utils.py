#!/usr/bin/env python3
"""
Cloud checkpoint management utility for test infrastructure.

Usage:
    python cloud_checkpoint_utils.py --prepare   # Pre-test: delete old + create .keep
    python cloud_checkpoint_utils.py --cleanup   # Post-test: delete all

Supports: S3-compatible storage (AWS S3, MinIO)
Future: GCS (gs://), Azure Blob (azure://), etc.
"""

import argparse
import os
import sys


def is_cloud_storage(uri: str) -> bool:
    """Check if URI is cloud storage (contains protocol scheme)."""
    return "://" in uri


def parse_cloud_uri(uri: str) -> tuple[str, str, str]:
    """Parse cloud URI into protocol, bucket, and prefix."""
    if "://" not in uri:
        return "", "", ""

    protocol, rest = uri.split("://", 1)
    parts = rest.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    return protocol, bucket, prefix


def get_s3_client():
    """Create and return an S3 client using environment credentials."""
    s3_endpoint = os.getenv("AWS_DEFAULT_ENDPOINT", "")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    if not (s3_endpoint and s3_access_key and s3_secret_key):
        return None

    try:
        import boto3
        from botocore.config import Config
    except Exception as e:
        print(f"Warning: boto3 unavailable: {e}")
        return None

    endpoint_url = s3_endpoint if s3_endpoint.startswith("http") else f"https://{s3_endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        verify=False,
    )


def delete_all_objects(s3_client, bucket: str, prefix: str) -> int:
    """Delete all objects under the given S3 prefix.

    Returns: Number of objects deleted
    """
    if not prefix:
        print("Error: Cannot delete from empty prefix (safety check)")
        return 0

    deleted = 0
    full_prefix = prefix.rstrip("/") + "/"
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        for obj in page.get("Contents", []):
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1

    return deleted


def create_keep_file(s3_client, bucket: str, prefix: str) -> None:
    """Create a .keep placeholder file in S3 to ensure the prefix exists.

    This is required because MinIO/S3 doesn't create "folders" - paths only
    exist when files are present. The SDK's JITCheckpointCallback does ls()
    on init which fails if the prefix doesn't exist.
    """
    if not prefix:
        return

    keep_key = prefix.rstrip("/") + "/.keep"
    s3_client.put_object(Bucket=bucket, Key=keep_key, Body=b"placeholder")


def prepare_checkpoint_storage() -> int:
    """Prepare cloud checkpoint storage for test (delete old + create .keep)."""
    checkpoint_uri = os.getenv("CHECKPOINT_OUTPUT_DIR", "")

    if not is_cloud_storage(checkpoint_uri):
        print("Checkpoint prep: skip (not cloud storage)")
        return 0

    protocol, bucket, prefix = parse_cloud_uri(checkpoint_uri)

    if not bucket or not prefix:
        print(f"Checkpoint prep: skip (invalid URI: {checkpoint_uri})")
        return 0

    if protocol != "s3":
        print(f"Checkpoint prep: protocol '{protocol}' not yet supported")
        return 0

    s3_client = get_s3_client()
    if not s3_client:
        print("Checkpoint prep: skip (missing S3 credentials/boto3)")
        return 0

    try:
        deleted = delete_all_objects(s3_client, bucket, prefix)
        create_keep_file(s3_client, bucket, prefix)
        print(
            f"Checkpoint prep complete: cleaned {deleted} objects and created "
            f"s3://{bucket}/{prefix.rstrip('/')}/.keep"
        )
        return 0
    except Exception as e:
        print(f"Checkpoint prep warning: {e}")
        return 0


def cleanup_checkpoint_storage() -> int:
    """Cleanup cloud checkpoint storage after test (delete all)."""
    checkpoint_uri = os.getenv("CHECKPOINT_OUTPUT_DIR", "")

    if not is_cloud_storage(checkpoint_uri):
        print("Checkpoint cleanup: skip (not cloud storage)")
        return 0

    protocol, bucket, prefix = parse_cloud_uri(checkpoint_uri)

    if not bucket or not prefix:
        print(f"Checkpoint cleanup: skip (invalid URI: {checkpoint_uri})")
        return 0

    if protocol != "s3":
        print(f"Checkpoint cleanup: protocol '{protocol}' not yet supported")
        return 0

    s3_client = get_s3_client()
    if not s3_client:
        print("Checkpoint cleanup: skip (missing S3 credentials/boto3)")
        return 0

    try:
        print(f"Checkpoint cleanup: starting for {protocol}://{bucket}/{prefix}")
        deleted = delete_all_objects(s3_client, bucket, prefix)
        print(f"Checkpoint cleanup: deleted {deleted} objects from s3://{bucket}/{prefix}")
        return 0
    except Exception as e:
        print(f"Checkpoint cleanup warning: {e}")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Cloud checkpoint management utility")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--prepare", action="store_true", help="Prepare checkpoint storage (pre-test)")
    group.add_argument("--cleanup", action="store_true", help="Cleanup checkpoint storage (post-test)")

    args = parser.parse_args()

    if args.prepare:
        return prepare_checkpoint_storage()
    elif args.cleanup:
        return cleanup_checkpoint_storage()

    return 1


if __name__ == "__main__":
    sys.exit(main())
