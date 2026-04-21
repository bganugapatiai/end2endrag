"""
Amazon S3 access for ingestion: metadata, bounded reads, and key listing.

Uses boto3 with explicit mapping of ``ClientError`` codes to
:class:`~ingestion.amcus_ingestion.exceptions.S3ObjectError` and size limits to
:class:`~ingestion.amcus_ingestion.exceptions.ObjectTooLargeError`.
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

import boto3
from botocore.exceptions import ClientError

from .exceptions import ObjectTooLargeError, S3ObjectError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3ObjectMetadata:
    """
    Snapshot of S3 object identity, HTTP metadata, and tagging.

    Attributes:
        bucket: Bucket name.
        key: Object key.
        etag: Normalized ETag (quotes stripped), used for idempotent document IDs.
        last_modified: Object last modified time (UTC-aware when S3 returns naive).
        content_length: Size from HEAD, if present.
        content_type: Content-Type from HEAD, if present.
        tags: Key/value map from ``get_object_tagging`` (empty if tagging fails).
    """

    bucket: str
    key: str
    etag: str
    last_modified: datetime
    content_length: int | None
    content_type: str | None
    tags: dict[str, str]


def _s3_client(region: str) -> Any:
    """Construct a regional S3 client."""
    return boto3.client("s3", region_name=region)


def head_object_metadata(
    bucket: str,
    key: str,
    *,
    region: str,
) -> S3ObjectMetadata:
    """
    Perform HEAD on an object and load object tags for RBAC metadata.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        region: AWS region for the S3 client.

    Returns:
        :class:`S3ObjectMetadata` including tags when permitted.

    Raises:
        S3ObjectError: Not found, access denied, or other API failure.
    """
    client = _s3_client(region)
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise S3ObjectError(f"S3 object not found: s3://{bucket}/{key}") from e
        if code == "403" or code == "AccessDenied":
            raise S3ObjectError(f"S3 access denied: s3://{bucket}/{key}") from e
        raise S3ObjectError(f"S3 head_object failed: s3://{bucket}/{key}: {e}") from e

    lm = resp.get("LastModified")
    if lm is not None and lm.tzinfo is None:
        lm = lm.replace(tzinfo=timezone.utc)

    etag = (resp.get("ETag") or "").strip('"')
    tags: dict[str, str] = {}
    try:
        tag_resp = client.get_object_tagging(Bucket=bucket, Key=key)
        for t in tag_resp.get("TagSet", []):
            k, v = t.get("Key"), t.get("Value")
            if k is not None and v is not None:
                tags[k] = v
    except ClientError as e:
        logger.warning("get_object_tagging failed for s3://%s/%s: %s", bucket, key, e)

    return S3ObjectMetadata(
        bucket=bucket,
        key=key,
        etag=etag,
        last_modified=lm or datetime.now(timezone.utc),
        content_length=resp.get("ContentLength"),
        content_type=resp.get("ContentType"),
        tags=tags,
    )


def read_object_body(
    bucket: str,
    key: str,
    *,
    region: str,
    max_bytes: int,
) -> bytes:
    """
    Download object bytes with a hard cap on total size.

    Streams in 64 KiB chunks so memory stays bounded. If ``ContentLength`` is
    known and exceeds ``max_bytes``, raises before reading the body.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        region: AWS region.
        max_bytes: Maximum bytes to read; raises :class:`ObjectTooLargeError` if exceeded.

    Raises:
        S3ObjectError: Same conditions as :func:`head_object_metadata`.
        ObjectTooLargeError: Object or stream exceeds ``max_bytes``.
    """
    client = _s3_client(region)
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise S3ObjectError(f"S3 object not found: s3://{bucket}/{key}") from e
        if code in ("403", "AccessDenied"):
            raise S3ObjectError(f"S3 access denied: s3://{bucket}/{key}") from e
        raise S3ObjectError(f"S3 get_object failed: s3://{bucket}/{key}: {e}") from e

    cl = resp.get("ContentLength")
    if cl is not None and cl > max_bytes:
        resp["Body"].close()
        raise ObjectTooLargeError(
            f"Object size {cl} exceeds max_object_bytes {max_bytes}",
            size_bytes=cl,
            max_bytes=max_bytes,
        )

    body = resp["Body"]
    chunks: list[bytes] = []
    total = 0
    try:
        for chunk in body.iter_chunks(chunk_size=65536):
            total += len(chunk)
            if total > max_bytes:
                raise ObjectTooLargeError(
                    f"Streamed object exceeded max_object_bytes {max_bytes}",
                    size_bytes=total,
                    max_bytes=max_bytes,
                )
            chunks.append(chunk)
    finally:
        body.close()

    return b"".join(chunks)


def stream_object_to_spooled_file(
    bucket: str,
    key: str,
    *,
    region: str,
    max_bytes: int,
    spool_max_memory_bytes: int = 5 * 1024 * 1024,
) -> tuple[tempfile.SpooledTemporaryFile[bytes], int, str]:
    """
    Stream object bytes into a spooled temp file and return size + sha256.

    The file stays in memory up to ``spool_max_memory_bytes`` and spills to disk
    for larger inputs, which keeps ingestion stable for huge PDFs.
    """
    import hashlib

    client = _s3_client(region)
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise S3ObjectError(f"S3 object not found: s3://{bucket}/{key}") from e
        if code in ("403", "AccessDenied"):
            raise S3ObjectError(f"S3 access denied: s3://{bucket}/{key}") from e
        raise S3ObjectError(f"S3 get_object failed: s3://{bucket}/{key}: {e}") from e

    cl = resp.get("ContentLength")
    if cl is not None and cl > max_bytes:
        resp["Body"].close()
        raise ObjectTooLargeError(
            f"Object size {cl} exceeds max_object_bytes {max_bytes}",
            size_bytes=cl,
            max_bytes=max_bytes,
        )

    body = resp["Body"]
    hasher = hashlib.sha256()
    out = tempfile.SpooledTemporaryFile(max_size=spool_max_memory_bytes, mode="w+b")
    total = 0
    try:
        for chunk in body.iter_chunks(chunk_size=65536):
            total += len(chunk)
            if total > max_bytes:
                raise ObjectTooLargeError(
                    f"Streamed object exceeded max_object_bytes {max_bytes}",
                    size_bytes=total,
                    max_bytes=max_bytes,
                )
            hasher.update(chunk)
            out.write(chunk)
    finally:
        body.close()
    out.seek(0)
    return out, total, hasher.hexdigest()


def iter_keys_under_prefix(
    bucket: str,
    prefix: str,
    *,
    region: str,
) -> Iterator[str]:
    """
    Yield object keys under ``prefix`` using paginated ``ListObjectsV2``.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix filter (may be empty for whole-bucket listing; use with care).
        region: AWS region.

    Yields:
        Object key strings.

    Raises:
        S3ObjectError: List API failure.
    """
    client = _s3_client(region)
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        try:
            resp = client.list_objects_v2(**kwargs)
        except ClientError as e:
            raise S3ObjectError(f"S3 list_objects_v2 failed: {e}") from e
        for obj in resp.get("Contents", []) or []:
            k = obj.get("Key")
            if k:
                yield k
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
        if not token:
            break
