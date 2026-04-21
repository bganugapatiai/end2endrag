"""
AWS Lambda handler for on-demand ingestion via API Gateway or direct invoke.

Environment variables use the ``INGESTION_`` prefix; see
:class:`ingestion.amcus_ingestion.settings.IngestionSettings`.

Event formats:

- **API Gateway (proxy)**: ``body`` is a JSON string ``{"bucket":"...","key":"..."}``.
- **Direct invoke**: same object at the top level.
- **Bucket default**: optional ``S3_BUCKET`` env var if ``bucket`` is omitted from payload.

Response: API Gateway proxy shape with ``statusCode``, ``headers``, and JSON ``body``
containing :class:`~ingestion.amcus_ingestion.pipeline.IngestionResult` fields.
HTTP 413 when the object exceeds ``INGESTION_MAX_OBJECT_BYTES``.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any

from ingestion.amcus_ingestion.pipeline import ingest_object
from ingestion.amcus_ingestion.settings import IngestionSettings

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _parse_event(event: dict[str, Any]) -> tuple[str, str]:
    """
    Extract bucket and key from Lambda event (API Gateway or direct).

    Raises:
        ValueError: If bucket or key cannot be resolved.
    """
    body = event.get("body")
    if isinstance(body, str):
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")
        payload = json.loads(body) if body else {}
    elif isinstance(body, dict):
        payload = body
    else:
        payload = {**event}
    bucket = payload.get("bucket") or os.environ.get("S3_BUCKET")
    key = payload.get("key")
    if not bucket or not key:
        raise ValueError("Event must include bucket and key")
    return bucket, key


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda entrypoint: ingest one S3 object and return JSON API Gateway response.

    Args:
        event: Lambda event dict.
        context: Lambda context (unused).

    Returns:
        Dict with ``statusCode``, ``headers``, and ``body`` (JSON string).
    """
    settings = IngestionSettings()
    bucket, key = _parse_event(event)
    result = ingest_object(bucket, key, settings, ensure_index=True)
    out = {
        "bucket": result.bucket,
        "key": result.key,
        "status": result.status,
        "chunks_indexed": result.chunks_indexed,
        "skipped_reason": result.skipped_reason,
        "error": result.error,
        "dlq_payload": result.dlq_payload,
    }
    status_code = 200 if result.status in ("ok", "empty", "unsupported", "too_large") else 500
    if result.status == "too_large":
        status_code = 413
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(out, default=str),
    }
