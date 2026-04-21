"""
Amazon Bedrock embedding generation for chunk text.

Calls ``bedrock-runtime`` ``invoke_model`` with the Titan Text Embeddings v2
request shape (``inputText``, ``dimensions``, ``normalize``). Retries with
exponential backoff when Bedrock returns throttling errors.

This module is used to embed text into a vector for embedding and indexing. 
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

from .settings import IngestionSettings

logger = logging.getLogger(__name__)


def _bedrock_client(region: str) -> Any:
    """Return a regional Bedrock Runtime client."""
    return boto3.client("bedrock-runtime", region_name=region)


def embed_texts(
    texts: list[str],
    settings: IngestionSettings,
) -> list[list[float]]:
    """
    Embed each string in order; one Bedrock request per string.

    Args:
        texts: Chunk texts (same order as :class:`~ingestion.amcus_ingestion.metadata.ChunkDocument` list).
        settings: Model id, dimensions, and retry policy.

    Returns:
        List of embedding vectors (list of floats), same length as ``texts``.

    Note:
        For large documents, callers should rely on chunking settings to keep
        each ``inputText`` within model limits.
    """
    client = _bedrock_client(settings.aws_region)
    out: list[list[float]] = []
    for text in texts:
        out.append(_embed_one(client, text, settings))
    return out


def _embed_one(client: Any, text: str, settings: IngestionSettings) -> list[float]:
    """Invoke embedding model once with throttle retries."""
    body = json.dumps(
        {
            "inputText": text,
            "dimensions": settings.embedding_dimensions,
            "normalize": True,
        }
    )
    attempt = 0
    while True:
        try:
            resp = client.invoke_model(
                modelId=settings.bedrock_embedding_model_id,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
            raw = resp["body"].read()
            data = json.loads(raw)
            emb = data.get("embedding")
            if not isinstance(emb, list):
                raise RuntimeError(f"Unexpected embedding response: {data!r}")
            return [float(x) for x in emb]
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            throttled = code in ("ThrottlingException", "TooManyRequestsException")
            if not throttled or attempt >= settings.max_embedding_retries:
                raise
            delay = min(
                settings.embedding_backoff_base_seconds * (2**attempt)
                + random.random() * 0.1,
                60.0,
            )
            logger.warning(
                "Bedrock throttled (attempt %s), sleeping %.2fs",
                attempt + 1,
                delay,
            )
            time.sleep(delay)
            attempt += 1
