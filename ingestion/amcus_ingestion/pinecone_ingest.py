"""
Batch upsert chunk vectors into Pinecone.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

from .exceptions import PineconeError
from .metadata import ChunkDocument
from .settings import IngestionSettings

logger = logging.getLogger(__name__)


def upsert_chunks(
    index_client: Any,
    settings: IngestionSettings,
    docs_with_embeddings: list[tuple[ChunkDocument, list[float]]],
) -> dict[str, Any]:
    """Upsert all (document, embedding) pairs into Pinecone."""
    if not docs_with_embeddings:
        return {"indexed": 0, "failed_ids": []}

    batch_size = settings.bulk_batch_size
    total = 0
    for start in range(0, len(docs_with_embeddings), batch_size):
        batch = docs_with_embeddings[start : start + batch_size]
        _upsert_with_retry(index_client, settings, batch)
        total += len(batch)
    return {"indexed": total, "failed_ids": []}


def _upsert_with_retry(
    index_client: Any,
    settings: IngestionSettings,
    batch: list[tuple[ChunkDocument, list[float]]],
) -> None:
    vectors = [
        {"id": doc.doc_id, "values": emb, "metadata": doc.to_pinecone_metadata()}
        for doc, emb in batch
    ]

    attempt = 0
    while True:
        try:
            index_client.upsert(vectors=vectors, namespace=settings.pinecone_namespace)
            return
        except Exception as e:
            if attempt >= settings.max_bulk_retries:
                raise PineconeError(f"Pinecone upsert failed after retries: {e}") from e
            delay = min(
                settings.bulk_backoff_base_seconds * (2**attempt) + random.random() * 0.15,
                settings.bulk_backoff_max_seconds,
            )
            logger.warning(
                "Pinecone upsert retry (attempt %s) after %.2fs: %s",
                attempt + 1,
                delay,
                e,
            )
            time.sleep(delay)
            attempt += 1
