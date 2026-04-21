"""
End-to-end orchestration: S3 object to chunked, embedded documents in Pinecone.

Typical flow:
    head metadata → read bytes → decode text → split chunks → build documents
    → embed → upsert vectors.

Errors that should not abort the whole product (unsupported MIME, empty content,
oversize object) are returned as :class:`IngestionResult` with non-ok status.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Iterator, TypeVar

from .chunking import split_into_chunks
from .embeddings import embed_texts
from .exceptions import (
    EmptyObjectError,
    ObjectTooLargeError,
    UnsupportedContentError,
)
from .metadata import ChunkDocument, build_chunk_documents
from .pinecone_client import build_pinecone_index_client
from .pinecone_index import ensure_ingestion_index
from .pinecone_ingest import upsert_chunks
from .parsing import bytes_to_text
from .s3_source import head_object_metadata, read_object_body, stream_object_to_spooled_file
from .settings import IngestionSettings

logger = logging.getLogger(__name__)
T = TypeVar("T")


def _iter_batches(items: list[T], batch_size: int) -> Iterator[list[T]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


@dataclass
class IngestionResult:
    """
    Outcome of processing a single S3 object.

    Attributes:
        bucket: S3 bucket name.
        key: Object key.
        status: ``ok``, ``empty``, ``unsupported``, ``too_large``, or ``error``.
        chunks_indexed: Number of vectors written when ``status == "ok"``.
        skipped_reason: Human-readable reason when skipped.
        dlq_payload: Structured metadata for dead-letter queues or metrics.
        error: Exception message when ``status == "error"``.
    """

    bucket: str
    key: str
    status: str
    chunks_indexed: int = 0
    skipped_reason: str | None = None
    dlq_payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


def ingest_object(
    bucket: str,
    key: str,
    settings: IngestionSettings,
    *,
    client: Any | None = None,
    ensure_index: bool = True,
) -> IngestionResult:
    """
    Run the full ingestion pipeline for one S3 object.

    Opens or reuses a Pinecone index client, optionally ensures the index exists,
    then reads and indexes the object.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        settings: Ingestion configuration.
        client: Optional pre-built Pinecone index client (tests or connection reuse).
        ensure_index: If True, ensure Pinecone index exists before upsert.

    Returns:
        :class:`IngestionResult` with status and chunk count or error details.

    Note:
        Deterministic document IDs imply upsert semantics when the same object
        version is reprocessed.
    """
    index_client = client or build_pinecone_index_client(settings)
    try:
        if ensure_index:
            ensure_ingestion_index(settings)

        meta = head_object_metadata(bucket, key, region=settings.aws_region)

        ct = (meta.content_type or "").split(";")[0].strip().lower()
        if ct == "application/pdf":
            try:
                from .parsing import extract_pdf_text_from_file

                spooled, _size, content_sha256 = stream_object_to_spooled_file(
                    bucket,
                    key,
                    region=settings.aws_region,
                    max_bytes=settings.max_object_bytes,
                )
            except ObjectTooLargeError as e:
                return IngestionResult(
                    bucket=bucket,
                    key=key,
                    status="too_large",
                    error=str(e),
                    dlq_payload={
                        "reason": "object_too_large",
                        "size_bytes": e.size_bytes,
                        "max_bytes": e.max_bytes,
                    },
                )
            try:
                text = extract_pdf_text_from_file(spooled)
            except UnsupportedContentError as e:
                return IngestionResult(
                    bucket=bucket,
                    key=key,
                    status="unsupported",
                    skipped_reason=str(e),
                    dlq_payload={"reason": "unsupported_content", "detail": str(e)},
                )
            finally:
                spooled.close()
        else:
            try:
                raw = read_object_body(
                    bucket,
                    key,
                    region=settings.aws_region,
                    max_bytes=settings.max_object_bytes,
                )
            except ObjectTooLargeError as e:
                return IngestionResult(
                    bucket=bucket,
                    key=key,
                    status="too_large",
                    error=str(e),
                    dlq_payload={
                        "reason": "object_too_large",
                        "size_bytes": e.size_bytes,
                        "max_bytes": e.max_bytes,
                    },
                )

            try:
                text = bytes_to_text(raw, content_type=meta.content_type, settings=settings)
            except UnsupportedContentError as e:
                return IngestionResult(
                    bucket=bucket,
                    key=key,
                    status="unsupported",
                    skipped_reason=str(e),
                    dlq_payload={"reason": "unsupported_content", "detail": str(e)},
                )
            content_sha256 = hashlib.sha256(raw).hexdigest()

        if not text.strip():
            raise EmptyObjectError("No text content after decoding")

        chunks = split_into_chunks(text, settings)
        if not chunks:
            return IngestionResult(
                bucket=bucket,
                key=key,
                status="empty",
                skipped_reason="no_chunks_after_split",
                dlq_payload={"reason": "empty_chunks"},
            )

        total_indexed = 0
        chunk_batch_size = max(settings.bulk_batch_size, 1)
        for chunk_batch in _iter_batches(chunks, chunk_batch_size):
            docs = build_chunk_documents(
                meta=meta,
                chunks=chunk_batch,
                content_sha256=content_sha256,
                settings=settings,
            )
            texts = [d.text for d in docs]
            vectors = embed_texts(texts, settings)
            paired: list[tuple[ChunkDocument, list[float]]] = list(zip(docs, vectors, strict=True))
            upsert_chunks(index_client, settings, paired)
            total_indexed += len(docs)
        return IngestionResult(
            bucket=bucket,
            key=key,
            status="ok",
            chunks_indexed=total_indexed,
        )
    except EmptyObjectError as e:
        return IngestionResult(
            bucket=bucket,
            key=key,
            status="empty",
            skipped_reason=str(e),
            dlq_payload={"reason": "empty_object"},
        )
    except Exception as e:
        logger.exception("ingest_object failed for s3://%s/%s", bucket, key)
        return IngestionResult(
            bucket=bucket,
            key=key,
            status="error",
            error=str(e),
            dlq_payload={"reason": "exception", "detail": str(e)},
        )


def ingest_object_bytes(
    meta: Any,
    raw: bytes,
    settings: IngestionSettings,
    *,
    client: Any,
    ensure_index: bool = False,
) -> IngestionResult:
    """
    Ingest from in-memory bytes and metadata (used in tests; skips S3 GET).

    Args:
        meta: :class:`~ingestion.amcus_ingestion.s3_source.S3ObjectMetadata` instance.
        raw: Raw object bytes.
        settings: Ingestion configuration.
        client: Pinecone index client (required).
        ensure_index: Whether to ensure index exists first.

    Returns:
        Same semantics as :func:`ingest_object`.
    """
    from .s3_source import S3ObjectMetadata

    if not isinstance(meta, S3ObjectMetadata):
        raise TypeError("meta must be S3ObjectMetadata")

    if ensure_index:
        ensure_ingestion_index(settings)

    try:
        text = bytes_to_text(raw, content_type=meta.content_type, settings=settings)
    except UnsupportedContentError as e:
        return IngestionResult(
            bucket=meta.bucket,
            key=meta.key,
            status="unsupported",
            skipped_reason=str(e),
        )

    if not text.strip():
        return IngestionResult(
            bucket=meta.bucket,
            key=meta.key,
            status="empty",
            skipped_reason="no_text",
        )

    chunks = split_into_chunks(text, settings)
    if not chunks:
        return IngestionResult(
            bucket=meta.bucket,
            key=meta.key,
            status="empty",
            skipped_reason="no_chunks_after_split",
        )

    content_sha256 = hashlib.sha256(raw).hexdigest()
    total_indexed = 0
    for chunk_batch in _iter_batches(chunks, max(settings.bulk_batch_size, 1)):
        docs = build_chunk_documents(
            meta=meta,
            chunks=chunk_batch,
            content_sha256=content_sha256,
            settings=settings,
        )
        texts = [d.text for d in docs]
        vectors = embed_texts(texts, settings)
        paired = list(zip(docs, vectors, strict=True))
        upsert_chunks(client, settings, paired)
        total_indexed += len(docs)
    return IngestionResult(
        bucket=meta.bucket,
        key=meta.key,
        status="ok",
        chunks_indexed=total_indexed,
    )
