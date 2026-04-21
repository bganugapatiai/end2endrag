"""
Build per-chunk documents with recency, RBAC-oriented fields, and provenance.

Document IDs are deterministic SHA-256 hashes of
``(bucket, key, etag, chunk_index, chunker_version)`` so re-ingesting the same
S3 version upserts the same vector id.

RBAC lists are parsed from S3 object tags (keys configured in
:class:`~ingestion.amcus_ingestion.settings.IngestionSettings`) using
:func:`_parse_tag_list` (JSON array, comma/semicolon/pipe, or whitespace).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .chunking import TextChunk
from .s3_source import S3ObjectMetadata
from .settings import IngestionSettings


def _parse_tag_list(raw: str | None) -> list[str]:
    """
    Parse a tag value into a list of group or role strings.

    Supports JSON array literals, delimiter-separated values, or whitespace-
    separated tokens when no delimiter is present.
    """
    if not raw:
        return []
    raw = raw.strip()
    if raw.startswith("["):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
        except json.JSONDecodeError:
            pass
    if re.search(r"[;,|]", raw):
        return [x.strip() for x in re.split(r"[;,|]", raw) if x.strip()]
    return [x.strip() for x in raw.split() if x.strip()]


def _doc_id(
    bucket: str,
    key: str,
    etag: str,
    chunk_index: int,
    chunker_version: str,
) -> str:
    """Return hex SHA-256 used as deterministic vector id."""
    payload = f"{bucket}|{key}|{etag}|{chunk_index}|{chunker_version}".encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class ChunkDocument:
    """
    Logical document for one chunk before embedding.

    :meth:`to_pinecone_metadata` returns scalar fields stored as Pinecone metadata.
    """

    doc_id: str
    text: str
    chunk_index: int
    total_chunks: int
    tenant_id: str
    allowed_groups: list[str]
    allowed_roles: list[str]
    source_last_modified: str
    indexed_at: str
    s3_bucket: str
    s3_key: str
    etag: str
    content_sha256: str
    idempotency_key: str
    chunker_version: str
    content_type: str | None

    def to_pinecone_metadata(self) -> dict[str, Any]:
        """
        Build metadata payload for one vector upsert.
        """
        return {
            "text": self.text,
            "chunk_index": self.chunk_index,
            "total_chunks": self.total_chunks,
            "tenant_id": self.tenant_id,
            "allowed_groups": self.allowed_groups,
            "allowed_roles": self.allowed_roles,
            "source_last_modified": self.source_last_modified,
            "indexed_at": self.indexed_at,
            "s3_bucket": self.s3_bucket,
            "s3_key": self.s3_key,
            "etag": self.etag,
            "content_sha256": self.content_sha256,
            "idempotency_key": self.idempotency_key,
            "chunker_version": self.chunker_version,
            "content_type": self.content_type,
        }


def build_chunk_documents(
    *,
    meta: S3ObjectMetadata,
    chunks: list[TextChunk],
    content_sha256: str,
    settings: IngestionSettings,
) -> list[ChunkDocument]:
    """
    Create one :class:`ChunkDocument` per :class:`TextChunk`.

    Args:
        meta: S3 metadata including tags for tenant and RBAC lists.
        chunks: Output of :func:`~ingestion.amcus_ingestion.chunking.split_into_chunks`.
        content_sha256: SHA-256 digest for source object bytes.
        settings: Chunker version and default RBAC when tags are absent.

    Returns:
        Ordered list of chunk documents ready for embedding.
    """
    indexed_at = datetime.now(timezone.utc).isoformat()
    source_last_modified = meta.last_modified.astimezone(timezone.utc).isoformat()

    tenant = meta.tags.get(settings.tenant_tag_key) or settings.default_tenant_id
    groups_raw = meta.tags.get(settings.rbac_groups_tag_key)
    roles_raw = meta.tags.get(settings.rbac_roles_tag_key)
    groups = _parse_tag_list(groups_raw) if groups_raw else list(settings.default_allowed_groups)
    roles = _parse_tag_list(roles_raw) if roles_raw else list(settings.default_allowed_roles)

    out: list[ChunkDocument] = []
    for ch in chunks:
        doc_id = _doc_id(
            meta.bucket,
            meta.key,
            meta.etag,
            ch.chunk_index,
            settings.chunker_version,
        )
        idempotency_key = f"{meta.bucket}:{meta.key}:{meta.etag}:{ch.chunk_index}:{settings.chunker_version}"
        out.append(
            ChunkDocument(
                doc_id=doc_id,
                text=ch.text,
                chunk_index=ch.chunk_index,
                total_chunks=ch.total_chunks,
                tenant_id=tenant,
                allowed_groups=groups,
                allowed_roles=roles,
                source_last_modified=source_last_modified,
                indexed_at=indexed_at,
                s3_bucket=meta.bucket,
                s3_key=meta.key,
                etag=meta.etag,
                content_sha256=content_sha256,
                idempotency_key=idempotency_key,
                chunker_version=settings.chunker_version,
                content_type=meta.content_type,
            )
        )
    return out
