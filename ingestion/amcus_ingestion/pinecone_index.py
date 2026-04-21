"""
Ensure the target Pinecone index exists with expected dimensions.
"""

from __future__ import annotations

from pinecone import ServerlessSpec

from .pinecone_client import build_pinecone_client
from .settings import IngestionSettings


def index_exists(settings: IngestionSettings) -> bool:
    """Return True when configured Pinecone index already exists."""
    pc = build_pinecone_client(settings)
    indexes = pc.list_indexes()
    if hasattr(indexes, "names"):
        return settings.resolved_index_name in indexes.names()
    return any(item.get("name") == settings.resolved_index_name for item in indexes)


def ensure_ingestion_index(settings: IngestionSettings) -> None:
    """
    Create Pinecone index if it does not already exist.

    The index uses embedding dimensions from settings.
    """
    pc = build_pinecone_client(settings)
    name = settings.resolved_index_name
    indexes = pc.list_indexes()
    names = indexes.names() if hasattr(indexes, "names") else [i["name"] for i in indexes]
    if name in names:
        return

    pc.create_index(
        name=name,
        dimension=settings.embedding_dimensions,
        metric=settings.pinecone_metric,
        spec=ServerlessSpec(
            cloud=settings.pinecone_cloud,
            region=settings.pinecone_region,
        ),
    )
