"""
Create a Pinecone index client for ingestion writes.
"""

from __future__ import annotations

from pinecone import Pinecone

from .settings import IngestionSettings


def build_pinecone_client(settings: IngestionSettings) -> Pinecone:
    """Build a Pinecone API client using configured API key."""
    api_key = settings.pinecone_api_key
    if not api_key:
        raise RuntimeError(
            "Missing Pinecone API key. Set INGESTION_PINECONE_API_KEY or PINECONE_API_KEY."
        )
    return Pinecone(api_key=api_key)


def build_pinecone_index_client(settings: IngestionSettings):
    """Return an Index client bound to the configured index name."""
    pc = build_pinecone_client(settings)
    return pc.Index(settings.resolved_index_name)
