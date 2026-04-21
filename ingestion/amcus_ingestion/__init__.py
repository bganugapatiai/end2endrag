"""
``amcus_ingestion`` — S3 to Pinecone ingestion library.

Exports the main pipeline entrypoints and settings. Prefer
:func:`~ingestion.amcus_ingestion.pipeline.ingest_object` for production use.
"""

from .pipeline import IngestionResult, ingest_object, ingest_object_bytes
from .settings import IngestionSettings, get_settings

__all__ = [
    "IngestionResult",
    "IngestionSettings",
    "get_settings",
    "ingest_object",
    "ingest_object_bytes",
]
