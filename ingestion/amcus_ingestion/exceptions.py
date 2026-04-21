"""
Typed exceptions for the S3-to-vector-store ingestion pipeline.

These types allow callers (Lambda, EKS workers, tests) to branch on failure mode:
missing objects, oversize payloads, unsupported MIME types, and write failures.
"""

from __future__ import annotations


class IngestionError(Exception):
    """Base class for all ingestion failures."""

    pass


class S3ObjectError(IngestionError):
    """Raised when S3 HEAD/GET/LIST fails (not found, access denied, or API error)."""

    pass


class ObjectTooLargeError(IngestionError):
    """
    Raised when an object exceeds ``max_object_bytes`` for the current run.

    Attributes:
        size_bytes: Observed or streamed size that exceeded the limit.
        max_bytes: Configured cap from :class:`~ingestion.amcus_ingestion.settings.IngestionSettings`.
    """

    def __init__(self, message: str, size_bytes: int, max_bytes: int):
        super().__init__(message)
        self.size_bytes = size_bytes
        self.max_bytes = max_bytes


class UnsupportedContentError(IngestionError):
    """Raised when MIME type is not allowlisted or content cannot be decoded as text."""

    pass


class EmptyObjectError(IngestionError):
    """Raised when an object decodes to no usable text or produces no chunks."""

    pass


class PineconeError(IngestionError):
    """Raised when Pinecone upsert fails after configured retries."""

    pass
