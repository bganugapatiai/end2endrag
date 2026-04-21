"""
Runtime configuration for ingestion via environment variables.

All settings support the ``INGESTION_`` prefix (e.g. ``INGESTION_PINECONE_INDEX_NAME``).
See :class:`IngestionSettings` fields for the full list. Values can also be loaded
from a ``.env`` file in the working directory when using pydantic-settings.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# pydantic-settings loads env_file paths relative to the *process* cwd, not this file.
# Search common locations so `uv run` / IDE / `python -m ingestion.worker` all find `.env`.
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parents[2]
_ENV_CANDIDATES = (
    _REPO_ROOT / ".env",
    _HERE / ".env",
    Path(".env"),
)


class IngestionSettings(BaseSettings):
    """
    Central configuration for S3 reads, Bedrock embeddings, and Pinecone upserts.

    RBAC metadata:
        Default tenant and group/role lists apply when S3 object tags omit the
        configured tag keys (``tenant_tag_key``, ``rbac_groups_tag_key``, ``rbac_roles_tag_key``).
    """

    model_config = SettingsConfigDict(
        env_prefix="INGESTION_",
        env_file=_ENV_CANDIDATES,
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    aws_region: str = Field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))

    # Pinecone index settings (accept PINECONE_API_KEY or INGESTION_PINECONE_API_KEY in .env)
    pinecone_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "pinecone_api_key",
            "PINECONE_API_KEY",
            "INGESTION_PINECONE_API_KEY",
        ),
    )
    pinecone_index_name: str | None = None
    index_name: str = "ingestion-chunks"
    pinecone_namespace: str = "default"
    pinecone_cloud: str = "aws"
    pinecone_region: str = Field(
        default_factory=lambda: os.getenv("PINECONE_REGION", "us-east-1"),
        validation_alias=AliasChoices("pinecone_region", "PINECONE_REGION", "INGESTION_PINECONE_REGION"),
    )
    pinecone_metric: Literal["cosine", "dotproduct", "euclidean"] = "cosine"

    # Titan Embeddings V2 default; dimensions must match index mapping
    bedrock_embedding_model_id: str = "amazon.titan-embed-text-v2:0"
    embedding_dimensions: int = 512

    chunk_size: int = 1000
    chunk_overlap: int = 200
    chunker_version: str = "1"

    # RBAC defaults when not overridden by tags / sidecar
    default_tenant_id: str = "default"
    default_allowed_groups: list[str] = Field(default_factory=list)
    default_allowed_roles: list[str] = Field(default_factory=list)

    # S3 object tag keys used for RBAC (comma-separated in env: KEY1,KEY2)
    rbac_groups_tag_key: str = "ingestion:allowed_groups"
    rbac_roles_tag_key: str = "ingestion:allowed_roles"
    tenant_tag_key: str = "ingestion:tenant_id"

    # Max bytes read in Lambda-style runs (fail fast for EKS offload)
    max_object_bytes: int = 15 * 1024 * 1024

    # Allowed content types (prefix match for text/*)
    allowed_mime_prefixes: tuple[str, ...] = ("text/", "application/json")
    allowed_mime_exact: tuple[str, ...] = ("application/pdf", "application/xml", "application/xhtml+xml")

    # Bulk / retries
    bulk_batch_size: int = 50
    max_bulk_retries: int = 5
    bulk_backoff_base_seconds: float = 0.5
    bulk_backoff_max_seconds: float = 30.0

    # Bedrock embedding retries
    max_embedding_retries: int = 5
    embedding_backoff_base_seconds: float = 0.25

    @property
    def resolved_index_name(self) -> str:
        """Return configured Pinecone index name with fallback to legacy index_name."""
        return self.pinecone_index_name or self.index_name

    @field_validator("default_allowed_groups", "default_allowed_roles", mode="before")
    @classmethod
    def split_csv(cls, v: str | list[str]) -> list[str]:
        """Allow comma-separated env strings for group/role lists."""
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return list(v)


@lru_cache
def get_settings() -> IngestionSettings:
    """
    Return a cached :class:`IngestionSettings` instance.

    Note:
        Call ``get_settings.cache_clear()`` in tests if you mutate environment
        variables and need a fresh settings object.
    """
    return IngestionSettings()

if __name__ == "__main__":
    s = get_settings()
    print("resolved_index_name:", s.resolved_index_name)
    print("pinecone_api_key set:", bool(s.pinecone_api_key))
