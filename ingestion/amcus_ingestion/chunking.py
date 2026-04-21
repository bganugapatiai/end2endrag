"""
Split text into ordered chunks for embedding and indexing.

Uses LangChain's :class:`langchain_text_splitters.RecursiveCharacterTextSplitter`
with ``chunk_size`` and ``chunk_overlap`` from
:class:`~ingestion.amcus_ingestion.settings.IngestionSettings`.
Whitespace-only input yields an empty list (no chunks).
"""
from __future__ import annotations

from dataclasses import dataclass

from langchain_text_splitters import RecursiveCharacterTextSplitter

from .settings import IngestionSettings


@dataclass(frozen=True)
class TextChunk:
    """
    One segment of text with stable positional metadata.

    Attributes:
        text: Chunk content.
        chunk_index: Zero-based index within this document.
        total_chunks: Total number of chunks for this document.
    """

    text: str
    chunk_index: int
    total_chunks: int


def split_into_chunks(text: str, settings: IngestionSettings) -> list[TextChunk]:
    """
    Split ``text`` into :class:`TextChunk` records.

    Args:
        text: Full document text after decoding.
        settings: Supplies ``chunk_size`` and ``chunk_overlap``.

    Returns:
        A list of chunks, or an empty list if ``text`` is empty or whitespace-only.
    """
    if not text.strip():
        return []

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.chunk_size,
        chunk_overlap=settings.chunk_overlap,
        length_function=len,
    )
    parts = splitter.split_text(text)
    total = len(parts)
    return [
        TextChunk(text=p, chunk_index=i, total_chunks=total)
        for i, p in enumerate(parts)
    ]
