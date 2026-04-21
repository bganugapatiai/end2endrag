"""
Decode raw object bytes to Unicode text for chunking.

Enforces a MIME allowlist from :class:`~ingestion.amcus_ingestion.settings.IngestionSettings`.
For text-like content, tries UTF-8 first, then charset-normalizer for legacy
encodings, and rejects content that looks binary. For PDF content types, extracts
text via ``pdfplumber``.
"""

from __future__ import annotations

import logging
from io import BytesIO
from typing import TYPE_CHECKING, BinaryIO

from charset_normalizer import from_bytes
import pdfplumber

from .exceptions import UnsupportedContentError

if TYPE_CHECKING:
    from .settings import IngestionSettings

logger = logging.getLogger(__name__)


def extract_pdf_text_from_file(fileobj: BinaryIO) -> str:
    """Extract concatenated text from a PDF file-like object using pdfplumber."""
    try:
        fileobj.seek(0)
        with pdfplumber.open(fileobj) as pdf:
            parts: list[str] = []
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text:
                    parts.append(page_text)
            return "\n".join(parts).strip()
    except Exception as exc:  # pragma: no cover - parser internals vary by file
        raise UnsupportedContentError(f"Failed to parse PDF content: {exc}") from exc

def _extract_pdf_text(raw: bytes) -> str:
    """Extract concatenated text from PDF bytes."""
    return extract_pdf_text_from_file(BytesIO(raw))


def _mime_allowed(content_type: str | None, settings: IngestionSettings) -> bool:
    """
    Return True if the Content-Type is allowed for text ingestion.

    Unknown ``content_type`` returns True so decoding can still proceed (useful
    when S3 omits Content-Type).
    """
    if not content_type:
        return True
    ct = content_type.split(";")[0].strip().lower()
    if ct in settings.allowed_mime_exact:
        return True
    for prefix in settings.allowed_mime_prefixes:
        if ct.startswith(prefix):
            return True
    return False


def bytes_to_text(
    raw: bytes,
    *,
    content_type: str | None,
    settings: IngestionSettings,
) -> str:
    """
    Decode ``raw`` to a string suitable for text splitting.

    Args:
        raw: Object body bytes.
        content_type: S3 Content-Type header value (may be None).
        settings: Ingestion settings controlling MIME allowlist.

    Returns:
        Decoded text (empty string if ``raw`` is empty).

    Raises:
        UnsupportedContentError: MIME not allowed, UTF-8 decode fails and
            charset detection fails, or binary-like content detected.
    """
    if not raw:
        return ""

    if not _mime_allowed(content_type, settings):
        raise UnsupportedContentError(
            f"Content type not allowed for ingestion: {content_type!r}"
        )

    ct = (content_type or "").split(";")[0].strip().lower()
    if ct == "application/pdf":
        return _extract_pdf_text(raw)

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = None

    if text is not None:
        if "\x00" in text:
            raise UnsupportedContentError("NUL bytes in decoded text; likely binary")
        return text

    match = from_bytes(raw)
    if not match:
        raise UnsupportedContentError("Could not detect charset for binary-looking content")

    best = match.best()
    if best is None:
        raise UnsupportedContentError("Charset detection produced no candidate")

    if best.coherence < 0.2 and len(raw) > 100:
        logger.warning(
            "Low charset coherence (%.3f); content may be binary",
            best.coherence,
        )
        raise UnsupportedContentError("Content appears binary or undecodable as text")

    text = str(best)
    if "\x00" in text:
        raise UnsupportedContentError("NUL bytes in decoded text; likely binary")

    return text
