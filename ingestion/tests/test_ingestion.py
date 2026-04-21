"""
Unit tests for S3 to Pinecone ingestion (mocked AWS).

Use pytest markers to run focused subsets, e.g.:

    pytest ingestion/tests/test_ingestion.py -m ingestion_s3
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from ingestion import langserve_app
from ingestion.amcus_ingestion.chunking import split_into_chunks
from ingestion.amcus_ingestion.exceptions import PineconeError, UnsupportedContentError
from ingestion.amcus_ingestion.metadata import ChunkDocument, build_chunk_documents
from ingestion.amcus_ingestion.pipeline import IngestionResult
from ingestion.amcus_ingestion.parsing import bytes_to_text
from ingestion.amcus_ingestion.pinecone_index import ensure_ingestion_index, index_exists
from ingestion.amcus_ingestion.pinecone_ingest import upsert_chunks
from ingestion.amcus_ingestion.pipeline import ingest_object_bytes
from ingestion.amcus_ingestion.s3_source import head_object_metadata, read_object_body
from ingestion.amcus_ingestion.settings import IngestionSettings


def _settings(**kwargs: object) -> IngestionSettings:
    defaults: dict[str, object] = {
        "aws_region": "us-east-1",
        "index_name": "test-index",
        "pinecone_index_name": "test-index",
        "pinecone_api_key": "test-key",
        "embedding_dimensions": 8,
    }
    defaults.update(kwargs)
    return IngestionSettings(**defaults)


def _doc(settings: IngestionSettings, doc_id: str = "abc") -> tuple[ChunkDocument, list[float]]:
    doc = ChunkDocument(
        doc_id=doc_id,
        text="hi",
        chunk_index=0,
        total_chunks=1,
        tenant_id="t",
        allowed_groups=[],
        allowed_roles=[],
        source_last_modified="2024-01-01T00:00:00+00:00",
        indexed_at="2024-01-01T00:00:00+00:00",
        s3_bucket="b",
        s3_key="k",
        etag="e",
        content_sha256="x",
        idempotency_key="ik",
        chunker_version="1",
        content_type="text/plain",
    )
    emb = [0.1] * settings.embedding_dimensions
    return doc, emb


@pytest.mark.ingestion_metadata
def test_doc_id_stable_across_calls():
    import hashlib

    from ingestion.amcus_ingestion.chunking import TextChunk
    from ingestion.amcus_ingestion.s3_source import S3ObjectMetadata

    meta = S3ObjectMetadata(
        bucket="b",
        key="k",
        etag="e",
        last_modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
        content_length=5,
        content_type="text/plain",
        tags={},
    )
    s = _settings()
    chunks = [TextChunk("hello", 0, 1)]
    raw = b"hello"
    content_sha256 = hashlib.sha256(raw).hexdigest()
    d1 = build_chunk_documents(meta=meta, chunks=chunks, content_sha256=content_sha256, settings=s)
    d2 = build_chunk_documents(meta=meta, chunks=chunks, content_sha256=content_sha256, settings=s)
    assert d1[0].doc_id == d2[0].doc_id
    assert d1[0].idempotency_key == "b:k:e:0:1"


@pytest.mark.ingestion_chunking
def test_split_empty_returns_empty():
    assert split_into_chunks("   ", _settings()) == []


@pytest.mark.ingestion_parsing
def test_unsupported_mime():
    s = _settings()
    with pytest.raises(UnsupportedContentError):
        bytes_to_text(b"x", content_type="application/octet-stream", settings=s)


@pytest.mark.ingestion_parsing
@patch("ingestion.amcus_ingestion.parsing.pdfplumber.open")
def test_pdf_extracts_text(mock_pdf_open):
    s = _settings()
    page = MagicMock()
    page.extract_text.return_value = "Hello from PDF"
    pdf = MagicMock()
    pdf.pages = [page]
    cm = MagicMock()
    cm.__enter__.return_value = pdf
    cm.__exit__.return_value = None
    mock_pdf_open.return_value = cm

    text = bytes_to_text(b"%PDF-fake", content_type="application/pdf", settings=s)

    assert text == "Hello from PDF"


@pytest.mark.ingestion_parsing
@patch("ingestion.amcus_ingestion.parsing.pdfplumber.open")
def test_invalid_pdf_raises_unsupported(mock_pdf_open):
    s = _settings()
    mock_pdf_open.side_effect = RuntimeError("bad pdf")

    with pytest.raises(UnsupportedContentError):
        bytes_to_text(b"not-a-pdf", content_type="application/pdf", settings=s)


@mock_aws
@pytest.mark.ingestion_s3
def test_s3_head_and_read():
    region = "us-east-1"
    conn = boto3.client("s3", region_name=region)
    conn.create_bucket(Bucket="ingest-test")
    conn.put_object(
        Bucket="ingest-test",
        Key="docs/a.txt",
        Body=b"hello world",
        ContentType="text/plain",
    )
    conn.put_object_tagging(
        Bucket="ingest-test",
        Key="docs/a.txt",
        Tagging={
            "TagSet": [
                {"Key": "ingestion:tenant_id", "Value": "t1"},
                {"Key": "ingestion:allowed_groups", "Value": "g1 g2"},
            ]
        },
    )

    m = head_object_metadata("ingest-test", "docs/a.txt", region=region)
    assert m.bucket == "ingest-test"
    assert m.key == "docs/a.txt"
    assert m.tags.get("ingestion:tenant_id") == "t1"
    assert m.tags.get("ingestion:allowed_groups") == "g1 g2"

    body = read_object_body("ingest-test", "docs/a.txt", region=region, max_bytes=1024)
    assert body == b"hello world"


@pytest.mark.ingestion_pinecone_ingest
def test_upsert_success():
    settings = _settings()
    index_client = MagicMock()

    result = upsert_chunks(index_client, settings, [_doc(settings)])

    assert result == {"indexed": 1, "failed_ids": []}
    index_client.upsert.assert_called_once()


@patch("ingestion.amcus_ingestion.pinecone_ingest.time.sleep", return_value=None)
@pytest.mark.ingestion_pinecone_ingest
def test_upsert_retries_then_success(_mock_sleep):
    settings = _settings(max_bulk_retries=2)
    index_client = MagicMock()
    index_client.upsert.side_effect = [RuntimeError("throttled"), None]

    upsert_chunks(index_client, settings, [_doc(settings)])

    assert index_client.upsert.call_count == 2


@patch("ingestion.amcus_ingestion.pinecone_ingest.time.sleep", return_value=None)
@pytest.mark.ingestion_pinecone_ingest
def test_upsert_raises_after_retries(_mock_sleep):
    settings = _settings(max_bulk_retries=1)
    index_client = MagicMock()
    index_client.upsert.side_effect = RuntimeError("bad")

    with pytest.raises(PineconeError):
        upsert_chunks(index_client, settings, [_doc(settings)])


@pytest.mark.ingestion_pinecone_index
@patch("ingestion.amcus_ingestion.pinecone_index.build_pinecone_client")
def test_ensure_index_creates_when_missing(mock_build):
    settings = _settings(pinecone_index_name="new-index")
    pc = MagicMock()
    mock_build.return_value = pc

    list_result = MagicMock()
    list_result.names.return_value = []
    pc.list_indexes.return_value = list_result

    ensure_ingestion_index(settings)

    pc.create_index.assert_called_once()


@pytest.mark.ingestion_pinecone_index
@patch("ingestion.amcus_ingestion.pinecone_index.build_pinecone_client")
def test_index_exists_true(mock_build):
    settings = _settings(pinecone_index_name="existing-index")
    pc = MagicMock()
    mock_build.return_value = pc

    list_result = MagicMock()
    list_result.names.return_value = ["existing-index"]
    pc.list_indexes.return_value = list_result

    assert index_exists(settings) is True


@mock_aws
@patch("ingestion.amcus_ingestion.pipeline.embed_texts")
@pytest.mark.ingestion_pipeline
def test_pipeline_ingest_object_bytes(mock_embed):
    mock_embed.return_value = [[0.0] * 8]
    region = "us-east-1"
    conn = boto3.client("s3", region_name=region)
    conn.create_bucket(Bucket="pipeline-test-bucket")
    conn.put_object(
        Bucket="pipeline-test-bucket",
        Key="f.txt",
        Body=("The quick brown fox jumps over the lazy dog. " * 10).encode("utf-8"),
        ContentType="text/plain",
    )
    m = head_object_metadata("pipeline-test-bucket", "f.txt", region=region)
    raw = read_object_body("pipeline-test-bucket", "f.txt", region=region, max_bytes=1_000_000)
    settings = _settings(embedding_dimensions=8)
    client = MagicMock()

    r = ingest_object_bytes(m, raw, settings, client=client, ensure_index=False)
    assert r.status == "ok"
    assert r.chunks_indexed >= 1
    mock_embed.assert_called()
    client.upsert.assert_called()


@pytest.mark.ingestion_metadata
def test_pinecone_metadata_json_serializable():
    doc, _ = _doc(_settings(), doc_id="id")
    src = doc.to_pinecone_metadata()
    json.dumps(src)


@pytest.mark.ingestion_settings
def test_settings_defaults_and_override():
    s = IngestionSettings(
        pinecone_index_name="idx",
        index_name="legacy",
        pinecone_api_key="key",
        embedding_dimensions=512,
    )
    assert s.resolved_index_name == "idx"
    assert s.embedding_dimensions == 512


@pytest.mark.ingestion_exceptions
def test_object_too_large_error_attributes():
    from ingestion.amcus_ingestion.exceptions import ObjectTooLargeError

    e = ObjectTooLargeError("msg", size_bytes=999, max_bytes=100)
    assert e.size_bytes == 999
    assert e.max_bytes == 100


@pytest.mark.ingestion_api
def test_langserve_ingest_invoke_route(monkeypatch):
    def fake_ingest_object(bucket, key, settings, *, ensure_index=True, client=None):
        assert bucket == "demo-bucket"
        assert key == "docs/a.pdf"
        assert ensure_index is False
        return IngestionResult(
            bucket=bucket,
            key=key,
            status="ok",
            chunks_indexed=3,
        )

    monkeypatch.setattr(langserve_app, "ingest_object", fake_ingest_object)
    client = TestClient(langserve_app.app)

    response = client.post(
        "/ingest/invoke",
        json={
            "input": {
                "bucket": "demo-bucket",
                "key": "docs/a.pdf",
                "ensure_index": False,
            }
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["output"]["bucket"] == "demo-bucket"
    assert payload["output"]["key"] == "docs/a.pdf"
    assert payload["output"]["status"] == "ok"
    assert payload["output"]["chunks_indexed"] == 3
