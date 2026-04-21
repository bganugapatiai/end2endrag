"""
LangServe-powered FastAPI app for on-demand ingestion.
"""

from __future__ import annotations

from fastapi import FastAPI
from langchain_core.runnables import RunnableLambda
from langserve import add_routes
from pydantic import BaseModel, Field

from ingestion.amcus_ingestion.pipeline import IngestionResult, ingest_object
from ingestion.amcus_ingestion.settings import IngestionSettings

app = FastAPI(
    title="Ingestion Service",
    version="0.1.0",
    description="S3 to Pinecone ingestion exposed through LangServe.",
)


class IngestRequest(BaseModel):
    """Payload used by the LangServe route."""

    bucket: str = Field(..., description="S3 bucket")
    key: str = Field(..., description="S3 object key")
    ensure_index: bool = Field(default=True, description="Create index if missing")


class IngestResponse(BaseModel):
    """Serialized ingestion result."""

    bucket: str
    key: str
    status: str
    chunks_indexed: int
    skipped_reason: str | None = None
    error: str | None = None
    dlq_payload: dict[str, object] = Field(default_factory=dict)


def _run_ingestion(payload: IngestRequest | dict[str, object]) -> IngestResponse:
    if isinstance(payload, dict):
        payload = IngestRequest.model_validate(payload)

    settings = IngestionSettings()
    result: IngestionResult = ingest_object(
        payload.bucket,
        payload.key,
        settings,
        ensure_index=payload.ensure_index,
    )
    return IngestResponse(
        bucket=result.bucket,
        key=result.key,
        status=result.status,
        chunks_indexed=result.chunks_indexed,
        skipped_reason=result.skipped_reason,
        error=result.error,
        dlq_payload=result.dlq_payload,
    )


ingest_runnable = RunnableLambda(_run_ingestion).with_types(
    input_type=IngestRequest,
    output_type=IngestResponse,
)
add_routes(app, ingest_runnable, path="/ingest")
