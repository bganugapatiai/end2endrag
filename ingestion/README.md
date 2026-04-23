# RAG ingestion (S3 to Pinecone)

This folder contains the ingestion package that:

1. Reads source documents from Amazon S3.
2. Parses and chunks content (PDF parsing uses `pdfplumber`).
3. Generates embeddings with Amazon Bedrock.
4. Upserts vectors and metadata into Pinecone.

## Layout

| Path | Role |
|------|------|
| `amcus_ingestion/` | Core ingestion library modules. |
| `worker.py` | CLI worker for single key or prefix ingestion. |
| `lambda_handler.py` | Lambda entrypoint for API Gateway/direct invoke. |
| `tests/test_ingestion.py` | Unit tests (moto S3 + mocked Pinecone/Bedrock). |
| `DEPLOY.md` | Deployment notes for AWS + Pinecone setup. |

## Run tests

From repository root:

```bash
uv sync --extra dev
pytest ingestion/tests/test_ingestion.py -v
```

Run by marker:

```bash
pytest ingestion/tests/test_ingestion.py -m ingestion_s3
pytest ingestion/tests/test_ingestion.py -m ingestion_pinecone_ingest
pytest ingestion/tests/test_ingestion.py -m ingestion_pinecone_index
```

## Run FastAPI (LangServe)

From repository root:

```bash
uvicorn ingestion.langserve_app:app --host 0.0.0.0 --port 8000 --reload
```

LangServe endpoints are mounted under `/ingest`, including:

- `POST /ingest/invoke`
- `POST /ingest/batch`
- `GET /ingest/input_schema`
- `GET /ingest/output_schema`

## Key configuration

`IngestionSettings` loads from AWS Secrets Manager and `INGESTION_` env vars.
Set the secret id with:

- `INGESTION_AWS_SECRETS_MANAGER_SECRET_ID`
- `INGESTION_AWS_SECRETS_MANAGER_REGION` (optional; defaults to `AWS_REGION`)

Environment variables still override secret values when both are present.

Required Pinecone settings:

- `PINECONE_API_KEY`
- `INGESTION_PINECONE_INDEX_NAME` (or fallback `INGESTION_INDEX_NAME`)
- `INGESTION_PINECONE_NAMESPACE`
- `INGESTION_PINECONE_CLOUD`
- `INGESTION_PINECONE_REGION`

Other important settings:

- `INGESTION_BEDROCK_EMBEDDING_MODEL_ID`
- `INGESTION_EMBEDDING_DIMENSIONS`
- `INGESTION_MAX_OBJECT_BYTES`
- `INGESTION_BULK_BATCH_SIZE`

### Example AWS secret payload

Store a JSON object in Secrets Manager:

```json
{
  "INGESTION_PINECONE_API_KEY": "pc-xxxx",
  "INGESTION_PINECONE_INDEX_NAME": "ingestion-chunks",
  "INGESTION_PINECONE_NAMESPACE": "default",
  "INGESTION_PINECONE_CLOUD": "aws",
  "INGESTION_PINECONE_REGION": "us-east-1",
  "INGESTION_BEDROCK_EMBEDDING_MODEL_ID": "amazon.titan-embed-text-v2:0",
  "INGESTION_EMBEDDING_DIMENSIONS": 512
}
```

## Huge file ingestion behavior

- PDFs are streamed from S3 into a spooled temp file (memory then disk spillover).
- Non-PDF objects still enforce `INGESTION_MAX_OBJECT_BYTES`.
- Chunk embedding/upsert runs in batches to avoid loading all vectors in memory.

## RBAC metadata

Each chunk stores `tenant_id`, `allowed_groups`, and `allowed_roles` in Pinecone metadata.
Your retrieval layer should apply these metadata filters at query time.
