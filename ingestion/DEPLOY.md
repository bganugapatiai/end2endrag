# Deploying the ingestion pipeline (AWS + Pinecone)

Build context is the **repository root** (where `pyproject.toml` and `uv.lock` live).

## 1. Pinecone setup

1. Create a Pinecone project and serverless index.
2. Set index dimensions to match your embedding model output.
3. Save your API key and configure:
   - `PINECONE_API_KEY`
   - `INGESTION_PINECONE_INDEX_NAME`
   - `INGESTION_PINECONE_NAMESPACE`
   - `INGESTION_PINECONE_CLOUD`
   - `INGESTION_PINECONE_REGION`

## 2. IAM for the ingestion role

Grant:

- **S3**: `s3:GetObject`, `s3:GetObjectTagging`, `s3:ListBucket` (scoped to source bucket/prefix).
- **Bedrock**: `bedrock:InvokeModel` on your embedding model (e.g. `amazon.titan-embed-text-v2:0`).

## 3. ECR

```bash
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker build -f ingestion/Dockerfile -t ingestion:latest .
docker tag ingestion:latest <account>.dkr.ecr.<region>.amazonaws.com/ingestion:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/ingestion:latest
```

Use the same image for the CLI worker, batch jobs, or the FastAPI service; only the container **command** changes.

## 4. Lambda + API Gateway

1. Package the repo (or a slim layer) so `PYTHONPATH` includes the project root and `ingestion` imports resolve.
2. Set handler to `ingestion.lambda_handler.handler` (or copy `lambda_handler.py` to root and set `lambda_handler.handler`).
3. Configure environment variables (`INGESTION_*`, `PINECONE_API_KEY`, `AWS_REGION`).
4. Attach the ingestion IAM role.
5. Create an HTTP API or REST API; integrate `POST` with the Lambda. Request body: `{"bucket":"...","key":"..."}`.

## 5. EKS

### 5.1 Batch / CLI worker

1. Create a Kubernetes `ServiceAccount` with IRSA mapping to the ingestion IAM role.
2. Run a `Job` or `Deployment` using the ECR image; override command, for example:

   ```yaml
   command: ["uv", "run", "python", "-m", "ingestion.worker"]
   args: ["--bucket", "my-bucket", "--prefix", "docs/"]
   ```

3. For large backfills, optionally drive work from **SQS** (S3 events → queue) and scale workers with **KEDA**.

### 5.2 FastAPI (LangServe) — same ECR image

The default image `CMD` runs the worker help text. For HTTP ingestion, override the command so **uvicorn listens on all interfaces** (required inside the pod for `Service` / `Ingress` to reach the process):

```yaml
command: ["uv", "run", "uvicorn", "ingestion.langserve_app:app", "--host", "0.0.0.0", "--port", "8000"]
```

1. **Push** the image from [ECR](#3-ecr) above (for example `<account>.dkr.ecr.<region>.amazonaws.com/ingestion:latest`).
2. **Configure** the pod with the same env vars as local runs: `PINECONE_API_KEY`, `INGESTION_*`, `AWS_REGION`, and any credentials your `IngestionSettings` expects (use `Secret` / External Secrets, not plain manifests).
3. **Expose** the app with a `Service` on port **8000** (targeting container port 8000). Typical patterns:
   - **`LoadBalancer`** or **NLB**: use the provisioned hostname or IP as the base URL.
   - **`ClusterIP` + Ingress** (ALB Ingress Controller, NGINX, etc.): use the Ingress host and path rules you define.
   - **Port-forward** (debug only): `kubectl port-forward deploy/<name> 8000:8000` then call `http://127.0.0.1:8000/...` from your machine.

**Invoke the API** (replace `<base>` with `http://<load-balancer-host>` or `https://<ingress-host>`):

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `<base>/docs` | OpenAPI / Swagger UI |
| `POST` | `<base>/ingest/invoke` | LangServe single invoke |
| `POST` | `<base>/ingest/batch` | LangServe batch |

Example (from a shell that can reach the cluster endpoint):

```bash
curl -sS -X POST "<base>/ingest/invoke" \
  -H "Content-Type: application/json" \
  -d '{"input": {"bucket": "my-bucket", "key": "path/to/doc.pdf", "ensure_index": true}}'
```

LangServe expects the runnable payload under an `input` key for `/invoke` routes; adjust if your client sends the schema LangServe documents at `<base>/ingest/input_schema`.

Minimal `Deployment` + `Service` sketch (image and env are placeholders):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion-api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ingestion-api
  template:
    metadata:
      labels:
        app: ingestion-api
    spec:
      serviceAccountName: ingestion-sa
      containers:
        - name: api
          image: <account>.dkr.ecr.<region>.amazonaws.com/ingestion:latest
          command: ["uv", "run", "uvicorn", "ingestion.langserve_app:app", "--host", "0.0.0.0", "--port", "8000"]
          ports:
            - containerPort: 8000
          envFrom:
            - secretRef:
                name: ingestion-env
---
apiVersion: v1
kind: Service
metadata:
  name: ingestion-api
spec:
  selector:
    app: ingestion-api
  ports:
    - port: 80
      targetPort: 8000
  type: LoadBalancer
```

Change `type` and add an `Ingress` as needed for your cluster; keep **targetPort 8000** aligned with uvicorn.

## 6. Optional: S3 → SQS → Lambda

1. Configure S3 event notifications to an SQS queue (filter by prefix/suffix).
2. Lambda consumes SQS messages; parse `Records[].s3.bucket.name` and `object.key` and call `ingest_object`.

## Local tests

```bash
uv sync --extra dev
pytest ingestion/tests/test_ingestion.py -v
```

## Environment reference

All settings use the `INGESTION_` prefix; see `ingestion/amcus_ingestion/settings.py` for fields (Pinecone index config, chunk sizes, RBAC tag keys, retry settings, and limits).
