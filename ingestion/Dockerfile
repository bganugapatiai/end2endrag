# Build from repository root:
#   docker build -f cursor_project/rag/ingestion/Dockerfile .
#
# Pushes to ECR for EKS Jobs/Deployments or custom workers.

FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

# Full project so `uv sync` resolves all dependencies from pyproject.toml / uv.lock
COPY . .

RUN uv sync --frozen --no-dev

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# EKS: override with command/args, e.g.:
#   uv run python -m ingestion.worker --bucket my-bucket --prefix docs/
CMD ["uv", "run", "python", "-m", "ingestion.worker", "--help"]
#CMD ["uv", "run", "python", "-m", "ingestion.worker", "--help"]
#CMD ["uv", "run", "python", "-m", "worker", "--help"
