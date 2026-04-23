"""
CLI worker for batch ingestion (EKS Job, CronJob, or local runs).

Examples:

    # Single object
    uv run python -m ingestion.worker --bucket my-bucket --key path/doc.txt

    # All keys under a prefix (use with care on large buckets)
    uv run python -m ingestion.worker --bucket my-bucket --prefix docs/

Exit code is 0 on success for single-key mode; prefix mode exits 1 on any
``error`` status (other skip statuses are non-fatal).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from amcus_ingestion.pipeline import ingest_object
from amcus_ingestion.settings import IngestionSettings
from amcus_ingestion.s3_source import iter_keys_under_prefix

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    """Parse CLI arguments and run ingestion for one key or a prefix listing."""
    p = argparse.ArgumentParser(description="S3 to Pinecone ingestion worker")
    p.add_argument("--bucket", required=True)
    p.add_argument("--key", help="Single object key (omit with --prefix for listing)")
    p.add_argument("--prefix", default="", help="List all keys under this prefix")
    args = p.parse_args()

    settings = IngestionSettings()
    if args.key:
        result = ingest_object(args.bucket, args.key, settings, ensure_index=True)
        print(json.dumps(result.__dict__, default=str))
        sys.exit(0 if result.status == "ok" else 1)

    for key in iter_keys_under_prefix(args.bucket, args.prefix, region=settings.aws_region):
        result = ingest_object(args.bucket, key, settings, ensure_index=True)
        logger.info("%s %s", key, result.status)
        if result.status not in ("ok", "empty", "unsupported", "too_large"):
            sys.exit(1)


if __name__ == "__main__":
    main()
