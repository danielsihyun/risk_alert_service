"""
Storage abstraction for reading Parquet files from multiple backends.

Supported URI schemes:
  - file:///path/to/file.parquet   (local filesystem)
  - gs://bucket/path/file.parquet  (Google Cloud Storage)
  - s3://bucket/path/file.parquet  (AWS S3 — interface defined, implementation stubbed)

Usage:
    from app.storage import open_uri
    table = open_uri("file:///data/monthly.parquet", columns=["account_id", "month", "status"])
    table = open_uri("gs://bucket/file.parquet", filters=[("month", ">=", "2025-06-01")])
"""

import os
import tempfile
from urllib.parse import urlparse

import pyarrow.parquet as pq


def open_uri(source_uri: str, columns: list[str] | None = None, filters=None):
    """
    Open a Parquet file from a URI and return a PyArrow Table.

    Args:
        source_uri: URI string (file://, gs://, s3://)
        columns: Optional list of column names to read (projection pushdown)
        filters: Optional PyArrow row-group filters (predicate pushdown)

    Returns:
        pyarrow.Table with only the requested columns/rows
    """
    parsed = urlparse(source_uri)
    scheme = parsed.scheme.lower()

    if scheme == "file" or scheme == "":
        return _read_local(parsed.path, columns, filters)
    elif scheme == "gs":
        return _read_gcs(parsed.netloc, parsed.path.lstrip("/"), columns, filters)
    elif scheme == "s3":
        return _read_s3(parsed.netloc, parsed.path.lstrip("/"), columns, filters)
    else:
        raise ValueError(f"Unsupported URI scheme: {scheme}. Supported: file, gs, s3")


def _read_local(path: str, columns, filters):
    """Read Parquet from local filesystem."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Local file not found: {path}")
    return pq.read_table(path, columns=columns, filters=filters)


def _read_gcs(bucket: str, key: str, columns, filters):
    """
    Read Parquet from Google Cloud Storage.

    Requires GOOGLE_APPLICATION_CREDENTIALS env var to be set,
    or running in an environment with default credentials (GKE, Cloud Run).
    """
    try:
        from google.cloud import storage as gcs
    except ImportError:
        raise ImportError(
            "google-cloud-storage is required for gs:// URIs. "
            "Install with: pip install google-cloud-storage"
        )

    client = gcs.Client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(key)

    # Download to a temp file rather than holding entire blob in memory.
    # This lets PyArrow use memory-mapped I/O and predicate pushdown.
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        blob.download_to_filename(tmp_path)

    try:
        return pq.read_table(tmp_path, columns=columns, filters=filters)
    finally:
        os.unlink(tmp_path)


def _read_s3(bucket: str, key: str, columns, filters):
    """
    Read Parquet from AWS S3.

    Requires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars,
    or running in an environment with an IAM role (ECS, EKS, Lambda).
    """
    try:
        import boto3
    except ImportError:
        raise ImportError(
            "boto3 is required for s3:// URIs. "
            "Install with: pip install boto3"
        )

    s3 = boto3.client("s3")

    # Download to a temp file — same pattern as GCS.
    # This lets PyArrow use memory-mapped I/O and predicate pushdown.
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        s3.download_file(bucket, key, tmp_path)

    try:
        return pq.read_table(tmp_path, columns=columns, filters=filters)
    finally:
        os.unlink(tmp_path)
