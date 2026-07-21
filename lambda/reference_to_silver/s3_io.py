"""S3 read/write helpers for the reference-to-Silver Lambda."""

import io
import logging

import boto3
import pandas as pd

logger = logging.getLogger()

s3 = boto3.client("s3")


def read_s3_object(bucket: str, key: str) -> bytes:
    """Read one S3 object body as bytes."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def write_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    metadata: dict[str, str] | None = None,
) -> None:
    """Serialize a DataFrame to Parquet in memory, then upload it to S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    put_args = {
        "Bucket": bucket,
        "Key": key,
        "Body": buffer.getvalue(),
        "ContentType": "application/vnd.apache.parquet",
    }
    if metadata:
        put_args["Metadata"] = {
            metadata_key: str(value) for metadata_key, value in metadata.items()
        }

    s3.put_object(**put_args)
    logger.info("Wrote s3://%s/%s (%d rows)", bucket, key, len(df))
