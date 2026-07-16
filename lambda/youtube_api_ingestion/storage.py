"""S3 key, metadata, and write helpers for YouTube API Bronze ingestion."""

from datetime import datetime
import json
import logging
from typing import Any

import boto3

from config import Settings

logger = logging.getLogger()

s3 = boto3.client("s3")


def build_s3_metadata(
    payload: dict[str, Any],
    dataset: str,
    region_code: str,
    run_timestamp: datetime,
    run_id: str,
) -> dict[str, str]:
    """Build S3 object metadata without changing the raw JSON body."""
    return {
        "dataset": dataset,
        "region": region_code.upper(),
        "ingestion_timestamp": run_timestamp.isoformat(),
        "run_id": run_id,
        "source": "youtube_data_api_v3",
        "item_count": str(len(payload.get("items", []))),
    }


def build_videos_key(
    region_code: str,
    run_timestamp: datetime,
    run_id: str,
    settings: Settings,
) -> str:
    """Build a partitioned Bronze S3 key for raw trending video API responses."""
    date_partition = run_timestamp.strftime("%Y-%m-%d")
    hour_partition = run_timestamp.strftime("%H")
    region = region_code.lower()
    return (
        f"{settings.videos_prefix}/"
        f"region={region}/"
        f"date={date_partition}/"
        f"hour={hour_partition}/"
        f"{run_id}.json"
    )


def build_categories_key(
    region_code: str,
    run_timestamp: datetime,
    run_id: str,
    settings: Settings,
) -> str:
    """Build a partitioned Bronze S3 key for raw category reference data."""
    date_partition = run_timestamp.strftime("%Y-%m-%d")
    region = region_code.lower()
    return (
        f"{settings.categories_prefix}/"
        f"region={region}/"
        f"date={date_partition}/"
        f"category_id_{run_id}.json"
    )


def write_json_to_s3(
    payload: dict[str, Any],
    bucket: str,
    key: str,
    metadata: dict[str, str],
) -> None:
    """Write a JSON payload to S3 using a consistent UTF-8 encoding."""
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata=metadata,
    )
    logger.info("Wrote %s items to s3://%s/%s", metadata["item_count"], bucket, key)
