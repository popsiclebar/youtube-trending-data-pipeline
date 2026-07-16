"""
YouTube API ingestion Lambda: fetch raw YouTube data into the Bronze S3 layer.

This Lambda is intended to run once per day from Amazon EventBridge. It does not
clean or reshape the data; it stores raw API responses for later Silver jobs.
"""

from datetime import UTC, datetime
import json
import logging
from typing import Any
from uuid import uuid4

import boto3

from config import load_settings, parse_region_codes
from storage import (
    build_categories_key,
    build_s3_metadata,
    build_videos_key,
    write_json_to_s3,
)
from youtube_api import fetch_trending_videos, fetch_video_categories

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SETTINGS = load_settings()
sns = boto3.client("sns")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Fetch configured regions from YouTube API and write raw JSON to Bronze."""
    region_codes = get_region_codes_from_event(event) or SETTINGS.region_codes
    run_timestamp = datetime.now(UTC)
    run_id = build_run_id(run_timestamp)
    written_objects = []
    failures = []

    for region_code in region_codes:
        try:
            logger.info("Fetching YouTube trending videos for region=%s", region_code)
            videos_payload = fetch_trending_videos(region_code, SETTINGS)
            videos_key = build_videos_key(region_code, run_timestamp, run_id, SETTINGS)
            videos_metadata = build_s3_metadata(
                videos_payload,
                "trending_videos",
                region_code,
                run_timestamp,
                run_id,
            )
            write_json_to_s3(
                videos_payload,
                SETTINGS.bronze_bucket,
                videos_key,
                videos_metadata,
            )
            written_objects.append(videos_key)
        except Exception as exc:
            logger.exception(
                "Failed to ingest trending videos for region=%s", region_code
            )
            failures.append(build_failure_record(region_code, "trending_videos", exc))
            continue

        if not SETTINGS.fetch_categories:
            continue

        try:
            logger.info("Fetching YouTube categories for region=%s", region_code)
            categories_payload = fetch_video_categories(region_code, SETTINGS)
            categories_key = build_categories_key(
                region_code,
                run_timestamp,
                run_id,
                SETTINGS,
            )
            categories_metadata = build_s3_metadata(
                categories_payload,
                "video_categories",
                region_code,
                run_timestamp,
                run_id,
            )
            write_json_to_s3(
                categories_payload,
                SETTINGS.bronze_bucket,
                categories_key,
                categories_metadata,
            )
            written_objects.append(categories_key)
        except Exception as exc:
            logger.exception("Failed to ingest categories for region=%s", region_code)
            failures.append(build_failure_record(region_code, "video_categories", exc))

    if failures:
        send_failure_notification(failures, written_objects, run_timestamp, run_id)

    return {
        "statusCode": 207 if failures else 200,
        "body": json.dumps(
            {
                "message": "YouTube API ingestion completed with failures."
                if failures
                else "YouTube API ingestion completed.",
                "bucket": SETTINGS.bronze_bucket,
                "run_id": run_id,
                "objects_written": written_objects,
                "failures": failures,
            }
        ),
    }


def get_region_codes_from_event(event: dict[str, Any]) -> tuple[str, ...] | None:
    """Allow manual Lambda test events to override configured regions."""
    event_regions = event.get("region_codes")
    if not event_regions:
        return None

    if isinstance(event_regions, str):
        return parse_region_codes(event_regions)

    return tuple(
        str(region).strip().upper() for region in event_regions if str(region).strip()
    )


def build_run_id(run_timestamp: datetime) -> str:
    """Build a readable unique id for one ingestion run."""
    timestamp_part = run_timestamp.strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp_part}_{uuid4().hex[:8]}"


def build_failure_record(
    region_code: str,
    dataset: str,
    exc: Exception,
) -> dict[str, str]:
    """Create a JSON-serializable failure record for logs and SNS."""
    return {
        "region": region_code.upper(),
        "dataset": dataset,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
    }


def send_failure_notification(
    failures: list[dict[str, str]],
    written_objects: list[str],
    run_timestamp: datetime,
    run_id: str,
) -> None:
    """Publish an SNS alert when any region or dataset fails."""
    if not SETTINGS.sns_topic_arn:
        logger.warning("SNS_TOPIC_ARN is not configured; skipping failure notification.")
        return

    message = {
        "run_id": run_id,
        "ingestion_timestamp": run_timestamp.isoformat(),
        "bronze_bucket": SETTINGS.bronze_bucket,
        "failures": failures,
        "objects_written_before_or_during_failure": written_objects,
    }
    sns.publish(
        TopicArn=SETTINGS.sns_topic_arn,
        Subject="[YouTube Pipeline] API ingestion failure",
        Message=json.dumps(message, indent=2),
    )
    logger.info("Published failure notification to SNS topic.")
