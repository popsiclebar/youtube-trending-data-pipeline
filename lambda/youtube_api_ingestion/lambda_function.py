"""
YouTube API ingestion Lambda: fetch raw YouTube data into the Bronze S3 layer.

The caller can select video ingestion, category ingestion, or both. The Lambda
does not clean or reshape the data; it stores raw API responses for later Silver
jobs.
"""

from datetime import UTC, datetime
import json
import logging
from typing import Any

import boto3

from config import load_settings, parse_bool, parse_region_codes
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
    fetch_videos = parse_bool(event.get("fetch_videos", True))
    fetch_categories = parse_bool(
        event.get("fetch_categories", SETTINGS.fetch_categories)
    )
    if not fetch_videos and not fetch_categories:
        raise ValueError("At least one of fetch_videos or fetch_categories must be true.")

    run_timestamp = get_run_timestamp(event)
    run_id = str(event.get("batch_id") or build_run_id(run_timestamp))
    process_date = run_timestamp.strftime("%Y-%m-%d")
    process_hour = run_timestamp.strftime("%H")
    video_objects = []
    category_objects = []
    failures = []

    for region_code in region_codes:
        if fetch_videos:
            try:
                logger.info("Fetching YouTube trending videos for region=%s", region_code)
                videos_payload = fetch_trending_videos(region_code, SETTINGS)
                videos_key = build_videos_key(region_code, run_timestamp, SETTINGS)
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
                video_objects.append(videos_key)
            except Exception as exc:
                logger.exception(
                    "Failed to ingest trending videos for region=%s", region_code
                )
                failures.append(
                    build_failure_record(region_code, "trending_videos", exc)
                )

        if fetch_categories:
            try:
                logger.info("Fetching YouTube categories for region=%s", region_code)
                categories_payload = fetch_video_categories(region_code, SETTINGS)
                categories_key = build_categories_key(
                    region_code,
                    run_timestamp,
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
                category_objects.append(categories_key)
            except Exception as exc:
                logger.exception("Failed to ingest categories for region=%s", region_code)
                failures.append(
                    build_failure_record(region_code, "video_categories", exc)
                )

    written_objects = video_objects + category_objects
    if failures:
        send_failure_notification(failures, written_objects, run_timestamp, run_id)

    return {
        "statusCode": 207 if failures else 200,
        "message": "YouTube API ingestion completed with failures."
        if failures
        else "YouTube API ingestion completed.",
        "bucket": SETTINGS.bronze_bucket,
        "run_id": run_id,
        "process_date": process_date,
        "process_hour": process_hour,
        "regions": list(region_codes),
        "fetch_videos": fetch_videos,
        "fetch_categories": fetch_categories,
        "bronze_video_prefix": SETTINGS.videos_prefix,
        "bronze_category_prefix": SETTINGS.categories_prefix,
        "video_objects": video_objects,
        "category_objects": category_objects,
        "objects_written": written_objects,
        "failures": failures,
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
    """Build a stable hourly batch id for scheduled ingestion retries."""
    return run_timestamp.strftime("%Y%m%dT%H0000Z")


def get_run_timestamp(event: dict[str, Any]) -> datetime:
    """Use the scheduled event time when supplied, otherwise use current UTC."""
    timestamp_value = event.get("scheduled_time") or event.get("time")
    if not timestamp_value:
        return datetime.now(UTC)

    timestamp = datetime.fromisoformat(str(timestamp_value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


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
