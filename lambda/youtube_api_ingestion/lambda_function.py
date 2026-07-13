"""
YouTube API ingestion Lambda: fetch trending YouTube data and land it in Bronze S3.

This Lambda is intended to be triggered once per day by Amazon EventBridge.
It does not clean or reshape the data. Its job is to capture the raw API
response and store it in S3 so later Bronze-to-Silver jobs can process it.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")
secretsmanager = boto3.client("secretsmanager")

DEFAULT_REGION_CODES = "CA,DE,FR,GB,IN,JP,KR,MX,RU,US"


@dataclass(frozen=True)
class Settings:
    """Runtime configuration loaded from Lambda environment variables."""

    bronze_bucket: str
    youtube_api_key: str
    region_codes: tuple[str, ...]
    max_results: int
    sns_topic_arn: str | None = None
    api_base_url: str = "https://www.googleapis.com/youtube/v3"
    videos_prefix: str = "youtube/api_raw/videos"
    categories_prefix: str = "youtube/api_raw/categories"
    fetch_categories: bool = True


def required_env(name: str) -> str:
    """Read a required environment variable and fail loudly if it is missing."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def env_value(name: str, default: str) -> str:
    """Read an optional environment variable with a default value."""
    return os.environ.get(name, default).strip()


def optional_env(name: str) -> str | None:
    """Read an optional environment variable and return None when it is blank."""
    value = os.environ.get(name, "").strip()
    return value or None


def parse_bool(value: str) -> bool:
    """Convert friendly string values like true/false into a Python bool."""
    return value.strip().lower() in {"1", "true", "yes", "y"}


def parse_region_codes(value: str) -> tuple[str, ...]:
    """
    Convert a comma-separated string into normalized YouTube region codes.

    Example:
        "US,CA,GB" -> ("US", "CA", "GB")
    """
    regions = tuple(region.strip().upper() for region in value.split(",") if region.strip())
    if not regions:
        raise RuntimeError("REGION_CODES must contain at least one region code.")
    return regions


def parse_max_results(value: str) -> int:
    """
    YouTube's videos.list endpoint allows 1 to 50 results per request.

    Keeping this validation in code prevents accidental bad configuration from
    creating confusing API errors at runtime.
    """
    max_results = int(value)
    if max_results < 1 or max_results > 50:
        raise RuntimeError("MAX_RESULTS must be between 1 and 50.")
    return max_results


def normalized_prefix(name: str, default: str) -> str:
    """Normalize S3 prefixes so later key building has exactly one slash."""
    return env_value(name, default).strip("/")


def load_youtube_api_key() -> str:
    """
    Load the YouTube API key from Lambda config or AWS Secrets Manager.

    - YOUTUBE_API_KEY stores the key directly as a Lambda environment variable.
    - YOUTUBE_API_KEY_SECRET_ID stores the key in Secrets Manager and gives
      Lambda only the secret id.
    """
    direct_key = optional_env("YOUTUBE_API_KEY")
    if direct_key:
        return direct_key

    secret_id = optional_env("YOUTUBE_API_KEY_SECRET_ID")
    if not secret_id:
        raise RuntimeError(
            "Missing required environment variable: YOUTUBE_API_KEY "
            "or YOUTUBE_API_KEY_SECRET_ID"
        )

    response = secretsmanager.get_secret_value(SecretId=secret_id)
    secret_key = response.get("SecretString", "").strip()
    if not secret_key:
        raise RuntimeError(f"Secret {secret_id} does not contain a SecretString value.")
    return secret_key


def load_settings() -> Settings:
    """Create a Settings object once when the Lambda runtime starts."""
    return Settings(
        bronze_bucket=required_env("BRONZE_BUCKET"),
        youtube_api_key=load_youtube_api_key(),
        region_codes=parse_region_codes(env_value("REGION_CODES", DEFAULT_REGION_CODES)),
        max_results=parse_max_results(env_value("MAX_RESULTS", "50")),
        sns_topic_arn=optional_env("SNS_TOPIC_ARN"),
        api_base_url=env_value("YOUTUBE_API_BASE_URL", "https://www.googleapis.com/youtube/v3").rstrip("/"),
        videos_prefix=normalized_prefix("VIDEOS_BRONZE_PREFIX", "youtube/api_raw/videos"),
        categories_prefix=normalized_prefix("CATEGORIES_BRONZE_PREFIX", "youtube/api_raw/categories"),
        fetch_categories=parse_bool(env_value("FETCH_CATEGORIES", "true")),
    )


SETTINGS = load_settings()


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Fetch trending videos, and optionally category reference data, for each region.

    EventBridge can trigger this Lambda every 24 hours. Scheduled events usually
    do not need to pass a custom payload.
    For manual tests, you can override regions with:

        {"region_codes": ["US", "CA"]}
    """
    region_codes = get_region_codes_from_event(event) or SETTINGS.region_codes
    run_timestamp = datetime.now(UTC)
    run_id = build_run_id(run_timestamp)
    written_objects = []
    failures = []

    for region_code in region_codes:
        try:
            logger.info("Fetching YouTube trending videos for region=%s", region_code)
            videos_payload = fetch_trending_videos(region_code)
            videos_key = build_videos_key(region_code, run_timestamp, run_id)
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
            logger.exception("Failed to ingest trending videos for region=%s", region_code)
            failures.append(
                build_failure_record(region_code, "trending_videos", exc)
            )
            continue

        if not SETTINGS.fetch_categories:
            continue

        try:
            logger.info("Fetching YouTube categories for region=%s", region_code)
            categories_payload = fetch_video_categories(region_code)
            categories_key = build_categories_key(region_code, run_timestamp, run_id)
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
            failures.append(
                build_failure_record(region_code, "video_categories", exc)
            )

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

    return tuple(str(region).strip().upper() for region in event_regions if str(region).strip())


def fetch_trending_videos(region_code: str) -> dict[str, Any]:
    """
    Call YouTube videos.list for the most popular videos in one region.

    It requests snippet, statistics, and contentDetails.
    """
    return call_youtube_api(
        "videos",
        {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": region_code,
            "maxResults": str(SETTINGS.max_results),
        },
    )


def fetch_video_categories(region_code: str) -> dict[str, Any]:
    """
    Call YouTube videoCategories.list for category lookup data.
    """
    return call_youtube_api(
        "videoCategories",
        {
            "part": "snippet",
            "regionCode": region_code,
        },
    )


def call_youtube_api(endpoint: str, params: dict[str, str]) -> dict[str, Any]:
    """
    Make an HTTPS GET request to the YouTube Data API.

    The API key is added here, close to the outbound request, and is never logged.
    """
    query_params = {**params, "key": SETTINGS.youtube_api_key}
    url = f"{SETTINGS.api_base_url}/{endpoint}?{urlencode(query_params)}"
    request = Request(url, headers={"Accept": "application/json"})

    try:
        with urlopen(request, timeout=30) as response:
            response_body = response.read().decode("utf-8")
            return json.loads(response_body)
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        logger.error("YouTube API returned HTTP %s for endpoint=%s", exc.code, endpoint)
        raise RuntimeError(f"YouTube API request failed: {error_body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not connect to YouTube API: {exc.reason}") from exc


def build_s3_metadata(
    payload: dict[str, Any],
    dataset: str,
    region_code: str,
    run_timestamp: datetime,
    run_id: str,
) -> dict[str, str]:
    """
    Build S3 object metadata without changing the raw JSON body.

    Metadata is stored on the S3 object itself. S3 metadata values must be
    strings, so item_count is converted to a string.
    """
    return {
        "dataset": dataset,
        "region": region_code.upper(),
        "ingestion_timestamp": run_timestamp.isoformat(),
        "run_id": run_id,
        "source": "youtube_data_api_v3",
        "item_count": str(len(payload.get("items", []))),
    }


def build_run_id(run_timestamp: datetime) -> str:
    """
    Build a readable and unique id for one Lambda ingestion run.

    The timestamp makes S3 objects easy to inspect by eye. The short UUID suffix
    prevents accidental overwrites if two runs start in the same second.
    """
    timestamp_part = run_timestamp.strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp_part}_{uuid4().hex[:8]}"


def build_videos_key(region_code: str, run_timestamp: datetime, run_id: str) -> str:
    """Build a partitioned Bronze S3 key for raw trending video API responses."""
    date_partition = run_timestamp.strftime("%Y-%m-%d")
    hour_partition = run_timestamp.strftime("%H")
    region = region_code.lower()
    return (
        f"{SETTINGS.videos_prefix}/"
        f"region={region}/"
        f"date={date_partition}/"
        f"hour={hour_partition}/"
        f"{run_id}.json"
    )


def build_categories_key(region_code: str, run_timestamp: datetime, run_id: str) -> str:
    """Build a partitioned Bronze S3 key for raw category reference data."""
    date_partition = run_timestamp.strftime("%Y-%m-%d")
    region = region_code.lower()
    return (
        f"{SETTINGS.categories_prefix}/"
        f"region={region}/"
        f"date={date_partition}/"
        f"category_id_{run_id}.json"
    )


def build_failure_record(region_code: str, dataset: str, exc: Exception) -> dict[str, str]:
    """Create a simple JSON-serializable failure record for logs and SNS."""
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
    """
    Publish an SNS alert when any region or dataset fails.

    SNS_TOPIC_ARN is optional so local development and early AWS setup can still
    run without notifications. In production, set SNS_TOPIC_ARN to an SNS topic.
    """
    if not SETTINGS.sns_topic_arn:
        logger.warning("SNS_TOPIC_ARN is not configured; skipping failure notification.")
        return

    subject = "[YouTube Pipeline] API ingestion failure"
    message = {
        "run_id": run_id,
        "ingestion_timestamp": run_timestamp.isoformat(),
        "bronze_bucket": SETTINGS.bronze_bucket,
        "failures": failures,
        "objects_written_before_or_during_failure": written_objects,
    }
    sns.publish(
        TopicArn=SETTINGS.sns_topic_arn,
        Subject=subject,
        Message=json.dumps(message, indent=2),
    )
    logger.info("Published failure notification to SNS topic.")


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
