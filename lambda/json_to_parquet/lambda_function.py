"""
Bronze to Silver Lambda: read raw YouTube data from S3, clean it, write Parquet.

Triggered by S3 ObjectCreated events on the bronze bucket.
"""

from dataclasses import dataclass
import io
import json
import logging
import os
import re
from datetime import datetime
from urllib.parse import unquote_plus

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


@dataclass(frozen=True)
class Settings:
    """Runtime configuration injected by Lambda environment variables."""

    silver_bucket: str
    raw_prefix: str = "youtube/raw/"
    reference_prefix: str = "youtube/raw_reference_data/"
    api_videos_prefix: str = "youtube/api_raw/videos/"
    api_categories_prefix: str = "youtube/api_raw/categories/"
    videos_output_prefix: str = "youtube/videos/"
    categories_output_prefix: str = "youtube/categories/"


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def env_value(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip().strip("/")
    if not value:
        raise RuntimeError(f"Environment variable cannot be empty: {name}")
    return value


def load_settings() -> Settings:
    return Settings(
        silver_bucket=required_env("SILVER_BUCKET"),
        raw_prefix=f"{env_value('RAW_PREFIX', 'youtube/raw')}/",
        reference_prefix=f"{env_value('REFERENCE_PREFIX', 'youtube/raw_reference_data')}/",
        api_videos_prefix=f"{env_value('API_VIDEOS_PREFIX', 'youtube/api_raw/videos')}/",
        api_categories_prefix=f"{env_value('API_CATEGORIES_PREFIX', 'youtube/api_raw/categories')}/",
        videos_output_prefix=f"{env_value('VIDEOS_OUTPUT_PREFIX', 'youtube/videos')}/",
        categories_output_prefix=f"{env_value('CATEGORIES_OUTPUT_PREFIX', 'youtube/categories')}/",
    )


SETTINGS = load_settings()

# Matches hive-style partition paths like .../region=ca/...
REGION_PATTERN = re.compile(r"region=([a-z]{2})")
DATE_PATTERN = re.compile(r"(?:date|ingestion_date)=([0-9]{4}-[0-9]{2}-[0-9]{2})")


def lambda_handler(event, context):
    """Entry point for each S3 upload notification."""
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        logger.info("Processing s3://%s/%s", bucket, key)

        if key.startswith(SETTINGS.reference_prefix) and key.endswith(".json"):
            transform_category_json(bucket, key, source="kaggle")
        elif key.startswith(SETTINGS.raw_prefix) and key.endswith(".csv"):
            transform_kaggle_videos_csv(bucket, key)
        elif key.startswith(SETTINGS.api_videos_prefix) and key.endswith(".json"):
            transform_api_videos_json(bucket, key)
        elif key.startswith(SETTINGS.api_categories_prefix) and key.endswith(".json"):
            transform_category_json(bucket, key, source="youtube_api")
        else:
            logger.warning("Skipping unsupported file: %s", key)

    return {"statusCode": 200, "body": "OK"}


def extract_region(s3_key: str) -> str:
    """Pull region code from a path"""
    match = REGION_PATTERN.search(s3_key)
    if not match:
        raise ValueError(f"Could not find region partition in key: {s3_key}")
    return match.group(1)


def extract_date_partition(s3_key: str) -> str | None:
    """Pull a date partition from a path like .../date=2026-07-14/..."""
    match = DATE_PATTERN.search(s3_key)
    return match.group(1) if match else None


def read_s3_object(bucket: str, key: str) -> bytes:
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Serialize a DataFrame to Parquet in memory, then upload to S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )
    logger.info("Wrote s3://%s/%s (%d rows)", bucket, key, len(df))


def parse_trending_date(value: str) -> datetime | None:
    """
    Parse trending_date from the Kaggle dataset format: yy.dd.mm
    Example: '17.14.11' → 2017-11-14
    """
    if pd.isna(value) or not str(value).strip():
        return None
    try:
        yy, dd, mm = str(value).split(".")
        return datetime(2000 + int(yy), int(mm), int(dd))
    except (ValueError, TypeError):
        logger.warning("Could not parse trending_date: %s", value)
        return None


def nullable_int(value) -> int | None:
    """Convert API string counts like '123' to int while preserving missing values."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def transform_kaggle_videos_csv(bronze_bucket: str, bronze_key: str) -> None:
    """
    Bronze: Kaggle CSV with messy types
    Silver: normalized Parquet with proper types and source/region columns
    """
    region = extract_region(bronze_key)
    raw_bytes = read_s3_object(bronze_bucket, bronze_key)

    raw_df = pd.read_csv(io.BytesIO(raw_bytes))
    df = pd.DataFrame(index=raw_df.index)

    df["source"] = "kaggle"
    df["region"] = region
    df["video_id"] = raw_df.get("video_id")
    df["trending_date"] = raw_df["trending_date"].apply(parse_trending_date)
    df["published_at"] = pd.to_datetime(raw_df["publish_time"], errors="coerce")
    df["channel_id"] = raw_df.get("channel_id")
    df["channel_title"] = raw_df.get("channel_title")
    df["title"] = raw_df.get("title")
    df["description"] = raw_df.get("description")
    df["category_id"] = pd.to_numeric(raw_df["category_id"], errors="coerce").astype("Int64")
    df["tags"] = raw_df.get("tags", pd.Series(dtype="object")).apply(
        lambda x: x.split("|") if isinstance(x, str) and x else []
    )
    df["view_count"] = pd.to_numeric(raw_df["views"], errors="coerce").astype("Int64")
    df["like_count"] = pd.to_numeric(raw_df["likes"], errors="coerce").astype("Int64")
    df["dislike_count"] = pd.to_numeric(raw_df["dislikes"], errors="coerce").astype("Int64")
    df["favorite_count"] = pd.Series([None] * len(raw_df), dtype="Int64")
    df["comment_count"] = pd.to_numeric(raw_df["comment_count"], errors="coerce").astype("Int64")
    df["thumbnail_url"] = raw_df.get("thumbnail_link")
    df["comments_disabled"] = raw_df.get("comments_disabled").map(to_bool)
    df["ratings_disabled"] = raw_df.get("ratings_disabled").map(to_bool)
    df["video_error_or_removed"] = raw_df.get("video_error_or_removed").map(to_bool)
    df["duration"] = None
    df["definition"] = None
    df["caption"] = None
    df["licensed_content"] = None
    df = coerce_video_types(df)

    filename = bronze_key.rsplit("/", 1)[-1].replace(".csv", ".parquet")
    silver_key = f"{SETTINGS.videos_output_prefix}source=kaggle/region={region}/{filename}"
    write_parquet_to_s3(df, SETTINGS.silver_bucket, silver_key)


def transform_api_videos_json(bronze_bucket: str, bronze_key: str) -> None:
    """
    Bronze: raw YouTube Data API videos.list JSON
    Silver: normalized Parquet aligned with the Kaggle Silver videos schema
    """
    region = extract_region(bronze_key)
    trending_date = extract_date_partition(bronze_key)
    raw_bytes = read_s3_object(bronze_bucket, bronze_key)
    payload = json.loads(raw_bytes)

    rows = []
    for item in payload.get("items", []):
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})
        thumbnails = snippet.get("thumbnails", {})
        default_thumbnail = thumbnails.get("default", {})

        rows.append(
            {
                "source": "youtube_api",
                "region": region,
                "video_id": item.get("id"),
                "trending_date": trending_date,
                "published_at": snippet.get("publishedAt"),
                "channel_id": snippet.get("channelId"),
                "channel_title": snippet.get("channelTitle"),
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "category_id": nullable_int(snippet.get("categoryId")),
                "tags": snippet.get("tags", []),
                "view_count": nullable_int(statistics.get("viewCount")),
                "like_count": nullable_int(statistics.get("likeCount")),
                "dislike_count": None,
                "favorite_count": nullable_int(statistics.get("favoriteCount")),
                "comment_count": nullable_int(statistics.get("commentCount")),
                "thumbnail_url": default_thumbnail.get("url"),
                "comments_disabled": None,
                "ratings_disabled": None,
                "video_error_or_removed": None,
                "duration": content_details.get("duration"),
                "definition": content_details.get("definition"),
                "caption": to_bool(content_details.get("caption")),
                "licensed_content": content_details.get("licensedContent"),
            }
        )

    df = pd.DataFrame(rows, columns=video_columns())
    df = coerce_video_types(df)

    filename = bronze_key.rsplit("/", 1)[-1].replace(".json", ".parquet")
    silver_key = f"{SETTINGS.videos_output_prefix}source=youtube_api/region={region}/{filename}"
    write_parquet_to_s3(df, SETTINGS.silver_bucket, silver_key)


def to_bool(value):
    """Normalize common boolean representations from CSV/API data."""
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def video_columns() -> list[str]:
    """Keep the Silver videos column order consistent across sources."""
    return [
        "source",
        "region",
        "video_id",
        "trending_date",
        "published_at",
        "channel_id",
        "channel_title",
        "title",
        "description",
        "category_id",
        "tags",
        "view_count",
        "like_count",
        "dislike_count",
        "favorite_count",
        "comment_count",
        "thumbnail_url",
        "comments_disabled",
        "ratings_disabled",
        "video_error_or_removed",
        "duration",
        "definition",
        "caption",
        "licensed_content",
    ]


def coerce_video_types(df: pd.DataFrame) -> pd.DataFrame:
    """Apply consistent Silver types to video records from any source."""
    if df.empty:
        return pd.DataFrame(columns=video_columns())

    df["trending_date"] = pd.to_datetime(df["trending_date"], errors="coerce").dt.date
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    for col in (
        "category_id",
        "view_count",
        "like_count",
        "dislike_count",
        "favorite_count",
        "comment_count",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in (
        "comments_disabled",
        "ratings_disabled",
        "video_error_or_removed",
        "caption",
        "licensed_content",
    ):
        df[col] = df[col].map(to_bool)

    return df[video_columns()]


def transform_category_json(bronze_bucket: str, bronze_key: str, source: str) -> None:
    """
    Bronze: nested category JSON
    Silver: flat Parquet lookup table (category_id to title) partitioned by source
    """
    region = extract_region(bronze_key)
    raw_bytes = read_s3_object(bronze_bucket, bronze_key)
    payload = json.loads(raw_bytes)

    rows = []
    for item in payload.get("items", []):
        snippet = item.get("snippet", {})
        rows.append(
            {
                "source": source,
                "region": region,
                "category_id": int(item["id"]),
                "category_title": snippet.get("title"),
                "channel_id": snippet.get("channelId"),
                "assignable": snippet.get("assignable"),
            }
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "source",
            "region",
            "category_id",
            "category_title",
            "channel_id",
            "assignable",
        ],
    )
    if not df.empty:
        df["category_id"] = df["category_id"].astype("Int64")

    filename = bronze_key.rsplit("/", 1)[-1].replace(".json", ".parquet")
    silver_key = f"{SETTINGS.categories_output_prefix}source={source}/region={region}/{filename}"
    write_parquet_to_s3(df, SETTINGS.silver_bucket, silver_key)
