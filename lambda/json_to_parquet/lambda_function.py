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
        videos_output_prefix=f"{env_value('VIDEOS_OUTPUT_PREFIX', 'youtube/videos')}/",
        categories_output_prefix=f"{env_value('CATEGORIES_OUTPUT_PREFIX', 'youtube/categories')}/",
    )


SETTINGS = load_settings()

# Matches hive-style partition paths like .../region=ca/...
REGION_PATTERN = re.compile(r"region=([a-z]{2})")


def lambda_handler(event, context):
    """Entry point for each S3 upload notification."""
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        logger.info("Processing s3://%s/%s", bucket, key)

        if key.startswith(SETTINGS.reference_prefix) and key.endswith(".json"):
            transform_category_json(bucket, key)
        elif key.startswith(SETTINGS.raw_prefix) and key.endswith(".csv"):
            transform_videos_csv(bucket, key)
        else:
            logger.warning("Skipping unsupported file: %s", key)

    return {"statusCode": 200, "body": "OK"}


def extract_region(s3_key: str) -> str:
    """Pull region code from a path"""
    match = REGION_PATTERN.search(s3_key)
    if not match:
        raise ValueError(f"Could not find region partition in key: {s3_key}")
    return match.group(1)


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


def transform_videos_csv(bronze_bucket: str, bronze_key: str) -> None:
    """
    Bronze: CSV with messy types (strings, mixed date formats)
    Silver: Parquet with proper types and a region column
    """
    region = extract_region(bronze_key)
    raw_bytes = read_s3_object(bronze_bucket, bronze_key)

    df = pd.read_csv(io.BytesIO(raw_bytes))

    # Type coercion is the core Silver step: make data analytics-ready.
    df["region"] = region
    df["trending_date"] = df["trending_date"].apply(parse_trending_date)
    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")

    for col in ("views", "likes", "dislikes", "comment_count", "category_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ("comments_disabled", "ratings_disabled", "video_error_or_removed"):
        df[col] = df[col].map({"True": True, "False": False, True: True, False: False})

    # tags are pipe-delimited in the source; keep as a list column in silver
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(
            lambda x: x.split("|") if isinstance(x, str) and x else []
        )

    filename = bronze_key.rsplit("/", 1)[-1].replace(".csv", ".parquet")
    silver_key = f"{SETTINGS.videos_output_prefix}region={region}/{filename}"
    write_parquet_to_s3(df, SETTINGS.silver_bucket, silver_key)


def transform_category_json(bronze_bucket: str, bronze_key: str) -> None:
    """
    Bronze: nested YouTube API JSON (items[].snippet.title, etc.)
    Silver: flat Parquet lookup table (category_id → title)
    """
    region = extract_region(bronze_key)
    raw_bytes = read_s3_object(bronze_bucket, bronze_key)
    payload = json.loads(raw_bytes)

    rows = []
    for item in payload.get("items", []):
        snippet = item.get("snippet", {})
        rows.append(
            {
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
    silver_key = f"{SETTINGS.categories_output_prefix}region={region}/{filename}"
    write_parquet_to_s3(df, SETTINGS.silver_bucket, silver_key)
