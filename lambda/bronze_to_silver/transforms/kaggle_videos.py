"""Transform Kaggle trending video CSV files from Bronze to Silver."""

from datetime import UTC, datetime
import io

import pandas as pd

from config import Settings
from s3_io import read_s3_object, write_parquet_to_s3
from transforms.common import (
    coerce_video_types,
    require_columns,
    validate_video_silver,
)
from utils import extract_region, parse_kaggle_trending_date, to_bool


def transform_kaggle_videos_csv(
    bronze_bucket: str,
    bronze_key: str,
    settings: Settings,
) -> None:
    """Convert Kaggle video CSV from Bronze into standardized Silver Parquet."""
    region = extract_region(bronze_key)
    raw_df = pd.read_csv(io.BytesIO(read_s3_object(bronze_bucket, bronze_key)))
    require_columns(
        raw_df,
        {
            "video_id",
            "trending_date",
            "publish_time",
            "category_id",
            "views",
            "likes",
            "dislikes",
            "comment_count",
        },
        "Kaggle videos CSV",
    )

    df = pd.DataFrame(index=raw_df.index)
    df["source"] = "kaggle"
    df["region"] = region
    df["video_id"] = raw_df["video_id"]
    df["trending_date"] = raw_df["trending_date"].apply(parse_kaggle_trending_date)
    df["published_at"] = pd.to_datetime(raw_df["publish_time"], errors="coerce")
    df["channel_id"] = raw_df.get("channel_id")
    df["channel_title"] = raw_df.get("channel_title")
    df["title"] = raw_df.get("title")
    df["description"] = raw_df.get("description")
    df["category_id"] = raw_df["category_id"]
    df["tags"] = raw_df.get("tags", pd.Series(dtype="object")).apply(split_tags)
    df["view_count"] = raw_df["views"]
    df["like_count"] = raw_df["likes"]
    df["dislike_count"] = raw_df["dislikes"]
    df["favorite_count"] = pd.Series([None] * len(raw_df), dtype="Int64")
    df["comment_count"] = raw_df["comment_count"]
    df["thumbnail_url"] = raw_df.get("thumbnail_link")
    df["comments_disabled"] = raw_df.get("comments_disabled").map(to_bool)
    df["ratings_disabled"] = raw_df.get("ratings_disabled").map(to_bool)
    df["video_error_or_removed"] = raw_df.get("video_error_or_removed").map(to_bool)
    df["duration"] = None
    df["definition"] = None
    df["caption"] = None
    df["licensed_content"] = None

    df = coerce_video_types(df)
    validate_video_silver(df, "Kaggle videos CSV")

    filename = bronze_key.rsplit("/", 1)[-1].replace(".csv", ".parquet")
    silver_key = f"{settings.videos_output_prefix}source=kaggle/region={region}/{filename}"
    write_parquet_to_s3(
        df,
        settings.silver_bucket,
        silver_key,
        metadata={
            "pipeline-layer": "silver",
            "source": "kaggle",
            "dataset": "videos",
            "region": region,
            "record-count": str(len(df)),
            "ingestion-timestamp": datetime.now(UTC).isoformat(),
            "bronze-bucket": bronze_bucket,
            "bronze-key": bronze_key,
        },
    )


def split_tags(value) -> list[str]:
    """Convert Kaggle pipe-delimited tags into a list."""
    return value.split("|") if isinstance(value, str) and value else []
