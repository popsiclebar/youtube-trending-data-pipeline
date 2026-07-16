"""Transform YouTube API trending video JSON files from Bronze to Silver."""

from datetime import UTC, datetime
import json

import pandas as pd

from config import Settings
from s3_io import read_s3_object, write_parquet_to_s3
from transforms.common import coerce_video_types, validate_video_silver, video_columns
from utils import extract_date_partition, extract_region, nullable_int, to_bool


def transform_api_videos_json(
    bronze_bucket: str,
    bronze_key: str,
    settings: Settings,
) -> None:
    """Convert YouTube API videos JSON from Bronze into Silver Parquet."""
    region = extract_region(bronze_key)
    trending_date = extract_date_partition(bronze_key)
    payload = json.loads(read_s3_object(bronze_bucket, bronze_key))

    items = payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("YouTube API videos JSON must contain an items list.")

    rows = []
    for item in items:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})
        default_thumbnail = snippet.get("thumbnails", {}).get("default", {})

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

    df = coerce_video_types(pd.DataFrame(rows, columns=video_columns()))
    validate_video_silver(df, "YouTube API videos JSON")

    filename = bronze_key.rsplit("/", 1)[-1].replace(".json", ".parquet")
    silver_key = (
        f"{settings.videos_output_prefix}source=youtube_api/region={region}/{filename}"
    )
    write_parquet_to_s3(
        df,
        settings.silver_bucket,
        silver_key,
        metadata={
            "pipeline-layer": "silver",
            "source": "youtube_api",
            "dataset": "videos",
            "region": region,
            "record-count": str(len(df)),
            "ingestion-timestamp": datetime.now(UTC).isoformat(),
            "bronze-bucket": bronze_bucket,
            "bronze-key": bronze_key,
        },
    )
