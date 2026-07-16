"""Shared Silver schema and validation helpers for video transforms."""

import pandas as pd

from utils import to_bool


def video_columns() -> list[str]:
    """Return the standard Silver video column order."""
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


def require_columns(df: pd.DataFrame, required: set[str], dataset_name: str) -> None:
    """Fail fast when a raw input file does not contain expected columns."""
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: {sorted(missing)}"
        )


def coerce_video_types(df: pd.DataFrame) -> pd.DataFrame:
    """Apply consistent Silver types to video records from any source."""
    if df.empty:
        return pd.DataFrame(columns=video_columns())

    df["trending_date"] = pd.to_datetime(df["trending_date"], errors="coerce").dt.date
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

    numeric_columns = (
        "category_id",
        "view_count",
        "like_count",
        "dislike_count",
        "favorite_count",
        "comment_count",
    )
    boolean_columns = (
        "comments_disabled",
        "ratings_disabled",
        "video_error_or_removed",
        "caption",
        "licensed_content",
    )

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in boolean_columns:
        df[col] = df[col].map(to_bool)

    return df[video_columns()]


def validate_video_silver(df: pd.DataFrame, dataset_name: str) -> None:
    """Validate the minimum quality needed before writing Silver video data."""
    if df.empty:
        raise ValueError(f"{dataset_name} produced no Silver video rows.")
    if df["video_id"].isna().all():
        raise ValueError(f"{dataset_name} has no usable video_id values.")
    if df["category_id"].isna().all():
        raise ValueError(f"{dataset_name} has no usable category_id values.")
