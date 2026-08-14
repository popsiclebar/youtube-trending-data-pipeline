"""Transform category reference JSON files from Bronze to Silver."""

from datetime import UTC, datetime
import json
import re

import pandas as pd

from config import Settings
from s3_io import read_s3_object, register_category_partition, write_parquet_to_s3

REGION_PATTERN = re.compile(r"region=([a-z]{2})")
DATE_PATTERN = re.compile(r"date=([0-9]{4}-[0-9]{2}-[0-9]{2})")


def transform_category_json(
    bronze_bucket: str,
    bronze_key: str,
    source: str,
    settings: Settings,
) -> str:
    """Convert one category JSON file into a flat Silver Parquet lookup table."""
    region = extract_region(bronze_key)
    date = extract_date(bronze_key, source)
    payload = json.loads(read_s3_object(bronze_bucket, bronze_key))
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Category JSON must contain an items list.")

    rows = []
    for item in items:
        snippet = item.get("snippet", {})
        rows.append(
            {
                "source": source,
                "region": region,
                "date": date,
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
            "date",
            "category_id",
            "category_title",
            "channel_id",
            "assignable",
        ],
    )
    validate_category_silver(df, source)
    df = df.drop_duplicates(
        subset=["source", "region", "date", "category_id"], keep="last"
    )
    df["category_id"] = df["category_id"].astype("Int64")
    df["date"] = df["date"].astype("string")

    filename = (
        f"categories_{date}.parquet"
        if date is not None
        else "categories.parquet"
    )
    silver_key = (
        f"{settings.categories_output_prefix}source={source}/region={region}/"
        f"{filename}"
    )
    parquet_df = df.drop(columns=["source", "region"])
    metadata = {
        "pipeline-layer": "silver",
        "source": source,
        "dataset": "categories",
        "region": region,
        "record-count": str(len(parquet_df)),
        "ingestion-timestamp": datetime.now(UTC).isoformat(),
        "bronze-bucket": bronze_bucket,
        "bronze-key": bronze_key,
    }
    if date is not None:
        metadata["date"] = date
    write_parquet_to_s3(
        parquet_df,
        settings.silver_bucket,
        silver_key,
        metadata=metadata,
    )
    register_category_partition(
        settings.silver_database,
        settings.categories_table,
        settings.silver_bucket,
        settings.categories_output_prefix,
        source,
        region,
    )
    return silver_key


def extract_region(s3_key: str) -> str:
    """Pull a region code from a Hive-style S3 path such as region=us."""
    match = REGION_PATTERN.search(s3_key)
    if not match:
        raise ValueError(f"Could not find region partition in key: {s3_key}")
    return match.group(1).lower()


def extract_date(s3_key: str, source: str) -> str | None:
    """Return the API date; Kaggle category lookups are undated."""
    if source == "kaggle":
        return None

    match = DATE_PATTERN.search(s3_key)
    if not match:
        raise ValueError(f"Could not find date partition in API category key: {s3_key}")
    return match.group(1)


def validate_category_silver(df: pd.DataFrame, source: str) -> None:
    """Validate the minimum quality needed before writing Silver categories."""
    if df.empty:
        raise ValueError(f"{source} category JSON produced no Silver rows.")
    if bool(df["category_id"].isna().all()):
        raise ValueError(f"{source} category JSON has no usable category_id values.")
