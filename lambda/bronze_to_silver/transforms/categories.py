"""Transform category reference JSON files from Bronze to Silver."""

from datetime import UTC, datetime
import json

import pandas as pd

from config import Settings
from s3_io import read_s3_object, write_parquet_to_s3
from utils import extract_region


def transform_category_json(
    bronze_bucket: str,
    bronze_key: str,
    source: str,
    settings: Settings,
) -> None:
    """Convert category JSON from Bronze into a flat Silver lookup table."""
    region = extract_region(bronze_key)
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
                "category_id": int(item["id"]),
                "category_title": snippet.get("title"),
                "channel_id": snippet.get("channelId"),
                "assignable": snippet.get("assignable"),
            }
        )

    columns = [
        "source",
        "region",
        "category_id",
        "category_title",
        "channel_id",
        "assignable",
    ]
    df = pd.DataFrame(rows, columns=columns)
    validate_category_silver(df, source)
    df = df.drop_duplicates(subset=["source", "region", "category_id"], keep="last")
    df["category_id"] = df["category_id"].astype("Int64")

    filename = bronze_key.rsplit("/", 1)[-1].replace(".json", ".parquet")
    silver_key = (
        f"{settings.categories_output_prefix}source={source}/region={region}/{filename}"
    )
    write_parquet_to_s3(
        df,
        settings.silver_bucket,
        silver_key,
        metadata={
            "pipeline-layer": "silver",
            "source": source,
            "dataset": "categories",
            "region": region,
            "record-count": str(len(df)),
            "ingestion-timestamp": datetime.now(UTC).isoformat(),
            "bronze-bucket": bronze_bucket,
            "bronze-key": bronze_key,
        },
    )


def validate_category_silver(df: pd.DataFrame, source: str) -> None:
    """Validate the minimum quality needed before writing Silver categories."""
    if df.empty:
        raise ValueError(f"{source} category JSON produced no Silver rows.")
    if df["category_id"].isna().all():
        raise ValueError(f"{source} category JSON has no usable category_id values.")
