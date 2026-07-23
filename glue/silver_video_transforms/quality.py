"""
Data cleansing, quality checks, and deduplication helpers.

Silver is the contract layer for downstream analytics, so this file makes the
quality work explicit while respecting the source differences:
- Kaggle has `trending_date` and `dislikes` in the CSV.
- YouTube API has `publishedAt`, but no `dislikeCount`.
- YouTube API region and observed trending date come from the S3 key.
"""

import json
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger()

REQUIRED_VIDEO_FIELDS = ["video_id", "category_id"]
REQUIRED_OBSERVATION_FIELDS = ["source", "region", "trending_date", "published_at"]
CRITICAL_COLUMNS = REQUIRED_VIDEO_FIELDS + REQUIRED_OBSERVATION_FIELDS
NUMERIC_COLUMNS = [
    "view_count",
    "like_count",
    "dislike_count",
    "favorite_count",
    "comment_count",
]
TEXT_COLUMNS = [
    "video_id",
    "channel_id",
    "channel_title",
    "title",
    "description",
    "thumbnail_url",
    "duration",
    "definition",
]


def apply_quality_checks(
    df: DataFrame, dataset_name: str, args: dict[str, str]
) -> tuple[DataFrame, dict]:
    """
    Run the Silver quality workflow.

    Workflow:
    1. Check the transform produced rows.
    2. Clean common formatting issues.
    3. Measure data quality problems.
    4. Fail if quality is below the configured threshold.
    5. Remove rows missing required video or observation fields.
    6. Deduplicate and return clean data plus metrics.
    """
    # Step 1: Validate that the transform produced records.
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"{dataset_name} produced no rows.")

    # Step 2: Standardize values before checking quality.
    cleaned_df = cleanse_video_data(df)

    # Step 3: Collect data quality metrics for logging and alert context.
    dq_metrics = collect_quality_metrics(cleaned_df, dataset_name, row_count)

    # Step 4: Stop the job when quality rules are violated.
    fail_on_quality_thresholds(dq_metrics, args)

    # Step 5: Remove records missing required video or observation fields.
    valid_df = cleaned_df.filter(~critical_field_is_invalid())
    valid_count = valid_df.count()
    if valid_count == 0:
        raise ValueError(f"{dataset_name} has no valid rows after quality checks.")

    # Step 6: Keep one record per source, region, video, and observed date.
    deduped_df = deduplicate_videos(valid_df)
    deduped_count = deduped_df.count()
    duplicate_count = valid_count - deduped_count

    metrics = {
        "dataset": dataset_name,
        "input_rows": row_count,
        "invalid_rows_removed": dq_metrics["invalid_critical_rows"],
        "duplicate_rows_removed": duplicate_count,
        "output_rows": deduped_count,
        "quality_checks": dq_metrics,
    }
    logger.info("Data quality metrics: %s", json.dumps(metrics))
    return deduped_df, metrics


def cleanse_video_data(df: DataFrame) -> DataFrame:
    """Normalize common fields before validation and writing."""
    clean_df = df.withColumn("source", F.lower(F.trim(F.col("source")))).withColumn(
        "region", F.upper(F.trim(F.col("region")))
    )

    for column_name in TEXT_COLUMNS:
        if column_name in clean_df.columns:
            clean_df = clean_df.withColumn(column_name, F.trim(F.col(column_name)))

    # Required fields cannot be empty strings in Silver.
    for column_name in CRITICAL_COLUMNS + ["title", "channel_title"]:
        if column_name in clean_df.columns:
            clean_df = clean_df.withColumn(
                column_name,
                F.when(F.col(column_name).cast("string") == "", F.lit(None)).otherwise(
                    F.col(column_name)
                ),
            )

    return clean_df.withColumn(
        "silver_quality_checked_at", F.current_timestamp()
    )


def collect_quality_metrics(
    df: DataFrame, dataset_name: str, input_rows: int
) -> dict[str, object]:
    """Collect row counts that describe Silver data quality."""
    invalid_critical_rows = df.filter(critical_field_is_invalid()).count()
    invalid_numeric_rows = df.filter(numeric_field_is_negative()).count()
    missing_context = {
        "video_id": df.filter(F.col("video_id").isNull()).count(),
        "category_id": df.filter(F.col("category_id").isNull()).count(),
        "region": df.filter(F.col("region").isNull()).count(),
        "trending_date": df.filter(F.col("trending_date").isNull()).count(),
        "published_at": df.filter(F.col("published_at").isNull()).count(),
        "title": df.filter(F.col("title").isNull()).count(),
        "channel_title": df.filter(F.col("channel_title").isNull()).count(),
    }

    metrics = {
        "dataset": dataset_name,
        "input_rows": input_rows,
        "invalid_critical_rows": invalid_critical_rows,
        "invalid_critical_row_ratio": invalid_critical_rows / input_rows,
        "invalid_numeric_rows": invalid_numeric_rows,
        "missing_context": missing_context,
    }
    logger.info("Collected quality metrics: %s", json.dumps(metrics))
    return metrics


def fail_on_quality_thresholds(metrics: dict[str, object], args: dict[str, str]) -> None:
    """Fail the job when the Silver dataset is too poor to trust."""
    max_invalid_ratio = float(args["max_invalid_row_ratio"])
    invalid_ratio = float(metrics["invalid_critical_row_ratio"])
    if invalid_ratio > max_invalid_ratio:
        raise ValueError(
            "Invalid critical row ratio "
            f"{invalid_ratio:.2%} exceeds threshold {max_invalid_ratio:.2%}."
        )

    if int(metrics["invalid_numeric_rows"]) > 0:
        raise ValueError(
            f"{metrics['dataset']} contains {metrics['invalid_numeric_rows']} "
            "rows with negative numeric metrics."
        )


def deduplicate_videos(df: DataFrame) -> DataFrame:
    """Keep one record for each source, region, video, and observed date."""
    window = Window.partitionBy("source", "region", "video_id", "trending_date").orderBy(
        F.col("published_at").desc_nulls_last(),
        F.col("silver_ingestion_timestamp").desc_nulls_last(),
    )
    return (
        df.withColumn("_dedup_rank", F.row_number().over(window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


def critical_field_is_invalid():
    """Build the critical-field invalid condition."""
    condition = F.lit(False)
    for column_name in CRITICAL_COLUMNS:
        condition = condition | F.col(column_name).isNull()
    return condition


def numeric_field_is_negative():
    """Build the negative-metric invalid condition."""
    condition = F.lit(False)
    for column_name in NUMERIC_COLUMNS:
        condition = condition | (F.col(column_name).isNotNull() & (F.col(column_name) < 0))
    return condition


def require_columns(df: DataFrame, required: set[str], dataset_name: str) -> None:
    """Fail fast when a Bronze dataset does not contain expected columns."""
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{dataset_name} missing required columns: {sorted(missing)}")
