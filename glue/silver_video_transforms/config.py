"""
Configuration helpers for the Silver video AWS Glue job.

Glue passes job settings as command-line arguments such as
`--BRONZE_BUCKET my-bucket`. This file keeps argument parsing separate from the
ETL business logic.
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F


def load_args() -> dict[str, str]:
    """Load required and optional Glue job arguments."""
    required = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET"])
    args = {
        "job_name": required["JOB_NAME"],
        "bronze_bucket": required["BRONZE_BUCKET"],
        "silver_bucket": required["SILVER_BUCKET"],
        "bronze_database": optional_arg("BRONZE_DATABASE", "youtube_bronze"),
        "silver_database": optional_arg("SILVER_DATABASE", "youtube_silver"),
        "source": optional_arg("SOURCE", "all").lower(),
        "kaggle_raw_prefix": optional_arg("KAGGLE_RAW_PREFIX", "youtube/raw"),
        "api_videos_prefix": optional_arg("API_VIDEOS_PREFIX", "youtube/api_raw/videos"),
        "silver_videos_prefix": optional_arg("SILVER_VIDEOS_PREFIX", "youtube/videos"),
        "sns_topic_arn": optional_arg("SNS_TOPIC_ARN", ""),
        "max_invalid_row_ratio": optional_arg("MAX_INVALID_ROW_RATIO", "0.05"),
    }

    if args["source"] not in {"kaggle", "youtube_api", "all"}:
        raise ValueError("SOURCE must be one of: kaggle, youtube_api, all")
    if not 0 <= float(args["max_invalid_row_ratio"]) <= 1:
        raise ValueError("MAX_INVALID_ROW_RATIO must be between 0 and 1")

    return args


def optional_arg(name: str, default: str) -> str:
    """Read an optional Glue argument from sys.argv."""
    token = f"--{name}"
    if token not in sys.argv:
        return default
    return sys.argv[sys.argv.index(token) + 1]


def s3_path(bucket: str, prefix: str) -> str:
    """Build an S3 URI from bucket and prefix."""
    return f"s3://{bucket}/{prefix.strip('/')}"


def extract_region_from_path(column_name: str):
    """Extract region=xx from the S3 object path."""
    return F.lower(F.regexp_extract(F.col(column_name), r"region=([^/]+)", 1))


def extract_date_from_path(column_name: str):
    """Extract date=yyyy-mm-dd from the S3 object path."""
    return F.to_date(F.regexp_extract(F.col(column_name), r"date=([0-9-]+)", 1))


def to_bool(column_name: str):
    """Normalize string/boolean columns into boolean values."""
    value = F.lower(F.col(column_name).cast("string"))
    return F.when(value.isin("true", "1", "yes", "y"), F.lit(True)).when(
        value.isin("false", "0", "no", "n"), F.lit(False)
    )
