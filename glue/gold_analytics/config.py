"""Configuration for the Silver-to-Gold AWS Glue job."""

from datetime import date
import re
import sys

from awsglue.utils import getResolvedOptions


_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_SAFE_PREFIX = re.compile(r"^[A-Za-z0-9_./-]+$")


def load_args() -> dict[str, str]:
    """Load and validate the processing date and AWS resource names."""
    required = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "SILVER_DATABASE",
            "GOLD_DATABASE",
            "GOLD_BUCKET",
            "PROCESS_DATE",
        ],
    )
    args = {
        "job_name": required["JOB_NAME"],
        "silver_database": required["SILVER_DATABASE"],
        "gold_database": required["GOLD_DATABASE"],
        "gold_bucket": required["GOLD_BUCKET"],
        "process_date": required["PROCESS_DATE"],
        "video_table": optional_arg("VIDEO_TABLE", "clean_video_statistics"),
        "category_table": optional_arg("CATEGORY_TABLE", "clean_category_data"),
        "trending_table": optional_arg("TRENDING_TABLE", "trending_analytics"),
        "channel_table": optional_arg("CHANNEL_TABLE", "channel_analytics"),
        "category_analytics_table": optional_arg(
            "CATEGORY_ANALYTICS_TABLE", "category_analytics"
        ),
        "gold_prefix": optional_arg("GOLD_PREFIX", "youtube"),
        "sns_topic_arn": optional_arg("SNS_TOPIC_ARN", ""),
    }

    date.fromisoformat(args["process_date"])
    for key in (
        "job_name",
        "silver_database",
        "gold_database",
        "video_table",
        "category_table",
        "trending_table",
        "channel_table",
        "category_analytics_table",
    ):
        if not _SAFE_NAME.fullmatch(args[key]):
            raise ValueError(f"Unsafe Glue configuration value for {key}: {args[key]!r}")
    if not _SAFE_NAME.fullmatch(args["gold_bucket"]):
        raise ValueError(f"Unsafe Gold bucket name: {args['gold_bucket']!r}")
    if not _SAFE_PREFIX.fullmatch(args["gold_prefix"]):
        raise ValueError(f"Unsafe Gold prefix: {args['gold_prefix']!r}")

    return args


def optional_arg(name: str, default: str) -> str:
    """Read one optional Glue argument without requiring it at deployment."""
    token = f"--{name}"
    if token not in sys.argv:
        return default

    value_index = sys.argv.index(token) + 1
    if value_index >= len(sys.argv) or sys.argv[value_index].startswith("--"):
        return default
    return sys.argv[value_index] or default


def table_ref(database: str, table: str) -> str:
    """Return a quoted Spark catalog table reference."""
    return f"`{database}`.`{table}`"


def s3_path(bucket: str, *parts: str) -> str:
    """Build an S3 URI without duplicate separators."""
    suffix = "/".join(part.strip("/") for part in parts if part)
    return f"s3://{bucket}/{suffix}"
