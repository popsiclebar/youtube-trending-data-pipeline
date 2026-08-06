"""Configuration and invocation scope for the Silver data-quality gate."""

from dataclasses import dataclass
from datetime import date
import os
import re


DEFAULT_CHECKS = frozenset(
    {
        "catalog_schema",
        "video_rows",
        "video_critical_fields",
        "video_metric_ranges",
        "video_duplicates",
        "video_freshness",
        "region_coverage",
        "hour_coverage",
        "category_rows",
        "category_critical_fields",
        "category_duplicates",
        "category_mapping",
    }
)

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class Settings:
    silver_database: str
    video_table: str
    category_table: str
    athena_workgroup: str
    athena_output_location: str
    sns_topic_arn: str
    default_source: str
    default_regions: tuple[str, ...]
    default_expected_hours: tuple[str, ...]
    min_video_rows: int
    min_category_rows: int
    max_freshness_hours: int
    query_timeout_seconds: int


@dataclass(frozen=True)
class QualityScope:
    source: str
    process_date: str | None
    regions: tuple[str, ...]
    expected_hours: tuple[str, ...]
    checks: frozenset[str]


def _csv_values(raw_value: str, *, lowercase: bool = True) -> tuple[str, ...]:
    values = []
    for item in raw_value.split(","):
        value = item.strip()
        if not value:
            continue
        value = value.lower() if lowercase else value
        if not _SAFE_VALUE.fullmatch(value):
            raise ValueError(f"Unsupported configuration value: {value!r}")
        if value not in values:
            values.append(value)
    return tuple(values)


def _event_values(raw_value: object, *, lowercase: bool = True) -> tuple[str, ...]:
    if isinstance(raw_value, str):
        return _csv_values(raw_value, lowercase=lowercase)
    return _csv_values(",".join(str(value) for value in raw_value), lowercase=lowercase)


def load_settings() -> Settings:
    """Load environment settings and reject unsafe catalog identifiers."""
    database = os.environ.get("SILVER_DATABASE", "yt-pipeline-silver-test").strip()
    video_table = os.environ.get("VIDEO_TABLE", "clean_video_statistics").strip()
    category_table = os.environ.get("CATEGORY_TABLE", "clean_category_data").strip()
    workgroup = os.environ.get("ATHENA_WORKGROUP", "primary").strip()
    for value in (database, video_table, category_table, workgroup):
        if not _SAFE_NAME.fullmatch(value):
            raise ValueError(f"Unsafe AWS resource name: {value!r}")

    output_location = os.environ.get("ATHENA_OUTPUT_LOCATION", "").strip()
    if not output_location.startswith("s3://"):
        raise RuntimeError("ATHENA_OUTPUT_LOCATION must be a complete s3:// URI")

    return Settings(
        silver_database=database,
        video_table=video_table,
        category_table=category_table,
        athena_workgroup=workgroup,
        athena_output_location=output_location.rstrip("/") + "/",
        sns_topic_arn=os.environ.get("SNS_TOPIC_ARN", "").strip(),
        default_source=os.environ.get("DEFAULT_SOURCE", "youtube_api").strip().lower(),
        default_regions=_csv_values(os.environ.get("DEFAULT_REGIONS", "ca,gb,us")),
        default_expected_hours=_csv_values(
            os.environ.get("EXPECTED_HOURS", ""), lowercase=False
        ),
        min_video_rows=int(os.environ.get("MIN_VIDEO_ROWS", "1")),
        min_category_rows=int(os.environ.get("MIN_CATEGORY_ROWS", "1")),
        max_freshness_hours=int(os.environ.get("MAX_FRESHNESS_HOURS", "48")),
        query_timeout_seconds=int(os.environ.get("QUERY_TIMEOUT_SECONDS", "180")),
    )


def scope_from_event(event: dict, settings: Settings) -> QualityScope:
    """Apply safe event overrides while retaining useful deployment defaults."""
    source = str(event.get("source") or settings.default_source).strip().lower()
    if not _SAFE_VALUE.fullmatch(source):
        raise ValueError(f"Unsupported source: {source!r}")

    process_date = event.get("process_date")
    if process_date:
        process_date = str(process_date).strip()
        date.fromisoformat(process_date)

    raw_regions = event.get("regions")
    regions = (
        _event_values(raw_regions)
        if raw_regions is not None
        else settings.default_regions
    )
    if not regions:
        raise ValueError("At least one region must be configured")

    raw_hours = event.get("expected_hours")
    expected_hours = (
        _event_values(raw_hours, lowercase=False)
        if raw_hours is not None
        else settings.default_expected_hours
    )
    invalid_hours = [
        hour
        for hour in expected_hours
        if not re.fullmatch(r"[0-2][0-9]", hour) or int(hour) > 23
    ]
    if invalid_hours:
        raise ValueError(f"Invalid expected_hours: {invalid_hours}")

    raw_checks = event.get("checks")
    checks = frozenset(_event_values(raw_checks)) if raw_checks else DEFAULT_CHECKS
    unknown_checks = checks - DEFAULT_CHECKS
    if unknown_checks:
        raise ValueError(f"Unknown data-quality checks: {sorted(unknown_checks)}")

    return QualityScope(source, process_date, regions, expected_hours, checks)
