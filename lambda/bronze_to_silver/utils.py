"""Small parsing and type-normalization helpers for Silver transforms."""

import logging
import re
from datetime import datetime

import pandas as pd

logger = logging.getLogger()

REGION_PATTERN = re.compile(r"region=([a-z]{2})")
DATE_PATTERN = re.compile(r"(?:date|ingestion_date)=([0-9]{4}-[0-9]{2}-[0-9]{2})")


def extract_region(s3_key: str) -> str:
    """Pull a region code from a Hive-style S3 path such as region=us."""
    match = REGION_PATTERN.search(s3_key)
    if not match:
        raise ValueError(f"Could not find region partition in key: {s3_key}")
    return match.group(1)


def extract_date_partition(s3_key: str) -> str | None:
    """Pull a date partition from a path like date=2026-07-14."""
    match = DATE_PATTERN.search(s3_key)
    return match.group(1) if match else None


def parse_kaggle_trending_date(value: str) -> datetime | None:
    """Parse Kaggle trending_date values from yy.dd.mm into datetime objects."""
    if pd.isna(value) or not str(value).strip():
        return None
    try:
        yy, dd, mm = str(value).split(".")
        return datetime(2000 + int(yy), int(mm), int(dd))
    except (ValueError, TypeError):
        logger.warning("Could not parse trending_date: %s", value)
        return None


def nullable_int(value) -> int | None:
    """Convert string counts like '123' to int while preserving missing values."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_bool(value):
    """Normalize common boolean representations from CSV/API data."""
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}
