"""Runtime settings for the YouTube API ingestion Lambda."""

from dataclasses import dataclass
import os

import boto3

DEFAULT_REGION_CODES = "CA,DE,FR,GB,IN,JP,KR,MX,RU,US"

secretsmanager = boto3.client("secretsmanager")


@dataclass(frozen=True)
class Settings:
    """Environment-driven settings used by ingestion, storage, and alerts."""

    bronze_bucket: str
    youtube_api_key: str
    region_codes: tuple[str, ...]
    max_results: int
    sns_topic_arn: str | None = None
    api_base_url: str = "https://www.googleapis.com/youtube/v3"
    videos_prefix: str = "youtube/api_raw/videos"
    categories_prefix: str = "youtube/api_raw/categories"
    fetch_categories: bool = True


def required_env(name: str) -> str:
    """Read a required environment variable and fail loudly if it is missing."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str) -> str | None:
    """Read an optional environment variable and return None when it is blank."""
    value = os.environ.get(name, "").strip()
    return value or None


def env_value(name: str, default: str) -> str:
    """Read an optional environment variable with a default value."""
    return os.environ.get(name, default).strip()


def parse_bool(value: str) -> bool:
    """Convert friendly string values like true/false into a Python bool."""
    return value.strip().lower() in {"1", "true", "yes", "y"}


def parse_region_codes(value: str) -> tuple[str, ...]:
    """Convert comma-separated region codes into normalized uppercase values."""
    regions = tuple(
        region.strip().upper() for region in value.split(",") if region.strip()
    )
    if not regions:
        raise RuntimeError("REGION_CODES must contain at least one region code.")
    return regions


def parse_max_results(value: str) -> int:
    """Validate YouTube videos.list maxResults, which must be between 1 and 50."""
    max_results = int(value)
    if max_results < 1 or max_results > 50:
        raise RuntimeError("MAX_RESULTS must be between 1 and 50.")
    return max_results


def normalized_prefix(name: str, default: str) -> str:
    """Normalize an S3 prefix so key building has no double slashes."""
    return env_value(name, default).strip("/")


def load_youtube_api_key() -> str:
    """Load the YouTube API key from Lambda env or AWS Secrets Manager."""
    direct_key = optional_env("YOUTUBE_API_KEY")
    if direct_key:
        return direct_key

    secret_id = optional_env("YOUTUBE_API_KEY_SECRET_ID")
    if not secret_id:
        raise RuntimeError(
            "Missing required environment variable: YOUTUBE_API_KEY "
            "or YOUTUBE_API_KEY_SECRET_ID"
        )

    response = secretsmanager.get_secret_value(SecretId=secret_id)
    secret_key = response.get("SecretString", "").strip()
    if not secret_key:
        raise RuntimeError(f"Secret {secret_id} does not contain a SecretString value.")
    return secret_key


def load_settings() -> Settings:
    """Create a Settings object once when the Lambda runtime starts."""
    return Settings(
        bronze_bucket=required_env("BRONZE_BUCKET"),
        youtube_api_key=load_youtube_api_key(),
        region_codes=parse_region_codes(
            env_value("REGION_CODES", DEFAULT_REGION_CODES)
        ),
        max_results=parse_max_results(env_value("MAX_RESULTS", "50")),
        sns_topic_arn=optional_env("SNS_TOPIC_ARN"),
        api_base_url=env_value(
            "YOUTUBE_API_BASE_URL",
            "https://www.googleapis.com/youtube/v3",
        ).rstrip("/"),
        videos_prefix=normalized_prefix(
            "VIDEOS_BRONZE_PREFIX",
            "youtube/api_raw/videos",
        ),
        categories_prefix=normalized_prefix(
            "CATEGORIES_BRONZE_PREFIX",
            "youtube/api_raw/categories",
        ),
        fetch_categories=parse_bool(env_value("FETCH_CATEGORIES", "true")),
    )
