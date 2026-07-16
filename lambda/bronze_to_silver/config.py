"""Runtime configuration for the Bronze-to-Silver Lambda."""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    """Environment-driven settings used by the Lambda handler and transforms."""

    silver_bucket: str
    raw_prefix: str = "youtube/raw/"
    reference_prefix: str = "youtube/raw_reference_data/"
    api_videos_prefix: str = "youtube/api_raw/videos/"
    api_categories_prefix: str = "youtube/api_raw/categories/"
    videos_output_prefix: str = "youtube/videos/"
    categories_output_prefix: str = "youtube/categories/"
    sns_topic_arn: str = ""


def required_env(name: str) -> str:
    """Read a required environment variable and fail fast when it is missing."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str) -> str:
    """Read an optional environment variable, returning an empty string if absent."""
    return os.environ.get(name, "").strip()


def env_prefix(name: str, default: str) -> str:
    """Read an S3 prefix from the environment and normalize one trailing slash."""
    value = os.environ.get(name, default).strip().strip("/")
    if not value:
        raise RuntimeError(f"Environment variable cannot be empty: {name}")
    return f"{value}/"


def load_settings() -> Settings:
    """Create settings once when the Lambda runtime starts."""
    return Settings(
        silver_bucket=required_env("SILVER_BUCKET"),
        raw_prefix=env_prefix("RAW_PREFIX", "youtube/raw"),
        reference_prefix=env_prefix("REFERENCE_PREFIX", "youtube/raw_reference_data"),
        api_videos_prefix=env_prefix("API_VIDEOS_PREFIX", "youtube/api_raw/videos"),
        api_categories_prefix=env_prefix(
            "API_CATEGORIES_PREFIX", "youtube/api_raw/categories"
        ),
        videos_output_prefix=env_prefix("VIDEOS_OUTPUT_PREFIX", "youtube/videos"),
        categories_output_prefix=env_prefix(
            "CATEGORIES_OUTPUT_PREFIX", "youtube/categories"
        ),
        sns_topic_arn=optional_env("SNS_TOPIC_ARN"),
    )
