"""Lambda entry point for transforming explicit category objects to Silver."""

import json
import logging
from typing import Any

import boto3

from category_transform import transform_category_json
from config import load_settings

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SETTINGS = load_settings()
sns = boto3.client("sns")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Transform the Bronze category objects supplied by the orchestrator."""
    bucket = str(event.get("bucket") or "").strip()
    object_keys = event.get("object_keys")
    if not bucket:
        raise ValueError("bucket is required")
    if not isinstance(object_keys, list) or not object_keys:
        raise ValueError("object_keys must be a non-empty list")

    processed = []
    errors = []

    for raw_key in object_keys:
        key = str(raw_key).strip()
        try:
            logger.info("Processing s3://%s/%s", bucket, key)

            if key.startswith(SETTINGS.reference_prefix) and key.endswith(".json"):
                silver_key = transform_category_json(
                    bucket, key, source="kaggle", settings=SETTINGS
                )

            elif key.startswith(SETTINGS.api_categories_prefix) and key.endswith(
                ".json"
            ):
                silver_key = transform_category_json(
                    bucket,
                    key,
                    source="youtube_api",
                    settings=SETTINGS,
                )
            else:
                raise ValueError(f"Unsupported category object: {key}")

            processed.append(
                {"bronze_bucket": bucket, "bronze_key": key, "silver_key": silver_key}
            )

        except Exception as exc:
            logger.exception("Failed to process category object: %s", key)
            errors.append({"bucket": bucket, "key": key, "error": str(exc)})

    if errors:
        send_failure_alert(errors)
        raise RuntimeError(
            f"Reference-to-Silver transform failed for {len(errors)} file(s)."
        )

    return {"statusCode": 200, "processed": processed}


def send_failure_alert(errors: list[dict]) -> None:
    """Publish a compact SNS alert when one or more transforms fail."""
    if not SETTINGS.sns_topic_arn:
        logger.info("SNS_TOPIC_ARN is not configured; skipping failure alert.")
        return

    try:
        sns.publish(
            TopicArn=SETTINGS.sns_topic_arn,
            Subject="[YT Pipeline] Reference-to-Silver transform failed",
            Message=json.dumps({"errors": errors}, indent=2),
        )
    except Exception:
        logger.exception("Failed to publish SNS failure alert.")
