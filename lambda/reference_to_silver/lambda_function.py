"""Lambda entry point for routing category JSON files to Silver Parquet."""

import json
import logging
from urllib.parse import unquote_plus

import boto3

from category_transform import transform_category_json
from config import load_settings

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SETTINGS = load_settings()
sns = boto3.client("sns")


def lambda_handler(event, context):
    """Transform supported category JSON S3 events into Silver Parquet."""
    processed = []
    errors = []

    for record in event.get("Records", []):
        try:
            s3_info = record["s3"]
            bucket = s3_info["bucket"]["name"]
            key = unquote_plus(s3_info["object"]["key"])
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
                logger.warning("Skipping unsupported file: %s", key)
                continue

            processed.append(
                {"bronze_bucket": bucket, "bronze_key": key, "silver_key": silver_key}
            )

        except Exception as exc:
            logger.exception("Failed to process S3 event record: %s", record)
            errors.append({"record": record, "error": str(exc)})

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

    sns.publish(
        TopicArn=SETTINGS.sns_topic_arn,
        Subject="[YT Pipeline] Reference-to-Silver transform failed",
        Message=json.dumps({"errors": errors}, indent=2),
    )
