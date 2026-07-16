"""Lambda entry point for routing Bronze S3 objects to Silver transforms."""

import json
import logging
from urllib.parse import unquote_plus

import boto3

from config import load_settings
from transforms import (
    transform_api_videos_json,
    transform_category_json,
    transform_kaggle_videos_csv,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SETTINGS = load_settings()
sns = boto3.client("sns")


def lambda_handler(event, context):
    """Route each S3 event record to the matching Bronze-to-Silver transform."""
    processed = []
    errors = []

    for record in event.get("Records", []):
        try:
            s3_info = record["s3"]
            bucket = s3_info["bucket"]["name"]
            key = unquote_plus(s3_info["object"]["key"])
            logger.info("Processing s3://%s/%s", bucket, key)

            if key.startswith(SETTINGS.reference_prefix) and key.endswith(".json"):
                transform_category_json(
                    bucket, key, source="kaggle", settings=SETTINGS
                )

            elif key.startswith(SETTINGS.raw_prefix) and key.endswith(".csv"):
                transform_kaggle_videos_csv(bucket, key, settings=SETTINGS)

            elif key.startswith(SETTINGS.api_videos_prefix) and key.endswith(".json"):
                transform_api_videos_json(bucket, key, settings=SETTINGS)

            elif key.startswith(SETTINGS.api_categories_prefix) and key.endswith(
                ".json"
            ):
                transform_category_json(
                    bucket,
                    key,
                    source="youtube_api",
                    settings=SETTINGS,
                )
            else:
                logger.warning("Skipping unsupported file: %s", key)
                continue

            processed.append({"bucket": bucket, "key": key})

        except Exception as exc:
            logger.exception("Failed to process S3 event record: %s", record)
            errors.append({"record": record, "error": str(exc)})

    if errors:
        send_failure_alert(errors)
        raise RuntimeError(
            f"Bronze-to-Silver transform failed for {len(errors)} file(s)."
        )

    return {"statusCode": 200, "processed": processed}


def send_failure_alert(errors: list[dict]) -> None:
    """Publish a compact SNS alert when one or more transforms fail."""
    if not SETTINGS.sns_topic_arn:
        logger.info("SNS_TOPIC_ARN is not configured; skipping failure alert.")
        return

    sns.publish(
        TopicArn=SETTINGS.sns_topic_arn,
        Subject="[YT Pipeline] Bronze-to-Silver transform failed",
        Message=json.dumps({"errors": errors}, indent=2),
    )
