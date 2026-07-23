"""
AWS Glue job: transform Bronze video data into Silver Parquet.

This is the Glue job entrypoint. It keeps orchestration here and delegates the
source-specific logic to small modules so the ETL flow is easier to learn,
test, and maintain.
"""

import json
import logging

import boto3
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.context import SparkContext

from config import load_args, s3_path
from quality import apply_quality_checks
from transforms import transform_api_videos, transform_kaggle_videos

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sns = boto3.client("sns")


def main() -> None:
    """Run the selected Bronze-to-Silver video transformations."""
    args = load_args()
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        metrics = []

        if args["source"] in {"kaggle", "all"}:
            kaggle_df = transform_kaggle_videos(spark, args)
            clean_kaggle_df, kaggle_metrics = apply_quality_checks(
                kaggle_df, "kaggle_videos", args
            )
            write_silver_videos(clean_kaggle_df, args)
            metrics.append(kaggle_metrics)

        if args["source"] in {"youtube_api", "all"}:
            api_df = transform_api_videos(spark, args)
            clean_api_df, api_metrics = apply_quality_checks(
                api_df, "youtube_api_videos", args
            )
            write_silver_videos(clean_api_df, args)
            metrics.append(api_metrics)

        logger.info("Silver video transform completed: %s", json.dumps(metrics))

    except Exception as exc:
        send_failure_alert(args, exc)
        raise


def write_silver_videos(df: DataFrame, args: dict[str, str]) -> None:
    """Write Silver video data as Parquet partitioned by source and region."""
    output_path = s3_path(args["silver_bucket"], args["silver_videos_prefix"])
    (
        df.write.mode("overwrite")
        .format("parquet")
        .partitionBy("source", "region")
        .save(output_path)
    )
    logger.info("Wrote Silver videos to %s", output_path)


def send_failure_alert(args: dict[str, str], exc: Exception) -> None:
    """Publish an SNS alert when the Glue video transform fails."""
    if not args["sns_topic_arn"]:
        logger.info("SNS_TOPIC_ARN is not configured; skipping failure alert.")
        return

    message = {
        "job_name": args["job_name"],
        "source": args["source"],
        "bronze_database": args["bronze_database"],
        "silver_database": args["silver_database"],
        "error_type": type(exc).__name__,
        "error_message": str(exc),
    }
    sns.publish(
        TopicArn=args["sns_topic_arn"],
        Subject="[YT Pipeline] Silver video Glue transform failed",
        Message=json.dumps(message, indent=2),
    )


if __name__ == "__main__":
    main()
