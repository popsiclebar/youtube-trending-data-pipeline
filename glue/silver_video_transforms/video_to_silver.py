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
from awsglue.dynamicframe import DynamicFrame
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
            write_silver_videos(glue_context, clean_kaggle_df, args)
            metrics.append(kaggle_metrics)

        if args["source"] in {"youtube_api", "all"}:
            api_df = transform_api_videos(spark, args)
            clean_api_df, api_metrics = apply_quality_checks(
                api_df, "youtube_api_videos", args
            )
            write_silver_videos(glue_context, clean_api_df, args)
            metrics.append(api_metrics)

        logger.info("Silver video transform completed: %s", json.dumps(metrics))

    except Exception as exc:
        send_failure_alert(args, exc)
        raise


def write_silver_videos(
    glue_context: GlueContext, df: DataFrame, args: dict[str, str]
) -> None:
    """Write Silver video Parquet and update the Glue Catalog table."""
    output_path = s3_path(args["silver_bucket"], args["silver_videos_prefix"])
    purge_silver_partitions(glue_context, df, output_path)
    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "clean_video_statistics")

    sink = glue_context.getSink(
        connection_type="s3",
        path=output_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["source", "region", "trending_date"],
    )
    sink.setCatalogInfo(
        catalogDatabase=args["silver_database"],
        catalogTableName=args["silver_videos_table"],
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynamic_frame)
    logger.info(
        "Wrote Silver videos to %s and updated %s.%s",
        output_path,
        args["silver_database"],
        args["silver_videos_table"],
    )


def purge_silver_partitions(
    glue_context: GlueContext, df: DataFrame, output_path: str
) -> None:
    """Delete existing files for the exact daily partitions being rewritten."""
    partition_rows = df.select("source", "region", "trending_date").distinct().collect()
    for row in partition_rows:
        partition_path = (
            f"{output_path}/source={row['source']}/region={row['region']}/"
            f"trending_date={row['trending_date']}/"
        )
        glue_context.purge_s3_path(partition_path, {"retentionPeriod": 0})
        logger.info("Purged existing Silver partition: %s", partition_path)


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
