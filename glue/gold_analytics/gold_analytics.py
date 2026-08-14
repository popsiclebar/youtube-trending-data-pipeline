"""AWS Glue entry point for daily Silver-to-Gold snapshot aggregations."""

import json
import logging

import boto3
from awsglue.context import GlueContext
from pyspark import SparkContext
from pyspark.sql import functions as F

from config import load_args, table_ref
from transforms import (
    build_category_analytics,
    build_channel_analytics,
    build_trending_analytics,
    prepare_gold_input,
)
from writer import write_gold_table


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main() -> None:
    """Rebuild all Gold analytics tables for one complete Silver date."""
    args = load_args()
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session

    try:
        videos = spark.table(
            table_ref(args["silver_database"], args["video_table"])
        ).filter(F.col("date") == F.lit(args["process_date"]).cast("date"))
        if videos.limit(1).count() == 0:
            raise ValueError(f"No Silver videos found for {args['process_date']}")

        categories = spark.table(
            table_ref(args["silver_database"], args["category_table"])
        )
        gold_input = prepare_gold_input(
            videos, categories, args["process_date"]
        ).cache()

        outputs = (
            (
                args["trending_table"],
                "trending_analytics",
                build_trending_analytics(gold_input),
            ),
            (
                args["channel_table"],
                "channel_analytics",
                build_channel_analytics(gold_input),
            ),
            (
                args["category_analytics_table"],
                "category_analytics",
                build_category_analytics(gold_input),
            ),
        )
        for table_name, table_prefix, output in outputs:
            write_gold_table(glue_context, output, args, table_name, table_prefix)

        gold_input.unpersist()
        logger.info("Gold analytics completed for date=%s", args["process_date"])
    except Exception as exc:
        _send_failure_alert(args, exc)
        raise


def _send_failure_alert(args: dict[str, str], exc: Exception) -> None:
    """Publish a failure notification when an SNS topic is configured."""
    if not args["sns_topic_arn"]:
        return
    boto3.client("sns").publish(
        TopicArn=args["sns_topic_arn"],
        Subject="[YT Pipeline] Gold analytics Glue job failed",
        Message=json.dumps(
            {
                "job_name": args["job_name"],
                "process_date": args["process_date"],
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
            indent=2,
        ),
    )


if __name__ == "__main__":
    main()
