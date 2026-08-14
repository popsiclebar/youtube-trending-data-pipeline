"""Idempotent S3 and Glue Catalog writes for Gold date partitions."""

import logging

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame

from config import s3_path


logger = logging.getLogger(__name__)


def write_gold_table(
    glue_context,
    df: DataFrame,
    args: dict[str, str],
    table_name: str,
    table_prefix: str,
) -> None:
    """Replace one complete date partition and update its catalog metadata."""
    output_path = s3_path(args["gold_bucket"], args["gold_prefix"], table_prefix)
    date_path = f"{output_path}/date={args['process_date']}/"
    glue_context.purge_s3_path(date_path, {"retentionPeriod": 0})
    logger.info("Purged existing Gold partition: %s", date_path)

    dynamic_frame = DynamicFrame.fromDF(df.coalesce(1), glue_context, table_name)
    sink = glue_context.getSink(
        connection_type="s3",
        path=output_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["date"],
    )
    sink.setCatalogInfo(
        catalogDatabase=args["gold_database"], catalogTableName=table_name
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynamic_frame)
    logger.info("Wrote %s for date=%s to %s", table_name, args["process_date"], output_path)
