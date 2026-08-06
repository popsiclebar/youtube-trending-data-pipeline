"""S3 read/write helpers for the reference-to-Silver Lambda."""

import io
import logging

import boto3
import pandas as pd

logger = logging.getLogger()

s3 = boto3.client("s3")
glue = boto3.client("glue")


def read_s3_object(bucket: str, key: str) -> bytes:
    """Read one S3 object body as bytes."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def write_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    metadata: dict[str, str] | None = None,
) -> None:
    """Serialize a DataFrame to Parquet in memory, then upload it to S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    put_args = {
        "Bucket": bucket,
        "Key": key,
        "Body": buffer.getvalue(),
        "ContentType": "application/vnd.apache.parquet",
    }
    if metadata:
        put_args["Metadata"] = {
            metadata_key: str(value) for metadata_key, value in metadata.items()
        }

    s3.put_object(**put_args)
    logger.info("Wrote s3://%s/%s (%d rows)", bucket, key, len(df))


def register_category_partition(
    database_name: str,
    table_name: str,
    silver_bucket: str,
    categories_prefix: str,
    source: str,
    region: str,
) -> None:
    """Register one source/region category partition in the Glue Catalog."""
    location = (
        f"s3://{silver_bucket}/{categories_prefix.strip('/')}/"
        f"source={source}/region={region}/"
    )
    partition_input = {
        "Values": [source, region],
        "StorageDescriptor": {
            "Columns": [
                {"Name": "category_id", "Type": "int"},
                {"Name": "category_title", "Type": "string"},
                {"Name": "channel_id", "Type": "string"},
                {"Name": "assignable", "Type": "boolean"},
                {"Name": "date", "Type": "string"},
            ],
            "Location": location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
        },
    }

    try:
        glue.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input,
        )
        logger.info(
            "Registered Glue partition %s.%s/%s/%s",
            database_name,
            table_name,
            source,
            region,
        )
    except glue.exceptions.AlreadyExistsException:
        glue.update_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValueList=[source, region],
            PartitionInput=partition_input,
        )
        logger.info(
            "Updated existing Glue partition: %s.%s/%s/%s",
            database_name,
            table_name,
            source,
            region,
        )
