"""
Source transformations for the Silver video Glue job.

This file contains both input contracts:
- Kaggle historical CSV files.
- YouTube API raw JSON responses.

Both functions return the same Silver video schema so the quality step can treat
the sources consistently.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import extract_date_from_path, extract_region_from_path, s3_path, to_bool
from quality import require_columns


def transform_kaggle_videos(spark, args: dict[str, str]) -> DataFrame:
    """Read Kaggle Bronze CSV files and standardize them to the Silver schema."""
    path = s3_path(args["bronze_bucket"], args["kaggle_raw_prefix"])
    raw_df = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("recursiveFileLookup", "true")
        .csv(path)
        .withColumn("_source_file", F.input_file_name())
    )
    require_columns(
        raw_df,
        {
            "video_id",
            "trending_date",
            "title",
            "channel_title",
            "category_id",
            "publish_time",
            "tags",
            "views",
            "likes",
            "dislikes",
            "comment_count",
        },
        "Kaggle videos CSV",
    )

    date_parts = F.split(F.col("trending_date"), "\\.")
    trending_date = F.to_date(
        F.concat_ws(
            "-",
            F.concat(F.lit("20"), date_parts.getItem(0)),
            date_parts.getItem(2),
            date_parts.getItem(1),
        )
    )

    return raw_df.select(
        F.lit("kaggle").alias("source"),
        extract_region_from_path("_source_file").alias("region"),
        F.col("video_id"),
        trending_date.alias("trending_date"),
        F.to_timestamp("publish_time").alias("published_at"),
        F.lit(None).cast("string").alias("channel_id"),
        F.col("channel_title"),
        F.col("title"),
        F.col("description"),
        F.col("category_id").cast("int").alias("category_id"),
        F.split(F.coalesce(F.col("tags"), F.lit("")), "\\|").alias("tags"),
        F.col("views").cast("long").alias("view_count"),
        F.col("likes").cast("long").alias("like_count"),
        F.col("dislikes").cast("long").alias("dislike_count"),
        F.lit(None).cast("long").alias("favorite_count"),
        F.col("comment_count").cast("long").alias("comment_count"),
        F.col("thumbnail_link").alias("thumbnail_url"),
        to_bool("comments_disabled").alias("comments_disabled"),
        to_bool("ratings_disabled").alias("ratings_disabled"),
        to_bool("video_error_or_removed").alias("video_error_or_removed"),
        F.lit(None).cast("string").alias("duration"),
        F.lit(None).cast("string").alias("definition"),
        F.lit(None).cast("boolean").alias("caption"),
        F.lit(None).cast("boolean").alias("licensed_content"),
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )


def transform_api_videos(spark, args: dict[str, str]) -> DataFrame:
    """Read YouTube API Bronze JSON files and standardize them to the Silver schema."""
    path = s3_path(args["bronze_bucket"], args["api_videos_prefix"])
    raw_df = (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(path)
        .withColumn("_source_file", F.input_file_name())
    )
    require_columns(raw_df, {"items"}, "YouTube API videos JSON")

    exploded_df = raw_df.withColumn("item", F.explode_outer("items"))

    return exploded_df.select(
        F.lit("youtube_api").alias("source"),
        extract_region_from_path("_source_file").alias("region"),
        F.col("item.id").alias("video_id"),
        # API JSON has no trending_date; this is the date observed in the S3 key.
        extract_date_from_path("_source_file").alias("trending_date"),
        F.to_timestamp("item.snippet.publishedAt").alias("published_at"),
        F.col("item.snippet.channelId").alias("channel_id"),
        F.col("item.snippet.channelTitle").alias("channel_title"),
        F.col("item.snippet.title").alias("title"),
        F.col("item.snippet.description").alias("description"),
        F.col("item.snippet.categoryId").cast("int").alias("category_id"),
        F.coalesce(F.col("item.snippet.tags"), F.expr("array()")).alias("tags"),
        F.col("item.statistics.viewCount").cast("long").alias("view_count"),
        F.col("item.statistics.likeCount").cast("long").alias("like_count"),
        # The YouTube API response does not provide dislikeCount.
        F.lit(None).cast("long").alias("dislike_count"),
        F.col("item.statistics.favoriteCount").cast("long").alias("favorite_count"),
        F.col("item.statistics.commentCount").cast("long").alias("comment_count"),
        F.col("item.snippet.thumbnails.default.url").alias("thumbnail_url"),
        F.lit(None).cast("boolean").alias("comments_disabled"),
        F.lit(None).cast("boolean").alias("ratings_disabled"),
        F.lit(None).cast("boolean").alias("video_error_or_removed"),
        F.col("item.contentDetails.duration").alias("duration"),
        F.col("item.contentDetails.definition").alias("definition"),
        to_bool("item.contentDetails.caption").alias("caption"),
        F.col("item.contentDetails.licensedContent")
        .cast("boolean")
        .alias("licensed_content"),
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )
