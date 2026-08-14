"""Business aggregations for the three Gold analytics tables."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def prepare_gold_input(
    videos: DataFrame, categories: DataFrame, process_date: str
) -> DataFrame:
    """Join a complete Silver video date to its source-specific category lookup."""
    scoped_videos = videos.filter(F.col("date") == F.lit(process_date).cast("date"))
    video_lookup_date = F.when(
        F.col("source") == "kaggle", F.lit(None).cast("date")
    ).otherwise(F.col("date"))
    scoped_videos = scoped_videos.withColumn("_category_date", video_lookup_date)

    scoped_categories = (
        categories.withColumn("_category_date", F.to_date("date"))
        .filter(
            F.col("_category_date").isNull()
            | (F.col("_category_date") == F.lit(process_date).cast("date"))
        )
        .select("source", "region", "_category_date", "category_id", "category_title")
        .dropDuplicates(["source", "region", "_category_date", "category_id"])
    )

    video = scoped_videos.alias("video")
    category = F.broadcast(scoped_categories).alias("category")
    join_condition = (
        (F.col("video.source") == F.col("category.source"))
        & (F.col("video.region") == F.col("category.region"))
        & (F.col("video.category_id") == F.col("category.category_id"))
        & F.col("video._category_date").eqNullSafe(F.col("category._category_date"))
    )
    enriched = video.join(category, join_condition, "left").select(
        "video.*", F.col("category.category_title")
    )

    if enriched.filter(F.col("category_title").isNull()).limit(1).count():
        raise ValueError(
            f"Silver videos for {process_date} contain an unmapped category"
        )

    return (
        enriched.drop("_category_date")
        .withColumn(
            "_channel_key",
            F.coalesce(F.col("channel_id"), F.col("channel_title")),
        )
    )


def build_trending_analytics(df: DataFrame) -> DataFrame:
    """Build one regional snapshot per source, date, and available batch hour."""
    grouping = ["source", "date", "region", "batch_hour"]
    return (
        df.groupBy(*grouping)
        .agg(
            *_snapshot_metrics(),
            F.count_distinct("_channel_key").alias("channel_count"),
            F.count_distinct("category_id").alias("category_count"),
        )
        .withColumn("gold_aggregation_timestamp", F.current_timestamp())
        .select(
            "source",
            "region",
            "batch_hour",
            "video_count",
            "total_view_count",
            "total_like_count",
            "total_dislike_count",
            "total_comment_count",
            "average_view_count",
            "maximum_view_count",
            "channel_count",
            "category_count",
            "engagement_rate",
            "gold_aggregation_timestamp",
            "date",
        )
    )


def build_channel_analytics(df: DataFrame) -> DataFrame:
    """Build channel performance snapshots without combining cumulative hours."""
    grouping = ["source", "date", "region", "batch_hour", "_channel_key"]
    result = df.groupBy(*grouping).agg(
        F.first("channel_id", ignorenulls=True).alias("channel_id"),
        F.first("channel_title", ignorenulls=True).alias("channel_title"),
        *_snapshot_metrics(),
        F.count_distinct("category_id").alias("category_count"),
    )
    ranking = Window.partitionBy("source", "date", "region", "batch_hour").orderBy(
        F.col("total_view_count").desc(), F.col("_channel_key").asc()
    )
    return (
        result.withColumn("view_rank", F.row_number().over(ranking))
        .withColumn("gold_aggregation_timestamp", F.current_timestamp())
        .select(
            "source",
            "region",
            "batch_hour",
            "channel_id",
            "channel_title",
            "video_count",
            "total_view_count",
            "total_like_count",
            "total_dislike_count",
            "total_comment_count",
            "average_view_count",
            "maximum_view_count",
            "category_count",
            "engagement_rate",
            "view_rank",
            "gold_aggregation_timestamp",
            "date",
        )
    )


def build_category_analytics(df: DataFrame) -> DataFrame:
    """Build category snapshots and their share of regional snapshot views."""
    grouping = [
        "source",
        "date",
        "region",
        "batch_hour",
        "category_id",
        "category_title",
    ]
    result = df.groupBy(*grouping).agg(
        *_snapshot_metrics(),
        F.count_distinct("_channel_key").alias("channel_count"),
    )
    snapshot_window = Window.partitionBy("source", "date", "region", "batch_hour")
    ranking = snapshot_window.orderBy(
        F.col("total_view_count").desc(), F.col("category_id").asc()
    )
    snapshot_view_count = F.sum("total_view_count").over(snapshot_window)
    return (
        result.withColumn(
            "view_share_percentage",
            F.when(
                snapshot_view_count > 0,
                F.round(F.col("total_view_count") / snapshot_view_count * 100, 4),
            ),
        )
        .withColumn("view_rank", F.row_number().over(ranking))
        .withColumn("gold_aggregation_timestamp", F.current_timestamp())
        .select(
            "source",
            "region",
            "batch_hour",
            "category_id",
            "category_title",
            "video_count",
            "total_view_count",
            "total_like_count",
            "total_dislike_count",
            "total_comment_count",
            "average_view_count",
            "maximum_view_count",
            "channel_count",
            "engagement_rate",
            "view_share_percentage",
            "view_rank",
            "gold_aggregation_timestamp",
            "date",
        )
    )


def _snapshot_metrics() -> list:
    """Return metrics that are valid within one cumulative-statistics snapshot."""
    total_views = F.sum("view_count")
    total_likes = F.sum("like_count")
    total_comments = F.sum("comment_count")
    return [
        F.count_distinct("video_id").alias("video_count"),
        total_views.alias("total_view_count"),
        total_likes.alias("total_like_count"),
        F.sum("dislike_count").alias("total_dislike_count"),
        total_comments.alias("total_comment_count"),
        F.avg("view_count").alias("average_view_count"),
        F.max("view_count").alias("maximum_view_count"),
        F.when(
            total_views > 0,
            (
                F.coalesce(total_likes, F.lit(0))
                + F.coalesce(total_comments, F.lit(0))
            )
            / total_views,
        ).alias("engagement_rate"),
    ]
