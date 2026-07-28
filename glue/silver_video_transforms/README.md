# Silver Video Transforms

AWS Glue ETL jobs for heavier video transformations.

This area will handle:

- Kaggle trending video CSV to Silver Parquet.
- YouTube API trending video JSON to Silver Parquet.
- Schema alignment between historical Kaggle data and live API data.
- Partitioned Silver outputs under
  `youtube/videos/source=.../region=.../trending_date=...`.

The category/reference JSON transform stays in Lambda because it is small and
simple. Video transforms belong in Glue because they can grow larger and need
better support for backfills, partition management, and future Gold aggregation.

## Job Script

```text
silver_video_transforms/
|
|-- video_to_silver.py       # Glue job entrypoint and orchestration
|-- transforms.py            # Kaggle CSV and YouTube API JSON mapping
|-- config.py                # Job arguments and small shared helpers
`-- quality.py               # Data quality and deduplication
```

The job standardizes both sources into the same Silver video schema and writes
Parquet to:

```text
s3://<silver-bucket>/youtube/videos/source=<source>/region=<region>/trending_date=<date>/...
```

## Required Arguments

```text
--JOB_NAME=<glue-job-name>
--BRONZE_BUCKET=<bronze-s3-bucket>
--SILVER_BUCKET=<silver-s3-bucket>
```

## Optional Arguments

```text
--BRONZE_DATABASE=youtube_bronze
--SILVER_DATABASE=youtube_silver
--SILVER_VIDEOS_TABLE=clean_video_statistics
--SOURCE=all|kaggle|youtube_api
--KAGGLE_RAW_PREFIX=youtube/kaggle_raw/raw
--API_VIDEOS_PREFIX=youtube/api_raw/videos
--SILVER_VIDEOS_PREFIX=youtube/videos
--PROCESS_DATE=<optional-yyyy-mm-dd>
--PROCESS_HOUR=<optional-hh>
--SNS_TOPIC_ARN=<optional-sns-topic-arn>
--MAX_INVALID_ROW_RATIO=0.05
```

The database names keep the job aligned with the Glue Data Catalog. The current
job writes Parquet through a Glue Catalog sink, updating
`clean_video_statistics` with `source`, `region`, and `trending_date`
partitions.

For YouTube API runs, `PROCESS_DATE` and `PROCESS_HOUR` limit the read to one
Bronze API batch:

```text
s3://<bronze-bucket>/youtube/api_raw/videos/region=*/date=<PROCESS_DATE>/hour=<PROCESS_HOUR>/
```

The job keeps `batch_hour` as a normal lineage column instead of a partition
folder. Rerunning the same source/region/date can overwrite that daily
partition, while avoiding hour-level over-partitioning.

Before writing, the job purges only the exact `source`/`region`/`trending_date`
partitions present in the current clean DataFrame. This keeps reruns idempotent
without deleting unrelated regions or dates.

## Deployment Note

AWS Glue runs one main script from S3. Because this job is split into helper
modules, `scripts/deploy_silver_video_glue_job.sh` also builds a small zip file
containing `config.py`, `transforms.py`, and `quality.py`. CloudFormation passes
that zip to Glue with `--extra-py-files` so imports work in AWS.

## Data Quality

The job applies Silver quality rules in four stages:

1. Schema validation: required raw columns must exist before transformation.
2. Cleansing: source/region/text fields are normalized and empty critical
   strings are treated as missing values.
3. Quality checks: the job measures missing critical fields, missing context
   fields, negative numeric metrics, invalid-row ratio, and output row counts.
4. Deduplication: duplicate rows are resolved by `source`, `region`,
   `video_id`, observed date, and `batch_hour`, keeping the most recent
   available record.

Required Silver fields:

- `video_id`: Kaggle `video_id` or YouTube API item `id`.
- `category_id`: Kaggle `category_id` or YouTube API `snippet.categoryId`.
- `region`: from the Bronze S3 key for API data, and from partitioned S3 paths
  for Kaggle data.
- `trending_date`: the date the video was observed as trending. For API data,
  this comes from the Bronze S3 key, not from the JSON body.
- `batch_hour`: `historical` for Kaggle data, or the Bronze `hour=` partition
  for YouTube API data.
- `published_at`: Kaggle `publish_time` or YouTube API `snippet.publishedAt`.

`dislike_count` is nullable because the YouTube API response does not provide
dislike data.

The job fails when:

- the source produces no rows;
- all rows are invalid after quality checks;
- the invalid critical-row ratio is higher than `MAX_INVALID_ROW_RATIO`;
- any row contains negative video metrics such as views, likes, or comments.

Failures publish to SNS when `SNS_TOPIC_ARN` is configured. Later, the job can
add quarantine outputs for rejected rows, explicit Glue table updates, and
richer data quality reports before Gold processing.
