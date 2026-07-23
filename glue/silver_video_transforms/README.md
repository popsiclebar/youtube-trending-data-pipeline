# Silver Video Transforms

AWS Glue ETL jobs for heavier video transformations.

This area will handle:

- Kaggle trending video CSV to Silver Parquet.
- YouTube API trending video JSON to Silver Parquet.
- Schema alignment between historical Kaggle data and live API data.
- Partitioned Silver outputs under `youtube/videos/source=.../region=...`.

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
s3://<silver-bucket>/youtube/videos/source=<source>/region=<region>/...
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
--SOURCE=all|kaggle|youtube_api
--KAGGLE_RAW_PREFIX=youtube/raw
--API_VIDEOS_PREFIX=youtube/api_raw/videos
--SILVER_VIDEOS_PREFIX=youtube/videos
--SNS_TOPIC_ARN=<optional-sns-topic-arn>
--MAX_INVALID_ROW_RATIO=0.05
```

The database names keep the job aligned with the Glue Data Catalog. The current
job writes Parquet to S3 first; Silver table creation can be done later with a
Crawler or explicit table definition after the output schema is validated.

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
   `video_id`, and observed date, keeping the most recent available record.

Required Silver fields:

- `video_id`: Kaggle `video_id` or YouTube API item `id`.
- `category_id`: Kaggle `category_id` or YouTube API `snippet.categoryId`.
- `region`: from the Bronze S3 key for API data, and from partitioned S3 paths
  for Kaggle data.
- `trending_date`: the date the video was observed as trending. For API data,
  this comes from the Bronze S3 key, not from the JSON body.
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
