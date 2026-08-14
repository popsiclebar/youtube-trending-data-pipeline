# YouTube Trending Data Pipeline

This repository implements an end-to-end AWS data pipeline for processing
historical and live YouTube trending data from Kaggle and the YouTube Data API.
It organizes data into Bronze, Silver, and Gold layers on Amazon S3, uses Lambda
and AWS Glue for ingestion and transformation, enforces data quality through
Athena-backed validation, and publishes query-ready trending, channel, and
category analytics through the Glue Data Catalog. The design emphasizes
explicit schemas, deterministic writes, idempotent daily processing, and
repeatable infrastructure deployment.

## Architecture

```text
Data Sources
|
|-- Kaggle dataset                     # Historical CSV and JSON
|-- YouTube Data API                   # Live trending snapshots
|
`-- Bronze Layer: Amazon S3            # Raw source data
    |
    |-- youtube/kaggle_raw/...
    `-- youtube/api_raw/...
        |
        v
    Silver Layer: Amazon S3            # Standardized Parquet
    |
    |-- youtube/categories/...
    `-- youtube/videos/...
        |
        v
    Data Quality Gate                  # Lambda and Athena validation
        |
        v
    Gold Layer: Amazon S3              # Analytics aggregations
    |
    |-- trending_analytics/
    |-- channel_analytics/
    `-- category_analytics/
        |
        v
    Analytics / Consumption            # Athena and QuickSight
```

EventBridge and Step Functions provide orchestration. IAM, CloudWatch, SNS, and
CloudFormation provide security, monitoring, notifications, and deployment.

## Processing model

The live API pipeline is designed for two snapshots per UTC day, at hours `00`
and `12`. Scheduling stays outside the ingestion Lambda: EventBridge starts a
Standard Step Functions workflow, and a Wait state coordinates the second
snapshot before daily Silver, quality, and Gold processing.

YouTube statistics are cumulative snapshots. Gold therefore keeps each
`batch_hour` separate; it never adds the same video's views or likes across
hours. A daily rerun rebuilds one complete `date` partition and leaves all other
dates unchanged.

## Data Lake Layers

| Layer | Purpose | Storage and data contract |
| --- | --- | --- |
| Bronze | Preserve source data for replay and audit | Kaggle CSV/JSON and API JSON in S3, organized by dataset, region, date, and API hour |
| Silver | Clean and standardize both sources | Parquet tables `clean_video_statistics` and `clean_category_data`; videos partition by `source`, `region`, and `date` |
| Gold | Serve query-ready business metrics | Parquet tables `trending_analytics`, `channel_analytics`, and `category_analytics`, partitioned only by `date` |

Silver category joins use `source + region + date + category_id` for API data
and `source + region + category_id` for Kaggle. Gold retains `source`, `region`,
and `batch_hour` as columns and produces regional, channel, and category
snapshots without combining cumulative statistics across hours.

## Data quality

The `data_quality` Lambda runs aggregate Athena queries instead of loading
Silver Parquet into Lambda memory. The default suite validates:

- Glue Catalog columns;
- minimum video and category row counts;
- critical nulls and negative metrics;
- duplicate video and category keys;
- Silver freshness;
- expected region and batch-hour coverage; and
- video-to-category referential integrity.

It returns a Step Functions-friendly `quality_passed` decision. Failed checks
publish an SNS alert; execution or permission errors raise an exception for
workflow retry/catch handling. Athena results are written to a dedicated query
results bucket, not the Silver data bucket.

Example invocation:

```json
{
  "source": "youtube_api",
  "process_date": "2026-08-14",
  "regions": ["ca", "gb", "us"],
  "expected_hours": ["00", "12"]
}
```

## Repository structure

```text
.
├── data_quality/                  # Athena-backed Silver quality gate
├── glue/
│   ├── gold_analytics/            # Silver-to-Gold Glue job
│   └── silver_video_transforms/   # Bronze-to-Silver video Glue job
├── infra/cloudformation/          # Glue, Lambda, IAM, and workflow templates
├── lambda/
│   ├── reference_to_silver/       # Category JSON-to-Parquet transform
│   └── youtube_api_ingestion/     # Dataset-selectable API ingestion
├── scripts/                       # Packaging, deployment, upload, and backfill
├── typings/                       # Local AWS Glue type stubs
└── README.md
```

## Prerequisites

- An AWS account and AWS CLI credentials with permission to deploy IAM,
  Lambda, Glue, CloudFormation, S3, SNS, and Step Functions resources.
- Python 3.11 for Lambda packaging and local checks.
- Existing Bronze, Silver, Gold, and deployment-artifact S3 buckets.
- Existing Bronze, Silver, and Gold Glue databases.
- A YouTube Data API key stored in AWS Secrets Manager or Lambda environment
  configuration. Never commit API keys to the repository.

## Deployment

Set the shared environment first:

```bash
export AWS_REGION=eu-north-1
export BRONZE_BUCKET=<bronze-bucket>
export SILVER_BUCKET=<silver-bucket>
export GOLD_BUCKET=<gold-bucket>
export DEPLOYMENT_BUCKET=<deployment-artifacts-bucket>
export BRONZE_DATABASE=<bronze-database>
export SILVER_DATABASE=<silver-database>
export GOLD_DATABASE=<gold-database>
export SNS_TOPIC_ARN=<sns-topic-arn>
```

Deploy category-to-Silver processing:

```bash
./scripts/deploy_reference_to_silver.sh
```

Deploy the Silver video Glue job:

```bash
./scripts/deploy_silver_video_glue_job.sh
```

Deploy the Gold analytics Glue job and its three Catalog tables:

```bash
./scripts/deploy_gold_analytics_glue_job.sh
```

The Gold deployment expects the Gold bucket and Glue database to exist. Daily
or backfill executions must provide `--PROCESS_DATE`.

Package the API ingestion Lambda when updating it independently:

```bash
./scripts/package_youtube_api_ingestion.sh
```

## Manual batch execution

Run Silver for one complete API date:

```bash
aws glue start-job-run \
  --job-name yt-silver-video-transform \
  --arguments '{"--SOURCE":"youtube_api","--PROCESS_DATE":"2026-08-14"}'
```

Run the data-quality gate after both Silver datasets are ready:

```bash
aws lambda invoke \
  --function-name yt-data-pipeline-data-quality-test \
  --cli-binary-format raw-in-base64-out \
  --payload '{"source":"youtube_api","process_date":"2026-08-14","regions":["ca","gb","us"],"expected_hours":["00","12"]}' \
  data-quality-result.json
```

Only after `quality_passed` is true, rebuild Gold for that date:

```bash
aws glue start-job-run \
  --job-name yt-gold-analytics \
  --arguments '{"--PROCESS_DATE":"2026-08-14"}'
```
