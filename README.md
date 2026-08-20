# YouTube Trending Data Pipeline

This repository implements an end-to-end AWS data pipeline for processing
historical and live YouTube trending data from Kaggle and the YouTube Data API.
It organizes data into Bronze, Silver, and Gold layers on Amazon S3, uses Lambda
and AWS Glue for ingestion and transformation, enforces data quality through
Athena-backed validation, and publishes query-ready trending, channel, and
category analytics through the Glue Data Catalog. The design emphasizes
explicit schemas, deterministic writes, idempotent daily processing, and
repeatable infrastructure deployment.

The complete Bronze-to-Gold workflow has been validated successfully in the
AWS test environment. Its EventBridge schedule is deployed but intentionally
disabled, so executions remain manual until automated scheduling is required.

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

EventBridge starts the daily workflow, and Step Functions coordinates ingestion,
parallel Silver transformations, validation, and Gold processing. IAM,
CloudWatch, SNS, and CloudFormation provide security, monitoring,
notifications, and repeatable deployment.

## Processing model

The live API pipeline ingests one video and category snapshot per UTC day.
Scheduling stays outside the ingestion Lambda: EventBridge starts a Standard
Step Functions workflow, which captures one stable execution timestamp and
uses it through every retry.

After ingestion and a short stabilization wait, Step Functions runs the
category Lambda and Silver video Glue job in parallel. The workflow waits for
both branches before running the data-quality gate, and Gold runs only when all
quality checks pass.

YouTube statistics are cumulative snapshots. Gold therefore retains
`batch_hour` as lineage and never adds the same video's views or likes across
hours. The hourly Bronze path also preserves earlier raw observations. Each
successful workflow execution selects its own hour as the authoritative Silver
snapshot and replaces that date's Silver and Gold partitions.

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
  "expected_hours": ["00"]
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
├── orchestration/                 # Step Functions ASL workflow definition
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

The Gold deployment expects the Gold bucket and Glue database to exist. Gold
executions require `--PROCESS_DATE`; YouTube API Silver executions require both
`--PROCESS_DATE` and `--PROCESS_HOUR`.

Package the API ingestion Lambda when updating it independently:

```bash
./scripts/package_youtube_api_ingestion.sh
```

Before enabling orchestration, remove the API-category event notification from
the Bronze S3 bucket. Category processing is invoked synchronously by Step
Functions; leaving the old notification enabled would run it twice. Historical
Kaggle category files remain available through
`scripts/backfill_reference_to_silver.sh`.

Deploy the state machine with the schedule disabled for its first test:

```bash
export STATE_MACHINE_ROLE_NAME=yt-pipeline-step-functions-role-test
export ENABLE_SCHEDULE=false
./scripts/deploy_youtube_pipeline.sh
```

The default schedule is `00:10 UTC` each day. Keep `ENABLE_SCHEDULE=false` for
manual operation, or deploy with `ENABLE_SCHEDULE=true` when automatic daily
execution is required.

### Invocation permissions

Pipeline Lambda functions and Glue jobs are production worker components. The
CloudFormation workflow policy grants their invocation permissions to the Step
Functions execution role. A production operator role should receive Step
Functions start, inspect, and redrive permissions without direct Lambda or Glue
invocation permissions.

Direct Lambda testing remains appropriate in the test environment. The tester's
IAM role can retain `lambda:InvokeFunction` for the `-test` ingestion function
while the production operator role does not. An IAM denial returns
`AccessDeniedException` before Lambda code runs; denied attempts remain
auditable through CloudTrail.

## Manual batch execution

Run Silver for one complete API date:

```bash
aws glue start-job-run \
  --job-name yt-silver-video-transform \
  --arguments '{"--SOURCE":"youtube_api","--PROCESS_DATE":"2026-08-14","--PROCESS_HOUR":"00"}'
```

Run the data-quality gate after both Silver datasets are ready:

```bash
aws lambda invoke \
  --function-name yt-data-pipeline-data-quality-test \
  --cli-binary-format raw-in-base64-out \
  --payload '{"source":"youtube_api","process_date":"2026-08-14","regions":["ca","gb","us"],"expected_hours":["00"]}' \
  data-quality-result.json
```

Only after `quality_passed` is true, rebuild Gold for that date:

```bash
aws glue start-job-run \
  --job-name yt-gold-analytics \
  --arguments '{"--PROCESS_DATE":"2026-08-14"}'
```
