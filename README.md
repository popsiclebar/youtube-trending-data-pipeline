# YouTube Trending Data Pipeline

AWS data engineering project for ingesting YouTube trending data, storing raw
source data in an S3 Bronze layer, transforming it into standardized Silver
Parquet datasets, and preparing for Gold-layer business analytics.

The project is intentionally learning-focused, but follows production-style data
engineering patterns: source-specific raw storage, environment-based
configuration, repeatable deployment, partitioned datasets, and clear separation
between Bronze, Silver, and Gold responsibilities.

## Architecture

```text
Data Sources
|
|-- Kaggle YouTube Dataset              # Static historical CSV/JSON files
|-- YouTube Data API                    # Scheduled live API ingestion
|
`-- Bronze Layer: Amazon S3             # Raw source data, kept source-specific
    |
    |-- youtube/raw/...                 # Kaggle trending video CSV
    |-- youtube/raw_reference_data/...  # Kaggle category JSON
    |-- youtube/api_raw/videos/...      # YouTube API trending video JSON
    `-- youtube/api_raw/categories/...  # YouTube API category JSON
        |
        v
    Silver Layer: Amazon S3             # Cleansed, typed, standardized Parquet
    |
    |-- youtube/categories/source=.../region=...
    `-- youtube/videos/source=.../region=...       # Planned Glue output
        |
        v
    Data Quality Gate                   # Planned validation before Gold
        |
        v
    Gold Layer: Amazon S3               # Planned business aggregations
    |
    |-- trending_analytics
    |-- channel_analytics
    `-- category_analytics
        |
        v
    Analytics / Consumption             # Planned Athena and QuickSight access
```

Planned orchestration uses AWS Step Functions to coordinate ingestion, wait
states, Silver transformations, data quality checks, Gold aggregation, and SNS
notifications. CloudWatch, IAM, Glue Data Catalog, Athena, and QuickSight are
part of the target AWS architecture.

## Data Lake Layers

**Bronze**

Raw data exactly as received from each source. Kaggle files remain CSV/JSON, and
YouTube API responses remain JSON. Pipeline metadata is stored separately where
possible, not mixed into raw payloads.

**Silver**

Cleansed Parquet datasets with consistent column names, types, source
partitions, and region partitions. Silver is not the final analytics layer; it
is the standardized base for validation and downstream aggregation.

**Gold**

Planned curated business tables such as `trending_analytics`,
`channel_analytics`, and `category_analytics`. Gold will be built from Silver
using AWS Glue ETL jobs and queried through Athena or dashboards.

## Current Implementation

```text
youtube-trending-data-pipeline/
|
|-- data/
|   `-- kaggle/                        # Local copy of Kaggle source files
|
|-- lambda/
|   |
|   |-- youtube_api_ingestion/          # YouTube API -> Bronze S3 JSON
|   |   |-- config.py
|   |   |-- lambda_function.py
|   |   |-- storage.py
|   |   `-- youtube_api.py
|   |
|   `-- reference_to_silver/            # Category JSON -> Silver Parquet
|       |-- category_transform.py       # Category flattening and validation
|       |-- config.py                   # Environment variable settings
|       |-- lambda_function.py
|       `-- s3_io.py                    # S3 read/write helpers
|
|-- glue/                               # Planned heavier Silver/Gold ETL jobs
|   `-- silver_video_transforms/        # Planned CSV/API video cleansing jobs
|
|-- infra/
|   `-- cloudformation/
|       `-- reference-to-silver-lambda.yaml # Deploys category transform Lambda
|
|-- scripts/
|   |-- aws_copy.sh                     # Uploads Kaggle files to Bronze S3
|   |-- deploy_reference_to_silver.sh   # Deploys category Lambda zip + layer
|   `-- package_youtube_api_ingestion.sh # Builds ingestion Lambda zip
|
`-- README.md
```

Implemented so far:

- Kaggle source files for 10 regions: CA, DE, FR, GB, IN, JP, KR, MX, RU, US.
- Parameterized upload script for loading Kaggle files into Bronze S3.
- YouTube API ingestion Lambda code for writing raw video/category JSON to
  Bronze S3. Deployment and scheduling are still manual/planned.
- Reference-to-Silver Lambda for converting Kaggle and YouTube API category JSON
  into source-partitioned Silver Parquet.
- Zip-based deployment path for the Reference-to-Silver Lambda using the AWS SDK
  for pandas managed Lambda layer.
- Video Silver transformations are planned for AWS Glue instead of Lambda.

## Bronze And Silver Layout

Bronze input layout:

```text
s3://<bronze-bucket>/youtube/raw/region=<region>/...
s3://<bronze-bucket>/youtube/raw_reference_data/region=<region>/...
s3://<bronze-bucket>/youtube/api_raw/videos/region=<region>/date=<yyyy-mm-dd>/hour=<hh>/...
s3://<bronze-bucket>/youtube/api_raw/categories/region=<region>/date=<yyyy-mm-dd>/...
```

Silver output layout:

```text
s3://<silver-bucket>/youtube/videos/source=kaggle/region=<region>/...
s3://<silver-bucket>/youtube/videos/source=youtube_api/region=<region>/...
s3://<silver-bucket>/youtube/categories/source=kaggle/region=<region>/...
s3://<silver-bucket>/youtube/categories/source=youtube_api/region=<region>/...
```

The `source` partition keeps lineage visible in Silver while allowing downstream
Glue and Athena jobs to compare or combine sources intentionally.

## Glue Catalog And Athena Strategy

AWS Glue Crawlers can be used to register tables for each data lake layer:

- Bronze tables expose raw source data for inspection, debugging, and lineage.
- Silver tables expose cleaned Parquet data for most analytical SQL.
- Gold tables expose curated business metrics for dashboards and reporting.

Bronze tables are useful, but they should not replace Silver transforms. For
example, a Bronze Crawler can make Kaggle CSV queryable in Athena, but the data
is still raw CSV. The Silver layer converts it to typed Parquet, standardizes
columns across Kaggle and YouTube API sources, and gives downstream jobs a more
stable contract. Category/reference JSON is small enough for Lambda; heavier
video CSV/API transformations are planned for AWS Glue.

## Configuration

The Lambda functions use environment variables instead of hard-coded resource
names or secrets.

YouTube API ingestion Lambda:

```text
BRONZE_BUCKET=<bronze-s3-bucket>
YOUTUBE_API_KEY=<youtube-data-api-key>
YOUTUBE_API_KEY_SECRET_ID=<optional-secrets-manager-secret-id>
REGION_CODES=CA,DE,FR,GB,IN,JP,KR,MX,RU,US
MAX_RESULTS=50
SNS_TOPIC_ARN=<optional-sns-topic-arn>
```

Reference-to-Silver Lambda:

```text
SILVER_BUCKET=<silver-s3-bucket>
REFERENCE_PREFIX=youtube/raw_reference_data
API_CATEGORIES_PREFIX=youtube/api_raw/categories
CATEGORIES_OUTPUT_PREFIX=youtube/categories
SNS_TOPIC_ARN=<optional-sns-topic-arn>
```

Silver Parquet objects also include S3 object metadata such as source, dataset,
region, record count, ingestion timestamp, and the original Bronze object. This
keeps operational lineage available without adding pipeline-only columns to the
analytical tables.

## Deployment

The Reference-to-Silver Lambda is deployed as a zip package with the AWS SDK for
pandas managed Lambda layer. The zip contains only project code; the layer
provides `pandas` and `pyarrow`.

Prerequisites:

- AWS CLI configured with credentials and a default region.
- Existing Bronze and Silver S3 buckets.
- An S3 deployment bucket for uploading the Lambda zip package.
- The AWS SDK for pandas layer ARN for the Lambda runtime and region.

Deploy:

```bash
export AWS_REGION=eu-north-1
export BRONZE_BUCKET=<your-bronze-bucket-name>
export SILVER_BUCKET=<your-silver-bucket-name>
export DEPLOYMENT_BUCKET=<your-deployment-artifacts-bucket>

./scripts/deploy_reference_to_silver.sh
```

The deploy script:

- Builds a zip from `lambda/reference_to_silver/`.
- Uploads the zip to the deployment bucket.
- Deploys or updates the CloudFormation stack.
- Attaches the AWS SDK for pandas Lambda layer.
- Configures Lambda environment variables and IAM permissions.

By default, the script uses the Python 3.12 x86_64 AWS SDK for pandas layer ARN
for `eu-north-1`:

```text
arn:aws:lambda:eu-north-1:336392948345:layer:AWSSDKPandas-Python312:29
```

Set `AWS_SDK_PANDAS_LAYER_ARN` if you use a different region, runtime, or
architecture.

The YouTube API ingestion Lambda currently uses a zip package because it only
needs Python standard library modules plus `boto3`, which is already available in
the Lambda runtime. Package the full folder, not only `lambda_function.py`:

```bash
./scripts/package_youtube_api_ingestion.sh
```

Then upload `build/youtube_api_ingestion.zip` in the Lambda console, or with the
AWS CLI:

```bash
aws lambda update-function-code \
  --function-name <your-youtube-api-ingestion-function-name> \
  --zip-file fileb://build/youtube_api_ingestion.zip
```

Keep the Lambda handler set to `lambda_function.lambda_handler`. The extra files
are imported by `lambda_function.py` because they are packaged in the same zip.

S3 ObjectCreated triggers and the YouTube API ingestion infrastructure will be
moved into Infrastructure as Code as the project matures. For now, those can be
configured manually while the AWS foundation is still evolving.

## Roadmap

- Add Infrastructure as Code for the YouTube API ingestion Lambda.
- Add EventBridge schedule for daily API ingestion.
- Add S3 event triggers for Reference-to-Silver transformation.
- Add Glue Crawlers or Glue Catalog table definitions for Silver datasets.
- Add AWS Glue ETL jobs for Silver video transforms.
- Add data quality checks before Gold processing.
- Build Gold aggregation jobs for trending, channel, and category analytics.
- Add Athena queries and optional QuickSight dashboarding.
