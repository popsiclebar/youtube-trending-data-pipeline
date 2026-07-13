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
    |-- youtube/videos/source=.../region=...
    `-- youtube/categories/source=.../region=...
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
|-- data/                               # Sample Kaggle source files
|
|-- lambda/
|   |
|   |-- youtube_api_ingestion/          # YouTube API -> Bronze S3 JSON
|   |   `-- lambda_function.py
|   |
|   `-- json_to_parquet/                # Bronze CSV/JSON -> Silver Parquet
|       |-- Dockerfile                  # Container image for Lambda deployment
|       |-- lambda_function.py
|       `-- requirements.txt
|
|-- infra/
|   `-- cloudformation/
|       `-- json-to-parquet-lambda.yaml # Deploys the JSON-to-Parquet Lambda
|
|-- scripts/
|   |-- aws_copy.sh                     # Uploads Kaggle files to Bronze S3
|   `-- deploy_json_to_parquet.sh       # Builds/pushes/deploys Lambda image
|
`-- README.md
```

Implemented so far:

- Kaggle source files for 10 regions: CA, DE, FR, GB, IN, JP, KR, MX, RU, US.
- Parameterized upload script for loading Kaggle files into Bronze S3.
- YouTube API ingestion Lambda code for writing raw video/category JSON to
  Bronze S3. Deployment and scheduling are still manual/planned.
- Bronze-to-Silver Lambda for converting Kaggle CSV/JSON and YouTube API JSON
  into source-partitioned Silver Parquet.
- Container-based deployment path for the JSON-to-Parquet Lambda using Docker,
  Amazon ECR, and AWS CloudFormation.

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

JSON-to-Parquet Lambda:

```text
SILVER_BUCKET=<silver-s3-bucket>
RAW_PREFIX=youtube/raw
REFERENCE_PREFIX=youtube/raw_reference_data
API_VIDEOS_PREFIX=youtube/api_raw/videos
API_CATEGORIES_PREFIX=youtube/api_raw/categories
VIDEOS_OUTPUT_PREFIX=youtube/videos
CATEGORIES_OUTPUT_PREFIX=youtube/categories
```

## Deployment

The JSON-to-Parquet Lambda is deployed as a container image. This is useful for
data-processing dependencies such as `pandas` and `pyarrow`, which are easier to
package reliably in a Linux Lambda container than in a local zip file.

Prerequisites:

- AWS CLI configured with credentials and a default region.
- Docker running locally.
- Existing Bronze and Silver S3 buckets.

Deploy:

```bash
export AWS_REGION=eu-north-1
export BRONZE_BUCKET=<your-bronze-bucket-name>
export SILVER_BUCKET=<your-silver-bucket-name>

./scripts/deploy_json_to_parquet.sh
```

The deploy script:

- Creates the ECR repository if needed.
- Builds the Lambda container image.
- Pushes the image to Amazon ECR.
- Deploys or updates the CloudFormation stack.
- Configures Lambda environment variables and IAM permissions.

S3 ObjectCreated triggers and the YouTube API ingestion deployment will be moved
into Infrastructure as Code as the project matures. For now, those can be
configured manually while the AWS foundation is still evolving.

## Roadmap

- Add Infrastructure as Code for the YouTube API ingestion Lambda.
- Add EventBridge schedule for daily API ingestion.
- Add S3 event triggers for Bronze-to-Silver transformation.
- Add Glue Crawlers or Glue Catalog table definitions for Silver datasets.
- Add data quality checks before Gold processing.
- Build Gold aggregation jobs for trending, channel, and category analytics.
- Add Athena queries and optional QuickSight dashboarding.
