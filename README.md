# youtube-trending-data-pipeline

An AWS-based data engineering side project for building a YouTube Trending Data
Pipeline from raw source files to analytics-ready datasets.

The project is designed as a learning-focused implementation of a modern data
lake pattern. The target architecture follows a Bronze, Silver, and Gold
structure, with AWS services used for ingestion, transformation, data quality,
orchestration, monitoring, and analytics.

This repository is still in progress. The current implementation starts with
local source files and a Bronze-to-Silver Lambda transformation, while the full
AWS setup will be added gradually.

## Planned Architecture

```text
Data Sources
|
|-- Kaggle YouTube Dataset              # Current local source data
|-- YouTube Data API                    # Planned scheduled ingestion source
|
`-- Ingestion
    |
    |-- Python upload scripts           # Load local files to S3
    |-- Amazon EventBridge              # Planned scheduled/event trigger
    |
    `-- Amazon S3 Bronze                # Raw CSV/JSON files by region
        |
        |-- AWS Glue Crawler            # Discover raw schemas
        |
        `-- Silver Transformations
            |
            |-- AWS Lambda              # JSON category mapping to Parquet
            |-- AWS Glue ETL            # Larger CSV/JSON cleansing jobs
            |
            `-- Amazon S3 Silver        # Cleaned typed Parquet datasets
                |
                |-- AWS Glue Data Catalog
                |
                `-- Data Quality Gate
                    |
                    |-- AWS Lambda      # Validation checks
                    |-- Amazon SNS      # Failure alerts
                    |
                    `-- Gold Aggregation
                        |
                        |-- AWS Glue ETL
                        |-- trending_analytics
                        |-- channel_analytics
                        |-- category_analytics
                        |
                        `-- Amazon S3 Gold
                            |
                            |-- AWS Glue Data Catalog
                            |-- Amazon Athena
                            `-- Amazon QuickSight
```

AWS Step Functions is planned as the orchestration layer for ingestion,
parallel Silver transformations, quality checks, Gold aggregation, and pipeline
notifications. IAM, CloudWatch, and SNS will support permissions, monitoring,
logging, and alerts across the workflow.

## Planned Project Structure

```text
youtube-trending-data-pipeline/
|
|-- data/                               # Sample source data for local exploration
|
|-- lambda/                             # Event-driven Lambda functions
|   |
|   |-- youtube_api_ingestion/          # Planned ingestion Lambda
|   |   `-- lambda_function.py          # Fetches trending videos/categories
|   |
|   |-- json_to_parquet/                # Bronze-to-Silver transformation Lambda
|   |   |-- Dockerfile                  # Container image build for AWS Lambda
|   |   |-- lambda_function.py          # Converts raw JSON/CSV to Parquet
|   |   `-- requirements.txt            # Lambda packaging dependencies
|   |
|   |-- data_quality/                   # Planned validation Lambda
|   |   `-- lambda_function.py          # Checks Silver data before Gold jobs
|   |
|   `-- notifications/                  # Planned alerting Lambda
|       `-- lambda_function.py          # Sends pipeline success/failure messages
|
|-- glue/                               # AWS Glue jobs, crawlers, and catalog setup
|   |
|   |-- jobs/                           # Cleansing and aggregation ETL scripts
|   |-- crawlers/                       # Bronze/Silver/Gold crawler definitions
|   `-- catalog/                        # Glue database/table definitions
|
|-- athena/                             # Athena DDL, views, and analytics queries
|   |
|   |-- ddl/                            # External table definitions
|   |-- views/                          # Curated query views
|   `-- queries/                        # Analysis queries for the Gold layer
|
|-- step_functions/                     # Pipeline orchestration definitions
|   `-- youtube_pipeline.asl.json       # Planned state machine workflow
|
|-- scripts/                            # Local and operational helper scripts
|   |-- aws_copy.sh                     # Uploads local source files to Bronze S3
|   `-- deploy_json_to_parquet.sh       # Builds and deploys the Lambda image
|
|-- infra/                              # Planned Infrastructure as Code
|   `-- cloudformation/                 # AWS CloudFormation templates
|
|-- docs/                               # Architecture notes and runbooks
|-- README.md                           # Project overview
`-- .gitignore                          # Local files excluded from Git
```

## Current Progress

- Added sample Kaggle YouTube trending source files.
- Added a helper upload script for loading raw files into the Bronze S3 layout.
- Added an initial Lambda function that converts raw CSV and category JSON files
  into Silver Parquet outputs.
- Added environment-based Lambda configuration so bucket names and path prefixes
  are not hard-coded into the function logic.
- Added a container-based deployment path for the JSON-to-Parquet Lambda using
  Docker, Amazon ECR, and AWS CloudFormation.

## Deploy JSON-to-Parquet Lambda

The deployment path uses a Lambda container image. This avoids
manually uploading code in the AWS console and handles heavier dependencies such
as `pandas` and `pyarrow` more reliably.

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

The deployment script will:

- Create an ECR repository if it does not exist.
- Build the Lambda container image from `lambda/json_to_parquet/Dockerfile`.
- Push the image to Amazon ECR.
- Deploy or update the CloudFormation stack.
- Configure Lambda environment variables such as `SILVER_BUCKET`,
  `RAW_PREFIX`, and `REFERENCE_PREFIX`.
- Create the Lambda IAM role with read access to the Bronze prefixes and write
  access to the Silver bucket.

After deployment, connect the Bronze S3 bucket ObjectCreated event to the Lambda
function. This trigger will be moved into Infrastructure as Code later when the
S3 bucket resources are also managed by the project.
