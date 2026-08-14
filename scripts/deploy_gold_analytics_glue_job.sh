#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GLUE_DIR="${ROOT_DIR}/glue/gold_analytics"
SCRIPT_FILE="${GLUE_DIR}/gold_analytics.py"
TEMPLATE_FILE="${ROOT_DIR}/infra/cloudformation/gold-analytics-glue-job.yaml"
BUILD_DIR="${ROOT_DIR}/build/gold_analytics"
MODULE_ZIP="${BUILD_DIR}/gold_analytics_modules.zip"

STACK_NAME="${STACK_NAME:-yt-gold-analytics-glue-job}"
JOB_NAME="${JOB_NAME:-yt-gold-analytics}"
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
SILVER_DATABASE="${SILVER_DATABASE:-yt-pipeline-silver-test}"
GOLD_DATABASE="${GOLD_DATABASE:-yt-pipeline-gold-test}"
VIDEO_TABLE="${VIDEO_TABLE:-clean_video_statistics}"
CATEGORY_TABLE="${CATEGORY_TABLE:-clean_category_data}"
TRENDING_TABLE="${TRENDING_TABLE:-trending_analytics}"
CHANNEL_TABLE="${CHANNEL_TABLE:-channel_analytics}"
CATEGORY_ANALYTICS_TABLE="${CATEGORY_ANALYTICS_TABLE:-category_analytics}"
GOLD_PREFIX="${GOLD_PREFIX:-youtube}"
PROCESS_DATE="${PROCESS_DATE:-}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
GLUE_VERSION="${GLUE_VERSION:-4.0}"
WORKER_TYPE="${WORKER_TYPE:-G.1X}"
NUMBER_OF_WORKERS="${NUMBER_OF_WORKERS:-2}"
TIMEOUT_MINUTES="${TIMEOUT_MINUTES:-30}"

if [[ -z "${AWS_REGION}" ]]; then
  echo "AWS_REGION is required. Set it or configure a default AWS CLI region." >&2
  exit 1
fi
if [[ -z "${SILVER_BUCKET:-}" ]]; then
  echo "SILVER_BUCKET is required." >&2
  exit 1
fi
if [[ -z "${GOLD_BUCKET:-}" ]]; then
  echo "GOLD_BUCKET is required." >&2
  exit 1
fi
if [[ -z "${DEPLOYMENT_BUCKET:-}" ]]; then
  echo "DEPLOYMENT_BUCKET is required for the Glue script and modules." >&2
  exit 1
fi

if [[ -z "${CODE_TAG:-}" ]]; then
  if [[ -z "$(git -C "${ROOT_DIR}" status --porcelain)" ]]; then
    CODE_TAG="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)"
  else
    CODE_TAG="local-$(date +%Y%m%d%H%M%S)"
  fi
fi

SCRIPT_S3_KEY="glue/gold_analytics/${CODE_TAG}/gold_analytics.py"
EXTRA_PY_FILES_S3_KEY="glue/gold_analytics/${CODE_TAG}/gold_analytics_modules.zip"

mkdir -p "${BUILD_DIR}"
rm -f "${MODULE_ZIP}"
(
  cd "${GLUE_DIR}"
  zip -q "${MODULE_ZIP}" config.py transforms.py writer.py
)

aws s3 cp "${SCRIPT_FILE}" "s3://${DEPLOYMENT_BUCKET}/${SCRIPT_S3_KEY}" \
  --region "${AWS_REGION}"
aws s3 cp "${MODULE_ZIP}" "s3://${DEPLOYMENT_BUCKET}/${EXTRA_PY_FILES_S3_KEY}" \
  --region "${AWS_REGION}"

aws cloudformation deploy \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --template-file "${TEMPLATE_FILE}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    JobName="${JOB_NAME}" \
    ScriptS3Bucket="${DEPLOYMENT_BUCKET}" \
    ScriptS3Key="${SCRIPT_S3_KEY}" \
    ExtraPyFilesS3Key="${EXTRA_PY_FILES_S3_KEY}" \
    SilverBucketName="${SILVER_BUCKET}" \
    GoldBucketName="${GOLD_BUCKET}" \
    SilverDatabaseName="${SILVER_DATABASE}" \
    GoldDatabaseName="${GOLD_DATABASE}" \
    VideoTableName="${VIDEO_TABLE}" \
    CategoryTableName="${CATEGORY_TABLE}" \
    TrendingTableName="${TRENDING_TABLE}" \
    ChannelTableName="${CHANNEL_TABLE}" \
    CategoryAnalyticsTableName="${CATEGORY_ANALYTICS_TABLE}" \
    GoldPrefix="${GOLD_PREFIX}" \
    ProcessDate="${PROCESS_DATE}" \
    SnsAlertTopicArn="${SNS_TOPIC_ARN}" \
    GlueVersion="${GLUE_VERSION}" \
    WorkerType="${WORKER_TYPE}" \
    NumberOfWorkers="${NUMBER_OF_WORKERS}" \
    TimeoutMinutes="${TIMEOUT_MINUTES}"

aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
