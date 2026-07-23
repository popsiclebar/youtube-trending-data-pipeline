#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GLUE_DIR="${ROOT_DIR}/glue/silver_video_transforms"
SCRIPT_FILE="${ROOT_DIR}/glue/silver_video_transforms/video_to_silver.py"
TEMPLATE_FILE="${ROOT_DIR}/infra/cloudformation/silver-video-glue-job.yaml"
BUILD_DIR="${ROOT_DIR}/build/silver_video_transforms"
MODULE_ZIP="${BUILD_DIR}/silver_video_transforms_modules.zip"

STACK_NAME="${STACK_NAME:-yt-silver-video-glue-job}"
JOB_NAME="${JOB_NAME:-yt-silver-video-transform}"
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
BRONZE_DATABASE="${BRONZE_DATABASE:-youtube_bronze}"
SILVER_DATABASE="${SILVER_DATABASE:-youtube_silver}"
SOURCE="${SOURCE:-all}"
KAGGLE_RAW_PREFIX="${KAGGLE_RAW_PREFIX:-youtube/raw}"
API_VIDEOS_PREFIX="${API_VIDEOS_PREFIX:-youtube/api_raw/videos}"
SILVER_VIDEOS_PREFIX="${SILVER_VIDEOS_PREFIX:-youtube/videos}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
MAX_INVALID_ROW_RATIO="${MAX_INVALID_ROW_RATIO:-0.05}"
GLUE_VERSION="${GLUE_VERSION:-4.0}"
WORKER_TYPE="${WORKER_TYPE:-G.1X}"
NUMBER_OF_WORKERS="${NUMBER_OF_WORKERS:-2}"
TIMEOUT_MINUTES="${TIMEOUT_MINUTES:-60}"

if [[ -z "${AWS_REGION}" ]]; then
  echo "AWS_REGION is required. Set AWS_REGION or configure a default AWS CLI region." >&2
  exit 1
fi

if [[ -z "${BRONZE_BUCKET:-}" ]]; then
  echo "BRONZE_BUCKET is required." >&2
  exit 1
fi

if [[ -z "${SILVER_BUCKET:-}" ]]; then
  echo "SILVER_BUCKET is required." >&2
  exit 1
fi

if [[ -z "${DEPLOYMENT_BUCKET:-}" ]]; then
  echo "DEPLOYMENT_BUCKET is required for uploading the Glue script." >&2
  exit 1
fi

if [[ -z "${CODE_TAG:-}" ]]; then
  if [[ -z "$(git -C "${ROOT_DIR}" status --porcelain)" ]]; then
    CODE_TAG="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)"
  else
    CODE_TAG="local-$(date +%Y%m%d%H%M%S)"
  fi
fi

SCRIPT_S3_KEY="glue/silver_video_transforms/${CODE_TAG}/video_to_silver.py"
EXTRA_PY_FILES_S3_KEY="glue/silver_video_transforms/${CODE_TAG}/silver_video_transforms_modules.zip"

mkdir -p "${BUILD_DIR}"
rm -f "${MODULE_ZIP}"

(
  cd "${GLUE_DIR}"
  zip -q "${MODULE_ZIP}" \
    config.py \
    quality.py \
    transforms.py
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
    BronzeBucketName="${BRONZE_BUCKET}" \
    SilverBucketName="${SILVER_BUCKET}" \
    BronzeDatabaseName="${BRONZE_DATABASE}" \
    SilverDatabaseName="${SILVER_DATABASE}" \
    Source="${SOURCE}" \
    KaggleRawPrefix="${KAGGLE_RAW_PREFIX}" \
    ApiVideosPrefix="${API_VIDEOS_PREFIX}" \
    SilverVideosPrefix="${SILVER_VIDEOS_PREFIX}" \
    SnsAlertTopicArn="${SNS_TOPIC_ARN}" \
    MaxInvalidRowRatio="${MAX_INVALID_ROW_RATIO}" \
    GlueVersion="${GLUE_VERSION}" \
    WorkerType="${WORKER_TYPE}" \
    NumberOfWorkers="${NUMBER_OF_WORKERS}" \
    TimeoutMinutes="${TIMEOUT_MINUTES}"

aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
