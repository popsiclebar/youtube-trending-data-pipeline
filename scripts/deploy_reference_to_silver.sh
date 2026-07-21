#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAMBDA_DIR="${ROOT_DIR}/lambda/reference_to_silver"
BUILD_DIR="${ROOT_DIR}/build/reference_to_silver"
ZIP_FILE="${ROOT_DIR}/build/reference_to_silver.zip"
TEMPLATE_FILE="${ROOT_DIR}/infra/cloudformation/reference-to-silver-lambda.yaml"

STACK_NAME="${STACK_NAME:-yt-reference-to-silver-lambda}"
FUNCTION_NAME="${FUNCTION_NAME:-yt-reference-to-silver}"
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
REFERENCE_PREFIX="${REFERENCE_PREFIX:-youtube/raw_reference_data}"
API_CATEGORIES_PREFIX="${API_CATEGORIES_PREFIX:-youtube/api_raw/categories}"
CATEGORIES_OUTPUT_PREFIX="${CATEGORIES_OUTPUT_PREFIX:-youtube/categories}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
LAMBDA_MEMORY_SIZE="${LAMBDA_MEMORY_SIZE:-1024}"
LAMBDA_TIMEOUT="${LAMBDA_TIMEOUT:-300}"

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
  echo "DEPLOYMENT_BUCKET is required for uploading the Lambda zip package." >&2
  exit 1
fi

if [[ -z "${AWS_SDK_PANDAS_LAYER_ARN:-}" ]]; then
  if [[ "${AWS_REGION}" == "eu-north-1" ]]; then
    AWS_SDK_PANDAS_LAYER_ARN="arn:aws:lambda:eu-north-1:336392948345:layer:AWSSDKPandas-Python312:29"
  else
    echo "AWS_SDK_PANDAS_LAYER_ARN is required outside eu-north-1." >&2
    exit 1
  fi
fi

if [[ -z "${CODE_TAG:-}" ]]; then
  if [[ -z "$(git -C "${ROOT_DIR}" status --porcelain)" ]]; then
    CODE_TAG="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)"
  else
    CODE_TAG="local-$(date +%Y%m%d%H%M%S)"
  fi
fi

CODE_S3_KEY="lambda/reference_to_silver/${CODE_TAG}.zip"

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
cp "${LAMBDA_DIR}"/*.py "${BUILD_DIR}/"

(
  cd "${BUILD_DIR}"
  zip -qr "${ZIP_FILE}" .
)

aws s3 cp "${ZIP_FILE}" "s3://${DEPLOYMENT_BUCKET}/${CODE_S3_KEY}" \
  --region "${AWS_REGION}"

aws cloudformation deploy \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --template-file "${TEMPLATE_FILE}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    FunctionName="${FUNCTION_NAME}" \
    CodeS3Bucket="${DEPLOYMENT_BUCKET}" \
    CodeS3Key="${CODE_S3_KEY}" \
    AwsSdkPandasLayerArn="${AWS_SDK_PANDAS_LAYER_ARN}" \
    BronzeBucketName="${BRONZE_BUCKET}" \
    SilverBucketName="${SILVER_BUCKET}" \
    ReferencePrefix="${REFERENCE_PREFIX}" \
    ApiCategoriesPrefix="${API_CATEGORIES_PREFIX}" \
    CategoriesOutputPrefix="${CATEGORIES_OUTPUT_PREFIX}" \
    SnsAlertTopicArn="${SNS_TOPIC_ARN}" \
    LambdaMemorySize="${LAMBDA_MEMORY_SIZE}" \
    LambdaTimeout="${LAMBDA_TIMEOUT}"

aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
