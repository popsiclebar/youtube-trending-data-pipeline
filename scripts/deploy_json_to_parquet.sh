#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAMBDA_DIR="${ROOT_DIR}/lambda/json_to_parquet"
TEMPLATE_FILE="${ROOT_DIR}/infra/cloudformation/json-to-parquet-lambda.yaml"

STACK_NAME="${STACK_NAME:-yt-json-to-parquet-lambda}"
FUNCTION_NAME="${FUNCTION_NAME:-yt-json-to-parquet}"
ECR_REPOSITORY="${ECR_REPOSITORY:-yt-json-to-parquet}"
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
RAW_PREFIX="${RAW_PREFIX:-youtube/raw}"
REFERENCE_PREFIX="${REFERENCE_PREFIX:-youtube/raw_reference_data}"
VIDEOS_OUTPUT_PREFIX="${VIDEOS_OUTPUT_PREFIX:-youtube/videos}"
CATEGORIES_OUTPUT_PREFIX="${CATEGORIES_OUTPUT_PREFIX:-youtube/categories}"
LAMBDA_MEMORY_SIZE="${LAMBDA_MEMORY_SIZE:-1024}"
LAMBDA_TIMEOUT="${LAMBDA_TIMEOUT:-300}"

if [[ -z "${IMAGE_TAG:-}" ]]; then
  if [[ -z "$(git -C "${ROOT_DIR}" status --porcelain)" ]]; then
    IMAGE_TAG="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)"
  else
    IMAGE_TAG="local-$(date +%Y%m%d%H%M%S)"
  fi
fi

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

AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ECR_URI="${ECR_REGISTRY}/${ECR_REPOSITORY}"
IMAGE_URI="${ECR_URI}:${IMAGE_TAG}"

echo "Deploying ${FUNCTION_NAME} to ${AWS_REGION}"
echo "Image: ${IMAGE_URI}"

if ! aws ecr describe-repositories \
  --region "${AWS_REGION}" \
  --repository-names "${ECR_REPOSITORY}" >/dev/null 2>&1; then
  aws ecr create-repository \
    --region "${AWS_REGION}" \
    --repository-name "${ECR_REPOSITORY}" >/dev/null
fi

aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

docker build \
  --platform linux/amd64 \
  --tag "${IMAGE_URI}" \
  "${LAMBDA_DIR}"

docker push "${IMAGE_URI}"

aws cloudformation deploy \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --template-file "${TEMPLATE_FILE}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    FunctionName="${FUNCTION_NAME}" \
    ImageUri="${IMAGE_URI}" \
    BronzeBucketName="${BRONZE_BUCKET}" \
    SilverBucketName="${SILVER_BUCKET}" \
    RawPrefix="${RAW_PREFIX}" \
    ReferencePrefix="${REFERENCE_PREFIX}" \
    VideosOutputPrefix="${VIDEOS_OUTPUT_PREFIX}" \
    CategoriesOutputPrefix="${CATEGORIES_OUTPUT_PREFIX}" \
    LambdaMemorySize="${LAMBDA_MEMORY_SIZE}" \
    LambdaTimeout="${LAMBDA_TIMEOUT}"

aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
