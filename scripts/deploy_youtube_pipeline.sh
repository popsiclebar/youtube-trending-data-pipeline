#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_FILE="${ROOT_DIR}/infra/cloudformation/youtube-pipeline-state-machine.yaml"
DEFINITION_FILE="${ROOT_DIR}/orchestration/youtube_pipeline.asl.json"

STACK_NAME="${STACK_NAME:-yt-youtube-pipeline}"
STATE_MACHINE_NAME="${STATE_MACHINE_NAME:-yt-youtube-pipeline}"
STATE_MACHINE_ROLE_NAME="${STATE_MACHINE_ROLE_NAME:-yt-pipeline-step-functions-role-test}"
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
YOUTUBE_API_INGESTION_FUNCTION="${YOUTUBE_API_INGESTION_FUNCTION:-yt-youtube-api-ingestion}"
CATEGORY_TRANSFORM_FUNCTION="${CATEGORY_TRANSFORM_FUNCTION:-yt-reference-to-silver}"
DATA_QUALITY_FUNCTION="${DATA_QUALITY_FUNCTION:-yt-data-pipeline-data-quality-test}"
SILVER_VIDEO_GLUE_JOB="${SILVER_VIDEO_GLUE_JOB:-yt-silver-video-transform}"
GOLD_GLUE_JOB="${GOLD_GLUE_JOB:-yt-gold-analytics}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
SCHEDULE_EXPRESSION="${SCHEDULE_EXPRESSION:-cron(10 0 * * ? *)}"
ENABLE_SCHEDULE="${ENABLE_SCHEDULE:-false}"

if [[ -z "${AWS_REGION}" ]]; then
  echo "AWS_REGION is required. Set AWS_REGION or configure a default AWS CLI region." >&2
  exit 1
fi

if [[ -z "${DEPLOYMENT_BUCKET:-}" ]]; then
  echo "DEPLOYMENT_BUCKET is required for the Step Functions definition." >&2
  exit 1
fi

if [[ -z "${SNS_TOPIC_ARN}" ]]; then
  echo "SNS_TOPIC_ARN is required for pipeline notifications." >&2
  exit 1
fi

if [[ -z "${CODE_TAG:-}" ]]; then
  if [[ -z "$(git -C "${ROOT_DIR}" status --porcelain)" ]]; then
    CODE_TAG="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)"
  else
    CODE_TAG="local-$(date +%Y%m%d%H%M%S)"
  fi
fi

DEFINITION_S3_KEY="step-functions/youtube_pipeline/${CODE_TAG}.asl.json"

aws iam get-role \
  --role-name "${STATE_MACHINE_ROLE_NAME}" \
  --query "Role.Arn" \
  --output text >/dev/null

aws s3 cp "${DEFINITION_FILE}" "s3://${DEPLOYMENT_BUCKET}/${DEFINITION_S3_KEY}" \
  --region "${AWS_REGION}"

aws cloudformation deploy \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --template-file "${TEMPLATE_FILE}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    StateMachineName="${STATE_MACHINE_NAME}" \
    StateMachineRoleName="${STATE_MACHINE_ROLE_NAME}" \
    DefinitionS3Bucket="${DEPLOYMENT_BUCKET}" \
    DefinitionS3Key="${DEFINITION_S3_KEY}" \
    YouTubeApiIngestionFunctionName="${YOUTUBE_API_INGESTION_FUNCTION}" \
    CategoryTransformFunctionName="${CATEGORY_TRANSFORM_FUNCTION}" \
    DataQualityFunctionName="${DATA_QUALITY_FUNCTION}" \
    SilverVideoGlueJobName="${SILVER_VIDEO_GLUE_JOB}" \
    GoldGlueJobName="${GOLD_GLUE_JOB}" \
    SnsTopicArn="${SNS_TOPIC_ARN}" \
    ScheduleExpression="${SCHEDULE_EXPRESSION}" \
    EnableSchedule="${ENABLE_SCHEDULE}"

aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
