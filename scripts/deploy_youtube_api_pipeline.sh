#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_FILE="${ROOT_DIR}/infra/cloudformation/youtube-api-pipeline-state-machine.yaml"

STACK_NAME="${STACK_NAME:-yt-youtube-api-pipeline}"
STATE_MACHINE_NAME="${STATE_MACHINE_NAME:-yt-youtube-api-pipeline}"
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
YOUTUBE_API_INGESTION_FUNCTION="${YOUTUBE_API_INGESTION_FUNCTION:-yt-youtube-api-ingestion}"
SILVER_VIDEO_GLUE_JOB="${SILVER_VIDEO_GLUE_JOB:-yt-silver-video-transform}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
SCHEDULE_EXPRESSION="${SCHEDULE_EXPRESSION:-rate(1 day)}"
ENABLE_SCHEDULE="${ENABLE_SCHEDULE:-false}"

if [[ -z "${AWS_REGION}" ]]; then
  echo "AWS_REGION is required. Set AWS_REGION or configure a default AWS CLI region." >&2
  exit 1
fi

if [[ -z "${SNS_TOPIC_ARN}" ]]; then
  echo "SNS_TOPIC_ARN is required for pipeline notifications." >&2
  exit 1
fi

aws cloudformation deploy \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --template-file "${TEMPLATE_FILE}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    StateMachineName="${STATE_MACHINE_NAME}" \
    YouTubeApiIngestionFunctionName="${YOUTUBE_API_INGESTION_FUNCTION}" \
    SilverVideoGlueJobName="${SILVER_VIDEO_GLUE_JOB}" \
    SnsTopicArn="${SNS_TOPIC_ARN}" \
    ScheduleExpression="${SCHEDULE_EXPRESSION}" \
    EnableSchedule="${ENABLE_SCHEDULE}"

aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
