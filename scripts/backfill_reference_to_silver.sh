#!/usr/bin/env bash
set -euo pipefail

# Invoke the Reference-to-Silver Lambda for existing Bronze category JSON files.
# This is useful when files were uploaded before the S3 trigger was configured.

FUNCTION_NAME="${FUNCTION_NAME:-yt-reference-to-silver}"
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
REFERENCE_PREFIX="${REFERENCE_PREFIX:-youtube/kaggle_raw/raw_reference_data}"
DRY_RUN="${DRY_RUN:-false}"

if [[ -z "${AWS_REGION}" ]]; then
  echo "AWS_REGION is required. Set AWS_REGION or configure a default AWS CLI region." >&2
  exit 1
fi

if [[ -z "${BRONZE_BUCKET:-}" ]]; then
  echo "BRONZE_BUCKET is required." >&2
  exit 1
fi

prefix="${REFERENCE_PREFIX%/}/"
echo "Scanning s3://${BRONZE_BUCKET}/${prefix}"

keys="$(
  aws s3api list-objects-v2 \
    --region "${AWS_REGION}" \
    --bucket "${BRONZE_BUCKET}" \
    --prefix "${prefix}" \
    --query "Contents[].Key" \
    --output text
)"

if [[ -z "${keys}" || "${keys}" == "None" ]]; then
  echo "No objects found under s3://${BRONZE_BUCKET}/${prefix}"
  exit 0
fi

for key in ${keys}; do
  if [[ "${key}" != *.json ]]; then
    continue
  fi

  echo "Invoking ${FUNCTION_NAME} for s3://${BRONZE_BUCKET}/${key}"

  if [[ "${DRY_RUN}" == "true" ]]; then
    continue
  fi

  payload="$(
    python3 -c '
import json
import sys

bucket = sys.argv[1]
key = sys.argv[2]

print(json.dumps({
    "Records": [
        {
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key},
            }
        }
    ]
}))
' "${BRONZE_BUCKET}" "${key}"
  )"

  response_file="$(mktemp)"
  aws lambda invoke \
    --region "${AWS_REGION}" \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload "${payload}" \
    "${response_file}" \
    --output text >/dev/null

  cat "${response_file}"
  echo
  rm -f "${response_file}"
done
