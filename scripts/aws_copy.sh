#!/usr/bin/env bash
set -euo pipefail

# By default this script reads from data/kaggle.
# BRONZE_BUCKET=<your-bronze-bucket-name> ./scripts/aws_copy.sh

if [[ -z "${BRONZE_BUCKET:-}" ]]; then
  echo "BRONZE_BUCKET is required." >&2
  exit 1
fi

regions=(ca de fr gb in jp kr mx ru us)
data_dir="${DATA_DIR:-data/kaggle}"

for region in "${regions[@]}"; do
  upper_region="$(tr "[:lower:]" "[:upper:]" <<< "${region}")"

  aws s3 cp \
    "${data_dir}/${upper_region}videos.csv" \
    "s3://${BRONZE_BUCKET}/youtube/raw/region=${region}/"

  aws s3 cp \
    "${data_dir}/${upper_region}_category_id.json" \
    "s3://${BRONZE_BUCKET}/youtube/raw_reference_data/region=${region}/"
done
