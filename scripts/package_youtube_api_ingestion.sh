#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAMBDA_DIR="${ROOT_DIR}/lambda/youtube_api_ingestion"
BUILD_DIR="${ROOT_DIR}/build/youtube_api_ingestion"
ZIP_FILE="${ROOT_DIR}/build/youtube_api_ingestion.zip"

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

cp "${LAMBDA_DIR}"/*.py "${BUILD_DIR}/"

(
  cd "${BUILD_DIR}"
  zip -qr "${ZIP_FILE}" .
)

echo "Created ${ZIP_FILE}"
echo "Lambda handler: lambda_function.lambda_handler"
