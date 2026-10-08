#!/usr/bin/env bash
# Build the deployment zip that infra/lambda.tf uploads.
#
# The dependencies are fetched for the target platform rather than this one.
# psycopg and pydantic-core are compiled, so a zip built from a developer's
# own site-packages installs a macOS wheel into an arm64 Linux runtime and
# fails at import with an error that names neither.
set -euo pipefail

cd "$(dirname "$0")/.."

PLATFORM="manylinux2014_aarch64"
PYTHON_VERSION="3.11"
BUILD_DIR="build/package"
ZIP_PATH="build/ingest.zip"

rm -rf "$BUILD_DIR" "$ZIP_PATH"
mkdir -p "$BUILD_DIR"

python3.11 -m pip install \
  --quiet \
  --target "$BUILD_DIR" \
  --platform "$PLATFORM" \
  --python-version "$PYTHON_VERSION" \
  --implementation cp \
  --only-binary=:all: \
  --no-compile \
  "boto3>=1.35.0" "psycopg[binary]>=3.2.0" "pydantic>=2.8.0" "pydantic-settings>=2.4.0"

cp -R src/handlers src/ingest "$BUILD_DIR/"

# Only the caches. --no-compile should mean there are none, and a stray one
# from a rebuild is worth removing anyway.
#
# The dist-info directories stay. Deleting them saves 0.7 MiB of the 42 and it
# breaks any dependency that reads its own version through importlib.metadata
# at import time, which fails nowhere except in the runtime. Under a megabyte
# is not worth a failure mode that cannot be reproduced on the machine that
# built the zip.
find "$BUILD_DIR" -type d -name "__pycache__" -prune -exec rm -rf {} +

( cd "$BUILD_DIR" && zip -qr "../../$ZIP_PATH" . )

# Sizes are the sum of file sizes, because that is what Lambda's 250 MB
# unzipped limit counts. du answers in 4 KiB disk blocks instead, and this tree
# is thousands of small botocore JSON files, so du overstates it by about 7 MiB
# and does not even agree with itself: 48M from -sh, 49 from -sm.
content_mib() {
  find "$1" -type f -exec cat {} + | wc -c | awk '{printf "%.0f", $1 / 1048576}'
}

echo "$ZIP_PATH  $(content_mib "$ZIP_PATH") MiB zipped  $(content_mib "$BUILD_DIR") MiB unzipped"
