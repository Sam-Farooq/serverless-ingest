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

# Tests, caches and dist-info metadata are not needed at runtime and are a
# third of the unzipped size.
find "$BUILD_DIR" -type d -name "__pycache__" -prune -exec rm -rf {} +
find "$BUILD_DIR" -type d -name "*.dist-info" -prune -exec rm -rf {} +
find "$BUILD_DIR" -type d -name "tests" -prune -exec rm -rf {} +

( cd "$BUILD_DIR" && zip -qr "../../$ZIP_PATH" . )

echo "$ZIP_PATH  $(du -h "$ZIP_PATH" | cut -f1)  unzipped $(du -sh "$BUILD_DIR" | cut -f1)"
