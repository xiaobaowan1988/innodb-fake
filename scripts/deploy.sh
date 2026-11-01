#!/usr/bin/env bash
# 用法: scripts/deploy.sh <MYSQL_BUILD_DIR> <PLUGIN_DIR>
set -euo pipefail
BUILD=${1:?mysql build dir}; PLUGIN_DIR=${2:?plugin_dir}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cp "$BUILD/plugin_output_directory/ha_myengine.so" "$PLUGIN_DIR/"
cp "$ROOT/go/libmyengine.so" "$PLUGIN_DIR/"
echo "export LD_LIBRARY_PATH=$PLUGIN_DIR:\${LD_LIBRARY_PATH:-}"
echo "mysqld --console --user=mysql"
