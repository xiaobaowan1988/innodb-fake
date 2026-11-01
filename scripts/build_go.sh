#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../go"
go build -buildmode=c-shared -o libmyengine.so myengine.go
echo "built: $(pwd)/libmyengine.so, libmyengine.h"
