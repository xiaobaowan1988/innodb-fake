#!/usr/bin/env bash
# 本地迭代：重编 Go 库与插件 -> 部署 -> 重启 mysqld -> 重新 INSTALL PLUGIN
# 路径可用环境变量覆盖。
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD=${MYBUILD:-/opt/mybuild}; DATA=${MYDATA:-/opt/mydata}; PLUGDIR=${MYPLUGIN:-/opt/myplugin}
SOCK=${MYSOCK:-/tmp/my.sock}
"$ROOT/scripts/build_go.sh"
(cd "$BUILD" && make myengine 2>&1 | grep -E "error|Built target myengine")
pkill mysqld 2>/dev/null && sleep 3 || true
mkdir -p "$PLUGDIR"
cp "$BUILD/plugin_output_directory/ha_myengine.so" "$ROOT/go/libmyengine.so" "$PLUGDIR/"
export LD_LIBRARY_PATH=$PLUGDIR
nohup "$BUILD/runtime_output_directory/mysqld" --no-defaults --console --user=mysql --datadir="$DATA" \
  --basedir="$BUILD" --lc-messages-dir="$BUILD/share" --plugin-dir="$PLUGDIR" --socket="$SOCK" --port=3399 \
  --mysqlx=OFF > "$BUILD/server.log" 2>&1 &
timeout 90 bash -c "until grep -q 'ready for connections' $BUILD/server.log; do sleep 2; done"
M="$BUILD/runtime_output_directory/mysql --no-defaults -uroot -S$SOCK"
$M -e "UNINSTALL PLUGIN myengine" 2>/dev/null || true
$M -e "INSTALL PLUGIN myengine SONAME 'ha_myengine.so'"
echo "mysqld restarted, plugin installed"
