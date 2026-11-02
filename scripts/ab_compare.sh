#!/usr/bin/env bash
# 同一台机器、同一时间，对不同提交各自构建插件并并排压测（SQL 层 A/B）。
# 用法: ab_compare.sh build <commit|WORKTREE> <标签>   构建该提交的 Go 库和插件到 /opt/ab/<标签>
#       ab_compare.sh run <标签> <bench.py 参数...>    用该插件启动 mysqld 并跑压测（sync_binlog=0）
# 为什么需要它：机器噪声约 ±10~30%，不同时间点测出的数字不可比；之前"二级索引扫描慢了 26%"就是靠它定位到
# 回归来自树的细粒度加锁（B1），而不是后面的步骤。
set -euo pipefail
REPO=/home/user/innodb-fake; B=/opt/mybuild; SRC=/opt/mysrc/mysql-8.0.40
cmd=$1; shift
if [ "$cmd" = build ]; then
  c=$1; tag=$2; out=/opt/ab/$tag; mkdir -p $out
  if [ "$c" = WORKTREE ]; then wt=$REPO; else wt=/tmp/wt_$tag; rm -rf $wt; git -C $REPO worktree add -q $wt $c; fi
  (cd $wt/go && go build -buildmode=c-shared -o libmyengine.so ./)
  ln -sfn $wt/storage/myengine $SRC/storage/myengine
  (cd $B && cmake -DMYENGINE_GO_DIR=$wt/go . >/dev/null && make myengine 2>&1 | grep -E "error|Built target myengine")
  cp $B/plugin_output_directory/ha_myengine.so $wt/go/libmyengine.so $out/
  ln -sfn $REPO/storage/myengine $SRC/storage/myengine
  (cd $B && cmake -DMYENGINE_GO_DIR=$REPO/go . >/dev/null)
  echo "built $tag from $c"
else
  tag=$1; shift; out=/opt/ab/$tag
  pkill -x mysqld 2>/dev/null || true; for i in $(seq 1 30); do pgrep -x mysqld >/dev/null || break; sleep 1; done
  export LD_LIBRARY_PATH=$out
  nohup $B/runtime_output_directory/mysqld --no-defaults --console --user=mysql --datadir=/opt/mydata --basedir=$B --lc-messages-dir=$B/share --plugin-dir=$out --socket=/tmp/my.sock --port=3399 --mysqlx=OFF > $B/server.log 2>&1 &
  timeout 90 bash -c "until grep -q 'ready for connections' $B/server.log; do sleep 2; done"
  M="$B/runtime_output_directory/mysql --no-defaults -uroot -S/tmp/my.sock"
  $M -e "UNINSTALL PLUGIN myengine" 2>/dev/null || true
  $M -e "INSTALL PLUGIN myengine SONAME 'ha_myengine.so'"
  $M -e "SET GLOBAL sync_binlog=0"
  $REPO/scripts/bench.py --socket /tmp/my.sock "$@" 2>&1 | grep MyEngine | sed 's/ p50.*cpu=/ cpu=/' | sed "s/^/[$tag] /"
  $M -e "SET GLOBAL sync_binlog=1" || true
fi
