#!/usr/bin/env bash
# 用 InnoDB 作为对照：同样的表/数据/查询在两个引擎上执行，输出必须逐字一致。
# 用法: scripts/compare_innodb.sh <mysql客户端路径> <socket>
set -uo pipefail
MYSQL=${1:?mysql client}; SOCK=${2:?socket}
ROOT=$(cd "$(dirname "$0")" && pwd)
OUT=$(mktemp -d)
run() { "$MYSQL" --no-defaults -uroot -S"$SOCK" -t -f "$@" 2>&1; }
for eng in InnoDB MyEngine; do
  db="cmp_$eng"
  run -e "DROP DATABASE IF EXISTS $db; CREATE DATABASE $db" >/dev/null
  sed "s/__ENGINE__/$eng/g" "$ROOT/compare.sql" | run "$db" > "$OUT/$eng.txt"
done
# 防止"两边都连不上服务、输出相同"被误判为通过
for eng in InnoDB MyEngine; do
  if grep -qE "ERROR 20(02|03|13)" "$OUT/$eng.txt" || [ "$(wc -l < "$OUT/$eng.txt")" -lt 100 ]; then
    echo "FAIL: $eng output looks invalid (connection error or too short):"; head -5 "$OUT/$eng.txt"; exit 2
  fi
done
if diff -u "$OUT/InnoDB.txt" "$OUT/MyEngine.txt" > "$OUT/diff.txt"; then
  echo "PASS: $(grep -c '^+--' "$OUT/InnoDB.txt" 2>/dev/null) result sets identical ($(wc -l < "$OUT/InnoDB.txt") lines)"
else
  echo "FAIL: differences between InnoDB and MyEngine:"; head -80 "$OUT/diff.txt"; exit 1
fi
