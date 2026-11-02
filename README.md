# MyEngine：MySQL -> C++ Handler -> CGO -> Go（内存 B+ 树存储引擎）

- `go/btree.go`：内存 B+ 树（叶子双向链表、分裂/借位/合并），`go test ./go` 与排序切片做随机对照。
- `go/myengine.go`：CGO 导出接口 `MyEngine_Create/Open/Drop/Rename/Count/Put/Get/Delete/Cursor*`。
- `storage/myengine/`：MySQL 8.0 插件（`ha_myengine.{h,cc}` + `CMakeLists.txt`）。
- `scripts/`：`build_go.sh`、`restart.sh`（重编+部署+重启+安装插件）、`compare_innodb.sh`（与 InnoDB 对照）、`verify.sql`。

## 数据布局
- 主索引：主键编码 -> 整行 `record[0]` 字节；二级索引：索引键编码+主键编码 -> 空。
- 键编码：每个 key part = [可空则 1 字节标记] + `Field::make_sort_key` 定宽输出，可直接 memcmp 比较。
- 游标按 key 重新定位（`Step` = UpperBound/LastLess），扫描期间增删不会使游标失效。

## 限制
必须有主键；不支持 BLOB/TEXT、前缀索引、降序索引、事务；**并发语句之间没有互斥，存在丢失更新、UNIQUE 被破坏、自增重复、二级索引读到中间状态等逻辑问题，见 `docs/concurrency-issues.md`**；**仅内存，mysqld 重启后表结构还在但数据为空**。

## 使用
1. `scripts/build_go.sh`（只改 Go：几秒）；改 C++ 后 `cd <build> && make myengine`（几秒）
2. `scripts/restart.sh`
3. `scripts/compare_innodb.sh <mysql客户端> <socket>`：147 个结果集与 InnoDB 逐字一致即 PASS
4. `MYENGINE_QUIET=1` 关闭 C++ 调用轨迹，`MYENGINE_VERBOSE=1` 打开 Go 日志（含 B+ 树每次查找从根到叶的逐层选择）

## 构建环境（已在 MySQL 8.0.40 上验证）
`/opt/mysrc/mysql-8.0.40/storage/myengine` 软链到本仓库 `storage/myengine`；cmake 需 `-DWITH_BOOST=<src>/boost -DDOWNLOAD_BOOST=0`；依赖 `libtirpc-dev`。

## 踩坑记录
- 不要声明 `HA_STATS_RECORDS_IS_EXACT`（优化器会认为表为空，不下探索引）。
- 必须重写 `max_supported_keys` 等，否则建主键表报 ERROR 1069。
- 可空索引列需 `HA_NULL_IN_KEY`；重复键报错文本依赖 `info(HA_STATUS_ERRKEY)` 里设置 `errkey`。
