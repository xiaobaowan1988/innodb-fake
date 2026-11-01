# MyEngine：MySQL -> C++ Handler -> CGO -> Go（全链路 Mock）

- `go/myengine.go`：Go 端导出 `MyEngine_Open/Close/Put/Seek`，全部 Mock 返回 0。
- `storage/myengine/`：MySQL 8.0 插件（`ha_myengine.{h,cc}` + `CMakeLists.txt`），每个入口 `fprintf(stderr, ...)`。
- `scripts/`：`build_go.sh` 编 Go 库，`deploy.sh` 部署，`verify.sql` 验证 SQL。

## 步骤
1. `scripts/build_go.sh`
2. 将 `storage/myengine` 拷贝/软链到 MySQL 8.0 源码 `storage/myengine`，重新 cmake 并 make `myengine`（产物 `ha_myengine.so`）
3. `scripts/deploy.sh <build_dir> <plugin_dir>`，设置 `LD_LIBRARY_PATH`
4. `mysqld --console --user=mysql`，另开终端执行 `scripts/verify.sql`

## 状态
Go 库已编译并通过 C 程序调用验证；C++ 插件尚未在真实 MySQL 源码树中编译（环境中无 MySQL 源码）。

## 验证结果（MySQL 8.0.40）
- `CREATE TABLE ... ENGINE=MyEngine` -> `create`、`open`、`[go] Open`
- `INSERT` -> `write_row` -> `[go] Put ... swallowed`，客户端 `Query OK`
- `SELECT * FROM t1` -> `rnd_init`、`rnd_next -> HA_ERR_END_OF_FILE`，`Empty set`
- `SELECT * FROM t1 WHERE id = 1` -> `index_init`、`index_read_map`、`[go] Seek`，`Empty set`

注意：不要声明 `HA_STATS_RECORDS_IS_EXACT`，否则优化器认定表为空，永远不会下探索引。
引擎必须重写 `max_supported_keys` 等，否则 `PRIMARY KEY` 建表报 ERROR 1069。
