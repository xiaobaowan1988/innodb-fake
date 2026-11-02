# 计划：行锁 + MVCC + 事务

> 规则：**代码与本计划不一致时，先改计划再改代码。** 文末有"变更记录"和各阶段的完成勾选。

## 0. 目标与非目标

**目标**：解决 `docs/concurrency-issues.md` 里的四类问题（丢失更新、UNIQUE 被破坏、读到中间状态、自增 id 重复），
并提供真正的事务：`BEGIN/COMMIT/ROLLBACK`、语句级回滚、`SAVEPOINT`、`REPEATABLE READ`/`READ COMMITTED`、
`SELECT … FOR UPDATE / FOR SHARE`、死锁检测、锁等待超时。验收标准是：**同样的并发/隔离场景在 InnoDB 与 MyEngine 上结果一致**。

**非目标（明确不做，写进文档说明）**
- **持久化/崩溃恢复**：仍是纯内存引擎，事务只有 ACI，没有 D。
- **间隙锁 / next-key 锁**：因此加锁读(`FOR UPDATE/SHARE`、`SERIALIZABLE`)下可能出现幻读；快照读(普通 SELECT)在 RR 下不会幻读。
- 与其他引擎（如 InnoDB）的跨引擎原子提交（不实现 `prepare`/XA）。
- `START TRANSACTION WITH CONSISTENT SNAPSHOT`、外键、在线 DDL、`READ UNCOMMITTED`（按 `READ COMMITTED` 处理）。
- 物理层面的细粒度并发（B+ 树仍用一把全局 `RWMutex` 保护结构）；本计划只做**逻辑**并发（行锁/多版本）。

## 1. 总体架构

```
MySQL 执行器 ──handler 接口──▶ ha_myengine.cc (C++ 胶水：事务注册、键编码、错误码映射)
                                   │ cgo
                                   ▼
                          myengine.go (薄封装，把 C 类型转成 Go)
                                   │
                                   ▼
                       engine.go (纯 Go，可单测/-race)：
                         Txn / ReadView / 版本链 / 行锁管理器 / 回滚 / purge
                                   │
                                   ▼
                            btree.go (值类型改为 any)
```
**核心逻辑全部放进不依赖 cgo 的 `engine.go`**，这样 `go test -race` 能直接测并发。

## 2. 数据模型

### 2.1 版本链（多版本放在 B+ 树的值里）
```go
type rowVer struct {
    trx     *Txn      // 创建这个版本的事务
    deleted bool      // 墓碑版本：表示该事务删除了这一行
    data    []byte    // 整行 record[0] 字节（墓碑为 nil）
    sec     [][]byte  // 每个二级索引在该版本下的键（不含主键后缀），nil 表示该索引无此项
    prev    *rowVer   // 更旧的版本
}
type rowHead struct{ head *rowVer }   // 主索引的值；新版本插到链头
```
- **主索引**：`主键编码 → *rowHead`。键一旦创建，只有 purge 才会删除。
- **二级索引**：`索引键编码+主键编码 → nil`。条目**不带版本**，只增不减，直到 purge 清理（"索引是线索、以主索引为准"）。
  读者通过主索引找到**自己可见的版本**，再核对 `version.sec[idx] == 条目键去掉主键后缀`，不相等就跳过该条目（它属于别的版本）。
  因此二级索引扫描不会返回重复行，也不会返回不可见的行。
- 回滚时弹出版本；该版本新增的二级索引条目如果不再被链上任何版本引用就删除。

### 2.2 事务与可见性
```go
type Txn struct {
    id        uint64
    commitTS  atomic.Uint64   // 0 = 未提交（活跃或已回滚）
    state     (active|committed|aborted)
    view      *ReadView       // RR：事务内第一次一致性读时创建；RC：每条语句重新创建
    undo      []undoRec       // {tbl, key, head, ver}：回滚、语句回滚、savepoint 都靠它
    stmtStart int             // 当前语句在 undo 里的起点
    locks     []lockKey       // 持有的行锁（事务结束统一释放，2PL）
    isolation int; lockTimeout time.Duration
}
type ReadView struct{ snapTS uint64; me *Txn }
```
- **提交时间戳**：全局 `clock`。提交时在 `tmu` 下 `ts = ++clock; t.commitTS = ts`；创建 ReadView 时在同一把 `tmu` 下读 `clock`。
  两者同锁，避免"快照已包含 ts 但事务还没标成已提交"导致的不可重复读。
- **可见性**：版本 `v` 对视图 `V` 可见 ⇔ `v.trx == V.me`，或 `v.trx.commitTS != 0 && v.trx.commitTS <= V.snapTS`。
- **一致性读**（普通 SELECT）：沿版本链找第一个可见版本；若是墓碑则该行不存在。
- **当前读**（`UPDATE/DELETE/FOR UPDATE/FOR SHARE`）：先拿行锁，再读**链头**（锁在手里，链头只可能是已提交版本或自己的版本）；链头是墓碑则行不存在。
  这就是 InnoDB 不丢更新的原因：UPDATE 读最新而不是快照。

### 2.3 行锁管理器
- 锁的对象：`(表, 主键编码)`。模式 `S`、`X`；`S` 与 `S` 兼容，其余互斥；同一事务可重入、可由 S 升级为 X。
- 一把独立的 `lmu` 保护锁表；**等待时不持有 `lmu`，更不持有全局树锁 `mu`**。
- 事务结束（提交或回滚）一次性释放所有行锁，并唤醒该锁的等待者（重试）。
- **死锁检测**：每个事务记录 `blockedOn`（正在等待的事务集合）。要等待前先沿 `blockedOn` 做 DFS，若回到自己 → 死锁，
  让**请求者**当牺牲者，返回 `HA_ERR_LOCK_DEADLOCK`（MySQL 报 1213），并 `thd_mark_transaction_to_rollback` 整个事务回滚（与 InnoDB 一致）。
- **等待超时**：默认 50 秒（InnoDB 的默认值），环境变量 `MYENGINE_LOCK_WAIT_SECS` 可改。超时只回滚**当前语句**，返回 `HA_ERR_LOCK_WAIT_TIMEOUT`（1205）。
  *（已验证：`thd_lock_wait_timeout` 是 InnoDB 内部函数，插件不可用，所以不能读 `innodb_lock_wait_timeout`。）*

## 3. 关键算法

### 3.1 写操作（在 Go 内一次调用完成，持锁顺序：行锁 → 全局 `mu`）
- **Insert(pk, row, sec)**
  1. 取 `(表,pk)` 的 X 锁（可能阻塞：别的事务正在插/删/改同一个键）。
  2. `mu.Lock`；链头存在且**不是墓碑** → 主键重复（当前读语义，不管是否对我可见）。
  3. 对每个 UNIQUE 二级索引（键不含 NULL）：扫描该前缀的条目，逐个解析其主键的**链头**，若链头未删除且 `sec[idx]` 相同且主键不同 → 冲突。
     若该行的链头版本属于**另一个活跃事务**：松开 `mu`，对那一行取 S 锁（等它结束），重新加 `mu` 并重试。
  4. 通过：把新版本推到链头（或创建新链），写入二级索引条目，记 undo。
- **Update(oldpk, newpk, row, sec)**：持有 `oldpk` 的 X 锁（读的时候已拿）。主键不变 → 推新版本；
  主键变 → 等价于 `Delete(oldpk)` + `Insert(newpk)`。UNIQUE 检查同上，但跳过与自己相同的行。
- **Delete(pk)**：持有 X 锁，推墓碑版本。
- 每个写操作失败（重复键、死锁、超时）都**不留半截状态**：版本推入前完成所有检查；推入之后的步骤不会失败。

### 3.2 读操作与游标
- 游标记录位置并带 `txn + lockMode(none|S|X) + pkWidth`。`Seek`/`Step` 在读锁下定位到下一个**候选条目**，然后：
  - **不加锁读**：解析可见版本；主索引不可见/墓碑 → 跳过继续；二级索引再做 `sec` 核对。
  - **加锁读**：松开 `mu` → 取行锁（可能阻塞）→ 重新 `mu.RLock` → 读链头（行可能在等待期间被删/改，重新核对条件，被删则跳过继续）。
- 因为有跨语言的 `Fetch` 一次调用完成"移动+回表+拷出整行"，所有这些循环都在 Go 内部完成。
- `Get(pk)`（`rnd_pos`、`ORDER BY` 回表）：按同样规则取一行。

### 3.3 事务生命周期与 MySQL 的对接（已对照 8.0.40 头文件）
- 每个 MySQL 连接把事务句柄放在 `thd_get_ha_data(thd, hton)`。
- `external_lock(F_RDLCK/F_WRLCK)`（语句开始）：没有事务则 `TxnBegin`；`StmtBegin(iso)`；
  `trans_register_ha(thd, false, hton, nullptr)`（语句级）；若处于多语句事务（`thd_test_options(OPTION_NOT_AUTOCOMMIT|OPTION_BEGIN)`）
  再 `trans_register_ha(thd, true, hton, nullptr)`（事务级）。隔离级别用 `thd_tx_isolation(thd)`。
- `hton->commit(hton, thd, all)`：`all` 为真，或不在多语句事务里（autocommit）→ 提交整个事务；否则什么都不做（语句成功结束，undo 留着）。
- `hton->rollback(hton, thd, all)`：同样条件 → 回滚整个事务；否则**只回滚当前语句**（undo 回到 `stmtStart`）。
- `hton->savepoint_set/rollback/release`（`savepoint_offset = sizeof(uint64)`）：保存/回到 undo 的位置。
- `hton->close_connection`：连接断开时回滚未结束的事务并释放锁。
- `store_lock(lock_type)`：`TL_READ_WITH_SHARED_LOCKS` → S 锁定读；`lock_type >= TL_WRITE_ALLOW_WRITE`（UPDATE/DELETE/INSERT/`FOR UPDATE`）→ X；
  其它 → 一致性读。`SERIALIZABLE` 下把普通读升为 S 锁定读。同时把写锁类型降为 `TL_WRITE_ALLOW_WRITE`，让表级锁不互相阻塞。
- 表级 `THR_LOCK` 不再承担并发控制（仍保留对象以满足接口；DDL 的互斥由 MDL 保证）。
- `table_flags` 去掉 `HA_NO_TRANSACTIONS`；`delete_all_rows` 不再覆盖（默认返回 `HA_ERR_WRONG_COMMAND`，`DELETE FROM t` 逐行删除；`TRUNCATE` 走 `HTON_CAN_RECREATE` 重建）。

### 3.4 AUTO_INCREMENT
重写 `get_auto_increment`：每表一个 Go 端的原子计数器。第一次使用时由 C++ 读取"最新非删除行"的自增列最大值初始化；
之后取号不依赖任何读。`write_row` 成功后若自增列值（含用户显式给的）大于计数器则抬高计数器。回滚不回收号码（和 InnoDB 一样会有空洞）。

### 3.5 Purge（回收旧版本）
- 事务提交时把它改过的 `(表, 主键)` 连同 `commitTS` 放进队列。
- 后台 goroutine（约每 50ms）在 `mu.Lock` 下限量处理：对每个键，计算最老的活跃视图 `oldest = min(各活跃事务视图.snapTS, 当前 clock)`，
  保留"对 oldest 可见的最新版本"及更新的所有版本，丢弃更旧的；若链只剩一个**已提交、早于 oldest** 的墓碑 → 从主索引删掉该键，
  并删除不再被引用的二级索引条目。
- 目的：内存不会无限增长；不影响正确性（可见性只依赖版本链）。

### 3.6 错误码
死锁 → `HA_ERR_LOCK_DEADLOCK`；锁超时 → `HA_ERR_LOCK_WAIT_TIMEOUT`；重复键 → `HA_ERR_FOUND_DUPP_KEY`，
并通过出参告诉 C++ 冲突的是哪个索引（给 `errkey`，保证报错文本里的键名正确）。

## 4. 对现有代码的改动清单
| 文件 | 改动 |
|---|---|
| `go/btree.go` | 值类型 `[]byte` → `any`；测试同步修改 |
| `go/engine.go`（新） | Txn、ReadView、版本链、锁管理器、行操作、回滚、purge、自增计数器（纯 Go） |
| `go/cursor.go` | 游标带 txn/锁模式，跳过不可见项，二级索引核对 |
| `go/myengine.go` | 新的事务与行操作导出函数；旧的 `Put/Delete/Get` 导出函数移除 |
| `storage/myengine/ha_myengine.{h,cc}` | handlerton 钩子、`external_lock/store_lock`、写路径改用新 API、`get_auto_increment`、错误码映射 |
| `scripts/` | 新增 `txn_check.py`（隔离/锁/事务场景，InnoDB 对照）；`concurrency_check.py` 作为回归 |
| `docs/` | 更新 `concurrency-issues.md`、README 限制 |

## 5. 测试计划
1. **Go 单测（`go test -race`）**：可见性规则；版本链回滚；语句回滚/savepoint；锁兼容矩阵；死锁检测（两方、三方环）；
   锁超时；purge 不丢可见版本；并发随机事务 + 与顺序模型对照（不变量检查）。
2. **SQL 场景（`scripts/txn_check.py`，每项在 InnoDB 与 MyEngine 上各跑，结果必须一致）**
   脏读被阻止；RR 下不可重复读被阻止/RC 下允许；RR 快照读无幻读；丢失更新被阻止；`FOR UPDATE` 阻塞与放行；
   死锁（报 1213，对方事务成功）；锁等待超时（报 1205）；`ROLLBACK` 撤销全部；多行 INSERT 中途重复键 → 语句回滚；
   `SAVEPOINT / ROLLBACK TO`；并发自增；UNIQUE 竞争；二级索引读一致性。
3. **回归**：`compare_innodb.sh`（147 个结果集）、`concurrency_check.py`（四项全为 InnoDB 的结果）。
4. **性能**：重跑 `bench.py`，记录 MVCC 的开销（预期点查略慢、写更慢）。

## 6. 阶段与完成标准
- [x] **P1 纯 Go 核心**：`engine.go` + 单测通过（含 `-race`）。12 个测试；另做了 6 个变异检查（可见性恒真、二级索引不核对、死锁检测失效、不取加锁读、回滚不弹版本、purge 过度）均被测试抓到。
- [x] **P2 接入 MySQL**：cgo 层与 C++ handler 改造；`compare_innodb.sh` 仍 147 个结果集一致；`concurrency_check.py` 四项异常数均为 0（UNIQUE 加压到 2000 值×8 客户端，连续 2 次 0 重复；此前每轮都重复）。
- [x] **P3 场景验证**：`txn_check.py` 21 个场景（含锁等待超时，需 `MYENGINE_LOCK_WAIT_SECS=2` 与 `--timeout-tests`）全部与 InnoDB 一致；`concurrency_check.py` 四项异常数均为 0；`compare_innodb.sh` 147 个结果集一致；`go test -race` 通过。
- [ ] **P4 收尾**：性能重测、文档更新、限制清单。

## 7. 风险
- **binlog 与事务引擎的交互**：默认开启 binlog，引擎变成事务引擎后会走 binlog 的事务缓存；若出现问题，用 `--skip-log-bin` 运行测试并记录。
- **语句回滚的语义**：必须在 `commit/rollback` 钩子里正确区分语句级与事务级，否则会提前提交。用 `txn_check.py` 里的"中途失败"用例覆盖。
- **持锁顺序**：统一为 行锁 → 全局 `mu`；任何持有 `mu` 的路径都不得去等行锁，否则会死锁。代码里用注释标明，单测用 `-race` 和超时兜底。
- **二级索引条目只增不减**：purge 之前扫描会多走一些过期条目，性能下降但不影响正确性；需要在 P4 量化。

## 变更记录
- v1：初稿。
- v2（P1 完成时）：实现中与初稿不同或初稿未写明的点——
  1. **B+ 树在表打开时一次建好**：`Open(name, nkeys)` 传入索引个数，主索引和所有二级索引的树都已存在，
     游标只需持有固定的树指针，读路径不再需要"不存在就创建"（因此去掉了 `treeRO/emptyTree`）。
  2. **游标分两层**：`rawCursor` 只管"树里的位置"（沿叶子链走，树被改过则按 key 重新定位），不依赖事务；
     `Cursor` 在其上叠加可见性、行锁、二级索引核对。
  3. **跳过不可见候选时的方向与前缀规则**：`seekExact`/`seekPrefixLast` 下候选必须仍以查找键为前缀，否则视为没找到
     （防止 EXACT 滑到邻居行）；`LE/LT/Last/PrefixLast` 向前（-1）找，其余向后（+1）。
  4. **死锁检测的一个坑**：被唤醒但尚未重试的等待者，其 `blockedOn` 已过期，会让后续检测误判成环；
     唤醒时必须清掉（`releaseAll` 里已处理）。
  5. 主键被修改的 `Update` 在**应用之前**完成所有检查（新键占用、唯一冲突），失败时不留半截状态，与初稿一致。
  6. 后台 purge 协程由 `StartPurger` 提供，P2 在加载库时启动。
- v3（P2 完成时）：实现中与初稿不同或初稿未写明的点——
  1. **cgo 接口按"句柄"组织**：事务、表、游标都是 `runtime/cgo.Handle`（C++ 当作 64 位整数）。
     新导出：`TxnBegin/TxnStmtBegin/TxnEnd(commit)/TxnRollbackStmt/TxnSavepoint/TxnRollbackTo`、
     `TableOpen(name,nkeys)/TableClose`、`RowInsert/RowUpdate/RowDelete`（二级索引键打包成 `[idx u32][flags u32][len u32][key]`）、
     `GetRow`、带事务的 `CursorOpen/SeekFetch/Fetch`、`AutoReserve/AutoInit/AutoBump/IndexLastLatest`；旧的 `Put/Get/Delete` 已移除。
  2. **事务结束用 `TxnEnd(commit)` 一个函数**（提交或回滚，并释放句柄），C++ 随后把 THD 上的句柄置空。
  3. **自增每次只预留 1 个值**（不产生空洞，取号本身只是一次原子操作）；初始化用 `IndexLastLatest`
     （取自增列所在索引上最大键对应的最新未删除行，不管可见性），避免初始化时看不到别的事务未提交的最大值。
  4. **`table_flags` 增加 `HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE`**（事务引擎应声明）；
     默认开启 binlog 时没有出现警告或错误，风险项里的 binlog 问题暂未出现。
  5. **C++ 调用轨迹的开关反了**：事务化之后轨迹太吵，改为默认关闭，`MYENGINE_TRACE=1` 打开
     （原来的 `MYENGINE_QUIET` 不再使用）。
  6. **`start_stmt` 也要实现**（`LOCK TABLES` 场景下 MySQL 调它而不是 `external_lock`），与 `external_lock` 共用 `begin_stmt`。
  7. 表级写锁在 `store_lock` 里降级为 `TL_WRITE_ALLOW_WRITE`（InnoDB 的做法），原始锁类型另存用来决定行锁模式。
- v4（P3 完成时）：
  1. **验证中发现并修复两个 handler bug**（都不属于事务设计本身，但此前没有测试覆盖）：
     - `index_read_map` 用 `popcount(keypart_map)` 当键的列数。`INSERT … ON DUPLICATE KEY UPDATE`/`REPLACE`
       内部用 `HA_WHOLE_KEY`（全 1 位图）调用，得到 64，越界读取 `key_part`，**崩溃 mysqld**。现改为与索引列数取较小值。
     - 把查找键 `key_restore` 到 `record[0]`，会覆盖这类语句放在 `record[0]` 里的待插入新行
       （同时它们让引擎把已有行读到 `record[1]`）。现改用私有缓冲区 `key_scratch`。
  2. **`txn_check.py` 的死锁场景不比较"谁是牺牲者"**：InnoDB 与本引擎的挑选规则不同，只检查"恰好一个 1213，另一个成功"。
  3. **已用实验确认"无间隙锁"的限制**：A 在 `BEGIN` 后对 `id >= 2` 做 `SELECT … FOR UPDATE`，B 往该范围插入——
     InnoDB 阻塞 B（间隙锁），本引擎不阻塞，A 再次 `FOR UPDATE` 读到 3 行（幻读）。这是计划里的非目标，写入文档。
