# 并发正确性：已解决的问题与遗留限制

此前（全局锁、无事务）`scripts/concurrency_check.py` 暴露了四类逻辑错误。引入行锁 + MVCC + 事务（见 `docs/plan-mvcc.md`）后，
同样的检查在 MyEngine 上与 InnoDB 结果一致：

| 检查项（8 个并发客户端） | 之前 MyEngine | 现在 MyEngine | InnoDB |
|---|---|---|---|
| T1 丢失更新：8×500 次 `n=n+1`，期望 4000 | 2756 | **4000** | 4000 |
| T2 UNIQUE 竞争（2000 个值×8 客户端）被破坏的值 | 31（6 轮累计，每轮都有） | **0**（连续 2 次） | 0 |
| T3 二级索引扫描行数应恒为 1000，异常次数 | 1545 / 13129 | **0 / 13035** | 0 / 7688 |
| T4 并发 AUTO_INCREMENT：报错 / 行数（期望 0 / 2400） | 340 / 2060 | **0 / 2400** | 0 / 2400 |

另有 `scripts/txn_check.py` 的 21 个事务/隔离/锁场景（脏读、可重复读、读已提交、无幻读的快照读、二级索引快照、丢失更新、
`FOR UPDATE/SHARE` 阻塞、SERIALIZABLE、死锁、回滚、语句原子性、SAVEPOINT、唯一冲突等待、自增、UPSERT/REPLACE、
autocommit=0、断连回滚、锁等待超时），MyEngine 与 InnoDB 的执行过程逐行一致。

## 间隙锁与物理并发（已解决）
- **间隙锁**：RR / SERIALIZABLE 下的加锁读、`UPDATE`、`DELETE` 现在会登记间隙锁（next-key），插入通过"插入意向"等待。
  `scripts/txn_check.py` 里 31 个间隙锁场景（范围、等值命中/未命中、二级索引、全表扫描、反向扫描、`LIMIT`、边界是否恰好存在、
  经典死锁、READ COMMITTED 不加锁、SERIALIZABLE 普通读……）与 MySQL 8.0.40 的 InnoDB **逐行一致**；
  规则都是用 InnoDB 实测得到的（见 `docs/plan-gap-latch.md` 变更记录 v3）。
- **物理并发**：不再有保护所有 B+ 树的全局读写锁。每棵树两级锁（SMO 锁 + 叶子锁），版本链受所在主索引叶子的锁保护，
  行锁表分片，唯一键条纹锁，purge 先 `tryLock` 行 X 锁。写和读不再互相阻塞（引擎核心基准：90% 读 + 10% 写的混合负载 4 核下快 2.2 倍，
  扩展倍数从 1.0 提高到 3.0）。

## 并发逻辑审计（修复了两个问题）
- **语句原子性（触发器）**：此前显式事务里的语句如果触发了触发器/存储函数而中途失败，只会回滚到触发器打开第二张表之后的位置，
  前面的修改残留。现在语句起点只在顶层语句开始时设置一次。
- **KILL**：此前 `KILL QUERY` 打断不了被锁阻塞的语句（只能等 50 秒超时）。现在所有锁等待每 100ms 检查一次，被杀则返回 1317。
- 审计中确认**没有问题**的：S→X 升级死锁、`INSERT … ON DUPLICATE KEY UPDATE` 并发、并发改主键、同事务删除再插入同一键的快照一致性、
  并发结束后无残留锁。脚本：`scripts/concurrency_audit.py`、`scripts/txn_check.py`。

## 第二轮审计（又修复了四个问题）
- **锁队列公平性**：此前后来的读锁可以插队，持续有人 `FOR SHARE` 时写者会被饿死。现在按到达顺序排队（锁升级例外，不排队）。
- **`NOWAIT` / `SKIP LOCKED`**：此前都一直阻塞。现在与 InnoDB 一致（3572 / 跳过被锁的行）。
- **`START TRANSACTION WITH CONSISTENT SNAPSHOT`**：此前快照不在那一刻建立。现在 RR 下立即建立视图。
- **外键**：此前被静默忽略（孤儿行能写进去）。现在建表和 `ALTER TABLE ADD FOREIGN KEY` 都明确拒绝（1478）。
- 与 InnoDB 一致、没发现问题的：`LOCK TABLES` 下带触发器的语句原子性、`KILL CONNECTION`、`ALTER TABLE`、多表 `UPDATE/DELETE`、跨引擎事务。

## 遗留限制（有意不做，或尚未做）
0. **锁升级的一个有意差异**：A 持 S、B 等 X、A 升级为 X——InnoDB 报死锁 1213，本引擎让 A 升级成功（升级不排队）。

1. **二级索引扫描的停止条目**：InnoDB 还会锁它的记录部分（实测：`WHERE k BETWEEN 2 AND 3 FOR UPDATE` 之后，
   `UPDATE ... SET k=9 WHERE id=<停止条目的行>` 被 InnoDB 阻塞，本引擎不阻塞）。我们只锁间隙。这只是少锁一点（并发略高）：
   范围内的幻读仍被间隙锁完全防住，停止条目移入范围会撞上间隙锁。
2. **没有持久化**：事务只有 ACI，没有 D。mysqld 重启后表结构还在、数据为空。
3. **不参与 XA/两阶段提交**：与其他引擎（如 InnoDB）混用事务时，跨引擎的提交不是原子的。
4. **死锁牺牲者的选择规则与 InnoDB 不同**：本引擎总是让"形成环的那个请求者"当牺牲者；InnoDB 按事务权重选择。结果都是恰好一个 1213。
5. **SMO 仍是整棵树独占**：节点分裂/合并时整棵树短暂停顿（`maxKeys=32` 时约每 16 次插入一次）。更细的 SMO 并发（B-link 树）没有做。
6. **一致性读的单核开销比改造前高**：细粒度加锁的固有代价（见 `docs/bench-baseline.md` 第五轮）。进一步的办法是读路径完全无锁
   （写时复制的叶子 + 原子指针），没有做。
7. 不支持外键（建表时明确拒绝）、在线 DDL；`READ UNCOMMITTED` 按 `READ COMMITTED` 处理。
8. 加锁读会给扫描过的每一行加锁且不提前释放（InnoDB 在 READ COMMITTED 下会释放不匹配行的锁）。
9. 锁等待超时是引擎自己的配置（环境变量 `MYENGINE_LOCK_WAIT_SECS`，默认 50 秒），读不到 `innodb_lock_wait_timeout`。
