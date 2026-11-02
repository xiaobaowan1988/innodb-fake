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

## 遗留限制（有意不做，或尚未做）
1. **没有间隙锁/next-key 锁**。已用实验确认：A 在事务里对 `id >= 2` 做 `SELECT … FOR UPDATE`，B 往该范围插入——
   InnoDB 会阻塞 B，MyEngine 不会，A 再次 `FOR UPDATE` 会读到新行（幻读）。快照读（普通 SELECT）不受影响。
   同理 SERIALIZABLE 只有行级 S 锁，不防幻读。
2. **没有持久化**：事务只有 ACI，没有 D。mysqld 重启后表结构还在、数据为空。
3. **不参与 XA/两阶段提交**：与其他引擎（如 InnoDB）混用事务时，跨引擎的提交不是原子的。
4. **死锁牺牲者的选择规则与 InnoDB 不同**：本引擎总是让"形成环的那个请求者"当牺牲者；InnoDB 按事务权重选择。结果都是恰好一个 1213。
5. **B+ 树的物理并发仍是一把全局读写锁**：逻辑上不同行的写不会互相等待行锁，但每次树操作仍在全局锁内串行，写吞吐受限于此。
6. 不支持 `START TRANSACTION WITH CONSISTENT SNAPSHOT`、外键、在线 DDL；`READ UNCOMMITTED` 按 `READ COMMITTED` 处理。
7. 加锁读会给扫描过的每一行加锁且不提前释放（InnoDB 在 READ COMMITTED 下会释放不匹配行的锁）。
8. 锁等待超时是引擎自己的配置（环境变量 `MYENGINE_LOCK_WAIT_SECS`，默认 50 秒），读不到 `innodb_lock_wait_timeout`。
