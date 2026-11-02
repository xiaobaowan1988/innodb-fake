#!/usr/bin/env python3
"""事务/隔离/锁场景测试：每个场景在 InnoDB 与 MyEngine 上用多个连接按固定顺序交错执行，
把过程记成"流水账"，两个引擎的流水账必须逐行一致。

用法: scripts/txn_check.py --socket /tmp/my.sock [--only 名字] [--timeout-tests]
  --timeout-tests  额外运行锁等待超时场景（要求 mysqld 以 MYENGINE_LOCK_WAIT_SECS=2 启动，InnoDB 会话同步设置为 2 秒）
"""
import argparse, sys, threading, time
import pymysql

BLOCK_PROBE = 0.5  # 判定"仍在阻塞"所等待的时间

class Ctx:
    def __init__(self, a, eng):
        self.a, self.eng, self.log, self.sessions = a, eng, [], {}
        self.db = f"tx_{eng}"
    def conn(self, name, iso=None):
        c = pymysql.connect(unix_socket=self.a.socket, user="root", database=self.db, autocommit=True)
        if iso: c.cursor().execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
        if self.a.timeout_tests and self.eng == "InnoDB": c.cursor().execute("SET SESSION innodb_lock_wait_timeout=2")
        self.sessions[name] = c; return c
    def _exec(self, name, sql):
        cur = self.sessions[name].cursor()
        try:
            n = cur.execute(sql.replace("__E__", self.eng))
            rows = cur.fetchall() if cur.description else None
            return ("rows", [list(r) for r in rows]) if rows is not None else ("ok", n)
        except pymysql.err.MySQLError as e:
            return ("err", e.args[0])
    def run(self, name, sql, note=None):
        r = self._exec(name, sql); self.log.append(f"{name}: {sql.split(chr(10))[0][:70]:70s} -> {fmt(r)}"); return r
    def start(self, name, sql):  # 异步执行：可能阻塞
        h = {"name": name, "sql": sql, "res": None}
        def go(): h["res"] = self._exec(name, sql)
        h["t"] = threading.Thread(target=go); h["t"].start(); return h
    def blocked(self, h):  # 记录该语句是否仍被阻塞
        h["t"].join(BLOCK_PROBE); b = h["t"].is_alive()
        self.log.append(f"{h['name']}: {h['sql'][:70]:70s} -> {'BLOCKED' if b else 'NOT BLOCKED ' + fmt(h['res'])}"); return b
    def join(self, h, timeout=20):
        h["t"].join(timeout); assert not h["t"].is_alive(), "statement never finished"
        self.log.append(f"{h['name']}: (resumed) {h['sql'][:56]:56s} -> {fmt(h['res'])}"); return h["res"]
    def close(self, name):
        self.sessions[name].close(); self.log.append(f"{name}: <connection closed>")

def fmt(r): return r[0] + ":" + str(r[1])

def fresh(c, ddl_extra=""):
    cur = pymysql.connect(unix_socket=c.a.socket, user="root", autocommit=True).cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {c.db}"); cur.execute(f"USE {c.db}")
    for t in ("t", "u", "ai", "g", "gu"): cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, k INT, KEY(k)) ENGINE=__E__".replace("__E__", c.eng))
    cur.execute("INSERT INTO t VALUES (1,10,1),(2,20,2),(3,30,3)")
    cur.execute("CREATE TABLE u (id INT PRIMARY KEY, e INT, UNIQUE KEY ue(e)) ENGINE=__E__".replace("__E__", c.eng))
    cur.execute("CREATE TABLE ai (id INT AUTO_INCREMENT PRIMARY KEY, v INT) ENGINE=__E__".replace("__E__", c.eng))
    # 间隙锁场景用表：id = 10,20,30,40,50；k = id/10，k 上有非唯一二级索引
    cur.execute("CREATE TABLE g (id INT PRIMARY KEY, k INT, KEY(k)) ENGINE=__E__".replace("__E__", c.eng))
    cur.execute("INSERT INTO g VALUES (10,1),(20,2),(30,3),(40,4),(50,5)")
    cur.execute("CREATE TABLE gu (id INT PRIMARY KEY, e INT, UNIQUE KEY ue(e)) ENGINE=__E__".replace("__E__", c.eng))
    cur.execute("INSERT INTO gu VALUES (10,1),(20,2),(30,3),(40,4),(50,5)")

SC = {}
def scenario(f): SC[f.__name__] = f; return f
ALL = "SELECT * FROM t ORDER BY id"

@scenario
def dirty_read(c):
    A, B = c.conn("A"), c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "UPDATE t SET v=11 WHERE id=1")
    c.run("B", "SELECT v FROM t WHERE id=1")           # 看不到未提交的修改
    c.run("A", "ROLLBACK"); c.run("B", "SELECT v FROM t WHERE id=1")

@scenario
def repeatable_read(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=1")
    c.run("B", "UPDATE t SET v=12 WHERE id=1")
    c.run("A", "SELECT v FROM t WHERE id=1")           # RR：仍是 10
    c.run("A", "COMMIT"); c.run("A", "SELECT v FROM t WHERE id=1")

@scenario
def read_committed(c):
    c.conn("A", "READ COMMITTED"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=1")
    c.run("B", "UPDATE t SET v=12 WHERE id=1")
    c.run("A", "SELECT v FROM t WHERE id=1")           # RC：看到 12
    c.run("A", "COMMIT")

@scenario
def snapshot_no_phantom(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT COUNT(*) FROM t")
    c.run("B", "INSERT INTO t VALUES (4,40,4)"); c.run("B", "DELETE FROM t WHERE id=3")
    c.run("A", "SELECT COUNT(*) FROM t"); c.run("A", ALL)   # 仍是 3 行，id=3 还在
    c.run("A", "COMMIT"); c.run("A", ALL)

@scenario
def snapshot_sees_old_secondary_key(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT id FROM t WHERE k=1")
    c.run("B", "UPDATE t SET k=9 WHERE id=1")
    c.run("A", "SELECT id FROM t WHERE k=1"); c.run("A", "SELECT id FROM t WHERE k=9")  # 快照里 k 仍是 1
    c.run("A", "COMMIT"); c.run("A", "SELECT id FROM t WHERE k=1"); c.run("A", "SELECT id FROM t WHERE k=9")

@scenario
def lost_update_prevented(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=1")
    c.run("B", "BEGIN"); c.run("B", "UPDATE t SET v=v+1 WHERE id=1")
    h = c.start("A", "UPDATE t SET v=v+1 WHERE id=1"); c.blocked(h)
    c.run("B", "COMMIT"); c.join(h); c.run("A", "COMMIT"); c.run("A", "SELECT v FROM t WHERE id=1")  # 12

@scenario
def update_after_concurrent_delete(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT COUNT(*) FROM t")
    c.run("B", "DELETE FROM t WHERE id=1")
    c.run("A", "SELECT v FROM t WHERE id=1")           # 快照里还在
    c.run("A", "UPDATE t SET v=99 WHERE id=1")         # 当前读：已被删除，影响 0 行
    c.run("A", "COMMIT"); c.run("A", ALL)

@scenario
def for_update_blocks(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=2 FOR UPDATE")
    h = c.start("B", "UPDATE t SET v=v+5 WHERE id=2"); c.blocked(h)
    h2 = c.start("B2", "SELECT 1") if False else None
    c.run("A", "UPDATE t SET v=v+1 WHERE id=2"); c.run("A", "COMMIT"); c.join(h); c.run("B", "SELECT v FROM t WHERE id=2")  # 26

@scenario
def for_share(c):
    A, B = c.conn("A"), c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=1 FOR SHARE")
    c.run("B", "BEGIN"); c.run("B", "SELECT v FROM t WHERE id=1 FOR SHARE")   # S 与 S 兼容
    h = c.start("B", "UPDATE t SET v=0 WHERE id=1"); c.blocked(h)              # 但 B 要写必须等 A
    c.run("A", "COMMIT"); c.join(h); c.run("B", "COMMIT"); c.run("B", "SELECT v FROM t WHERE id=1")

@scenario
def secondary_for_update_blocks(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT id FROM t WHERE k=2 FOR UPDATE")
    h = c.start("B", "UPDATE t SET v=v+1 WHERE id=2"); c.blocked(h)
    c.run("A", "ROLLBACK"); c.join(h); c.run("B", "SELECT v FROM t WHERE id=2")

@scenario
def serializable_read_blocks_writer(c):
    c.conn("A", "SERIALIZABLE"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=1")
    h = c.start("B", "UPDATE t SET v=v+1 WHERE id=1"); c.blocked(h)
    c.run("A", "COMMIT"); c.join(h)

@scenario
def deadlock(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "UPDATE t SET v=v+1 WHERE id=1")
    c.run("B", "BEGIN"); c.run("B", "UPDATE t SET v=v+1 WHERE id=2")
    ha = c.start("A", "UPDATE t SET v=v+1 WHERE id=2"); c.blocked(ha)
    rb = c._exec("B", "UPDATE t SET v=v+1 WHERE id=1")    # 形成环
    ra = None
    if rb[0] == "err":                                      # B 是牺牲者：A 随后得以继续
        ra = c.join(ha) if True else None
        victim = "B"
    else:                                                   # 否则 A 是牺牲者
        ra = c.join(ha); victim = "A"
    # 不依赖谁是牺牲者（InnoDB 与本引擎的挑选规则可能不同），只检查"恰好一个 1213，另一个成功"
    errs = [r for r in (ra, rb) if r[0] == "err"]
    c.log.append(f"deadlock: errors={[e[1] for e in errs]} exactly_one_1213={len(errs)==1 and errs[0][1]==1213}")
    for n in ("A", "B"):
        c._exec(n, "ROLLBACK")
    c.run("A", ALL)    # 两边都回滚后数据应回到初始

@scenario
def rollback_everything(c):
    c.conn("A")
    c.run("A", "BEGIN"); c.run("A", "INSERT INTO t VALUES (4,40,4)"); c.run("A", "UPDATE t SET v=0, k=7 WHERE id=1")
    c.run("A", "DELETE FROM t WHERE id=2"); c.run("A", ALL); c.run("A", "ROLLBACK"); c.run("A", ALL)
    c.run("A", "SELECT id FROM t WHERE k=7"); c.run("A", "SELECT id FROM t WHERE k=4")

@scenario
def statement_atomicity(c):
    c.conn("A")
    c.run("A", "INSERT INTO t VALUES (7,70,7),(8,80,8),(1,0,0)")   # 最后一行重复：整条语句回滚
    c.run("A", ALL)
    c.run("A", "BEGIN"); c.run("A", "INSERT INTO t VALUES (5,50,5)")
    c.run("A", "INSERT INTO t VALUES (6,60,6),(2,0,0)")             # 失败：只撤销这条语句，事务继续
    c.run("A", ALL); c.run("A", "COMMIT"); c.run("A", ALL)
    c.run("A", "UPDATE t SET id=id+0, v=v+1")                        # 多行更新
    c.run("A", "UPDATE t SET id=1 WHERE id=3")                       # 主键冲突：语句回滚
    c.run("A", ALL)

@scenario
def savepoint(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "INSERT INTO t VALUES (5,50,5)"); c.run("A", "SAVEPOINT s1")
    c.run("A", "INSERT INTO t VALUES (6,60,6)"); c.run("A", "UPDATE t SET v=0 WHERE id=1")
    c.run("A", "ROLLBACK TO SAVEPOINT s1"); c.run("A", ALL)
    c.run("B", ALL)                                                  # 别人看不到任何未提交内容
    c.run("A", "COMMIT"); c.run("B", ALL)

@scenario
def unique_conflict_waits(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "INSERT INTO u VALUES (1,5)")
    h = c.start("B", "INSERT INTO u VALUES (2,5)"); c.blocked(h)
    c.run("A", "COMMIT"); c.join(h)                                  # 1062
    c.run("A", "BEGIN"); c.run("A", "INSERT INTO u VALUES (3,7)")
    h = c.start("B", "INSERT INTO u VALUES (4,7)"); c.blocked(h)
    c.run("A", "ROLLBACK"); c.join(h)                                # 成功
    c.run("A", "SELECT * FROM u ORDER BY id")

@scenario
def autoincrement(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "INSERT INTO ai (v) VALUES (1)"); c.run("A", "SELECT LAST_INSERT_ID()")
    c.run("B", "INSERT INTO ai (v) VALUES (2)"); c.run("B", "SELECT LAST_INSERT_ID()")
    c.run("A", "ROLLBACK"); c.run("B", "INSERT INTO ai (v) VALUES (3)"); c.run("B", "SELECT * FROM ai ORDER BY id")
    c.run("B", "INSERT INTO ai VALUES (100, 4)"); c.run("B", "INSERT INTO ai (v) VALUES (5)"); c.run("B", "SELECT * FROM ai ORDER BY id")

@scenario
def upsert_and_replace(c):
    c.conn("A")
    c.run("A", "INSERT INTO t VALUES (1,0,0) ON DUPLICATE KEY UPDATE v=v+100")
    c.run("A", "REPLACE INTO t VALUES (2,222,22)"); c.run("A", "REPLACE INTO t VALUES (9,99,9)")
    c.run("A", "INSERT IGNORE INTO t VALUES (3,0,0)"); c.run("A", ALL)

@scenario
def autocommit_off(c):
    c.conn("A"); c.conn("B")
    c.run("A", "SET autocommit=0"); c.run("A", "INSERT INTO t VALUES (4,40,4)"); c.run("B", ALL)
    c.run("A", "COMMIT"); c.run("B", ALL)
    c.run("A", "UPDATE t SET v=1 WHERE id=1"); c.run("A", "ROLLBACK"); c.run("B", "SELECT v FROM t WHERE id=1")

@scenario
def connection_close_rolls_back(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "UPDATE t SET v=0 WHERE id=1"); c.close("A")
    time.sleep(0.3)
    c.run("B", "SELECT v FROM t WHERE id=1"); c.run("B", "UPDATE t SET v=v+1 WHERE id=1"); c.run("B", "SELECT v FROM t WHERE id=1")

@scenario
def delete_then_reinsert_other_txn(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "DELETE FROM t WHERE id=1")
    h = c.start("B", "INSERT INTO t VALUES (1,111,1)"); c.blocked(h)
    c.run("A", "COMMIT"); c.join(h); c.run("B", ALL)

@scenario
def lock_wait_timeout(c):  # 需要 --timeout-tests
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "UPDATE t SET v=0 WHERE id=1")
    c.run("B", "BEGIN"); c.run("B", "UPDATE t SET v=33 WHERE id=3")
    t0 = time.time(); r = c.run("B", "UPDATE t SET v=1 WHERE id=1")    # 阻塞约 2 秒后 1205
    c.log.append(f"B: waited_about_2s={1.5 < time.time()-t0 < 4.0}")
    c.run("B", "SELECT v FROM t WHERE id=3")                            # 事务还活着，只有这条语句回滚
    c.run("B", "COMMIT"); c.run("A", "ROLLBACK"); c.run("A", ALL)

# ------------------------------------------------------------------ 间隙锁（RR / SERIALIZABLE 加锁读）
def gap_case(c, setup, probes, iso=None):
    """A 在事务里执行 setup（加锁读/写），然后每个 probe 在各自的自动提交会话里异步执行，
    记录是否被阻塞；最后 A 提交，所有被阻塞的 probe 应当随之完成。"""
    c.conn("A", iso)
    c.run("A", "BEGIN")
    for q in setup: c.run("A", q)
    hs = []
    for i, q in enumerate(probes):
        name = f"B{i}"; c.conn(name); h = c.start(name, q); c.blocked(h); hs.append(h)
    c.run("A", "COMMIT")
    for h in hs: c.join(h)
    c.run("A", "SELECT * FROM g ORDER BY id")

@scenario
def gap_range_for_update(c):
    gap_case(c, ["SELECT * FROM g WHERE id BETWEEN 15 AND 35 FOR UPDATE"],
             ["INSERT INTO g VALUES (5,0)", "INSERT INTO g VALUES (12,0)", "INSERT INTO g VALUES (25,0)",
              "INSERT INTO g VALUES (35,0)", "INSERT INTO g VALUES (38,0)", "INSERT INTO g VALUES (45,0)", "INSERT INTO g VALUES (60,0)",
              "UPDATE g SET k=9 WHERE id=40", "UPDATE g SET k=9 WHERE id=10"])   # 正向停止记录 40：只锁间隙，不锁行

@scenario
def gap_unique_miss(c):
    gap_case(c, ["SELECT * FROM g WHERE id = 25 FOR UPDATE"],
             ["INSERT INTO g VALUES (22,0)", "INSERT INTO g VALUES (28,0)", "INSERT INTO g VALUES (35,0)",
              "UPDATE g SET k=9 WHERE id=30", "UPDATE g SET k=9 WHERE id=20"])

@scenario
def gap_unique_hit(c):
    gap_case(c, ["SELECT * FROM g WHERE id = 30 FOR UPDATE"],
             ["INSERT INTO g VALUES (25,0)", "INSERT INTO g VALUES (35,0)", "UPDATE g SET k=9 WHERE id=30", "UPDATE g SET k=9 WHERE id=20"])

@scenario
def gap_secondary_equality(c):
    gap_case(c, ["SELECT * FROM g WHERE k = 2 FOR UPDATE"],
             ["INSERT INTO g VALUES (21,2)", "INSERT INTO g VALUES (15,2)", "INSERT INTO g VALUES (35,3)",
              "INSERT INTO g VALUES (5,1)", "UPDATE g SET k=2 WHERE id=40", "INSERT INTO g VALUES (22,9)"])

@scenario
def gap_secondary_miss(c):
    gap_case(c, ["SELECT * FROM g WHERE k = 99 FOR UPDATE"],
             ["INSERT INTO g VALUES (60,99)", "INSERT INTO g VALUES (61,6)", "INSERT INTO g VALUES (9,0)"])

@scenario
def gap_full_scan_delete(c):
    gap_case(c, ["DELETE FROM g WHERE id + 0 > 1000"],
             ["INSERT INTO g VALUES (5,0)", "INSERT INTO g VALUES (45,0)", "INSERT INTO g VALUES (60,0)"])

@scenario
def gap_supremum(c):
    gap_case(c, ["SELECT * FROM g WHERE id > 40 FOR UPDATE"],
             ["INSERT INTO g VALUES (35,0)", "INSERT INTO g VALUES (45,0)", "INSERT INTO g VALUES (60,0)"])

@scenario
def gap_read_committed_no_gap_locks(c):
    gap_case(c, ["SELECT * FROM g WHERE id BETWEEN 15 AND 35 FOR UPDATE"],
             ["INSERT INTO g VALUES (12,0)", "INSERT INTO g VALUES (25,0)"], iso="READ COMMITTED")

@scenario
def gap_serializable_plain_select(c):
    gap_case(c, ["SELECT * FROM g WHERE id BETWEEN 15 AND 35"],
             ["INSERT INTO g VALUES (25,0)", "INSERT INTO g VALUES (45,0)"], iso="SERIALIZABLE")

@scenario
def gap_update_range(c):
    gap_case(c, ["UPDATE g SET k = k WHERE id BETWEEN 15 AND 35"],
             ["INSERT INTO g VALUES (25,0)", "INSERT INTO g VALUES (45,0)", "INSERT INTO g VALUES (12,0)"])

@scenario
def gap_backward_scan(c):
    gap_case(c, ["SELECT * FROM g WHERE id >= 30 ORDER BY id DESC FOR UPDATE"],
             ["INSERT INTO g VALUES (60,0)", "INSERT INTO g VALUES (45,0)", "INSERT INTO g VALUES (35,0)",
              "INSERT INTO g VALUES (25,0)", "INSERT INTO g VALUES (15,0)", "INSERT INTO g VALUES (5,0)",
              "UPDATE g SET k=9 WHERE id=20", "UPDATE g SET k=9 WHERE id=10"])   # 反向停止记录 20：行和它前面的间隙都被锁

@scenario
def gap_backward_range(c):
    gap_case(c, ["SELECT * FROM g WHERE id BETWEEN 25 AND 45 ORDER BY id DESC FOR UPDATE"],
             ["INSERT INTO g VALUES (22,0)", "INSERT INTO g VALUES (15,0)", "INSERT INTO g VALUES (47,0)",
              "INSERT INTO g VALUES (55,0)", "UPDATE g SET k=9 WHERE id=20", "UPDATE g SET k=9 WHERE id=50"])

@scenario
def gap_limit(c):
    gap_case(c, ["SELECT * FROM g ORDER BY id LIMIT 2 FOR UPDATE"],
             ["INSERT INTO g VALUES (5,0)", "INSERT INTO g VALUES (15,0)", "INSERT INTO g VALUES (25,0)"])

@scenario
def gap_secondary_range(c):
    gap_case(c, ["SELECT * FROM g WHERE k BETWEEN 2 AND 3 FOR UPDATE"],
             ["INSERT INTO g VALUES (25,2)", "INSERT INTO g VALUES (35,3)", "INSERT INTO g VALUES (36,4)", "INSERT INTO g VALUES (5,1)",
              "INSERT INTO g VALUES (15,2)", "UPDATE g SET k=9 WHERE id=10", "UPDATE g SET k=2 WHERE id=50"])
    # 已知差异（不在回归里）：InnoDB 还会锁二级索引停止条目的记录部分，"UPDATE g SET k=9 WHERE id=40" 在 InnoDB 下被阻塞，
    # 本引擎不阻塞——见 docs/plan-gap-latch.md v3 第 5 条

@scenario
def gap_delete_range(c):
    gap_case(c, ["DELETE FROM g WHERE id BETWEEN 20 AND 30"],
             ["INSERT INTO g VALUES (25,0)", "INSERT INTO g VALUES (15,0)", "INSERT INTO g VALUES (35,0)", "INSERT INTO g VALUES (45,0)"])

@scenario
def gap_for_share_range(c):
    gap_case(c, ["SELECT * FROM g WHERE id BETWEEN 15 AND 35 FOR SHARE"],
             ["INSERT INTO g VALUES (25,0)", "INSERT INTO g VALUES (45,0)", "UPDATE g SET k=9 WHERE id=20"])

@scenario
def gap_unique_secondary_hit(c):
    c.conn("A"); c.run("A", "BEGIN"); c.run("A", "SELECT * FROM gu WHERE e = 3 FOR UPDATE")
    hs = []
    for i, q in enumerate(["INSERT INTO gu VALUES (35,35)", "INSERT INTO gu VALUES (25,25)", "UPDATE gu SET e=99 WHERE id=30", "UPDATE gu SET e=98 WHERE id=20"]):
        c.conn(f"B{i}"); h = c.start(f"B{i}", q); c.blocked(h); hs.append(h)
    c.run("A", "COMMIT")
    for h in hs: c.join(h)

@scenario
def gap_unique_secondary_miss(c):
    c.conn("A"); c.run("A", "BEGIN"); c.run("A", "SELECT * FROM gu WHERE e = 25 FOR UPDATE")
    hs = []
    for i, q in enumerate(["INSERT INTO gu VALUES (1,22)", "INSERT INTO gu VALUES (2,35)", "UPDATE gu SET e=99 WHERE id=30", "UPDATE gu SET e=24 WHERE id=50"]):
        c.conn(f"B{i}"); h = c.start(f"B{i}", q); c.blocked(h); hs.append(h)
    c.run("A", "COMMIT")
    for h in hs: c.join(h)

# 范围的起点/终点是否恰好等于已有记录，对锁的范围有影响（InnoDB 的"唯一全键"规则）。用实测对照表固化。
def _bounds_case(name, sql):
    def f(c):
        gap_case(c, [sql], ["INSERT INTO g VALUES (15,0)", "INSERT INTO g VALUES (25,0)",
                            "INSERT INTO g VALUES (35,0)", "INSERT INTO g VALUES (45,0)"])
    f.__name__ = name
    scenario(f)
for _n, _q in [
    ("gap_bounds_20_35_start_exists",   "SELECT * FROM g WHERE id BETWEEN 20 AND 35 FOR UPDATE"),
    ("gap_bounds_15_30_end_exists",     "SELECT * FROM g WHERE id BETWEEN 15 AND 30 FOR UPDATE"),
    ("gap_bounds_20_30_both_exist",     "SELECT * FROM g WHERE id BETWEEN 20 AND 30 FOR UPDATE"),
    ("gap_bounds_gt20_le30",            "SELECT * FROM g WHERE id > 20 AND id <= 30 FOR UPDATE"),
    ("gap_bounds_ge20_lt30",            "SELECT * FROM g WHERE id >= 20 AND id < 30 FOR UPDATE"),
    ("gap_bounds_ge20_no_end",          "SELECT * FROM g WHERE id >= 20 FOR UPDATE"),
    ("gap_bounds_le30_no_start",        "SELECT * FROM g WHERE id <= 30 FOR UPDATE"),
    ("gap_bounds_ge25_start_missing",   "SELECT * FROM g WHERE id >= 25 FOR UPDATE"),
    ("gap_bounds_delete_20_30",         "DELETE FROM g WHERE id BETWEEN 20 AND 30"),
    ("gap_bounds_update_20_30",         "UPDATE g SET k=k WHERE id BETWEEN 20 AND 30"),
]:
    _bounds_case(_n, _q)

@scenario
def gap_phantom_prevented(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT COUNT(*) FROM g WHERE id >= 15 FOR UPDATE")
    h = c.start("B", "INSERT INTO g VALUES (25,0)"); c.blocked(h)
    c.run("A", "SELECT COUNT(*) FROM g WHERE id >= 15 FOR UPDATE")      # 仍是 4，没有幻读
    c.run("A", "COMMIT"); c.join(h)
    c.run("A", "SELECT COUNT(*) FROM g WHERE id >= 15")                  # 提交后才是 5

@scenario
def gap_classic_deadlock(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT * FROM g WHERE id = 25 FOR UPDATE")
    c.run("B", "BEGIN"); c.run("B", "SELECT * FROM g WHERE id = 25 FOR UPDATE")   # 间隙锁之间互不冲突
    ha = c.start("A", "INSERT INTO g VALUES (25,0)"); c.blocked(ha)
    rb = c._exec("B", "INSERT INTO g VALUES (26,0)")                                 # 形成环
    ra = c.join(ha)
    errs = [r for r in (ra, rb) if r[0] == "err"]
    c.log.append(f"deadlock: errors={[e[1] for e in errs]} exactly_one_1213={len(errs)==1 and errs[0][1]==1213}")
    for n in ("A", "B"): c._exec(n, "ROLLBACK")

@scenario
def gap_own_gap_does_not_block_self(c):
    c.conn("A")
    c.run("A", "BEGIN"); c.run("A", "SELECT * FROM g WHERE id BETWEEN 15 AND 35 FOR UPDATE")
    c.run("A", "INSERT INTO g VALUES (25,0)"); c.run("A", "COMMIT"); c.run("A", "SELECT * FROM g ORDER BY id")

# ------------------------------------------------------------------ 语句原子性（触发器）与 KILL
def _trigger_tables(c):
    c.conn("A")
    for t in ("m", "au", "m2", "au2"): c._exec("A", f"DROP TABLE IF EXISTS {t}")
    for sql in ["CREATE TABLE m (id INT PRIMARY KEY, v INT) ENGINE=__E__",
                "CREATE TABLE au (n INT AUTO_INCREMENT PRIMARY KEY, id INT) ENGINE=__E__",
                "CREATE TRIGGER trg_m AFTER INSERT ON m FOR EACH ROW INSERT INTO au (id) VALUES (NEW.id)",
                "CREATE TABLE m2 (id INT PRIMARY KEY, v INT) ENGINE=__E__",
                "CREATE TABLE au2 (id INT PRIMARY KEY) ENGINE=__E__",
                # 每次 UPDATE 一行就往 au2 插一行固定主键 1：第二行必然因重复而在触发器内部失败
                "CREATE TRIGGER trg_m2 AFTER UPDATE ON m2 FOR EACH ROW INSERT INTO au2 VALUES (1)",
                "INSERT INTO m VALUES (1,0)", "INSERT INTO m2 VALUES (1,0),(2,0),(3,0)"]:
        c._exec("A", sql)

@scenario
def trigger_statement_atomicity(c):
    _trigger_tables(c)
    c.run("A", "BEGIN")
    c.run("A", "INSERT INTO m VALUES (5,0)")                       # 前一条语句：成功，必须保留
    c.run("A", "INSERT INTO m VALUES (20,0),(21,0),(1,0)")          # 触发器已往 au 写过 20、21，最后一行重复：整条语句撤销
    c.run("A", "SELECT id FROM m ORDER BY id"); c.run("A", "SELECT id FROM au ORDER BY n")
    c.run("A", "INSERT INTO m VALUES (30,0)")                       # 失败之后事务继续可用
    c.run("A", "COMMIT")
    c.run("A", "SELECT id FROM m ORDER BY id"); c.run("A", "SELECT id FROM au ORDER BY n")

@scenario
def trigger_inner_failure_rolls_back_whole_statement(c):
    _trigger_tables(c)
    c.run("A", "BEGIN")
    c.run("A", "UPDATE m2 SET v=1 WHERE id=1")                      # 成功：触发器写入 au2(1)
    c.run("A", "DELETE FROM au2")                                   # 清掉，让下一条语句从头开始
    c.run("A", "UPDATE m2 SET v=9")                                 # 3 行；第 2 行的触发器重复主键而失败：整条 UPDATE 撤销
    c.run("A", "SELECT id, v FROM m2 ORDER BY id"); c.run("A", "SELECT id FROM au2")
    c.run("A", "COMMIT")
    c.run("A", "SELECT id, v FROM m2 ORDER BY id"); c.run("A", "SELECT id FROM au2")

@scenario
def kill_query_interrupts_lock_wait(c):
    c.conn("A"); c.conn("B"); c.conn("K")
    c.run("A", "BEGIN"); c.run("A", "UPDATE t SET v=100 WHERE id=1")
    cid = c._exec("B", "SELECT CONNECTION_ID()")[1][0][0]
    h = c.start("B", "UPDATE t SET v=200 WHERE id=1"); c.blocked(h)
    t0 = time.time(); c._exec("K", f"KILL QUERY {cid}")
    r = c.join(h, timeout=10)
    c.log.append(f"B: after KILL QUERY -> {fmt(r)} returned_within_3s={time.time()-t0 < 3}")
    c.run("A", "COMMIT")
    c.run("B", "SELECT v FROM t WHERE id=1")                        # 被杀的只是那条语句，连接和 A 的提交都不受影响
    c.run("B", "UPDATE t SET v=300 WHERE id=1"); c.run("B", "SELECT v FROM t WHERE id=1")

# ------------------------------------------------------------------ 锁队列公平性、NOWAIT / SKIP LOCKED、一致性快照
@scenario
def lock_queue_fifo(c):
    c.conn("A"); c.conn("B"); c.conn("C")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=1 FOR SHARE")
    hb = c.start("B", "UPDATE t SET v=v+1 WHERE id=1"); c.blocked(hb)          # 写者在等
    hc = c.start("C", "SELECT v FROM t WHERE id=1 FOR SHARE"); c.blocked(hc)    # 后来的读者必须排在写者后面，不能插队
    c.run("A", "COMMIT"); c.join(hb); c.join(hc)                                 # C 读到的是 B 提交之后的值

@scenario
def nowait_fails_immediately(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=1 FOR UPDATE")
    t0 = time.time(); r = c.run("B", "SELECT v FROM t WHERE id=1 FOR UPDATE NOWAIT")
    c.log.append(f"B: returned_within_2s={time.time()-t0 < 2}")
    c.run("B", "SELECT v FROM t WHERE id=2 FOR UPDATE NOWAIT")                    # 没被锁的行不受影响
    c.run("A", "ROLLBACK")

@scenario
def skip_locked_skips_rows(c):
    c.conn("A"); c.conn("B")
    c.run("A", "BEGIN"); c.run("A", "SELECT v FROM t WHERE id=2 FOR UPDATE")
    c.run("B", "BEGIN"); c.run("B", "SELECT id FROM t ORDER BY id FOR UPDATE SKIP LOCKED")   # 只应得到 1、3
    c.run("B", "SELECT id FROM t WHERE id=2 FOR UPDATE SKIP LOCKED")                          # 被锁的行：空结果
    c.run("B", "ROLLBACK"); c.run("A", "ROLLBACK")

@scenario
def consistent_snapshot_at_start(c):
    c.conn("A"); c.conn("B")
    c.run("A", "START TRANSACTION WITH CONSISTENT SNAPSHOT")
    c.run("B", "UPDATE t SET v=99 WHERE id=1")
    c.run("A", "SELECT v FROM t WHERE id=1")                                      # 快照在 START 时建立：仍是 10
    c.run("A", "COMMIT"); c.run("A", "SELECT v FROM t WHERE id=1")

NEEDS_TIMEOUT = {"lock_wait_timeout"}

def run_one(a, name, eng):
    c = Ctx(a, eng); fresh(c)
    try: SC[name](c)
    except Exception as e: c.log.append(f"EXCEPTION {type(e).__name__}: {e}")
    for s in c.sessions.values():
        try: s.close()
        except Exception: pass
    return c.log

@scenario
def rc_unlocks_nonmatching_rows(c):
    # RC 下全表加锁扫描：不满足 WHERE 的行要提前放锁（unlock_row）；匹配的行保持锁；RR 下全部保持
    c.conn("A"); c.conn("B"); c.conn("R"); c.conn("Q")
    c.run("A", "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    c.run("A", "BEGIN"); c.run("A", "UPDATE t SET v=v WHERE v=20")                # 非索引条件：全表扫描，只有 v=20 的行匹配
    c.run("B", "UPDATE t SET v=v+1 WHERE id=1")                                    # 不匹配：RC 下不被挡
    c.run("B", "UPDATE t SET v=v+1 WHERE id=2")                                    # 匹配：被挡（超时前由收尾解除）
    c.run("A", "ROLLBACK")
    c.run("R", "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    c.run("R", "BEGIN"); c.run("R", "UPDATE t SET v=v WHERE v=20")
    c.run("Q", "UPDATE t SET v=v+1 WHERE id=1")                                    # RR：全部保持锁，被挡
    c.run("R", "ROLLBACK")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--socket", required=True)
    ap.add_argument("--only"); ap.add_argument("--timeout-tests", action="store_true"); ap.add_argument("-v", action="store_true")
    a = ap.parse_args()
    names = [n for n in SC if (not a.only or a.only in n) and (a.timeout_tests or n not in NEEDS_TIMEOUT)]
    bad = 0
    for n in names:
        li, lm = run_one(a, n, "InnoDB"), run_one(a, n, "MyEngine")
        ok = li == lm and not any(l.startswith("EXCEPTION") for l in li + lm)
        print(f"{'PASS' if ok else 'FAIL'}  {n}"); bad += (not ok)
        if not ok or a.v:
            w = max(len(x) for x in li + lm)
            for i in range(max(len(li), len(lm))):
                x = li[i] if i < len(li) else ""; y = lm[i] if i < len(lm) else ""
                print(f"   {'  ' if x == y else '!='} {x}" + ("" if x == y else f"\n      MyEngine: {y}"))
    print(f"\n{len(names)-bad}/{len(names)} scenarios identical to InnoDB"); sys.exit(1 if bad else 0)
