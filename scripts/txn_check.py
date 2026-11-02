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
    for t in ("t", "u", "ai"): cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, k INT, KEY(k)) ENGINE=__E__".replace("__E__", c.eng))
    cur.execute("INSERT INTO t VALUES (1,10,1),(2,20,2),(3,30,3)")
    cur.execute("CREATE TABLE u (id INT PRIMARY KEY, e INT, UNIQUE KEY ue(e)) ENGINE=__E__".replace("__E__", c.eng))
    cur.execute("CREATE TABLE ai (id INT AUTO_INCREMENT PRIMARY KEY, v INT) ENGINE=__E__".replace("__E__", c.eng))

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

NEEDS_TIMEOUT = {"lock_wait_timeout"}

def run_one(a, name, eng):
    c = Ctx(a, eng); fresh(c)
    try: SC[name](c)
    except Exception as e: c.log.append(f"EXCEPTION {type(e).__name__}: {e}")
    for s in c.sessions.values():
        try: s.close()
        except Exception: pass
    return c.log

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
