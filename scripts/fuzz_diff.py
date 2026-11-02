#!/usr/bin/env python3
"""差分模糊测试：随机的多会话 SQL 序列，在 InnoDB 与 MyEngine 上用同样的种子、同样的交错执行，
逐条对比结果（行、影响行数、错误码）以及"哪些会话被阻塞"，发现第一处分歧就停下来报告。

比较规则：死锁牺牲者的选择与 InnoDB 不同（InnoDB 按事务权重选，本引擎选形成环的请求者），一旦出现 1213，之后双方状态就会分叉，
所以只比较**第一次 1213 出现之前**的内容；另外每个引擎各自检查"最终状态下主索引与各二级索引互相一致"（不依赖 InnoDB）。

用法: scripts/fuzz_diff.py --socket /tmp/my.sock [--seeds 0-39] [--steps 70] [--sessions 3]
要求 mysqld 以 MYENGINE_LOCK_WAIT_SECS=3 启动；脚本会把 InnoDB 的 innodb_lock_wait_timeout 也设成 3 秒。
"""
import argparse, random, sys, threading, time
import pymysql

SOCK = None
PROBE = 0.08  # 判定"被阻塞"所等待的时间

def conn(db):
    return pymysql.connect(unix_socket=SOCK, user="root", database=db, autocommit=True)

def run_sql(c, sql):
    try:
        cur = c.cursor(); n = cur.execute(sql)
        return ("rows", [list(r) for r in cur.fetchall()]) if cur.description else ("ok", n)
    except pymysql.err.MySQLError as e:
        return ("err", e.args[0])

class Session:
    def __init__(self, db, name):
        self.c = conn(db); self.name = name; self.thread = None; self.res = None; self.sql = None

    def start(self, sql):
        self.sql = sql; self.res = None
        def go(): self.res = run_sql(self.c, sql)
        self.thread = threading.Thread(target=go); self.thread.start()

    def settle(self, timeout):
        if self.thread is not None:
            self.thread.join(timeout)
            if not self.thread.is_alive(): self.thread = None
        return self.thread is None

    @property
    def blocked(self): return self.thread is not None

def gen(r, in_txn_state):
    """生成一条随机语句。键空间很小，让冲突、重复键、间隙锁频繁发生。"""
    i = lambda: r.randint(1, 12)
    k = lambda: r.randint(0, 4)
    u = lambda: r.choice(["NULL"] + [str(x) for x in range(0, 14)])
    v = lambda: r.randint(0, 99)
    lo = r.randint(0, 11); hi = lo + r.randint(0, 5)
    c = r.random()
    if c < 0.17: return f"INSERT INTO t VALUES ({i()},{k()},{u()},{v()})"
    if c < 0.22: return f"INSERT INTO t VALUES ({i()},{k()},{u()},{v()}),({i()},{k()},{u()},{v()})"
    if c < 0.25: return f"INSERT IGNORE INTO t VALUES ({i()},{k()},{u()},{v()})"
    if c < 0.28: return f"INSERT INTO t VALUES ({i()},{k()},{u()},{v()}) ON DUPLICATE KEY UPDATE v=v+1"
    if c < 0.30: return f"REPLACE INTO t VALUES ({i()},{k()},{u()},{v()})"
    if c < 0.37: return f"UPDATE t SET v={v()} WHERE id={i()}"
    if c < 0.40: return f"UPDATE t SET v=v+1 WHERE k={k()}"
    if c < 0.42: return f"UPDATE t SET k={k()} WHERE id BETWEEN {lo} AND {hi}"
    if c < 0.44: return f"UPDATE t SET u={u()} WHERE id={i()}"
    if c < 0.45: return f"UPDATE t SET id={i()} WHERE id={i()}"
    if c < 0.50: return f"DELETE FROM t WHERE id={i()}"
    if c < 0.52: return f"DELETE FROM t WHERE k={k()}"
    if c < 0.54: return f"DELETE FROM t WHERE id BETWEEN {lo} AND {hi}"
    if c < 0.60: return f"SELECT * FROM t ORDER BY id"
    if c < 0.63: return f"SELECT * FROM t WHERE k={k()} ORDER BY id"
    if c < 0.66: return f"SELECT * FROM t WHERE id BETWEEN {lo} AND {hi} ORDER BY id DESC"
    if c < 0.68: return f"SELECT COUNT(*), SUM(v) FROM t"
    if c < 0.71: return f"SELECT * FROM t WHERE id={i()} FOR UPDATE"
    if c < 0.73: return f"SELECT * FROM t WHERE id={i()} FOR SHARE"
    if c < 0.76: return f"SELECT * FROM t WHERE id BETWEEN {lo} AND {hi} ORDER BY id FOR UPDATE"
    if c < 0.78: return f"SELECT * FROM t WHERE k={k()} ORDER BY id FOR SHARE"
    if c < 0.79: return f"SELECT * FROM t WHERE u={u() if r.random() < .9 else 3} ORDER BY id FOR UPDATE"
    if c < 0.80: return f"SELECT * FROM t WHERE id={i()} FOR UPDATE NOWAIT"
    if c < 0.81: return f"SELECT * FROM t ORDER BY id FOR UPDATE SKIP LOCKED"
    if c < 0.89: return "BEGIN"
    if c < 0.94: return "COMMIT"
    if c < 0.97: return "ROLLBACK"
    if c < 0.985: return f"SAVEPOINT sp{r.randint(1,2)}"
    return f"ROLLBACK TO SAVEPOINT sp{r.randint(1,2)}"

PURGE_GAP = 0.12

def one_run(seed, steps, nsess, isos):
    logs = {}
    for eng in ("InnoDB", "MyEngine"):
        db = f"fz_{eng}"
        root = conn("mysql"); cur = root.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}"); cur.execute(f"USE {db}"); cur.execute("DROP TABLE IF EXISTS t")
        cur.execute(f"CREATE TABLE t (id INT PRIMARY KEY, k INT, u INT, v INT, KEY ik(k), UNIQUE KEY uu(u)) ENGINE={eng}")
        cur.execute("INSERT INTO t VALUES (2,1,5,10),(4,2,7,20),(6,1,NULL,30),(8,3,9,40),(10,0,11,50)")
        cur.execute("SET GLOBAL innodb_lock_wait_timeout=20")
        r = random.Random(seed); sess = [Session(db, f"S{n+1}") for n in range(nsess)]
        for s, iso in zip(sess, isos):
            run_sql(s.c, f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
        log = []
        def harvest():
            for s in sess:
                if s.blocked and s.settle(0.04):
                    log.append(f"   {s.name} resumed: {s.sql} -> {s.res}")
        for step in range(steps):
            free = [s for s in sess if not s.blocked]
            if not free:
                log.append("   ALL SESSIONS BLOCKED"); break
            s = r.choice(free); sql = gen(r, None)
            s.start(sql); s.settle(PROBE)
            if not s.blocked: time.sleep(PURGE_GAP)  # 本引擎的 purge 每 50ms 一轮，InnoDB 近乎即时；留出时间避免把清理时机差当成逻辑错误
            log.append(f"{step:3d} {s.name}: {sql} -> " + ("BLOCKED" if s.blocked else str(s.res)))
            harvest()
        # 收尾：让还在阻塞的会话都能结束（先回滚持锁者）
        for _ in range(4):
            for s in sess:
                if not s.blocked: run_sql(s.c, "ROLLBACK")
            time.sleep(0.05); harvest()
            if not any(s.blocked for s in sess): break
        for s in sess: s.settle(5)
        # 最终状态：主索引与各二级索引的内容都必须一致
        fin = []
        for q in ("SELECT * FROM t ORDER BY id", "SELECT id,k FROM t FORCE INDEX(ik) ORDER BY k,id", "SELECT id,u FROM t FORCE INDEX(uu) WHERE u IS NOT NULL ORDER BY u"):
            fin.append(f"   final {q} -> {run_sql(cur.connection, q)}")
        logs[eng] = log + fin
        for s in sess: s.c.close()
        root.close()
    return logs["InnoDB"], logs["MyEngine"]

def self_consistent(log):
    """最终状态里主索引扫描与两个二级索引的强制扫描必须互相一致。返回问题描述或 None。"""
    fin = [l for l in log if l.startswith("   final")]
    if len(fin) != 3: return "missing final dump"
    ev = lambda l: eval(l.split(" -> ", 1)[1])
    pk, ik, uu = ev(fin[0]), ev(fin[1]), ev(fin[2])
    if pk[0] != "rows" or ik[0] != "rows" or uu[0] != "rows": return f"final scans failed: {pk[0]} {ik[0]} {uu[0]}"
    rows = pk[1]
    if ik[1] != [[r[0], r[1]] for r in sorted(rows, key=lambda r: (r[1], r[0]))]: return f"index ik disagrees with primary scan: {ik[1]} vs {rows}"
    if uu[1] != [[r[0], r[2]] for r in sorted((r for r in rows if r[2] is not None), key=lambda r: (r[2], r[0]))]: return f"index uu disagrees with primary scan: {uu[1]} vs {rows}"
    return None

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--socket", required=True)
    ap.add_argument("--seeds", default="0-29"); ap.add_argument("--steps", type=int, default=70); ap.add_argument("--sessions", type=int, default=3)
    ap.add_argument("--iso", default="REPEATABLE READ,READ COMMITTED,REPEATABLE READ"); ap.add_argument("--stop-first", action="store_true")
    a = ap.parse_args(); SOCK = a.socket
    lo, hi = map(int, a.seeds.split("-")); isos = a.iso.split(",")
    bad = []; compared = 0; total_lines = 0
    for seed in range(lo, hi + 1):
        li, lm = one_run(seed, a.steps, a.sessions, isos)
        problems = []
        for eng, log in (("InnoDB", li), ("MyEngine", lm)):
            pr = self_consistent(log)
            if pr: problems.append(f"{eng}: {pr}")
        # 第一次 1213 之前必须逐行一致
        cut = min([n for n, l in enumerate(li) if "1213" in l] + [n for n, l in enumerate(lm) if "1213" in l] + [min(len(li), len(lm))])
        div = next((n for n in range(cut) if (li[n] if n < len(li) else None) != (lm[n] if n < len(lm) else None)), None)
        compared += cut; total_lines += len(li)
        if div is None and not problems:
            print(f"seed {seed}: ok  (compared {cut}/{len(li)} lines{'' if cut >= len(li) - 3 else ', then a deadlock made the runs diverge by design'})"); continue
        print(f"seed {seed}: PROBLEM" + (f"  {problems}" if problems else ""))
        if div is not None:
            for n in range(max(0, div - 6), min(cut + 2, div + 3)):
                x = li[n] if n < len(li) else "<none>"; y = lm[n] if n < len(lm) else "<none>"
                print(f"   {'  ' if x == y else '!='} {x}" + ("" if x == y else f"\n      MyEngine: {y}"))
        bad.append(seed)
        if a.stop_first: break
    print(f"\n{len(bad)} problem seeds: {bad}" if bad else f"\nno problems ({compared} lines compared across {hi-lo+1} seeds)")
    sys.exit(1 if bad else 0)
