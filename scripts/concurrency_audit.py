#!/usr/bin/env python3
"""并发逻辑审计：用 InnoDB 做对照的一组探针（不是性能测试）。
覆盖：S->X 升级死锁、INSERT..ON DUPLICATE KEY UPDATE 并发计数、并发修改主键、同事务内 DELETE 再 INSERT 同一键 + 快照读者、
并发结束后没有残留的行锁/间隙锁。语句原子性（触发器）与 KILL 在 scripts/txn_check.py 里。
用法: scripts/concurrency_audit.py --socket /tmp/my.sock
"""
import argparse, multiprocessing as mp, random, threading, time
import pymysql

SOCK = None
def conn(db="audit"): return pymysql.connect(unix_socket=SOCK, user="root", database=db, autocommit=True)
def q(c, sql, p=None):
    try:
        cur = c.cursor(); cur.execute(sql, p); return ("ok", cur.fetchall() if cur.description else cur.rowcount)
    except pymysql.err.MySQLError as e: return ("err", e.args[0])

def worker(args):
    sock, eng, kind, w, n = args
    global SOCK; SOCK = sock; c = conn(); r = random.Random(w)
    for i in range(n):
        if kind == "odku":
            q(c, f"INSERT INTO cnt_{eng} VALUES (1,1) ON DUPLICATE KEY UPDATE n=n+1")
        elif kind == "pk":
            q(c, f"UPDATE pk_{eng} SET id=id+%s WHERE id=%s", (1000 + w * 100000 + i, r.randint(1, 40)))
        elif kind == "delins":
            k = r.randint(1, 30); c.begin()
            e1 = q(c, f"DELETE FROM di_{eng} WHERE id=%s", (k,)); e2 = q(c, f"INSERT INTO di_{eng} VALUES (%s,%s)", (k, w))
            c.commit() if e1[0] == "ok" and e2[0] == "ok" else c.rollback()

def snapshot_reader(args):
    sock, eng, secs = args
    global SOCK; SOCK = sock; c = conn(); bad = total = 0; end = time.time() + secs
    while time.time() < end:
        c.begin(); n1 = q(c, f"SELECT COUNT(*) FROM di_{eng}")[1][0][0]; time.sleep(0.002)
        n2 = q(c, f"SELECT COUNT(*) FROM di_{eng}")[1][0][0]; c.commit(); total += 1; bad += (n1 != 30 or n2 != 30)
    return bad, total

def blocked_for(c, sqls, limit=4):
    out = {}
    def run():
        for s in sqls: out[s] = q(c, s)
    t = threading.Thread(target=run); t0 = time.time(); t.start(); t.join(limit)
    return t.is_alive(), time.time() - t0

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--socket", required=True); a = ap.parse_args(); SOCK = a.socket
    root = pymysql.connect(unix_socket=SOCK, user="root", autocommit=True); root.cursor().execute("CREATE DATABASE IF NOT EXISTS audit")
    rows = []
    for eng in ("InnoDB", "MyEngine"):
        c = conn()
        for s in [f"DROP TABLE IF EXISTS s_{eng}", f"CREATE TABLE s_{eng} (id INT PRIMARY KEY, v INT) ENGINE={eng}", f"INSERT INTO s_{eng} VALUES (1,0)",
                  f"DROP TABLE IF EXISTS cnt_{eng}", f"CREATE TABLE cnt_{eng} (id INT PRIMARY KEY, n INT) ENGINE={eng}",
                  f"DROP TABLE IF EXISTS pk_{eng}", f"CREATE TABLE pk_{eng} (id INT PRIMARY KEY, v INT, KEY(v)) ENGINE={eng}",
                  f"INSERT INTO pk_{eng} VALUES " + ",".join(f"({i},{i})" for i in range(1, 41)),
                  f"DROP TABLE IF EXISTS di_{eng}", f"CREATE TABLE di_{eng} (id INT PRIMARY KEY, w INT) ENGINE={eng}",
                  f"INSERT INTO di_{eng} VALUES " + ",".join(f"({i},0)" for i in range(1, 31))]: q(c, s)
        # S->X 升级死锁：两个事务都先 FOR SHARE 再 UPDATE，恰好一个 1213
        x, y = conn(), conn(); q(x, "BEGIN"); q(y, "BEGIN")
        q(x, f"SELECT * FROM s_{eng} WHERE id=1 FOR SHARE"); q(y, f"SELECT * FROM s_{eng} WHERE id=1 FOR SHARE"); res = {}
        th = threading.Thread(target=lambda: res.__setitem__("x", q(x, f"UPDATE s_{eng} SET v=1 WHERE id=1"))); th.start(); time.sleep(0.4)
        res["y"] = q(y, f"UPDATE s_{eng} SET v=2 WHERE id=1"); q(y, "ROLLBACK"); th.join(10); q(x, "ROLLBACK")
        dl = sorted(v[1] for v in res.values() if v[0] == "err")
        with mp.Pool(8) as p: p.map(worker, [(SOCK, eng, "odku", w, 400) for w in range(8)])
        n = q(c, f"SELECT n FROM cnt_{eng}")[1][0][0]
        with mp.Pool(6) as p: p.map(worker, [(SOCK, eng, "pk", w, 150) for w in range(6)])
        r1 = q(c, f"SELECT COUNT(*) FROM pk_{eng}")[1][0][0]; r2 = q(c, f"SELECT COUNT(*) FROM pk_{eng} FORCE INDEX(v)")[1][0][0]
        r3 = q(c, f"SELECT COUNT(DISTINCT id) FROM pk_{eng}")[1][0][0]
        with mp.Pool(7) as p:
            rd = [p.apply_async(snapshot_reader, ((SOCK, eng, 4),)) for _ in range(2)]
            ws = [p.apply_async(worker, ((SOCK, eng, "delins", w, 200),)) for w in range(5)]
            [t.get() for t in ws]; rr = [t.get() for t in rd]
        bad, tot = sum(t[0] for t in rr), sum(t[1] for t in rr)
        # 残留锁探测：没有任何其他事务时，整表/两端的加锁读不应被阻塞
        stuck = []
        for tbl in (f"pk_{eng}", f"di_{eng}", f"cnt_{eng}", f"s_{eng}"):
            for sqls in (["BEGIN", f"SELECT * FROM {tbl} FOR UPDATE", "COMMIT"],
                         ["BEGIN", f"SELECT * FROM {tbl} WHERE id > 100000000 FOR UPDATE", f"SELECT * FROM {tbl} WHERE id < 0 FOR UPDATE", "COMMIT"]):
                if blocked_for(conn(), sqls)[0]: stuck.append(tbl)
        rows.append((eng, dl, n, (r1, r2, r3), (bad, tot), q(c, f"SELECT COUNT(*) FROM di_{eng}")[1][0][0], stuck))
    print(f"{'检查项':58s} {'InnoDB':>22s} {'MyEngine':>22s}")
    items = [("S->X 升级死锁：出错的错误码(期望恰好 [1213])", lambda r: str(r[1])),
             ("ODKU 并发计数 8x400 (期望 3200)", lambda r: str(r[2])),
             ("并发改主键：主索引行数/二级索引行数/不同id数 (期望 40/40/40)", lambda r: "/".join(map(str, r[3]))),
             ("DELETE+INSERT 同一键：快照里行数≠30 的次数/总次数 (期望 0/..)", lambda r: f"{r[4][0]}/{r[4][1]}"),
             ("DELETE+INSERT 结束后的行数 (期望 30)", lambda r: str(r[5])),
             ("残留的行锁/间隙锁（被阻塞的探测数，期望 0）", lambda r: str(len(r[6])))]
    for name, f in items: print(f"{name:58s} {f(rows[0]):>22s} {f(rows[1]):>22s}")
