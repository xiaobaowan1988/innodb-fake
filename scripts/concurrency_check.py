#!/usr/bin/env python3
"""并发正确性检查（不是性能）：同样的并发负载在 MyEngine 与 InnoDB 上各跑一遍，InnoDB 作为参照。
用法: scripts/concurrency_check.py --socket /tmp/my.sock
"""
import argparse, multiprocessing as mp, random, time
import pymysql

W = 8

def conn(a, db="cc"):
    return pymysql.connect(unix_socket=a.socket, user="root", database=db, autocommit=True)

def setup(a, eng):
    c = pymysql.connect(unix_socket=a.socket, user="root", autocommit=True); cur = c.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS cc"); cur.execute("USE cc")
    for t in ("cnt", "uq", "ai", "tg"): cur.execute(f"DROP TABLE IF EXISTS {t}_{eng}")
    cur.execute(f"CREATE TABLE cnt_{eng} (id INT PRIMARY KEY, n INT) ENGINE={eng}"); cur.execute(f"INSERT INTO cnt_{eng} VALUES (1,0)")
    cur.execute(f"CREATE TABLE uq_{eng} (pk INT PRIMARY KEY, v INT, UNIQUE KEY uv(v)) ENGINE={eng}")
    cur.execute(f"CREATE TABLE ai_{eng} (id INT AUTO_INCREMENT PRIMARY KEY, w INT) ENGINE={eng}")
    cur.execute(f"CREATE TABLE tg_{eng} (id INT PRIMARY KEY, g INT, KEY kg(g)) ENGINE={eng}")
    cur.execute(f"INSERT INTO tg_{eng} VALUES " + ",".join(f"({i},{i})" for i in range(1, 1001)))
    c.close()

# ---- T1 丢失更新：8 个客户端各 500 次 n=n+1
def t1(args):
    a, eng, w = args; cur = conn(a).cursor()
    for _ in range(500): cur.execute(f"UPDATE cnt_{eng} SET n=n+1 WHERE id=1")

# ---- T2 UNIQUE 竞争：所有客户端抢同样的 300 个 v
def t2(args):
    a, eng, w = args; cur = conn(a).cursor(); ok = 0
    vs = list(range(300)); random.Random(w).shuffle(vs)
    for v in vs:
        try: cur.execute(f"INSERT INTO uq_{eng} VALUES (%s,%s)", (w * 10000 + v, v)); ok += 1
        except pymysql.err.IntegrityError: pass
    return ok

# ---- T3 读一致性：写者改二级索引列，读者数 "用二级索引扫描得到的行数"（应恒为 1000）
def t3_writer(args):
    a, eng, n = args; cur = conn(a).cursor(); r = random.Random(5)
    for _ in range(n): cur.execute(f"UPDATE tg_{eng} SET g=%s WHERE id=%s", (r.randint(0, 5000), r.randint(1, 1000)))
    return 0
def t3_reader(args):
    a, eng, secs = args; cur = conn(a).cursor(); bad = 0; total = 0; seen = set(); end = time.time() + secs
    while time.time() < end:
        cur.execute(f"SELECT COUNT(*) FROM tg_{eng} FORCE INDEX(kg) WHERE g >= 0"); c = cur.fetchone()[0]
        total += 1
        if c != 1000: bad += 1; seen.add(c)
    return bad, total, sorted(seen)[:6]

# ---- T4 AUTO_INCREMENT 并发分配
def t4(args):
    a, eng, w = args; cur = conn(a).cursor(); err = 0
    for _ in range(300):
        try: cur.execute(f"INSERT INTO ai_{eng} (w) VALUES (%s)", (w,))
        except Exception: err += 1
    return err

def run(a, eng):
    setup(a, eng); res = {}; c = conn(a).cursor()
    with mp.Pool(W) as p: p.map(t1, [(a, eng, w) for w in range(W)])
    c.execute(f"SELECT n FROM cnt_{eng}"); res["T1 丢失更新: n (期望 4000)"] = c.fetchone()[0]
    with mp.Pool(W) as p: oks = p.map(t2, [(a, eng, w) for w in range(W)])
    c.execute(f"SELECT COUNT(*) FROM uq_{eng}"); rows = c.fetchone()[0]
    c.execute(f"SELECT COUNT(*) FROM (SELECT v FROM uq_{eng} IGNORE INDEX(uv) GROUP BY v HAVING COUNT(*)>1) x"); dups = c.fetchone()[0]
    res["T2 UNIQUE 竞争: 行数 (期望 300) / 重复的 v 个数 (期望 0)"] = f"{rows} / {dups}"
    with mp.Pool(4) as p:
        wr = p.apply_async(t3_writer, ((a, eng, 4000),)); rd = [p.apply_async(t3_reader, ((a, eng, 6),)) for _ in range(3)]
        wr.get(); r = [x.get() for x in rd]
    bad = sum(x[0] for x in r); tot = sum(x[1] for x in r); seen = sorted(set(v for x in r for v in x[2]))[:6]
    res["T3 读一致性: 二级索引扫描行数≠1000 的次数 / 总次数"] = f"{bad} / {tot}" + (f"  (出现过的错误行数: {seen})" if bad else "")
    c.execute(f"SELECT COUNT(*) FROM tg_{eng}"); res["T3b 更新后主索引总行数 (期望 1000)"] = c.fetchone()[0]
    c.execute(f"SELECT COUNT(*) FROM tg_{eng} FORCE INDEX(kg) WHERE g>=0"); res["T3c 更新后二级索引行数 (期望 1000)"] = c.fetchone()[0]
    with mp.Pool(W) as p: errs = p.map(t4, [(a, eng, w) for w in range(W)])
    c.execute(f"SELECT COUNT(*), COUNT(DISTINCT id) FROM ai_{eng}"); n, d = c.fetchone()
    res["T4 自增并发: 报错次数 / 行数 (期望 0 / 2400)"] = f"{sum(errs)} / {n}"
    return res

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--socket", required=True); a = ap.parse_args()
    out = {e: run(a, e) for e in ("InnoDB", "MyEngine")}
    keys = list(out["InnoDB"])
    print(f"{'检查项':62s} {'InnoDB':>14s} {'MyEngine':>14s}")
    for k in keys: print(f"{k:62s} {str(out['InnoDB'][k]):>14s} {str(out['MyEngine'][k]):>14s}")
