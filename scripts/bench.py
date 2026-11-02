#!/usr/bin/env python3
"""并发压测：同一份数据在 MyEngine 与 InnoDB 上跑相同负载，对比吞吐与延迟。

用法: scripts/bench.py --socket /tmp/my.sock [--rows 50000] [--secs 4] [--conc 1,2,4,8]
每个客户端是独立进程(避开 Python GIL)、独立长连接、autocommit。
注意：压测客户端与 mysqld 共用同一台机器的 CPU，绝对数字只用于相对比较。
"""
import argparse, json, multiprocessing as mp, os, random, subprocess, time
import pymysql

def connect(a, db="bench"):
    return pymysql.connect(unix_socket=a.socket, user="root", database=db, autocommit=True)

def load(a):
    c = pymysql.connect(unix_socket=a.socket, user="root", autocommit=True)
    cur = c.cursor()
    cur.execute("SET GLOBAL innodb_flush_log_at_trx_commit=2")  # 避免 InnoDB 每次提交 fsync，公平一些
    cur.execute("DROP DATABASE IF EXISTS bench"); cur.execute("CREATE DATABASE bench"); cur.execute("USE bench")
    for eng in ("MyEngine", "InnoDB"):
        cur.execute(f"CREATE TABLE t_{eng} (id INT PRIMARY KEY, grp INT, pad VARCHAR(32), KEY k_grp(grp)) ENGINE={eng}")
        for lo in range(1, a.rows + 1, 5000):
            vals = ",".join(f"({i},{i % 100},'row{i}')" for i in range(lo, min(lo + 5000, a.rows + 1)))
            cur.execute(f"INSERT INTO t_{eng} VALUES {vals}")
    c.close()

def worker(args):
    a, eng, wl, wid, secs, run_id = args
    rnd = random.Random(wid * 7919 + 1)
    conn = connect(a); cur = conn.cursor(); t = f"t_{eng}"; n = a.rows
    next_id = n + 1 + (run_id * 16 + wid) * 1_000_000  # 每个(轮次,进程)独立的插入主键区间
    def op():
        nonlocal next_id
        if wl == "point":
            cur.execute(f"SELECT * FROM {t} WHERE id=%s", (rnd.randint(1, n),)); cur.fetchall()
        elif wl == "range100":
            x = rnd.randint(1, n - 100); cur.execute(f"SELECT * FROM {t} WHERE id BETWEEN %s AND %s", (x, x + 99)); cur.fetchall()
        elif wl == "secscan":  # 二级索引上约 rows/100 行，且需回表
            cur.execute(f"SELECT SUM(id) FROM {t} FORCE INDEX(k_grp) WHERE grp=%s", (rnd.randint(0, 99),)); cur.fetchall()
        elif wl == "insert":
            next_id += 1; cur.execute(f"INSERT INTO {t} VALUES (%s,%s,'x')", (next_id, next_id % 100))
        elif wl == "mixed":  # 90% 点查 + 10% 更新
            if rnd.random() < 0.9:
                cur.execute(f"SELECT * FROM {t} WHERE id=%s", (rnd.randint(1, n),)); cur.fetchall()
            else:
                cur.execute(f"UPDATE {t} SET pad='u' WHERE id=%s", (rnd.randint(1, n),))
    for _ in range(50): op()  # 预热
    lat = []; end = time.time() + secs
    while time.time() < end:
        s = time.perf_counter(); op(); lat.append(time.perf_counter() - s)
    conn.close(); return lat

RUN = [0]
def mysqld_cpu():  # mysqld 累计 CPU 秒（用户+内核）
    try:
        pid = subprocess.check_output(["pgrep", "-x", "mysqld"]).split()[0].decode()
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")
    except Exception:
        return 0.0
def run(a, eng, wl, conc):
    RUN[0] += 1
    c0, t0 = mysqld_cpu(), time.time()
    with mp.Pool(conc) as p:
        res = p.map(worker, [(a, eng, wl, i, a.secs, RUN[0]) for i in range(conc)])
    cpu = (mysqld_cpu() - c0) / (time.time() - t0) * 100
    lat = sorted(x for r in res for x in r)
    return {"mysqld_cpu_pct": cpu, "ops": len(lat), "qps": len(lat) / a.secs, "p50_ms": lat[len(lat) // 2] * 1e3, "p99_ms": lat[int(len(lat) * .99)] * 1e3}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--socket", required=True); ap.add_argument("--rows", type=int, default=50000)
    ap.add_argument("--secs", type=float, default=4); ap.add_argument("--conc", default="1,2,4,8")
    ap.add_argument("--workloads", default="point,range100,secscan,insert,mixed"); ap.add_argument("--json")
    a = ap.parse_args(); load(a)
    out = {}
    for wl in a.workloads.split(","):
        for conc in map(int, a.conc.split(",")):
            for eng in ("MyEngine", "InnoDB"):
                r = run(a, eng, wl, conc); out[f"{wl}|{conc}|{eng}"] = r
                print(f"{wl:9s} c={conc} {eng:8s} qps={r['qps']:9.0f} p50={r['p50_ms']:7.3f}ms p99={r['p99_ms']:7.3f}ms mysqld_cpu={r['mysqld_cpu_pct']:4.0f}%", flush=True)
    if a.json: json.dump(out, open(a.json, "w"), indent=1)
