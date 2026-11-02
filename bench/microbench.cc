// 脱离 MySQL 的微基准：用与 ha_myengine 相同的调用序列驱动 Go 库，对比"二级索引扫描一行"的成本。
// 用法: microbench [rows]
//   old = 旧接口，每行 3 次跨语言调用：CursorStep + CursorKey + Get(回表)
//   new = 新接口，每行 1 次：CursorFetch（Go 端完成 移动 + 回表 + 拷出整行）
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "libmyengine.h"

static double now() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
static void be32(unsigned char *p, unsigned v) { p[0] = (v >> 24) ^ 0x80; p[1] = v >> 16; p[2] = v >> 8; p[3] = v; }

int main(int argc, char **argv) {
  int n = argc > 1 ? atoi(argv[1]) : 50000;
  const size_t RECLEN = 150, PKW = 4;
  char *tbl = (char *)"mb";
  MyEngine_Create(tbl);
  unsigned char pk[4], sk[9], row[RECLEN];
  memset(row, 'x', sizeof row);
  for (int i = 1; i <= n; i++) {  // 与 handler 一致：主索引 + 二级索引(null标记+grp+pk)
    be32(pk, i);
    MyEngine_Put(tbl, 0, pk, 4, row, RECLEN, 0);
    sk[0] = 1; be32(sk + 1, i % 100); memcpy(sk + 5, pk, 4);
    MyEngine_Put(tbl, 1, sk, 9, nullptr, 0, 1);
  }
  const int Q = 3000, PER = n / 100;  // 每次查询扫描一个 grp：n/100 行
  unsigned char q[5], out[RECLEN]; q[0] = 1;
  for (int round = 0; round < 2; round++) {
    for (int v = 0; v < 2; v++) {
      double t0 = now(); long rows = 0;
      for (int it = 0; it < Q; it++) {
        be32(q + 1, it % 100);
        unsigned long long c = MyEngine_CursorOpen(tbl, 1);
        size_t klen = 0, vlen = 0;
        unsigned char kbuf[600];
        if (v == 0) {
          int rc = MyEngine_CursorSeek(c, q, 5, 0);
          while (rc == 0) {
            MyEngine_CursorKey(c, kbuf, sizeof kbuf, &klen);
            MyEngine_Get(tbl, 0, kbuf + klen - PKW, PKW, out, RECLEN, &vlen);
            rows++;
            rc = MyEngine_CursorStep(c, 1);
            if (rc == 0 && ++klen && rows % PER == 0) break;  // MySQL 在 C++ 侧判断 grp 是否变化；这里用行数代替
          }
        } else {
          int rc = MyEngine_CursorSeekFetch(c, q, 5, 0, PKW, out, RECLEN, &vlen);
          while (rc == 0) {
            rows++;
            if (rows % PER == 0) break;
            rc = MyEngine_CursorFetch(c, 1, PKW, out, RECLEN, &vlen);
          }
        }
        MyEngine_CursorClose(c);
      }
      double dt = now() - t0;
      if (round == 1) printf("%-34s %6.0f ns/行   %6.0f 次查询/秒\n", v == 0 ? "old: Step+Key+Get (3次跨语言/行)" : "new: Fetch (1次跨语言/行)", dt / rows * 1e9, Q / dt);
    }
  }
  MyEngine_ProfileStop();
  return 0;
}
