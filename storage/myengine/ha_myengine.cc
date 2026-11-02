#include "storage/myengine/ha_myengine.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>

#include "my_dbug.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/key_spec.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/field.h"
#include "sql/key.h"
#include "sql/query_options.h"
#include "sql/table.h"
#include "libmyengine.h"  // cgo 生成的头文件

// 默认不打印调用轨迹；设置 MYENGINE_TRACE=1 打开。Go 端日志用 MYENGINE_VERBOSE=1 打开。
static const bool g_trace = getenv("MYENGINE_TRACE") != nullptr;
#define TRACE(fmt, ...) \
  do { if (g_trace) fprintf(stderr, "[myengine] %s " fmt "\n", __func__, ##__VA_ARGS__); } while (0)

// Go 端返回码
enum { RC_OK = 0, RC_NOT_FOUND = 1, RC_DUP = 2, RC_TOO_SMALL = 3, RC_DEADLOCK = 5, RC_TIMEOUT = 6, RC_KILLED = 7, RC_NOWAIT = 8 };
// 行锁模式
enum { LOCK_NONE = 0, LOCK_S = 1, LOCK_X = 2 };
// Go 端 seek 模式
enum { SEEK_EXACT = 0, SEEK_GE = 1, SEEK_AFTER = 2, SEEK_LE = 3, SEEK_LT = 4,
       SEEK_FIRST = 5, SEEK_LAST = 6, SEEK_PREFIX_LAST = 7 };

static handlerton *myengine_hton = nullptr;

static inline unsigned long long trx_of(THD *thd) {
  return (unsigned long long)(uintptr_t)thd_get_ha_data(thd, myengine_hton);
}
static inline void set_trx(THD *thd, unsigned long long h) {
  thd_set_ha_data(thd, myengine_hton, (void *)(uintptr_t)h);
}
static inline bool in_multi_stmt_txn(THD *thd) {
  return thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) != 0;
}

// ---------------------------------------------------------------- 事务钩子

// 每条顶层语句结束时 MySQL 都会调用 commit/rollback（all=false），不论成功与否、是否在多语句事务里。
// 用它来标记"语句结束"：之后的 external_lock/start_stmt 才算新语句（见 Txn.StmtBegin）。
static int myengine_commit(handlerton *, THD *thd, bool all) {
  unsigned long long h = trx_of(thd);
  if (!h) return 0;
  if (all || !in_multi_stmt_txn(thd)) {  // 事务结束（显式 COMMIT 或 autocommit 的语句结束）
    TRACE("commit all=%d", (int)all);
    MyEngine_TxnEnd(h, 1);
    set_trx(thd, 0);
  } else {
    MyEngine_TxnStmtEnd(h);  // 多语句事务里一条语句成功结束：事务继续，undo 留着
  }
  return 0;
}

static int myengine_rollback(handlerton *, THD *thd, bool all) {
  unsigned long long h = trx_of(thd);
  if (!h) return 0;
  if (all || !in_multi_stmt_txn(thd)) {
    TRACE("rollback all=%d", (int)all);
    MyEngine_TxnEnd(h, 0);
    set_trx(thd, 0);
  } else {
    TRACE("rollback statement");
    MyEngine_TxnRollbackStmt(h);  // 多语句事务里只撤销当前语句
    MyEngine_TxnStmtEnd(h);
  }
  return 0;
}

// 供 Go 端的锁等待轮询调用：连接是否被 KILL。
static int myengine_killed(void *thd) { return thd_killed(thd); }

static int myengine_close_connection(handlerton *, THD *thd) {
  unsigned long long h = trx_of(thd);
  if (h) {
    MyEngine_TxnEnd(h, 0);
    set_trx(thd, 0);
  }
  return 0;
}

static int myengine_savepoint_set(handlerton *, THD *thd, void *sv) {
  unsigned long long h = trx_of(thd);
  *(unsigned long long *)sv = h ? MyEngine_TxnSavepoint(h) : 0;
  return 0;
}
static int myengine_savepoint_rollback(handlerton *, THD *thd, void *sv) {
  unsigned long long h = trx_of(thd);
  if (h) MyEngine_TxnRollbackTo(h, *(unsigned long long *)sv);
  return 0;
}
static int myengine_savepoint_release(handlerton *, THD *, void *) { return 0; }

// START TRANSACTION WITH CONSISTENT SNAPSHOT：RR 下立即开始事务并建立视图。
static int myengine_start_consistent_snapshot(handlerton *, THD *thd) {
  int iso = thd_tx_isolation(thd);
  unsigned long long h = trx_of(thd);
  if (!h) {
    h = MyEngine_TxnBegin(iso);
    MyEngine_TxnSetKill(h, (void *)myengine_killed, thd);
    set_trx(thd, h);
  }
  trans_register_ha(thd, true, myengine_hton, nullptr);  // 让 COMMIT/ROLLBACK 时我们的钩子被调用
  if (iso == ISO_REPEATABLE_READ || iso == ISO_SERIALIZABLE) MyEngine_TxnSnapshot(h);
  return 0;
}

static handler *myengine_create_handler(handlerton *hton, TABLE_SHARE *table, bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_myengine(hton, table);
}

static int myengine_init(void *p) {
  handlerton *hton = static_cast<handlerton *>(p);
  myengine_hton = hton;
  hton->state = SHOW_OPTION_YES;
  hton->create = myengine_create_handler;
  hton->flags = HTON_CAN_RECREATE;
  hton->commit = myengine_commit;
  hton->rollback = myengine_rollback;
  hton->close_connection = myengine_close_connection;
  hton->start_consistent_snapshot = myengine_start_consistent_snapshot;
  hton->savepoint_offset = sizeof(unsigned long long);
  hton->savepoint_set = myengine_savepoint_set;
  hton->savepoint_rollback = myengine_savepoint_rollback;
  hton->savepoint_release = myengine_savepoint_release;
  return 0;
}

ha_myengine::ha_myengine(handlerton *hton, TABLE_SHARE *table_arg) : handler(hton, table_arg) {}

// ---------------------------------------------------------------- 键编码

uint ha_myengine::part_width(const Field *f) const {
  uint w;
  switch (f->type()) {
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING: {
      const CHARSET_INFO *cs = f->charset();
      w = (uint)cs->coll->strnxfrmlen(cs, f->field_length);
      break;
    }
    default:
      w = f->pack_length();
  }
  return w + (f->is_nullable() ? 1 : 0);
}

void ha_myengine::compute_widths() {
  key_width.assign(table->s->keys, 0);
  for (uint i = 0; i < table->s->keys; i++) {
    KEY *ki = &table->key_info[i];
    for (uint p = 0; p < ki->user_defined_key_parts; p++) key_width[i] += part_width(ki->key_part[p].field);
  }
  pk_width = table->s->keys ? key_width[0] : 0;
}

// 把 rec 中索引 idx 的前 nparts 个 key part 编码为可 memcmp 比较的字节串。
void ha_myengine::key_for(uint idx, const uchar *rec, uint nparts, std::string &out) const {
  KEY *ki = &table->key_info[idx];
  ptrdiff_t diff = rec - table->record[0];
  out.clear();
  for (uint p = 0; p < nparts; p++) {
    Field *f = ki->key_part[p].field;
    uint w = part_width(f);
    f->move_field_offset(diff);
    size_t base = out.size();
    out.resize(base + w, '\0');
    uchar *dst = reinterpret_cast<uchar *>(&out[base]);
    if (f->is_nullable()) {
      if (f->is_real_null()) {
        dst[0] = 0;  // NULL 排在最前
      } else {
        dst[0] = 1;
        f->make_sort_key(dst + 1, w - 1);
      }
    } else {
      f->make_sort_key(dst, w);
    }
    f->move_field_offset(-diff);
  }
}

bool ha_myengine::key_has_null(uint idx, const uchar *rec) const {
  KEY *ki = &table->key_info[idx];
  ptrdiff_t diff = rec - table->record[0];
  for (uint p = 0; p < ki->user_defined_key_parts; p++) {
    Field *f = ki->key_part[p].field;
    f->move_field_offset(diff);
    bool null = f->is_nullable() && f->is_real_null();
    f->move_field_offset(-diff);
    if (null) return true;
  }
  return false;
}

// 打包所有二级索引的键：重复 [idx u32][flags u32 (bit0=唯一且不含 NULL)][len u32][key]
void ha_myengine::pack_sec(const uchar *rec, std::string &out) const {
  out.clear();
  for (uint i = 1; i < table->s->keys; i++) {
    KEY *ki = &table->key_info[i];
    std::string k;
    key_for(i, rec, ki->user_defined_key_parts, k);
    uint32_t idx = i, flags = ((ki->flags & HA_NOSAME) && !key_has_null(i, rec)) ? 1 : 0, len = (uint32_t)k.size();
    out.append((const char *)&idx, 4);
    out.append((const char *)&flags, 4);
    out.append((const char *)&len, 4);
    out.append(k);
  }
}

// ---------------------------------------------------------------- 生命周期

int ha_myengine::create(const char *name, TABLE *form, HA_CREATE_INFO *, dd::Table *) {
  TRACE("name=%s", name);
  // 引擎不支持外键：绝不能像 MyISAM 那样静默忽略（那样孤儿行会悄悄写进去，InnoDB 会报 1452）。
  // 对没有 HTON_SUPPORTS_FOREIGN_KEYS 的引擎，服务器在调用 create() 之前就把外键从表定义里丢掉了，
  // 引擎唯一能看到外键子句的地方是当前语句的 alter_info->key_list。
  THD *cur_thd = current_thd;
  if (cur_thd != nullptr && cur_thd->lex != nullptr && cur_thd->lex->alter_info != nullptr) {
    for (const Key_spec *k : cur_thd->lex->alter_info->key_list) {
      if (k->type == KEYTYPE_FOREIGN) {
        my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "MyEngine", "FOREIGN KEY");
        return HA_WRONG_CREATE_OPTION;
      }
    }
  }
  if (form->s->primary_key == MAX_KEY) {
    my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "MyEngine", "table without PRIMARY KEY");
    return HA_WRONG_CREATE_OPTION;
  }
  if (form->s->blob_fields) {
    my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "MyEngine", "BLOB/TEXT columns");
    return HA_WRONG_CREATE_OPTION;
  }
  for (uint i = 0; i < form->s->keys; i++) {
    KEY *ki = &form->key_info[i];
    for (uint p = 0; p < ki->user_defined_key_parts; p++) {
      KEY_PART_INFO *kp = &ki->key_part[p];
      if (kp->key_part_flag & HA_PART_KEY_SEG) {
        my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "MyEngine", "prefix index");
        return HA_WRONG_CREATE_OPTION;
      }
      if (kp->key_part_flag & HA_REVERSE_SORT) {
        my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "MyEngine", "descending index");
        return HA_WRONG_CREATE_OPTION;
      }
    }
  }
  return MyEngine_Create(const_cast<char *>(name), form->s->keys);
}

int ha_myengine::open(const char *name, int, uint, const dd::Table *) {
  TRACE("name=%s", name);
  tbl_name = name;
  thr_lock_init(&lock);
  thr_lock_data_init(&lock, &lock_data, nullptr);
  compute_widths();
  ref_length = pk_width;
  tbl_h = MyEngine_TableOpen(const_cast<char *>(name), table->s->keys);
  return 0;
}

int ha_myengine::close() {
  TRACE("");
  close_cursor();
  MyEngine_TableClose(tbl_h);
  tbl_h = 0;
  thr_lock_delete(&lock);
  return 0;
}

int ha_myengine::delete_table(const char *name, const dd::Table *) {
  TRACE("name=%s", name);
  return MyEngine_Drop(const_cast<char *>(name));
}

int ha_myengine::rename_table(const char *from, const char *to, const dd::Table *, dd::Table *) {
  TRACE("%s -> %s", from, to);
  return MyEngine_Rename(const_cast<char *>(from), const_cast<char *>(to));
}

// ---------------------------------------------------------------- 错误码映射

int ha_myengine::map_rc(int rc, int not_found_code) {
  switch (rc) {
    case RC_OK: return 0;
    case RC_DEADLOCK:
      thd_mark_transaction_to_rollback(ha_thd(), 1);  // 与 InnoDB 一致：死锁时整个事务回滚
      return HA_ERR_LOCK_DEADLOCK;
    case RC_TIMEOUT: return HA_ERR_LOCK_WAIT_TIMEOUT;
    case RC_NOWAIT:  // NOWAIT：与 InnoDB 一致，报 ER_LOCK_NOWAIT(3572)
      my_error(ER_LOCK_NOWAIT, MYF(0));
      return HA_ERR_NO_WAIT_LOCK;
    case RC_KILLED:  // 与 InnoDB 一致：设置 kill 状态（客户端收到 1317），返回通用错误
      thd_set_kill_status(ha_thd());
      return HA_ERR_GENERIC;
    default: return not_found_code;
  }
}

// ---------------------------------------------------------------- 写路径

void ha_myengine::init_auto_inc() {
  ulonglong max = 0;
  std::vector<uchar> buf(table->s->reclength);
  size_t len = 0;
  uint ni = table->s->next_number_index;
  if (MyEngine_IndexLastLatest(tbl_h, ni, pk_width, buf.data(), buf.size(), &len) == RC_OK) {
    Field *f = table->found_next_number_field;
    ptrdiff_t diff = buf.data() - table->record[0];
    f->move_field_offset(diff);
    longlong v = f->val_int();
    f->move_field_offset(-diff);
    max = (f->is_unsigned() || v > 0) ? (ulonglong)v : 0;
  }
  MyEngine_AutoInit(tbl_h, max);
}

void ha_myengine::get_auto_increment(ulonglong offset, ulonglong increment, ulonglong, ulonglong *first_value,
                                     ulonglong *nb_reserved_values) {
  for (int attempt = 0; attempt < 2; attempt++) {
    unsigned long long first = 0;
    if (MyEngine_AutoReserve(tbl_h, offset, increment, 1, &first) == 0) {
      *first_value = first;
      *nb_reserved_values = 1;  // 每次只预留 1 个：不产生空洞，取号本身很便宜
      return;
    }
    init_auto_inc();
  }
  *first_value = ~(ulonglong)0;  // 出错
}

int ha_myengine::write_row(uchar *buf) {
  if (table->next_number_field && buf == table->record[0]) {
    int e = update_auto_increment();
    if (e) return e;
  }
  std::string pk, sec;
  key_for(0, buf, table->key_info[0].user_defined_key_parts, pk);
  pack_sec(buf, sec);
  TRACE("pk_len=%zu", pk.size());
  int dup = 0;
  int rc = MyEngine_RowInsert(txn_h, tbl_h, &pk[0], pk.size(), buf, table->s->reclength, sec.data(), sec.size(), &dup);
  if (rc == RC_DUP) {
    last_dup_key = dup;
    return HA_ERR_FOUND_DUPP_KEY;
  }
  int e = map_rc(rc, HA_ERR_INTERNAL_ERROR);
  if (!e && table->next_number_field) {  // 显式给出了更大的自增值：抬高计数器
    longlong v = table->next_number_field->val_int();
    if (table->next_number_field->is_unsigned() || v > 0) MyEngine_AutoBump(tbl_h, (ulonglong)v);
  }
  return e;
}

int ha_myengine::update_row(const uchar *old_data, uchar *new_data) {
  std::string old_pk, new_pk, sec;
  key_for(0, old_data, table->key_info[0].user_defined_key_parts, old_pk);
  key_for(0, new_data, table->key_info[0].user_defined_key_parts, new_pk);
  pack_sec(new_data, sec);
  TRACE("pk_changed=%d", (int)(old_pk != new_pk));
  int dup = 0;
  int rc = MyEngine_RowUpdate(txn_h, tbl_h, &old_pk[0], old_pk.size(), &new_pk[0], new_pk.size(), new_data,
                              table->s->reclength, sec.data(), sec.size(), &dup);
  if (rc == RC_DUP) {
    last_dup_key = dup;
    return HA_ERR_FOUND_DUPP_KEY;
  }
  return map_rc(rc, HA_ERR_KEY_NOT_FOUND);
}

int ha_myengine::delete_row(const uchar *buf) {
  std::string pk;
  key_for(0, buf, table->key_info[0].user_defined_key_parts, pk);
  TRACE("");
  return map_rc(MyEngine_RowDelete(txn_h, tbl_h, &pk[0], pk.size()), HA_ERR_INTERNAL_ERROR);
}

// ---------------------------------------------------------------- 游标与读路径

void ha_myengine::close_cursor() {
  if (cur) MyEngine_CursorClose(cur);
  cur = 0;
}

void ha_myengine::open_cursor(uint idx) {
  close_cursor();
  cur = MyEngine_CursorOpen(txn_h, tbl_h, idx, row_lock, pk_width, wait_policy);
  if (semi_consistent) MyEngine_CursorSemi(cur, 1);
}

// 每行只跨一次语言边界：Go 端完成 移动游标 + 可见性判断/加锁 + (二级索引)回表 + 拷出整行。
int ha_myengine::step(uchar *buf, int dir) {
  if (!cur) return HA_ERR_END_OF_FILE;
  size_t len = 0;
  return map_rc(MyEngine_CursorFetch(cur, dir, buf, table->s->reclength, &len), HA_ERR_END_OF_FILE);
}

int ha_myengine::seek_fetch(uchar *buf, const void *key, size_t klen, int mode, int flags) {
  size_t len = 0;
  return map_rc(MyEngine_CursorSeekFetch(cur, const_cast<void *>(key), klen, mode, flags, buf,
                                         table->s->reclength, &len),
                HA_ERR_END_OF_FILE);
}

// 把 MySQL 的 key_range（key 格式）编码成与存储一致的可比较键。
void ha_myengine::encode_range_key(uint idx, const key_range *kr, std::string &out) {
  KEY *ki = &table->key_info[idx];
  uint nparts = std::min<uint>(__builtin_popcountll(kr->keypart_map), ki->user_defined_key_parts);
  uint key_len = calculate_key_len(table, idx, kr->keypart_map);
  if (key_scratch.size() < table->s->rec_buff_length) key_scratch.resize(table->s->rec_buff_length);
  memset(key_scratch.data(), 0, key_scratch.size());
  key_restore(key_scratch.data(), kr->key, ki, key_len);
  key_for(idx, key_scratch.data(), nparts, out);
}

// 优化器估算范围内的行数。没有它优化器选不出 range/ref，只能全表扫描——而全表扫描在 RR 下会锁住所有间隙。
// 至少返回 1（与 InnoDB 一致：估算为 0 时按 1 处理，避免被当成"不可能的范围"）。
ha_rows ha_myengine::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
  std::string lo, hi;
  int has_lo = 0, lo_incl = 1, has_hi = 0, hi_incl = 1;
  if (min_key) {
    encode_range_key(inx, min_key, lo);
    has_lo = 1;
    lo_incl = min_key->flag != HA_READ_AFTER_KEY;  // KEY_EXACT / KEY_OR_NEXT 含起点，AFTER_KEY 不含
  }
  if (max_key) {
    encode_range_key(inx, max_key, hi);
    has_hi = 1;
    hi_incl = max_key->flag != HA_READ_BEFORE_KEY;  // AFTER_KEY 含终点，BEFORE_KEY 不含
  }
  long long n = MyEngine_RecordsInRange(tbl_h, inx, lo.data(), lo.size(), has_lo, lo_incl, hi.data(), hi.size(),
                                        has_hi, hi_incl, 10000);
  return n < 1 ? 1 : (ha_rows)n;
}

// 把 MySQL 的范围终点（handler::end_range）编码后告诉 Go 端游标：越过终点的第一个条目只让它前面的间隙被锁住，
// 而不会锁它的行（InnoDB 的做法）。每次定位前都要调用，因为首个候选也可能已经越界。
// dir: 本次定位的扫描方向（+1 正向 / -1 反向）。handler::range_scan_direction 是私有成员，所以由调用者按定位方式给出。
void ha_myengine::apply_end_bound(int dir) {
  if (!cur) return;
  if (end_range == nullptr) {
    MyEngine_CursorBound(cur, 0, nullptr, 0, 0, 0);
    return;
  }
  KEY *ki = &table->key_info[active_index];
  uint nparts = std::min<uint>(__builtin_popcountll(end_range->keypart_map), ki->user_defined_key_parts);
  uint key_len = calculate_key_len(table, active_index, end_range->keypart_map);
  if (key_scratch.size() < table->s->rec_buff_length) key_scratch.resize(table->s->rec_buff_length);
  memset(key_scratch.data(), 0, key_scratch.size());
  key_restore(key_scratch.data(), end_range->key, ki, key_len);
  std::string k;
  key_for(active_index, key_scratch.data(), nparts, k);
  // 终点是唯一索引的全键（且不含 NULL）：扫到恰好等于终点的记录就结束，不再读后面的记录
  int uniq = (nparts == ki->user_defined_key_parts && (ki->flags & HA_NOSAME) &&
              !key_has_null(active_index, key_scratch.data()))
                 ? 1
                 : 0;
  // 端点相等时是否算在范围内——与 handler::compare_key_icp 一致：key_compare_result_on_equal 对 BEFORE_KEY 为 +1(范围外)、
  // 对 AFTER_KEY 为 -1(范围内)、其它为 0(范围内)，反向扫描时再取反。所以正向：flag != BEFORE_KEY 算包含；反向：flag != AFTER_KEY 算包含。
  int incl = dir > 0 ? (end_range->flag != HA_READ_BEFORE_KEY) : (end_range->flag != HA_READ_AFTER_KEY);
  MyEngine_CursorBound(cur, dir > 0 ? 2 : 3, k.data(), k.size(), incl, uniq);
}

int ha_myengine::rnd_init(bool scan) {
  TRACE("scan=%d", (int)scan);
  open_cursor(0);
  scan_started = false;
  return 0;
}

// 服务器在 READ COMMITTED 下发现当前行不满足 WHERE 时调用：把这次扫描刚取到的行锁提前放掉（与 InnoDB 一致）。
bool ha_myengine::was_semi_consistent_read() { return cur && MyEngine_CursorWasSemi(cur) != 0; }

void ha_myengine::try_semi_consistent_read(bool yes) {
  semi_consistent = yes;
  if (cur) MyEngine_CursorSemi(cur, yes ? 1 : 0);
}

void ha_myengine::unlock_row() {
  if (cur) MyEngine_CursorUnlockRow(cur);
}

int ha_myengine::rnd_end() {
  TRACE("");
  close_cursor();
  return 0;
}

int ha_myengine::rnd_next(uchar *buf) {
  int e = scan_started ? step(buf, 1) : seek_fetch(buf, nullptr, 0, SEEK_FIRST);
  scan_started = true;
  return e;
}

void ha_myengine::position(const uchar *record) {
  std::string pk;
  key_for(0, record, table->key_info[0].user_defined_key_parts, pk);
  memcpy(ref, pk.data(), pk.size());
}

int ha_myengine::rnd_pos(uchar *buf, uchar *pos) {
  size_t len = 0;
  int rc = MyEngine_GetRow(txn_h, tbl_h, pos, pk_width, row_lock, wait_policy, buf, table->s->reclength, &len);
  return map_rc(rc, HA_ERR_KEY_NOT_FOUND);
}

int ha_myengine::index_init(uint idx, bool sorted) {
  TRACE("idx=%u sorted=%d", idx, (int)sorted);
  active_index = idx;
  open_cursor(idx);
  return 0;
}

int ha_myengine::index_end() {
  TRACE("");
  close_cursor();
  active_index = MAX_KEY;
  return 0;
}

int ha_myengine::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                                enum ha_rkey_function find_flag) {
  TRACE("idx=%u keypart_map=%lu find_flag=%d", active_index, (unsigned long)keypart_map, (int)find_flag);
  int mode;
  switch (find_flag) {
    case HA_READ_KEY_EXACT: mode = SEEK_EXACT; break;
    case HA_READ_KEY_OR_NEXT: mode = SEEK_GE; break;
    case HA_READ_AFTER_KEY: mode = SEEK_AFTER; break;
    case HA_READ_KEY_OR_PREV: mode = SEEK_LE; break;
    case HA_READ_BEFORE_KEY: mode = SEEK_LT; break;
    // PREFIX_LAST：这个键（前缀）的最后一行，之后沿同键往回走；PREFIX_LAST_OR_PREV：<= 该键的最后一行，
    // 是反向范围扫描的起点，之后要越过这个键继续往回走——所以不能带同键前缀边界，映射成 LE。
    case HA_READ_PREFIX_LAST: mode = SEEK_PREFIX_LAST; break;
    case HA_READ_PREFIX_LAST_OR_PREV: mode = SEEK_LE; break;
    default: return HA_ERR_UNSUPPORTED;
  }
  apply_end_bound(mode == SEEK_LE || mode == SEEK_LT || mode == SEEK_PREFIX_LAST ? -1 : 1);  // 先于定位：首个候选也可能已经越过范围终点
  // 把 key 格式还原进私有缓冲区（绝不能写 record[0]：INSERT ... ON DUPLICATE KEY UPDATE / REPLACE
  // 正是在 record[0] 里放着待插入的新行，同时让我们把已存在的行读到 record[1]），再编码成可比较键。
  KEY *ki = &table->key_info[active_index];
  uint key_len = calculate_key_len(table, active_index, keypart_map);
  // HA_WHOLE_KEY 是全 1 位图，popcount 会远超索引列数，必须截断
  uint nparts = std::min<uint>(__builtin_popcountll(keypart_map), ki->user_defined_key_parts);
  if (key_scratch.size() < table->s->rec_buff_length) key_scratch.resize(table->s->rec_buff_length);
  memset(key_scratch.data(), 0, key_scratch.size());
  key_restore(key_scratch.data(), key, ki, key_len);
  std::string k;
  key_for(active_index, key_scratch.data(), nparts, k);
  // 唯一索引的全键（且不含 NULL）的等值查找或范围起点 >= K：K 恰好存在时只需要记录锁，不需要间隙锁
  int flags = ((find_flag == HA_READ_KEY_EXACT || find_flag == HA_READ_KEY_OR_NEXT) && nparts == ki->user_defined_key_parts && (ki->flags & HA_NOSAME) &&
               !key_has_null(active_index, key_scratch.data()))
                  ? 1
                  : 0;
  int e = seek_fetch(buf, k.data(), k.size(), mode, flags);
  return e == HA_ERR_END_OF_FILE ? HA_ERR_KEY_NOT_FOUND : e;
}

// 读取与给定键（前缀）匹配的最后一行（ORDER BY ... DESC 的 ref 访问、MAX() 优化会用到），之后用 index_prev 往回走。
int ha_myengine::index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map) {
  TRACE("keypart_map=%lu", (unsigned long)keypart_map);
  return index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST);
}

int ha_myengine::index_next(uchar *buf) { TRACE(""); return step(buf, 1); }
int ha_myengine::index_prev(uchar *buf) { TRACE(""); return step(buf, -1); }
int ha_myengine::index_first(uchar *buf) {
  TRACE("");
  apply_end_bound(1);
  return seek_fetch(buf, nullptr, 0, SEEK_FIRST);
}
int ha_myengine::index_last(uchar *buf) {
  TRACE("");
  apply_end_bound(-1);
  return seek_fetch(buf, nullptr, 0, SEEK_LAST);
}

// ---------------------------------------------------------------- 统计、语句边界与锁

int ha_myengine::info(uint flag) {
  if (flag & HA_STATUS_ERRKEY) errkey = last_dup_key;
  long long n = tbl_h ? MyEngine_Count(tbl_h) : 0;
  stats.records = (ha_rows)n;
  stats.deleted = 0;
  stats.mean_rec_length = table->s->reclength;
  stats.data_file_length = (ulonglong)n * table->s->reclength;
  stats.index_file_length = 0;
  return 0;
}

// 语句开始：取得/创建事务，登记到 MySQL 的事务管理，决定本语句对该表的行锁模式。
int ha_myengine::begin_stmt(THD *thd) {
  int iso = thd_tx_isolation(thd);
  unsigned long long h = trx_of(thd);
  if (!h) {
    h = MyEngine_TxnBegin(iso);
    MyEngine_TxnSetKill(h, (void *)myengine_killed, thd);
    set_trx(thd, h);
  }
  MyEngine_TxnStmtBegin(h, iso);
  trans_register_ha(thd, false, ht, nullptr);                     // 语句级
  if (in_multi_stmt_txn(thd)) trans_register_ha(thd, true, ht, nullptr);  // 事务级
  txn_h = h;

  if (stmt_lock_type == TL_READ_WITH_SHARED_LOCKS) {
    row_lock = LOCK_S;                                  // SELECT ... FOR SHARE
  } else if (stmt_lock_type >= TL_WRITE_ALLOW_WRITE) {
    row_lock = LOCK_X;                                  // UPDATE / DELETE / INSERT / SELECT ... FOR UPDATE
  } else if (iso == ISO_SERIALIZABLE && in_multi_stmt_txn(thd)) {
    row_lock = LOCK_S;                                  // SERIALIZABLE：普通读也加 S 锁（无间隙锁）
  } else {
    row_lock = LOCK_NONE;                               // 一致性(快照)读
  }
  TRACE("txn=%llu iso=%d row_lock=%d", h, iso, row_lock);
  return 0;
}

int ha_myengine::external_lock(THD *thd, int lock_type) {
  if (lock_type != F_UNLCK) return begin_stmt(thd);
  return 0;
}

int ha_myengine::start_stmt(THD *thd, thr_lock_type) { return begin_stmt(thd); }

THR_LOCK_DATA **ha_myengine::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE) {
    stmt_lock_type = lock_type;  // 记住原始锁类型，用于决定行锁模式
    // SELECT ... FOR UPDATE NOWAIT / SKIP LOCKED：策略由 TABLE_LIST 的 lock_descriptor 给出
    switch (table->pos_in_table_list->lock_descriptor().action) {
      case THR_NOWAIT: wait_policy = 1; break;
      case THR_SKIP: wait_policy = 2; break;
      default: wait_policy = 0; break;
    }
  }
  if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK) {
    // 行级并发由引擎自己的行锁负责：把表级写锁降级，避免不同连接在表级互相阻塞（InnoDB 的做法）
    if (lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE && !thd_in_lock_tables(thd))
      lock_type = TL_WRITE_ALLOW_WRITE;
    else if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
      lock_type = TL_READ;
    lock_data.type = lock_type;
  }
  *to++ = &lock_data;
  return to;
}

struct st_mysql_storage_engine myengine_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(myengine){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &myengine_storage_engine,
    "MyEngine",
    "myengine",
    "In-memory MVCC B+ tree storage engine: MySQL -> C++ -> CGO -> Go",
    PLUGIN_LICENSE_GPL,
    myengine_init,
    nullptr,
    nullptr,
    0x0003,
    nullptr,
    nullptr,
    nullptr,
    0,
} mysql_declare_plugin_end;
