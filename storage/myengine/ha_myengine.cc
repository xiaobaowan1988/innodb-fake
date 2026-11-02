#include "storage/myengine/ha_myengine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "my_dbug.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/field.h"
#include "sql/key.h"
#include "sql/table.h"
#include "libmyengine.h"  // cgo 生成的头文件

// 默认打印调用轨迹；设置 MYENGINE_QUIET=1 可关闭。Go 端日志用 MYENGINE_VERBOSE=1 打开。
static const bool g_trace = getenv("MYENGINE_QUIET") == nullptr;
#define TRACE(fmt, ...) \
  do { if (g_trace) fprintf(stderr, "[myengine] %s " fmt "\n", __func__, ##__VA_ARGS__); } while (0)

// Go 端返回码
enum { RC_OK = 0, RC_NOT_FOUND = 1, RC_DUP = 2, RC_TOO_SMALL = 3, RC_ROW_MISSING = 4 };
// Go 端 seek 模式
enum { SEEK_EXACT = 0, SEEK_GE = 1, SEEK_AFTER = 2, SEEK_LE = 3, SEEK_LT = 4,
       SEEK_FIRST = 5, SEEK_LAST = 6, SEEK_PREFIX_LAST = 7 };

static handler *myengine_create_handler(handlerton *hton, TABLE_SHARE *table, bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_myengine(hton, table);
}

static int myengine_init(void *p) {
  handlerton *hton = static_cast<handlerton *>(p);
  hton->state = SHOW_OPTION_YES;
  hton->create = myengine_create_handler;
  hton->flags = HTON_CAN_RECREATE;
  TRACE("plugin loaded");
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

// ---------------------------------------------------------------- 生命周期

int ha_myengine::create(const char *name, TABLE *form, HA_CREATE_INFO *, dd::Table *) {
  TRACE("name=%s", name);
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
      if ((kp->key_part_flag & HA_PART_KEY_SEG)) {
        my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "MyEngine", "prefix index");
        return HA_WRONG_CREATE_OPTION;
      }
      if (kp->key_part_flag & HA_REVERSE_SORT) {
        my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "MyEngine", "descending index");
        return HA_WRONG_CREATE_OPTION;
      }
    }
  }
  return MyEngine_Create(const_cast<char *>(name));
}

int ha_myengine::open(const char *name, int, uint, const dd::Table *) {
  TRACE("name=%s", name);
  tbl_name = name;
  thr_lock_init(&lock);
  thr_lock_data_init(&lock, &lock_data, nullptr);
  compute_widths();
  ref_length = pk_width;
  return MyEngine_Open(const_cast<char *>(name));
}

int ha_myengine::close() {
  TRACE("");
  close_cursor();
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

// ---------------------------------------------------------------- 写路径

static inline int rc_to_ha(int rc) { return rc == RC_OK ? 0 : HA_ERR_INTERNAL_ERROR; }

// 二级 UNIQUE 索引冲突检查（含 NULL 的键允许重复）
bool ha_myengine::unique_conflict(uint idx, const uchar *rec) {
  KEY *ki = &table->key_info[idx];
  for (uint p = 0; p < ki->user_defined_key_parts; p++) {
    Field *f = ki->key_part[p].field;
    ptrdiff_t diff = rec - table->record[0];
    f->move_field_offset(diff);
    bool null = f->is_nullable() && f->is_real_null();
    f->move_field_offset(-diff);
    if (null) return false;
  }
  std::string k;
  key_for(idx, rec, ki->user_defined_key_parts, k);
  unsigned long long c = MyEngine_CursorOpen(const_cast<char *>(tbl_name.c_str()), idx);
  bool hit = MyEngine_CursorSeek(c, &k[0], k.size(), SEEK_EXACT) == RC_OK;
  MyEngine_CursorClose(c);
  return hit;
}

int ha_myengine::insert_index_entries(const uchar *rec, const std::string &pk) {
  for (uint i = 1; i < table->s->keys; i++) {
    std::string k;
    key_for(i, rec, table->key_info[i].user_defined_key_parts, k);
    k += pk;
    MyEngine_Put(const_cast<char *>(tbl_name.c_str()), i, &k[0], k.size(), nullptr, 0, 1);
  }
  return 0;
}

void ha_myengine::delete_index_entries(const uchar *rec, const std::string &pk) {
  for (uint i = 1; i < table->s->keys; i++) {
    std::string k;
    key_for(i, rec, table->key_info[i].user_defined_key_parts, k);
    k += pk;
    MyEngine_Delete(const_cast<char *>(tbl_name.c_str()), i, &k[0], k.size());
  }
}

int ha_myengine::write_row(uchar *buf) {
  if (table->next_number_field && buf == table->record[0]) {
    int e = update_auto_increment();
    if (e) return e;
  }
  std::string pk;
  key_for(0, buf, table->key_info[0].user_defined_key_parts, pk);
  TRACE("pk_len=%zu", pk.size());
  for (uint i = 1; i < table->s->keys; i++) {
    if ((table->key_info[i].flags & HA_NOSAME) && unique_conflict(i, buf)) {
      last_dup_key = i;
      return HA_ERR_FOUND_DUPP_KEY;
    }
  }
  int rc = MyEngine_Put(const_cast<char *>(tbl_name.c_str()), 0, &pk[0], pk.size(), buf, table->s->reclength, 0);
  if (rc == RC_DUP) {
    last_dup_key = 0;
    return HA_ERR_FOUND_DUPP_KEY;
  }
  if (rc != RC_OK) return HA_ERR_INTERNAL_ERROR;
  insert_index_entries(buf, pk);
  return 0;
}

int ha_myengine::update_row(const uchar *old_data, uchar *new_data) {
  std::string old_pk, new_pk;
  key_for(0, old_data, table->key_info[0].user_defined_key_parts, old_pk);
  key_for(0, new_data, table->key_info[0].user_defined_key_parts, new_pk);
  TRACE("pk_changed=%d", (int)(old_pk != new_pk));
  char *tn = const_cast<char *>(tbl_name.c_str());
  // 唯一二级索引：键变化时才检查
  for (uint i = 1; i < table->s->keys; i++) {
    if (!(table->key_info[i].flags & HA_NOSAME)) continue;
    std::string ok, nk;
    key_for(i, old_data, table->key_info[i].user_defined_key_parts, ok);
    key_for(i, new_data, table->key_info[i].user_defined_key_parts, nk);
    if (ok != nk && unique_conflict(i, new_data)) {
      last_dup_key = i;
      return HA_ERR_FOUND_DUPP_KEY;
    }
  }
  if (old_pk != new_pk) {
    if (MyEngine_Put(tn, 0, &new_pk[0], new_pk.size(), new_data, table->s->reclength, 0) == RC_DUP) {
      last_dup_key = 0;
      return HA_ERR_FOUND_DUPP_KEY;
    }
    MyEngine_Delete(tn, 0, &old_pk[0], old_pk.size());
  } else {
    MyEngine_Put(tn, 0, &new_pk[0], new_pk.size(), new_data, table->s->reclength, 1);
  }
  delete_index_entries(old_data, old_pk);
  insert_index_entries(new_data, new_pk);
  return 0;
}

int ha_myengine::delete_row(const uchar *buf) {
  std::string pk;
  key_for(0, buf, table->key_info[0].user_defined_key_parts, pk);
  TRACE("");
  MyEngine_Delete(const_cast<char *>(tbl_name.c_str()), 0, &pk[0], pk.size());
  delete_index_entries(buf, pk);
  return 0;
}

int ha_myengine::delete_all_rows() {
  TRACE("");
  return MyEngine_Create(const_cast<char *>(tbl_name.c_str()));
}

// ---------------------------------------------------------------- 游标与读路径

void ha_myengine::close_cursor() {
  if (cur) MyEngine_CursorClose(cur);
  cur = 0;
}

void ha_myengine::open_cursor(uint idx) {
  close_cursor();
  cur = MyEngine_CursorOpen(const_cast<char *>(tbl_name.c_str()), idx);
}

// Go 端合并接口的返回码 -> handler 错误码。
static inline int fetch_rc(int rc) {
  if (rc == RC_OK) return 0;
  return rc == RC_ROW_MISSING ? HA_ERR_KEY_NOT_FOUND : HA_ERR_END_OF_FILE;
}

// 每行只跨一次语言边界：Go 端完成 移动游标 + (二级索引)回表 + 拷出整行。
int ha_myengine::step(uchar *buf, int dir) {
  if (!cur) return HA_ERR_END_OF_FILE;
  size_t len = 0;
  return fetch_rc(MyEngine_CursorFetch(cur, dir, pk_width, buf, table->s->reclength, &len));
}

int ha_myengine::seek_fetch(uchar *buf, const void *key, size_t klen, int mode) {
  size_t len = 0;
  return fetch_rc(MyEngine_CursorSeekFetch(cur, const_cast<void *>(key), klen, mode, pk_width, buf,
                                           table->s->reclength, &len));
}

int ha_myengine::rnd_init(bool scan) {
  TRACE("scan=%d", (int)scan);
  open_cursor(0);
  scan_started = false;
  return 0;
}

int ha_myengine::rnd_end() {
  TRACE("");
  close_cursor();
  return 0;
}

int ha_myengine::rnd_next(uchar *buf) {
  int e = scan_started ? step(buf, 1) : seek_fetch(buf, nullptr, 0, SEEK_FIRST);
  scan_started = true;
  if (e == HA_ERR_END_OF_FILE) TRACE("-> HA_ERR_END_OF_FILE");
  return e;
}

void ha_myengine::position(const uchar *record) {
  std::string pk;
  key_for(0, record, table->key_info[0].user_defined_key_parts, pk);
  memcpy(ref, pk.data(), pk.size());
}

int ha_myengine::rnd_pos(uchar *buf, uchar *pos) {
  size_t len = 0;
  int rc = MyEngine_Get(const_cast<char *>(tbl_name.c_str()), 0, pos, pk_width, buf, table->s->reclength, &len);
  return rc == RC_OK ? 0 : HA_ERR_KEY_NOT_FOUND;
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
    case HA_READ_PREFIX_LAST:
    case HA_READ_PREFIX_LAST_OR_PREV: mode = SEEK_PREFIX_LAST; break;
    default: return HA_ERR_UNSUPPORTED;
  }
  // 把 key 格式还原进 record[0]，再按 Field 编码成与存储一致的可比较键
  uint key_len = calculate_key_len(table, active_index, keypart_map);
  uint nparts = __builtin_popcountll(keypart_map);
  key_restore(table->record[0], key, &table->key_info[active_index], key_len);
  std::string k;
  key_for(active_index, table->record[0], nparts, k);
  int e = seek_fetch(buf, k.data(), k.size(), mode);
  if (e == HA_ERR_END_OF_FILE && find_flag == HA_READ_PREFIX_LAST_OR_PREV) {
    e = seek_fetch(buf, k.data(), k.size(), SEEK_LE);
  }
  return e == HA_ERR_END_OF_FILE ? HA_ERR_KEY_NOT_FOUND : e;
}

int ha_myengine::index_next(uchar *buf) { TRACE(""); return step(buf, 1); }
int ha_myengine::index_prev(uchar *buf) { TRACE(""); return step(buf, -1); }

int ha_myengine::index_first(uchar *buf) {
  TRACE("");
  return seek_fetch(buf, nullptr, 0, SEEK_FIRST);
}

int ha_myengine::index_last(uchar *buf) {
  TRACE("");
  return seek_fetch(buf, nullptr, 0, SEEK_LAST);
}

// ---------------------------------------------------------------- 统计与锁

int ha_myengine::info(uint flag) {
  long long n = MyEngine_Count(const_cast<char *>(tbl_name.c_str()));
  TRACE("flag=%u records=%lld", flag, n);
  if (flag & HA_STATUS_ERRKEY) errkey = last_dup_key;
  stats.records = (ha_rows)n;
  stats.deleted = 0;
  stats.mean_rec_length = table->s->reclength;
  stats.data_file_length = (ulonglong)n * table->s->reclength;
  stats.index_file_length = 0;
  return 0;
}

int ha_myengine::external_lock(THD *, int lock_type) {
  TRACE("lock_type=%d", lock_type);
  return 0;
}

THR_LOCK_DATA **ha_myengine::store_lock(THD *, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK) lock_data.type = lock_type;
  *to++ = &lock_data;
  return to;
}

struct st_mysql_storage_engine myengine_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(myengine){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &myengine_storage_engine,
    "MyEngine",
    "myengine",
    "In-memory B+ tree storage engine: MySQL -> C++ -> CGO -> Go",
    PLUGIN_LICENSE_GPL,
    myengine_init,
    nullptr,
    nullptr,
    0x0002,
    nullptr,
    nullptr,
    nullptr,
    0,
} mysql_declare_plugin_end;
