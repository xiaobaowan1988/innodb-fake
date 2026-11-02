#ifndef HA_MYENGINE_H
#define HA_MYENGINE_H

#include <string>
#include <vector>

#include "my_base.h"
#include "sql/handler.h"
#include "thr_lock.h"

/*
  数据布局与事务语义见 docs/plan-mvcc.md。
    主索引(idx 0)  : 主键的可比较编码          -> 版本链（每个版本含整行 record[0] 字节）
    二级索引(idx n): 索引键编码 + 主键编码     -> 空（条目不带版本，读时回主索引核对）
  键编码：每个 key part = [可空则 1 字节 null 标记] + Field::make_sort_key 定宽输出，可直接 memcmp 比较。
  事务：MVCC 快照读 + 行级 S/X 锁 + 死锁检测；RR/RC 隔离；语句级回滚与 SAVEPOINT。
  限制：必须有主键；不支持 BLOB/TEXT、前缀索引、降序索引；无间隙锁(加锁读可能幻读)；仅内存，无持久化。
*/
class ha_myengine : public handler {
  THR_LOCK_DATA lock_data;
  THR_LOCK lock;  // 仅为满足接口；并发控制由引擎内的行锁完成，DDL 互斥由 MDL 保证

  std::string tbl_name;
  std::vector<uint> key_width;  // 每个索引的编码宽度（不含主键后缀）
  uint pk_width = 0;
  unsigned long long tbl_h = 0;   // Go 端表句柄
  unsigned long long txn_h = 0;   // 当前语句所属事务句柄
  unsigned long long cur = 0;     // 当前游标句柄，0 表示无
  int row_lock = 0;               // 本语句对该表的行锁模式：0 一致性读，1 S，2 X
  enum thr_lock_type stmt_lock_type = TL_UNLOCK;  // store_lock 收到的原始锁类型
  bool scan_started = false;
  std::vector<uchar> key_scratch;  // index_read_map 还原查找键用的私有缓冲区
  uint last_dup_key = 0;

  void key_for(uint idx, const uchar *rec, uint nparts, std::string &out) const;
  bool key_has_null(uint idx, const uchar *rec) const;
  void pack_sec(const uchar *rec, std::string &out) const;
  uint part_width(const Field *f) const;
  void compute_widths();
  void close_cursor();
  void open_cursor(uint idx);
  int begin_stmt(THD *thd);
  int map_rc(int rc, int not_found_code);
  int step(uchar *buf, int dir);
  int seek_fetch(uchar *buf, const void *key, size_t klen, int mode);
  void init_auto_inc();

 public:
  ha_myengine(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_myengine() override = default;

  const char *table_type() const override { return "MyEngine"; }
  ulonglong table_flags() const override {
    return HA_NULL_IN_KEY | HA_PRIMARY_KEY_IN_READ_INDEX | HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE;
  }
  ulong index_flags(uint, uint, bool) const override {
    return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE;
  }
  uint max_supported_keys() const override { return 64; }
  uint max_supported_key_parts() const override { return 16; }
  uint max_supported_key_length() const override { return 3072; }
  uint max_supported_key_part_length(HA_CREATE_INFO *) const override { return 3072; }

  int create(const char *name, TABLE *form, HA_CREATE_INFO *info, dd::Table *dd) override;
  int open(const char *name, int mode, uint test_if_locked, const dd::Table *dd) override;
  int close() override;
  int delete_table(const char *name, const dd::Table *dd) override;
  int rename_table(const char *from, const char *to, const dd::Table *from_dd, dd::Table *to_dd) override;

  int write_row(uchar *buf) override;
  int update_row(const uchar *old_data, uchar *new_data) override;
  int delete_row(const uchar *buf) override;
  void get_auto_increment(ulonglong offset, ulonglong increment, ulonglong nb_desired_values,
                          ulonglong *first_value, ulonglong *nb_reserved_values) override;

  int rnd_init(bool scan) override;
  int rnd_end() override;
  int rnd_next(uchar *buf) override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  void position(const uchar *record) override;

  int index_init(uint idx, bool sorted) override;
  int index_end() override;
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;
  int index_next(uchar *buf) override;
  int index_prev(uchar *buf) override;
  int index_first(uchar *buf) override;
  int index_last(uchar *buf) override;

  int info(uint flag) override;
  int external_lock(THD *thd, int lock_type) override;
  int start_stmt(THD *thd, thr_lock_type lock_type) override;
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) override;
};

#endif
