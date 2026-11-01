#ifndef HA_MYENGINE_H
#define HA_MYENGINE_H

#include "my_base.h"
#include "sql/handler.h"
#include "thr_lock.h"

class ha_myengine : public handler {
  THR_LOCK_DATA lock_data;
  THR_LOCK lock;

 public:
  ha_myengine(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_myengine() override = default;

  const char *table_type() const override { return "MyEngine"; }
  ulonglong table_flags() const override { return HA_NO_TRANSACTIONS; }
  ulong index_flags(uint, uint, bool) const override { return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE; }
  uint max_supported_keys() const override { return 64; }
  uint max_supported_key_parts() const override { return 16; }
  uint max_supported_key_length() const override { return 3072; }
  uint max_supported_key_part_length(HA_CREATE_INFO *) const override { return 3072; }

  int create(const char *name, TABLE *form, HA_CREATE_INFO *info, dd::Table *dd) override;
  int open(const char *name, int mode, uint test_if_locked, const dd::Table *dd) override;
  int close() override;

  int write_row(uchar *buf) override;

  int rnd_init(bool scan) override;
  int rnd_next(uchar *buf) override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  void position(const uchar *record) override;

  int index_init(uint idx, bool sorted) override;
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;

  int info(uint flag) override;
  int external_lock(THD *thd, int lock_type) override;
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) override;
};

#endif
