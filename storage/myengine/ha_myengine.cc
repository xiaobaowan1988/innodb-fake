#include "storage/myengine/ha_myengine.h"

#include <stdio.h>

#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/table.h"
#include "libmyengine.h"  // cgo 生成的头文件

#define TRACE(fmt, ...) fprintf(stderr, "[myengine] %s " fmt "\n", __func__, ##__VA_ARGS__)

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

int ha_myengine::create(const char *name, TABLE *, HA_CREATE_INFO *, dd::Table *) {
  TRACE("name=%s", name);
  return 0;
}

int ha_myengine::open(const char *name, int, uint, const dd::Table *) {
  TRACE("name=%s", name);
  thr_lock_init(&lock);
  thr_lock_data_init(&lock, &lock_data, nullptr);
  return MyEngine_Open(const_cast<char *>(name));
}

int ha_myengine::close() {
  TRACE("");
  thr_lock_delete(&lock);
  return MyEngine_Close(const_cast<char *>(table->s->normalized_path.str));
}

int ha_myengine::write_row(uchar *buf) {
  TRACE("reclength=%lu", (unsigned long)table->s->reclength);
  // 假装写入成功：交给 Go 端 Mock 吞掉
  return MyEngine_Put(buf, table->s->reclength, nullptr, 0);
}

int ha_myengine::rnd_init(bool scan) {
  TRACE("scan=%d", (int)scan);
  return 0;
}

int ha_myengine::rnd_next(uchar *) {
  TRACE("-> HA_ERR_END_OF_FILE");
  return HA_ERR_END_OF_FILE;
}

int ha_myengine::rnd_pos(uchar *, uchar *) {
  TRACE("");
  return HA_ERR_END_OF_FILE;
}

void ha_myengine::position(const uchar *) { TRACE(""); }

int ha_myengine::index_init(uint idx, bool sorted) {
  TRACE("idx=%u sorted=%d", idx, (int)sorted);
  active_index = idx;
  return 0;
}

int ha_myengine::index_read_map(uchar *, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag) {
  TRACE("keypart_map=%lu find_flag=%d", (unsigned long)keypart_map, (int)find_flag);
  MyEngine_Seek(const_cast<uchar *>(key), 0);
  return HA_ERR_KEY_NOT_FOUND;
}

int ha_myengine::info(uint flag) {
  TRACE("flag=%u", flag);
  stats.records = 0;
  stats.deleted = 0;
  stats.data_file_length = 0;
  stats.index_file_length = 0;
  stats.mean_rec_length = 0;
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
    "Mock storage engine: MySQL -> C++ -> CGO -> Go",
    PLUGIN_LICENSE_GPL,
    myengine_init,
    nullptr,
    nullptr,
    0x0001,
    nullptr,
    nullptr,
    nullptr,
    0,
} mysql_declare_plugin_end;
