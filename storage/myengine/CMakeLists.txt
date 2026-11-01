# 放在 MySQL 源码树 storage/myengine/ 下，由 MySQL 顶层 CMake 自动发现。
# 先执行 scripts/build_go.sh 生成 libmyengine.so / libmyengine.h 到 ${MYENGINE_GO_DIR}
GET_FILENAME_COMPONENT(_default_go_dir "${CMAKE_CURRENT_LIST_DIR}/../../go" REALPATH)
SET(MYENGINE_GO_DIR "${_default_go_dir}" CACHE PATH "Directory holding libmyengine.so/.h")

# 插件目标名也叫 myengine，故 Go 库用 IMPORTED 目标(go_myengine)按路径引用，避免重名
ADD_LIBRARY(go_myengine SHARED IMPORTED)
SET_TARGET_PROPERTIES(go_myengine PROPERTIES
  IMPORTED_LOCATION "${MYENGINE_GO_DIR}/libmyengine.so"
  IMPORTED_NO_SONAME TRUE)

MYSQL_ADD_PLUGIN(myengine ha_myengine.cc
  STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES go_myengine)

TARGET_INCLUDE_DIRECTORIES(myengine PRIVATE ${MYENGINE_GO_DIR}
  ${CMAKE_SOURCE_DIR}/extra/rapidjson/include)
# 运行时：优先 $ORIGIN(与 ha_myengine.so 同目录)，再回退到构建期 Go 目录
SET_TARGET_PROPERTIES(myengine PROPERTIES
  BUILD_RPATH "${MYENGINE_GO_DIR}" INSTALL_RPATH "$ORIGIN")
