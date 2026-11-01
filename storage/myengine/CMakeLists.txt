# 放在 MySQL 源码树 storage/myengine/ 下，由 MySQL 顶层 CMake 自动发现。
# 先执行 scripts/build_go.sh 生成 libmyengine.so / libmyengine.h 到 ${MYENGINE_GO_DIR}
SET(MYENGINE_GO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../go" CACHE PATH "Directory holding libmyengine.so/.h")

INCLUDE_DIRECTORIES(${MYENGINE_GO_DIR})
LINK_DIRECTORIES(${MYENGINE_GO_DIR})

MYSQL_ADD_PLUGIN(myengine ha_myengine.cc
  STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES myengine)

# 运行时通过 rpath 兜底；仍建议配置 LD_LIBRARY_PATH
IF(TARGET myengine)
  SET_TARGET_PROPERTIES(myengine PROPERTIES
    BUILD_RPATH "${MYENGINE_GO_DIR}" INSTALL_RPATH "$ORIGIN")
ENDIF()
