# brpc - complex library with many patches
# brpc CMake needs protobuf, gflags, glog, leveldb, snappy, openssl
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_BRPC_TOOLS OFF CACHE BOOL "" FORCE)
set(BUILD_BRPC_UNITTEST OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(WITH_GLOG ON CACHE BOOL "" FORCE)
set(BRPC_WITH_GLOG ON CACHE BOOL "" FORCE)
set(OPENSSL_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl CACHE PATH "" FORCE)

# ============================================================================
# Pre-set ALL dependency variables that brpc's CMakeLists.txt searches via
# find_path / find_library / find_package. brpc uses specific variable names
# that differ from standard CMake conventions.
# ============================================================================

# GFLAGS (brpc uses custom cmake/FindGFLAGS.cmake)
get_target_property(_gflags_bin gflags_static BINARY_DIR)
set(GFLAGS_INCLUDE_PATH "${_gflags_bin}/include" CACHE PATH "" FORCE)
set(GFLAGS_LIBRARY "${_gflags_bin}/libgflags.a" CACHE FILEPATH "" FORCE)
set(GFLAGS_FOUND TRUE CACHE BOOL "" FORCE)
set(GFLAGS_NS "gflags" CACHE STRING "" FORCE)

# GLOG — brpc line 200-201: find_path(GLOG_INCLUDE_PATH) find_library(GLOG_LIB)
get_target_property(_glog_src glog SOURCE_DIR)
get_target_property(_glog_bin glog BINARY_DIR)
set(GLOG_INCLUDE_PATH "${_glog_src}/src" CACHE PATH "" FORCE)
set(GLOG_LIB "${_glog_bin}/libglog.a" CACHE FILEPATH "" FORCE)
set(GLOG_FOUND TRUE CACHE BOOL "" FORCE)

# PROTOBUF — brpc uses FindProtobuf + PROTOC_LIB (line 228)
set(PROTOBUF_INCLUDE_DIR "${TP_SOURCE_DIR}/protobuf-21.11/src" CACHE PATH "" FORCE)
set(PROTOBUF_INCLUDE_DIRS "${TP_SOURCE_DIR}/protobuf-21.11/src" CACHE PATH "" FORCE)
set(Protobuf_LIBRARIES "libprotobuf" CACHE STRING "" FORCE)
set(Protobuf_LIBRARY "libprotobuf" CACHE STRING "" FORCE)
set(Protobuf_INCLUDE_DIR "${TP_SOURCE_DIR}/protobuf-21.11/src" CACHE PATH "" FORCE)
set(Protobuf_INCLUDE_DIRS "${TP_SOURCE_DIR}/protobuf-21.11/src" CACHE PATH "" FORCE)
set(Protobuf_FOUND TRUE CACHE BOOL "" FORCE)
# brpc line 228: find_library(PROTOC_LIB NAMES protoc)
get_target_property(_protoc_bin libprotoc BINARY_DIR)
set(PROTOC_LIB "${_protoc_bin}/libprotoc.a" CACHE FILEPATH "" FORCE)
# Set protoc executable path so brpc's protobuf compilation finds protoc
set(Protobuf_PROTOC_EXECUTABLE "$<TARGET_FILE:protoc>" CACHE STRING "" FORCE)
set(PROTOBUF_PROTOC_EXECUTABLE "$<TARGET_FILE:protoc>" CACHE STRING "" FORCE)

# LEVELDB — brpc line 184-185: find_path(LEVELDB_INCLUDE_PATH) find_library(LEVELDB_LIB)
get_target_property(_leveldb_src leveldb SOURCE_DIR)
get_target_property(_leveldb_bin leveldb BINARY_DIR)
set(LEVELDB_INCLUDE_PATH "${_leveldb_src}/include" CACHE PATH "" FORCE)
set(LEVELDB_LIB "${_leveldb_bin}/libleveldb.a" CACHE FILEPATH "" FORCE)

# SNAPPY — brpc line 191-192: find_path(SNAPPY_INCLUDE_PATH) find_library(SNAPPY_LIB)
get_target_property(_snappy_src snappy SOURCE_DIR)
get_target_property(_snappy_bin snappy BINARY_DIR)
set(SNAPPY_INCLUDE_PATH "${_snappy_src}" CACHE PATH "" FORCE)
set(SNAPPY_LIB "${_snappy_bin}/libsnappy.a" CACHE FILEPATH "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/brpc-1.4.0 ${CMAKE_CURRENT_BINARY_DIR}/brpc EXCLUDE_FROM_ALL)
if(TARGET brpc-static)
    add_library(brpc ALIAS brpc-static)
    # Add include dirs as PRIVATE so brpc source files can find dependency headers
    # Using target_include_directories instead of include_directories to avoid
    # polluting the global scope (which caused OpenBLAS MB macro conflicts).
    target_include_directories(brpc-static PRIVATE
        ${_glog_src}/src                                    # glog/logging.h
        ${_glog_bin}                                        # glog/export.h (generated)
        ${_gflags_bin}/include                              # gflags/gflags.h
        ${TP_SOURCE_DIR}/protobuf-21.11/src                 # google/protobuf/...
        ${_leveldb_src}/include                             # leveldb/...
        ${_snappy_src}                                      # snappy.h
        ${TP_SOURCE_DIR}/boost_1_81_0                       # boost/...
        ${CMAKE_CURRENT_BINARY_DIR}/openssl/include         # openssl/...
    )
    # brpc has internal targets (BUTIL_LIB, SOURCES_LIB, OBJ_LIB) that also need include paths
    foreach(_brpc_internal_target BUTIL_LIB SOURCES_LIB OBJ_LIB)
        if(TARGET ${_brpc_internal_target})
            target_include_directories(${_brpc_internal_target} PRIVATE
                ${_glog_src}/src
                ${_glog_bin}
                ${_gflags_bin}/include
                ${TP_SOURCE_DIR}/protobuf-21.11/src
                ${_leveldb_src}/include
                ${_snappy_src}
                ${TP_SOURCE_DIR}/boost_1_81_0
                ${CMAKE_CURRENT_BINARY_DIR}/openssl/include
            )
        endif()
    endforeach()
endif()
