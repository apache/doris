# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# rocksdb — hand-written add_library (no add_subdirectory)
set(ROCKSDB_SRC_DIR ${TP_SOURCE_DIR}/rocksdb-5.14.2)

# Collect all source files matching the upstream SOURCES list
file(GLOB_RECURSE ROCKSDB_SRCS
    "${ROCKSDB_SRC_DIR}/cache/*.cc"
    "${ROCKSDB_SRC_DIR}/db/*.cc"
    "${ROCKSDB_SRC_DIR}/env/*.cc"
    "${ROCKSDB_SRC_DIR}/memtable/*.cc"
    "${ROCKSDB_SRC_DIR}/monitoring/*.cc"
    "${ROCKSDB_SRC_DIR}/options/*.cc"
    "${ROCKSDB_SRC_DIR}/port/port_posix.cc"
    "${ROCKSDB_SRC_DIR}/port/stack_trace.cc"
    "${ROCKSDB_SRC_DIR}/table/*.cc"
    "${ROCKSDB_SRC_DIR}/util/*.cc"
    "${ROCKSDB_SRC_DIR}/utilities/*.cc"
)
# Add specific tool files needed by the library (not rdb, not benchmarks)
list(APPEND ROCKSDB_SRCS
    "${ROCKSDB_SRC_DIR}/tools/db_bench_tool.cc"
    "${ROCKSDB_SRC_DIR}/tools/dump/db_dump_tool.cc"
    "${ROCKSDB_SRC_DIR}/tools/ldb_cmd.cc"
    "${ROCKSDB_SRC_DIR}/tools/ldb_tool.cc"
    "${ROCKSDB_SRC_DIR}/tools/sst_dump_tool.cc"
)
# Exclude test/bench files (164 test files in tree)
list(FILTER ROCKSDB_SRCS EXCLUDE REGEX "test")
list(FILTER ROCKSDB_SRCS EXCLUDE REGEX "bench")
list(FILTER ROCKSDB_SRCS EXCLUDE REGEX "/mock/")
# Re-add testutil.cc which is needed by the main library (not actually a test)
list(APPEND ROCKSDB_SRCS "${ROCKSDB_SRC_DIR}/util/testutil.cc")
# Exclude optional modules we don't need
list(FILTER ROCKSDB_SRCS EXCLUDE REGEX "env_librados")
list(FILTER ROCKSDB_SRCS EXCLUDE REGEX "env_hdfs")

# Generate build_version.cc
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/rocksdb/util/build_version.cc"
    "#include \"util/build_version.h\"\n"
    "const char* rocksdb_build_git_sha = \"rocksdb_build_git_sha:0\";\n"
    "const char* rocksdb_build_git_date = \"rocksdb_build_git_date:2024-01-01\";\n"
    "const char* rocksdb_build_compile_date = __DATE__;\n"
)
list(APPEND ROCKSDB_SRCS "${CMAKE_CURRENT_BINARY_DIR}/rocksdb/util/build_version.cc")

add_library(rocksdb STATIC ${ROCKSDB_SRCS})

target_include_directories(rocksdb SYSTEM PUBLIC
    ${ROCKSDB_SRC_DIR}/include
)

target_include_directories(rocksdb PRIVATE
    ${ROCKSDB_SRC_DIR}
    ${ROCKSDB_SRC_DIR}/third-party/gtest-1.7.0/fused-src
    ${CMAKE_CURRENT_BINARY_DIR}/rocksdb
)

target_compile_options(rocksdb PRIVATE -fPIC -w -include cstdint -frtti)

target_compile_definitions(rocksdb PRIVATE
    ROCKSDB_PLATFORM_POSIX
    ROCKSDB_LIB_IO_POSIX
    ROCKSDB_SUPPORT_THREAD_LOCAL
    OS_LINUX
    HAVE_SSE42
    ROCKSDB_FALLOCATE_PRESENT
    ROCKSDB_MALLOC_USABLE_SIZE
    ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
    ROCKSDB_RANGESYNC_PRESENT
    ROCKSDB_SCHED_GETCPU_PRESENT
    NDEBUG
)

# Per-file SSE4.2 flags
set_source_files_properties(
    ${ROCKSDB_SRC_DIR}/util/crc32c.cc
    PROPERTIES COMPILE_FLAGS "-msse4.2 -mpclmul"
)

find_package(Threads REQUIRED)
target_link_libraries(rocksdb PRIVATE Threads::Threads)

# Dependencies
if(TARGET snappy)
    target_link_libraries(rocksdb PRIVATE snappy)
    target_compile_definitions(rocksdb PRIVATE SNAPPY)
endif()
if(TARGET lz4_static)
    target_link_libraries(rocksdb PRIVATE lz4_static)
    target_compile_definitions(rocksdb PRIVATE LZ4)
endif()
if(TARGET libzstd_static)
    target_link_libraries(rocksdb PRIVATE libzstd_static)
    target_compile_definitions(rocksdb PRIVATE ZSTD)
endif()
if(TARGET zlibstatic)
    target_link_libraries(rocksdb PRIVATE zlibstatic)
    target_compile_definitions(rocksdb PRIVATE ZLIB)
endif()

set_target_properties(rocksdb PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
