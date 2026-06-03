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

# leveldb — hand-written add_library (no add_subdirectory)
set(LEVELDB_SRC_DIR ${TP_SOURCE_DIR}/leveldb-1.23)

# ---------- configure_file: port_config.h ----------
set(HAVE_FDATASYNC 1)
set(HAVE_FULLFSYNC 0)
set(HAVE_O_CLOEXEC 1)
set(HAVE_CRC32C 0)
set(HAVE_SNAPPY 0)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/leveldb/include/port)
configure_file(
    "${LEVELDB_SRC_DIR}/port/port_config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/leveldb/include/port/port_config.h"
)

# ---------- add_library ----------
add_library(leveldb STATIC
    # db/
    ${LEVELDB_SRC_DIR}/db/builder.cc
    ${LEVELDB_SRC_DIR}/db/c.cc
    ${LEVELDB_SRC_DIR}/db/dbformat.cc
    ${LEVELDB_SRC_DIR}/db/db_impl.cc
    ${LEVELDB_SRC_DIR}/db/db_iter.cc
    ${LEVELDB_SRC_DIR}/db/dumpfile.cc
    ${LEVELDB_SRC_DIR}/db/filename.cc
    ${LEVELDB_SRC_DIR}/db/log_reader.cc
    ${LEVELDB_SRC_DIR}/db/log_writer.cc
    ${LEVELDB_SRC_DIR}/db/memtable.cc
    ${LEVELDB_SRC_DIR}/db/repair.cc
    ${LEVELDB_SRC_DIR}/db/table_cache.cc
    ${LEVELDB_SRC_DIR}/db/version_edit.cc
    ${LEVELDB_SRC_DIR}/db/version_set.cc
    ${LEVELDB_SRC_DIR}/db/write_batch.cc
    # table/
    ${LEVELDB_SRC_DIR}/table/block_builder.cc
    ${LEVELDB_SRC_DIR}/table/block.cc
    ${LEVELDB_SRC_DIR}/table/filter_block.cc
    ${LEVELDB_SRC_DIR}/table/format.cc
    ${LEVELDB_SRC_DIR}/table/iterator.cc
    ${LEVELDB_SRC_DIR}/table/merger.cc
    ${LEVELDB_SRC_DIR}/table/table_builder.cc
    ${LEVELDB_SRC_DIR}/table/table.cc
    ${LEVELDB_SRC_DIR}/table/two_level_iterator.cc
    # util/
    ${LEVELDB_SRC_DIR}/util/arena.cc
    ${LEVELDB_SRC_DIR}/util/bloom.cc
    ${LEVELDB_SRC_DIR}/util/cache.cc
    ${LEVELDB_SRC_DIR}/util/coding.cc
    ${LEVELDB_SRC_DIR}/util/comparator.cc
    ${LEVELDB_SRC_DIR}/util/crc32c.cc
    ${LEVELDB_SRC_DIR}/util/env.cc
    ${LEVELDB_SRC_DIR}/util/env_posix.cc
    ${LEVELDB_SRC_DIR}/util/filter_policy.cc
    ${LEVELDB_SRC_DIR}/util/hash.cc
    ${LEVELDB_SRC_DIR}/util/logging.cc
    ${LEVELDB_SRC_DIR}/util/options.cc
    ${LEVELDB_SRC_DIR}/util/status.cc
    # helpers/
    ${LEVELDB_SRC_DIR}/helpers/memenv/memenv.cc
)

target_include_directories(leveldb SYSTEM PUBLIC
    ${LEVELDB_SRC_DIR}/include
)

target_include_directories(leveldb PRIVATE
    ${LEVELDB_SRC_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/leveldb/include
)

target_compile_options(leveldb PRIVATE -fPIC -w)
target_compile_definitions(leveldb PRIVATE LEVELDB_PLATFORM_POSIX=1)

find_package(Threads REQUIRED)
target_link_libraries(leveldb PRIVATE Threads::Threads)

# Note: HAVE_SNAPPY=0 to match the original add_subdirectory behavior
# (check_library_exists fails in the thirdparty context).

set_target_properties(leveldb PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
