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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/util")

file(GLOB_RECURSE UTIL_FILES CONFIGURE_DEPENDS *.cpp *.cc)

if (OS_MACOSX)
    list(REMOVE_ITEM UTIL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/perf_counters.cpp ${CMAKE_CURRENT_SOURCE_DIR}/disk_info.cpp)
else ()
    list(REMOVE_ITEM UTIL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/perf_counters_mac.cpp ${CMAKE_CURRENT_SOURCE_DIR}/disk_info_mac.cpp)
endif()

if (ENABLE_TLS)
    list(REMOVE_ITEM UTIL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/oss/client_connection_provider.cpp)
    list(APPEND UTIL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/../${TLS_MODULE_DIR}/client_connection_provider.cpp)
endif()

add_library(Util STATIC
    ${UTIL_FILES}
)

# Keep compile-time hash generation out of the PCH and unity batches;
# callers only include the lightweight weekday_parser.h.
set_source_files_properties(weekday_parser.cpp PROPERTIES SKIP_PRECOMPILE_HEADERS ON)
set(DORIS_PERFECT_HASH_SOURCE ${CONTRIB_PATH}/perfect_hash/singleheader/perfect_hash.h)
if (NOT EXISTS "${DORIS_PERFECT_HASH_SOURCE}")
    message(FATAL_ERROR "Initialize the contrib/perfect_hash submodule before building BE")
endif()
# The pinned header uses if consteval only to select compile-time byte loops
# versus runtime loads; neither branch calls immediate functions. Express these
# conditions in C++20 so GCC 11 remains supported, without editing the submodule.
file(READ "${DORIS_PERFECT_HASH_SOURCE}" DORIS_PERFECT_HASH_HEADER)
string(REPLACE "if consteval {" "if (std::is_constant_evaluated()) {"
    DORIS_PERFECT_HASH_HEADER "${DORIS_PERFECT_HASH_HEADER}")
# libstdc++ 11 does not provide constexpr std::string with Clang. The diagnostic
# descriptions use std::to_string and are runtime-only, independent of lookup.
string(REPLACE "constexpr std::string algorithm_description()" "std::string algorithm_description()"
    DORIS_PERFECT_HASH_HEADER "${DORIS_PERFECT_HASH_HEADER}")
set(DORIS_PERFECT_HASH_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/perfect_hash)
file(GENERATE OUTPUT "${DORIS_PERFECT_HASH_INCLUDE_DIR}/perfect_hash.h"
    CONTENT "${DORIS_PERFECT_HASH_HEADER}")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${DORIS_PERFECT_HASH_SOURCE}")
target_include_directories(Util SYSTEM PRIVATE ${DORIS_PERFECT_HASH_INCLUDE_DIR})

pch_reuse(Util)

# Unity build for the small util TUs (parse-overhead dominated). Files that
# leak file-scope macros stay individual; hash/city.cc keeps its generic
# k0..k4 static hash constants away from sibling scopes. perf_counters.cpp
# is a Linux-only member (macOS builds its _mac variant, which is clean).
set(UTIL_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/bfd_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/hash/city.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/hash/murmur_hash3.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/network_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/os_info.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/os_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/perf_counters.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/simd/parquet_kernels.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/thrift_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/weekday_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/zlib.cpp)
doris_skip_unity_inclusion(${UTIL_UNITY_SKIP})
set_target_properties(Util PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                      UNITY_BUILD_BATCH_SIZE 24)
