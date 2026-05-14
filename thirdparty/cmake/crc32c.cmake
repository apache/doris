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

# crc32c — hand-written add_library (no add_subdirectory)
set(CRC32C_SRC_DIR ${TP_SOURCE_DIR}/crc32c-1.1.2)

# ---------- Architecture-dependent config ----------
set(BYTE_ORDER_BIG_ENDIAN 0)
set(HAVE_BUILTIN_PREFETCH 1)
set(CRC32C_TESTS_BUILT_WITH_GLOG 0)

set(CRC32C_SOURCES
    ${CRC32C_SRC_DIR}/src/crc32c.cc
    ${CRC32C_SRC_DIR}/src/crc32c_portable.cc
)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    set(HAVE_MM_PREFETCH 0)
    set(HAVE_SSE42 0)
    set(HAVE_ARM64_CRC32C 1)
    set(HAVE_STRONG_GETAUXVAL 1)
    set(HAVE_WEAK_GETAUXVAL 1)
    list(APPEND CRC32C_SOURCES ${CRC32C_SRC_DIR}/src/crc32c_arm64.cc)
    set_source_files_properties(
        ${CRC32C_SRC_DIR}/src/crc32c_arm64.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8-a+crc+crypto"
    )
else()
    # x86_64
    set(HAVE_MM_PREFETCH 1)
    set(HAVE_SSE42 1)
    set(HAVE_ARM64_CRC32C 0)
    set(HAVE_STRONG_GETAUXVAL 0)
    set(HAVE_WEAK_GETAUXVAL 1)
    list(APPEND CRC32C_SOURCES ${CRC32C_SRC_DIR}/src/crc32c_sse42.cc)
    set_source_files_properties(
        ${CRC32C_SRC_DIR}/src/crc32c_sse42.cc
        PROPERTIES COMPILE_FLAGS "-msse4.2"
    )
endif()

# ---------- configure_file: crc32c_config.h ----------
configure_file(
    "${CRC32C_SRC_DIR}/src/crc32c_config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/crc32c/include/crc32c/crc32c_config.h"
)

# ---------- add_library ----------
add_library(crc32c STATIC ${CRC32C_SOURCES})

target_include_directories(crc32c SYSTEM PUBLIC
    ${CRC32C_SRC_DIR}/include
)

target_include_directories(crc32c PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/crc32c/include
    ${CRC32C_SRC_DIR}/src
)

target_compile_options(crc32c PRIVATE -fPIC -w)

set_target_properties(crc32c PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
