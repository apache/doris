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

# zstd — hand-written add_library (no add_subdirectory)
set(ZSTD_SRC_DIR ${TP_SOURCE_DIR}/zstd-1.5.7)

file(GLOB ZSTD_COMMON_SRCS "${ZSTD_SRC_DIR}/lib/common/*.c")
file(GLOB ZSTD_COMPRESS_SRCS "${ZSTD_SRC_DIR}/lib/compress/*.c")
file(GLOB ZSTD_DECOMPRESS_SRCS "${ZSTD_SRC_DIR}/lib/decompress/*.c")
file(GLOB ZSTD_DEPRECATED_SRCS "${ZSTD_SRC_DIR}/lib/deprecated/*.c")
file(GLOB ZSTD_DICTBUILDER_SRCS "${ZSTD_SRC_DIR}/lib/dictBuilder/*.c")

add_library(libzstd_static STATIC
    ${ZSTD_COMMON_SRCS}
    ${ZSTD_COMPRESS_SRCS}
    ${ZSTD_DECOMPRESS_SRCS}
    ${ZSTD_DEPRECATED_SRCS}
    ${ZSTD_DICTBUILDER_SRCS}
)

target_include_directories(libzstd_static SYSTEM PUBLIC
    ${ZSTD_SRC_DIR}/lib
    ${ZSTD_SRC_DIR}/lib/common
)

target_compile_options(libzstd_static PRIVATE -fPIC -w)

target_compile_definitions(libzstd_static PRIVATE
    XXHASH_NAMESPACE=ZSTD_
    ZSTD_MULTITHREAD
    ZSTD_DISABLE_ASM
)

find_package(Threads REQUIRED)
target_link_libraries(libzstd_static PRIVATE Threads::Threads)

set_target_properties(libzstd_static PROPERTIES
    OUTPUT_NAME zstd
    POSITION_INDEPENDENT_CODE ON
)

add_library(zstd ALIAS libzstd_static)
