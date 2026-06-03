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

# libdeflate — hand-written add_library (no add_subdirectory)
set(LIBDEFLATE_SRC_DIR ${TP_SOURCE_DIR}/libdeflate-1.19)

add_library(libdeflate_static STATIC
    # core
    ${LIBDEFLATE_SRC_DIR}/lib/utils.c
    ${LIBDEFLATE_SRC_DIR}/lib/arm/cpu_features.c
    ${LIBDEFLATE_SRC_DIR}/lib/x86/cpu_features.c
    # compression
    ${LIBDEFLATE_SRC_DIR}/lib/deflate_compress.c
    # decompression
    ${LIBDEFLATE_SRC_DIR}/lib/deflate_decompress.c
    # zlib
    ${LIBDEFLATE_SRC_DIR}/lib/adler32.c
    ${LIBDEFLATE_SRC_DIR}/lib/zlib_compress.c
    ${LIBDEFLATE_SRC_DIR}/lib/zlib_decompress.c
    # gzip
    ${LIBDEFLATE_SRC_DIR}/lib/crc32.c
    ${LIBDEFLATE_SRC_DIR}/lib/gzip_compress.c
    ${LIBDEFLATE_SRC_DIR}/lib/gzip_decompress.c
)

target_include_directories(libdeflate_static SYSTEM PUBLIC
    ${LIBDEFLATE_SRC_DIR}
)

target_compile_options(libdeflate_static PRIVATE -fPIC -w)

set_target_properties(libdeflate_static PROPERTIES
    OUTPUT_NAME deflate
    POSITION_INDEPENDENT_CODE ON
)

add_library(libdeflate::libdeflate_static ALIAS libdeflate_static)

# Alias used by COMMON_THIRDPARTY
add_library(deflate ALIAS libdeflate_static)
