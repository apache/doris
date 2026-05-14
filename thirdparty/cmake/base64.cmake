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

# base64 — hand-written add_library (no add_subdirectory)
set(BASE64_SRC_DIR ${TP_SOURCE_DIR}/base64-0.5.2)

add_library(base64 STATIC
    ${BASE64_SRC_DIR}/lib/lib.c
    ${BASE64_SRC_DIR}/lib/codec_choose.c
    ${BASE64_SRC_DIR}/lib/tables/tables.c
    ${BASE64_SRC_DIR}/lib/arch/generic/codec.c
    ${BASE64_SRC_DIR}/lib/arch/ssse3/codec.c
    ${BASE64_SRC_DIR}/lib/arch/sse41/codec.c
    ${BASE64_SRC_DIR}/lib/arch/sse42/codec.c
    ${BASE64_SRC_DIR}/lib/arch/avx/codec.c
    ${BASE64_SRC_DIR}/lib/arch/avx2/codec.c
    ${BASE64_SRC_DIR}/lib/arch/avx512/codec.c
    ${BASE64_SRC_DIR}/lib/arch/neon32/codec.c
    ${BASE64_SRC_DIR}/lib/arch/neon64/codec.c
)

target_include_directories(base64 SYSTEM PUBLIC
    ${BASE64_SRC_DIR}/include
)

target_compile_options(base64 PRIVATE -fPIC -w)

target_compile_definitions(base64 PUBLIC BASE64_STATIC_DEFINE)

set_target_properties(base64 PROPERTIES
    C_STANDARD 99
    C_STANDARD_REQUIRED YES
    C_EXTENSIONS OFF
    POSITION_INDEPENDENT_CODE ON
)

# Generate config.h with all SIMD codecs enabled (x86_64 builds)
set(BASE64_WITH_SSSE3 1)
set(BASE64_WITH_SSE41 1)
set(BASE64_WITH_SSE42 1)
set(BASE64_WITH_AVX 1)
set(BASE64_WITH_AVX2 1)
set(BASE64_WITH_AVX512 1)
set(BASE64_WITH_NEON32 0)
set(BASE64_WITH_NEON64 0)
configure_file(
    "${BASE64_SRC_DIR}/cmake/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/base64/config.h"
    @ONLY
)
target_include_directories(base64 PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/base64")

# Set SIMD compile flags per arch codec file
set_source_files_properties(${BASE64_SRC_DIR}/lib/arch/ssse3/codec.c  PROPERTIES COMPILE_FLAGS "-mssse3")
set_source_files_properties(${BASE64_SRC_DIR}/lib/arch/sse41/codec.c  PROPERTIES COMPILE_FLAGS "-msse4.1")
set_source_files_properties(${BASE64_SRC_DIR}/lib/arch/sse42/codec.c  PROPERTIES COMPILE_FLAGS "-msse4.2")
set_source_files_properties(${BASE64_SRC_DIR}/lib/arch/avx/codec.c    PROPERTIES COMPILE_FLAGS "-mavx")
set_source_files_properties(${BASE64_SRC_DIR}/lib/arch/avx2/codec.c   PROPERTIES COMPILE_FLAGS "-mavx2")
set_source_files_properties(${BASE64_SRC_DIR}/lib/arch/avx512/codec.c PROPERTIES COMPILE_FLAGS "-mavx512vl -mavx512vbmi")
