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

# brotli — hand-written add_library (no add_subdirectory)
set(BROTLI_SRC_DIR ${TP_SOURCE_DIR}/brotli-1.0.9)

# Common library
add_library(brotlicommon STATIC
    ${BROTLI_SRC_DIR}/c/common/constants.c
    ${BROTLI_SRC_DIR}/c/common/context.c
    ${BROTLI_SRC_DIR}/c/common/dictionary.c
    ${BROTLI_SRC_DIR}/c/common/platform.c
    ${BROTLI_SRC_DIR}/c/common/transform.c
)

target_include_directories(brotlicommon SYSTEM PUBLIC
    ${BROTLI_SRC_DIR}/c/include
)

target_compile_options(brotlicommon PRIVATE -fPIC -w)

set_target_properties(brotlicommon PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Decoder library
add_library(brotlidec STATIC
    ${BROTLI_SRC_DIR}/c/dec/bit_reader.c
    ${BROTLI_SRC_DIR}/c/dec/decode.c
    ${BROTLI_SRC_DIR}/c/dec/huffman.c
    ${BROTLI_SRC_DIR}/c/dec/state.c
)

target_include_directories(brotlidec SYSTEM PUBLIC
    ${BROTLI_SRC_DIR}/c/include
)

target_link_libraries(brotlidec PUBLIC brotlicommon)
target_compile_options(brotlidec PRIVATE -fPIC -w)

set_target_properties(brotlidec PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Encoder library
file(GLOB BROTLI_ENC_SRCS "${BROTLI_SRC_DIR}/c/enc/*.c")

add_library(brotlienc STATIC ${BROTLI_ENC_SRCS})

target_include_directories(brotlienc SYSTEM PUBLIC
    ${BROTLI_SRC_DIR}/c/include
)

target_link_libraries(brotlienc PUBLIC brotlicommon)
target_compile_options(brotlienc PRIVATE -fPIC -w)

set_target_properties(brotlienc PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
