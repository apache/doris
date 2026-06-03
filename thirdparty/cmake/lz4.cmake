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

# lz4 — hand-written add_library (no add_subdirectory)
set(LZ4_SRC_DIR ${TP_SOURCE_DIR}/lz4-1.9.4)

add_library(lz4_static STATIC
    ${LZ4_SRC_DIR}/lib/lz4.c
    ${LZ4_SRC_DIR}/lib/lz4hc.c
    ${LZ4_SRC_DIR}/lib/lz4frame.c
    ${LZ4_SRC_DIR}/lib/xxhash.c
)

target_include_directories(lz4_static SYSTEM PUBLIC
    ${LZ4_SRC_DIR}/lib
)

target_compile_options(lz4_static PRIVATE -fPIC -w)

set_target_properties(lz4_static PROPERTIES
    OUTPUT_NAME lz4
    POSITION_INDEPENDENT_CODE ON
)

add_library(lz4 ALIAS lz4_static)

# Create nested lz4/ include directory so #include <lz4/lz4.h> works
set(LZ4_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/lz4_headers/include/lz4)
file(MAKE_DIRECTORY ${LZ4_NESTED_DIR})
file(GLOB _LZ4_HEADERS "${LZ4_SRC_DIR}/lib/lz4*.h")
file(COPY ${_LZ4_HEADERS} DESTINATION ${LZ4_NESTED_DIR})
add_library(lz4_headers INTERFACE)
target_include_directories(lz4_headers SYSTEM INTERFACE
    ${CMAKE_CURRENT_BINARY_DIR}/lz4_headers/include
)
