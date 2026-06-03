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

# zlib — hand-written add_library (no add_subdirectory)
set(ZLIB_SRC_DIR ${TP_SOURCE_DIR}/zlib-1.3.1)

# Generate zconf.h from cmake template
set(Z_HAVE_UNISTD_H 1)
configure_file(
    "${ZLIB_SRC_DIR}/zconf.h.cmakein"
    "${CMAKE_CURRENT_BINARY_DIR}/zlib/zconf.h"
)

add_library(zlibstatic STATIC
    ${ZLIB_SRC_DIR}/adler32.c
    ${ZLIB_SRC_DIR}/compress.c
    ${ZLIB_SRC_DIR}/crc32.c
    ${ZLIB_SRC_DIR}/deflate.c
    ${ZLIB_SRC_DIR}/gzclose.c
    ${ZLIB_SRC_DIR}/gzlib.c
    ${ZLIB_SRC_DIR}/gzread.c
    ${ZLIB_SRC_DIR}/gzwrite.c
    ${ZLIB_SRC_DIR}/infback.c
    ${ZLIB_SRC_DIR}/inffast.c
    ${ZLIB_SRC_DIR}/inflate.c
    ${ZLIB_SRC_DIR}/inftrees.c
    ${ZLIB_SRC_DIR}/trees.c
    ${ZLIB_SRC_DIR}/uncompr.c
    ${ZLIB_SRC_DIR}/zutil.c
)

target_include_directories(zlibstatic SYSTEM PUBLIC
    ${ZLIB_SRC_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/zlib
)

target_compile_options(zlibstatic PRIVATE -fPIC -w
    # gzread.c / gzwrite.c / gzlib.c use POSIX read/write/close/lseek without
    # including <unistd.h>. Clang 16+ makes implicit-function-declaration a
    # hard error (no longer suppressed by -w); downgrade it for legacy zlib C.
    -Wno-error=implicit-function-declaration)

set_target_properties(zlibstatic PROPERTIES
    OUTPUT_NAME z
    POSITION_INDEPENDENT_CODE ON
)

add_library(z ALIAS zlibstatic)
add_library(libz ALIAS zlibstatic)
