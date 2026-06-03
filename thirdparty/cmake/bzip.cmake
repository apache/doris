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

# bzip2 - simple makefile-based, compile manually
set(BZIP2_SRC ${TP_SOURCE_DIR}/bzip2-1.0.8)
add_library(libbz2 STATIC
    ${BZIP2_SRC}/blocksort.c
    ${BZIP2_SRC}/huffman.c
    ${BZIP2_SRC}/crctable.c
    ${BZIP2_SRC}/randtable.c
    ${BZIP2_SRC}/compress.c
    ${BZIP2_SRC}/decompress.c
    ${BZIP2_SRC}/bzlib.c
)
target_include_directories(libbz2 PUBLIC ${BZIP2_SRC})
set_target_properties(libbz2 PROPERTIES POSITION_INDEPENDENT_CODE ON)
