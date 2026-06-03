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

# streamvbyte — hand-written add_library (no add_subdirectory)
set(STREAMVBYTE_SRC_DIR ${TP_SOURCE_DIR}/streamvbyte-1.0.0)

add_library(streamvbyte STATIC
    ${STREAMVBYTE_SRC_DIR}/src/streamvbyte_encode.c
    ${STREAMVBYTE_SRC_DIR}/src/streamvbyte_decode.c
    ${STREAMVBYTE_SRC_DIR}/src/streamvbyte_zigzag.c
    ${STREAMVBYTE_SRC_DIR}/src/streamvbytedelta_encode.c
    ${STREAMVBYTE_SRC_DIR}/src/streamvbytedelta_decode.c
    ${STREAMVBYTE_SRC_DIR}/src/streamvbyte_0124_encode.c
    ${STREAMVBYTE_SRC_DIR}/src/streamvbyte_0124_decode.c
)

target_include_directories(streamvbyte SYSTEM PUBLIC
    ${STREAMVBYTE_SRC_DIR}/include
)

# Internal headers (streamvbyte_isadetection.h etc.)
target_include_directories(streamvbyte PRIVATE
    ${STREAMVBYTE_SRC_DIR}/src
)

target_compile_options(streamvbyte PRIVATE -fPIC -w)

set_target_properties(streamvbyte PROPERTIES
    C_STANDARD 99
    POSITION_INDEPENDENT_CODE ON
)
