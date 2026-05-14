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

# roaring — hand-written add_library (no add_subdirectory)
set(ROARING_SRC_DIR ${TP_SOURCE_DIR}/CRoaring-2.1.2)

file(GLOB_RECURSE ROARING_SRCS "${ROARING_SRC_DIR}/src/*.c")

add_library(roaring STATIC ${ROARING_SRCS})

target_include_directories(roaring SYSTEM PUBLIC
    ${ROARING_SRC_DIR}/include
)

target_include_directories(roaring PRIVATE
    ${ROARING_SRC_DIR}/src
)

target_compile_options(roaring PRIVATE -fPIC -w)

set_target_properties(roaring PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
