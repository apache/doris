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

# flatbuffers — hand-written add_library (no add_subdirectory)
set(FLATBUFFERS_SRC_DIR ${TP_SOURCE_DIR}/flatbuffers-2.0.0)

add_library(flatbuffers STATIC
    ${FLATBUFFERS_SRC_DIR}/src/idl_parser.cpp
    ${FLATBUFFERS_SRC_DIR}/src/idl_gen_text.cpp
    ${FLATBUFFERS_SRC_DIR}/src/reflection.cpp
    ${FLATBUFFERS_SRC_DIR}/src/util.cpp
)

target_include_directories(flatbuffers SYSTEM PUBLIC
    ${FLATBUFFERS_SRC_DIR}/include
)

target_compile_options(flatbuffers PRIVATE -fPIC -w)

set_target_properties(flatbuffers PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
