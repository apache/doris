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

# simdjson — hand-written add_library (no add_subdirectory)
set(SIMDJSON_SRC_DIR ${TP_SOURCE_DIR}/simdjson-3.11.6)

add_library(simdjson STATIC
    ${SIMDJSON_SRC_DIR}/singleheader/simdjson.cpp
)

target_include_directories(simdjson SYSTEM PUBLIC
    ${SIMDJSON_SRC_DIR}/singleheader
    ${SIMDJSON_SRC_DIR}/include
)

target_compile_options(simdjson PRIVATE -fPIC -w)
target_compile_definitions(simdjson PUBLIC SIMDJSON_THREADS_ENABLED=1)

find_package(Threads REQUIRED)
target_link_libraries(simdjson PRIVATE Threads::Threads)

set_target_properties(simdjson PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

add_library(simdjson::simdjson ALIAS simdjson)
