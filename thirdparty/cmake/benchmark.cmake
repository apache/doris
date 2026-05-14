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

# google benchmark — hand-written add_library (no add_subdirectory)
set(BENCHMARK_SRC_DIR ${TP_SOURCE_DIR}/benchmark-1.8.0)

file(GLOB BENCHMARK_SRCS "${BENCHMARK_SRC_DIR}/src/*.cc")
list(FILTER BENCHMARK_SRCS EXCLUDE REGEX "/benchmark_main\\.cc$")

add_library(benchmark STATIC ${BENCHMARK_SRCS})

target_include_directories(benchmark SYSTEM PUBLIC
    ${BENCHMARK_SRC_DIR}/include
)

target_include_directories(benchmark PRIVATE
    ${BENCHMARK_SRC_DIR}/src
)

target_compile_options(benchmark PRIVATE -fPIC -w)
target_compile_definitions(benchmark PUBLIC BENCHMARK_STATIC_DEFINE)

find_package(Threads REQUIRED)
target_link_libraries(benchmark PRIVATE Threads::Threads)

set_target_properties(benchmark PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# benchmark_main
add_library(benchmark_main STATIC
    ${BENCHMARK_SRC_DIR}/src/benchmark_main.cc
)

target_link_libraries(benchmark_main PUBLIC benchmark)
target_compile_options(benchmark_main PRIVATE -fPIC -w)

set_target_properties(benchmark_main PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
