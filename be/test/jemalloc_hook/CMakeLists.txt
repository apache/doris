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

# This allocator test must not use ASAN: its interceptors would hide missing
# glibc allocation entry points. Keep it separate from doris_be_test, which
# run-be-ut.sh builds with USE_JEMALLOC=OFF.
cmake_minimum_required(VERSION 3.20)
project(doris_jemalloc_hook_test LANGUAGES CXX)

if (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "jemalloc hook tests require Linux/glibc")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
get_filename_component(DORIS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed")
find_package(Threads REQUIRED)

foreach (LIB_NAME gtest gtest_main jemalloc_doris unwind lzma)
    find_library(${LIB_NAME}_LIBRARY NAMES ${LIB_NAME}
        PATHS "${THIRDPARTY_DIR}/lib" "${THIRDPARTY_DIR}/lib64"
        NO_DEFAULT_PATH REQUIRED)
endforeach()

add_library(jemalloc_hook_test_dso SHARED jemalloc_hook_test_dso.cpp)
target_compile_options(jemalloc_hook_test_dso PRIVATE -ftls-model=global-dynamic)

add_executable(jemalloc_hook_test
    jemalloc_hook_test.cpp
    "${DORIS_SOURCE_DIR}/be/src/runtime/memory/jemalloc_hook.cpp")
target_include_directories(jemalloc_hook_test PRIVATE "${THIRDPARTY_DIR}/include")
target_compile_definitions(jemalloc_hook_test PRIVATE
    JEMALLOC_HOOK_TEST_DSO="$<TARGET_FILE:jemalloc_hook_test_dso>")
# Do not enable ENABLE_EXPORTS: verify the hook also works with the production
# executable's default symbol visibility.
target_link_libraries(jemalloc_hook_test PRIVATE
    "${gtest_main_LIBRARY}" "${gtest_LIBRARY}" "${jemalloc_doris_LIBRARY}"
    "${unwind_LIBRARY}" "${lzma_LIBRARY}" Threads::Threads ${CMAKE_DL_LIBS})
add_dependencies(jemalloc_hook_test jemalloc_hook_test_dso)
