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

set(LIBCXXABI_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src/llvm-project-20.1.7/libcxxabi")
set(LIBCXX_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src/llvm-project-20.1.7/libcxx")
set(LIBCXX_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated-include/c++/v1")
# Shared __config_site from libcxx-msan-cmake directory
set(CONFIG_SITE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/../libcxx-msan-cmake/__config_site")

set(SRCS
"${LIBCXXABI_SOURCE_DIR}/src/abort_message.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_aux_runtime.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_default_handlers.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_demangle.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_exception.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_exception_storage.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_guard.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_handlers.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_personality.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_thread_atexit.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_vector.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_virtual.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/fallback_malloc.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/private_typeinfo.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_exception.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_new_delete.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_stdexcept.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_typeinfo.cpp"
)

# Copy __config_site and generate __assertion_handler
file(MAKE_DIRECTORY "${LIBCXX_GENERATED_INCLUDE_DIR}")
configure_file("${CONFIG_SITE_SRC}" "${LIBCXX_GENERATED_INCLUDE_DIR}/__config_site" COPYONLY)
file(WRITE "${LIBCXX_GENERATED_INCLUDE_DIR}/__assertion_handler"
"#ifndef _LIBCPP___ASSERTION_HANDLER
#define _LIBCPP___ASSERTION_HANDLER
#include <cstdlib>
#define _LIBCPP_ASSERTION_HANDLER(message) ((void)message, std::abort())
#endif
")

add_library(cxxabi_msan STATIC ${SRCS})
set_target_properties(cxxabi_msan PROPERTIES FOLDER "contrib/libcxxabi-msan")
target_compile_options(cxxabi_msan PRIVATE -w)

# Generated include dir MUST come before source include dir
target_include_directories(cxxabi_msan SYSTEM BEFORE
    PUBLIC $<BUILD_INTERFACE:${LIBCXXABI_SOURCE_DIR}/include>
    PRIVATE $<BUILD_INTERFACE:${LIBCXX_GENERATED_INCLUDE_DIR}>
    PRIVATE $<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/include>
    PRIVATE $<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/src>
)
target_compile_definitions(cxxabi_msan PRIVATE -D_LIBCPP_BUILDING_LIBRARY -DHAS_THREAD_LOCAL)
# C++20 required for constinit; -nostdinc++ to avoid system headers
target_compile_options(cxxabi_msan PRIVATE -std=c++20 -nostdinc++ -fno-sanitize=undefined -Wno-macro-redefined)
target_compile_options(cxxabi_msan PRIVATE -fomit-frame-pointer -momit-leaf-frame-pointer)
target_compile_options(cxxabi_msan PRIVATE -fno-sanitize-memory-track-origins)
