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

set(LIBCXX_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src/llvm-project-20.1.7/libcxx")
set(LIBCXX_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(LIBCXX_GENERATED_INCLUDE_DIR "${LIBCXX_BINARY_DIR}/include/c++/v1")
set(CONFIG_SITE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/__config_site")

set(SRCS
"${LIBCXX_SOURCE_DIR}/src/algorithm.cpp"
"${LIBCXX_SOURCE_DIR}/src/any.cpp"
"${LIBCXX_SOURCE_DIR}/src/atomic.cpp"
"${LIBCXX_SOURCE_DIR}/src/barrier.cpp"
"${LIBCXX_SOURCE_DIR}/src/bind.cpp"
"${LIBCXX_SOURCE_DIR}/src/call_once.cpp"
# charconv.cpp excluded: requires LLVM libc internal headers (shared/fp_bits.h).
# Use our snprintf-based stubs for to_chars(float/double/long double) instead.
"${CMAKE_CURRENT_SOURCE_DIR}/charconv_stubs.cpp"
"${LIBCXX_SOURCE_DIR}/src/chrono.cpp"
"${LIBCXX_SOURCE_DIR}/src/condition_variable.cpp"
"${LIBCXX_SOURCE_DIR}/src/condition_variable_destructor.cpp"
"${LIBCXX_SOURCE_DIR}/src/error_category.cpp"
"${LIBCXX_SOURCE_DIR}/src/exception.cpp"
# expected.cpp excluded: requires C++23 (bad_expected_access)
"${LIBCXX_SOURCE_DIR}/src/experimental/keep.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/directory_entry.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/directory_iterator.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/filesystem_clock.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/filesystem_error.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/int128_builtins.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/operations.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/path.cpp"
"${LIBCXX_SOURCE_DIR}/src/fstream.cpp"
"${LIBCXX_SOURCE_DIR}/src/functional.cpp"
"${LIBCXX_SOURCE_DIR}/src/future.cpp"
"${LIBCXX_SOURCE_DIR}/src/hash.cpp"
"${LIBCXX_SOURCE_DIR}/src/ios.cpp"
"${LIBCXX_SOURCE_DIR}/src/ios.instantiations.cpp"
"${LIBCXX_SOURCE_DIR}/src/iostream.cpp"
"${LIBCXX_SOURCE_DIR}/src/locale.cpp"
"${LIBCXX_SOURCE_DIR}/src/memory.cpp"
"${LIBCXX_SOURCE_DIR}/src/memory_resource.cpp"
"${LIBCXX_SOURCE_DIR}/src/mutex.cpp"
"${LIBCXX_SOURCE_DIR}/src/mutex_destructor.cpp"
"${LIBCXX_SOURCE_DIR}/src/new.cpp"
"${LIBCXX_SOURCE_DIR}/src/new_handler.cpp"
"${LIBCXX_SOURCE_DIR}/src/new_helpers.cpp"
"${LIBCXX_SOURCE_DIR}/src/optional.cpp"
"${LIBCXX_SOURCE_DIR}/src/ostream.cpp"
"${LIBCXX_SOURCE_DIR}/src/print.cpp"
"${LIBCXX_SOURCE_DIR}/src/random.cpp"
"${LIBCXX_SOURCE_DIR}/src/random_shuffle.cpp"
"${LIBCXX_SOURCE_DIR}/src/regex.cpp"
"${LIBCXX_SOURCE_DIR}/src/ryu/d2fixed.cpp"
"${LIBCXX_SOURCE_DIR}/src/ryu/d2s.cpp"
"${LIBCXX_SOURCE_DIR}/src/ryu/f2s.cpp"
"${LIBCXX_SOURCE_DIR}/src/shared_mutex.cpp"
"${LIBCXX_SOURCE_DIR}/src/stdexcept.cpp"
"${LIBCXX_SOURCE_DIR}/src/string.cpp"
"${LIBCXX_SOURCE_DIR}/src/strstream.cpp"
"${LIBCXX_SOURCE_DIR}/src/system_error.cpp"
"${LIBCXX_SOURCE_DIR}/src/thread.cpp"
"${LIBCXX_SOURCE_DIR}/src/typeinfo.cpp"
"${LIBCXX_SOURCE_DIR}/src/valarray.cpp"
"${LIBCXX_SOURCE_DIR}/src/variant.cpp"
"${LIBCXX_SOURCE_DIR}/src/vector.cpp"
"${LIBCXX_SOURCE_DIR}/src/verbose_abort.cpp"
)

# Copy shared __config_site and generate __assertion_handler
file(MAKE_DIRECTORY "${LIBCXX_GENERATED_INCLUDE_DIR}")
configure_file("${CONFIG_SITE_SRC}" "${LIBCXX_GENERATED_INCLUDE_DIR}/__config_site" COPYONLY)

if(EXISTS "${LIBCXX_SOURCE_DIR}/include/__assertion_handler")
    configure_file("${LIBCXX_SOURCE_DIR}/include/__assertion_handler" "${LIBCXX_GENERATED_INCLUDE_DIR}/__assertion_handler" COPYONLY)
else()
    file(WRITE "${LIBCXX_GENERATED_INCLUDE_DIR}/__assertion_handler"
"#ifndef _LIBCPP___ASSERTION_HANDLER
#define _LIBCPP___ASSERTION_HANDLER
#include <cstdlib>
#define _LIBCPP_ASSERTION_HANDLER(message) ((void)message, std::abort())
#endif
")
endif()

add_library(cxx_msan STATIC ${SRCS})
set_target_properties(cxx_msan PROPERTIES FOLDER "contrib/libcxx-msan")

target_include_directories(cxx_msan SYSTEM BEFORE PRIVATE $<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/src>)
target_include_directories(cxx_msan SYSTEM BEFORE PUBLIC
    $<$<COMPILE_LANGUAGE:CXX>:$<BUILD_INTERFACE:${LIBCXX_GENERATED_INCLUDE_DIR}>>
    $<$<COMPILE_LANGUAGE:CXX>:$<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/include>>
)
target_compile_definitions(cxx_msan PRIVATE -D_LIBCPP_BUILDING_LIBRARY -DLIBCXX_BUILDING_LIBCXXABI)
target_compile_definitions(cxx_msan PUBLIC -D_LIBCPP_REMOVE_TRANSITIVE_INCLUDES=1)
target_compile_options(cxx_msan PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-nostdinc++>)
target_compile_options(cxx_msan PRIVATE -std=c++20 -w)
target_link_libraries(cxx_msan PUBLIC cxxabi_msan)
