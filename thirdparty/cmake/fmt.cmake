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

# fmt — hand-written add_library (no add_subdirectory)
set(FMT_SRC_DIR ${TP_SOURCE_DIR}/fmt-8.1.1)

# fmt-8.1.1 ranges.h uses unqualified `format_to(...)`. With clang 21 +
# libstdc++15 (which ships C++23 `<format>`), `fmt::appender` =
# `std::back_insert_iterator<...>` brings the std namespace in via ADL, and
# `std::format_to` overloads become ambiguous candidates with fmt's own. The
# fix is to qualify the four call sites in ranges.h. Apply idempotently.
set(_FMT_RANGES_H ${FMT_SRC_DIR}/include/fmt/ranges.h)
if (EXISTS ${_FMT_RANGES_H})
    file(READ ${_FMT_RANGES_H} _fmt_ranges_content)
    string(REPLACE "out = format_to(" "out = ::fmt::format_to(" _fmt_ranges_patched "${_fmt_ranges_content}")
    if (NOT "${_fmt_ranges_patched}" STREQUAL "${_fmt_ranges_content}")
        file(WRITE ${_FMT_RANGES_H} "${_fmt_ranges_patched}")
        message(STATUS "fmt: patched ranges.h format_to ambiguity (libstdc++15 / clang21)")
    endif()
endif()

add_library(fmt STATIC
    ${FMT_SRC_DIR}/src/format.cc
    ${FMT_SRC_DIR}/src/os.cc
)

target_include_directories(fmt SYSTEM PUBLIC
    ${FMT_SRC_DIR}/include
)

target_compile_options(fmt PRIVATE -fPIC -w)

set_target_properties(fmt PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

add_library(fmt::fmt ALIAS fmt)

# Header-only variant (same as upstream)
add_library(fmt-header-only INTERFACE)
target_compile_definitions(fmt-header-only INTERFACE FMT_HEADER_ONLY=1)
target_include_directories(fmt-header-only SYSTEM INTERFACE
    ${FMT_SRC_DIR}/include
)
add_library(fmt::fmt-header-only ALIAS fmt-header-only)
