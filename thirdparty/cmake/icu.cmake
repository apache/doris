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

# ICU — pure CMake build (no autoconf)
# Build icuuc (common), icui18n, and icudata from source.
# ICU has ~200 .cpp in common/ and ~240 .cpp in i18n/.
# Data is stubdata only (minimal, no full locale data embedded).

set(ICU_SRC ${TP_SOURCE_DIR}/icu-release-69-1/icu4c/source)
set(ICU_CONFIG_DIR ${CMAKE_CURRENT_BINARY_DIR}/icu_config)
file(MAKE_DIRECTORY ${ICU_CONFIG_DIR})

# --- icudata (stub data — provides icudt69_dat symbol) ---
add_library(_icudata STATIC ${ICU_SRC}/stubdata/stubdata.cpp)
target_include_directories(_icudata PUBLIC ${ICU_SRC}/common)
target_compile_definitions(_icudata PRIVATE U_COMMON_IMPLEMENTATION)
target_compile_options(_icudata PRIVATE -fPIC -w)
add_library(icudata ALIAS _icudata)

# --- icuuc (common) ---
file(GLOB ICU_COMMON_SRCS "${ICU_SRC}/common/*.cpp" "${ICU_SRC}/common/*.c")


add_library(_icuuc STATIC ${ICU_COMMON_SRCS})
target_include_directories(_icuuc
    PUBLIC ${ICU_SRC}/common
)
target_compile_definitions(_icuuc PRIVATE
    U_COMMON_IMPLEMENTATION
    U_STATIC_IMPLEMENTATION
    U_CHARSET_IS_UTF8=1
    _GNU_SOURCE
    HAVE_DLOPEN=1
)
target_compile_options(_icuuc PRIVATE -fPIC -w)
target_link_libraries(_icuuc PRIVATE _icudata pthread dl)
add_library(icuuc ALIAS _icuuc)

# --- icui18n ---
file(GLOB ICU_I18N_SRCS "${ICU_SRC}/i18n/*.cpp" "${ICU_SRC}/i18n/*.c")


add_library(_icui18n STATIC ${ICU_I18N_SRCS})
target_include_directories(_icui18n
    PRIVATE ${ICU_SRC}/i18n
    PUBLIC  ${ICU_SRC}/common
    PUBLIC  ${ICU_SRC}/i18n
)
target_compile_definitions(_icui18n PRIVATE
    U_I18N_IMPLEMENTATION
    U_STATIC_IMPLEMENTATION
    U_CHARSET_IS_UTF8=1
)
target_compile_options(_icui18n PRIVATE -fPIC -w)
target_link_libraries(_icui18n PRIVATE _icuuc)
add_library(icui18n ALIAS _icui18n)
