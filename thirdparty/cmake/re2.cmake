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

# re2 — hand-written add_library (no add_subdirectory)
set(RE2_SRC_DIR ${TP_SOURCE_DIR}/re2-2021-02-02)

add_library(re2 STATIC
    ${RE2_SRC_DIR}/re2/bitstate.cc
    ${RE2_SRC_DIR}/re2/compile.cc
    ${RE2_SRC_DIR}/re2/dfa.cc
    ${RE2_SRC_DIR}/re2/filtered_re2.cc
    ${RE2_SRC_DIR}/re2/mimics_pcre.cc
    ${RE2_SRC_DIR}/re2/nfa.cc
    ${RE2_SRC_DIR}/re2/onepass.cc
    ${RE2_SRC_DIR}/re2/parse.cc
    ${RE2_SRC_DIR}/re2/perl_groups.cc
    ${RE2_SRC_DIR}/re2/prefilter.cc
    ${RE2_SRC_DIR}/re2/prefilter_tree.cc
    ${RE2_SRC_DIR}/re2/prog.cc
    ${RE2_SRC_DIR}/re2/re2.cc
    ${RE2_SRC_DIR}/re2/regexp.cc
    ${RE2_SRC_DIR}/re2/set.cc
    ${RE2_SRC_DIR}/re2/simplify.cc
    ${RE2_SRC_DIR}/re2/stringpiece.cc
    ${RE2_SRC_DIR}/re2/tostring.cc
    ${RE2_SRC_DIR}/re2/unicode_casefold.cc
    ${RE2_SRC_DIR}/re2/unicode_groups.cc
    ${RE2_SRC_DIR}/util/rune.cc
    ${RE2_SRC_DIR}/util/strutil.cc
)

target_include_directories(re2 SYSTEM PUBLIC
    ${RE2_SRC_DIR}
)

target_compile_options(re2 PRIVATE -fPIC -w -pthread)

set_target_properties(re2 PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# grpc's Findre2.cmake checks for this target
add_library(re2::re2 ALIAS re2)
set(re2_FOUND TRUE CACHE BOOL "" FORCE)
