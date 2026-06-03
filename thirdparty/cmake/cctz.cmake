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

# cctz — hand-written add_library (no add_subdirectory)
set(CCTZ_SRC_DIR ${TP_SOURCE_DIR}/cctz-2.5)

add_library(cctz STATIC
    ${CCTZ_SRC_DIR}/src/civil_time_detail.cc
    ${CCTZ_SRC_DIR}/src/time_zone_fixed.cc
    ${CCTZ_SRC_DIR}/src/time_zone_format.cc
    ${CCTZ_SRC_DIR}/src/time_zone_if.cc
    ${CCTZ_SRC_DIR}/src/time_zone_impl.cc
    ${CCTZ_SRC_DIR}/src/time_zone_info.cc
    ${CCTZ_SRC_DIR}/src/time_zone_libc.cc
    ${CCTZ_SRC_DIR}/src/time_zone_lookup.cc
    ${CCTZ_SRC_DIR}/src/time_zone_posix.cc
    ${CCTZ_SRC_DIR}/src/zone_info_source.cc
)

target_include_directories(cctz SYSTEM PUBLIC
    ${CCTZ_SRC_DIR}/include
)

target_compile_options(cctz PRIVATE -fPIC -w)

set_target_properties(cctz PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

add_library(cctz::cctz ALIAS cctz)
