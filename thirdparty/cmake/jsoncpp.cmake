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

# jsoncpp — hand-written add_library (no add_subdirectory)
set(JSONCPP_SRC_DIR ${TP_SOURCE_DIR}/jsoncpp-1.9.5)

add_library(jsoncpp_static STATIC
    ${JSONCPP_SRC_DIR}/src/lib_json/json_reader.cpp
    ${JSONCPP_SRC_DIR}/src/lib_json/json_value.cpp
    ${JSONCPP_SRC_DIR}/src/lib_json/json_writer.cpp
)

target_include_directories(jsoncpp_static SYSTEM PUBLIC
    ${JSONCPP_SRC_DIR}/include
)

target_compile_options(jsoncpp_static PRIVATE -fPIC -w)

set_target_properties(jsoncpp_static PROPERTIES
    OUTPUT_NAME jsoncpp
    POSITION_INDEPENDENT_CODE ON
)

add_library(jsoncpp_lib ALIAS jsoncpp_static)
