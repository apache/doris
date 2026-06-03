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

# nlohmann_json — hand-written INTERFACE library (header-only, no add_subdirectory)
set(NLOHMANN_JSON_SRC_DIR ${TP_SOURCE_DIR}/json-3.10.1)

add_library(nlohmann_json INTERFACE)

target_include_directories(nlohmann_json SYSTEM INTERFACE
    ${NLOHMANN_JSON_SRC_DIR}/single_include
)

add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)
