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

# Find module shim for RapidJSONAlt — header-only library
# Arrow's FindRapidJSONAlt.cmake checks RapidJSONAlt_FOUND at line 18-20
if(TARGET rapidjson)
    get_target_property(_rj_inc rapidjson INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _rj_inc 0 RAPIDJSON_INCLUDE_DIR)
    set(RAPIDJSON_VERSION "1.1.0")
    set(RapidJSONAlt_FOUND TRUE)
    set(RapidJSON_FOUND TRUE)
    
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(RapidJSONAlt
        REQUIRED_VARS RAPIDJSON_INCLUDE_DIR
        VERSION_VAR RAPIDJSON_VERSION)

    if(NOT TARGET RapidJSON)
        add_library(RapidJSON INTERFACE IMPORTED)
        target_include_directories(RapidJSON INTERFACE "${RAPIDJSON_INCLUDE_DIR}")
    endif()
endif()
