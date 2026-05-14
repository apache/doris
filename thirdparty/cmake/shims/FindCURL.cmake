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

# FindCURL shim — point find_package(CURL) to the source-built libcurl target
if(TARGET libcurl)
    get_target_property(_curl_includes libcurl INTERFACE_INCLUDE_DIRECTORIES)
    get_target_property(_curl_location libcurl BINARY_DIR)

    set(CURL_FOUND TRUE)
    set(CURL_INCLUDE_DIR "${_curl_includes}" CACHE STRING "" FORCE)
    set(CURL_INCLUDE_DIRS "${_curl_includes}" CACHE STRING "" FORCE)
    set(CURL_LIBRARY "libcurl" CACHE STRING "" FORCE)
    set(CURL_LIBRARIES "libcurl" CACHE STRING "" FORCE)

    if(NOT TARGET CURL::libcurl)
        add_library(CURL::libcurl ALIAS libcurl)
    endif()
endif()
