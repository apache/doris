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

# Find module shim for zlib (lowercase, used by rocksdb)
if(TARGET zlibstatic)
    set(ZLIB_FOUND TRUE)
    set(zlib_FOUND TRUE)
    get_target_property(_zlib_inc zlibstatic INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _zlib_inc 0 ZLIB_INCLUDE_DIR)
    set(ZLIB_INCLUDE_DIRS ${_zlib_inc})
    set(ZLIB_LIBRARIES zlibstatic)
    set(ZLIB_LIBRARY zlibstatic)
    if(NOT TARGET ZLIB::ZLIB)
        add_library(ZLIB::ZLIB INTERFACE IMPORTED GLOBAL)
        target_link_libraries(ZLIB::ZLIB INTERFACE zlibstatic)
    endif()
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(zlib DEFAULT_MSG ZLIB_LIBRARIES ZLIB_INCLUDE_DIR)
endif()
