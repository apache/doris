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

# Find module shim for zstd (lowercase, used by rocksdb)
if(TARGET libzstd_static)
    set(ZSTD_FOUND TRUE)
    set(zstd_FOUND TRUE)
    get_target_property(_zstd_inc libzstd_static INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _zstd_inc 0 ZSTD_INCLUDE_DIR)
    set(ZSTD_INCLUDE_DIRS "${ZSTD_INCLUDE_DIR}")
    set(ZSTD_LIBRARIES libzstd_static)
    set(ZSTD_LIBRARY libzstd_static)
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(zstd DEFAULT_MSG ZSTD_LIBRARIES ZSTD_INCLUDE_DIR)
    if(NOT TARGET zstd::libzstd_static)
        add_library(zstd::libzstd_static ALIAS libzstd_static)
    endif()
endif()
