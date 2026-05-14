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

# Find module shim for gflagsAlt
if(TARGET gflags_static)
    get_target_property(_gflags_inc gflags_static INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _gflags_inc 0 GFLAGS_INCLUDE_DIR)
    set(gflags_LIB gflags_static)
    set(gflagsAlt_FOUND TRUE)
    set(GFLAGS_FOUND TRUE)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(gflagsAlt DEFAULT_MSG gflags_LIB GFLAGS_INCLUDE_DIR)

    if(NOT TARGET gflags::gflags)
        add_library(gflags::gflags INTERFACE IMPORTED GLOBAL)
        target_link_libraries(gflags::gflags INTERFACE gflags_static)
    endif()
    if(NOT TARGET gflags::gflags_static)
        add_library(gflags::gflags_static INTERFACE IMPORTED GLOBAL)
        target_link_libraries(gflags::gflags_static INTERFACE gflags_static)
    endif()
endif()
