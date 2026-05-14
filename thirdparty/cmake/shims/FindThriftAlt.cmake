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

# Find module shim for ThriftAlt – exposes Doris's source-built thrift to Arrow
# Arrow's own FindThriftAlt.cmake checks ThriftAlt_FOUND and returns early,
# so we just need to set the right variables before it runs.

if(TARGET thrift_static)
    # Use the target directly instead of a file path — avoids generate-time file existence checks
    get_target_property(_thrift_binary_dir thrift_static BINARY_DIR)
    set(ThriftAlt_LIB "${_thrift_binary_dir}/lib/libthrift.a" CACHE INTERNAL "")
    set(ThriftAlt_INCLUDE_DIR "${TP_SOURCE_DIR}/thrift-0.16.0/lib/cpp/src" CACHE INTERNAL "")
    set(ThriftAlt_VERSION "0.16.0" CACHE INTERNAL "")
    set(ThriftAlt_FOUND TRUE CACHE INTERNAL "")
    set(Thrift_FOUND TRUE CACHE INTERNAL "")
    set(Thrift_VERSION "0.16.0" CACHE INTERNAL "")
    set(THRIFT_VERSION "0.16.0" CACHE INTERNAL "")

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(ThriftAlt
        REQUIRED_VARS ThriftAlt_LIB ThriftAlt_INCLUDE_DIR
        VERSION_VAR ThriftAlt_VERSION)

    if(NOT TARGET thrift::thrift)
        # Use INTERFACE IMPORTED with target_link_libraries to avoid
        # file-existence checks that STATIC IMPORTED + IMPORTED_LOCATION triggers
        add_library(thrift::thrift INTERFACE IMPORTED GLOBAL)
        target_link_libraries(thrift::thrift INTERFACE thrift_static)
        target_include_directories(thrift::thrift INTERFACE "${ThriftAlt_INCLUDE_DIR}")
    endif()
endif()
