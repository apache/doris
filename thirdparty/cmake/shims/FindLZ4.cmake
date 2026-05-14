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

# Find module shim for LZ4 (uppercase, used by arrow/ORC find_package(LZ4))
if(TARGET lz4_static)
    set(LZ4_FOUND TRUE)
    get_target_property(_lz4_inc lz4_static INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _lz4_inc 0 LZ4_INCLUDE_DIR)
    set(LZ4_LIBRARY lz4_static)
    set(LZ4_STATIC_LIB lz4_static)
    set(LZ4_LIBRARIES lz4_static)
    message(STATUS "Found the LZ4 header: ${LZ4_INCLUDE_DIR}/lz4.h")
    message(STATUS "Found the LZ4 library: lz4_static (target)")
endif()
