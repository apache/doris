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

# Find module shim for BrotliAlt
if(TARGET brotlicommon)
    get_target_property(_brotli_inc brotlicommon INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _brotli_inc 0 BROTLI_INCLUDE_DIR)
    set(BROTLI_COMMON_LIBRARY brotlicommon)
    set(BROTLI_ENC_LIBRARY brotlienc)
    set(BROTLI_DEC_LIBRARY brotlidec)
    set(BrotliAlt_FOUND TRUE)
    set(BROTLI_FOUND TRUE)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(BrotliAlt DEFAULT_MSG BROTLI_COMMON_LIBRARY BROTLI_ENC_LIBRARY BROTLI_DEC_LIBRARY BROTLI_INCLUDE_DIR)

    if(NOT TARGET Brotli::brotlicommon)
        add_library(Brotli::brotlicommon INTERFACE IMPORTED GLOBAL)
        target_link_libraries(Brotli::brotlicommon INTERFACE brotlicommon)
    endif()
    if(NOT TARGET Brotli::brotlienc)
        add_library(Brotli::brotlienc INTERFACE IMPORTED GLOBAL)
        target_link_libraries(Brotli::brotlienc INTERFACE brotlienc)
    endif()
    if(NOT TARGET Brotli::brotlidec)
        add_library(Brotli::brotlidec INTERFACE IMPORTED GLOBAL)
        target_link_libraries(Brotli::brotlidec INTERFACE brotlidec)
    endif()
endif()
