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

# Find module shim for BZip2
if(TARGET libbz2)
    get_target_property(_bz2_src_dir libbz2 SOURCE_DIR)
    set(BZIP2_INCLUDE_DIR "${_bz2_src_dir}")
    set(BZIP2_LIBRARIES libbz2)
    set(BZIP2_FOUND TRUE)
    set(BZip2_FOUND TRUE)
    
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(BZip2 DEFAULT_MSG BZIP2_LIBRARIES BZIP2_INCLUDE_DIR)

    if(NOT TARGET BZip2::BZip2)
        add_library(BZip2::BZip2 INTERFACE IMPORTED GLOBAL)
        target_link_libraries(BZip2::BZip2 INTERFACE libbz2)
    endif()
endif()
