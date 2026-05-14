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

# Find module shim for c-ares
if(TARGET c-ares::cares OR TARGET c-ares::cares_static OR TARGET c-ares_static)
    set(c-ares_FOUND TRUE)
    if(TARGET c-ares_static)
        get_target_property(_CARES_INC c-ares_static INTERFACE_INCLUDE_DIRECTORIES)
    endif()
    if(_CARES_INC)
        set(c-ares_INCLUDE_DIR ${_CARES_INC})
    endif()
    set(c-ares_LIBRARY "c-ares::cares")
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(c-ares DEFAULT_MSG c-ares_LIBRARY c-ares_INCLUDE_DIR)
endif()
