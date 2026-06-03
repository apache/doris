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

# FindLibevent.cmake shim for source-built libevent
# Maps our add_subdirectory-built libevent targets to what thrift expects

if(TARGET event_core_static OR TARGET event_core)
    set(Libevent_FOUND TRUE)
    set(LIBEVENT_FOUND TRUE)
    set(LIBEVENT_LIBRARIES event_core_static event_extra_static)
    # Get include dirs from our source-built libevent
    if(TARGET event_core_static)
        get_target_property(_evt_inc event_core_static INTERFACE_INCLUDE_DIRECTORIES)
        if(_evt_inc)
            set(LIBEVENT_INCLUDE_DIRS ${_evt_inc})
        endif()
    endif()
    if(NOT LIBEVENT_INCLUDE_DIRS)
        # Fallback to source directory
        set(LIBEVENT_INCLUDE_DIRS
            ${TP_SOURCE_DIR}/libevent-release-2.1.12-stable/include
            ${CMAKE_CURRENT_BINARY_DIR}/libevent/include
        )
    endif()
endif()
