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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/util")

file(GLOB_RECURSE UTIL_FILES CONFIGURE_DEPENDS *.cpp *.cc)

if (OS_MACOSX)
    list(REMOVE_ITEM UTIL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/perf_counters.cpp ${CMAKE_CURRENT_SOURCE_DIR}/disk_info.cpp)
else ()
    list(REMOVE_ITEM UTIL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/perf_counters_mac.cpp ${CMAKE_CURRENT_SOURCE_DIR}/disk_info_mac.cpp)
endif()

list(REMOVE_ITEM UTIL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/libjvm_loader.cpp)

add_library(Util STATIC
    ${UTIL_FILES}
)

pch_reuse(Util)
