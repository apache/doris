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

# add_subdirectory(bufferpool)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

file(GLOB_RECURSE RUNTIME_FILES CONFIGURE_DEPENDS *.cpp *.cc)

if (NOT USE_JEMALLOC OR NOT USE_MEM_TRACKER)
    list(REMOVE_ITEM RUNTIME_FILES ${CMAKE_CURRENT_SOURCE_DIR}/memory/jemalloc_hook.cpp)
endif()

add_library(Runtime STATIC
    ${RUNTIME_FILES}
)

pch_reuse(Runtime)
