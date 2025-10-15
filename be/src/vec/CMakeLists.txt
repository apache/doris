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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/vec")
# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/vec")

file(GLOB_RECURSE VEC_FILES CONFIGURE_DEPENDS *.cpp)

# Exclude all .cpp files from be/src/vec/functions/python/server directory
file(GLOB_RECURSE SERVER_CPP_FILES "${CMAKE_CURRENT_SOURCE_DIR}/functions/python/server/*.cpp")

# Remove these files from the list of files to compile
list(REMOVE_ITEM VEC_FILES ${SERVER_CPP_FILES})

if (NOT WITH_MYSQL)
    list(REMOVE_ITEM VEC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/exec/vmysql_scan_node.cpp ${CMAKE_CURRENT_SOURCE_DIR}/exec/scan/mysql_scanner.cpp)
endif ()

add_library(Vec STATIC
        ${VEC_FILES}
)

pch_reuse(Vec)
