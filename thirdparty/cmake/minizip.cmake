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

# minizip - from zlib contrib
# minizip provides unzOpen/unzClose etc. used by user_function_cache.cpp
set(MINIZIP_SRC ${TP_SOURCE_DIR}/zlib-1.3.1/contrib/minizip)

add_library(minizip STATIC
    ${MINIZIP_SRC}/ioapi.c
    ${MINIZIP_SRC}/unzip.c
    ${MINIZIP_SRC}/zip.c
    ${MINIZIP_SRC}/mztools.c
)
target_include_directories(minizip PUBLIC ${MINIZIP_SRC})
# minizip depends on zlib
if(TARGET zlibstatic)
    target_link_libraries(minizip PRIVATE zlibstatic)
    target_include_directories(minizip PRIVATE
        ${TP_SOURCE_DIR}/zlib-1.3.1
        ${CMAKE_CURRENT_BINARY_DIR}/zlib)
endif()
set_target_properties(minizip PROPERTIES POSITION_INDEPENDENT_CODE ON)
