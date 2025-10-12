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

set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(OUTPUT_DIR "${BASE_DIR}/output")

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
include_directories(${Python3_INCLUDE_DIRS})
message(STATUS "Find Python3 executable: ${Python3_EXECUTABLE}")
message(STATUS "Find Python3 include dirs: ${Python3_INCLUDE_DIRS}")
message(STATUS "Find Python3 libraries: ${Python3_LIBRARIES}")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../common/")

set(SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
file(GLOB_RECURSE SOURCE_FILES "${SOURCE_DIR}/*")
message(STATUS "Source files: ${SOURCE_FILES}")

file(GLOB_RECURSE COMMON_SOURCE_FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/../common/*.cpp"
)

list(APPEND SOURCE_FILES ${COMMON_SOURCE_FILES})

add_executable(doris_py_udf ${SOURCE_FILES})

find_package(Threads REQUIRED)
target_link_libraries(doris_py_udf
        PRIVATE
        ${Python3_LIBRARIES}
        Threads::Threads
        dl
        util
)

install(TARGETS doris_py_udf DESTINATION ${OUTPUT_DIR}/bin)