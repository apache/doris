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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/vector")

find_package(OpenMP REQUIRED)
if (OpenMP_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

set(CMAKE_COMPILE_WARNING_AS_ERROR OFF)
add_compile_options(-Wno-error=attributes)
add_compile_options(-Wno-deprecated-copy)
add_compile_options(-Wno-reorder)
add_compile_options(-Wno-unused-but-set-variable)
add_compile_options(-Wno-error=unused-variable)


file(GLOB_RECURSE SRC_FILES  *.cpp)
add_library(vector STATIC ${SRC_FILES})

