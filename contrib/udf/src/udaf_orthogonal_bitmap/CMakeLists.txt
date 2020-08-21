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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/udaf_orthogonal_bitmap")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/udaf_orthogonal_bitmap")


# set CMAKE_BUILD_TARGET_ARCH                                                        
# use `lscpu | grep 'Architecture' | awk '{print $2}'` only support system which language is en_US.UTF-8
execute_process(COMMAND bash "-c" "uname -m"
                OUTPUT_VARIABLE
                CMAKE_BUILD_TARGET_ARCH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Build target arch is ${CMAKE_BUILD_TARGET_ARCH}")

# Set dirs
set(SRC_DIR "$ENV{DORIS_HOME}/be/src/")
set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed/")

# Set include dirs
include_directories(./)
include_directories(${THIRDPARTY_DIR}/include/)

# message
message(STATUS "base dir is ${BASE_DIR}")
message(STATUS "doris home dir is $ENV{DORIS_HOME}")
message(STATUS "src dir is ${SRC_DIR}")
message(STATUS "libroaring dir is ${THIRDPARTY_DIR}/lib/libroaring.a")
message(STATUS "thirdparty dir is $ENV{DORIS_THIRDPARTY}")

# Set all libraries
add_library(roaring STATIC IMPORTED)
set_target_properties(roaring PROPERTIES IMPORTED_LOCATION
        ${THIRDPARTY_DIR}/lib/libroaring.a)

# Set FLAGS
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -Wno-unknown-pragmas -pthread")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-strict-aliasing -fno-omit-frame-pointer")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++11 -D__STDC_FORMAT_MACROS")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated -Wno-vla")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-unused-function")
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -msse4.2")
endif()
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS}  -Wno-attributes -DS2_USE_GFLAGS -DS2_USE_GLOG")

# For any gcc builds:
#   -g: Enable symbols for profiler tools
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
set(CXX_GCC_FLAGS "-g -Wno-unused-local-typedefs -O0 -gdwarf-2 -DNDEBUG")

SET(CMAKE_CXX_FLAGS ${CXX_GCC_FLAGS})

SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

message(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")

# static link gcc's lib
set(LINK_LIBS
  -Wl,--whole-archive
  roaring
  udf
  -Wl,--no-whole-archive
  -static-libstdc++
  -static-libgcc
)

set(DIR_SRCS 
   ./orthogonal_bitmap_function.cpp
)

add_library(udaf_orthogonal_bitmap SHARED ${DIR_SRCS})
target_link_libraries(udaf_orthogonal_bitmap
  ${LINK_LIBS}
)
