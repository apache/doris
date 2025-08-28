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

#INCLUDE_DIRECTORIES(${GLOG_INCLUDE_DIR})

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/gutil")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/gutil")

SET(SOURCE_FILES
        hash/city.cc
        ref_counted.cc
        threading/thread_collision_warner.cc)

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    set(SOURCE_FILES ${SOURCE_FILES} atomicops-internals-x86.cc)
endif()

add_library(Gutil STATIC ${SOURCE_FILES})

set_target_properties(Gutil PROPERTIES COMPILE_FLAGS "-funsigned-char -Wno-deprecated")
target_compile_options(Gutil PRIVATE -Wno-char-subscripts -Wno-implicit-fallthrough)
# target_link_libraries(Gutil glog protobuf rt)

#set(GUTIL_LIBS
#  glog
#  protobuf)

#if (NOT APPLE)
#  set(GUTIL_LIBS
#    ${GUTIL_LIBS}
#    rt) # clock_gettime() requires -lrt
#endif()

#ADD_EXPORTABLE_LIBRARY(gutil
#  SRCS ${GUTIL_SRCS}
#  DEPS ${GUTIL_LIBS}
  # Disable warnings which trigger a lot in the Google code:
#  COMPILE_FLAGS "-funsigned-char -Wno-deprecated -Wno-char-subscripts")
