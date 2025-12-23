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

# LZ4_HOME environment variable is used to check for LZ4 headers and static library

# LZ4_INCLUDE_DIR: directory containing headers
# LZ4_LIBRARY: path to liblz4
# LZ4_STATIC_LIB: path to lz4.a
# LZ4_FOUND: whether LZ4 has been found

if( NOT "${LZ4_HOME}" STREQUAL "")
    file (TO_CMAKE_PATH "${LZ4_HOME}" _lz4_path)
endif()

message (STATUS "LZ4_HOME: ${LZ4_HOME}")

find_path (LZ4_INCLUDE_DIR lz4.h HINTS
  ${_lz4_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library (LZ4_LIBRARY NAMES lz4 HINTS
  ${_lz4_path}
  PATH_SUFFIXES "lib" "lib64")

find_library (LZ4_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${LZ4_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
  ${_lz4_path}
  PATH_SUFFIXES "lib" "lib64")

if (LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
  set (LZ4_FOUND TRUE)
  set (LZ4_HEADER_NAME lz4.h)
  set (LZ4_HEADER ${LZ4_INCLUDE_DIR}/${LZ4_HEADER_NAME})
else ()
  set (LZ4_FOUND FALSE)
endif ()

if (LZ4_FOUND)
  message (STATUS "Found the LZ4 header: ${LZ4_HEADER}")
  message (STATUS "Found the LZ4 library: ${LZ4_LIBRARY}")
  if (LZ4_STATIC_LIB)
    message (STATUS "Found the LZ4 static library: ${LZ4_STATIC_LIB}")
  endif ()
else()
  if (_lz4_path)
    set (LZ4_ERR_MSG "Could not find LZ4. Looked in ${_lz4_path}.")
  else ()
    set (LZ4_ERR_MSG "Could not find LZ4 in system search paths.")
  endif ()

  if (LZ4_FIND_REQUIRED)
    message (FATAL_ERROR "${LZ4_ERR_MSG}")
  else ()
    message (STATUS "${LZ4_ERR_MSG}")
  endif ()
endif()

mark_as_advanced (
  LZ4_INCLUDE_DIR
  LZ4_STATIC_LIB
  LZ4_LIBRARY
)
