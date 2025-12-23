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

# ZLIB_HOME environmental variable is used to check for ZLIB headers and static library

# ZLIB_INCLUDE_DIR: directory containing headers
# ZLIB_LIBRARY: path to libz/libzlib
# ZLIB_STATIC_LIB: path to zlib.a
# ZLIB_FOUND: whether ZLIB has been found

if( NOT "${ZLIB_HOME}" STREQUAL "")
    file (TO_CMAKE_PATH "${ZLIB_HOME}" _zlib_path)
endif()

message (STATUS "ZLIB_HOME: ${ZLIB_HOME}")

find_path (ZLIB_INCLUDE_DIR zlib.h HINTS
  ${_zlib_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

if (NOT ZLIB_STATIC_LIB_NAME)
  set (ZLIB_STATIC_LIB_NAME z)
endif()

find_library (ZLIB_LIBRARY NAMES z zlib ${ZLIB_STATIC_LIB_NAME} HINTS
  ${_zlib_path}
  PATH_SUFFIXES "lib")

find_library (ZLIB_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${ZLIB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
  ${_zlib_path}
  PATH_SUFFIXES "lib")

if (ZLIB_INCLUDE_DIR AND ZLIB_LIBRARY)
  set (ZLIB_FOUND TRUE)
  set (ZLIB_HEADER_NAME zlib.h)
  set (ZLIB_HEADER ${ZLIB_INCLUDE_DIR}/${ZLIB_HEADER_NAME})
else ()
  set (ZLIB_FOUND FALSE)
endif ()

if (ZLIB_FOUND)
  message (STATUS "Found the ZLIB header: ${ZLIB_HEADER}")
  message (STATUS "Found the ZLIB library: ${ZLIB_LIBRARY}")
  if (ZLIB_STATIC_LIB)
    message (STATUS "Found the ZLIB static library: ${ZLIB_STATIC_LIB}")
  endif ()
else()
  if (_zlib_path)
    set (ZLIB_ERR_MSG "Could not find ZLIB. Looked in ${_zlib_path}.")
  else ()
    set (ZLIB_ERR_MSG "Could not find ZLIB in system search paths.")
  endif()

  if (ZLIB_FIND_REQUIRED)
    message (FATAL_ERROR "${ZLIB_ERR_MSG}")
  else ()
    message (STATUS "${ZLIB_ERR_MSG}")
  endif ()
endif()

mark_as_advanced (
  ZLIB_INCLUDE_DIR
  ZLIB_STATIC_LIB
  ZLIB_LIBRARY
)
