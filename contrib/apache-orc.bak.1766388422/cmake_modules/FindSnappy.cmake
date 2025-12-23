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

# SNAPPY_HOME environmental variable is used to check for Snappy headers and static library

# SNAPPY_INCLUDE_DIR: directory containing headers
# SNAPPY_LIBRARY: path to libsnappy
# SNAPPY_STATIC_LIB: path to libsnappy.a
# SNAPPY_FOUND: whether snappy has been found

if( NOT "${SNAPPY_HOME}" STREQUAL "")
    file (TO_CMAKE_PATH "${SNAPPY_HOME}" _snappy_path)
endif()

message (STATUS "SNAPPY_HOME: ${SNAPPY_HOME}")

find_path (SNAPPY_INCLUDE_DIR snappy.h HINTS
  ${_snappy_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library (SNAPPY_LIBRARY NAMES snappy HINTS
  ${_snappy_path}
  PATH_SUFFIXES "lib" "lib64")

find_library (SNAPPY_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
  ${_snappy_path}
  PATH_SUFFIXES "lib" "lib64")

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIBRARY)
  set (SNAPPY_FOUND TRUE)
  set (SNAPPY_HEADER_NAME snappy.h)
  set (SNAPPY_HEADER ${SNAPPY_INCLUDE_DIR}/${SNAPPY_HEADER_NAME})
else ()
  set (SNAPPY_FOUND FALSE)
endif ()

if (SNAPPY_FOUND)
  message (STATUS "Found the Snappy header: ${SNAPPY_HEADER}")
  message (STATUS "Found the Snappy library: ${SNAPPY_LIBRARY}")
  if (SNAPPY_STATIC_LIB)
      message (STATUS "Found the Snappy static library: ${SNAPPY_STATIC_LIB}")
  endif()
else()
  if (_snappy_path)
    set (SNAPPY_ERR_MSG "Could not find Snappy. Looked in ${_snappy_path}.")
  else ()
    set (SNAPPY_ERR_MSG "Could not find Snappy in system search paths.")
  endif()

  if (Snappy_FIND_REQUIRED)
    message (FATAL_ERROR "${SNAPPY_ERR_MSG}")
  else ()
    message (STATUS "${SNAPPY_ERR_MSG}")
  endif ()
endif()

mark_as_advanced (
  SNAPPY_INCLUDE_DIR
  SNAPPY_STATIC_LIB
  SNAPPY_LIBRARY
)
