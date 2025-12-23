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

# GTEST_HOME environmental variable is used to check for GTest headers and static library

# GTEST_INCLUDE_DIR: directory containing headers
# GMOCK_LIBRARY: path to libgmock
# GTEST_LIBRARY: path to libgtest
# GMOCK_STATIC_LIB: is set to gmock.a static library
# GTEST_STATIC_LIB: is set to gtest.a static library
# GTEST_FOUND is set if GTEST is found

if (NOT "${GTEST_HOME}" STREQUAL "")
  message (STATUS "GTEST_HOME set: ${GTEST_HOME}")
endif ()

file (TO_CMAKE_PATH "${GTEST_HOME}" _gtest_path )

find_path (GTEST_INCLUDE_DIR gmock/gmock.h HINTS
  ${_gtest_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library (GMOCK_LIBRARY NAMES gmock HINTS
  ${_gtest_path}
  PATH_SUFFIXES "lib")

find_library (GTEST_LIBRARY NAMES gtest HINTS
  ${_gtest_path}
  PATH_SUFFIXES "lib")

find_library (GMOCK_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
  ${_gtest_path}
  PATH_SUFFIXES "lib")

find_library (GTEST_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
  ${_gtest_path}
  PATH_SUFFIXES "lib")

if (GTEST_INCLUDE_DIR AND GMOCK_LIBRARY)
  set (GTEST_FOUND TRUE)
  set (GTEST_HEADER_NAME gmock/gmock.h)
  set (GTEST_HEADER ${GTEST_INCLUDE_DIR}/${GTEST_HEADER_NAME})
else ()
  set (GTEST_FOUND FALSE)
endif ()

if (GTEST_FOUND)
  message (STATUS "Found the GTest header: ${GTEST_HEADER}")
  message (STATUS "Found the GTest library: ${GTEST_LIBRARY}")
  message (STATUS "Found the GMock library: ${GMOCK_LIBRARY}")
  if (GTEST_STATIC_LIB)
    message (STATUS "Found the GTest static library: ${GTEST_STATIC_LIB}")
  endif ()
  if (GMOCK_STATIC_LIB)
    message (STATUS "Found the GMock static library: ${GMOCK_STATIC_LIB}")
  endif ()
else ()
  if (_gtest_path)
    set (GTEST_ERR_MSG "Could not find GTest. Looked in ${_gtest_path}.")
  else ()
    set (GTEST_ERR_MSG "Could not find GTest in system search paths.")
  endif()

  if (GTest_FIND_REQUIRED)
    message (FATAL_ERROR "${GTEST_ERR_MSG}")
  else ()
    message (STATUS "${GTEST_ERR_MSG}")
  endif ()
endif ()

mark_as_advanced (
  GTEST_INCLUDE_DIR
  GMOCK_STATIC_LIB
  GTEST_STATIC_LIB
  GMOCK_LIBRARY
  GTEST_LIBRARY
)
