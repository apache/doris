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

cmake_minimum_required (VERSION 3.12.0)
if (POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif ()
if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif ()

project(ORC C CXX)

# Version number of package
SET(CPACK_PACKAGE_VERSION_MAJOR "1")
SET(CPACK_PACKAGE_VERSION_MINOR "9")
SET(CPACK_PACKAGE_VERSION_PATCH "0-SNAPSHOT")
SET(ORC_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake_modules")

option (BUILD_JAVA
    "Include ORC Java library in the build process"
     ON)

option (ANALYZE_JAVA
    "Run static analysis of the Java"
     OFF)

option (BUILD_LIBHDFSPP
    "Include LIBHDFSPP library in the build process"
     ON)

option(BUILD_CPP_TESTS
    "Build the googletest unit tests"
    ON)

option(BUILD_TOOLS
    "Build the tools"
    ON)

option(TEST_VALGRIND_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)

option(INSTALL_VENDORED_LIBS
    "Install vendored thirdparty libraries along with liborc"
    ON)

option(STOP_BUILD_ON_WARNING
    "Fail the build on C++ warnings"
    ON)

option(BUILD_POSITION_INDEPENDENT_LIB
    "Compile static libraries with position independent code"
    OFF)

option(BUILD_CPP_ENABLE_METRICS
    "Enable the metrics collection at compile phase"
    OFF)

# Make sure that a build type is selected
if (NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to ReleaseWithDebugInfo")
  set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
else ()
  message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Set the package format
SET(CPACK_GENERATOR "TGZ")
SET(CPACK_PACKAGE_VENDOR "Apache ORC")
SET(CPACK_PACKAGE_CONTACT "Apache ORC <dev@orc.apache.org>")

INCLUDE(CPack)
INCLUDE(ExternalProject)

if (BUILD_POSITION_INDEPENDENT_LIB)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

#
# Compiler specific flags
#
# This ensures that things like c++17 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
elseif(${CMAKE_CXX_STANDARD} VERSION_LESS 17)
  message(FATAL_ERROR "Cannot set a CMAKE_CXX_STANDARD smaller than 17")
endif()
# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if (NOT MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG -fno-omit-frame-pointer")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
endif ()
message(STATUS "compiler ${CMAKE_CXX_COMPILER_ID} version ${CMAKE_CXX_COMPILER_VERSION}")
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR
      CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
    message(FATAL_ERROR "A c++17-compliant compiler is required, please use at least Clang 5")
  else ()
    set (CXX17_FLAGS "-std=c++17")
  endif ()
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-missing-noreturn -Wno-unknown-pragmas")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-gnu-zero-variadic-macro-arguments")
  set (WARN_FLAGS "${WARN_FLAGS} -Wconversion")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "13.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-reserved-identifier")
  endif()
  if (CMAKE_HOST_APPLE AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "12.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++2a-compat -Wno-unknown-warning-option")
  elseif (CMAKE_HOST_APPLE AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "11.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++2a-compat")
  endif ()
  if (STOP_BUILD_ON_WARNING)
    set (WARN_FLAGS "${WARN_FLAGS} -Werror")
  endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR
      CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
    message(FATAL_ERROR "A c++17-compliant compiler is required, please use at least GCC 5")
  else ()
    set (CXX17_FLAGS "-std=c++17")
  endif ()
  set (WARN_FLAGS "-Wall -Wno-unknown-pragmas -Wconversion")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "12.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-array-bounds -Wno-stringop-overread") # To compile protobuf in Fedora37
  endif ()
  if (STOP_BUILD_ON_WARNING)
    set (WARN_FLAGS "${WARN_FLAGS} -Werror")
  endif ()
elseif (MSVC)
  include(CheckCXXCompilerFlag)
  CHECK_CXX_COMPILER_FLAG("/std:c++17" CPP17_FLAG_SUPPORTED)
  if (CPP17_FLAG_SUPPORTED)
    add_compile_options("/std:c++17")
  else ()
    message(FATAL_ERROR "A c++17-compliant compiler is required")
  endif ()
  add_definitions (-D_SCL_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_NONSTDC_NO_DEPRECATE) # The POSIX name for this item is deprecated
  set (WARN_FLAGS "${WARN_FLAGS} -wd4521") # multiple copy constructors specified
  set (WARN_FLAGS "${WARN_FLAGS} -wd4146") # unary minus operator applied to unsigned type, result still unsigned
endif ()

if (BUILD_CPP_ENABLE_METRICS)
  message(STATUS "Enable the metrics collection")
  add_compile_definitions(ENABLE_METRICS=1)
else ()
  message(STATUS "Disable the metrics collection")
  add_compile_definitions(ENABLE_METRICS=0)
endif ()

enable_testing()

INCLUDE(CheckSourceCompiles)
INCLUDE(ThirdpartyToolchain)

set (EXAMPLE_DIRECTORY ${CMAKE_SOURCE_DIR}/examples)

add_subdirectory(c++)

install(
  FILES LICENSE NOTICE
  DESTINATION "share/doc/orc")

if (BUILD_JAVA)
  add_subdirectory(java)
endif()

if (BUILD_TOOLS)
  add_subdirectory(tools)
endif ()

if (BUILD_CPP_TESTS)
  # Add another target called test-out that prints the results on failure
  if (CMAKE_CONFIGURATION_TYPES)
    add_custom_target (test-out
      COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
        --output-on-failure --build-config "$<CONFIGURATION>"
    )
  else ()
    add_custom_target (test-out
      COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
        --output-on-failure
    )
  endif ()
endif ()
