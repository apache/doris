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

include_directories(
  ${PROJECT_SOURCE_DIR}/c++/include
  ${PROJECT_SOURCE_DIR}/c++/src
  ${PROJECT_SOURCE_DIR}/tools-c++/src
  ${PROJECT_BINARY_DIR}/c++/include
  ${PROJECT_BINARY_DIR}/c++/src
)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX17_FLAGS} ${WARN_FLAGS}")

add_executable (tool-test
  gzip.cc
  TestCSVFileImport.cc
  TestFileContents.cc
  TestFileMetadata.cc
  TestFileScan.cc
  TestFileStatistics.cc
  TestMatch.cc
  ToolTest.cc
)

target_link_libraries (tool-test
  orc
  orc::protobuf
  orc::zlib
  orc::snappy
  orc::gtest
  orc::gmock
)

add_dependencies(tool-test tool-set)

if (TEST_VALGRIND_MEMCHECK)
  add_test (tool-test valgrind --tool=memcheck --leak-check=full --error-exitcode=1 ./tool-test ${EXAMPLE_DIRECTORY} ${PROJECT_BINARY_DIR})
else ()
  add_test (tool-test tool-test ${EXAMPLE_DIRECTORY} ${PROJECT_BINARY_DIR})
endif ()
