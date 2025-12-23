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

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TODO:
# - orc-metadata relies on the protobuf routines, meaning protobuf and
#   binary_dir/c++/src still need to be included
# - timezone-dump relies on non-public timezone routines. I *think* this
#   executable can just be removed, as it looks like it was written for testing
#   alone.

include_directories (
  ${PROJECT_SOURCE_DIR}/c++/include
  ${PROJECT_BINARY_DIR}/c++/include
  ${PROJECT_SOURCE_DIR}/c++/src
  ${PROJECT_BINARY_DIR}/c++/src
  )

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ${CXX17_FLAGS} ${WARN_FLAGS}")

add_executable (orc-contents
  FileContents.cc
  ToolsHelper.cc
  )

target_link_libraries (orc-contents
  orc
  ${CMAKE_THREAD_LIBS_INIT}
  )

add_executable (orc-scan
  FileScan.cc
  ToolsHelper.cc
  )

target_link_libraries (orc-scan
  orc
  ${CMAKE_THREAD_LIBS_INIT}
  )

add_executable (orc-metadata
  FileMetadata.cc
  ToolsHelper.cc
  )

target_link_libraries (orc-metadata
  orc
  orc::protobuf
  ${CMAKE_THREAD_LIBS_INIT}
  )

 add_executable (orc-statistics
  FileStatistics.cc
  )

target_link_libraries (orc-statistics
  orc
  ${CMAKE_THREAD_LIBS_INIT}
  )

add_executable (orc-memory
  FileMemory.cc
  ToolsHelper.cc
  )

target_link_libraries (orc-memory
  orc
  ${CMAKE_THREAD_LIBS_INIT}
  )

add_executable (timezone-dump
  TimezoneDump.cc
  )

target_link_libraries (timezone-dump
  orc
  ${CMAKE_THREAD_LIBS_INIT}
  )

add_executable (csv-import
  CSVFileImport.cc
  )

target_link_libraries (csv-import
  orc
  ${CMAKE_THREAD_LIBS_INIT}
  )

set(CPP_TOOL_NAMES
  orc-contents
  orc-metadata
  orc-statistics
  orc-scan
  orc-memory
  timezone-dump
  csv-import
  )

add_custom_target(tool-set ALL DEPENDS ${CPP_TOOL_NAMES})

install(TARGETS
   ${CPP_TOOL_NAMES}
   DESTINATION bin)
