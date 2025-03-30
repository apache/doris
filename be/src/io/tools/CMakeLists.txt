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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/tools")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/tools")

# 编译proto文件
execute_process(
    COMMAND make clean -C ${CMAKE_CURRENT_SOURCE_DIR}/proto
    RESULT_VARIABLE CLEAN_RESULT
)

execute_process(
    COMMAND make -C ${CMAKE_CURRENT_SOURCE_DIR}/proto
    RESULT_VARIABLE MAKE_RESULT
)

if(NOT ${MAKE_RESULT} EQUAL 0)
    message(FATAL_ERROR "Failed to compile proto files")
endif()

# 打印当前源代码目录
message(STATUS "CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")

# 查找生成的proto文件
file(GLOB PROTO_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/build/proto/*.pb.cc")
file(GLOB PROTO_HDRS "${CMAKE_CURRENT_SOURCE_DIR}/build/proto/*.pb.h")

# 打印PROTO_SRCS和PROTO_HDRS
message(STATUS "PROTO_SRCS: ${PROTO_SRCS}")
message(STATUS "PROTO_HDRS: ${PROTO_HDRS}")

# 添加 file_cache_microbench 可执行文件
add_executable(file_cache_microbench
    file_cache_microbench.cpp
    ${PROTO_SRCS}
)

# 添加proto生成文件的包含路径
target_include_directories(file_cache_microbench PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/build/proto
)

# 链接所需的库
target_link_libraries(file_cache_microbench
    ${DORIS_LINK_LIBS}
    protobuf
)

# 安装规则
install(TARGETS file_cache_microbench DESTINATION ${OUTPUT_DIR}/lib/)
