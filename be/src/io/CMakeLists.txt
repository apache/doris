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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/io")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/io")

file(GLOB_RECURSE IO_FILES CONFIGURE_DEPENDS *.cpp)
if(BUILD_AZURE STREQUAL "OFF")
    list(REMOVE_ITEM IO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/fs/azure_obj_storage_client.cpp")
endif()
list(REMOVE_ITEM IO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/fs/benchmark/fs_benchmark_tool.cpp")

add_library(IO STATIC ${IO_FILES})

pch_reuse(IO)

if (${BUILD_FS_BENCHMARK} STREQUAL "ON")
    add_executable(fs_benchmark_tool
        fs/benchmark/fs_benchmark_tool.cpp
    )

    pch_reuse(fs_benchmark_tool)

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(fs_benchmark_tool PROPERTIES ENABLE_EXPORTS 1)

    target_link_libraries(fs_benchmark_tool
        ${DORIS_LINK_LIBS}
        benchmark
    )

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
    install(TARGETS fs_benchmark_tool DESTINATION ${OUTPUT_DIR}/lib/)

    add_custom_command(TARGET fs_benchmark_tool POST_BUILD
        COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:fs_benchmark_tool> $<TARGET_FILE:fs_benchmark_tool>.dbg
        COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:fs_benchmark_tool>
        COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:fs_benchmark_tool>.dbg $<TARGET_FILE:fs_benchmark_tool>
        )

endif()
