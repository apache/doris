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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exec")

file(GLOB_RECURSE EXEC_FILES CONFIGURE_DEPENDS *.cpp)

# Task executor simulator files are built as a separate executable
list(FILTER EXEC_FILES EXCLUDE REGEX ".*scan/task_executor/tools/simulator/.*")

if (WITH_LZO)
    set(EXEC_FILES ${EXEC_FILES}
        lzo_decompressor.cpp
    )
endif()

add_library(Exec STATIC
    ${EXEC_FILES}
)

pch_reuse(Exec)

if (${BUILD_TASK_EXECUTOR_SIMULATOR} STREQUAL "ON")
    file(GLOB_RECURSE SIMULATOR_FILES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/scan/task_executor/tools/simulator/*.cpp"
    )
    message(STATUS "Add Task Executor Simulator To Build")
    add_executable(task_executor_simulator ${SIMULATOR_FILES})

    pch_reuse(task_executor_simulator)

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(task_executor_simulator PROPERTIES ENABLE_EXPORTS 1)

    target_link_libraries(task_executor_simulator
            ${DORIS_LINK_LIBS}
            )

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
    install(TARGETS task_executor_simulator DESTINATION ${OUTPUT_DIR}/lib/)

    if (NOT OS_MACOSX)
        add_custom_command(TARGET task_executor_simulator POST_BUILD
                COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:task_executor_simulator> $<TARGET_FILE:task_executor_simulator>.dbg
                COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:task_executor_simulator>
                COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:task_executor_simulator>.dbg $<TARGET_FILE:task_executor_simulator>
                )
    endif()
endif()