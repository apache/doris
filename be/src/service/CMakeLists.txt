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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/service")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
list(REMOVE_ITEM SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/doris_main.cpp)

add_library(Service STATIC ${SRC_FILES})

pch_reuse(Service)

if (${MAKE_TEST} STREQUAL "OFF")
    add_executable(doris_be
        doris_main.cpp
    )

    if (ENABLE_CLANG_COVERAGE AND ENABLE_CLANG_COVERAGE STREQUAL ON AND COMPILER_CLANG)
        target_compile_definitions(doris_be PRIVATE -DLLVM_PROFILE)
    endif ()
    pch_reuse(doris_be)

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(doris_be PROPERTIES ENABLE_EXPORTS 1)

    target_link_libraries(doris_be
        ${DORIS_LINK_LIBS}
    )

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
    install(TARGETS doris_be DESTINATION ${OUTPUT_DIR}/lib/)

    if (OS_MACOSX AND ARCH_ARM AND "${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
        set(SHOULD_STRIP_DEBUG_INFO "ON")
    endif()

    if ("${STRIP_DEBUG_INFO}" STREQUAL "ON" OR "${SHOULD_STRIP_DEBUG_INFO}" STREQUAL "ON")
        if (OS_MACOSX)
            find_program(DSYMUTIL NAMES dsymutil)
            message(STATUS "dsymutil found: ${DSYMUTIL}")
            find_program(LLVM_STRIP NAMES llvm-strip)
            message(STATUS "llvm-strip found: ${LLVM_STRIP}")
            add_custom_command(TARGET doris_be POST_BUILD
                COMMAND ${DSYMUTIL} $<TARGET_FILE:doris_be>
                COMMAND ${LLVM_STRIP} --strip-all $<TARGET_FILE:doris_be>
                COMMAND mkdir -p ${OUTPUT_DIR}/lib
                COMMAND cp -rf doris_be.dSYM ${OUTPUT_DIR}/lib/
            )
        else()
            add_custom_command(TARGET doris_be POST_BUILD
                COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:doris_be> $<TARGET_FILE:doris_be>.dbg
                COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:doris_be>
                COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:doris_be>.dbg $<TARGET_FILE:doris_be>
                )

            install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
            install(FILES $<TARGET_FILE:doris_be>.dbg DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
        endif()
    endif()
endif()
