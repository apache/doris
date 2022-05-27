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

add_library(Service
    backend_options.cpp
    backend_service.cpp
    brpc_service.cpp
    http_service.cpp
    internal_service.cpp
)

if (${MAKE_TEST} STREQUAL "OFF")
    add_executable(palo_be
        doris_main.cpp
    )

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(palo_be PROPERTIES ENABLE_EXPORTS 1)

    target_link_libraries(palo_be
        ${DORIS_LINK_LIBS}
    )

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
    install(TARGETS palo_be DESTINATION ${OUTPUT_DIR}/lib/)

    if ("${STRIP_DEBUG_INFO}" STREQUAL "ON")
        add_custom_command(TARGET palo_be POST_BUILD
            COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:palo_be> $<TARGET_FILE:palo_be>.dbg
            COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:palo_be>
            COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:palo_be>.dbg $<TARGET_FILE:palo_be>
            )

        install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
        install(FILES $<TARGET_FILE:palo_be>.dbg DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
    endif()
endif()
