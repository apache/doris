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

add_executable(meta_tool
    meta_tool.cpp
)

# This permits libraries loaded by dlopen to link to the symbols in the program.
set_target_properties(meta_tool PROPERTIES ENABLE_EXPORTS 1)

target_link_libraries(meta_tool
    ${DORIS_LINK_LIBS}
)

install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
install(TARGETS meta_tool DESTINATION ${OUTPUT_DIR}/lib/)

if (NOT OS_MACOSX)
# Meta tool never need debug info
add_custom_command(TARGET meta_tool POST_BUILD
    COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:meta_tool> $<TARGET_FILE:meta_tool>.dbg
    COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:meta_tool>
    COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:meta_tool>.dbg $<TARGET_FILE:meta_tool>
    )
endif()
