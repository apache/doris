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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/index-tools")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/index-tools")

add_executable(index_tool
    index_tool.cpp
)

pch_reuse(index_tool)

# This permits libraries loaded by dlopen to link to the symbols in the program.
set_target_properties(index_tool PROPERTIES ENABLE_EXPORTS 1)


target_link_libraries(index_tool
    ${DORIS_LINK_LIBS}
)

if (COMPILER_CLANG)
    target_compile_options(index_tool PRIVATE
    -Wno-implicit-int-conversion
    -Wno-shorten-64-to-32
    )
endif()

install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
install(TARGETS index_tool DESTINATION ${OUTPUT_DIR}/lib/)
if (NOT OS_MACOSX)
# strip debug info to save space
add_custom_command(TARGET index_tool POST_BUILD
    COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:index_tool> $<TARGET_FILE:index_tool>.dbg
    COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:index_tool>
    COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:index_tool>.dbg $<TARGET_FILE:index_tool>
    )
endif()
