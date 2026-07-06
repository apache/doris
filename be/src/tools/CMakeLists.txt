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

target_include_directories(meta_tool PRIVATE ${PROJECT_SOURCE_DIR}/..)

pch_reuse(meta_tool)

# This permits libraries loaded by dlopen to link to the symbols in the program.
set_target_properties(meta_tool PROPERTIES ENABLE_EXPORTS 1)


if (COMPILER_CLANG)
    target_compile_options(meta_tool PRIVATE
    -Wno-implicit-int-conversion
    -Wno-shorten-64-to-32
    )
endif()

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

add_executable(packed_file_tool
    packed_file_tool.cpp
)

pch_reuse(packed_file_tool)

set_target_properties(packed_file_tool PROPERTIES ENABLE_EXPORTS 1)

if (COMPILER_CLANG)
    target_compile_options(packed_file_tool PRIVATE
    -Wno-implicit-int-conversion
    -Wno-shorten-64-to-32
    )
endif()

target_link_libraries(packed_file_tool
    ${DORIS_LINK_LIBS}
)

install(TARGETS packed_file_tool DESTINATION ${OUTPUT_DIR}/lib/)

if (NOT OS_MACOSX)
add_custom_command(TARGET packed_file_tool POST_BUILD
    COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:packed_file_tool> $<TARGET_FILE:packed_file_tool>.dbg
    COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:packed_file_tool>
    COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:packed_file_tool>.dbg $<TARGET_FILE:packed_file_tool>
    )
endif()

if (BUILD_INDEX_TOOL)
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
        add_custom_command(TARGET index_tool POST_BUILD
            COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:index_tool> $<TARGET_FILE:index_tool>.dbg
            COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:index_tool>
            COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:index_tool>.dbg $<TARGET_FILE:index_tool>
            )
    endif()
endif()

# Offline generator for the Japanese kuromoji dictionary. Compiles the UTF-8
# mecab-ipadic source into the four dict/kuromoji/*.bin files that the BE install
# rule ships. EXCLUDE_FROM_ALL and not installed: it is a maintenance tool, not
# part of doris_be. Regenerate with `ninja kuromoji_dict`.
add_executable(kuromoji_build_dict EXCLUDE_FROM_ALL
    kuromoji_build_dict.cpp
)

target_include_directories(kuromoji_build_dict PRIVATE ${PROJECT_SOURCE_DIR}/..)

pch_reuse(kuromoji_build_dict)

set_target_properties(kuromoji_build_dict PROPERTIES ENABLE_EXPORTS 1)

if (COMPILER_CLANG)
    target_compile_options(kuromoji_build_dict PRIVATE
    -Wno-implicit-int-conversion
    -Wno-shorten-64-to-32
    )
endif()

target_link_libraries(kuromoji_build_dict
    ${DORIS_LINK_LIBS}
)

# `ninja kuromoji_dict` runs the tool over the staged mecab-ipadic source and
# (re)writes dict/kuromoji/*.bin. Point KUROMOJI_IPADIC_SRC elsewhere if the
# source is not under the thirdparty share directory.
set(KUROMOJI_IPADIC_SRC "${THIRDPARTY_DIR}/share/mecab-ipadic-2.7.0-20250920"
    CACHE PATH "UTF-8 mecab-ipadic source directory used to generate the kuromoji dictionary")
set(KUROMOJI_DICT_OUT "${BASE_DIR}/dict/kuromoji")
add_custom_command(
    OUTPUT "${KUROMOJI_DICT_OUT}/system.bin" "${KUROMOJI_DICT_OUT}/matrix.bin"
           "${KUROMOJI_DICT_OUT}/chardef.bin" "${KUROMOJI_DICT_OUT}/unkdict.bin"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${KUROMOJI_DICT_OUT}"
    COMMAND $<TARGET_FILE:kuromoji_build_dict> "${KUROMOJI_IPADIC_SRC}" "${KUROMOJI_DICT_OUT}"
    DEPENDS kuromoji_build_dict
    COMMENT "Generating kuromoji IPADIC dictionary from ${KUROMOJI_IPADIC_SRC}"
    VERBATIM)
# A real build generates the dictionary as part of ALL so the package
# always contains it. However, Unit-test builds (MAKE_TEST=ON) keep
# kuromoji_dict as a manual target.
if (NOT MAKE_TEST)
    add_custom_target(kuromoji_dict ALL DEPENDS "${KUROMOJI_DICT_OUT}/system.bin")
else()
    add_custom_target(kuromoji_dict DEPENDS "${KUROMOJI_DICT_OUT}/system.bin")
endif()
