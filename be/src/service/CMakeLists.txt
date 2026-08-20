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

if (ENABLE_TLS)
    list(REMOVE_ITEM SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/server/oss/be_server_starter_factory.cpp)
    file(GLOB_RECURSE EXTRA_SOURCES CONFIGURE_DEPENDS
            "${CMAKE_CURRENT_SOURCE_DIR}/../${TLS_MODULE_DIR}/*.cpp")
    list(APPEND SRC_FILES ${EXTRA_SOURCES})
    list(REMOVE_ITEM SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/../${TLS_MODULE_DIR}/client_connection_provider.cpp)
endif()

add_library(Service STATIC ${SRC_FILES})

pch_reuse(Service)

# Unity build scoped to the http glue: ~60 small handlers re-parsing the same
# header closure once per file, merged into one unity TU (batch 0). Widening
# to the whole target at batch 8 was tried and measured slower: splitting the
# single http jumbo into 8-file batches repays the shared-closure parse per
# batch, and the heterogeneous non-http TUs gain too little to cover it
# (service segment 104s -> 132s slot time). So non-http sources stay
# individual. http_parser.cpp and be_thread_stack_action.cpp stay individual
# because their file-scope macros (CR/LF, UNW_LOCAL_ONLY) must not leak into
# unity siblings.
set(SERVICE_UNITY_SKIP ${SRC_FILES})
list(FILTER SERVICE_UNITY_SKIP EXCLUDE REGEX ".*/service/http/.*")
list(APPEND SERVICE_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/http/http_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/http/action/be_thread_stack_action.cpp)
doris_skip_unity_inclusion(${SERVICE_UNITY_SKIP})
set_target_properties(Service PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                         UNITY_BUILD_BATCH_SIZE 0)

if (${MAKE_TEST} STREQUAL "OFF" AND ${BUILD_BENCHMARK} STREQUAL "OFF")
    add_executable(doris_be
        doris_main.cpp
    )

    if (ENABLE_CLANG_COVERAGE AND ENABLE_CLANG_COVERAGE STREQUAL ON AND COMPILER_CLANG)
        target_compile_definitions(doris_be PRIVATE -DLLVM_PROFILE)
    endif ()
    pch_reuse(doris_be)

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(doris_be PROPERTIES ENABLE_EXPORTS 1)

    # ...but not the symbols of the RocksDB we link statically. Exporting those makes this
    # executable the definition every later-loaded library binds to, and a JNI library that
    # carries its own RocksDB then runs half on ours: the fluss scanner bundles frocksdbjni,
    # whose librocksdbjni.so defines 2576 rocksdb symbols under names identical to ours but
    # was built against the pre-C++11 libstdc++ string ABI. Objects laid out by one and used
    # by the other yield a garbage length, an std::bad_alloc that escapes the JNI frame, and
    # an aborted BE. Hiding this archive lets that library bind to its own copy.
    #
    # Scoped to the archive rather than dropping ENABLE_EXPORTS: what needs the exports is
    # native UDFs (runtime/user_function_cache.cpp dlopens them), and those use the Doris UDF
    # ABI, which has nothing to do with RocksDB. Crash stacks do not need it either -- they are
    # symbolized from debug info, which is why they name even anonymous-namespace functions.
    #
    # The same library also duplicates zstd, lz4, snappy, bzip2 and zlib symbols. Those are C
    # ABIs, stable across versions and layout-free, so they are left alone until something
    # shows otherwise -- unlike RocksDB, whose C++ objects are what actually corrupt.
    #
    # Linux-only: Apple's ld rejects --exclude-libs (a GNU ld option), and macOS does not
    # need it -- dyld's two-level namespace already binds each loaded library to the copy
    # it was linked against, so a JNI library carrying its own RocksDB never resolves to ours.
    if (NOT OS_MACOSX)
        target_link_options(doris_be PRIVATE "-Wl,--exclude-libs,librocksdb.a")
    endif()

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
