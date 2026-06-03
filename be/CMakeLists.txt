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

cmake_minimum_required(VERSION 3.19.2)

project(doris CXX C)

# Enable position-independent code globally.
# The pch target (STATIC_LIBRARY) inherits -fPIC from COMMON_THIRDPARTY dependencies;
# all consumer targets must also use -fPIC to avoid "is pie differs in AST file" PCH errors.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Write compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# set platforms

if (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
    set (ARCH_AMD64 1)
endif ()
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*)")
    set (ARCH_AARCH64 1)
endif ()
if (ARCH_AARCH64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
    set (ARCH_ARM 1)
endif ()
if (CMAKE_LIBRARY_ARCHITECTURE MATCHES "i386")
    set (ARCH_I386 1)
endif ()
if ((ARCH_ARM AND NOT ARCH_AARCH64) OR ARCH_I386)
    message (FATAL_ERROR "32bit platforms are not supported")
endif ()

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(ppc64le.*|PPC64LE.*)")
    set (ARCH_PPC64LE 1)
endif ()

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set (OS_LINUX 1)
    add_definitions(-D OS_LINUX)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set (OS_MACOSX 1)
    add_definitions(-D OS_MACOSX)
endif ()

if (OS_MACOSX)
    add_definitions(-DBOOST_PROCESS_POSIX_NO_SIGTIMEDWAIT -UBOOST_POSIX_HAS_SIGTIMEDWAIT)
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
    option(ENABLE_PCH "enable pch" OFF)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
    option(ENABLE_PCH "enable pch" ON)
endif ()

# Set boost/stacktrace use backtrace api to unwind
if (NOT OS_MACOSX)
    add_definitions(-DBOOST_STACKTRACE_USE_BACKTRACE)
else()
    add_definitions(-DBOOST_STACKTRACE_USE_NOOP)
endif()

# enable glog custom prefix
add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT)

# Options
option(GLIBC_COMPATIBILITY "Enable compatibility with older glibc libraries." ON)
option(USE_LIBCPP "Use libc++" OFF)
option(USE_UNWIND "Use libunwind" ON)
option(USE_JEMALLOC "Use jemalloc" ON)
if (OS_MACOSX)
    set(GLIBC_COMPATIBILITY OFF)
    set(USE_LIBCPP ON)
    set(USE_UNWIND OFF)
endif()
# MSAN requires instrumented C++ stdlib — force libc++ and disable jemalloc/unwind/glibc-compat
if ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    set(USE_LIBCPP ON)
    set(USE_JEMALLOC OFF)
    set(USE_UNWIND OFF)
    set(GLIBC_COMPATIBILITY OFF)
endif()

if (DISPLAY_BUILD_TIME)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "time -f 'TimeUsage: real=%es, user=%Us, sys=%Ss'")
endif()

if (BUILD_FILE_CACHE_MICROBENCH_TOOL)
    add_definitions(-DBE_TEST)
    add_definitions(-DBUILD_FILE_CACHE_MICROBENCH_TOOL)
    add_definitions(-DBE_BENCHMARK)
endif()

message(STATUS "GLIBC_COMPATIBILITY is ${GLIBC_COMPATIBILITY}")
message(STATUS "USE_LIBCPP is ${USE_LIBCPP}")
message(STATUS "USE_JEMALLOC is ${USE_JEMALLOC}")
message(STATUS "USE_UNWIND is ${USE_UNWIND}")
message(STATUS "ENABLE_PCH is ${ENABLE_PCH}")
message(STATUS "USE_AVX2 is ${USE_AVX2}")

# set CMAKE_BUILD_TYPE
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif()

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

# set CMAKE_BUILD_TARGET_ARCH
# use `lscpu | grep 'Architecture' | awk '{print $2}'` only support system which language is en_US.UTF-8
execute_process(COMMAND bash "-c" "uname -m"
                OUTPUT_VARIABLE
                CMAKE_BUILD_TARGET_ARCH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Build target arch is ${CMAKE_BUILD_TARGET_ARCH}")

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "aarch64" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "arm64")
    set (ARCH_ARM 1)
endif()

# Set dirs
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
if (DEFINED ENV{DORIS_HOME})
    message(STATUS "DORIS_HOME is $ENV{DORIS_HOME}")
else()
    message(FATAL_ERROR "DORIS_HOME is not defined")
endif()
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(GENSRC_DIR "${BASE_DIR}/../gensrc/build/")
set(COMMON_SRC_DIR "${BASE_DIR}/../common")
set(SRC_DIR "${BASE_DIR}/src/")
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")
if (NOT DEFINED ENV{DORIS_THIRDPARTY})
    set(ENV{DORIS_THIRDPARTY} "$ENV{DORIS_HOME}/thirdparty")
endif()
set(THIRDPARTY_SRC_DIR "$ENV{DORIS_THIRDPARTY}")
# Prebuilt thirdparty install prefix (used by USE_CONTRIB_SOURCE=OFF / prebuilt mode).
set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed")
message(STATUS "THIRDPARTY_DIR is ${THIRDPARTY_DIR}")



option(MAKE_TEST "ON for make unit test or OFF for not" OFF)
message(STATUS "make test: ${MAKE_TEST}")
option(BUILD_BENCHMARK "ON for make google benchmark or OFF for not" OFF)
message(STATUS "make benchmark: ${BUILD_BENCHMARK}")

option(BUILD_FS_BENCHMARK "ON for building fs benchmark tool or OFF for not" OFF)
message(STATUS "build fs benchmark tool: ${BUILD_FS_BENCHMARK}")

option(BUILD_TASK_EXECUTOR_SIMULATOR "ON for building task executor simulator or OFF for not" OFF)
message(STATUS "build task executor simulator: ${BUILD_TASK_EXECUTOR_SIMULATOR}")

option(BUILD_FILE_CACHE_LRU_TOOL "ON for building file cache lru tool or OFF for not" OFF)
message(STATUS "build file cache lru tool: ${BUILD_FILE_CACHE_LRU_TOOL}")

option(ENABLE_PAIMON_CPP "Enable Paimon C++ integration" OFF)
set(PAIMON_HOME "" CACHE PATH "Paimon install prefix")

# Allow env to override when reconfiguring (avoid picking /usr/local).
if (DEFINED ENV{ENABLE_PAIMON_CPP})
    set(ENABLE_PAIMON_CPP "$ENV{ENABLE_PAIMON_CPP}" CACHE BOOL "" FORCE)
endif()
if (DEFINED ENV{PAIMON_HOME} AND NOT PAIMON_HOME)
    set(PAIMON_HOME "$ENV{PAIMON_HOME}" CACHE PATH "" FORCE)
endif()

option(BUILD_RUST_READERS "Build Rust-based format readers (Lance, etc.)" OFF)
if (DEFINED ENV{BUILD_RUST_READERS})
    set(BUILD_RUST_READERS "$ENV{BUILD_RUST_READERS}" CACHE BOOL "" FORCE)
endif()
# Auto-enable if pre-built Rust library exists (from zigbuild or manylinux2014)
if (NOT BUILD_RUST_READERS)
    if (EXISTS "${SRC_DIR}/rust/doris-native/target/x86_64-unknown-linux-gnu/release/libdoris_ffi.a"
        OR EXISTS "${SRC_DIR}/rust/doris-native/target/release/libdoris_ffi.a")
        set(BUILD_RUST_READERS ON)
        message(STATUS "Auto-enabling BUILD_RUST_READERS: pre-built library found")
    endif()
endif()
if (BUILD_RUST_READERS)
    # rust.cmake detects pre-built .a or builds via Corrosion.
    # If neither exists, it sets BUILD_RUST_READERS=OFF and returns.
    include(cmake/rust.cmake)
endif()
if (BUILD_RUST_READERS)
    add_definitions(-DBUILD_RUST_READERS)
endif()

set(CMAKE_SKIP_RPATH TRUE)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME ON)

# Compile generated source if necessary
# First, ensure protoc and thrift compiler are available.
# When building from source (no thirdparty/installed), we compile them on the fly.
set(_PROTOC_BIN "$ENV{DORIS_THIRDPARTY}/installed/bin/protoc")
set(_THRIFT_BIN "$ENV{DORIS_THIRDPARTY}/installed/bin/thrift")
set(_GENSRC_TOOLS_DIR "$ENV{DORIS_THIRDPARTY}/_gensrc_tools")

if(NOT EXISTS "${_PROTOC_BIN}")
    message(STATUS "protoc not found at ${_PROTOC_BIN}, building from source...")
    set(_PROTOC_BUILD_DIR "${_GENSRC_TOOLS_DIR}/protoc_build")
    file(MAKE_DIRECTORY "${_PROTOC_BUILD_DIR}")
    execute_process(
        COMMAND ${CMAKE_COMMAND}
            -DCMAKE_BUILD_TYPE=Release
            -Dprotobuf_BUILD_TESTS=OFF
            -Dprotobuf_BUILD_SHARED_LIBS=OFF
            "${THIRDPARTY_SRC_DIR}/src/protobuf-21.11/cmake"
        WORKING_DIRECTORY "${_PROTOC_BUILD_DIR}"
        RESULT_VARIABLE _PROTOC_CMAKE_RESULT)
    if(_PROTOC_CMAKE_RESULT EQUAL 0)
        execute_process(
            COMMAND ${CMAKE_COMMAND} --build . --target protoc -j
            WORKING_DIRECTORY "${_PROTOC_BUILD_DIR}"
            RESULT_VARIABLE _PROTOC_BUILD_RESULT)
    endif()
    if(_PROTOC_BUILD_RESULT EQUAL 0 AND EXISTS "${_PROTOC_BUILD_DIR}/protoc")
        set(_PROTOC_BIN "${_PROTOC_BUILD_DIR}/protoc")
        message(STATUS "Built protoc: ${_PROTOC_BIN}")
    else()
        message(WARNING "Failed to build protoc from source")
    endif()
endif()

if(NOT EXISTS "${_THRIFT_BIN}")
    message(STATUS "thrift compiler not found at ${_THRIFT_BIN}, building from source...")
    set(_THRIFT_BUILD_DIR "${_GENSRC_TOOLS_DIR}/thrift_build")
    file(MAKE_DIRECTORY "${_THRIFT_BUILD_DIR}")
    execute_process(
        COMMAND ${CMAKE_COMMAND}
            -DCMAKE_BUILD_TYPE=Release
            -DBUILD_TESTING=OFF
            -DBUILD_COMPILER=ON
            -DBUILD_TUTORIALS=OFF
            -DBUILD_EXAMPLES=OFF
            -DBUILD_LIBRARIES=OFF
            -DWITH_CPP=OFF
            -DWITH_JAVA=OFF
            -DWITH_PYTHON=OFF
            -DWITH_HASKELL=OFF
            -DWITH_C_GLIB=OFF
            "${THIRDPARTY_SRC_DIR}/src/thrift-0.16.0"
        WORKING_DIRECTORY "${_THRIFT_BUILD_DIR}"
        RESULT_VARIABLE _THRIFT_CMAKE_RESULT)
    if(_THRIFT_CMAKE_RESULT EQUAL 0)
        execute_process(
            COMMAND ${CMAKE_COMMAND} --build . --target thrift-compiler -j
            WORKING_DIRECTORY "${_THRIFT_BUILD_DIR}"
            RESULT_VARIABLE _THRIFT_BUILD_RESULT)
    endif()
    # thrift compiler binary ends up in compiler/cpp/bin/thrift (RUNTIME_OUTPUT_DIRECTORY=bin/)
    find_program(_THRIFT_BUILT NAMES thrift
        PATHS "${_THRIFT_BUILD_DIR}/bin" "${_THRIFT_BUILD_DIR}/compiler/cpp/bin" "${_THRIFT_BUILD_DIR}/compiler/cpp"
        NO_DEFAULT_PATH)
    if(_THRIFT_BUILD_RESULT EQUAL 0 AND _THRIFT_BUILT)
        set(_THRIFT_BIN "${_THRIFT_BUILT}")
        message(STATUS "Built thrift compiler: ${_THRIFT_BIN}")
    else()
        message(WARNING "Failed to build thrift compiler from source")
    endif()
endif()

message(STATUS "build gensrc if necessary (PROTOC=${_PROTOC_BIN}, THRIFT=${_THRIFT_BIN})")
set(ENV{PROTOC} "${_PROTOC_BIN}")
set(ENV{THRIFT} "${_THRIFT_BIN}")
execute_process(COMMAND make -C ${BASE_DIR}/../gensrc/
                RESULT_VARIABLE MAKE_GENSRC_RESULT)
if(NOT ${MAKE_GENSRC_RESULT} EQUAL 0 AND NOT APPLE)
    message(FATAL_ERROR "Failed to build ${BASE_DIR}/../gensrc/")
endif()

# Set if use libazure or not
option(BUILD_AZURE "ON for building azure support for BE or OFF for not" OFF)
message(STATUS "build azure: ${BUILD_AZURE}")
if(BUILD_AZURE STREQUAL "ON")
    add_definitions(-DUSE_AZURE)
endif()

# =========================================================================
# Build thirdparty from source
# Compiler flags (ASAN/TSAN/Debug/Release) propagate to all thirdparty libs
# =========================================================================
# When using libc++, inject -stdlib=libc++ into CMAKE_CXX_FLAGS before thirdparty
# so all libraries (thirdparty, contrib/clucene, contrib/faiss) are compiled with
# libc++ for ABI compatibility. add_compile_options() alone doesn't propagate to
# add_subdirectory children.
if (USE_LIBCPP)
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
        # MSAN mode: use MSAN-instrumented libc++ built from source.
        # This prevents false positives from std::string, std::vector etc.
        # Built by: thirdparty/build_msan_libcxx/ (LLVM runtimes with LLVM_USE_SANITIZER=MemoryWithOrigins)
        set(_MSAN_LIBCXX_DIR "$ENV{DORIS_THIRDPARTY}/build_msan_libcxx")
        if (EXISTS "${_MSAN_LIBCXX_DIR}/lib/libc++.a")
            message(STATUS "MSAN: using instrumented libc++ from ${_MSAN_LIBCXX_DIR}")
            set(_LIBCPP_INC "${_MSAN_LIBCXX_DIR}/include/c++/v1")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -nostdinc++ -isystem ${_LIBCPP_INC}")
            # Propagate MSAN flags to thirdparty so ALL code is instrumented.
            # Aligned with ClickHouse: origins=2 (bare flag) + use-after-dtor.
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=memory -fsanitize-recover=memory -fsanitize-memory-track-origins=2 -fsanitize-memory-use-after-dtor -fsanitize-ignorelist=$ENV{DORIS_HOME}/conf/msan_ignorelist.txt -fno-omit-frame-pointer")
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=memory -fsanitize-recover=memory -fsanitize-memory-track-origins=2 -fsanitize-memory-use-after-dtor -fsanitize-ignorelist=$ENV{DORIS_HOME}/conf/msan_ignorelist.txt -fno-omit-frame-pointer")
            # Will link instrumented libc++.a later (see MSAN link section below)
            set(MSAN_LIBCXX_LIBS
                "${_MSAN_LIBCXX_DIR}/lib/libc++.a"
                "${_MSAN_LIBCXX_DIR}/lib/libc++abi.a"
                "${_MSAN_LIBCXX_DIR}/lib/libunwind.a")
        else()
            message(WARNING "MSAN: instrumented libc++ not found at ${_MSAN_LIBCXX_DIR}, falling back to toolchain libc++")
            execute_process(COMMAND ${CMAKE_CXX_COMPILER} --print-resource-dir
                OUTPUT_VARIABLE _CLANG_RES OUTPUT_STRIP_TRAILING_WHITESPACE)
            get_filename_component(_LIBCPP_INC "${_CLANG_RES}/../../../include/c++/v1" ABSOLUTE)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -isystem ${_LIBCPP_INC}")
        endif()
    else()
        execute_process(COMMAND ${CMAKE_CXX_COMPILER} --print-resource-dir
            OUTPUT_VARIABLE _CLANG_RES OUTPUT_STRIP_TRAILING_WHITESPACE)
        get_filename_component(_LIBCPP_INC "${_CLANG_RES}/../../../include/c++/v1" ABSOLUTE)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -isystem ${_LIBCPP_INC}")
    endif()
endif()
# Thirdparty mode selection
# - USE_CONTRIB_SOURCE=ON : build thirdparty from source under thirdparty/src/
#   via add_subdirectory. Required for sanitizer builds (ASAN/TSAN/MSAN/UBSAN/LSAN)
#   so the IR pass can instrument them.
# - USE_CONTRIB_SOURCE=OFF: link prebuilt static libs from $ENV{DORIS_THIRDPARTY}/installed.
#   Faster clean rebuilds and matches the legacy doris master flow. Only valid for
#   Release/Debug because prebuilt .a cannot be sanitizer-instrumented.
set(_DORIS_SANITIZER_BUILD_TYPES "ASAN" "ASAN_UT" "TSAN" "LSAN" "UBSAN" "MSAN")
if (CMAKE_BUILD_TYPE IN_LIST _DORIS_SANITIZER_BUILD_TYPES)
    if (DEFINED USE_CONTRIB_SOURCE AND NOT USE_CONTRIB_SOURCE)
        message(FATAL_ERROR
            "USE_CONTRIB_SOURCE=OFF is not supported with sanitizer build type "
            "${CMAKE_BUILD_TYPE}. Prebuilt thirdparty .a cannot be instrumented by "
            "the sanitizer IR pass. Drop -DUSE_CONTRIB_SOURCE=OFF or pick a "
            "non-sanitizer build type.")
    endif()
    set(USE_CONTRIB_SOURCE ON CACHE BOOL "Build thirdparty from source" FORCE)
    message(STATUS "Sanitizer build (${CMAKE_BUILD_TYPE}): forcing USE_CONTRIB_SOURCE=ON")
else()
    option(USE_CONTRIB_SOURCE "Build thirdparty from source" ON)
endif()

if (USE_CONTRIB_SOURCE)
    message(STATUS "USE_CONTRIB_SOURCE=ON: building thirdparty from source")
    add_subdirectory(${THIRDPARTY_SRC_DIR} ${CMAKE_BINARY_DIR}/thirdparty)
    # GPERFTOOLS_HOME points to source-built gperftools
    set(GPERFTOOLS_HOME "${CMAKE_BINARY_DIR}/thirdparty/gperftools")
    # COMMON_THIRDPARTY is set by thirdparty/CMakeLists.txt via PARENT_SCOPE
else()
    message(STATUS "USE_CONTRIB_SOURCE=OFF: linking prebuilt thirdparty from ${THIRDPARTY_DIR}")
    if (NOT EXISTS "${THIRDPARTY_DIR}/lib")
        message(FATAL_ERROR
            "Prebuilt thirdparty not found at ${THIRDPARTY_DIR}/lib. "
            "Run 'bash thirdparty/build-thirdparty.sh' first, or use "
            "-DUSE_CONTRIB_SOURCE=ON to build from source.")
    endif()
    # Prebuilt thirdparty installs gperftools under installed/gperftools/, not installed/.
    set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")

    # Source mode creates Boost::* / absl::* / OpenMP::* namespace targets via
    # add_subdirectory(thirdparty); prebuilt mode needs find_package() to declare
    # the equivalent IMPORTED targets against the prebuilt install tree.
    set(Boost_DEBUG FALSE)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_ROOT ${THIRDPARTY_DIR})
    set(Boost_NO_BOOST_CMAKE OFF)
    find_package(Boost REQUIRED COMPONENTS system date_time container)

    set(absl_DIR ${THIRDPARTY_DIR}/lib64/cmake/absl)
    find_package(absl REQUIRED)

    # be/cmake/thirdparty.cmake declares IMPORTED targets and populates COMMON_THIRDPARTY
    include(${BASE_DIR}/cmake/thirdparty.cmake)
endif()

find_program(THRIFT_COMPILER thrift ${CMAKE_SOURCE_DIR}/bin)

set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(BUILD_JAVA OFF)
set(BUILD_CPP_TESTS OFF)
set(STOP_BUILD_ON_WARNING OFF)
set(BUILD_LIBHDFSPP OFF)
# zlib is built from source via add_subdirectory(thirdparty)
SET(ZLIB_USE_STATIC_LIBS ON)
SET(CONTRIB_PATH "${PROJECT_SOURCE_DIR}/../contrib")

# Tell ORC to use our source-built zlib instead of vendoring its own.
# Set ZLIB_HOME so ORC calls find_package(ZLIB) with its own FindZLIB.cmake.
# Create a prefix dir with symlinks to source headers and build output.
set(_ORC_ZLIB_PREFIX "${CMAKE_BINARY_DIR}/orc_zlib_prefix")
file(MAKE_DIRECTORY "${_ORC_ZLIB_PREFIX}/include")
file(MAKE_DIRECTORY "${_ORC_ZLIB_PREFIX}/lib")
file(GLOB _ZLIB_HDRS "${CMAKE_SOURCE_DIR}/../thirdparty/src/zlib-1.3.1/*.h")
file(COPY ${_ZLIB_HDRS} DESTINATION "${_ORC_ZLIB_PREFIX}/include")
# Also copy generated zconf.h from build dir (if available)
if(EXISTS "${CMAKE_BINARY_DIR}/thirdparty/zlib/zconf.h")
    file(COPY "${CMAKE_BINARY_DIR}/thirdparty/zlib/zconf.h" DESTINATION "${_ORC_ZLIB_PREFIX}/include")
endif()
# Placeholder for libz.a; actual file is produced by zlibstatic target during build
file(WRITE "${_ORC_ZLIB_PREFIX}/lib/libz.a" "")
set(ZLIB_HOME "${_ORC_ZLIB_PREFIX}" CACHE PATH "" FORCE)

# Tell ORC to use our source-built protobuf, snappy, lz4, zstd instead of vendoring.
# Create prefix dirs with headers + symlinked libraries.
set(_TP_SRC "${CMAKE_SOURCE_DIR}/../thirdparty/src")

# Protobuf
set(_ORC_PB_PREFIX "${CMAKE_BINARY_DIR}/orc_protobuf_prefix")
file(MAKE_DIRECTORY "${_ORC_PB_PREFIX}/include" "${_ORC_PB_PREFIX}/lib" "${_ORC_PB_PREFIX}/bin")
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink "${_TP_SRC}/protobuf-21.11/src/google" "${_ORC_PB_PREFIX}/include/google" ERROR_QUIET)
file(WRITE "${_ORC_PB_PREFIX}/lib/libprotobuf.a" "")
file(WRITE "${_ORC_PB_PREFIX}/lib/libprotoc.a" "")
# protoc binary — use the one built by gensrc tools
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink "$ENV{DORIS_THIRDPARTY}/installed/bin/protoc" "${_ORC_PB_PREFIX}/bin/protoc" ERROR_QUIET)
set(PROTOBUF_HOME "${_ORC_PB_PREFIX}" CACHE PATH "" FORCE)

# Snappy
set(_ORC_SNAPPY_PREFIX "${CMAKE_BINARY_DIR}/orc_snappy_prefix")
file(MAKE_DIRECTORY "${_ORC_SNAPPY_PREFIX}/include" "${_ORC_SNAPPY_PREFIX}/lib")
file(GLOB _SNAPPY_HDRS "${_TP_SRC}/snappy-1.1.10/*.h")
file(COPY ${_SNAPPY_HDRS} DESTINATION "${_ORC_SNAPPY_PREFIX}/include")
file(WRITE "${_ORC_SNAPPY_PREFIX}/lib/libsnappy.a" "")
set(SNAPPY_HOME "${_ORC_SNAPPY_PREFIX}" CACHE PATH "" FORCE)

# LZ4
set(_ORC_LZ4_PREFIX "${CMAKE_BINARY_DIR}/orc_lz4_prefix")
file(MAKE_DIRECTORY "${_ORC_LZ4_PREFIX}/include" "${_ORC_LZ4_PREFIX}/lib")
file(GLOB _LZ4_HDRS "${_TP_SRC}/lz4-1.9.4/lib/*.h")
file(COPY ${_LZ4_HDRS} DESTINATION "${_ORC_LZ4_PREFIX}/include")
file(WRITE "${_ORC_LZ4_PREFIX}/lib/liblz4.a" "")
set(LZ4_HOME "${_ORC_LZ4_PREFIX}" CACHE PATH "" FORCE)

# ZSTD
set(_ORC_ZSTD_PREFIX "${CMAKE_BINARY_DIR}/orc_zstd_prefix")
file(MAKE_DIRECTORY "${_ORC_ZSTD_PREFIX}/include" "${_ORC_ZSTD_PREFIX}/lib")
file(GLOB _ZSTD_HDRS "${_TP_SRC}/zstd-1.5.7/lib/*.h")
file(COPY ${_ZSTD_HDRS} DESTINATION "${_ORC_ZSTD_PREFIX}/include")
file(WRITE "${_ORC_ZSTD_PREFIX}/lib/libzstd.a" "")
set(ZSTD_HOME "${_ORC_ZSTD_PREFIX}" CACHE PATH "" FORCE)

add_subdirectory(${CONTRIB_PATH}/apache-orc ${PROJECT_BINARY_DIR}/apache-orc EXCLUDE_FROM_ALL)
target_compile_options(orc PRIVATE -Wno-implicit-fallthrough -w -include functional)
# ORC Compression.cc needs libdeflate.h.
# In source mode: from thirdparty/src/libdeflate-1.19/.
# In prebuilt mode: from thirdparty/installed/include/ (populated by build-thirdparty.sh).
if (USE_CONTRIB_SOURCE)
    target_include_directories(orc PRIVATE
        ${CMAKE_SOURCE_DIR}/../thirdparty/src/libdeflate-1.19
    )
else()
    target_include_directories(orc PRIVATE
        ${CMAKE_SOURCE_DIR}/../thirdparty/installed/include
    )
endif()
# Override ORC's vendored library targets to use our source-built libraries.
# ORC creates orc_zlib/orc_snappy/orc_lz4/orc_zstd INTERFACE targets linking placeholder .a files.
# Re-link them to our actual CMake targets.
set_target_properties(orc_zlib PROPERTIES INTERFACE_LINK_LIBRARIES zlibstatic)
set_target_properties(orc_snappy PROPERTIES INTERFACE_LINK_LIBRARIES snappy)
set_target_properties(orc_lz4 PROPERTIES INTERFACE_LINK_LIBRARIES lz4_static)
set_target_properties(orc_zstd PROPERTIES INTERFACE_LINK_LIBRARIES libzstd_static)
if(TARGET orc_protobuf)
    set_target_properties(orc_protobuf PROPERTIES INTERFACE_LINK_LIBRARIES libprotobuf)
endif()

option(BUILD_STATIC_LIBRARIES "Build static libraries" ON)
option(BUILD_SHARED_LIBRARIES "Build shared libraries" OFF)
option(BUILD_CONTRIBS_LIB "Build contribs lib" ON)

# Boost and Roaring are built from source via add_subdirectory(thirdparty)
set(USE_STAT64 0)

# disable clucene bthread supported.
set(USE_BTHREAD OFF)

# Roaring is already built from source via add_subdirectory(thirdparty).
# Set Roaring_FOUND to prevent CLucene's FindRoaring.cmake from creating a duplicate target.
# CLucene uses #include <roaring/roaring.hh>, but CRoaring keeps roaring.hh in cpp/,
# so we copy it into include/roaring/ to create the expected layout.
set(_CROARING_SRC "${PROJECT_SOURCE_DIR}/../thirdparty/src/CRoaring-2.1.2")
if(NOT EXISTS "${_CROARING_SRC}/include/roaring/roaring.hh")
    file(COPY "${_CROARING_SRC}/cpp/roaring.hh" "${_CROARING_SRC}/cpp/roaring64map.hh"
         DESTINATION "${_CROARING_SRC}/include/roaring/")
endif()
set(Roaring_FOUND TRUE)
set(Roaring_LIBRARY roaring)
set(Roaring_INCLUDE_DIR "${_CROARING_SRC}/include")

# Pre-set clucene type detection results to avoid broken typedef generation
# on modern systems where stdint.h provides all these types natively.
set(TYPE_INT8_T "/* undef int8_t int8_t */" CACHE STRING "" FORCE)
set(TYPE_UINT8_T "/* undef uint8_t uint8_t */" CACHE STRING "" FORCE)
set(TYPE_INT16_T "/* undef int16_t int16_t */" CACHE STRING "" FORCE)
set(TYPE_UINT16_T "/* undef uint16_t uint16_t */" CACHE STRING "" FORCE)
set(TYPE_INT32_T "/* undef int32_t int32_t */" CACHE STRING "" FORCE)
set(TYPE_UINT32_T "/* undef uint32_t uint32_t */" CACHE STRING "" FORCE)
set(TYPE_INT64_T "/* undef int64_t int64_t */" CACHE STRING "" FORCE)
set(TYPE_UINT64_T "/* undef uint64_t uint64_t */" CACHE STRING "" FORCE)
set(TYPE_SIZE_T "/* undef size_t size_t */" CACHE STRING "" FORCE)
# CLucene: previously we stripped sanitizer flags because try_compile tests
# failed under MSAN. With pre-set results below, we can now keep sanitizer
# flags so CLucene is fully MSAN-instrumented.
# (For ASAN/LSAN, the strip may still be needed — will handle if issue arises.)
set(_CL_HAVE_NO_SNPRINTF_BUG 1 CACHE INTERNAL "" FORCE)
set(_CL_HAVE_NAMESPACES 1 CACHE INTERNAL "" FORCE)
# For ASAN/LSAN keep old strip behavior as a fallback.
if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    set(_SAVED_CXX_FLAGS_CLUCENE "${CMAKE_CXX_FLAGS}")
    set(_SAVED_C_FLAGS_CLUCENE "${CMAKE_C_FLAGS}")
    string(REGEX REPLACE "-fsanitize[^ ]*" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    string(REGEX REPLACE "-fsanitize[^ ]*" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
endif()
# Out of source build need to set the binary dir
add_subdirectory(${CONTRIB_PATH}/clucene ${PROJECT_BINARY_DIR}/clucene EXCLUDE_FROM_ALL)
if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    set(CMAKE_CXX_FLAGS "${_SAVED_CXX_FLAGS_CLUCENE}")
    set(CMAKE_C_FLAGS "${_SAVED_C_FLAGS_CLUCENE}")
endif()

set(clucene_options -w -Wall)
if (COMPILER_CLANG)
    set(clucene_options ${clucene_options} -Wno-c++11-narrowing)
else ()
    set(clucene_options ${clucene_options} -Wno-narrowing)
endif()

target_compile_options(clucene-core-static PRIVATE ${clucene_options})
target_compile_options(clucene-shared-static PRIVATE ${clucene_options})
target_compile_options(clucene-contribs-lib PRIVATE ${clucene_options})
target_compile_options(ic PRIVATE ${clucene_options})
# CLucene's FindRoaring.cmake does not add include dirs to targets, so we do it here.
target_include_directories(clucene-core-static PRIVATE "${Roaring_INCLUDE_DIR}")
target_include_directories(ic PRIVATE "${Roaring_INCLUDE_DIR}")

install(DIRECTORY
    ${CONTRIB_PATH}/clucene/src/contribs-lib/CLucene/analysis/jieba/dict
    DESTINATION ${OUTPUT_DIR})

install(DIRECTORY
    ${BASE_DIR}/dict/icu/uax29
    DESTINATION ${OUTPUT_DIR}/dict/icu)

install(DIRECTORY
    ${BASE_DIR}/dict/ik
    DESTINATION ${OUTPUT_DIR}/dict)

install(DIRECTORY
    ${BASE_DIR}/dict/pinyin
    DESTINATION ${OUTPUT_DIR}/dict)

# Check if functions are supported in this platform. All flags will generated
# in gensrc/build/common/env_config.h.
# You can check funcion here which depends on platform. Don't forget add this
# to be/src/common/env_config.h.in
include(CheckFunctionExists)
check_function_exists(sched_getcpu HAVE_SCHED_GETCPU)

function(TRY_TO_CHANGE_LINKER LINKER_COMMAND LINKER_NAME)
    if (CUSTUM_LINKER_COMMAND STREQUAL "ld")
        execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=${LINKER_COMMAND} -Wl,--version ERROR_QUIET OUTPUT_VARIABLE LD_VERSION)
        if ("${LD_VERSION}" MATCHES ${LINKER_NAME})
            message(STATUS "Linker ${LINKER_NAME} is available, change linker to ${LINKER_NAME}")
            set(CUSTUM_LINKER_COMMAND "${LINKER_COMMAND}" PARENT_SCOPE)
        endif()
    endif()
endfunction()

if (NOT OS_MACOSX) # MACOSX's lld will core dump
    # In terms of performance, mold> lld> gold> ld
    set(CUSTUM_LINKER_COMMAND "ld")
    TRY_TO_CHANGE_LINKER("mold" "mold")
    TRY_TO_CHANGE_LINKER("lld" "LLD")
    TRY_TO_CHANGE_LINKER("gold" "GNU gold")
    if (NOT CUSTUM_LINKER_COMMAND STREQUAL "ld")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${CUSTUM_LINKER_COMMAND}")
    endif()
endif()


set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 17)

add_compile_options(
  "-ffile-prefix-map=$ENV{DORIS_HOME}=."
)

add_compile_options(-g
                    -gdwarf-5
                    -Wall
                    -Wextra
                    -Werror
                    -Wundef
                    -pthread
                    -fstrict-aliasing
                    -fno-omit-frame-pointer
                    $<$<COMPILE_LANGUAGE:CXX>:-Wnon-virtual-dtor>)

add_compile_options(-Wno-unused-parameter
                    -Wno-sign-compare
                    -Wno-deprecated-declarations)

if (COMPILER_GCC)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.1")
        message(FATAL_ERROR "Need GCC version at least 11.1")
    endif()

    add_compile_options(-fdiagnostics-color=always
                        -Wno-nonnull
                        -Wno-stringop-overread
                        -Wno-stringop-overflow
                        -Wno-array-bounds
                        -Wno-maybe-uninitialized)
endif ()

if (COMPILER_CLANG)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "16")
        message(FATAL_ERROR "Need Clang version at least 16")
    endif()

    add_compile_options(-fcolor-diagnostics
                        -Wpedantic
                        -Wshadow
                        -Wshadow-field
                        -Wunused
                        -Wunused-command-line-argument
                        -Wunused-exception-parameter
                        -Wunused-volatile-lvalue
                        -Wunused-template
                        -Wunused-member-function
                        -Wunused-macros
                        -Wconversion
                        -Wthread-safety)
    add_compile_options(-Wno-gnu-statement-expression
                        -Wno-implicit-float-conversion
                        -Wno-implicit-int-conversion
                        -Wno-shorten-64-to-32
                        -Wno-sign-conversion)
    # -Wno-unnecessary-virtual-specifier is a clang 22+ flag, silenced via
    # -Wno-unknown-warning-option so older toolchains (CI ldb-toolchain-v0.26
    # ships clang 20) don't fail with -Werror,-Wunknown-warning-option.
    add_compile_options(-Wno-unknown-warning-option -Wno-unnecessary-virtual-specifier)
    if (USE_LIBCPP)
        # MSAN uses -nostdinc++ with instrumented libc++, not -stdlib=libc++
        if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "MSAN" OR NOT MSAN_LIBCXX_LIBS)
            add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-stdlib=libc++>)
        endif()
        add_definitions(-DUSE_LIBCPP)
    endif()
endif ()

add_definitions(-D__STDC_FORMAT_MACROS
                    -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                    -DBOOST_SYSTEM_NO_DEPRECATED
                    -DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX=1
                    -DBRPC_ENABLE_CPU_PROFILER
                    -DS2_USE_GFLAGS
                    -DS2_USE_GLOG
                    -DAWS_USE_EPOLL
                    -DAWS_SDK_VERSION_MAJOR=1
                    -DAWS_SDK_VERSION_MINOR=11
                    -DAWS_SDK_VERSION_PATCH=219
                    -DAZ_RTTI
                    -DBASE64_STATIC_DEFINE
                    -DBENCHMARK_STATIC_DEFINE
                    -DBROTLI_SHARED_COMPILATION
                    -DCARES_STATICLIB
                    -DCURL_STATICLIB
                    -DGFLAGS_IS_A_DLL=0
                    -DJSON_DIAGNOSTICS=0
                    -DJSON_USE_IMPLICIT_CONVERSIONS=1
                    -DLIBRDKAFKA_STATICLIB
                    -DSIMDJSON_THREADS_ENABLED=1
                    -DWITH_STATIC_LIB_libcrypto)

# Thrift requires these two definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H)

# Mac specific definitions - needed for system headers and third-party libs
if (OS_MACOSX)
    add_definitions(-D_DARWIN_C_SOURCE)
endif()

if (RECORD_COMPILER_SWITCHES)
    add_compile_options(-frecord-gcc-switches)
endif()

# simd for architectures
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    add_compile_options(-msse4.2)
    add_definitions(-DLIBDIVIDE_SSE2)
    if (USE_AVX2)
        add_compile_options(-mavx2)
        add_definitions(-DUSE_AVX2)
        add_definitions(-DLIBDIVIDE_AVX2)
    endif()
endif()

if (ARCH_ARM)
    add_compile_options(-march=${ARM_MARCH})
    message(STATUS "ARM_MARCH is ${ARM_MARCH}")
    add_definitions(-DLIBDIVIDE_NEON)
endif()
SET(ARM_MARCH "${ARM_MARCH}")

if (ENABLE_INJECTION_POINT)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DENABLE_INJECTION_POINT")
endif()

# Compile with jemalloc.
# Adding the option `USE_JEMALLOC=ON sh build.sh` when compiling can turn on building with jemalloc
if (USE_JEMALLOC)
    add_definitions(-DUSE_JEMALLOC)
endif()

# Compile with libunwind
if (USE_UNWIND)
    add_definitions(-DUSE_UNWIND)
    if (COMPILER_CLANG)
        add_compile_options(-gdwarf-aranges)
    endif()
endif()

# For CMAKE_BUILD_TYPE=Debug
if (OS_MACOSX AND ARCH_ARM)
    # Using -O0 may meet ARM64 branch out of range errors when linking with tcmalloc.
    set(CXX_FLAGS_DEBUG "-Og")
else()
    set(CXX_FLAGS_DEBUG "-O0")
endif()

# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
set(CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
message(STATUS "UBSAN_IGNORELIST is ${UBSAN_IGNORELIST}")
set(CXX_FLAGS_ASAN "-O0 -fsanitize=address -fsanitize=undefined -fno-sanitize=float-cast-overflow -DUNDEFINED_BEHAVIOR_SANITIZER -DADDRESS_SANITIZER")
if (COMPILER_CLANG)
    set(CXX_FLAGS_ASAN "${CXX_FLAGS_ASAN} -fsanitize-ignorelist=${UBSAN_IGNORELIST}")
endif()
set(CXX_FLAGS_LSAN "-O0 -fsanitize=leak -DLEAK_SANITIZER")
## Use for BE-UT
set(CXX_FLAGS_ASAN_UT "-O0 -fsanitize=address -DADDRESS_SANITIZER")

# Set the flags to the undefined behavior sanitizer, also known as "ubsan"
# Turn on sanitizer and debug symbols to get stack traces:
set(CXX_FLAGS_UBSAN "-O0 -fno-wrapv -mcmodel=medium -fsanitize=undefined -DUNDEFINED_BEHAVIOR_SANITIZER")

# Set the flags to the thread sanitizer, also known as "tsan"
# Turn on sanitizer and debug symbols to get stack traces:
# Use -Wno-builtin-declaration-mismatch to mute warnings like "new declaration ‘__tsan_atomic16 __tsan_atomic16_fetch_nand(..."
# If use -O0 to compile, BE will stack overflow when start. https://github.com/apache/doris/issues/8868
set(CXX_FLAGS_TSAN "-O1 -fsanitize=thread -DTHREAD_SANITIZER -Wno-missing-declarations")

# Set the flags to the memory sanitizer, also known as "msan"
# MSAN requires all code (including C++ stdlib) to be instrumented,
# so it forces USE_LIBCPP=ON with -stdlib=libc++.
# -stdlib=libc++ is in CMAKE_CXX_FLAGS (not add_compile_options) so it propagates
# to all subdirectories (thirdparty, contrib/clucene, contrib/faiss, etc.)
# ldb_toolchain's clang doesn't auto-discover libc++ headers with -stdlib=libc++,
# so we must add the include path explicitly.
execute_process(COMMAND ${CMAKE_CXX_COMPILER} --print-resource-dir OUTPUT_VARIABLE _CLANG_RESOURCE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
get_filename_component(_LIBCPP_INCLUDE "${_CLANG_RESOURCE_DIR}/../../../include/c++/v1" ABSOLUTE)
# MSAN uses instrumented libc++ built from source (see thirdparty/build_msan_libcxx/).
# -nostdlib++ prevents linking the toolchain's uninstrumented libc++.so;
# the instrumented libc++.a is linked explicitly via MSAN_LIBCXX_LIBS.
set(_MSAN_LIBCXX "$ENV{DORIS_THIRDPARTY}/build_msan_libcxx")
if (EXISTS "${_MSAN_LIBCXX}/lib/libc++.a")
    set(CXX_FLAGS_MSAN "-O1 -nostdinc++ -isystem ${_MSAN_LIBCXX}/include/c++/v1 -fsanitize=memory -fsanitize-recover=memory -fsanitize-memory-track-origins=2 -fsanitize-memory-use-after-dtor -fsanitize-ignorelist=$ENV{DORIS_HOME}/conf/msan_ignorelist.txt -fno-omit-frame-pointer -DMEMORY_SANITIZER")
else()
    set(CXX_FLAGS_MSAN "-O1 -stdlib=libc++ -isystem ${_LIBCPP_INCLUDE} -fsanitize=memory -fsanitize-recover=memory -fsanitize-memory-track-origins=2 -fsanitize-memory-use-after-dtor -fno-omit-frame-pointer -DMEMORY_SANITIZER")
endif()

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN}")
    # https://github.com/abseil/abseil-cpp/commit/68ce303da1920522a27e5d8e2732b3e50d3aae57
    # for disable some asan check of absl
    add_definitions(-DNDEBUG_SANITIZER)
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_LSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_UBSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_TSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_MSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN_UT")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN_UT}")
    add_definitions(-DNDEBUG_SANITIZER)
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Add flags that are common across build types
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS} ${EXTRA_CXX_FLAGS}")

# Re-inject libc++ flags after build-type override (which discards earlier settings).
# This ensures contrib/faiss, contrib/clucene etc. also get libc++ headers.
if (USE_LIBCPP AND DEFINED _LIBCPP_INC AND NOT MSAN_LIBCXX_LIBS)
    string(FIND "${CMAKE_CXX_FLAGS}" "-stdlib=libc++" _HAS_LIBCPP)
    if(_HAS_LIBCPP EQUAL -1)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -isystem ${_LIBCPP_INC}")
    endif()
endif()

# Copy CXX flags to C, but strip C++-only flags (-stdlib=libc++)
string(REPLACE "-stdlib=libc++" "" CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Set include dirs
include_directories(
    ${CONTRIB_PATH}/apache-orc/c++/include
    ${CMAKE_CURRENT_BINARY_DIR}/apache-orc/c++/include
)

include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}/clucene/src/shared
    ${CONTRIB_PATH}/clucene/src/core
    ${CONTRIB_PATH}/clucene/src/shared
    ${CONTRIB_PATH}/clucene/src/contribs-lib
    ${Roaring_INCLUDE_DIR}
)

# Third-party source include directories
# These ensure PCH and all BE source files can find thirdparty headers
# when building from source (replaces the old thirdparty/installed/include).
# IMPORTANT: Use SYSTEM to avoid shadowing openblas config.h with glog config.h.
set(TP_SRC_DIR "${CMAKE_SOURCE_DIR}/../thirdparty/src")
set(TP_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/thirdparty")
link_directories(${CMAKE_SOURCE_DIR}/../thirdparty/installed/lib)
link_directories(${CMAKE_SOURCE_DIR}/../thirdparty/installed/lib64)
include_directories(SYSTEM
    # thrift
    ${TP_SRC_DIR}/thrift-0.16.0/lib/cpp/src
    ${TP_BUILD_DIR}/thrift_gen
    # protobuf
    ${TP_SRC_DIR}/protobuf-21.11/src
    # arrow
    ${CMAKE_SOURCE_DIR}/../contrib/apache-arrow/cpp/src
    ${CMAKE_CURRENT_BINARY_DIR}/arrow/src
    # fmt
    ${TP_SRC_DIR}/fmt-8.1.1/include
    # glog
    ${TP_SRC_DIR}/glog-0.6.0/src
    ${TP_BUILD_DIR}/glog
    # boost
    ${TP_SRC_DIR}/boost_1_81_0
    # brpc/bvar
    ${TP_SRC_DIR}/brpc-1.4.0/src
    ${TP_BUILD_DIR}/brpc
    # zstd
    ${TP_SRC_DIR}/zstd-1.5.7/lib
    # lz4
    ${TP_SRC_DIR}/lz4-1.9.4/lib
    # sasl
    ${TP_SRC_DIR}/cyrus-sasl-2.1.27/include
    # streamvbyte
    ${TP_SRC_DIR}/streamvbyte-1.0.0/include
    # aws sdk — SDKConfig.h is configure_file'd into the build tree by
    # thirdparty/cmake/aws_sdk.cmake; source dir only ships SDKConfig.h.in
    # in a fresh tarball, so we need both src and gen include paths.
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-core/include
    ${TP_BUILD_DIR}/aws-sdk-gen/aws-cpp-sdk-core/include
    # backtrace
    ${TP_SRC_DIR}/libbacktrace-2446c66076480ce07a6bd868badcbceb3eeecc2e
    ${TP_BUILD_DIR}/backtrace
    # cctz
    ${TP_SRC_DIR}/cctz-2.5/include
    # crc32c
    ${TP_SRC_DIR}/crc32c-1.1.2/include
    # simdjson
    ${TP_SRC_DIR}/simdjson-3.11.6/include
    # leveldb
    ${TP_SRC_DIR}/leveldb-1.23/include
    # base64
    ${TP_SRC_DIR}/base64-0.5.2/include
    # gflags (generated headers in build dir)
    ${TP_BUILD_DIR}/gflags/include
    # re2
    ${TP_SRC_DIR}/re2-2021-02-02
    # snappy (build generates snappy-stubs-public.h)
    ${TP_SRC_DIR}/snappy-1.1.10
    ${TP_BUILD_DIR}/snappy
    # s2geometry
    ${TP_SRC_DIR}/s2geometry-0.10.0/src
    # bitshuffle
    ${TP_SRC_DIR}/bitshuffle-0.5.1/src
    # odbc
    ${TP_SRC_DIR}/unixODBC-2.3.7/include
    # gperftools
    ${TP_BUILD_DIR}/gperftools/include
    # xxHash
    ${TP_SRC_DIR}/xxHash-0.8.3
    # parallel_hashmap
    ${TP_SRC_DIR}/parallel-hashmap-1.3.8
    # concurrentqueue
    ${TP_SRC_DIR}/concurrentqueue-1.0.3
    # libdivide
    ${TP_SRC_DIR}/libdivide-5.2.0
    # rapidjson
    ${TP_SRC_DIR}/rapidjson-232389d4f1012dddec4ef84861face2d2ba85709/include
    # pdqsort (header directly in src/ root)
    ${TP_SRC_DIR}
    # rocksdb
    ${TP_SRC_DIR}/rocksdb-5.14.2/include
    # aws-crt-cpp
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/include
    # Azure SDK headers
    ${TP_SRC_DIR}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/core/azure-core/inc
    ${TP_SRC_DIR}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/identity/azure-identity/inc
    ${TP_SRC_DIR}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/storage/azure-storage-blobs/inc
    ${TP_SRC_DIR}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/storage/azure-storage-common/inc
    # In prebuilt mode (USE_CONTRIB_SOURCE=OFF), thirdparty/installed/include is the
    # canonical lookup path for all libs. In source mode it is intentionally NOT
    # added — every source-mode target propagates its own include via
    # INTERFACE_INCLUDE_DIRECTORIES, and adding installed/include would conflict
    # with source-mode Arrow / Protobuf / etc. headers if a prebuilt tarball is
    # also extracted there for testing.
    $<$<NOT:$<BOOL:${USE_CONTRIB_SOURCE}>>:${CMAKE_SOURCE_DIR}/../thirdparty/installed/include>
    # CRoaring (needs SYSTEM to suppress -Wundef/-Wshorten-64-to-32 from roaring headers)
    ${TP_SRC_DIR}/CRoaring-2.1.2/include
    # Nested include paths for libraries that use subdir-prefixed includes
    # (e.g. #include <lz4/lz4.h>, #include <snappy/snappy.h>)
    ${TP_BUILD_DIR}/lz4_headers/include
    ${TP_BUILD_DIR}/snappy_headers/include
    ${TP_BUILD_DIR}/mysql_headers/include
    ${TP_BUILD_DIR}/rdkafka_headers/include
    # jemalloc — generated jemalloc.h lives in the build tree; src/include
    # only has .h.in templates after a fresh tarball extract.
    ${TP_BUILD_DIR}/jemalloc_config
    ${TP_SRC_DIR}/jemalloc-5.3.0/include
    # krb5 headers (source + build-generated krb5/krb5.h)
    ${TP_BUILD_DIR}/krb5_headers/include
    ${TP_BUILD_DIR}/krb5/include
    # libunwind headers
    ${TP_SRC_DIR}/libunwind-1.6.2/include
    # abseil-cpp (for absl/strings, etc.)
    ${TP_SRC_DIR}/abseil-cpp-20250512.1
    # arrow source headers (thirdparty/src path; contrib/apache-arrow may not exist)
    ${TP_SRC_DIR}/arrow-apache-arrow-17.0.0/cpp/src
    ${TP_BUILD_DIR}/arrow/src
    # fast_float (for fast_float/fast_float.h)
    ${TP_SRC_DIR}/fast_float-3.9.0/include
    # aws-cpp-sdk-s3 generated headers (for aws/s3/S3Errors.h)
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-s3/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-s3-crt/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-transfer/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-identity-management/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-sts/include
    # aws-cpp-sdk-kinesis (routine_load Kinesis consumer, upstream #61325)
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-kinesis/include
    # aws-crt-cpp sub-library headers (needed transitively by aws/crt/Allocator.h, Types.h, etc.)
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-common/include
    # aws-c-common config.h is generated by configure_file() in aws_sdk.cmake
    # under ${CMAKE_BINARY_DIR}/thirdparty/aws-sdk-gen/aws-c-common/generated/include.
    # Without this path BE compiles intermittently fail on `aws/common/config.h
    # file not found` when ninja schedules a Core .cpp.o before transitive
    # INTERFACE_INCLUDE_DIRECTORIES from aws-cpp-sdk-core kicks in.
    ${TP_BUILD_DIR}/aws-sdk-gen/aws-c-common/generated/include
    ${TP_BUILD_DIR}/aws-sdk-gen/aws-crt-cpp/generated/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-io/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-cal/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-auth/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-http/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-s3/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-sdkutils/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-checksums/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-event-stream/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-compression/include
    ${TP_SRC_DIR}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-mqtt/include
    # libevent generated config (for event2/event-config.h)
    ${TP_BUILD_DIR}/libevent/include
    # hadoop hdfs
    ${TP_BUILD_DIR}/hadoop_hdfs_headers/include
    # ICU (for unicode/*.h)
    ${TP_SRC_DIR}/icu-release-69-1/icu4c/source/common
    ${TP_SRC_DIR}/icu-release-69-1/icu4c/source/i18n
    ${TP_SRC_DIR}/icu-release-69-1/icu4c/source/io
    # curl
    ${TP_SRC_DIR}/curl-8.2.1/include
    # ODBC (for sqltypes.h)
    ${TP_SRC_DIR}/unixODBC-2.3.7/include
    # ODBC generated config (unixodbc_conf.h written by odbc.cmake; in a
    # fresh tarball the src/ dir only has .h.in templates)
    ${TP_BUILD_DIR}/odbc_config
    # libevent (for event2/*.h)
    ${TP_SRC_DIR}/doris-thirdparty-libevent-2.1.12.1/include
    # pugixml
    ${TP_SRC_DIR}/pugixml-1.15/src
    # bzip2
    ${TP_SRC_DIR}/bzip2-1.0.8
    # paimon-cpp
    ${TP_SRC_DIR}/doris-thirdparty-paimon-cpp-0a4f4e2/include
    # nlohmann json
    ${TP_SRC_DIR}/json-3.10.1/single_include
    # minizip (zlib contrib)
    ${TP_SRC_DIR}/zlib-1.3.1/contrib
    # ---- BATCH: all remaining third-party source include paths ----
    # libdeflate
    ${TP_SRC_DIR}/libdeflate-1.19
    # simdjson
    ${TP_SRC_DIR}/simdjson-3.11.6/singleheader
    # xxHash
    ${TP_SRC_DIR}/xxHash-0.8.3
    # zstd
    ${TP_SRC_DIR}/zstd-1.5.7/lib
    # concurrentqueue
    ${TP_SRC_DIR}/concurrentqueue-1.0.3
    # streamvbyte
    ${TP_SRC_DIR}/streamvbyte-1.0.0/include
    # CRoaring
    ${TP_SRC_DIR}/CRoaring-2.1.2/include
    # re2
    ${TP_SRC_DIR}/re2-2021-02-02
    # flatbuffers
    ${TP_SRC_DIR}/flatbuffers-2.0.0/include
    # fmt
    ${TP_SRC_DIR}/fmt-8.1.1/include
    # parallel-hashmap
    ${TP_SRC_DIR}/parallel-hashmap-1.3.8
    # rapidjson
    ${TP_SRC_DIR}/rapidjson-232389d4f1012dddec4ef84861face2d2ba85709/include
    # orc
    ${TP_SRC_DIR}/orc-1.9.0/c++/include
    # leveldb
    ${TP_SRC_DIR}/leveldb-1.23/include
    # rocksdb
    ${TP_SRC_DIR}/rocksdb-5.14.2/include
    # boost
    ${TP_SRC_DIR}/boost_1_81_0
    # openssl
    ${TP_SRC_DIR}/openssl-OpenSSL_1_1_1s/include
    # libxml2
    ${TP_SRC_DIR}/libxml2-v2.9.10/include
    # lzo
    ${TP_SRC_DIR}/lzo-2.10/include
    # libdivide
    ${TP_SRC_DIR}/libdivide-5.2.0
    # zlib (also for zconf.h)
    ${TP_SRC_DIR}/zlib-1.3.1
    # brotli
    ${TP_SRC_DIR}/brotli-1.0.9/c/include
    # timsort
    ${TP_BUILD_DIR}/timsort_headers/include
    # bitshuffle
    ${TP_BUILD_DIR}/bitshuffle_headers/include
    # hyperscan
    ${TP_BUILD_DIR}/hyperscan_headers/include
    # gtest (for gtest_prod.h used in some source files)
    ${TP_SRC_DIR}/googletest-release-1.12.1/googletest/include
)

include_directories(
    ${SRC_DIR}/
)

include_directories(
    SYSTEM
    ${COMMON_SRC_DIR}
    ${GENSRC_DIR}/
    ${GPERFTOOLS_HOME}/include
)

if ("${DORIS_JAVA_HOME}" STREQUAL "")
    set(DORIS_JAVA_HOME "$ENV{JAVA_HOME}")
endif()

execute_process(COMMAND chmod 755 ${BASE_DIR}/../tools/find_libjvm.sh)
execute_process(COMMAND ${BASE_DIR}/../tools/find_libjvm.sh OUTPUT_VARIABLE LIBJVM_PATH OUTPUT_STRIP_TRAILING_WHITESPACE)
FILE(GLOB_RECURSE LIB_JVM ${LIBJVM_PATH})
if("${LIB_JVM}" STREQUAL "")
    message(STATUS "there is no libjvm found!")
else()
    set(DORIS_DEPENDENCIES
        ${DORIS_DEPENDENCIES}
        jvm
    )
    add_library(jvm SHARED IMPORTED)
    set_target_properties(jvm PROPERTIES IMPORTED_LOCATION ${LIB_JVM})
    include_directories(${DORIS_JAVA_HOME}/include)
    if (NOT OS_MACOSX)
        include_directories(${DORIS_JAVA_HOME}/include/linux)
    else()
        include_directories(${DORIS_JAVA_HOME}/include/darwin)
    endif()
    add_definitions("-DLIBJVM")
endif()

if (NOT OS_MACOSX)
    set(WL_START_GROUP "-Wl,--start-group")
    set(WL_END_GROUP "-Wl,--end-group")
endif()

set(KRB5_LIBS
    krb5support
    krb5
    com_err
    gssapi_krb5
    k5crypto)

# Set Doris libraries
set(DORIS_LINK_LIBS
    ${WL_START_GROUP}
    Agent
    Common
    Core
    Exec
    Exprs
    Format
    IO
    Storage
    Runtime
    Service
    Udf
    Util
    DorisGen
    Load
    InformationSchema
    Cloud
    ${WL_END_GROUP}
    CommonCPP
)

# absl is built from source via add_subdirectory(thirdparty)
set(absl_FOUND TRUE)

# COMMON_THIRDPARTY comes from thirdparty/CMakeLists.txt via PARENT_SCOPE.
# Prepend Boost targets and add conditional libs here.
set(COMMON_THIRDPARTY
    Boost::date_time
    Boost::container
    ${COMMON_THIRDPARTY}
    ${GPERFTOOLS_LIBS}
    ${AZURE_THIRDPARTY}
    ${PAIMON_THIRDPARTY}
)

if (ENABLE_PAIMON_CPP)
    message(STATUS "Paimon C++ enabled: legacy thirdparty static linkage mode")
    add_definitions(-DENABLE_PAIMON_CPP)
endif()

if ((ARCH_AMD64 OR ARCH_AARCH64) AND OS_LINUX)
    # hadoop_hdfs target is created by thirdparty/cmake/hadoop_hdfs.cmake
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        hadoop_hdfs
    )
    add_definitions(-DUSE_HADOOP_HDFS)
    add_definitions(-DUSE_DORIS_HADOOP_HDFS)
else()
    # TODO: use arm hadoop hdfs to replace this
    add_definitions(-DUSE_LIBHDFS3)
endif()

if (absl_FOUND)
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        absl::cord
        absl::cord_internal
        absl::cordz_functions
        absl::cordz_info
        absl::cordz_update_scope
        absl::cordz_update_tracker
        absl::crc_cord_state
        absl::flags
        absl::random_random
        absl::spinlock_wait
        absl::status
        absl::statusor
        absl::strings
    )
endif()

if (OS_MACOSX)
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        bfd
        iberty
        intl
    )
endif()

if (BUILD_BENCHMARK)
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        benchmark
    )
endif()

set(PAIMON_FACTORY_REGISTRY_LIBS)
if (ENABLE_PAIMON_CPP)
    # Plan B: Doris Arrow is now built with COMPUTE/DATASET/ACERO/FILESYSTEM,
    # so arrow, arrow_dataset, arrow_acero are all in COMMON_THIRDPARTY via
    # thirdparty.cmake.  paimon-cpp reuses the same Arrow (no paimon_deps).
    # No dual-stack selection needed — single Arrow for everything.

    # paimon_parquet_file_format depends on Arrow Dataset symbols.
    # Force-link it with --whole-archive so its factory registration runs.
    set(PAIMON_FACTORY_REGISTRY_LIBS paimon_parquet_file_format)
    list(REMOVE_ITEM COMMON_THIRDPARTY ${PAIMON_FACTORY_REGISTRY_LIBS})
endif()

# Help linker resolve bare -lcurl, -lcares, -ldeflate injected by AWS SDK CMake
link_directories(
    ${CMAKE_BINARY_DIR}/lib
    ${CMAKE_BINARY_DIR}/thirdparty/cares/lib64
    ${CMAKE_BINARY_DIR}/thirdparty/libdeflate
)

set(DORIS_DEPENDENCIES
    ${DORIS_DEPENDENCIES}
    ${WL_START_GROUP}
    ${COMMON_THIRDPARTY}
    ${KRB5_LIBS}
)

if (USE_UNWIND)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} libunwind)
endif()

set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} orc)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} krb5_headers)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} ic)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} clucene-core-static)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} clucene-shared-static)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} clucene-contribs-lib)

if (ENABLE_PAIMON_CPP)
    if (PAIMON_FACTORY_REGISTRY_LIBS)
        if (APPLE)
            foreach(lib ${PAIMON_FACTORY_REGISTRY_LIBS})
                set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES}
                    -Wl,-force_load,$<TARGET_FILE:${lib}>)
            endforeach()
        else()
            set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES}
                -Wl,--whole-archive
                ${PAIMON_FACTORY_REGISTRY_LIBS}
                -Wl,--no-whole-archive)
        endif()
    endif()

    # paimon-cpp internal dependencies (renamed with _paimon suffix)
    # These must come after paimon libraries to resolve symbols.
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} roaring_bitmap_paimon)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} xxhash_paimon)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} fmt_paimon)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} tbb_paimon)
endif()

if (BUILD_RUST_READERS)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} doris_ffi_lib)
endif()

set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} ${WL_END_GROUP})

# Add all external dependencies. They should come after the palo libs.
# static link gcc's lib
if (NOT OS_MACOSX)
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
        ${DORIS_DEPENDENCIES}
        -lresolv
    )
    if (USE_LIBCPP AND COMPILER_CLANG AND NOT MSAN_LIBCXX_LIBS)
        set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -stdlib=libc++ -lc++abi)
    else()
        set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -static-libstdc++ -static-libgcc -lstdc++fs)
    endif()
else()
    set(DORIS_LINK_LIBS
        ${DORIS_LINK_LIBS}
        ${DORIS_DEPENDENCIES}
        -lapple_nghttp2
        -lresolv
        -liconv
    )
    # On macOS arm64, large binaries (>2GB) can cause the virtual address space
    # to overlap with the dyld shared cache region, resulting in "dyld cache not
    # loaded: syscall to map cache into shared region failed". Use -dead_strip to
    # remove unreachable code/data and reduce binary size.
    add_link_options(-Wl,-dead_strip)
endif()


if (USE_JEMALLOC)
    set(MALLOCLIB jemalloc)
else ()
    set(MALLOCLIB tcmalloc)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(ASAN_LIBS -static-libasan)
    set(LSAN_LIBS -static-liblsan)
    set(UBSAN_LIBS -static-libubsan ${MALLOCLIB})
    set(TSAN_LIBS -static-libtsan)
else ()
    # clang sanitizers: -fsanitize=undefined auto-links UBSAN runtime, no
    # special *_LIBS needed (same as ASAN/TSAN/LSAN clang paths which leave
    # ASAN_LIBS/TSAN_LIBS/LSAN_LIBS empty and rely on -fsanitize=address etc).
    # The old `-rtlib=compiler-rt ${MALLOCLIB}` here dropped libgcc_s and
    # broke _Unwind_* resolution (doris's source-built libunwind.a only
    # provides unw_*, not the _Unwind_* ABI).
    set(UBSAN_LIBS)
endif ()

# Add sanitize static link flags
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${MALLOCLIB})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN" OR "${CMAKE_BUILD_TYPE}" STREQUAL "ASAN_UT")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${ASAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${LSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${UBSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${TSAN_LIBS})
    add_definitions("-DTHREAD_SANITIZER")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    # MSAN: provide stub HeapProfiler* symbols (real profiling not available under MSAN)
    file(WRITE "${CMAKE_BINARY_DIR}/msan_stubs.cpp" [[
extern "C" {
void HeapProfilerStart(const char*) {}
void HeapProfilerStop() {}
char* GetHeapProfile() { return const_cast<char*>(""); }

// Tell MSAN runtime to not abort on errors during static initialization.
// MSAN_OPTIONS env var is too late — it's read after __libc_csu_init where
// protobuf descriptor registration (from uninstrumented thirdparty code)
// triggers false positives. This callback is called by MSAN runtime itself
// during its own initialization, before any static constructors run.
__attribute__((used, retain, visibility("default")))
const char* __msan_default_options() {
    return "halt_on_error=0:abort_on_error=0:poison_in_dtor=1:exit_code=0:fast_unwind_on_fatal=1:fast_unwind_on_malloc=1";
}
}
]])
    add_library(msan_stubs STATIC "${CMAKE_BINARY_DIR}/msan_stubs.cpp")
    # Use --whole-archive to force msan_stubs symbols to override compiler-rt weak defaults
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -Wl,--whole-archive msan_stubs -Wl,--no-whole-archive pprof)
    # Link instrumented libc++ statically; -nostdlib++ prevents pulling in the
    # toolchain's uninstrumented libc++.so which conflicts with the static one.
    if (MSAN_LIBCXX_LIBS)
        set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${MSAN_LIBCXX_LIBS})
        add_link_options(-nostdlib++)
    endif()
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# NOTE(amos): This should come before -lc -lm to interpose symbols correctly.
if (GLIBC_COMPATIBILITY)
    add_subdirectory(${SRC_DIR}/glibc-compatibility)
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} glibc-compatibility-explicit glibc-compatibility)
endif()

if (NOT OS_MACOSX)
    set(CMAKE_CXX_STANDARD_LIBRARIES "-lrt -l:libbfd.a -liberty -lc -lm -ldl -pthread")
    set(CMAKE_C_STANDARD_LIBRARIES "-lrt -l:libbfd.a -liberty -lc -lm -ldl -pthread")
else()
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreText"
        "-framework Foundation"
        "-framework SystemConfiguration"
        "-framework Security"
    )
    if (USE_JEMALLOC OR (NOT CMAKE_BUILD_TYPE STREQUAL "DEBUG" AND NOT CMAKE_BUILD_TYPE STREQUAL "RELEASE"))
        set(DORIS_LINK_LIBS
            ${DORIS_LINK_LIBS}
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        )
    endif()
endif()

# Set libraries for test
set (TEST_LINK_LIBS ${DORIS_LINK_LIBS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

# Only build static libs
set(BUILD_SHARED_LIBS OFF)

option(ENABLE_CLANG_COVERAGE "coverage option" OFF)
if (ENABLE_CLANG_COVERAGE AND ENABLE_CLANG_COVERAGE STREQUAL ON AND COMPILER_CLANG)
    add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate)
    # ASAN + coverage produces a multi-GB binary. The clang driver auto-appends
    # libclang_rt.profile-x86_64.a at the *end* of the link line, which places
    # its .text past the 2 GiB PC32 boundary from __ehdr_start (provided by
    # lld at offset 0). InstrProfilingPlatformLinux.c.o references __ehdr_start
    # via R_X86_64_PC32 in __llvm_write_binary_ids and overflows.
    #
    # Force the profile runtime to be linked at the *start* of the link line
    # so its .text lands in the first 2 GiB. CMake EXE_LINKER_FLAGS are
    # emitted before the per-target objects, so prepending the archive there
    # gets us the right ordering. --no-as-needed forces the archive members
    # to be retained even though no symbols are referenced at that point.
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN" OR "${CMAKE_BUILD_TYPE}" STREQUAL "ASAN_UT")
        # clang -print-runtime-dir returns the per-triple path which doesn't
        # always exist (older Gentoo-style packaging keeps libs under lib/linux/).
        # Try both layouts.
        execute_process(
            COMMAND ${CMAKE_CXX_COMPILER} -print-resource-dir
            OUTPUT_VARIABLE _CLANG_RES_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        set(_CLANG_PROFILE_LIB "")
        foreach(_layout linux x86_64-pc-linux-gnu)
            set(_candidate "${_CLANG_RES_DIR}/lib/${_layout}/libclang_rt.profile-x86_64.a")
            if (EXISTS "${_candidate}")
                set(_CLANG_PROFILE_LIB "${_candidate}")
                break()
            endif()
        endforeach()
        if (_CLANG_PROFILE_LIB)
            message(STATUS "Prepending profile runtime for ASAN+coverage: ${_CLANG_PROFILE_LIB}")
            set(CMAKE_EXE_LINKER_FLAGS
                "-Wl,--whole-archive,${_CLANG_PROFILE_LIB},--no-whole-archive ${CMAKE_EXE_LINKER_FLAGS}")
        else()
            message(WARNING "ASAN+coverage: libclang_rt.profile-x86_64.a not found under ${_CLANG_RES_DIR}; link may fail with PC32 overflow")
        endif()
    endif ()
endif ()

if (MAKE_TEST)
    add_compile_options(-DGTEST_USE_OWN_TR1_TUPLE=0)
    # Only add GCC-style coverage when using GCC compiler
    # to avoid duplicate symbol errors (e.g., __gcov_fork, __gcov_reset)
    # between libgcov.a and libclang_rt.profile-x86_64.a
    if (COMPILER_GCC)
        add_compile_options(-fprofile-arcs -ftest-coverage)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
        if (NOT OS_MACOSX)
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
        endif()
    endif()
    add_definitions(-DBE_TEST)
    if (ARCH_ARM)
        add_compile_options(-ffp-contract=off)
    endif()
    if (COMPILER_CLANG)
        add_compile_options(
        -Wno-implicit-int-conversion
        -Wno-shorten-64-to-32
        )
    endif()
endif ()

# use this to avoid some runtime tracker. reuse BE_TEST symbol, no need another.
if (BUILD_BENCHMARK)
    add_definitions(-DBE_TEST)
# The separate BENCHMARK marker is introduced here because
# some BE UTs mock certain functions, and BENCHMARK cannot find their definitions.
    add_definitions(-DBE_BENCHMARK)
endif()

get_directory_property(COMPILER_FLAGS COMPILE_OPTIONS)
get_directory_property(COMPILER_DEFINES COMPILE_DEFINITIONS)
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "CXX Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "C Standard: ${CMAKE_C_STANDARD}")
message(STATUS "CXX Flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "C Flags: ${CMAKE_C_FLAGS}")
message(STATUS "CC Flags: ${CMAKE_CC_FLAGS}")
message(STATUS "Compiler Options: ${COMPILER_FLAGS}")
message(STATUS "Compiler Definitions: ${COMPILER_DEFINES}")
message(STATUS "Doris Dependencies: ${DORIS_DEPENDENCIES}")
if (NOT MAKE_TEST)
    message(STATUS "Link Flags: ${DORIS_LINK_LIBS}")
else()
    message(STATUS "Link Flags: ${TEST_LINK_LIBS}")
endif()

# Only apply the OpenMP-related adjustments when PCH is enabled, to avoid
# impacting non-PCH builds.
if (ENABLE_PCH)
    # Ensure consistent OpenMP usage across the whole BE (including PCH) to avoid
    # AST version mismatches in Clang when some targets use OpenMP and others don't.
    # We try to find OpenMP quietly and, if found, apply its flags globally.
    find_package(OpenMP QUIET)
    if (OpenMP_CXX_FOUND OR OpenMP_FOUND)
        if (DEFINED OpenMP_CXX_FLAGS AND NOT OpenMP_CXX_FLAGS STREQUAL "")
            # Apply the OpenMP flags to all C++ compilations so PCH and all TUs match.
            add_compile_options($<$<COMPILE_LANGUAGE:CXX>:${OpenMP_CXX_FLAGS}>)
        endif()
        # Ensure final link includes the OpenMP runtime when needed.
        if (TARGET OpenMP::OpenMP_CXX)
            set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} OpenMP::OpenMP_CXX)
        endif()
    endif()

    add_library(pch STATIC ${SRC_DIR}pch/pch.cc)
    # Link COMMON_THIRDPARTY to propagate INTERFACE_INCLUDE_DIRECTORIES to PCH.
    # PCH includes headers from many thirdparty libs (Arrow, CLucene, zstd, etc.).
    target_link_libraries(pch PUBLIC ${COMMON_THIRDPARTY})
    target_precompile_headers(
        pch
        PUBLIC
            ${SRC_DIR}pch/pch.h
    )
    if (OpenMP_CXX_FOUND OR OpenMP_FOUND)
        # Propagate OpenMP to the PCH target so that sources compiled with
        # -fopenmp can reuse the PCH without version conflicts.
        target_link_libraries(pch PUBLIC OpenMP::OpenMP_CXX)
        # Also add flags explicitly to be robust if usage requirements don’t apply during PCH.
        if (DEFINED OpenMP_CXX_FLAGS AND NOT OpenMP_CXX_FLAGS STREQUAL "")
            target_compile_options(pch PUBLIC ${OpenMP_CXX_FLAGS})
        endif()
        # Copy usage requirements from the imported OpenMP target onto PCH.
        if (TARGET OpenMP::OpenMP_CXX)
            get_property(_omp_copts TARGET OpenMP::OpenMP_CXX PROPERTY INTERFACE_COMPILE_OPTIONS)
            if (_omp_copts)
                target_compile_options(pch PUBLIC ${_omp_copts})
            endif()
            get_property(_omp_cdefs TARGET OpenMP::OpenMP_CXX PROPERTY INTERFACE_COMPILE_DEFINITIONS)
            if (_omp_cdefs)
                target_compile_definitions(pch PUBLIC ${_omp_cdefs})
            endif()
            get_property(_omp_cincs TARGET OpenMP::OpenMP_CXX PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
            if (_omp_cincs)
                target_include_directories(pch PUBLIC ${_omp_cincs})
            endif()
        endif()
    endif()
    if (COMPILER_CLANG)
        target_compile_options(pch PRIVATE -Xclang -fno-pch-timestamp)
    endif()
endif()

function(pch_reuse target)
    if (ENABLE_PCH)
        target_precompile_headers(${target} REUSE_FROM pch)
    endif()
endfunction(pch_reuse target)


add_subdirectory(${SRC_DIR}/agent)
add_subdirectory(${SRC_DIR}/common)
add_subdirectory(${SRC_DIR}/core)
add_subdirectory(${SRC_DIR}/exec)
add_subdirectory(${SRC_DIR}/exprs)
add_subdirectory(${SRC_DIR}/format)
add_subdirectory(${SRC_DIR}/gen_cpp)
add_subdirectory(${SRC_DIR}/io)
add_subdirectory(${SRC_DIR}/storage/index/ann)
add_subdirectory(${SRC_DIR}/storage)
add_subdirectory(${SRC_DIR}/runtime)
add_subdirectory(${SRC_DIR}/service) # this include doris_be
add_subdirectory(${SRC_DIR}/udf)
add_subdirectory(${SRC_DIR}/cloud)
add_subdirectory(${SRC_DIR}/load)
add_subdirectory(${SRC_DIR}/information_schema)

option(BUILD_META_TOOL "Build meta tool" OFF)
option(BUILD_INDEX_TOOL "Build index tool" OFF)
if (BUILD_META_TOOL OR BUILD_INDEX_TOOL)
    add_subdirectory(${SRC_DIR}/tools)
endif()

option(BUILD_FILE_CACHE_MICROBENCH_TOOL "Build file cache mirobench Tool" OFF)
if (BUILD_FILE_CACHE_MICROBENCH_TOOL)
    add_subdirectory(${SRC_DIR}/io/tools)
    install(FILES
        ${BASE_DIR}/../bin/start_file_cache_microbench.sh
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_WRITE GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
        DESTINATION ${OUTPUT_DIR}/bin)
endif()

add_subdirectory(${SRC_DIR}/util)

# this include doris_be_test
if (MAKE_TEST)
    include_directories(${TEST_DIR}/)
    # must after include!
    add_subdirectory(${TEST_DIR})
endif ()

add_subdirectory(${COMMON_SRC_DIR}/cpp ${BUILD_DIR}/src/common_cpp)

if(NOT BUILD_BENCHMARK)
    # Install be
    install(DIRECTORY DESTINATION ${OUTPUT_DIR})
    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/bin)
    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/conf)

    install(FILES
        ${BASE_DIR}/../bin/start_be.sh
        ${BASE_DIR}/../bin/stop_be.sh
        ${BASE_DIR}/../tools/jeprof
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_WRITE GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
        DESTINATION ${OUTPUT_DIR}/bin)

    install(FILES
        ${BASE_DIR}/../conf/be.conf
        ${BASE_DIR}/../conf/odbcinst.ini
        ${BASE_DIR}/../conf/asan_suppr.conf
        ${BASE_DIR}/../conf/lsan_suppr.conf
        ${BASE_DIR}/../conf/ubsan_suppr.conf
        ${BASE_DIR}/../conf/msan_suppr.conf
        ${BASE_DIR}/../conf/msan_ignorelist.txt
        DESTINATION ${OUTPUT_DIR}/conf)
endif()

get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach(dir ${dirs})
    message(STATUS "dir='${dir}'")
endforeach()

if (BUILD_BENCHMARK)
    if (NOT ${CMAKE_BUILD_TYPE} STREQUAL "RELEASE")
        message(FATAL_ERROR "Benchmark should be built with RELEASE build type, current build type is ${CMAKE_BUILD_TYPE}")
    endif()
    add_executable(benchmark_test ${BASE_DIR}/benchmark/benchmark_main.cpp)
    set_target_properties(benchmark_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
    target_link_libraries(benchmark_test ${DORIS_LINK_LIBS})
    message(STATUS "Add benchmark to build")
    install(TARGETS benchmark_test DESTINATION ${OUTPUT_DIR}/lib)
endif()
