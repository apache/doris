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

project(doris_cloud CXX C)

# Write compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(cppcheck)

# set platforms
if (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
    set (ARCH_AMD64 1)
endif ()
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
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

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
endif ()

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

# Set dirs
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
if (NOT DEFINED ENV{DORIS_BRANCH})
    set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed")
else()
    set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed-$ENV{DORIS_BRANCH}")
endif()
set(GENSRC_DIR "${BASE_DIR}/../gensrc/build/")
set(COMMON_SRC_DIR "${BASE_DIR}/../common")
set(SRC_DIR "${BASE_DIR}/src/")
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")
set(THIRDPARTY_SRC "$ENV{DORIS_THIRDPARTY}/src")

if (APPLE)
    set(MAKE_TEST "ON")
else()
    option(MAKE_TEST "ON for make unit test or OFF for not" OFF)
endif()
message(STATUS "make test: ${MAKE_TEST}")

# Check gcc
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.0")
        message(FATAL_ERROR "Need GCC version at least 11.0")
    endif()
endif()

set(CMAKE_SKIP_RPATH TRUE)

# Compile generated source if necessary
message(STATUS "build gensrc if necessary")
execute_process(COMMAND make -C ${BASE_DIR}/../gensrc/
                RESULT_VARIABLE MAKE_GENSRC_RESULT)
if(NOT ${MAKE_GENSRC_RESULT} EQUAL 0 AND NOT APPLE)
    message(FATAL_ERROR "Failed to build ${BASE_DIR}/../gensrc/")
endif()

set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")

# Set all libraries
include(thirdparty)

find_program(THRIFT_COMPILER thrift ${CMAKE_SOURCE_DIR}/bin)

# Check if functions are supported in this platform. All flags will generated
# in gensrc/build/common/env_config.h.
# You can check funcion here which depends on platform. Don't forget add this
# to be/src/common/env_config.h.in
include(CheckFunctionExists)
check_function_exists(sched_getcpu HAVE_SCHED_GETCPU)

# compiler flags that are common across debug/release builds
#  -Wall: Enable all warnings.
#  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#  -pthread: enable multithreaded malloc
#  -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
#  -fno-omit-frame-pointers: Keep frame pointer for functions in register
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -pthread -Werror")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fstrict-aliasing -fno-omit-frame-pointer")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++20 -D__STDC_FORMAT_MACROS")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_SYSTEM_NO_DEPRECATED")
# Enable the cpu and heap profile of brpc
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBRPC_ENABLE_CPU_PROFILER")

function(TRY_TO_CHANGE_LINKER LINKER_COMMAND LINKER_NAME)
    if (CUSTUM_LINKER_COMMAND STREQUAL "ld")
        execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=${LINKER_COMMAND} -Wl,--version ERROR_QUIET OUTPUT_VARIABLE LD_VERSION)
        if ("${LD_VERSION}" MATCHES ${LINKER_NAME})
            message("Linker ${LINKER_NAME} is available, change linker to ${LINKER_NAME}")
            set(CUSTUM_LINKER_COMMAND "${LINKER_COMMAND}" PARENT_SCOPE)
        endif()
    endif()
endfunction()

# In terms of performance, mold> lld> gold> ld
set(CUSTUM_LINKER_COMMAND "ld")
# TODO: mold will link fail on thirdparty brpc now, waiting for investigation.
# TRY_TO_CHANGE_LINKER("mold" "mold")
TRY_TO_CHANGE_LINKER("lld" "LLD")
TRY_TO_CHANGE_LINKER("gold" "GNU gold")
if (NOT CUSTUM_LINKER_COMMAND STREQUAL "ld")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${CUSTUM_LINKER_COMMAND}")
endif()

if (USE_LIBCPP AND COMPILER_CLANG)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++")
    add_definitions(-DUSE_LIBCPP)
endif()

if (COMPILER_GCC)
    # Avoid GCC 11 false alarm
    # https://stackoverflow.com/questions/67584073/gcc-11-false-array-subscript-is-partly-outside-array-bounds-warning
    # https://stackoverflow.com/questions/69426070/gcc-11-order-of-arguments-triggers-false-positive-wstringop-overflow-is-this-bu
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-array-bounds -Wno-stringop-overread")
    add_compile_options(-Wno-stringop-overflow -fdiagnostics-color=always)
endif ()

if (COMPILER_CLANG)
    add_compile_options (-fcolor-diagnostics)
    if(MAKE_TEST STREQUAL "OFF")
        add_compile_options(-Qunused-arguments)
    endif()
endif ()

# Compile with jemalloc.
# Adding the option `USE_JEMALLOC=ON sh build.sh` when compiling can turn on building with jemalloc 
if (USE_JEMALLOC)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DUSE_JEMALLOC")
endif()

if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 7.0)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -faligned-new")
endif()

if(BUILD_CHECK_META STREQUAL "ON")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBUILD_CHECK_META")
endif()

# For any gcc builds:
#   -g: Enable symbols for profiler tools. Produce debugging information in the operating system’s native formt
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -ggdb -Wno-unused-local-typedefs")

if (USE_DWARF)
    set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -gdwarf-5")
endif()

# For CMAKE_BUILD_TYPE=Debug
set(CXX_FLAGS_DEBUG "${CXX_GCC_FLAGS} -O0")

# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
set(CXX_FLAGS_RELEASE "${CXX_GCC_FLAGS} -O3 -DNDEBUG")
SET(CXX_FLAGS_ASAN "${CXX_GCC_FLAGS} -O0 -fsanitize=address -DADDRESS_SANITIZER")
SET(CXX_FLAGS_LSAN "${CXX_GCC_FLAGS} -O0 -fsanitize=leak -DLEAK_SANITIZER")

# Set the flags to the undefined behavior sanitizer, also known as "ubsan"
# Turn on sanitizer and debug symbols to get stack traces:
SET(CXX_FLAGS_UBSAN "${CXX_GCC_FLAGS} -O0 -fno-wrapv -fsanitize=undefined")

# Set the flags to the thread sanitizer, also known as "tsan"
# Turn on sanitizer and debug symbols to get stack traces:
# Use -Wno-builtin-declaration-mismatch to mute warnings like "new declaration ‘__tsan_atomic16 __tsan_atomic16_fetch_nand(..."
# If use -O0 to compile, process may stack overflow when start. https://github.com/apache/doris/issues/8868
SET(CXX_FLAGS_TSAN "${CXX_GCC_FLAGS} -O1 -fsanitize=thread -DTHREAD_SANITIZER -Wno-missing-declarations")

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_LSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_UBSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_TSAN}")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Add flags that are common across build types
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS} ${EXTRA_CXX_FLAGS}")

message(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")

# Thrift requires these two definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H)

# Set include dirs
include_directories(
    ${SRC_DIR}/
    ${TEST_DIR}/
)

include_directories(
    SYSTEM
    ${GENSRC_DIR}/
    ${COMMON_SRC_DIR}
    ${THIRDPARTY_DIR}/include
    ${GPERFTOOLS_HOME}/include
)

if ("${DORIS_JAVA_HOME}" STREQUAL "")
    set(DORIS_JAVA_HOME "$ENV{JAVA_HOME}")
endif()

include_directories(${DORIS_JAVA_HOME}/include)
if (NOT OS_MACOSX)
    include_directories(${DORIS_JAVA_HOME}/include/linux)
else()
    include_directories(${DORIS_JAVA_HOME}/include/darwin)
endif()

set(WL_START_GROUP "-Wl,--start-group")
set(WL_END_GROUP "-Wl,--end-group")

# Set Doris libraries
set(DORIS_LINK_LIBS
    ${WL_START_GROUP}
    ${WL_END_GROUP}
)

if ((ARCH_AMD64 OR ARCH_AARCH64) AND OS_LINUX)
    add_library(hadoop_hdfs STATIC IMPORTED)
    set_target_properties(hadoop_hdfs PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/hadoop_hdfs/native/libhdfs.a)

    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        hadoop_hdfs
    )
    add_definitions(-DUSE_HADOOP_HDFS)
else()
    add_library(hdfs3 STATIC IMPORTED)
    set_target_properties(hdfs3 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libhdfs3.a)

    # TODO: use arm hadoop hdfs to replace this
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        hdfs3
    )
    add_definitions(-DUSE_LIBHDFS3)
endif()

set(DORIS_DEPENDENCIES
    ${DORIS_DEPENDENCIES}
    ${WL_START_GROUP}
    ${COMMON_THIRDPARTY}
    ${WL_END_GROUP}
)

message(STATUS "DORIS_DEPENDENCIES is ${DORIS_DEPENDENCIES}")

if ("${DORIS_JAVA_HOME}" STREQUAL "")
    set(DORIS_JAVA_HOME "$ENV{JAVA_HOME}")
endif()

# Add all external dependencies. They should come after the project's libs.
# static link gcc's lib
set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
    ${WL_START_GROUP}
    CloudGen
    Common
    MetaService
    ResourceManager
    Recycler
    RateLimiter
    ${WL_END_GROUP}
    CommonCPP
    ${DORIS_DEPENDENCIES}
    -static-libstdc++
    -static-libgcc
    -lresolv
    -L${DORIS_JAVA_HOME}/lib/server
    -ljvm
)
if (NOT (USE_LIBCPP AND COMPILER_CLANG))
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -lstdc++fs)
endif()

if (USE_JEMALLOC)
    set(MALLOCLIB jemalloc)
else ()
    set(MALLOCLIB tcmalloc)
endif()

option(BUILD_AZURE "ON for building azure support for BE or OFF for not" OFF)
message(STATUS "build azure: ${BUILD_AZURE}")
if(BUILD_AZURE STREQUAL "ON")
    add_definitions(-DUSE_AZURE)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(ASAN_LIBS -static-libasan)
    set(LSAN_LIBS -static-liblsan)
    set(UBSAN_LIBS -static-libubsan ${MALLOCLIB})
    set(TSAN_LIBS -static-libtsan)
else ()
    set(UBSAN_LIBS -rtlib=compiler-rt ${MALLOCLIB})
endif ()

# Add sanitize static link flags or tcmalloc
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${MALLOCLIB})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${ASAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${LSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${UBSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${TSAN_LIBS})
    add_definitions("-DTHREAD_SANITIZER")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
    -lrt -l:libbfd.a -liberty -lc -lm -ldl -pthread
)

if(BUILD_CHECK_META STREQUAL "ON")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -lmysqlclient)
endif ()

# Set libraries for test
set (TEST_LINK_LIBS ${DORIS_LINK_LIBS}
    ${WL_START_GROUP}
    gmock
    gtest
    gtest_main
    ${WL_END_GROUP}
)

# Only build static libs
set(BUILD_SHARED_LIBS OFF)

option(ENABLE_CLANG_COVERAGE "coverage option" OFF)
if (ENABLE_CLANG_COVERAGE AND ENABLE_CLANG_COVERAGE STREQUAL ON AND COMPILER_CLANG)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif ()

if (${MAKE_TEST} STREQUAL "ON")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUNIT_TEST -fprofile-arcs -ftest-coverage -fno-access-control -DGTEST_USE_OWN_TR1_TUPLE=0")
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov")
    add_definitions(-DBE_TEST)
endif ()

# Add libs if needed, download to current dir -- ${BUILD_DIR}
set(FDB_LIB "fdb_lib_7_1_23.tar.xz")
file(GLOB RELEASE_FILE_LIST LIST_DIRECTORIES false "/etc/*release*")
execute_process(COMMAND "cat" ${RELEASE_FILE_LIST}
                RESULT_VARIABLE CAT_RET_CODE
                OUTPUT_VARIABLE CAT_RET_CONTENT)
string(TOUPPER "${CAT_RET_CONTENT}" CAT_RET_CONTENT)
if ("${CAT_RET_CONTENT}" MATCHES "UBUNTU")
    message("Ubuntu OS")
    SET(OS_RELEASE "Ubuntu")
    set(FDB_LIB_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/ubuntu/")
    string(APPEND FDB_LIB_URL "${FDB_LIB}")
    set(FDB_LIB_MD5SUM "a00fe45da95cfac4e0caffa274bb2b30")
else()
    # If it is not ubuntu, it is regarded as centos by default
    message("Centos OS")
    SET(OS_RELEASE "Centos")
    set(FDB_LIB_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/centos/")
    string(APPEND FDB_LIB_URL "${FDB_LIB}")
    set(FDB_LIB_MD5SUM "f9839a564849c0232a351143b4340de0")
endif()

if (NOT EXISTS "${THIRDPARTY_SRC}/${FDB_LIB}")
    file(MAKE_DIRECTORY ${THIRDPARTY_SRC})
    execute_process(COMMAND "curl --retry 10 --retry-delay 2 --retry-max-time 30" "${FDB_LIB_URL}"
                            "-o" "${THIRDPARTY_SRC}/${FDB_LIB}" "-k"
                    RESULTS_VARIABLE DOWNLOAD_RET)
    if (NOT ${DOWNLOAD_RET} STREQUAL "0")
        execute_process(COMMAND "rm" "-rf" "${THIRDPARTY_SRC}/${FDB_LIB}")
        message(FATAL_ERROR "Failed to download dependency of fdb ${FDB_LIB_URL}, remove it")
    endif ()
endif ()

# Add fdb dependencies
add_definitions(-DFDB_API_VERSION=710)
if (NOT EXISTS ${THIRDPARTY_DIR}/include/foundationdb)
    execute_process(COMMAND "md5sum" "${THIRDPARTY_SRC}/${FDB_LIB}"
                    RESULT_VARIABLE MD5SUM_RET_CODE
                    OUTPUT_VARIABLE MD5SUM_CONTENT)
    if (NOT "${MD5SUM_CONTENT}" MATCHES "${FDB_LIB_MD5SUM}")
        execute_process(COMMAND "rm" "-rf" "${THIRDPARTY_SRC}/${FDB_LIB}")
        message(FATAL_ERROR "${THIRDPARTY_SRC}/${FDB_LIB} md5sum check failed, remove it")
    endif ()
    execute_process(COMMAND "tar" "xf" "${THIRDPARTY_SRC}/${FDB_LIB}" "-C" "${THIRDPARTY_DIR}/")
endif ()

# enable glog custom prefix
add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")

add_subdirectory(${SRC_DIR}/common)
add_subdirectory(${SRC_DIR}/gen-cpp)
add_subdirectory(${SRC_DIR}/meta-service)
add_subdirectory(${SRC_DIR}/recycler)
add_subdirectory(${SRC_DIR}/rate-limiter)
add_subdirectory(${SRC_DIR}/resource-manager)

if (${MAKE_TEST} STREQUAL "ON")
    add_subdirectory(${TEST_DIR})
endif ()

add_subdirectory(${COMMON_SRC_DIR}/cpp ${BUILD_DIR}/src/common_cpp)

if (${MAKE_TEST} STREQUAL "OFF")
    add_executable(doris_cloud src/main.cpp)

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(doris_cloud PROPERTIES ENABLE_EXPORTS 1)
    target_link_libraries(doris_cloud ${DORIS_LINK_LIBS})

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib)
    install(TARGETS doris_cloud DESTINATION ${OUTPUT_DIR}/lib)

    if ("${STRIP_DEBUG_INFO}" STREQUAL "ON")
        add_custom_command(TARGET doris_cloud POST_BUILD
            COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:doris_cloud> $<TARGET_FILE:doris_cloud>.dbg
            COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:doris_cloud>
            COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:doris_cloud>.dbg $<TARGET_FILE:doris_cloud>
            )

        install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
        install(FILES $<TARGET_FILE:doris_cloud>.dbg DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
    endif()
endif()


message("install files at ${OUTPUT_DIR}")
install(DIRECTORY DESTINATION ${OUTPUT_DIR})
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/bin)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/conf)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib)

install(FILES
    ${BASE_DIR}/script/start.sh
    ${BASE_DIR}/script/stop.sh
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${OUTPUT_DIR}/bin)

install(FILES
    ${BASE_DIR}/conf/doris_cloud.conf
    DESTINATION ${OUTPUT_DIR}/conf)

install(FILES
    ${THIRDPARTY_DIR}/lib/libfdb_c.so
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${OUTPUT_DIR}/lib)
