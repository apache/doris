#!/usr/bin/env bash
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

#####################################################################
# This script is used to run unit test of Doris Backend
# Usage: $0 <options>
#  Optional options:
#     --clean            clean and build ut
#     --run              build and run all ut
#     --run --filter=xx  build and run specified ut
#     -j                 build parallel
#     -h                 print this help message
#
# All BE tests must use "_test" as the file suffix, and add the file
# to cloud/test/CMakeLists.txt.
#
# GTest result xml files will be in "cloud/ut_build_ASAN/gtest_output/"
#####################################################################

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

export DORIS_HOME="${ROOT}"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --clean            clean and build ut
     --run              build and run all ut
     --coverage         coverage after run ut
     --run --filter=x   build and run specified ut, filter x format is <binary_name>:<gtest_filter>,
                        a <binary_name> is the name of a cpp file without '.cpp' suffix.
                        e.g. binary_name of xxx_test.cpp is xxx_test
     --fdb              run with a specific fdb connection string, e.g fdb_cluster0:cluster0@192.168.1.100:4500
     -j                 build parallel
     -h                 print this help message

  Eg.
    $0                                                                          build tests
    $0 --run                                                                    build and run all tests
    $0 --run --filter=recycler_test:FooTest.*                                   runs everying of test suite FooTest in recycler_test.cpp
    $0 --run --filter=recycler_test:*Null*:*Constructor*                        runs any test whose full name contains either 'Null' or 'Constructor' in recycler_test.cpp
    $0 --run --filter=recycler_test:-*DeathTest.*                               runs all non-death tests in recycler_test.cpp
    $0 --run --filter=recycler_test:FooTest.*-FooTest.Bar                       runs everything in test suite FooTest except FooTest.Bar in recycler_test.cpp
    $0 --run --filter=recycler_test:FooTest.*:BarTest.*-FooTest.Bar:BarTest.Foo runs everything in test suite FooTest except FooTest.Bar and everything in test suite BarTest except BarTest.Foo in recycler_test.cpp
    $0 --run --fdb=fdb_cluster0:cluster0@192.168.1.100:4500                     run with specific fdb
    $0 --run --coverage                                                         run with coverage report
    $0 --clean                                                                  clean and build tests
    $0 --clean --run                                                            clean, build and run all tests
    "
    exit 1
}

if ! OPTS=$(getopt -n "$0" -o vhj:f: -l run,clean,filter:,fdb:,coverage -- "$@"); then
    usage
fi

set -eo pipefail

eval set -- "${OPTS}"

PARALLEL=$(($(nproc) / 5 + 1))

CLEAN=0
RUN=0
FILTER=""
FDB=""
ENABLE_CLANG_COVERAGE=OFF
echo "===================== filter: ${FILTER}"
if [[ $# != 1 ]]; then
    while true; do
        case "$1" in
        --clean)
            CLEAN=1
            shift
            ;;
        --run)
            RUN=1
            shift
            ;;
        --coverage)
            ENABLE_CLANG_COVERAGE="ON"
            shift
            ;;
        --fdb)
            FDB="$2"
            shift 2
            ;;
        -f | --filter)
            FILTER="$2"
            shift 2
            ;;
        -j)
            PARALLEL=$2
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            usage
            ;;
        esac
    done
fi

echo "===================== filter: ${FILTER}"

CMAKE_BUILD_TYPE=${BUILD_TYPE:-ASAN}
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE^^}"

echo "Get params:
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
    FILTER              -- ${FILTER}
    COVERAGE            -- ${ENABLE_CLANG_COVERAGE}
    FDB                 -- ${FDB}
"
echo "Build Doris Cloud UT"

if [[ "_${ENABLE_CLANG_COVERAGE}" == "_ON" ]]; then
    sed -i "s/DORIS_TOOLCHAIN=gcc/DORIS_TOOLCHAIN=clang/g" env.sh
    echo "export DORIS_TOOLCHAIN=clang" >>custom_env.sh
fi

. "${DORIS_HOME}/env.sh"

CMAKE_BUILD_DIR="${DORIS_HOME}/cloud/ut_build_${CMAKE_BUILD_TYPE}"
if [[ "${CLEAN}" -eq 1 ]]; then
    rm "${CMAKE_BUILD_DIR}" -rf
    rm "${DORIS_HOME}/cloud/output/" -rf
fi

if [[ ! -d "${CMAKE_BUILD_DIR}" ]]; then
    mkdir -p "${CMAKE_BUILD_DIR}"
fi

if [[ -z "${GLIBC_COMPATIBILITY}" ]]; then
    GLIBC_COMPATIBILITY=ON
fi

if [[ -z "${USE_DWARF}" ]]; then
    USE_DWARF=OFF
fi

MAKE_PROGRAM="$(command -v "${BUILD_SYSTEM}")"
echo "-- Make program: ${MAKE_PROGRAM}"

cd "${CMAKE_BUILD_DIR}"
find . -name "*.gcda" -exec rm {} \;

"${CMAKE_CMD}" -G "${GENERATOR}" \
    -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
    -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DMAKE_TEST=ON \
    -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
    -DUSE_DWARF="${USE_DWARF}" \
    -DUSE_MEM_TRACKER=ON \
    -DUSE_JEMALLOC=OFF \
    -DSTRICT_MEMORY_USE=OFF \
    -DENABLE_CLANG_COVERAGE="${ENABLE_CLANG_COVERAGE}" \
    "${CMAKE_USE_CCACHE}" \
    "${DORIS_HOME}/cloud/"
"${BUILD_SYSTEM}" -j "${PARALLEL}"
"${BUILD_SYSTEM}" install

mkdir -p "${CMAKE_BUILD_DIR}/test/log"

# prepare java jars
LIB_DIR="${CMAKE_BUILD_DIR}/test/lib"
rm -rf "${LIB_DIR}"
mkdir "${LIB_DIR}"
if [[ -d "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" ]]; then
    cp -r "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" "${LIB_DIR}"
fi

if [[ "${RUN}" -ne 1 ]]; then
    echo "Finished"
    exit 0
fi

echo "**********************************"
echo "   Running MetaService Unit Test  "
echo "**********************************"

# test binary output dir
cd test
# FILTER: binary_name:gtest_filter
# FILTER: meta_service_test:DetachSchemaKVTest.*
# ./run_all_tests.sh --test "\"$(echo "${FILTER}" | awk -F: '{print $1}')\"" --filter "\"$(echo "${FILTER}" | awk -F: '{print $2}')\"" --fdb "\"${FDB}\""
if [[ "_${ENABLE_CLANG_COVERAGE}" == "_ON" ]]; then
    bash -x ./run_all_tests.sh --coverage --test "$(echo "${FILTER}" | awk -F: '{print $1}')" --filter "$(echo "${FILTER}" | awk -F: '{print $2}')" --fdb "${FDB}"
else
    bash ./run_all_tests.sh --test "$(echo "${FILTER}" | awk -F: '{print $1}')" --filter "$(echo "${FILTER}" | awk -F: '{print $2}')" --fdb "${FDB}"
fi
