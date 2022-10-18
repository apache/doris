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
# to be/test/CMakeLists.txt.
#
# GTest result xml files will be in "be/ut_build_ASAN/gtest_output/"
#####################################################################

set -eo pipefail
set +o posix

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --benchmark        build benchmark-tool
     --clean            clean and build ut
     --run              build and run all ut
     --run --filter=xx  build and run specified ut
     -j                 build parallel
     -h                 print this help message

  Eg.
    $0                                                              build tests
    $0 --run                                                        build and run all tests
    $0 --run --filter=*                                             also runs everything
    $0 --run --filter=FooTest.*                                     runs everything in test suite FooTest
    $0 --run --filter=*Null*:*Constructor*                          runs any test whose full name contains either 'Null' or 'Constructor'
    $0 --run --filter=-*DeathTest.*                                 runs all non-death tests
    $0 --run --filter=FooTest.*-FooTest.Bar                         runs everything in test suite FooTest except FooTest.Bar
    $0 --run --filter=FooTest.*:BarTest.*-FooTest.Bar:BarTest.Foo   runs everything in test suite FooTest except FooTest.Bar and everything in test suite BarTest except BarTest.Foo
    $0 --clean                                                      clean and build tests
    $0 --clean --run                                                clean, build and run all tests
  "
    exit 1
}

if ! OPTS="$(getopt -n "$0" -o vhj:f: -l benchmark,run,clean,filter: -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

PARALLEL="$(($(nproc) / 5 + 1))"

CLEAN=0
RUN=0
BUILD_BENCHMARK_TOOL='OFF'
FILTER=""
if [[ "$#" != 1 ]]; then
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
        --benchmark)
            BUILD_BENCHMARK_TOOL='ON'
            shift
            ;;
        -f | --filter)
            FILTER="--gtest_filter=$2"
            shift 2
            ;;
        -j)
            PARALLEL="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            usage
            exit 0
            ;;
        esac
    done
fi

CMAKE_BUILD_TYPE="${BUILD_TYPE:-ASAN}"
CMAKE_BUILD_TYPE="$(echo "${CMAKE_BUILD_TYPE}" | awk '{ print(toupper($0)) }')"

echo "Get params:
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
"
echo "Build Backend UT"

. "${DORIS_HOME}/env.sh"

CMAKE_BUILD_DIR="${DORIS_HOME}/be/ut_build_${CMAKE_BUILD_TYPE}"
if [[ "${CLEAN}" -eq 1 ]]; then
    rm -rf "${CMAKE_BUILD_DIR}"
    rm -rf "${DORIS_HOME}/be/output"
fi

if [[ ! -d "${CMAKE_BUILD_DIR}" ]]; then
    mkdir -p "${CMAKE_BUILD_DIR}"
fi

if [[ -z "${GLIBC_COMPATIBILITY}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        GLIBC_COMPATIBILITY='ON'
    else
        GLIBC_COMPATIBILITY='OFF'
    fi
fi

if [[ -z "${USE_LIBCPP}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_LIBCPP='OFF'
    else
        USE_LIBCPP='ON'
    fi
fi

if [[ -z "${USE_DWARF}" ]]; then
    USE_DWARF='OFF'
fi

MAKE_PROGRAM="$(which "${BUILD_SYSTEM}")"
echo "-- Make program: ${MAKE_PROGRAM}"
echo "-- Use ccache: ${CMAKE_USE_CCACHE}"
echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"

cd "${CMAKE_BUILD_DIR}"
"${CMAKE_CMD}" -G "${GENERATOR}" \
    -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
    -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
    -DMAKE_TEST=ON \
    -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
    -DUSE_LIBCPP="${USE_LIBCPP}" \
    -DBUILD_META_TOOL=OFF \
    -DBUILD_BENCHMARK_TOOL="${BUILD_BENCHMARK_TOOL}" \
    -DWITH_MYSQL=OFF \
    -DUSE_DWARF="${USE_DWARF}" \
    -DUSE_MEM_TRACKER=ON \
    -DUSE_JEMALLOC=OFF \
    -DSTRICT_MEMORY_USE=OFF \
    -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
    ${CMAKE_USE_CCACHE:+${CMAKE_USE_CCACHE}} \
    "${DORIS_HOME}/be"
"${BUILD_SYSTEM}" -j "${PARALLEL}"

if [[ "${RUN}" -ne 1 ]]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "   Running Backend Unit Test  "
echo "******************************"

cd "${DORIS_HOME}"
export DORIS_TEST_BINARY_DIR="${CMAKE_BUILD_DIR}"
export TERM='xterm'
export UDF_RUNTIME_DIR="${DORIS_HOME}/lib/udf-runtime"
export LOG_DIR="${DORIS_HOME}/log"
while read -r variable; do
    eval "export ${variable}"
done < <(sed 's/ //g' "${DORIS_HOME}/conf/be.conf" | grep -E "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=")

mkdir -p "${LOG_DIR}"
mkdir -p "${UDF_RUNTIME_DIR}"
rm -f "${UDF_RUNTIME_DIR}"/*

# clean all gcda file
while read -r gcda_file; do
    rm "${gcda_file}"
done < <(find "${DORIS_TEST_BINARY_DIR}" -name "*gcda")

export DORIS_TEST_BINARY_DIR="${DORIS_TEST_BINARY_DIR}/test"

jdk_version() {
    local java_cmd="${1}"
    local result
    local IFS=$'\n'

    if [[ -z "${java_cmd}" ]]; then
        result=no_java
        return 1
    else
        local version
        # remove \r for Cygwin
        version="$("${java_cmd}" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n' | grep version | awk '{print $3}')"
        version="${version//\"/}"
        if [[ "${version}" =~ ^1\. ]]; then
            result="$(echo "${version}" | awk -F '.' '{print $2}')"
        else
            result="$(echo "${version}" | awk -F '.' '{print $1}')"
        fi
    fi
    echo "${result}"
    return 0
}

setup_java_env() {
    local java_version

    if [[ -z "${JAVA_HOME}" ]]; then
        return 1
    fi

    local jvm_arch='amd64'
    if [[ "$(uname -m)" == 'aarch64' ]]; then
        jvm_arch='aarch64'
    fi
    java_version="$(
        set -e
        jdk_version "${JAVA_HOME}/bin/java"
    )"
    if [[ "${java_version}" -gt 8 ]]; then
        export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
        # JAVA_HOME is jdk
    elif [[ -d "${JAVA_HOME}/jre" ]]; then
        export LD_LIBRARY_PATH="${JAVA_HOME}/jre/lib/${jvm_arch}/server:${JAVA_HOME}/jre/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
        # JAVA_HOME is jre
    else
        export LD_LIBRARY_PATH="${JAVA_HOME}/lib/${jvm_arch}/server:${JAVA_HOME}/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
    fi
}

# prepare jvm if needed
setup_java_env || true

# prepare gtest output dir
GTEST_OUTPUT_DIR="${CMAKE_BUILD_DIR}/gtest_output"
rm -rf "${GTEST_OUTPUT_DIR}"
mkdir "${GTEST_OUTPUT_DIR}"

# prepare util test_data
mkdir -p "${DORIS_TEST_BINARY_DIR}/util"
if [[ -d "${DORIS_TEST_BINARY_DIR}/util/test_data" ]]; then
    rm -rf "${DORIS_TEST_BINARY_DIR}/util/test_data"
fi
cp -r "${DORIS_HOME}/be/test/util/test_data" "${DORIS_TEST_BINARY_DIR}/util"/

# prepare ut temp dir
UT_TMP_DIR="${DORIS_HOME}/ut_dir"
rm -rf "${UT_TMP_DIR}"
mkdir "${UT_TMP_DIR}"
touch "${UT_TMP_DIR}/tmp_file"

# set asan and ubsan env to generate core file
export ASAN_OPTIONS=symbolize=1:abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1
export UBSAN_OPTIONS=print_stacktrace=1

# find all executable test files
test="${DORIS_TEST_BINARY_DIR}/doris_be_test"
file_name="${test##*/}"
if [[ -f "${test}" ]]; then
    "${test}" --gtest_output="xml:${GTEST_OUTPUT_DIR}/${file_name}.xml" --gtest_print_time=true "${FILTER}"
    echo "=== Finished. Gtest output: ${GTEST_OUTPUT_DIR}"
else
    echo "unit test file: ${test} does not exist."
fi
