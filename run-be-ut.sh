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

. "${DORIS_HOME}/env.sh"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --benchmark        build benchmark-tool
     --clean            clean and build ut
     --run              build and run all ut
     --run --filter=xx  build and run specified ut
     --coverage         coverage after run ut
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
    $0 --clean --run --coverage                                     clean, build, run all tests and coverage
  "
    exit 1
}

if ! OPTS="$(getopt -n "$0" -o vhj:f: -l coverage,benchmark,run,clean,filter: -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

CLEAN=0
RUN=0
BUILD_BENCHMARK_TOOL='OFF'
DENABLE_CLANG_COVERAGE='OFF'
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
        --coverage)
            DENABLE_CLANG_COVERAGE='ON'
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
            ;;
        esac
    done
fi

if [[ -z "${PARALLEL}" ]]; then
    PARALLEL="$(($(nproc) / 5 + 1))"
fi

CMAKE_BUILD_TYPE="${BUILD_TYPE_UT:-ASAN}"
CMAKE_BUILD_TYPE="$(echo "${CMAKE_BUILD_TYPE}" | awk '{ print(toupper($0)) }')"

echo "Get params:
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
    ENABLE_PCH          -- ${ENABLE_PCH}
"
echo "Build Backend UT"

update_submodule() {
    local submodule_path=$1
    local submodule_name=$2
    local archive_url=$3

    set +e
    cd "${DORIS_HOME}"
    echo "Update ${submodule_name} submodule ..."
    git submodule update --init --recursive "${submodule_path}"
    exit_code=$?
    set -e
    if [[ "${exit_code}" -ne 0 ]]; then
        # try to get submodule's current commit
        submodule_commit=$(git ls-tree HEAD "${submodule_path}" | awk '{print $3}')

        commit_specific_url=$(echo "${archive_url}" | sed "s/refs\/heads/${submodule_commit}/")
        echo "Update ${submodule_name} submodule failed, start to download and extract ${commit_specific_url}"

        mkdir -p "${DORIS_HOME}/${submodule_path}"
        curl -L "${commit_specific_url}" | tar -xz -C "${DORIS_HOME}/${submodule_path}" --strip-components=1
    fi
}

update_submodule "be/src/apache-orc" "apache-orc" "https://github.com/apache/doris-thirdparty/archive/refs/heads/orc.tar.gz"
update_submodule "be/src/clucene" "clucene" "https://github.com/apache/doris-thirdparty/archive/refs/heads/clucene.tar.gz"

if [[ "_${DENABLE_CLANG_COVERAGE}" == "_ON" ]]; then
    echo "export DORIS_TOOLCHAIN=clang" >>custom_env.sh
fi

if [[ -z ${CMAKE_BUILD_DIR} ]]; then
    CMAKE_BUILD_DIR="${DORIS_HOME}/be/ut_build_${CMAKE_BUILD_TYPE}"
fi
if [[ "${CLEAN}" -eq 1 ]]; then
    pushd "${DORIS_HOME}/gensrc"
    make clean
    popd

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

if [[ -z "${USE_MEM_TRACKER}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_MEM_TRACKER='ON'
    else
        USE_MEM_TRACKER='OFF'
    fi
fi

if [[ -z "${USE_DWARF}" ]]; then
    USE_DWARF='OFF'
fi

if [[ -z "${USE_UNWIND}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_UNWIND='ON'
    else
        USE_UNWIND='OFF'
    fi
fi

MAKE_PROGRAM="$(command -v "${BUILD_SYSTEM}")"
echo "-- Make program: ${MAKE_PROGRAM}"
echo "-- Use ccache: ${CMAKE_USE_CCACHE}"
echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"

if [[ "${CMAKE_BUILD_TYPE}" = "ASAN" ]]; then
    BUILD_TYPE="ASAN_UT"
else
    BUILD_TYPE="${CMAKE_BUILD_TYPE}"
fi

cd "${CMAKE_BUILD_DIR}"
"${CMAKE_CMD}" -G "${GENERATOR}" \
    -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DMAKE_TEST=ON \
    -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
    -DUSE_LIBCPP="${USE_LIBCPP}" \
    -DBUILD_META_TOOL=OFF \
    -DBUILD_BENCHMARK_TOOL="${BUILD_BENCHMARK_TOOL}" \
    -DWITH_MYSQL=ON \
    -DUSE_DWARF="${USE_DWARF}" \
    -DUSE_UNWIND="${USE_UNWIND}" \
    -DUSE_MEM_TRACKER="${USE_MEM_TRACKER}" \
    -DUSE_JEMALLOC=OFF \
    -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
    -DENABLE_CLANG_COVERAGE="${DENABLE_CLANG_COVERAGE}" \
    ${CMAKE_USE_CCACHE:+${CMAKE_USE_CCACHE}} \
    -DENABLE_PCH="${ENABLE_PCH}" \
    -DDORIS_JAVA_HOME="${JAVA_HOME}" \
    "${DORIS_HOME}/be"
"${BUILD_SYSTEM}" -j "${PARALLEL}"

if [[ "${RUN}" -ne 1 ]]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "   Running Backend Unit Test  "
echo "******************************"

# build running dir env
cd "${DORIS_HOME}"
export DORIS_TEST_BINARY_DIR="${CMAKE_BUILD_DIR}/test/"

# prepare conf dir
CONF_DIR="${DORIS_TEST_BINARY_DIR}/conf"
rm -rf "${CONF_DIR}"
mkdir "${CONF_DIR}"
cp "${DORIS_HOME}/conf/be.conf" "${CONF_DIR}"/

export TERM="xterm"
export UDF_RUNTIME_DIR="${DORIS_TEST_BINARY_DIR}/lib/udf-runtime"
export LOG_DIR="${DORIS_TEST_BINARY_DIR}/log"
while read -r variable; do
    eval "export ${variable}"
done < <(sed 's/[[:space:]]*\(=\)[[:space:]]*/\1/' "${DORIS_TEST_BINARY_DIR}/conf/be.conf" | grep -E "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=")

# prepare log dir
mkdir -p "${LOG_DIR}"
mkdir -p "${UDF_RUNTIME_DIR}"
rm -f "${UDF_RUNTIME_DIR}"/*

# clean all gcda file
while read -r gcda_file; do
    rm "${gcda_file}"
done < <(find "${CMAKE_BUILD_DIR}" -name "*gcda")

setup_java_env() {
    local java_version

    echo "JAVA_HOME: ${JAVA_HOME}"
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

    if [[ "$(uname -s)" == 'Darwin' ]]; then
        export DYLD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${DYLD_LIBRARY_PATH}"
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

# prepare java jars
LIB_DIR="${DORIS_TEST_BINARY_DIR}/lib/"
rm -rf "${LIB_DIR}"
mkdir "${LIB_DIR}"
if [[ -d "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" ]]; then
    cp -r "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" "${LIB_DIR}"
fi
if [[ -f "${DORIS_HOME}/output/be/lib/java-udf-jar-with-dependencies.jar" ]]; then
    cp "${DORIS_HOME}/output/be/lib/java-udf-jar-with-dependencies.jar" "${LIB_DIR}/"
fi

# add java libs
for f in "${LIB_DIR}"/*.jar; do
    if [[ -z "${DORIS_CLASSPATH}" ]]; then
        export DORIS_CLASSPATH="${f}"
    else
        export DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    fi
done

if [[ -d "${LIB_DIR}/hadoop_hdfs/" ]]; then
    # add hadoop libs
    for f in "${LIB_DIR}/hadoop_hdfs/common"/*.jar; do
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
    for f in "${LIB_DIR}/hadoop_hdfs/common/lib"/*.jar; do
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
    for f in "${LIB_DIR}/hadoop_hdfs/hdfs"/*.jar; do
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
    for f in "${LIB_DIR}/hadoop_hdfs/hdfs/lib"/*.jar; do
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
fi

# the CLASSPATH and LIBHDFS_OPTS is used for hadoop libhdfs
# and conf/ dir so that hadoop libhdfs can read .xml config file in conf/
export CLASSPATH="${DORIS_CLASSPATH}"
# DORIS_CLASSPATH is for self-managed jni
export DORIS_CLASSPATH="-Djava.class.path=${DORIS_CLASSPATH}"

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

# check java version and choose correct JAVA_OPTS
java_version="$(
    set -e
    jdk_version "${JAVA_HOME}/bin/java"
)"

CUR_DATE=$(date +%Y%m%d-%H%M%S)
LOG_PATH="-DlogPath=${DORIS_TEST_BINARY_DIR}/log/jni.log"
COMMON_OPTS="-Dsun.java.command=DorisBETEST -XX:-CriticalJNINatives"
JDBC_OPTS="-DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000"

if [[ "${java_version}" -gt 8 ]]; then
    if [[ -z ${JAVA_OPTS} ]]; then
        JAVA_OPTS="-Xmx1024m ${LOG_PATH} -Xloggc:${DORIS_TEST_BINARY_DIR}/log/be.gc.log.${CUR_DATE} ${COMMON_OPTS} ${JDBC_OPTS}"
    fi
    final_java_opt="${JAVA_OPTS}"
else
    if [[ -z ${JAVA_OPTS_FOR_JDK_9} ]]; then
        JAVA_OPTS_FOR_JDK_9="-Xmx1024m ${LOG_PATH} -Xlog:gc:${DORIS_TEST_BINARY_DIR}/log/be.gc.log.${CUR_DATE} ${COMMON_OPTS} ${JDBC_OPTS}"
    fi
    final_java_opt="${JAVA_OPTS_FOR_JDK_9}"
fi

MACHINE_OS=$(uname -s)
if [[ "${MACHINE_OS}" == "Darwin" ]]; then
    max_fd_limit='-XX:-MaxFDLimit'

    if ! echo "${final_java_opt}" | grep "${max_fd_limit/-/\\-}" >/dev/null; then
        final_java_opt="${final_java_opt} ${max_fd_limit}"
    fi

    if [[ -n "${JAVA_OPTS}" ]] && ! echo "${JAVA_OPTS}" | grep "${max_fd_limit/-/\\-}" >/dev/null; then
        JAVA_OPTS="${JAVA_OPTS} ${max_fd_limit}"
    fi
fi

# set LIBHDFS_OPTS for hadoop libhdfs
export LIBHDFS_OPTS="${final_java_opt}"

# set ORC_EXAMPLE_DIR for orc unit tests
export ORC_EXAMPLE_DIR="${DORIS_HOME}/be/src/apache-orc/examples"

# set asan and ubsan env to generate core file
export DORIS_HOME="${DORIS_TEST_BINARY_DIR}/"
## detect_container_overflow=0, https://github.com/google/sanitizers/issues/193
export ASAN_OPTIONS=symbolize=1:abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1:detect_container_overflow=0:check_malloc_usable_size=0
export UBSAN_OPTIONS=print_stacktrace=1
export JAVA_OPTS="-Xmx1024m -DlogPath=${DORIS_HOME}/log/jni.log -Xloggc:${DORIS_HOME}/log/be.gc.log.${CUR_DATE} -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000"

# find all executable test files
test="${DORIS_TEST_BINARY_DIR}/doris_be_test"
profraw=${DORIS_TEST_BINARY_DIR}/doris_be_test.profraw
profdata=${DORIS_TEST_BINARY_DIR}/doris_be_test.profdata

file_name="${test##*/}"
if [[ -f "${test}" ]]; then
    if [[ "_${DENABLE_CLANG_COVERAGE}" == "_ON" ]]; then
        LLVM_PROFILE_FILE="${profraw}" "${test}" --gtest_output="xml:${GTEST_OUTPUT_DIR}/${file_name}.xml" --gtest_print_time=true "${FILTER}"
        if [[ -d "${DORIS_TEST_BINARY_DIR}"/report ]]; then
            rm -rf "${DORIS_TEST_BINARY_DIR}"/report
        fi
        cmd1="${LLVM_PROFDATA} merge -o ${profdata} ${profraw}"
        echo "${cmd1}"
        eval "${cmd1}"
        cmd2="${LLVM_COV} show -output-dir=${DORIS_TEST_BINARY_DIR}/report -format=html \
            -ignore-filename-regex='(.*gensrc/.*)|(.*_test\.cpp$)|(.*be/test.*)|(.*apache-orc/.*)|(.*clucene/.*)' \
            -instr-profile=${profdata} \
            -object=${test}"
        echo "${cmd2}"
        eval "${cmd2}"
    else
        "${test}" --gtest_output="xml:${GTEST_OUTPUT_DIR}/${file_name}.xml" --gtest_print_time=true "${FILTER}"
    fi
    echo "=== Finished. Gtest output: ${GTEST_OUTPUT_DIR}"
else
    echo "unit test file: ${test} does not exist."
fi
