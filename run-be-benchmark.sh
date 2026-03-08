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
# This script is used to build and run Google Benchmark of Doris Backend.
# Usage: $0 <options>
#  Optional options:
#     --clean              clean and rebuild benchmark
#     --run                build and run benchmark
#     --run --filter=xx    build and run specified benchmark(s)
#     -j                   build parallel
#     -h                   print this help message
#
# Benchmark requires RELEASE build type.
# The build directory is: be/build_benchmark/
#####################################################################

set -eo pipefail
set +o posix

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export ROOT
export DORIS_HOME="${ROOT}"

. "${DORIS_HOME}/env.sh"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --clean              clean and rebuild benchmark
     --run                build and run benchmark
     --run --filter=xx    build and run specified benchmark(s) (Google Benchmark --benchmark_filter)
     -j                   build parallel
     -h                   print this help message

  Eg.
    $0                                           build benchmark only
    $0 --run                                     build and run all benchmarks
    $0 --run --filter=BM_ByteArrayDictDecode.*   build and run matching benchmarks
    $0 --clean                                   clean and rebuild benchmark
    $0 --clean --run                             clean, rebuild and run all benchmarks
    $0 -j 16 --run                               build with 16 jobs and run
  "
    exit 1
}

if ! OPTS="$(getopt -n "$0" -o hj: -l run,clean,filter: -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

CLEAN=0
RUN=0
FILTER=""
PARALLEL=""
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
        --filter)
            FILTER="$2"
            shift 2
            ;;
        -j)
            PARALLEL="$2"
            shift 2
            ;;
        -h)
            usage
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
    PARALLEL="$(($(nproc) / 4 + 1))"
fi

# Benchmark requires RELEASE build type
CMAKE_BUILD_TYPE="RELEASE"
CMAKE_BUILD_DIR="${DORIS_HOME}/be/build_benchmark"

echo "Get params:
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
    RUN                 -- ${RUN}
    FILTER              -- ${FILTER}
    CMAKE_BUILD_TYPE    -- ${CMAKE_BUILD_TYPE}
    CMAKE_BUILD_DIR     -- ${CMAKE_BUILD_DIR}
    ENABLE_PCH          -- ${ENABLE_PCH}
"
echo "Build Backend Benchmark"

# Update submodules (same as run-be-ut.sh)
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
        submodule_commit=$(git ls-tree HEAD "${submodule_path}" | awk '{print $3}')
        commit_specific_url=$(echo "${archive_url}" | sed "s/refs\/heads/${submodule_commit}/")
        echo "Update ${submodule_name} submodule failed, start to download and extract ${commit_specific_url}"
        mkdir -p "${DORIS_HOME}/${submodule_path}"
        curl -L "${commit_specific_url}" | tar -xz -C "${DORIS_HOME}/${submodule_path}" --strip-components=1
    fi
}

# Update submodules only if they are not initialized yet
if [[ ! -f "${DORIS_HOME}/contrib/apache-orc/CMakeLists.txt" ]]; then
    update_submodule "contrib/apache-orc" "apache-orc" "https://github.com/apache/doris-thirdparty/archive/refs/heads/orc.tar.gz"
fi
if [[ ! -f "${DORIS_HOME}/contrib/clucene/CMakeLists.txt" ]]; then
    update_submodule "contrib/clucene" "clucene" "https://github.com/apache/doris-thirdparty/archive/refs/heads/clucene.tar.gz"
fi

# Handle clean
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

# Platform defaults (same as run-be-ut.sh / build.sh)
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

if [[ -z "${USE_AVX2}" ]]; then
    USE_AVX2='ON'
fi

if [[ -z "${ARM_MARCH}" ]]; then
    ARM_MARCH='armv8-a+crc'
fi

if [[ -z "${USE_UNWIND}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_UNWIND='ON'
    else
        USE_UNWIND='OFF'
    fi
fi

if [[ -z "${USE_JEMALLOC}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_JEMALLOC='ON'
    else
        USE_JEMALLOC='OFF'
    fi
fi

if [[ "$(echo "${DISABLE_BUILD_AZURE}" | tr '[:lower:]' '[:upper:]')" == "ON" ]]; then
    BUILD_AZURE='OFF'
else
    BUILD_AZURE='ON'
fi

MAKE_PROGRAM="$(command -v "${BUILD_SYSTEM}")"
echo "-- Make program: ${MAKE_PROGRAM}"
echo "-- Use ccache: ${CMAKE_USE_CCACHE}"
echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"

# Configure and build
cd "${CMAKE_BUILD_DIR}"

# Only run cmake configure when needed:
#   1. No CMakeCache.txt yet (first build or after --clean)
#   2. User explicitly requested --clean
# Otherwise skip configure and let ninja/make handle incremental builds.
# Ninja will auto re-configure if CMakeLists.txt files changed.
if [[ ! -f "${CMAKE_BUILD_DIR}/CMakeCache.txt" ]]; then
    echo "-- Running cmake configure (first time or after clean) ..."
    "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DMAKE_TEST=OFF \
        -DBUILD_BENCHMARK=ON \
        -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
        -DUSE_LIBCPP="${USE_LIBCPP}" \
        -DBUILD_META_TOOL=OFF \
        -DBUILD_FILE_CACHE_MICROBENCH_TOOL=OFF \
        -DUSE_UNWIND="${USE_UNWIND}" \
        -DUSE_JEMALLOC="${USE_JEMALLOC}" \
        -DUSE_AVX2="${USE_AVX2}" \
        -DARM_MARCH="${ARM_MARCH}" \
        -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
        -DENABLE_CLANG_COVERAGE=OFF \
        -DENABLE_INJECTION_POINT=OFF \
        ${CMAKE_USE_CCACHE:+${CMAKE_USE_CCACHE}} \
        -DENABLE_PCH="${ENABLE_PCH}" \
        -DDORIS_JAVA_HOME="${JAVA_HOME}" \
        -DBUILD_AZURE="${BUILD_AZURE}" \
        "${DORIS_HOME}/be"
else
    echo "-- Skipping cmake configure (CMakeCache.txt exists, use --clean to force reconfigure)"
fi

"${BUILD_SYSTEM}" -j "${PARALLEL}" benchmark_test

if [[ "${RUN}" -ne 1 ]]; then
    echo "Build finished. Binary: ${CMAKE_BUILD_DIR}/bin/benchmark_test"
    echo "To run: $0 --run [--filter=<regex>]"
    exit 0
fi

echo "***********************************"
echo "   Running Backend Benchmark       "
echo "***********************************"

cd "${DORIS_HOME}"

# Setup Java env for JNI dependencies
jdk_version() {
    local java_cmd="${1}"
    local result
    local IFS=$'\n'
    if [[ -z "${java_cmd}" ]]; then
        result=no_java
        return 1
    else
        local version
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
    echo "JAVA_HOME: ${JAVA_HOME}"
    if [[ -z "${JAVA_HOME}" ]]; then
        return 1
    fi

    local jvm_arch='amd64'
    if [[ "$(uname -m)" == 'aarch64' ]]; then
        jvm_arch='aarch64'
    fi
    local java_version
    java_version="$(
        set -e
        jdk_version "${JAVA_HOME}/bin/java"
    )"
    if [[ "${java_version}" -gt 8 ]]; then
        export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
    elif [[ -d "${JAVA_HOME}/jre" ]]; then
        export LD_LIBRARY_PATH="${JAVA_HOME}/jre/lib/${jvm_arch}/server:${JAVA_HOME}/jre/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
    else
        export LD_LIBRARY_PATH="${JAVA_HOME}/lib/${jvm_arch}/server:${JAVA_HOME}/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
    fi

    if [[ "$(uname -s)" == 'Darwin' ]]; then
        export DYLD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${DYLD_LIBRARY_PATH}"
    fi
}

setup_java_env || true

# Prepare minimal runtime dirs
BENCHMARK_BINARY="${CMAKE_BUILD_DIR}/bin/benchmark_test"

CONF_DIR="${CMAKE_BUILD_DIR}/conf"
mkdir -p "${CONF_DIR}"
cp -f "${DORIS_HOME}/conf/be.conf" "${CONF_DIR}/"

LOG_DIR="${CMAKE_BUILD_DIR}/log"
mkdir -p "${LOG_DIR}"

export DORIS_HOME="${CMAKE_BUILD_DIR}"
export TERM="xterm"

# Prepare java classpath
LIB_DIR="${CMAKE_BUILD_DIR}/lib/"
mkdir -p "${LIB_DIR}"
if [[ -d "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" ]]; then
    cp -r "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" "${LIB_DIR}" 2>/dev/null || true
fi

DORIS_CLASSPATH=""
for f in "${LIB_DIR}"/*.jar; do
    [[ -f "${f}" ]] || continue
    if [[ -z "${DORIS_CLASSPATH}" ]]; then
        DORIS_CLASSPATH="${f}"
    else
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    fi
done
if [[ -d "${LIB_DIR}/hadoop_hdfs/" ]]; then
    for f in "${LIB_DIR}/hadoop_hdfs/common"/*.jar; do
        [[ -f "${f}" ]] || continue
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
    for f in "${LIB_DIR}/hadoop_hdfs/common/lib"/*.jar; do
        [[ -f "${f}" ]] || continue
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
    for f in "${LIB_DIR}/hadoop_hdfs/hdfs"/*.jar; do
        [[ -f "${f}" ]] || continue
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
    for f in "${LIB_DIR}/hadoop_hdfs/hdfs/lib"/*.jar; do
        [[ -f "${f}" ]] || continue
        DORIS_CLASSPATH="${f}:${DORIS_CLASSPATH}"
    done
fi
export CLASSPATH="${DORIS_CLASSPATH}"
export DORIS_CLASSPATH="-Djava.class.path=${DORIS_CLASSPATH}"

CUR_DATE=$(date +%Y%m%d-%H%M%S)
export JAVA_OPTS="-Xmx1024m -DlogPath=${LOG_DIR}/jni.log -Xloggc:${LOG_DIR}/be.gc.log.${CUR_DATE} -Dsun.java.command=DorisBEBenchmark -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000"
export LIBHDFS_OPTS="${JAVA_OPTS}"

# Run the benchmark
if [[ ! -f "${BENCHMARK_BINARY}" ]]; then
    echo "Error: benchmark binary not found: ${BENCHMARK_BINARY}"
    exit 1
fi

BENCHMARK_ARGS=()
if [[ -n "${FILTER}" ]]; then
    BENCHMARK_ARGS+=("--benchmark_filter=${FILTER}")
fi

echo "Running: ${BENCHMARK_BINARY} ${BENCHMARK_ARGS[*]}"
"${BENCHMARK_BINARY}" "${BENCHMARK_ARGS[@]}"

echo "=== Benchmark finished ==="
