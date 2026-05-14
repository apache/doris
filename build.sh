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

##############################################################
# This script is used to compile Apache Doris
# Usage:
#    sh build.sh --help
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"
if [[ -z "${DORIS_THIRDPARTY}" ]]; then
    export DORIS_THIRDPARTY="${DORIS_HOME}/thirdparty"
fi
export TP_INCLUDE_DIR="${DORIS_THIRDPARTY}/installed/include"
export TP_LIB_DIR="${DORIS_THIRDPARTY}/installed/lib"
HADOOP_DEPS_NAME="hadoop-deps"
. "${DORIS_HOME}/env.sh"

# ===== Build Profile =====
if [[ "${DORIS_BUILD_PROFILE}" == "1" ]]; then
    _BP_STATE="${DORIS_HOME}/.build_profile_state.$$"
    "${DORIS_HOME}/build_profile.sh" collect "${_BP_STATE}" "$*"
    trap '"${DORIS_HOME}/build_profile.sh" record "${_BP_STATE}" 130; exit 130' INT TERM
    trap '"${DORIS_HOME}/build_profile.sh" record "${_BP_STATE}" $?; exit $?' ERR
fi
# ===== End Build Profile =====

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     [no option]                build all components
     --fe                       build Frontend. Default ON.
     --be                       build Backend. Default ON.
     --asan                     build Backend in ASAN (AddressSanitizer) mode. Implies --be.
     --tsan                     build Backend in TSAN (ThreadSanitizer) mode. Implies --be.
     --ubsan                    build Backend in UBSAN (UndefinedBehaviorSanitizer) mode. Implies --be.
     --ut                       build Backend unit tests. Implies --be and ASAN mode (unless overridden).
     --meta-tool                build Backend meta tool. Default OFF.
     --file-cache-microbench    build Backend file cache microbench tool. Default OFF.
     --cloud                    build Cloud. Default OFF.
     --index-tool               build Backend inverted index tool. Default OFF.
     --benchmark                build Google Benchmark. Default OFF.
     --task-executor-simulator  build Backend task executor simulator. Default OFF.
     --broker                   build Broker. Default ON.
     --hive-udf                 build Hive UDF library for Ingestion Load. Default ON.
     --be-java-extensions       build Backend java extensions. Default ON.
     --be-cdc-client            build Cdc Client for backend. Default ON.
     --be-extension-ignore      build be-java-extensions package, choose which modules to ignore. Multiple modules separated by commas.
     --enable-dynamic-arch      enable dynamic CPU detection in OpenBLAS. Default ON.
     --disable-dynamic-arch     disable dynamic CPU detection in OpenBLAS.
     --clean                    clean and build target
     --use-source               build thirdparty from source (default; required for sanitizer builds)
     --use-prebuilt             link prebuilt thirdparty from \$DORIS_THIRDPARTY/installed (Release/Debug only)
     --output                   specify the output directory
     -j                         build Backend parallel

  Environment variables:
    USE_AVX2                    If the CPU does not support AVX2 instruction set, please set USE_AVX2=0. Default is ON.
    ENABLE_DYNAMIC_ARCH         If set ENABLE_DYNAMIC_ARCH=ON, it will enable dynamic CPU detection in OpenBLAS. Default is ON. Can also use --enable-dynamic-arch flag.
    ARM_MARCH                   Specify the ARM architecture instruction set. Default is armv8-a+crc.
    STRIP_DEBUG_INFO            If set STRIP_DEBUG_INFO=ON, the debug information in the compiled binaries will be stored separately in the 'be/lib/debug_info' directory. Default is OFF.
    DISABLE_BE_JAVA_EXTENSIONS  If set DISABLE_BE_JAVA_EXTENSIONS=ON, we will do not build binary with java-udf,hadoop-hudi-scanner,jdbc-scanner and so on Default is OFF.
    DISABLE_JAVA_CHECK_STYLE    If set DISABLE_JAVA_CHECK_STYLE=ON, it will skip style check of java code in FE.
    DISABLE_BUILD_AZURE         If set DISABLE_BUILD_AZURE=ON, it will not build azure into BE.
    DISABLE_BUILD_JUICEFS       If set DISABLE_BUILD_JUICEFS=OFF, it will package juicefs-hadoop jar into FE/BE output. Default is ON (skip).
    DISABLE_BUILD_JINDOFS       If set DISABLE_BUILD_JINDOFS=OFF, it will package jindofs jars into FE/BE output. Default is ON (skip).

  Eg.
    $0                                      build all
    $0 --be                                 build Backend
    $0 --meta-tool                          build Backend meta tool
    $0 --file-cache-microbench              build Backend file cache microbench tool
    $0 --cloud                              build Cloud
    $0 --index-tool                         build Backend inverted index tool
    $0 --benchmark                          build Google Benchmark of Backend
    $0 --fe --clean                         clean and build Frontend.
    $0 --fe --be --clean                    clean and build Frontend and Backend
    $0 --task-executor-simulator            build task executor simulator
    $0 --broker                             build Broker
    $0 --be --fe                            build Backend, Frontend, and Java UDF library
    $0 --be --coverage                      build Backend with coverage enabled
    $0 --be --asan                          build Backend with AddressSanitizer
    $0 --be --tsan                          build Backend with ThreadSanitizer
    $0 --be --ubsan                          build Backend with UndefinedBehaviorSanitizer
    $0 --ut                                 build Backend unit tests (ASAN mode)
    $0 --ut --tsan                          build Backend unit tests (TSAN mode)
    $0 --be --output PATH                   build Backend, the result will be output to PATH(relative paths are available)
    $0 --be-extension-ignore avro-scanner   build be-java-extensions, choose which modules to ignore. Multiple modules separated by commas, like --be-extension-ignore avro-scanner,hadoop-hudi-scanner
    $0 --be --use-prebuilt                  link prebuilt thirdparty (must run \`bash thirdparty/build-thirdparty.sh\` first)
    $0 --be --use-source                    build thirdparty from source (default for sanitizer builds)

    USE_AVX2=0 $0 --be                      build Backend and not using AVX2 instruction.
    USE_AVX2=0 STRIP_DEBUG_INFO=ON $0       build all and not using AVX2 instruction, and strip the debug info for Backend
    ARM_MARCH=armv8-a+crc+simd $0 --be      build Backend with specified ARM architecture instruction set
    $0 --be --disable-dynamic-arch          build Backend with DYNAMIC_ARCH disabled in OpenBLAS
    ENABLE_DYNAMIC_ARCH=OFF $0 --be         build Backend with DYNAMIC_ARCH disabled via environment variable
  "
    exit 1
}

clean_gensrc() {
    pushd "${DORIS_HOME}/gensrc"
    make clean
    rm -rf "${DORIS_HOME}/gensrc/build"
    rm -rf "${DORIS_HOME}/fe/fe-thrift/target"
    rm -rf "${DORIS_HOME}/fe/fe-common/target"
    rm -rf "${DORIS_HOME}/fe/fe-core/target"
    popd
}

clean_be() {
    pushd "${DORIS_HOME}"

    # "build.sh --clean" just cleans and exits, however CMAKE_BUILD_DIR is set
    # while building be.
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    CMAKE_BUILD_DIR="${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}"
    if [[ "${MAKE_TEST}" == "ON" ]]; then
        CMAKE_BUILD_DIR="${DORIS_HOME}/be/ut_build_${CMAKE_BUILD_TYPE}"
    fi

    rm -rf "${CMAKE_BUILD_DIR}"
    rm -rf "${DORIS_HOME}/be/output"
    popd
}

clean_fe() {
    pushd "${DORIS_HOME}/fe"
    "${MVN_CMD}" clean
    popd
}

# Copy the common files like licenses, notice.txt to output folder
function copy_common_files() {
    cp -r -p "${DORIS_HOME}/NOTICE.txt" "$1/"
    cp -r -p "${DORIS_HOME}/dist/LICENSE-dist.txt" "$1/"
    cp -r -p "${DORIS_HOME}/dist/licenses" "$1/"
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'fe' \
    -l 'be' \
    -l 'cloud' \
    -l 'broker' \
    -l 'meta-tool' \
    -l 'file-cache-microbench' \
    -l 'index-tool' \
    -l 'benchmark' \
    -l 'task-executor-simulator' \
    -l 'spark-dpp' \
    -l 'hive-udf' \
    -l 'be-java-extensions' \
    -l 'be-cdc-client' \
    -l 'be-extension-ignore:' \
    -l 'enable-dynamic-arch' \
    -l 'disable-dynamic-arch' \
    -l 'clean' \
    -l 'coverage' \
    -l 'asan' \
    -l 'tsan' \
    -l 'ut' \
    -l 'ubsan' \
    -l 'msan' \
    -l 'use-prebuilt' \
    -l 'use-source' \
    -l 'help' \
    -l 'output:' \
    -o 'hj:' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

PARALLEL="$(($(nproc) / 4 + 1))"
BUILD_FE=0
BUILD_BE=0
BUILD_CLOUD=0
BUILD_BROKER=0
BUILD_META_TOOL='OFF'
BUILD_FILE_CACHE_MICROBENCH_TOOL='OFF'
BUILD_INDEX_TOOL='OFF'
BUILD_BENCHMARK='OFF'
BUILD_TASK_EXECUTOR_SIMULATOR='OFF'
BUILD_BE_JAVA_EXTENSIONS=0
BUILD_BE_CDC_CLIENT=0
BUILD_OBS_DEPENDENCIES=1
BUILD_COS_DEPENDENCIES=1
BUILD_HIVE_UDF=0
ENABLE_DYNAMIC_ARCH='ON'
CLEAN=0
HELP=0
PARAMETER_COUNT="$#"
PARAMETER_FLAG=0
DENABLE_CLANG_COVERAGE='OFF'
BUILD_AZURE='ON'
MAKE_TEST='OFF'
BUILD_UI=1
if [[ "$#" == 1 ]]; then
    # default
    BUILD_FE=1
    BUILD_BE=1
    BUILD_CLOUD=1

    BUILD_BROKER=1
    BUILD_META_TOOL='OFF'
    BUILD_FILE_CACHE_MICROBENCH_TOOL='OFF'
    BUILD_TASK_EXECUTOR_SIMULATOR='OFF'
    BUILD_INDEX_TOOL='OFF'
    BUILD_BENCHMARK='OFF'
    BUILD_HIVE_UDF=1
    BUILD_BE_JAVA_EXTENSIONS=1
    BUILD_BE_CDC_CLIENT=1
    CLEAN=0
else
    while true; do
        case "$1" in
        --fe)
            BUILD_FE=1
            BUILD_HIVE_UDF=1
            BUILD_BE_JAVA_EXTENSIONS=1
            shift
            ;;
        --be)
            BUILD_BE=1
            BUILD_BE_JAVA_EXTENSIONS=1
            BUILD_BE_CDC_CLIENT=1
            shift
            ;;
        --cloud)
            BUILD_CLOUD=1
            BUILD_BE_JAVA_EXTENSIONS=1
            shift
            ;;
        --broker)
            BUILD_BROKER=1
            shift
            ;;
        --meta-tool)
            BUILD_META_TOOL='ON'
            shift
            ;;
        --file-cache-microbench)
            BUILD_FILE_CACHE_MICROBENCH_TOOL='ON'
            shift
            ;;
        --index-tool)
            BUILD_INDEX_TOOL='ON'
            shift
            ;;
        --benchmark)
            BUILD_BENCHMARK='ON'
            BUILD_BE=1 # go into BE cmake building, but benchmark instead of doris_be
            shift
            ;;
        --task-executor-simulator)
            BUILD_TASK_EXECUTOR_SIMULATOR='ON'
            BUILD_BE=1
            shift
            ;;
        --spark-dpp)
            BUILD_SPARK_DPP=1
            shift
            ;;
        --hive-udf)
            BUILD_HIVE_UDF=1
            shift
            ;;
        --be-java-extensions)
            BUILD_BE_JAVA_EXTENSIONS=1
            shift
            ;;
        --be-cdc-client)
            BUILD_BE_CDC_CLIENT=1
            shift
            ;;    
        --exclude-obs-dependencies)
            BUILD_OBS_DEPENDENCIES=0
            shift
            ;; 
        --exclude-cos-dependencies)
            BUILD_COS_DEPENDENCIES=0
            shift
            ;;
        --enable-dynamic-arch)
            ENABLE_DYNAMIC_ARCH='ON'
            shift
            ;;
        --disable-dynamic-arch)
            ENABLE_DYNAMIC_ARCH='OFF'
            shift
            ;;           
        --clean)
            CLEAN=1
            shift
            ;;
        --coverage)
            DENABLE_CLANG_COVERAGE='ON'
            shift
            ;;
        --asan)
            BUILD_TYPE='ASAN'
            BUILD_BE=1
            BUILD_BE_JAVA_EXTENSIONS=1
            BUILD_BE_CDC_CLIENT=1
            shift
            ;;
        --tsan)
            BUILD_TYPE='TSAN'
            BUILD_BE=1
            BUILD_BE_JAVA_EXTENSIONS=1
            BUILD_BE_CDC_CLIENT=1
            shift
            ;;
        --ubsan)
            BUILD_TYPE='UBSAN'
            BUILD_BE=1
            BUILD_BE_JAVA_EXTENSIONS=1
            BUILD_BE_CDC_CLIENT=1
            shift
            ;;
        --msan)
            BUILD_TYPE='MSAN'
            BUILD_BE=1
            BUILD_BE_JAVA_EXTENSIONS=1
            BUILD_BE_CDC_CLIENT=1
            shift
            ;;
        --use-prebuilt)
            USE_CONTRIB_SOURCE_OVERRIDE='OFF'
            shift
            ;;
        --use-source)
            USE_CONTRIB_SOURCE_OVERRIDE='ON'
            shift
            ;;
        --ut)
            MAKE_TEST='ON'
            BUILD_BE=1
            BUILD_BE_JAVA_EXTENSIONS=1
            BUILD_BE_CDC_CLIENT=1
            shift
            ;;
        -h)
            HELP=1
            shift
            ;;
        --help)
            HELP=1
            shift
            ;;
        -j)
            PARALLEL="$2"
            PARAMETER_FLAG=1
            shift 2
            ;;
        --output)
            DORIS_OUTPUT="$2"
            shift 2
            ;;
        --be-extension-ignore)
            BE_EXTENSION_IGNORE="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error"
            exit 1
            ;;
        esac
    done
    #only ./build.sh -j xx then build all
    if [[ "${PARAMETER_COUNT}" -eq 3 ]] && [[ "${PARAMETER_FLAG}" -eq 1 ]]; then
        BUILD_FE=1
        BUILD_BE=1
        BUILD_CLOUD=1
        BUILD_BROKER=1
        BUILD_META_TOOL='ON'
        BUILD_FILE_CACHE_MICROBENCH_TOOL='OFF'
        BUILD_INDEX_TOOL='ON'
	    BUILD_TASK_EXECUTOR_SIMULATOR='OFF'
        BUILD_HIVE_UDF=1
        BUILD_BE_JAVA_EXTENSIONS=1
        BUILD_BE_CDC_CLIENT=1
        CLEAN=0
    fi
fi

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi
# download thirdparty source code if necessary (CMake will build from source)
# "${DORIS_THIRDPARTY}/download-thirdparty.sh"

update_submodule() {
    local submodule_path=$1
    local submodule_name=$2
    local archive_url=$3

    set +e
    cd "${DORIS_HOME}"
    echo "Update ${submodule_name} submodule ..."
    git submodule update --init --recursive "${submodule_path}"
    exit_code=$?
    if [[ "${exit_code}" -eq 0 ]]; then
        cd "${submodule_path}"
        submodule_commit_id=$(git rev-parse HEAD)
        cd -
        expect_submodule_commit_id=$(git ls-tree HEAD "${submodule_path}" | awk '{print $3}')
        echo "Current commit ID of ${submodule_name} submodule: ${submodule_commit_id}, expected is ${expect_submodule_commit_id}"
    fi
    set -e
    if [[ "${exit_code}" -ne 0 ]]; then
        set +e
        # try to get submodule's current commit
        submodule_commit=$(git ls-tree HEAD "${submodule_path}" | awk '{print $3}')
        exit_code=$?
        if [[ "${exit_code}" = "0" ]]; then
            commit_specific_url=$(echo "${archive_url}" | sed "s/refs\/heads/${submodule_commit}/")
        else
            commit_specific_url="${archive_url}"
        fi
        set -e
        echo "Update ${submodule_name} submodule failed, start to download and extract ${commit_specific_url}"

        mkdir -p "${DORIS_HOME}/${submodule_path}"
        curl -L "${commit_specific_url}" | tar -xz -C "${DORIS_HOME}/${submodule_path}" --strip-components=1
    fi
}

if [[ "${CLEAN}" -eq 1 && "${BUILD_BE}" -eq 0 && "${BUILD_FE}" -eq 0 && ${BUILD_CLOUD} -eq 0 ]]; then
    clean_gensrc
    clean_be
    clean_fe
    exit 0
fi

if [[ -z "${GLIBC_COMPATIBILITY}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        GLIBC_COMPATIBILITY='ON'
    else
        GLIBC_COMPATIBILITY='OFF'
    fi
fi
if [[ -z "${USE_AVX2}" ]]; then
    USE_AVX2='ON'
fi
if [[ -z "${ARM_MARCH}" ]]; then
    ARM_MARCH='armv8-a+crc'
fi
if [[ -z "${USE_LIBCPP}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        USE_LIBCPP='OFF'
    else
        USE_LIBCPP='ON'
    fi
fi
if [[ -z "${STRIP_DEBUG_INFO}" ]]; then
    STRIP_DEBUG_INFO='OFF'
fi
BUILD_TYPE_LOWWER=$(echo "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')
# For UT builds, default to ASAN mode if no BUILD_TYPE is explicitly set
if [[ "${MAKE_TEST}" == 'ON' && -z "${BUILD_TYPE}" ]]; then
    BUILD_TYPE='ASAN'
    BUILD_TYPE_LOWWER='asan'
fi
if [[ "${BUILD_TYPE_LOWWER}" == "asan" || "${BUILD_TYPE_LOWWER}" == "tsan" || "${BUILD_TYPE_LOWWER}" == "ubsan" ]]; then
    USE_JEMALLOC='OFF'
    # Sanitizer builds must disable PCH to avoid PIE mismatch errors
    ENABLE_PCH='OFF'
    # UT builds enable injection points by default
    if [[ "${MAKE_TEST}" == 'ON' ]]; then
        ENABLE_INJECTION_POINT='ON'
    fi
elif [[ -z "${USE_JEMALLOC}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        USE_JEMALLOC='ON'
    else
        USE_JEMALLOC='OFF'
    fi
fi

if [[ -z "${USE_BTHREAD_SCANNER}" ]]; then
    USE_BTHREAD_SCANNER='OFF'
fi

if [[ -z "${USE_UNWIND}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        USE_UNWIND='ON'
    else
        USE_UNWIND='OFF'
    fi
fi

if [[ -z "${DISPLAY_BUILD_TIME}" ]]; then
    DISPLAY_BUILD_TIME='OFF'
fi

if [[ -z "${OUTPUT_BE_BINARY}" ]]; then
    OUTPUT_BE_BINARY=${BUILD_BE}
fi

if [[ -n "${DISABLE_BE_JAVA_EXTENSIONS}" ]]; then
    if [[ "${DISABLE_BE_JAVA_EXTENSIONS}" == "ON" ]]; then
        BUILD_BE_JAVA_EXTENSIONS=0
    else
        BUILD_BE_JAVA_EXTENSIONS=1
    fi
fi

if [[ -n "${DISABLE_BE_CDC_CLIENT}" ]]; then
    if [[ "${DISABLE_BE_CDC_CLIENT}" == "ON" ]]; then
        BUILD_BE_CDC_CLIENT=0
    else
        BUILD_BE_CDC_CLIENT=1
    fi
fi

if [[ -n "${DISABLE_BUILD_UI}" ]]; then
    if [[ "${DISABLE_BUILD_UI}" == "ON" ]]; then
        BUILD_UI=0
    fi
fi

if [[ -n "${DISABLE_BUILD_HIVE_UDF}" ]]; then
    if [[ "${DISABLE_BUILD_HIVE_UDF}" == "ON" ]]; then
        BUILD_HIVE_UDF=0
    fi
fi

if [[ -z "${DISABLE_JAVA_CHECK_STYLE}" ]]; then
    DISABLE_JAVA_CHECK_STYLE='OFF'
fi

if [[ "$(echo "${DISABLE_BUILD_AZURE}" | tr '[:lower:]' '[:upper:]')" == "ON" ]]; then
    BUILD_AZURE='OFF'
fi

if [[ "$(echo "${DISABLE_BUILD_JINDOFS}" | tr '[:lower:]' '[:upper:]')" == "OFF" ]]; then
    BUILD_JINDOFS='ON'
else
    BUILD_JINDOFS='OFF'
fi
export DISABLE_BUILD_JINDOFS

if [[ "$(echo "${DISABLE_BUILD_JUICEFS}" | tr '[:lower:]' '[:upper:]')" == "ON" ]]; then
    BUILD_JUICEFS='OFF'
else
    BUILD_JUICEFS='ON'
fi
export DISABLE_BUILD_JUICEFS

if [[ -z "${ENABLE_INJECTION_POINT}" ]]; then
    ENABLE_INJECTION_POINT='OFF'
fi

if [[ -z "${BUILD_BENCHMARK}" ]]; then
    BUILD_BENCHMARK='OFF'
fi

if [[ -z "${RECORD_COMPILER_SWITCHES}" ]]; then
    RECORD_COMPILER_SWITCHES='OFF'
fi

if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 && "${TARGET_SYSTEM}" == 'Darwin' ]]; then
    if [[ -z "${JAVA_HOME}" ]]; then
        CAUSE='the environment variable JAVA_HOME is not set'
    else
        LIBJVM="$(find -L "${JAVA_HOME}/" -name 'libjvm.dylib')"
        if [[ -z "${LIBJVM}" ]]; then
            CAUSE="the library libjvm.dylib is missing"
        elif [[ "$(file "${LIBJVM}" | awk '{print $NF}')" != "$(uname -m)" ]]; then
            CAUSE='the architecture which the library libjvm.dylib is built for does not match'
        fi
    fi

    if [[ -n "${CAUSE}" ]]; then
        echo -e "\033[33;1mWARNNING: \033[37;1mSkip building with BE Java extensions due to ${CAUSE}.\033[0m"
        BUILD_BE_JAVA_EXTENSIONS=0
        BUILD_BE_JAVA_EXTENSIONS_FALSE_IN_CONF=1
    fi
fi

if [[ -z "${WITH_TDE_DIR}" ]]; then
    WITH_TDE_DIR=''
fi

echo "Get params:
    BUILD_FE                            -- ${BUILD_FE}
    BUILD_BE                            -- ${BUILD_BE}
    BUILD_CLOUD                         -- ${BUILD_CLOUD}
    BUILD_BROKER                        -- ${BUILD_BROKER}
    BUILD_META_TOOL                     -- ${BUILD_META_TOOL}
    BUILD_FILE_CACHE_MICROBENCH_TOOL    -- ${BUILD_FILE_CACHE_MICROBENCH_TOOL}
    BUILD_INDEX_TOOL                    -- ${BUILD_INDEX_TOOL}
    BUILD_BENCHMARK                     -- ${BUILD_BENCHMARK}
    BUILD_TASK_EXECUTOR_SIMULATOR       -- ${BUILD_TASK_EXECUTOR_SIMULATOR}
    BUILD_BE_JAVA_EXTENSIONS            -- ${BUILD_BE_JAVA_EXTENSIONS}
    BUILD_BE_CDC_CLIENT                 -- ${BUILD_BE_CDC_CLIENT}
    BUILD_HIVE_UDF                      -- ${BUILD_HIVE_UDF}
    BUILD_JUICEFS                       -- ${BUILD_JUICEFS}
    BUILD_JINDOFS                       -- ${BUILD_JINDOFS}
    PARALLEL                            -- ${PARALLEL}
    CLEAN                               -- ${CLEAN}
    GLIBC_COMPATIBILITY                 -- ${GLIBC_COMPATIBILITY}
    USE_AVX2                            -- ${USE_AVX2}
    USE_LIBCPP                          -- ${USE_LIBCPP}
    USE_UNWIND                          -- ${USE_UNWIND}
    STRIP_DEBUG_INFO                    -- ${STRIP_DEBUG_INFO}
    USE_JEMALLOC                        -- ${USE_JEMALLOC}
    USE_BTHREAD_SCANNER                 -- ${USE_BTHREAD_SCANNER}
    ENABLE_INJECTION_POINT              -- ${ENABLE_INJECTION_POINT}
    DENABLE_CLANG_COVERAGE              -- ${DENABLE_CLANG_COVERAGE}
    DISPLAY_BUILD_TIME                  -- ${DISPLAY_BUILD_TIME}
    ENABLE_PCH                          -- ${ENABLE_PCH}
    WITH_TDE_DIR                        -- ${WITH_TDE_DIR}
"

FEAT=()
FEAT+=($([[ -n "${WITH_TDE_DIR}" ]] && echo "+TDE" || echo "-TDE"))
FEAT+=($([[ "${ENABLE_HDFS_STORAGE_VAULT:-OFF}" == "ON" ]] && echo "+HDFS_STORAGE_VAULT" || echo "-HDFS_STORAGE_VAULT"))
FEAT+=($([[ ${BUILD_UI} -eq 1 ]] && echo "+UI" || echo "-UI"))
FEAT+=($([[ "${BUILD_AZURE}" == "ON" ]] && echo "+AZURE_BLOB,+AZURE_STORAGE_VAULT" || echo "-AZURE_BLOB,-AZURE_STORAGE_VAULT"))
FEAT+=($([[ ${BUILD_HIVE_UDF} -eq 1 ]] && echo "+HIVE_UDF" || echo "-HIVE_UDF"))
FEAT+=($([[ ${BUILD_BE_JAVA_EXTENSIONS} -eq 1 ]] && echo "+BE_JAVA_EXTENSIONS" || echo "-BE_JAVA_EXTENSIONS"))

export DORIS_FEATURE_LIST=$(IFS=','; echo "${FEAT[*]}")
echo "Feature List: ${DORIS_FEATURE_LIST}"

# Clean and build generated code
if [[ "${CLEAN}" -eq 1 ]]; then
    clean_gensrc
fi

# Ensure protoc and thrift compilers are available before generating code.
# They may be missing after a --clean (broken symlinks pointing into deleted build dirs).
_PROTOC_BIN="${DORIS_THIRDPARTY}/installed/bin/protoc"
_THRIFT_BIN="${DORIS_THIRDPARTY}/installed/bin/thrift"
_GENSRC_TOOLS_DIR="${DORIS_THIRDPARTY}/_gensrc_tools"

# Make sure thirdparty source tree is populated before protoc/thrift fallback
# tries to compile them from src/. CI may export DORIS_THIRDPARTY= to force
# rebuild, so thirdparty/installed/ is empty AND thirdparty/src/ starts empty
# on a fresh clone.
if [[ "${USE_CONTRIB_SOURCE_OVERRIDE}" != "OFF" ]]; then
    _tp_src_count_pre=0
    if [[ -d "${DORIS_THIRDPARTY}/src" ]]; then
        _tp_src_count_pre=$(find "${DORIS_THIRDPARTY}/src" -maxdepth 1 -mindepth 1 | wc -l)
    fi
    if [[ "${_tp_src_count_pre}" -lt 10 ]]; then
        # Apache Doris CI mounts /var/local/thirdparty/src/ into the build
        # container with all extracted source trees pre-staged (the mirror at
        # doris-thirdparty-repo.bj.bcebos.com is missing a few newer tarballs,
        # so falling through to download-thirdparty.sh is flaky). Prefer the
        # CI-staged tree when present.
        _CI_TP_SRC="/var/local/thirdparty/src"
        _ci_src_count=0
        if [[ -d "${_CI_TP_SRC}" ]]; then
            _ci_src_count=$(find "${_CI_TP_SRC}" -maxdepth 1 -mindepth 1 | wc -l)
        fi
        if [[ "${_ci_src_count}" -ge 10 ]]; then
            echo "[build.sh] using ${_ci_src_count} staged thirdparty source entries from ${_CI_TP_SRC}"
            mkdir -p "${DORIS_THIRDPARTY}/src"
            for _ci_entry in "${_CI_TP_SRC}"/*; do
                _ci_name=$(basename "${_ci_entry}")
                if [[ ! -e "${DORIS_THIRDPARTY}/src/${_ci_name}" ]]; then
                    ln -sfn "${_ci_entry}" "${DORIS_THIRDPARTY}/src/${_ci_name}"
                fi
            done
        else
            echo "[build.sh] thirdparty/src appears empty (${_tp_src_count_pre} entries);"
            echo "[build.sh] running download-thirdparty.sh to fetch sources..."
            bash "${DORIS_THIRDPARTY}/download-thirdparty.sh"
        fi
    fi
fi

if [[ ! -x "${_PROTOC_BIN}" ]]; then
    echo "protoc not found or not executable at ${_PROTOC_BIN}, building from source..."
    _PROTOC_BUILD_DIR="${_GENSRC_TOOLS_DIR}/protoc_build"
    mkdir -p "${_PROTOC_BUILD_DIR}"
    "${CMAKE_CMD}" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER="${CC}" \
        -DCMAKE_CXX_COMPILER="${CXX}" \
        -Dprotobuf_BUILD_TESTS=OFF \
        -Dprotobuf_BUILD_SHARED_LIBS=OFF \
        -S "${DORIS_THIRDPARTY}/src/protobuf-21.11/cmake" \
        -B "${_PROTOC_BUILD_DIR}"
    "${CMAKE_CMD}" --build "${_PROTOC_BUILD_DIR}" --target protoc -j "${PARALLEL}"
    if [[ -f "${_PROTOC_BUILD_DIR}/protoc" ]]; then
        mkdir -p "$(dirname "${_PROTOC_BIN}")"
        # cp not ln -sf: see thrift fallback block below for rationale (CI
        # tarballs thirdparty/installed/ without _gensrc_tools).
        cp -f "${_PROTOC_BUILD_DIR}/protoc" "${_PROTOC_BIN}"
        echo "Built and installed protoc: ${_PROTOC_BIN} (from ${_PROTOC_BUILD_DIR}/protoc)"
    else
        echo "ERROR: Failed to build protoc from source" >&2
        exit 1
    fi
fi

if [[ ! -x "${_THRIFT_BIN}" ]]; then
    echo "thrift not found or not executable at ${_THRIFT_BIN}, building from source..."
    _THRIFT_BUILD_DIR="${_GENSRC_TOOLS_DIR}/thrift_build"
    mkdir -p "${_THRIFT_BUILD_DIR}"
    "${CMAKE_CMD}" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER="${CC}" \
        -DCMAKE_CXX_COMPILER="${CXX}" \
        -DBUILD_TESTING=OFF \
        -DBUILD_COMPILER=ON \
        -DBUILD_TUTORIALS=OFF \
        -DBUILD_EXAMPLES=OFF \
        -DBUILD_LIBRARIES=OFF \
        -DWITH_CPP=OFF \
        -DWITH_JAVA=OFF \
        -DWITH_PYTHON=OFF \
        -DWITH_HASKELL=OFF \
        -DWITH_C_GLIB=OFF \
        -S "${DORIS_THIRDPARTY}/src/thrift-0.16.0" \
        -B "${_THRIFT_BUILD_DIR}"
    "${CMAKE_CMD}" --build "${_THRIFT_BUILD_DIR}" --target thrift-compiler -j "${PARALLEL}"
    # thrift compiler binary may end up in different locations
    _THRIFT_BUILT=""
    for _candidate in "${_THRIFT_BUILD_DIR}/bin/thrift" "${_THRIFT_BUILD_DIR}/compiler/cpp/bin/thrift" "${_THRIFT_BUILD_DIR}/compiler/cpp/thrift"; do
        if [[ -f "${_candidate}" ]]; then
            _THRIFT_BUILT="${_candidate}"
            break
        fi
    done
    if [[ -n "${_THRIFT_BUILT}" ]]; then
        mkdir -p "$(dirname "${_THRIFT_BIN}")"
        # cp instead of ln -sf: CI tars thirdparty/installed/ and ships it to the
        # regression cluster, where _gensrc_tools/ isn't present — a symlink
        # would be dangling at runtime. Use a real file copy so the binary
        # survives the tarball round-trip.
        cp -f "${_THRIFT_BUILT}" "${_THRIFT_BIN}"
        echo "Built and installed thrift: ${_THRIFT_BIN} (from ${_THRIFT_BUILT})"
    else
        echo "ERROR: Failed to build thrift compiler from source" >&2
        exit 1
    fi
fi

# Pre-build ragel if not available (needed by hyperscan during main build)
_RAGEL_BIN="${DORIS_THIRDPARTY}/installed/bin/ragel"
if [[ ! -x "${_RAGEL_BIN}" ]]; then
    echo "ragel not found at ${_RAGEL_BIN}, building from source..."
    _RAGEL_BUILD_DIR="${_GENSRC_TOOLS_DIR}/ragel_build"
    _RAGEL_SRC="${DORIS_THIRDPARTY}/src/ragel-6.10"
    if [[ -d "${_RAGEL_SRC}" ]]; then
        mkdir -p "${_RAGEL_BUILD_DIR}"
        cd "${_RAGEL_BUILD_DIR}"
        "${_RAGEL_SRC}/configure" --prefix="${_RAGEL_BUILD_DIR}" 2>/dev/null
        make -j "${PARALLEL}"
        if [[ -f "${_RAGEL_BUILD_DIR}/ragel/ragel" ]]; then
            mkdir -p "$(dirname "${_RAGEL_BIN}")"
            ln -sf "${_RAGEL_BUILD_DIR}/ragel/ragel" "${_RAGEL_BIN}"
            echo "Built and linked ragel: ${_RAGEL_BIN}"
        fi
        cd "${DORIS_HOME}"
    fi
fi

"${DORIS_HOME}"/generated-source.sh noclean

# Assesmble FE modules
FE_MODULES=''
modules=("")
if [[ "${BUILD_FE}" -eq 1 ]]; then
    modules+=("fe-extension-spi")
    modules+=("fe-extension-loader")
    modules+=("fe-core")
    # Filesystem API and SPI plugin modules (loaded at runtime as plugins)
    modules+=("fe-filesystem/fe-filesystem-api")
    modules+=("fe-filesystem/fe-filesystem-spi")
    for _fs_mod in s3 oss cos obs azure hdfs local broker; do
        if [[ -d "${DORIS_HOME}/fe/fe-filesystem/fe-filesystem-${_fs_mod}" ]]; then
            modules+=("fe-filesystem/fe-filesystem-${_fs_mod}")
        fi
    done
    unset _fs_mod
    # Connector API, SPI, and plugin modules (loaded at runtime as plugins)
    modules+=("fe-connector/fe-connector-api")
    modules+=("fe-connector/fe-connector-spi")
    for _conn_mod in es jdbc maxcompute trino hms hive paimon hudi iceberg; do
        if [[ -d "${DORIS_HOME}/fe/fe-connector/fe-connector-${_conn_mod}" ]]; then
            modules+=("fe-connector/fe-connector-${_conn_mod}")
        fi
    done
    unset _conn_mod
    if [[ "${WITH_TDE_DIR}" != "" ]]; then
        modules+=("fe-${WITH_TDE_DIR}")
    fi
fi
if [[ "${BUILD_HIVE_UDF}" -eq 1 ]]; then
    modules+=("hive-udf")
fi
if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 ]]; then
    modules+=("be-java-extensions/iceberg-metadata-scanner")
    modules+=("be-java-extensions/hadoop-hudi-scanner")
    modules+=("be-java-extensions/java-common")
    modules+=("be-java-extensions/java-udf")
    modules+=("be-java-extensions/jdbc-scanner")
    modules+=("be-java-extensions/paimon-scanner")
    modules+=("be-java-extensions/trino-connector-scanner")
    modules+=("be-java-extensions/max-compute-connector")
    modules+=("be-java-extensions/avro-scanner")
    # lakesoul-scanner has been deprecated
    # modules+=("be-java-extensions/lakesoul-scanner")
    modules+=("be-java-extensions/preload-extensions")
    modules+=("be-java-extensions/${HADOOP_DEPS_NAME}")
    modules+=("be-java-extensions/java-writer")

    # If the BE_EXTENSION_IGNORE variable is not empty, remove the modules that need to be ignored from FE_MODULES
    if [[ -n "${BE_EXTENSION_IGNORE}" ]]; then
        IFS=',' read -r -a ignore_modules <<<"${BE_EXTENSION_IGNORE}"
        for module in "${ignore_modules[@]}"; do
            modules=("${modules[@]/be-java-extensions\/${module}/}")
        done
    fi
fi
FE_MODULES="$(
    IFS=','
    echo "${modules[*]}"
)"

# Clean and build Backend
if [[ "${BUILD_BE}" -eq 1 ]]; then
    update_submodule "contrib/apache-orc" "apache-orc" "https://github.com/apache/doris-thirdparty/archive/refs/heads/orc.tar.gz"
    update_submodule "contrib/clucene" "clucene" "https://github.com/apache/doris-thirdparty/archive/refs/heads/clucene.tar.gz"
    update_submodule "contrib/openblas" "openblas" "https://github.com/apache/doris-thirdparty/archive/refs/heads/openblas.tar.gz"
    update_submodule "contrib/faiss" "faiss" "https://github.com/apache/doris-thirdparty/archive/refs/heads/faiss.tar.gz"
    if [[ -e "${DORIS_HOME}/gensrc/build/gen_cpp/version.h" ]]; then
        rm -f "${DORIS_HOME}/gensrc/build/gen_cpp/version.h"
    fi
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    if [[ "${MAKE_TEST}" == 'ON' ]]; then
        CMAKE_BUILD_DIR="${DORIS_HOME}/be/ut_build_${CMAKE_BUILD_TYPE}"
        echo "Build UT: ON (build dir: ${CMAKE_BUILD_DIR})"
    else
        CMAKE_BUILD_DIR="${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}"
    fi
    if [[ "${CLEAN}" -eq 1 ]]; then
        clean_be
    fi

    # Ensure thirdparty is available for the selected mode.
    # Source mode (default, also forced by sanitizers): needs thirdparty/src/
    # populated by download-thirdparty.sh. Auto-trigger if empty so a fresh
    # `git clone && ./build.sh --be` just works.
    # Prebuilt mode (--use-prebuilt): needs thirdparty/installed/lib*/. Print
    # instructions and exit if missing — we don't auto-download the prebuilt
    # tarball because it's a release artifact and the user might want to pin
    # to a specific version.
    if [[ "${USE_CONTRIB_SOURCE_OVERRIDE}" != "OFF" ]]; then
        _tp_src_count=0
        if [[ -d "${DORIS_HOME}/thirdparty/src" ]]; then
            _tp_src_count=$(find "${DORIS_HOME}/thirdparty/src" -maxdepth 1 -mindepth 1 | wc -l)
        fi
        if [[ "${_tp_src_count}" -lt 10 ]]; then
            _CI_TP_SRC="/var/local/thirdparty/src"
            _ci_src_count=0
            if [[ -d "${_CI_TP_SRC}" ]]; then
                _ci_src_count=$(find "${_CI_TP_SRC}" -maxdepth 1 -mindepth 1 | wc -l)
            fi
            if [[ "${_ci_src_count}" -ge 10 ]]; then
                echo "[build.sh] using ${_ci_src_count} staged thirdparty source entries from ${_CI_TP_SRC}"
                mkdir -p "${DORIS_HOME}/thirdparty/src"
                for _ci_entry in "${_CI_TP_SRC}"/*; do
                    _ci_name=$(basename "${_ci_entry}")
                    if [[ ! -e "${DORIS_HOME}/thirdparty/src/${_ci_name}" ]]; then
                        ln -sfn "${_ci_entry}" "${DORIS_HOME}/thirdparty/src/${_ci_name}"
                    fi
                done
            else
                echo "[build.sh] thirdparty/src appears empty (${_tp_src_count} entries);"
                echo "[build.sh] running download-thirdparty.sh to fetch sources..."
                bash "${DORIS_HOME}/thirdparty/download-thirdparty.sh"
            fi
        fi
    else
        if [[ ! -d "${DORIS_HOME}/thirdparty/installed/lib" ]] \
           && [[ ! -d "${DORIS_HOME}/thirdparty/installed/lib64" ]]; then
            # Auto-download + extract official prebuilt tarball.
            # DORIS_PREBUILT_VERSION env var picks the release tag (master|2.1|3.0|3.1).
            _prebuilt_version="${DORIS_PREBUILT_VERSION:-master}"
            echo "[build.sh] thirdparty/installed/ is empty;"
            echo "[build.sh] auto-downloading prebuilt thirdparty (version=${_prebuilt_version})..."
            bash "${DORIS_HOME}/thirdparty/download-prebuild-thirdparty.sh" "${_prebuilt_version}"
            _prebuilt_tar=$(ls "${DORIS_HOME}/thirdparty"/doris-thirdparty-prebuilt-*.tar.xz 2>/dev/null | head -1)
            if [[ -z "${_prebuilt_tar}" ]] \
               || [[ ! -f "${_prebuilt_tar}" ]]; then
                echo "[build.sh] Prebuilt tarball not found after download."
                echo "[build.sh] Set DORIS_PREBUILT_VERSION (master|2.1|3.0|3.1) or run"
                echo "[build.sh]   bash thirdparty/build-thirdparty.sh"
                echo "[build.sh] to build prebuilt from source instead (slow, 1-2 hours)."
                exit 1
            fi
            echo "[build.sh] extracting ${_prebuilt_tar} → thirdparty/..."
            tar -Jxf "${_prebuilt_tar}" -C "${DORIS_HOME}/thirdparty/"
        fi
    fi

    MAKE_PROGRAM="$(command -v "${BUILD_SYSTEM}")"

    if [[ -z "${BUILD_FS_BENCHMARK}" ]]; then
        BUILD_FS_BENCHMARK=OFF
    fi

    if [[ -z "${BUILD_TASK_EXECUTOR_SIMULATOR}" ]]; then
        BUILD_TASK_EXECUTOR_SIMULATOR=OFF
    fi

    if [[ -z "${BUILD_FILE_CACHE_LRU_TOOL}" ]]; then
        BUILD_FILE_CACHE_LRU_TOOL=OFF
    fi

    echo "-- Make program: ${MAKE_PROGRAM}"
    echo "-- Use ccache: ${CMAKE_USE_CCACHE_CXX} and ${CMAKE_USE_CCACHE_C}"
    echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"
    echo "-- Build fs benchmark tool: ${BUILD_FS_BENCHMARK}"
    echo "-- Build task executor simulator: ${BUILD_TASK_EXECUTOR_SIMULATOR}"
    echo "-- Build file cache lru tool: ${BUILD_FILE_CACHE_LRU_TOOL}"

    mkdir -p "${CMAKE_BUILD_DIR}"
    cd "${CMAKE_BUILD_DIR}"
    "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
        -DCMAKE_C_COMPILER="${CC}" \
        -DCMAKE_CXX_COMPILER="${CXX}" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DENABLE_INJECTION_POINT="${ENABLE_INJECTION_POINT}" \
        -DMAKE_TEST="${MAKE_TEST}" \
        -DBUILD_BENCHMARK="${BUILD_BENCHMARK}" \
        -DBUILD_FS_BENCHMARK="${BUILD_FS_BENCHMARK}" \
        -DBUILD_TASK_EXECUTOR_SIMULATOR="${BUILD_TASK_EXECUTOR_SIMULATOR}" \
        -DBUILD_FILE_CACHE_LRU_TOOL="${BUILD_FILE_CACHE_LRU_TOOL}" \
        ${CMAKE_USE_CCACHE_CXX:+${CMAKE_USE_CCACHE_CXX}} \
        ${CMAKE_USE_CCACHE_C:+${CMAKE_USE_CCACHE_C}} \
        -DUBSAN_IGNORELIST="${UBSAN_IGNORELIST}" \
        -DUSE_LIBCPP="${USE_LIBCPP}" \
        -DBUILD_META_TOOL="${BUILD_META_TOOL}" \
        -DBUILD_FILE_CACHE_MICROBENCH_TOOL="${BUILD_FILE_CACHE_MICROBENCH_TOOL}" \
        -DBUILD_INDEX_TOOL="${BUILD_INDEX_TOOL}" \
        -DSTRIP_DEBUG_INFO="${STRIP_DEBUG_INFO}" \
        -DUSE_UNWIND="${USE_UNWIND}" \
        -DDISPLAY_BUILD_TIME="${DISPLAY_BUILD_TIME}" \
        -DENABLE_PCH="${ENABLE_PCH}" \
        -DUSE_JEMALLOC="${USE_JEMALLOC}" \
        ${USE_CONTRIB_SOURCE_OVERRIDE:+-DUSE_CONTRIB_SOURCE=${USE_CONTRIB_SOURCE_OVERRIDE}} \
        -DUSE_AVX2="${USE_AVX2}" \
        -DARM_MARCH="${ARM_MARCH}" \
        -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
        -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
        -DENABLE_CLANG_COVERAGE="${DENABLE_CLANG_COVERAGE}" \
        -DDORIS_JAVA_HOME="${JAVA_HOME}" \
        -DBUILD_AZURE="${BUILD_AZURE}" \
        -DENABLE_DYNAMIC_ARCH="${ENABLE_DYNAMIC_ARCH}" \
        -DWITH_TDE_DIR="${WITH_TDE_DIR}" \
        -DFAISS_ENABLE_GPU="${FAISS_ENABLE_GPU:-OFF}" \
        "${DORIS_HOME}/be"

    if [[ "${OUTPUT_BE_BINARY}" -eq 1 ]]; then
        # Fix bare library names injected by AWS SDK CMake internals
        if [[ -f build.ninja ]]; then
            sed -i 's| -lcurl | bin/libcurl.a |g' build.ninja
            sed -i 's| -llibcurl_static | bin/libcurl.a |g' build.ninja
            sed -i 's| -lcares | thirdparty/cares/lib64/libcares.a |g' build.ninja
        fi
        "${BUILD_SYSTEM}" -j "${PARALLEL}"
        "${BUILD_SYSTEM}" install
    fi

    cd "${DORIS_HOME}"
fi

# Populate installed/ with symlinks to source-built libraries for cloud module
if [[ "${BUILD_CLOUD}" -eq 1 ]]; then
    _BE_BUILD_DIR="${DORIS_HOME}/be/build_${BUILD_TYPE:-Release}"
    _TP_INSTALLED="${DORIS_THIRDPARTY}/installed"
    _TP_LIB="${_TP_INSTALLED}/lib"
    _TP_LIB64="${_TP_INSTALLED}/lib64"
    _TP_INC="${_TP_INSTALLED}/include"
    mkdir -p "${_TP_LIB}" "${_TP_LIB64}" "${_TP_INC}"

    # Helper: create symlink only if target exists
    _link() { [[ -f "$1" ]] && ln -sf "$1" "$2" 2>/dev/null || true; }
    # Helper: find .a file in lib/ or bin/ and link to dest
    _find_link() {
        local name="$1" dest="$2"
        local f="${_BE_BUILD_DIR}/lib/${name}"
        [[ -f "${f}" ]] || f="${_BE_BUILD_DIR}/bin/${name}"
        [[ -f "${f}" ]] && ln -sf "${f}" "${dest}" 2>/dev/null || true
    }

    # Bulk symlink all .a from BE build lib/ and bin/ dirs
    for _f in "${_BE_BUILD_DIR}"/lib/*.a "${_BE_BUILD_DIR}"/bin/*.a; do
        [[ -f "${_f}" ]] || continue
        _bn=$(basename "${_f}")
        _norm=$(echo "${_bn}" | sed 's/^lib_/lib/')
        ln -sf "${_f}" "${_TP_LIB}/${_norm}" 2>/dev/null || true
        ln -sf "${_f}" "${_TP_LIB64}/${_norm}" 2>/dev/null || true
    done

    # Subdirectory libraries
    for _f in "${_BE_BUILD_DIR}"/thirdparty/cares/lib64/*.a \
              "${_BE_BUILD_DIR}"/thirdparty/libevent/lib/*.a \
              "${_BE_BUILD_DIR}"/thirdparty/krb5/lib/*.a \
              "${_BE_BUILD_DIR}"/thirdparty/cyrus-sasl/lib/.libs/*.a \
              "${_BE_BUILD_DIR}"/thirdparty/hadoop_hdfs/*.a; do
        [[ -f "${_f}" ]] || continue
        _bn=$(basename "${_f}")
        ln -sf "${_f}" "${_TP_LIB}/${_bn}" 2>/dev/null || true
        ln -sf "${_f}" "${_TP_LIB64}/${_bn}" 2>/dev/null || true
    done

    # Special name mappings for cloud module
    _find_link "lib_jemalloc.a" "${_TP_LIB}/libjemalloc_doris.a"
    _find_link "lib_ssl.a" "${_TP_LIB}/libssl.a"
    _find_link "lib_crypto.a" "${_TP_LIB}/libcrypto.a"
    _find_link "lib_xml2.a" "${_TP_LIB64}/libxml2.a"
    _find_link "lib_lzma.a" "${_TP_LIB64}/liblzma.a"
    _find_link "lib_idn.a" "${_TP_LIB64}/libidn.a"
    _find_link "lib_gsasl.a" "${_TP_LIB}/libgsasl.a"
    _find_link "libs2n.a" "${_TP_LIB}/libs2n.a"
    _link "${_BE_BUILD_DIR}/thirdparty/cyrus-sasl/lib/.libs/libsasl2.a" "${_TP_LIB}/libsasl2.a"
    # hadoop_hdfs — cloud expects installed/lib/hadoop_hdfs/native/libhdfs.a
    mkdir -p "${_TP_LIB}/hadoop_hdfs/native"
    _link "${_BE_BUILD_DIR}/thirdparty/hadoop_hdfs/libhdfs.a" "${_TP_LIB}/hadoop_hdfs/native/libhdfs.a"

    # gperftools — cloud expects installed/gperftools/lib/
    _GPERF="${_TP_INSTALLED}/gperftools"
    mkdir -p "${_GPERF}/lib" "${_GPERF}/include"
    # lib_pprof.a may be in lib/ or bin/ depending on build
    _pprof_a="${_BE_BUILD_DIR}/lib/lib_pprof.a"
    [[ -f "${_pprof_a}" ]] || _pprof_a="${_BE_BUILD_DIR}/bin/lib_pprof.a"
    _link "${_pprof_a}" "${_GPERF}/lib/libprofiler.a"
    _link "${_pprof_a}" "${_GPERF}/lib/libtcmalloc.a"
    [[ -d "${DORIS_THIRDPARTY}/src/gperftools-2.10/src/gperftools" ]] && \
        cp -rn "${DORIS_THIRDPARTY}/src/gperftools-2.10/src/gperftools" "${_GPERF}/include/" 2>/dev/null || true

    # Populate installed/include with headers from source tree for cloud module
    _TP_SRC="${DORIS_THIRDPARTY}/src"
    # Direct include dir symlinks: src/xxx/include/yyy -> installed/include/yyy
    for _pair in \
        "glog-0.6.0/src:glog" \
        "gflags-2.2.2/include:gflags" \
        "protobuf-21.11/src:google" \
        "brpc-1.4.0/src:brpc" \
        "brpc-1.4.0/src:butil" \
        "brpc-1.4.0/src:bthread" \
        "brpc-1.4.0/src:bvar" \
        "brpc-1.4.0/src:braft" \
        "rocksdb-5.14.2/include:rocksdb" \
        "fmt-7.1.3/include:fmt" \
        "leveldb-1.23/include:leveldb" \
        "curl-8.2.1/include:curl" \
        "googletest-release-1.12.1/googletest/include:gtest" \
        "googletest-release-1.12.1/googlemock/include:gmock" \
        "aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-core/include:aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-common/include:aws" \
        "aliyun-openapi-cpp-sdk-1.36.1586/core/include:alibabacloud" \
        "jsoncpp-1.9.5/include:json" \
        "jemalloc-5.3.0/include:jemalloc" \
        "doris-thirdparty-hadoop-3.3.6.6-for-doris/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include/hdfs:hadoop_hdfs" \
    ; do
        _src_rel="${_pair%%:*}"
        _dest="${_pair##*:}"
        _src_full="${_TP_SRC}/${_src_rel}/${_dest}"
        [[ -e "${_src_full}" ]] && ln -sfn "${_src_full}" "${_TP_INC}/${_dest}" 2>/dev/null || true
    done
    # AWS SDK has scattered include dirs — merge them
    for _aws_sub in \
        "aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-s3/include/aws" \
        "aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-s3-crt/include/aws" \
        "aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-transfer/include/aws" \
        "aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-identity-management/include/aws" \
        "aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-sts/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-io/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-cal/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-auth/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-http/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-s3/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-sdkutils/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-checksums/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-event-stream/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-compression/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-mqtt/include/aws" \
        "aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/s2n/api" \
    ; do
        _full="${_TP_SRC}/${_aws_sub}"
        [[ -d "${_full}" ]] && cp -rsn "${_full}/"* "${_TP_INC}/aws/" 2>/dev/null || true
    done
    # Build-generated headers
    ln -sfn "${_BE_BUILD_DIR}/thirdparty/gflags/include/gflags" "${_TP_INC}/gflags" 2>/dev/null || true
    ln -sfn "${_BE_BUILD_DIR}/thirdparty/glog" "${_TP_INC}/glog_gen" 2>/dev/null || true
    # brpc proto-generated headers (hand-written build: at thirdparty/brpc/{brpc,butil,...}/*.pb.h)
    # Source headers are already symlinked above from brpc-1.4.0/src/{brpc,butil,bthread,bvar}
    # Only need to add generated .pb.h files on top
    mkdir -p "${_TP_INC}/brpc" "${_TP_INC}/brpc/policy"
    cp -f "${_BE_BUILD_DIR}/thirdparty/brpc/brpc/"*.pb.h "${_TP_INC}/brpc/" 2>/dev/null || true
    cp -f "${_BE_BUILD_DIR}/thirdparty/brpc/brpc/policy/"*.pb.h "${_TP_INC}/brpc/policy/" 2>/dev/null || true
    cp -f "${_BE_BUILD_DIR}/thirdparty/brpc/"*.pb.h "${_TP_INC}/" 2>/dev/null || true
    # thrift config.h (hand-written build: at thirdparty/thrift_gen/thrift/config.h)
    mkdir -p "${_TP_INC}/thrift"
    cp -f "${_BE_BUILD_DIR}/thirdparty/thrift_gen/thrift/config.h" "${_TP_INC}/thrift/" 2>/dev/null || true
    # Azure SDK headers
    for _az in core identity storage-blobs storage-common; do
        _az_inc="${_TP_SRC}/azure-sdk-for-cpp-azure-core_1.16.0/sdk"
        case ${_az} in
            core) _az_path="${_az_inc}/core/azure-core/inc/azure" ;;
            identity) _az_path="${_az_inc}/identity/azure-identity/inc/azure" ;;
            storage-blobs) _az_path="${_az_inc}/storage/azure-storage-blobs/inc/azure" ;;
            storage-common) _az_path="${_az_inc}/storage/azure-storage-common/inc/azure" ;;
        esac
        [[ -d "${_az_path}" ]] && cp -rsn "${_az_path}/"* "${_TP_INC}/azure/" 2>/dev/null || true
    done
    mkdir -p "${_TP_INC}/azure"
    # openssl headers
    ln -sfn "${_TP_SRC}/openssl-OpenSSL_1_1_1s/include/openssl" "${_TP_INC}/openssl" 2>/dev/null || true
    # uuid
    mkdir -p "${_TP_INC}/uuid"
    cp -n "${_TP_SRC}/libuuid-1.0.3/uuid.h" "${_TP_INC}/uuid/" 2>/dev/null || true
    # smithy (AWS SDK)
    cp -rsn "${_TP_SRC}/aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-core/include/smithy" "${_TP_INC}/" 2>/dev/null || true
    # rapidjson
    ln -sfn "${_TP_SRC}/rapidjson-232389d4f1012dddec4ef84861face2d2ba85709/include/rapidjson" "${_TP_INC}/rapidjson" 2>/dev/null || true
    # hadoop_hdfs (without version suffix, for cloud)
    mkdir -p "${_TP_INC}/hadoop_hdfs"
    cp -n "${_TP_SRC}/doris-thirdparty-hadoop-3.3.6.6-for-doris/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include/hdfs/hdfs.h" "${_TP_INC}/hadoop_hdfs/" 2>/dev/null || true
    # mysql headers from BE build
    ln -sfn "${_BE_BUILD_DIR}/thirdparty/mysql_headers/include/mysql" "${_TP_INC}/mysql" 2>/dev/null || true
    # Build-generated headers: glog, gflags, aws config.h
    mkdir -p "${_TP_INC}/glog"
    cp -rsn "${_TP_SRC}/glog-0.6.0/src/glog/"* "${_TP_INC}/glog/" 2>/dev/null || true
    cp -f "${_BE_BUILD_DIR}/thirdparty/glog/glog/"*.h "${_TP_INC}/glog/" 2>/dev/null || true
    cp -f "${_BE_BUILD_DIR}/thirdparty/gflags/include/gflags/"*.h "${_TP_INC}/gflags/" 2>/dev/null || true
    cp -f "${_BE_BUILD_DIR}/thirdparty/aws-sdk-gen/aws-c-common/generated/include/aws/common/config.h" "${_TP_INC}/aws/common/" 2>/dev/null || true
    # fdb_c (FoundationDB) - pre-built .so
    _link "${DORIS_THIRDPARTY}/installed/lib64/libfdb_c.so" "${_TP_LIB}/libfdb_c.so"

    # Build additional thirdparty targets needed by cloud but not by BE
    echo "Building additional thirdparty targets for cloud module..."
    cd "${_BE_BUILD_DIR}"
    "${CMAKE_CMD}" --build . --target jsoncpp_static -- -j "${PARALLEL}" 2>/dev/null || true
    # Symlink jsoncpp (output may be in lib/ or bin/)
    _find_link "libjsoncpp.a" "${_TP_LIB64}/libjsoncpp.a"
    _find_link "libjsoncpp.a" "${_TP_LIB}/libjsoncpp.a"
    # uuid — cloud needs libuuid
    _find_link "lib_libuuid.a" "${_TP_LIB64}/libuuid.a"
    _find_link "lib_libuuid.a" "${_TP_LIB}/libuuid.a"
    # ali-sdk — build from BE thirdparty (core only, added via add_subdirectory in ali_sdk.cmake)
    "${CMAKE_CMD}" --build "${_BE_BUILD_DIR}" --target core -- -j "${PARALLEL}" 2>/dev/null || true
    find "${_BE_BUILD_DIR}" -name "libalibabacloud-sdk-core.a" -exec ln -sf {} "${_TP_LIB64}/libalibabacloud-sdk-core.a" \; 2>/dev/null
    find "${_BE_BUILD_DIR}" -name "libalibabacloud-sdk-core.a" -exec ln -sf {} "${_TP_LIB}/libalibabacloud-sdk-core.a" \; 2>/dev/null
    # Re-run bulk symlink after extra builds to catch any new .a files
    for _f in "${_BE_BUILD_DIR}"/lib/*.a; do
        [[ -f "${_f}" ]] || continue
        _bn=$(basename "${_f}")
        _norm=$(echo "${_bn}" | sed 's/^lib_/lib/')
        ln -sf "${_f}" "${_TP_LIB}/${_norm}" 2>/dev/null || true
        ln -sf "${_f}" "${_TP_LIB64}/${_norm}" 2>/dev/null || true
    done

    cd "${DORIS_HOME}"
    echo "Symlinked source-built libraries to ${_TP_INSTALLED} for cloud module"
fi

# Clean and build cloud
if [[ "${BUILD_CLOUD}" -eq 1 ]]; then
    if [[ -e "${DORIS_HOME}/gensrc/build/gen_cpp/cloud_version.h" ]]; then
        rm -f "${DORIS_HOME}/gensrc/build/gen_cpp/cloud_version.h"
    fi
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    echo "Build Cloud: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR="${DORIS_HOME}/cloud/build_${CMAKE_BUILD_TYPE}"
    if [[ "${CLEAN}" -eq 1 ]]; then
        rm -rf "${CMAKE_BUILD_DIR}"
        echo "clean cloud"
    fi
    MAKE_PROGRAM="$(command -v "${BUILD_SYSTEM}")"
    echo "-- Make program: ${MAKE_PROGRAM}"
    echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"
    mkdir -p "${CMAKE_BUILD_DIR}"
    cd "${CMAKE_BUILD_DIR}"
    "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DENABLE_INJECTION_POINT="${ENABLE_INJECTION_POINT}" \
        -DMAKE_TEST=OFF \
        "${CMAKE_USE_CCACHE}" \
        -DUSE_LIBCPP="${USE_LIBCPP}" \
        -DENABLE_HDFS_STORAGE_VAULT=${ENABLE_HDFS_STORAGE_VAULT:-ON} \
        -DSTRIP_DEBUG_INFO="${STRIP_DEBUG_INFO}" \
        -DUSE_JEMALLOC="${USE_JEMALLOC}" \
        -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
        -DBUILD_AZURE="${BUILD_AZURE}" \
        -DBUILD_CHECK_META="${BUILD_CHECK_META:-OFF}" \
        -DENABLE_DYNAMIC_ARCH="${ENABLE_DYNAMIC_ARCH}" \
        ${USE_CONTRIB_SOURCE_OVERRIDE:+-DUSE_CONTRIB_SOURCE=${USE_CONTRIB_SOURCE_OVERRIDE}} \
        "${DORIS_HOME}/cloud/"
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    cd "${DORIS_HOME}"
    echo "Build cloud done"
fi

function build_ui() {
    NPM='npm'
    if ! ${NPM} --version; then
        echo "Error: npm is not found"
        exit 1
    fi
    if [[ -n "${CUSTOM_NPM_REGISTRY}" ]]; then
        "${NPM}" config set registry "${CUSTOM_NPM_REGISTRY}"
        npm_reg="$("${NPM}" get registry)"
        echo "NPM registry: ${npm_reg}"
    fi

    echo "Build Frontend UI"
    ui_dist="${DORIS_HOME}/ui/dist"
    if [[ -n "${CUSTOM_UI_DIST}" ]]; then
        ui_dist="${CUSTOM_UI_DIST}"
    else
        cd "${DORIS_HOME}/ui"
        "${NPM}" cache clean --force
        "${NPM}" install --legacy-peer-deps
        "${NPM}" run build
    fi
    echo "ui dist: ${ui_dist}"
    rm -rf "${DORIS_HOME}/fe/fe-core/src/main/resources/static"
    mkdir -p "${DORIS_HOME}/fe/fe-core/src/main/resources/static"
    cp -r "${ui_dist}"/* "${DORIS_HOME}/fe/fe-core/src/main/resources/static"/
}

function build_fe_modules() {
    local thread_count="${FE_MAVEN_THREADS:-1C}"
    local retry_thread_count="${FE_MAVEN_RETRY_THREADS:-1}"
    local log_file
    local -a dependency_mvn_opts=()
    local -a extra_mvn_opts=()
    local -a user_settings_opts=()
    local -a mvn_cmd=(
        "${MVN_CMD}"
        package
        -pl
        "${FE_MODULES}"
        -am
        -Dskip.doc=true
        -DskipTests
    )

    if [[ "${DISABLE_JAVA_CHECK_STYLE}" = "ON" ]]; then
        mvn_cmd+=("-Dcheckstyle.skip=true")
    fi
    if [[ -n "${MVN_OPT}" ]]; then
        # shellcheck disable=SC2206
        extra_mvn_opts=(${MVN_OPT})
    fi
    if [[ "${BUILD_OBS_DEPENDENCIES}" -eq 0 ]]; then
        dependency_mvn_opts+=("-Dobs.dependency.scope=provided")
    fi
    if [[ "${BUILD_COS_DEPENDENCIES}" -eq 0 ]]; then
        dependency_mvn_opts+=("-Dcos.dependency.scope=provided")
    fi
    if [[ -n "${USER_SETTINGS_MVN_REPO}" && -f "${USER_SETTINGS_MVN_REPO}" ]]; then
        user_settings_opts=(-gs "${USER_SETTINGS_MVN_REPO}")
    fi

    mvn_cmd+=("${extra_mvn_opts[@]}" "${dependency_mvn_opts[@]}" "${user_settings_opts[@]}" -T "${thread_count}")
    log_file="$(mktemp)"
    if "${mvn_cmd[@]}" 2>&1 | tee "${log_file}"; then
        rm -f "${log_file}"
        return 0
    fi
    if [[ "${thread_count}" != "${retry_thread_count}" ]] && \
            grep -Eq "Could not acquire lock\(s\)|isn't a file" "${log_file}"; then
        echo "FE Maven build hit parallel build issue (lock contention or reactor artifact race). Retrying with -T ${retry_thread_count}."
        mvn_cmd=("${mvn_cmd[@]:0:${#mvn_cmd[@]}-2}" -T "${retry_thread_count}")
        "${mvn_cmd[@]}"
        rm -f "${log_file}"
        return 0
    fi
    rm -f "${log_file}"
    return 1
}

# FE UI must be built before building FE
if [[ "${BUILD_FE}" -eq 1 ]]; then
    if [[ "${BUILD_UI}" -eq 1 ]]; then
        build_ui
    fi
fi

# Clean and build Frontend
if [[ "${FE_MODULES}" != '' ]]; then
    echo "Build Frontend Modules: ${FE_MODULES}"
    cd "${DORIS_HOME}/fe"
    if [[ "${CLEAN}" -eq 1 ]]; then
        clean_fe
    fi
    build_fe_modules
    cd "${DORIS_HOME}"
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_OUTPUT:="${DORIS_HOME}/output/"}
echo "OUTPUT DIR=${DORIS_OUTPUT}"
mkdir -p "${DORIS_OUTPUT}"

# Copy Frontend and Backend
if [[ "${BUILD_FE}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/fe/bin" "${DORIS_OUTPUT}/fe/conf" \
        "${DORIS_OUTPUT}/fe/webroot" "${DORIS_OUTPUT}/fe/lib"

    cp -r -p "${DORIS_HOME}/bin"/*_fe.sh "${DORIS_OUTPUT}/fe/bin"/
    cp -r -p "${DORIS_HOME}/conf/fe.conf" "${DORIS_OUTPUT}/fe/conf"/
    cp -r -p "${DORIS_HOME}/conf/ldap.conf" "${DORIS_OUTPUT}/fe/conf"/
    cp -r -p "${DORIS_HOME}/conf/mysql_ssl_default_certificate" "${DORIS_OUTPUT}/fe/"/
    rm -rf "${DORIS_OUTPUT}/fe/lib"/*
    unzip -q -o "${DORIS_HOME}/fe/fe-core/target/doris-fe-lib.zip" -d "${DORIS_OUTPUT}/fe/lib"
    cp -r -p "${DORIS_HOME}/fe/fe-core/target/doris-fe.jar" "${DORIS_OUTPUT}/fe/lib"/
    if [[ "${WITH_TDE_DIR}" != "" ]]; then
        cp -r -p "${DORIS_HOME}/fe/fe-${WITH_TDE_DIR}/target/fe-${WITH_TDE_DIR}-1.2-SNAPSHOT.jar" "${DORIS_OUTPUT}/fe/lib"/
    fi

    #cp -r -p "${DORIS_HOME}/docs/build/help-resource.zip" "${DORIS_OUTPUT}/fe/lib"/

    # Third-party filesystem jars (JuiceFS, JindoFS) are packaged by post-build.sh
    "${DORIS_HOME}/post-build.sh" --fe --output "${DORIS_OUTPUT}"

    cp -r -p "${DORIS_HOME}/minidump" "${DORIS_OUTPUT}/fe"/
    cp -r -p "${DORIS_HOME}/webroot/static" "${DORIS_OUTPUT}/fe/webroot"/

    if [[ -d "${DORIS_THIRDPARTY}/installed/webroot" ]]; then
        cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/fe/webroot/static"/
    fi
    copy_common_files "${DORIS_OUTPUT}/fe/"
    mkdir -p "${DORIS_OUTPUT}/fe/log"
    mkdir -p "${DORIS_OUTPUT}/fe/doris-meta"
    mkdir -p "${DORIS_OUTPUT}/fe/conf/ssl"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/jdbc_drivers/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/java_udf/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/connectors/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/hadoop_conf/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/java_extensions/"

    # Deploy filesystem provider plugins as independent plugin directories
    # Each sub-directory is one storage backend loaded at runtime by FileSystemPluginManager.
    FS_PLUGIN_DIR="${DORIS_OUTPUT}/fe/plugins/filesystem"
    for fs_module in s3 azure oss cos obs hdfs local broker; do
        fs_plugin_target="${FS_PLUGIN_DIR}/${fs_module}"
        fs_module_dir="${DORIS_HOME}/fe/fe-filesystem/fe-filesystem-${fs_module}"
        if [ ! -d "${fs_module_dir}" ]; then
            continue
        fi
        mkdir -p "${fs_plugin_target}"
        # Unpack the self-contained plugin zip produced by maven-assembly-plugin.
        # Layout inside the zip: <plugin>.jar at root + lib/*.jar for runtime deps.
        # DirectoryPluginRuntimeManager picks up both root and lib/ jars automatically.
        unzip -o "${fs_module_dir}/target/doris-fe-filesystem-${fs_module}.zip" \
            -d "${fs_plugin_target}/"
    done
    unset FS_PLUGIN_DIR fs_module fs_plugin_target fs_module_dir

    # Deploy connector provider plugins as independent plugin directories.
    # Each sub-directory is one connector backend loaded at runtime by ConnectorPluginManager.
    CONN_PLUGIN_DIR="${DORIS_OUTPUT}/fe/plugins/connector"
    for conn_module in es jdbc maxcompute trino hms hive paimon hudi iceberg; do
        conn_plugin_target="${CONN_PLUGIN_DIR}/${conn_module}"
        conn_module_dir="${DORIS_HOME}/fe/fe-connector/fe-connector-${conn_module}"
        if [ ! -d "${conn_module_dir}" ]; then
            continue
        fi
        conn_zip=$(find "${conn_module_dir}/target" -maxdepth 1 -name '*.zip' 2>/dev/null | head -1)
        if [ -z "${conn_zip}" ]; then
            continue
        fi
        mkdir -p "${conn_plugin_target}"
        unzip -o "${conn_zip}" -d "${conn_plugin_target}/"
    done
    unset CONN_PLUGIN_DIR conn_module conn_plugin_target conn_module_dir conn_zip

    if [ "${TARGET_SYSTEM}" = "Darwin" ] || [ "${TARGET_SYSTEM}" = "Linux" ]; then
      mkdir -p "${DORIS_OUTPUT}/fe/arthas"
      rm -rf "${DORIS_OUTPUT}/fe/arthas/*"
      unzip -o "${DORIS_OUTPUT}/fe/lib/arthas-packaging-*.jar" arthas-bin.zip -d "${DORIS_OUTPUT}/fe/arthas/"
      unzip -o "${DORIS_OUTPUT}/fe/arthas/arthas-bin.zip" -d "${DORIS_OUTPUT}/fe/arthas/"
      rm "${DORIS_OUTPUT}/fe/arthas/math-game.jar"
      rm "${DORIS_OUTPUT}/fe/arthas/arthas-bin.zip"
    fi
fi

if [[ "${OUTPUT_BE_BINARY}" -eq 1 ]]; then
    # need remove old version hadoop jars if $DORIS_OUTPUT been used multiple times, otherwise will cause jar conflict
    rm -rf "${DORIS_OUTPUT}/be/lib/hadoop_hdfs"
    install -d "${DORIS_OUTPUT}/be/bin" \
        "${DORIS_OUTPUT}/be/conf" \
        "${DORIS_OUTPUT}/be/lib" \
        "${DORIS_OUTPUT}/be/www" \
        "${DORIS_OUTPUT}/be/tools/FlameGraph"

    cp -r -p "${DORIS_HOME}/be/output/bin"/* "${DORIS_OUTPUT}/be/bin"/
    cp -r -p "${DORIS_HOME}/be/output/conf"/* "${DORIS_OUTPUT}/be/conf"/
    cp -r -p "${DORIS_HOME}/be/output/dict" "${DORIS_OUTPUT}/be/"

    if [[ -f "${DORIS_THIRDPARTY}/installed/lib/libz.so" ]]; then
        cp -r -p "${DORIS_THIRDPARTY}/installed/lib/libz.so"* "${DORIS_OUTPUT}/be/lib/"
    fi

    if [[ "${BUILD_BE_JAVA_EXTENSIONS_FALSE_IN_CONF}" -eq 1 ]]; then
        echo -e "\033[33;1mWARNNING: \033[37;1mDisable Java UDF support in be.conf due to the BE was built without Java UDF.\033[0m"
        cat >>"${DORIS_OUTPUT}/be/conf/be.conf" <<EOF

# Java UDF and BE-JAVA-EXTENSION support
enable_java_support = false
EOF
    fi

    # Fix Killed: 9 error on MacOS (arm64).
    # See: https://stackoverflow.com/questions/67378106/mac-m1-cping-binary-over-another-results-in-crash
    if [[ -f "${DORIS_HOME}/be/output/lib/doris_be" ]]; then
        rm -f "${DORIS_OUTPUT}/be/lib/doris_be"
        cp -r -p "${DORIS_HOME}/be/output/lib/doris_be" "${DORIS_OUTPUT}/be/lib"/
    fi
    if [[ -d "${DORIS_HOME}/be/output/lib/doris_be.dSYM" ]]; then
        rm -rf "${DORIS_OUTPUT}/be/lib/doris_be.dSYM"
        cp -r "${DORIS_HOME}/be/output/lib/doris_be.dSYM" "${DORIS_OUTPUT}/be/lib"/
    fi
    if [[ -f "${DORIS_HOME}/be/output/lib/fs_benchmark_tool" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/fs_benchmark_tool" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_META_TOOL}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/meta_tool" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_FILE_CACHE_MICROBENCH_TOOL}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/file_cache_microbench" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_INDEX_TOOL}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/index_tool" "${DORIS_OUTPUT}/be/lib"/
    fi

    cp -r -p "${DORIS_HOME}/webroot/be"/* "${DORIS_OUTPUT}/be/www"/
    cp -r -p "${DORIS_HOME}/tools/FlameGraph"/* "${DORIS_OUTPUT}/be/tools/FlameGraph"/
    if [[ "${STRIP_DEBUG_INFO}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/debug_info" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_BENCHMARK}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/benchmark_test" "${DORIS_OUTPUT}/be/lib/"/
    fi

    if [[ "${BUILD_FS_BENCHMARK}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/bin/run-fs-benchmark.sh" "${DORIS_OUTPUT}/be/bin/"/
    fi

    if [[ "${BUILD_TASK_EXECUTOR_SIMULATOR}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/bin/run-task-executor-simulator.sh" "${DORIS_OUTPUT}/be/bin/"/
        cp -r -p "${DORIS_HOME}/be/output/lib/task_executor_simulator" "${DORIS_OUTPUT}/be/lib/"/
    fi

    extensions_modules=("java-udf")
    extensions_modules+=("jdbc-scanner")
    extensions_modules+=("hadoop-hudi-scanner")
    extensions_modules+=("paimon-scanner")
    extensions_modules+=("trino-connector-scanner")
    extensions_modules+=("max-compute-connector")
    extensions_modules+=("avro-scanner")
    # lakesoul-scanner has been deprecated
    # extensions_modules+=("lakesoul-scanner")
    extensions_modules+=("preload-extensions")
    extensions_modules+=("iceberg-metadata-scanner")
    extensions_modules+=("${HADOOP_DEPS_NAME}")
    extensions_modules+=("java-writer")

    if [[ -n "${BE_EXTENSION_IGNORE}" ]]; then
        IFS=',' read -r -a ignore_modules <<<"${BE_EXTENSION_IGNORE}"
        new_modules=()
        for module in "${extensions_modules[@]}"; do
            module=${module// /}
            if [[ -n "${module}" ]]; then
                ignore=0
                for ignore_module in "${ignore_modules[@]}"; do
                    if [[ "${module}" == "${ignore_module}" ]]; then
                        ignore=1
                        break
                    fi
                done
                if [[ "${ignore}" -eq 0 ]]; then
                    new_modules+=("${module}")
                fi
            fi
        done
        extensions_modules=("${new_modules[@]}")
    fi

    BE_JAVA_EXTENSIONS_DIR="${DORIS_OUTPUT}/be/lib/java_extensions/"
    rm -rf "${BE_JAVA_EXTENSIONS_DIR}"
    mkdir "${BE_JAVA_EXTENSIONS_DIR}"
    for extensions_module in "${extensions_modules[@]}"; do
        module_jar="${DORIS_HOME}/fe/be-java-extensions/${extensions_module}/target/${extensions_module}-jar-with-dependencies.jar"
        module_proj_jar="${DORIS_HOME}/fe/be-java-extensions/${extensions_module}/target/${extensions_module}-project.jar"
        mkdir "${BE_JAVA_EXTENSIONS_DIR}"/"${extensions_module}"
        echo "Copy Be Extensions ${extensions_module} jar to ${BE_JAVA_EXTENSIONS_DIR}/${extensions_module}"
     if [[ "${extensions_module}" == "${HADOOP_DEPS_NAME}" ]]; then
          
            BE_HADOOP_HDFS_DIR="${DORIS_OUTPUT}/be/lib/hadoop_hdfs/"
            echo "Copy Be Extensions hadoop deps jars to ${BE_HADOOP_HDFS_DIR}"
            rm -rf "${BE_HADOOP_HDFS_DIR}"
            mkdir "${BE_HADOOP_HDFS_DIR}"
            HADOOP_DEPS_JAR_DIR="${DORIS_HOME}/fe/be-java-extensions/${HADOOP_DEPS_NAME}/target"
            echo "HADOOP_DEPS_JAR_DIR: ${HADOOP_DEPS_JAR_DIR}"
            if  [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 && ! -d "${HADOOP_DEPS_JAR_DIR}/lib" ]]; then
                echo "WARN: lib directory missing (likely due to Maven cache). Regenerating..."
                pushd "${DORIS_HOME}/fe/be-java-extensions/${HADOOP_DEPS_NAME}"
                "${MVN_CMD}" dependency:copy-dependencies -DskipTests -Dcheckstyle.skip=true
                mv target/dependency target/lib
                popd
            fi
            if [[ -f "${HADOOP_DEPS_JAR_DIR}/${HADOOP_DEPS_NAME}.jar" ]]; then
                echo "Copy Be Extensions hadoop deps jar to ${BE_HADOOP_HDFS_DIR}"
                cp "${HADOOP_DEPS_JAR_DIR}/${HADOOP_DEPS_NAME}.jar" "${BE_HADOOP_HDFS_DIR}"
            fi
            if [[ -d "${HADOOP_DEPS_JAR_DIR}/lib" ]]; then
                cp -r "${HADOOP_DEPS_JAR_DIR}/lib" "${BE_HADOOP_HDFS_DIR}/"
            fi
        else
            if [[ -f "${module_jar}" ]]; then
                cp "${module_jar}" "${BE_JAVA_EXTENSIONS_DIR}"/"${extensions_module}"
            fi
            if [[ -f "${module_proj_jar}" ]]; then
                cp "${module_proj_jar}" "${BE_JAVA_EXTENSIONS_DIR}"/"${extensions_module}"
            fi
            if [[ -d "${DORIS_HOME}/fe/be-java-extensions/${extensions_module}/target/lib" ]]; then
                cp -r "${DORIS_HOME}/fe/be-java-extensions/${extensions_module}/target/lib" "${BE_JAVA_EXTENSIONS_DIR}/${extensions_module}/"
            fi
        fi
    done        

    # Third-party filesystem jars (JuiceFS, JindoFS) are packaged by post-build.sh
    "${DORIS_HOME}/post-build.sh" --be --output "${DORIS_OUTPUT}"

    # Prebuilt thirdparty ships webroot static assets; source-mode builds may
    # not have it on a fresh tree. Only copy if the dir actually exists.
    if [[ -d "${DORIS_THIRDPARTY}/installed/webroot" ]]; then
        cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/be/www"/
    fi
    copy_common_files "${DORIS_OUTPUT}/be/"
    mkdir -p "${DORIS_OUTPUT}/be/log"
    mkdir -p "${DORIS_OUTPUT}/be/storage"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/jdbc_drivers/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/java_udf/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/python_udf/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/connectors/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/hadoop_conf/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/java_extensions/"
    cp -r -p "${DORIS_HOME}/be/src/udf/python/python_server.py" "${DORIS_OUTPUT}/be/plugins/python_udf/"
fi

if [[ "${BUILD_BROKER}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/apache_hdfs_broker"

    cd "${DORIS_HOME}/fs_brokers/apache_hdfs_broker"
    ./build.sh
    rm -rf "${DORIS_OUTPUT}/apache_hdfs_broker"/*
    cp -r -p "${DORIS_HOME}/fs_brokers/apache_hdfs_broker/output/apache_hdfs_broker"/* "${DORIS_OUTPUT}/apache_hdfs_broker"/
    copy_common_files "${DORIS_OUTPUT}/apache_hdfs_broker/"
    cd "${DORIS_HOME}"
fi

if [[ "${BUILD_BE_CDC_CLIENT}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/be/lib/cdc_client"
    cd "${DORIS_HOME}/fs_brokers/cdc_client"
    ./build.sh
    rm -rf "${DORIS_OUTPUT}/be/lib/cdc_client"/*
    cp -r -p "${DORIS_HOME}/fs_brokers/cdc_client/target/cdc-client.jar" "${DORIS_OUTPUT}/be/lib/cdc_client/"
    cd "${DORIS_HOME}"
fi

if [[ ${BUILD_CLOUD} -eq 1 ]]; then
    rm -rf "${DORIS_HOME}/output/ms"
    rm -rf "${DORIS_HOME}/cloud/output/lib/hadoop_hdfs"
    # If hadoop dependencies are required, building cloud module must be done after building be-java-extensions first
    # so when running ./build.sh --cloud,we also build be-java-extensions automatically.
    # If hadoop-depencies are not needed, you can disable it explicitly, by setting DISABLE_BE_JAVA_EXTENSIONS during the build.
    HADOOP_DEPS_JAR_DIR="${DORIS_HOME}/fe/be-java-extensions/${HADOOP_DEPS_NAME}/target"
    if [[ -d "${HADOOP_DEPS_JAR_DIR}/lib" ]]; then
        mkdir -p "${DORIS_HOME}/cloud/output/lib/hadoop_hdfs"
        cp -r "${HADOOP_DEPS_JAR_DIR}/lib/"* "${DORIS_HOME}/cloud/output/lib/hadoop_hdfs/"
    fi
    cp -r -p "${DORIS_HOME}/cloud/output" "${DORIS_HOME}/output/ms"
fi

mkdir -p "${DORIS_HOME}/output/tools"
cp -r -p tools/fdb "${DORIS_HOME}/output/tools"

echo "***************************************"
echo "Successfully build Doris"
echo "***************************************"

if [[ -n "${DORIS_POST_BUILD_HOOK}" ]]; then
    eval "${DORIS_POST_BUILD_HOOK}"
fi

if [[ "${DORIS_BUILD_PROFILE}" == "1" ]]; then
    "${DORIS_HOME}/build_profile.sh" record "${_BP_STATE}" 0
fi

exit 0
