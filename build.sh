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
    bash "${DORIS_HOME}/build_profile.sh" collect "${_BP_STATE}" "$*"
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
     --meta-tool                build Backend meta tool. Default OFF.
     --file-cache-microbench    build Backend file cache microbench tool. Default OFF.
     --cloud                    build Cloud. Default OFF.
     --index-tool               build Backend inverted index tool. Default OFF.
     --benchmark                build Google Benchmark. Default OFF.
     --task-executor-simulator  build Backend task executor simulator. Default OFF.
     --hive-udf                 build Hive UDF library for Ingestion Load. Default ON.
     --be-java-extensions       build Backend java extensions. Default ON.
     --be-cdc-client            build Cdc Client for backend. Default ON.
     --be-extension-ignore      build be-java-extensions package, choose which modules to ignore. Multiple modules separated by commas.
     --enable-dynamic-arch      enable dynamic CPU detection in OpenBLAS. Default ON.
     --disable-dynamic-arch     disable dynamic CPU detection in OpenBLAS.
     --exclude-obs-dependencies exclude all Huawei Cloud OBS (com.huaweicloud) dependencies and the
                                fe-filesystem-obs module; nothing from Huawei is resolved, compiled,
                                or bundled. Use when repo.huaweicloud.com is unreachable or forbidden.
     --exclude-cos-dependencies exclude all Tencent Cloud COS dependencies and the fe-filesystem-cos
                                module; nothing from Tencent COS is resolved, compiled, or bundled.
     --clean                    clean and build target
     --compile-bench            BE compile-speed benchmark: cold, cache-free BE-only build
                                (fresh dedicated build dir, ccache disabled) with a per-phase
                                and per-file timing report. Implies --be; FE/cloud/java
                                extensions/packaging are skipped. For build speed analysis only.
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
    EXTRA_FE_MODULES            Optional FE feature modules in feature=module_path format, separated by commas.
    EXTRA_BE_MODULES            Optional BE feature modules in feature=module_path format, separated by commas.
    EXTRA_CLOUD_MODULES         Optional CLOUD feature modules in feature=module_path format, separated by commas.
    COMPILE_BENCH_TRACE         If set COMPILE_BENCH_TRACE=ON together with --compile-bench (clang only),
                                compile with -ftime-trace and aggregate per-header/per-template costs
                                into the benchmark report. Default is OFF.
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
    $0 --be --fe                            build Backend, Frontend, and Java UDF library
    $0 --be --coverage                      build Backend with coverage enabled
    $0 --be --output PATH                   build Backend, the result will be output to PATH(relative paths are available)
    $0 --be-extension-ignore paimon-scanner build be-java-extensions, choose which modules to ignore. Multiple modules separated by commas, like --be-extension-ignore paimon-scanner,hadoop-hudi-scanner

    $0 --compile-bench                      benchmark a cold cache-free BE build and report the slowest files
    COMPILE_BENCH_TRACE=ON $0 --compile-bench   benchmark and also collect clang -ftime-trace data

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

trim_whitespace() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "${value}"
}

is_valid_extra_module_feature() {
    local feature="$1"
    [[ "${feature}" =~ ^[A-Za-z][A-Za-z0-9_-]*$ ]]
}

feature_to_cmake_name() {
    local feature="$1"
    printf '%s' "${feature}" | tr '[:lower:]-' '[:upper:]_'
}

parse_extra_modules() {
    local array_prefix="$1"
    local spec_value="$2"
    local base_dir="$3"
    local module_type="$4"
    local entry feature module_path existing
    local -a feature_keys=()
    local -a module_paths=()

    if [[ -z "${spec_value}" ]]; then
        eval "${array_prefix}_FEATURE_KEYS=()"
        eval "${array_prefix}_MODULE_PATHS=()"
        return
    fi

    IFS=',' read -r -a entries <<<"${spec_value}"
    for entry in "${entries[@]}"; do
        entry="$(trim_whitespace "${entry}")"
        if [[ -z "${entry}" ]]; then
            echo "Invalid ${array_prefix} module spec: empty entry"
            exit 1
        fi
        if [[ "${entry}" != *=* ]]; then
            echo "Invalid ${array_prefix} module spec '${entry}': expected feature=module_path"
            exit 1
        fi

        feature="${entry%%=*}"
        module_path="${entry#*=}"
        feature="$(trim_whitespace "${feature}")"
        module_path="$(trim_whitespace "${module_path}")"

        if [[ -z "${feature}" || -z "${module_path}" ]]; then
            echo "Invalid ${array_prefix} module spec '${entry}': feature and module_path must be non-empty"
            exit 1
        fi
        if ! is_valid_extra_module_feature "${feature}"; then
            echo "Invalid ${array_prefix} feature '${feature}': use letters, digits, '-' or '_' and start with a letter"
            exit 1
        fi

        for existing in "${feature_keys[@]}"; do
            if [[ "${existing}" == "${feature}" ]]; then
                echo "Duplicate ${array_prefix} feature '${feature}' in ${entry}"
                exit 1
            fi
        done

        if [[ "${module_type}" == "fe" ]]; then
            if [[ ! -f "${base_dir}/${module_path}/pom.xml" ]]; then
                echo "Missing ${array_prefix} FE module: ${base_dir}/${module_path}/pom.xml"
                exit 1
            fi
        elif [[ ! -d "${base_dir}/${module_path}" ]]; then
            echo "Missing ${array_prefix} module directory: ${base_dir}/${module_path}"
            exit 1
        fi

        feature_keys+=("${feature}")
        module_paths+=("${module_path}")
    done

    eval "${array_prefix}_FEATURE_KEYS=(\"\${feature_keys[@]}\")"
    eval "${array_prefix}_MODULE_PATHS=(\"\${module_paths[@]}\")"
}

feature_enabled() {
    local target_feature="$1"
    local existing

    for existing in "${FE_EXTRA_FEATURE_KEYS[@]}" "${BE_EXTRA_FEATURE_KEYS[@]}" "${CLOUD_EXTRA_FEATURE_KEYS[@]}"; do
        if [[ "${existing}" == "${target_feature}" ]]; then
            return 0
        fi
    done
    return 1
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
    -l 'exclude-obs-dependencies' \
    -l 'exclude-cos-dependencies' \
    -l 'clean' \
    -l 'compile-bench' \
    -l 'coverage' \
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
COMPILE_BENCH=0
HELP=0
PARAMETER_COUNT="$#"
PARAMETER_FLAG=0
DENABLE_CLANG_COVERAGE='OFF'
BUILD_AZURE='ON'
BUILD_UI=1
if [[ "$#" == 1 ]]; then
    # default
    BUILD_FE=1
    BUILD_BE=1
    BUILD_CLOUD=1

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
            BUILD_BE=1
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
        --broker)
            # Deprecated no-op: the in-tree apache_hdfs_broker daemon has been
            # removed. The option is still accepted so existing build/CI scripts
            # that pass --broker do not break, but it no longer builds anything.
            echo "Warning: --broker is deprecated and has no effect; the apache_hdfs_broker module has been removed."
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
        --compile-bench)
            COMPILE_BENCH=1
            shift
            ;;
        --coverage)
            DENABLE_CLANG_COVERAGE='ON'
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

if [[ "${CLEAN}" -eq 1 && "${BUILD_BE}" -eq 0 && "${BUILD_FE}" -eq 0 && ${BUILD_CLOUD} -eq 0 ]]; then
    clean_gensrc
    clean_be
    clean_fe
    exit 0
fi

# build thirdparty libraries if necessary. check last thirdparty lib installation
if [[ "${TARGET_SYSTEM}" == 'Darwin' ]]; then
    LAST_THIRDPARTY_LIB='libbrotlienc.a'
else
    LAST_THIRDPARTY_LIB='hadoop_hdfs_3_4/native/libhdfs.a'
fi

# The final-library sentinel only proves that some third-party build completed. It cannot
# distinguish an older prebuilt whose Arrow/Paimon closure predates the selected sources.
# shellcheck source=thirdparty/arrow-paimon-vars.sh
. "${DORIS_HOME}/thirdparty/arrow-paimon-vars.sh"
NEED_ARROW_PAIMON_THIRDPARTY=false
if [[ "${BUILD_BE}" -eq 1 || "${BUILD_CLOUD}" -eq 1 ||
    "${BUILD_META_TOOL}" == "ON" || "${BUILD_FILE_CACHE_MICROBENCH_TOOL}" == "ON" ||
    "${BUILD_INDEX_TOOL}" == "ON" ]]; then
    NEED_ARROW_PAIMON_THIRDPARTY=true
fi

rebuild_thirdparty_libraries() {
    local remove_installed="$1"
    shift
    local build_script="${DORIS_THIRDPARTY}/build-thirdparty.sh"
    local build_args=(-j "${PARALLEL}")
    local selected_thirdparty_root
    local checkout_thirdparty_root

    if [[ ! -f "${build_script}" ]]; then
        echo "Cannot rebuild thirdparty libraries: ${build_script} is missing." >&2
        echo "DORIS_THIRDPARTY=${DORIS_THIRDPARTY} is an install-only or incomplete prefix. Use a matching compilation image/prebuilt, or unset DORIS_THIRDPARTY to rebuild with this checkout's thirdparty tree." >&2
        exit 1
    fi
    selected_thirdparty_root="$(cd "${DORIS_THIRDPARTY}" && pwd -P)"
    checkout_thirdparty_root="$(cd "${DORIS_HOME}/thirdparty" && pwd -P)"
    if [[ "${selected_thirdparty_root}" != "${checkout_thirdparty_root}" ]]; then
        echo "Cannot rebuild thirdparty libraries with an external source tree: ${selected_thirdparty_root}." >&2
        echo "Unset DORIS_THIRDPARTY to rebuild with this checkout's thirdparty tree, then use the resulting version-matched installation." >&2
        exit 1
    fi
    build_script="${checkout_thirdparty_root}/build-thirdparty.sh"
    if [[ "${remove_installed}" == "true" ]]; then
        # Some libraries, such as lz4, fail when an earlier installation remains.
        rm -rf "${DORIS_THIRDPARTY}/installed"
    fi
    if [[ "${CLEAN}" -eq 1 ]]; then
        build_args+=(--clean)
    fi
    bash "${build_script}" "${build_args[@]}" "$@"
    if ! arrow_paimon_prebuilt_valid "${DORIS_THIRDPARTY}/installed"; then
        echo "Rebuilt Arrow/Paimon artifacts do not match this checkout's selected inputs." >&2
        exit 1
    fi
}

if [[ ! -f "${DORIS_THIRDPARTY}/installed/lib/${LAST_THIRDPARTY_LIB}" ]]; then
    echo "Thirdparty libraries need to be build ..."
    rebuild_thirdparty_libraries true
elif [[ "${NEED_ARROW_PAIMON_THIRDPARTY}" == "true" ]] &&
    ! arrow_paimon_prebuilt_valid "${DORIS_THIRDPARTY}/installed"; then
    echo "Arrow/Paimon thirdparty libraries need to be rebuilt ..."
    rebuild_thirdparty_libraries false "${ARROW_PAIMON_BUILD_PACKAGES[@]}"
fi

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
if [[ "${BUILD_TYPE_LOWWER}" == "asan" ]]; then
    USE_JEMALLOC='OFF'
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

# Same polarity as BUILD_JINDOFS above and as post-build.sh, which is what actually installs the
# jars: unset means OFF, matching the --help text. They used to disagree - this said ON when the
# variable was unset - and the wipe below then ran on a build that did not repackage, deleting
# lib/juicefs and not putting it back.
if [[ "$(echo "${DISABLE_BUILD_JUICEFS}" | tr '[:lower:]' '[:upper:]')" == "OFF" ]]; then
    BUILD_JUICEFS='ON'
else
    BUILD_JUICEFS='OFF'
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

EXTRA_FE_MODULES="${EXTRA_FE_MODULES:-}"
EXTRA_BE_MODULES="${EXTRA_BE_MODULES:-}"
EXTRA_CLOUD_MODULES="${EXTRA_CLOUD_MODULES:-}"

parse_extra_modules "FE_EXTRA" "${EXTRA_FE_MODULES}" "${DORIS_HOME}/fe" "fe"
parse_extra_modules "BE_EXTRA" "${EXTRA_BE_MODULES}" "${DORIS_HOME}/be/src" "be"
parse_extra_modules "CLOUD_EXTRA" "${EXTRA_CLOUD_MODULES}" "${DORIS_HOME}/cloud/src" "cloud"

BE_EXTRA_CMAKE_ARGS=()
COMPILE_BENCH_CMAKE_ARGS=()
for ((i = 0; i < ${#BE_EXTRA_FEATURE_KEYS[@]}; i++)); do
    feature_name="$(feature_to_cmake_name "${BE_EXTRA_FEATURE_KEYS[i]}")"
    BE_EXTRA_CMAKE_ARGS+=("-DENABLE_${feature_name}=ON")
    BE_EXTRA_CMAKE_ARGS+=("-D${feature_name}_MODULE_DIR=${BE_EXTRA_MODULE_PATHS[i]}")
done

CLOUD_EXTRA_CMAKE_ARGS=()
for ((i = 0; i < ${#CLOUD_EXTRA_FEATURE_KEYS[@]}; i++)); do
    feature_name="$(feature_to_cmake_name "${CLOUD_EXTRA_FEATURE_KEYS[i]}")"
    CLOUD_EXTRA_CMAKE_ARGS+=("-DENABLE_${feature_name}=ON")
    CLOUD_EXTRA_CMAKE_ARGS+=("-D${feature_name}_MODULE_DIR=${CLOUD_EXTRA_MODULE_PATHS[i]}")
done

if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
    # BE compile benchmark mode: measure a cold, cache-free BE C++ build.
    # Everything that is not the BE C++ build would only add noise, so force
    # a BE-only build regardless of the other options.
    BUILD_BE=1
    BUILD_FE=0
    BUILD_CLOUD=0
    BUILD_HIVE_UDF=0
    BUILD_BE_JAVA_EXTENSIONS=0
    BUILD_BE_CDC_CLIENT=0
    OUTPUT_BE_BINARY=0
    # shellcheck source=build-support/compile-bench/bench-lib.sh
    . "${DORIS_HOME}/build-support/compile-bench/bench-lib.sh"
    compile_bench_init "${DORIS_HOME}"
fi

echo "Get params:
    BUILD_FE                            -- ${BUILD_FE}
    BUILD_BE                            -- ${BUILD_BE}
    BUILD_CLOUD                         -- ${BUILD_CLOUD}
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
    STRIP_DEBUG_INFO                    -- ${STRIP_DEBUG_INFO}
    USE_JEMALLOC                        -- ${USE_JEMALLOC}
    USE_BTHREAD_SCANNER                 -- ${USE_BTHREAD_SCANNER}
    ENABLE_INJECTION_POINT              -- ${ENABLE_INJECTION_POINT}
    DENABLE_CLANG_COVERAGE              -- ${DENABLE_CLANG_COVERAGE}
    DISPLAY_BUILD_TIME                  -- ${DISPLAY_BUILD_TIME}
    ENABLE_PCH                          -- ${ENABLE_PCH}
    ENABLE_UNITY_BUILD                  -- ${ENABLE_UNITY_BUILD:-ON}
    EXTRA_FE_MODULES                    -- ${EXTRA_FE_MODULES}
    EXTRA_BE_MODULES                    -- ${EXTRA_BE_MODULES}
    EXTRA_CLOUD_MODULES                 -- ${EXTRA_CLOUD_MODULES}
"

FEAT=()
FEAT+=($(feature_enabled "tde" && echo "+TDE" || echo "-TDE"))
FEAT+=($(feature_enabled "tls" && echo "+TLS" || echo "-TLS"))
FEAT+=($(feature_enabled "variant-nested-group" && echo "+VARIANT_NESTED_GROUP" || echo "-VARIANT_NESTED_GROUP"))
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
if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
    compile_bench_phase_begin "gensrc"
fi
bash "${DORIS_HOME}"/generated-source.sh noclean
if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
    compile_bench_phase_end
fi

# Assesmble FE modules
FE_MODULES=''
modules=()
if [[ "${BUILD_FE}" -eq 1 ]]; then
    modules+=("fe-extension-spi")
    modules+=("fe-extension-loader")
    modules+=("fe-core")
    # Filesystem API and SPI plugin modules (loaded at runtime as plugins)
    modules+=("fe-filesystem/fe-filesystem-api")
    modules+=("fe-filesystem/fe-filesystem-spi")
    for _fs_mod in s3-base s3 gcs minio ozone oss cos obs azure hdfs-base hdfs oss-hdfs jfs local broker http; do
        # Skip the modules whose Maven profile is deactivated so the -pl list stays consistent with
        # the reactor: obs is absent under -Ddisable.obs=true, cos under -Ddisable.cos=true.
        if [[ "${_fs_mod}" == "obs" && "${BUILD_OBS_DEPENDENCIES}" -eq 0 ]]; then
            continue
        fi
        if [[ "${_fs_mod}" == "cos" && "${BUILD_COS_DEPENDENCIES}" -eq 0 ]]; then
            continue
        fi
        if [[ -d "${DORIS_HOME}/fe/fe-filesystem/fe-filesystem-${_fs_mod}" ]]; then
            modules+=("fe-filesystem/fe-filesystem-${_fs_mod}")
        fi
    done
    unset _fs_mod
    # Connector SPI and plugin modules (loaded at runtime as plugins)
    modules+=("fe-connector/fe-connector-spi")
    # Keep this list identical to the deploy loop's (search CONN_PLUGIN_DIR). A module missing here
    # but present there is not a no-op: the deploy step unzips whatever archive is left in the
    # module's target/ from some earlier build, so the plugin silently ships stale.
    for _conn_mod in es jdbc maxcompute trino hms hive paimon hudi iceberg adbc; do
        if [[ -d "${DORIS_HOME}/fe/fe-connector/fe-connector-${_conn_mod}" ]]; then
            modules+=("fe-connector/fe-connector-${_conn_mod}")
        fi
    done
    unset _conn_mod
    for extra_module_path in "${FE_EXTRA_MODULE_PATHS[@]}"; do
        modules+=("${extra_module_path}")
    done
fi
if [[ "${BUILD_HIVE_UDF}" -eq 1 ]]; then
    modules+=("hive-udf")
fi
if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 ]]; then
    # This list is the complete enumeration of be-java-extensions modules that get built, not just
    # the ones -am cannot reach. Keep it that way: reading it should answer "does my module get
    # built" without also having to work out who depends on whom.
    #
    # The plugins. Each one deploys as its own directory under plugins/jni; see the deploy
    # list far below, which maps module name -> plugin directory name.
    modules+=("be-java-extensions/iceberg-metadata-scanner")
    modules+=("be-java-extensions/hadoop-hudi-scanner")
    modules+=("be-java-extensions/java-udf")
    modules+=("be-java-extensions/jdbc-scanner")
    modules+=("be-java-extensions/paimon-scanner")
    modules+=("be-java-extensions/trino-connector-scanner")
    modules+=("be-java-extensions/max-compute-connector")
    modules+=("be-java-extensions/java-writer")
    # The hadoop drop C++ libhdfs loads. Not a plugin: it deploys whole into lib/hadoop_hdfs and BE
    # resolves it off the system classpath, so no plugin ever sees it and it has no plugin name.
    modules+=("be-java-extensions/${HADOOP_DEPS_NAME}")
    # The shared layer, deployed to lib/jni/spi. jni-spi is declared by every plugin, so -am would
    # reach it; jni-bootstrap is the loader and nothing depends on it, so this line is the only
    # thing that builds it. Both are named for the same reason as the rest of this list.
    modules+=("be-java-extensions/jni-spi")
    modules+=("be-java-extensions/jni-bootstrap")
    # Not deployed on their own; they are dependencies of the plugins above and land inside the
    # plugin directories. -am would reach them, but they are named here so this list stays a
    # complete enumeration.
    modules+=("be-java-extensions/plugin-toolkit")
    modules+=("be-java-extensions/hive-udf-shade")
    modules+=("be-java-extensions/hive-apache-shade")

    # If the BE_EXTENSION_IGNORE variable is not empty, remove the modules that need to be ignored from FE_MODULES
    if [[ -n "${BE_EXTENSION_IGNORE}" ]]; then
        # The values this accepts, spelled out. Every entry is a MODULE name, which is not always the
        # name the plugin deploys under (paimon-scanner deploys as "paimon"), and the deploy map far
        # below names the plugin directories - so "paimon" or "hudi" is the natural thing to try and
        # neither is a module. Rejecting the rest is the point: the removal below used to be bash's
        # substring replacement, so BE_EXTENSION_IGNORE=paimon rewrote the entry to the literal
        # "-scanner" and maven then failed with "Could not find the selected project in the reactor",
        # while BE_EXTENSION_IGNORE=hudi matched nothing at all and the module was built and deployed
        # anyway, silently.
        ignorable_modules=(
            "iceberg-metadata-scanner" "hadoop-hudi-scanner" "java-udf" "jdbc-scanner"
            "paimon-scanner" "trino-connector-scanner" "max-compute-connector" "java-writer"
            "${HADOOP_DEPS_NAME}"
        )
        IFS=',' read -r -a ignore_modules <<<"${BE_EXTENSION_IGNORE}"
        for module in "${ignore_modules[@]}"; do
            module="${module// /}"
            [[ -z "${module}" ]] && continue
            # jni-spi and jni-bootstrap are not extensions to leave out, they are the shared layer
            # every extension is loaded through. Dropping one produces a BE that reports success
            # and then fails every Java feature at runtime with a FindClass error, and nobody
            # connects that to a build argument given days earlier.
            if [[ "${module}" == 'jni-spi' || "${module}" == 'jni-bootstrap' ]]; then
                echo "Error: BE_EXTENSION_IGNORE cannot exclude ${module}: it is the plugin SPI and"
                echo "       loader that every Java extension is loaded through, not an extension."
                exit 1
            fi
            known=0
            for ignorable in "${ignorable_modules[@]}"; do
                if [[ "${module}" == "${ignorable}" ]]; then
                    known=1
                    break
                fi
            done
            if [[ "${known}" -eq 0 ]]; then
                echo "Error: BE_EXTENSION_IGNORE cannot exclude '${module}': it is not a be-java-extensions"
                echo "       module. These are the module names it accepts (note that a plugin's directory"
                echo "       name is not always its module name):"
                printf '           %s\n' "${ignorable_modules[@]}"
                exit 1
            fi
            kept_modules=()
            for entry in "${modules[@]}"; do
                if [[ "${entry}" != "be-java-extensions/${module}" ]]; then
                    kept_modules+=("${entry}")
                fi
            done
            modules=("${kept_modules[@]}")
        done
        unset ignorable_modules ignorable known kept_modules entry
    fi
fi
FE_MODULES="$(
    IFS=','
    echo "${modules[*]}"
)"

# Clean and build Backend
if [[ "${BUILD_BE}" -eq 1 ]]; then

    if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
        compile_bench_phase_begin "contrib_submodules"
    fi
    update_submodule "contrib/datasketches-cpp" "datasketches-cpp" "https://github.com/apache/datasketches-cpp/archive/refs/heads/master.tar.gz"
    update_submodule "contrib/apache-orc" "apache-orc" "https://github.com/apache/doris-thirdparty/archive/refs/heads/orc.tar.gz"
    update_submodule "contrib/clucene" "clucene" "https://github.com/apache/doris-thirdparty/archive/refs/heads/clucene.tar.gz"
    update_submodule "contrib/openblas" "openblas" "https://github.com/apache/doris-thirdparty/archive/refs/heads/openblas.tar.gz"
    update_submodule "contrib/faiss" "faiss" "https://github.com/apache/doris-thirdparty/archive/refs/heads/faiss.tar.gz"
    if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
        compile_bench_phase_end
    fi
    if [[ -e "${DORIS_HOME}/gensrc/build/gen_cpp/version.h" ]]; then
        rm -f "${DORIS_HOME}/gensrc/build/gen_cpp/version.h"
    fi
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR="${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}"
    if [[ "${CLEAN}" -eq 1 ]]; then
        clean_be
    fi
    if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
        # Dedicated always-cold build dir: no reused objects, no reused CMake
        # cache, and the developer's normal build dir stays untouched.
        CMAKE_BUILD_DIR="${COMPILE_BENCH_BUILD_DIR}"
        echo "Compile-bench: recreating build dir ${CMAKE_BUILD_DIR} from scratch"
        rm -rf "${CMAKE_BUILD_DIR}"
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
    if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
        compile_bench_phase_begin "cmake_configure"
    fi
    "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DENABLE_INJECTION_POINT="${ENABLE_INJECTION_POINT}" \
        -DMAKE_TEST=OFF \
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
        -DDISPLAY_BUILD_TIME="${DISPLAY_BUILD_TIME}" \
        -DENABLE_PCH="${ENABLE_PCH}" \
        -DENABLE_UNITY_BUILD="${ENABLE_UNITY_BUILD:-ON}" \
        -DUSE_JEMALLOC="${USE_JEMALLOC}" \
        -DUSE_AVX2="${USE_AVX2}" \
        -DARM_MARCH="${ARM_MARCH}" \
        -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
        -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
        -DENABLE_CLANG_COVERAGE="${DENABLE_CLANG_COVERAGE}" \
        -DDORIS_JAVA_HOME="${JAVA_HOME}" \
        -DBUILD_AZURE="${BUILD_AZURE}" \
        -DENABLE_DYNAMIC_ARCH="${ENABLE_DYNAMIC_ARCH}" \
        -DFAISS_ENABLE_GPU="${FAISS_ENABLE_GPU:-OFF}" \
        "${BE_EXTRA_CMAKE_ARGS[@]}" \
        "${COMPILE_BENCH_CMAKE_ARGS[@]}" \
        "${DORIS_HOME}/be"

    if [[ "${COMPILE_BENCH}" -eq 1 ]]; then
        compile_bench_phase_end

        compile_bench_phase_begin "build"
        set +e
        "${BUILD_SYSTEM}" -j "${PARALLEL}"
        compile_bench_build_rc=$?
        set -e
        compile_bench_phase_end

        # Generate the timing report even for a failed build, then stop:
        # install/packaging is out of scope for a compile benchmark.
        compile_bench_finish "${CMAKE_BUILD_DIR}" "${compile_bench_build_rc}"
        exit "${compile_bench_build_rc}"
    fi

    if [[ "${OUTPUT_BE_BINARY}" -eq 1 ]]; then
        "${BUILD_SYSTEM}" -j "${PARALLEL}"
        "${BUILD_SYSTEM}" install
    fi

    cd "${DORIS_HOME}"
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
        "${CLOUD_EXTRA_CMAKE_ARGS[@]}" \
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
        # Deactivates the `obs` Maven profile in fe-core, hadoop-deps and fe-filesystem, so no
        # com.huaweicloud artifact is resolved and the Huawei OBS module is not built or bundled.
        dependency_mvn_opts+=("-Ddisable.obs=true")
    fi
    if [[ "${BUILD_COS_DEPENDENCIES}" -eq 0 ]]; then
        # Deactivates the `cos` Maven profile in fe-core and fe-filesystem, so no Tencent COS
        # artifact is resolved and the fe-filesystem-cos module is not built or bundled.
        dependency_mvn_opts+=("-Ddisable.cos=true")
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
    # Everything EXCEPT jindofs/ and juicefs/, which post-build.sh installs below and only when
    # DISABLE_BUILD_JINDOFS/JUICEFS=OFF asks it to. Wiping them here regardless would mean a plain
    # --fe rebuild deletes the jars an earlier build installed and does not put them back, leaving
    # a FE that resolved oss-hdfs:// and jfs:// yesterday unable to today. Same rule as the BE side
    # far below: a third-party filesystem directory is only cleared behind the switch that
    # repopulates it.
    find "${DORIS_OUTPUT}/fe/lib" -mindepth 1 -maxdepth 1 \
        ! -name jindofs ! -name juicefs -exec rm -rf {} +
    if [[ "${BUILD_JUICEFS}" == 'ON' ]]; then
        rm -rf "${DORIS_OUTPUT}/fe/lib/juicefs"
    fi
    if [[ "${BUILD_JINDOFS}" == 'ON' ]]; then
        rm -rf "${DORIS_OUTPUT}/fe/lib/jindofs"
    fi
    unzip -q -o "${DORIS_HOME}/fe/fe-core/target/doris-fe-lib.zip" -d "${DORIS_OUTPUT}/fe/lib"
    cp -r -p "${DORIS_HOME}/fe/fe-core/target/doris-fe.jar" "${DORIS_OUTPUT}/fe/lib"/
    for extra_module_path in "${FE_EXTRA_MODULE_PATHS[@]}"; do
        module_target="${DORIS_HOME}/fe/${extra_module_path}/target"
        if [[ -d "${module_target}/lib" ]]; then
            cp -r -p "${module_target}/lib"/* "${DORIS_OUTPUT}/fe/lib"/
        fi
        shopt -s nullglob
        for module_jar in "${module_target}"/*.jar; do
            case "$(basename "${module_jar}")" in
            *-sources.jar | *-test-sources.jar | *tests.jar | original-*.jar)
                continue
                ;;
            esac
            cp -r -p "${module_jar}" "${DORIS_OUTPUT}/fe/lib"/
        done
        shopt -u nullglob
    done

    #cp -r -p "${DORIS_HOME}/docs/build/help-resource.zip" "${DORIS_OUTPUT}/fe/lib"/

    # Third-party filesystem jars (JuiceFS, JindoFS) are packaged by post-build.sh
    bash "${DORIS_HOME}/post-build.sh" --fe --output "${DORIS_OUTPUT}"

    cp -r -p "${DORIS_HOME}/minidump" "${DORIS_OUTPUT}/fe"/
    cp -r -p "${DORIS_HOME}/webroot/static" "${DORIS_OUTPUT}/fe/webroot"/

    cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/fe/webroot/static"/
    copy_common_files "${DORIS_OUTPUT}/fe/"
    mkdir -p "${DORIS_OUTPUT}/fe/log"
    mkdir -p "${DORIS_OUTPUT}/fe/doris-meta"
    mkdir -p "${DORIS_OUTPUT}/fe/conf/ssl"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/jdbc_drivers/"
    # Drop point for ADBC driver shared libraries. Doris does not ship the drivers themselves; the
    # same file must be placed here AND under be/plugins/adbc_drivers on every BE, because partition
    # descriptors are driver-private bytes with no interoperability across driver implementations.
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/adbc_drivers/"
    # The ADBC JNI shim, built by thirdparty. NOT the copy inside the adbc-driver-jni jar: the
    # released one needs GLIBC 2.34 / GLIBCXX 3.4.31, which the supported build hosts do not have.
    # conf/fe.conf points arrow.adbc.driver.jni.library.path at this directory.
    if [[ -f "${DORIS_THIRDPARTY}/installed/lib64/libadbc_driver_jni.so" ]]; then
        cp -p "${DORIS_THIRDPARTY}/installed/lib64/libadbc_driver_jni.so" "${DORIS_OUTPUT}/fe/lib/"
    fi
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/java_udf/"
    # Drop point for the trino-connector's own Trino plugins. Deliberately NOT the legacy
    # plugins/connectors/: that name is still read as a fallback for deployments upgrading from
    # <= 2.1.8, so a fresh install must not create it (an empty dir would be harmless, but the
    # one-letter gap to the plugins/connector/ tree above is not).
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/trino_plugins/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/hadoop_conf/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/java_extensions/"

    # Deploy filesystem provider plugins as independent plugin directories
    # Each sub-directory is one storage backend loaded at runtime by FileSystemPluginManager.
    FS_PLUGIN_DIR="${DORIS_OUTPUT}/fe/plugins/filesystem"
    for fs_module in s3 gcs minio ozone azure oss cos obs hdfs oss-hdfs jfs local broker http; do
        fs_plugin_target="${FS_PLUGIN_DIR}/${fs_module}"
        fs_module_dir="${DORIS_HOME}/fe/fe-filesystem/fe-filesystem-${fs_module}"
        if [ ! -d "${fs_module_dir}" ]; then
            continue
        fi
        # These modules are not built when their Maven profile is deactivated, so their plugin zip
        # does not exist; skip the unpack to keep packaging consistent with the reactor.
        if [[ "${fs_module}" == "obs" && "${BUILD_OBS_DEPENDENCIES}" -eq 0 ]]; then
            continue
        fi
        if [[ "${fs_module}" == "cos" && "${BUILD_COS_DEPENDENCIES}" -eq 0 ]]; then
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
    for conn_module in es jdbc maxcompute trino hms hive paimon hudi iceberg adbc; do
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
        # A connector's own settings file. The zip carries only <name>.conf.template; the live
        # <name>.conf is seeded from it here and never overwritten, so an upgrade that unzips a new
        # plugin build over this directory refreshes the jars and the template but leaves whatever the
        # administrator configured. Deliberately generic (globbed on *.conf.template, no connector
        # named): a new connector ships a template and needs no change here.
        for conn_conf_tpl in "${conn_plugin_target}"/*.conf.template; do
            [ -e "${conn_conf_tpl}" ] || continue
            cp -n "${conn_conf_tpl}" "${conn_conf_tpl%.template}"
        done
    done
    unset CONN_PLUGIN_DIR conn_module conn_plugin_target conn_module_dir conn_zip conn_conf_tpl

    # RC-4: self-contain the paimon connector plugin for OSS. The connector sets
    # fs.oss.impl=com.aliyun.jindodata.oss.JindoOssFileSystem; that impl lives in the jindofs jars,
    # which are packaged from thirdparty by post-build.sh into fe/lib/jindofs (NOT a maven artifact).
    # com.aliyun.jindodata is child-first (only org.apache.doris.connector./.filesystem. and
    # org.apache.hadoop. are parent-first, see ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES),
    # so without its OWN copy JindoOssFileSystem resolves from the parent 'app' classloader.
    # Historically that could not be cast to the plugin's child-loaded org.apache.hadoop.fs.FileSystem;
    # since org.apache.hadoop. became parent-first the cast itself would now succeed, but the copy stays:
    # it keeps the jindo classes in the plugin's own loader (same self-contained intent as the bundled
    # hadoop-aws/S3A) and is what the plugin falls back to once the FE kernel stops shipping hadoop.
    # Naturally gated: a no-op unless jindofs was packaged (DISABLE_BUILD_JINDOFS=OFF, or
    # post-build.sh --jindofs; build.sh itself takes no such flag).
    # CAVEAT (docker-gated, enablePaimonTest=true): jindo-core ships a native lib that can bind to only one
    # classloader per JVM, so this is safe only while no concurrent non-paimon path loads jindo from
    # fe/lib/jindofs in the same FE process.
    PAIMON_CONN_LIB="${DORIS_OUTPUT}/fe/plugins/connector/paimon/lib"
    if [[ -d "${PAIMON_CONN_LIB}" && -d "${DORIS_OUTPUT}/fe/lib/jindofs" ]]; then
        cp -p "${DORIS_OUTPUT}/fe/lib/jindofs/"*.jar "${PAIMON_CONN_LIB}/" 2>/dev/null || true
    fi
    unset PAIMON_CONN_LIB

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

    # Everything from here to the end of this block deploys what the Java extension build
    # produced, so it only runs when there was one. DISABLE_BE_JAVA_EXTENSIONS=ON (and the
    # Darwin fallback that sets the same flag when JAVA_HOME has no usable libjvm) leaves every
    # target/ below empty, and the plugin loop is a hard failure when a jar is missing - which
    # is how a BE-only build, .github/workflows/be-ut-mac.yml included, died here.
    if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 ]]; then

        # Every be-java-extensions module that BE addresses by name is a plugin now. The one exception
        # is the hadoop drop below, which is not a plugin and never was, so there is no list of
        # "extensions modules" left to iterate - only that one flag.
        deploy_hadoop_deps=1
        if [[ -n "${BE_EXTENSION_IGNORE}" ]]; then
            IFS=',' read -r -a ignore_modules <<<"${BE_EXTENSION_IGNORE}"
            for ignore_module in "${ignore_modules[@]}"; do
                if [[ "${ignore_module// /}" == "${HADOOP_DEPS_NAME}" ]]; then
                    deploy_hadoop_deps=0
                    break
                fi
            done
        fi

        # The shared layer: the SPI a plugin compiles against and the loader that reads the plugin
        # directory. These are the only Doris classes that live on both sides of the boundary, which
        # is why they are the only ones deployed where the system classpath can see them.
        BE_JAVA_SPI_DIR="${DORIS_OUTPUT}/be/lib/jni/spi"
        rm -rf "${DORIS_OUTPUT}/be/lib/jni"
        mkdir -p "${BE_JAVA_SPI_DIR}"
        for spi_module in jni-spi jni-bootstrap; do
            spi_jar="${DORIS_HOME}/fe/be-java-extensions/${spi_module}/target/doris-${spi_module}.jar"
            # Louder than the plugin loop below, not quieter: without these two jars there is no
            # loader at all, so every Java feature fails at runtime with a FindClass error that
            # names none of this. They are also not affected by BE_EXTENSION_IGNORE - see the
            # module list far above.
            if [[ ! -f "${spi_jar}" ]]; then
                echo "Error: ${spi_module} produced no ${spi_jar}. It carries the plugin SPI and the"
                echo "       loader that reads plugins/jni, so a BE without it can load no Java"
                echo "       plugin at all."
                exit 1
            fi
            echo "Copy Be shared layer ${spi_module} jar to ${BE_JAVA_SPI_DIR}"
            cp "${spi_jar}" "${BE_JAVA_SPI_DIR}"
        done

        # Plugins, one directory each: the module jar plus the runtime closure copy-dependencies put
        # beside it. The directory name is what BE addresses the plugin by and is deliberately not
        # required to equal the module name - paimon-scanner will deploy as "paimon" - so the mapping
        # is spelled out rather than derived.
        #
        # ATTN: a module named here must also be in the maven module list far above; adding it in one
        # place only means deploying whatever the last build happened to leave in target/, which looks
        # like a successful build of the wrong thing.
        BE_JAVA_PLUGINS_DIR="${DORIS_OUTPUT}/be/plugins/jni"
        # ATTN: this rm reaches into plugins/, which is otherwise the operator's tree - the drivers,
        # configs and UDF jars they dropped there. It must name plugins/jni and nothing above it;
        # widening it by one path element wipes a running deployment's drop points.
        rm -rf "${BE_JAVA_PLUGINS_DIR}"
        mkdir -p "${BE_JAVA_PLUGINS_DIR}"
        plugin_modules=("java-writer:java-writer")
        plugin_modules+=("jdbc-scanner:jdbc")
        plugin_modules+=("iceberg-metadata-scanner:iceberg")
        plugin_modules+=("max-compute-connector:max-compute")
        plugin_modules+=("paimon-scanner:paimon")
        plugin_modules+=("hadoop-hudi-scanner:hudi")
        plugin_modules+=("trino-connector-scanner:trino-connector")
        plugin_modules+=("java-udf:java-udf")

        if [[ -n "${BE_EXTENSION_IGNORE}" ]]; then
            IFS=',' read -r -a ignore_modules <<<"${BE_EXTENSION_IGNORE}"
            kept_plugins=()
            for plugin_entry in "${plugin_modules[@]}"; do
                ignore=0
                for ignore_module in "${ignore_modules[@]}"; do
                    if [[ "${plugin_entry%%:*}" == "${ignore_module// /}" ]]; then
                        ignore=1
                        break
                    fi
                done
                if [[ "${ignore}" -eq 0 ]]; then
                    kept_plugins+=("${plugin_entry}")
                fi
            done
            plugin_modules=("${kept_plugins[@]}")
        fi

        for plugin_entry in "${plugin_modules[@]}"; do
            plugin_module="${plugin_entry%%:*}"
            plugin_name="${plugin_entry##*:}"
            plugin_target="${DORIS_HOME}/fe/be-java-extensions/${plugin_module}/target"
            plugin_jar="${plugin_target}/${plugin_module}.jar"
            if [[ ! -f "${plugin_jar}" ]]; then
                echo "Error: ${plugin_module} produced no ${plugin_module}.jar. A plugin jar is named"
                echo "       after its module; deploying an empty plugin directory would surface much"
                echo "       later as 'Java plugin ${plugin_name} failed to load'."
                exit 1
            fi
            echo "Copy Be plugin ${plugin_module} to ${BE_JAVA_PLUGINS_DIR}/${plugin_name}"
            mkdir -p "${BE_JAVA_PLUGINS_DIR}/${plugin_name}"
            cp "${plugin_jar}" "${BE_JAVA_PLUGINS_DIR}/${plugin_name}"
            # Tested on the jars, not on the directory: target/lib is emptied before
            # copy-dependencies refills it, so an existing but empty directory is reachable and the
            # glob below would then expand to nothing and fail the whole build under set -e.
            if compgen -G "${plugin_target}/lib/*.jar" > /dev/null; then
                cp "${plugin_target}/lib"/*.jar "${BE_JAVA_PLUGINS_DIR}/${plugin_name}"
            fi
        done

        # The hadoop drop C++ libhdfs loads, and the JindoFS/JuiceFS drops the same libhdfs resolves
        # oss-hdfs:// and jfs:// through: none of the three is a plugin, so libhdfs finds each by a
        # fixed directory name on the system classpath rather than through a plugin loader. Each is
        # therefore wiped and deployed whole every build rather than merged with whatever a previous
        # build using the same output directory left behind - unwiped, a version bump would leave two
        # jar versions of the same filesystem side by side, and start_be.sh's *.jar glob would put
        # both of them on the classpath.
        if [[ "${deploy_hadoop_deps}" -eq 1 ]]; then
            BE_HADOOP_HDFS_DIR="${DORIS_OUTPUT}/be/lib/hadoop_hdfs/"
            echo "Copy Be Extensions hadoop deps jars to ${BE_HADOOP_HDFS_DIR}"
            # Wiped HERE, inside the branch that refills it, and not earlier: a machine that built once
            # with Java extensions and then again with DISABLE_BE_JAVA_EXTENSIONS=ON into the same
            # output/ used to lose this directory for good, and with libhdfs3 gone that BE has no HDFS
            # path left at all. Wiped rather than merged so that a version bump cannot leave two jar
            # versions of the same dependency side by side for start_be.sh's *.jar glob to find.
            rm -rf "${BE_HADOOP_HDFS_DIR}"
            mkdir "${BE_HADOOP_HDFS_DIR}"
            HADOOP_DEPS_JAR_DIR="${DORIS_HOME}/fe/be-java-extensions/${HADOOP_DEPS_NAME}/target"
            echo "HADOOP_DEPS_JAR_DIR: ${HADOOP_DEPS_JAR_DIR}"
            if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 && ! -d "${HADOOP_DEPS_JAR_DIR}/lib" ]]; then
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
        fi

        # The layout before plugins: one big jar per extension under lib/java_extensions. Nothing
        # deploys there any more, so an output directory reused across the change keeps serving the
        # previous version's jars - JvmLauncher::scan_class_path() still walks lib/ for a BE that was
        # not started by start_be.sh.
        #
        # Everything EXCEPT jindofs/ and juicefs/, which are not extensions and were never deployed
        # by the loop above: start_be.sh reads them from here as its WARN fallback, so that an
        # existing deployment keeps resolving oss-hdfs:// and jfs:// across the upgrade. Wiping them
        # is what the comment right here used to promise not to do.
        if [[ -d "${DORIS_OUTPUT}/be/lib/java_extensions" ]]; then
            find "${DORIS_OUTPUT}/be/lib/java_extensions" -mindepth 1 -maxdepth 1 \
                ! -name jindofs ! -name juicefs -exec rm -rf {} +
        fi

    fi # BUILD_BE_JAVA_EXTENSIONS

    # Wiped before post-build.sh repopulates them below, for the same reason as lib/hadoop_hdfs
    # above - and behind the same switch that repopulates them, so that a rebuild without
    # DISABLE_BUILD_JUICEFS/JINDOFS=OFF leaves the jars an earlier build installed alone instead of
    # deleting them and not putting them back.
    #
    # plugins/jni_fs/<name> is where these now go, because two things read them: the native libhdfs
    # reader through start_be.sh's system class path, and every Java plugin through PluginRegistry,
    # which appends them to the plugin's own classpath. The older lib/<name> is left alone on
    # purpose - start_be.sh still reads it as a fallback so an in-place upgrade keeps resolving
    # oss-hdfs:// and jfs:// for the native reader until the new tree is laid down.
    if [[ "${BUILD_JUICEFS}" == 'ON' ]]; then
        rm -rf "${DORIS_OUTPUT}/be/plugins/jni_fs/juicefs"
    fi
    if [[ "${BUILD_JINDOFS}" == 'ON' ]]; then
        rm -rf "${DORIS_OUTPUT}/be/plugins/jni_fs/jindofs"
    fi

    # Third-party filesystem jars (JuiceFS, JindoFS) are packaged by post-build.sh
    bash "${DORIS_HOME}/post-build.sh" --be --output "${DORIS_OUTPUT}"

    # plugins/jni_fs/{jindofs,juicefs} serves two readers, and nothing is copied anywhere.
    #
    # start_be.sh puts it on the system classpath, where the hadoop drop that C++ libhdfs loads
    # lives - resolving oss-hdfs:// and jfs:// for that reader is what these jars were originally
    # packaged for. PluginRegistry ALSO appends them to each Java plugin's own classpath (BE config
    # jni_plugin_fs_dir), because a plugin classloader cannot see the system classpath: without
    # them paimon-scanner's paimon-jindo adapter and iceberg-metadata-scanner's fs.oss.impl resolve
    # to nothing, and no plugin at all can open a jfs:// path. This build used to copy the JindoFS
    # jars into the iceberg and paimon plugin directories for that reason; appending one shared
    # directory covers every plugin instead, costs no disk, and adds no duplicate classes for the
    # check below to adjudicate - which matters because the JuiceFS SDK is a 180 MB fat jar that
    # collides with about 1500 classes in a lake-format plugin.
    #
    # CAVEAT, unchanged by that: jindo-core carries a native library, and a JVM binds one of those
    # to exactly one classloader. A process that reaches jindo from two plugins at once, or from a
    # plugin AND through libhdfs, makes the second bind and it fails. That is inherent to plugin
    # isolation - a single shared loader for these jars is impossible, since they need the hadoop
    # that lives inside each plugin.

    # The layout the isolation rests on, checked on the tree that was just deployed: the SPI jars
    # carry nothing but the SPI, no plugin ships a copy of them, no plugin directory holds the same
    # class twice, no class reachable from a plugin's Doris code - or from a filesystem or
    # credential provider Doris names by string - is missing, and the jar declaring the service
    # carries the API version this build serves. Four of the five have caught a real regression,
    # and none of them is visible in a compiler error - a dependency that turns into
    # <scope>provided</scope> by accident builds fine and fails in a user's query.
    #
    # What the check does NOT see, so that nobody reads more into a green run than is there:
    #   - plugins/jni_fs, which PluginRuntime appends to every plugin classloader. Those jars are
    #     not in a plugin directory, so neither the duplicate scan nor the closure walk covers
    #     them - and they do collide with a lake-format plugin on ~1500 class names, resolved
    #     deterministically by the plugin's own jars coming first in the URL list.
    #   - anything reached only by ServiceLoader or by a reflective lookup that Doris does not
    #     make itself. See the header of check_plugin_layout.py.
    #
    # Here rather than in a GitHub workflow because it needs a built output tree, which only a full
    # BE build produces. python3 is not a build requirement, so its absence is a warning.
    if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 ]] && command -v python3 > /dev/null; then
        # jdeps does the closure check and ships with the JDK, but is not necessarily on PATH.
        layout_check_status=0
        PATH="${JAVA_HOME:+${JAVA_HOME}/bin:}${PATH}" python3 \
            "${DORIS_HOME}/tools/be-java-plugins/check_plugin_layout.py" \
            "${BE_JAVA_SPI_DIR}" "${BE_JAVA_PLUGINS_DIR}" || layout_check_status="${?}"
        if [[ "${layout_check_status}" -eq 1 ]]; then
            echo "Error: the plugin tree just deployed breaks the isolation rules; see above."
            exit 1
        elif [[ "${layout_check_status}" -ne 0 ]]; then
            # 2 is "could not run" - no jdeps, nothing deployed - which is not a verdict on
            # the tree and must not fail a build that is otherwise complete.
            echo "WARN: the Java plugin layout check did not run (exit ${layout_check_status})"
        fi
    elif [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 ]]; then
        echo "WARN: python3 not found, skipping the Java plugin layout check"
    fi

    cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/be/www"/
    copy_common_files "${DORIS_OUTPUT}/be/"
    mkdir -p "${DORIS_OUTPUT}/be/log"
    mkdir -p "${DORIS_OUTPUT}/be/storage"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/jdbc_drivers/"
    # Mirrors the FE drop point above; every BE must hold the same ADBC driver file the FE holds.
    mkdir -p "${DORIS_OUTPUT}/be/plugins/adbc_drivers/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/java_udf/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/python_udf/"
    # Mirrors the FE drop point above; the BE JNI scanner loads the same Trino plugins independently.
    mkdir -p "${DORIS_OUTPUT}/be/plugins/trino_plugins/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/hadoop_conf/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/java_extensions/"
    cp -r -p "${DORIS_HOME}/be/src/udf/python/python_server.py" "${DORIS_OUTPUT}/be/plugins/python_udf/"
fi

if [[ "${BUILD_BE_CDC_CLIENT}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/be/lib/cdc_client"
    cd "${DORIS_HOME}/fs_brokers/cdc_client"
    bash ./build.sh
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
    # copy-dependencies writes only the transitive deps to target/lib; the patched
    # org.apache.hadoop.fs.FileSystem lives in the module's own jar at target/. Without this the
    # meta-service would run on the vanilla class and silently ignore doris.fs.cache.key.<scheme>.
    # cloud/script/start.sh loads it ahead of the vanilla hadoop jars beside it.
    if [[ -f "${HADOOP_DEPS_JAR_DIR}/${HADOOP_DEPS_NAME}.jar" ]]; then
        mkdir -p "${DORIS_HOME}/cloud/output/lib/hadoop_hdfs"
        cp "${HADOOP_DEPS_JAR_DIR}/${HADOOP_DEPS_NAME}.jar" "${DORIS_HOME}/cloud/output/lib/hadoop_hdfs/"
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
