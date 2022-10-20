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

. "${DORIS_HOME}/env.sh"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     [no option]        build all components
     --fe               build Frontend and Spark DPP application
     --be               build Backend
     --meta-tool        build Backend meta tool
     --broker           build Broker
     --audit            build audit loader
     --spark-dpp        build Spark DPP application
     --hive-udf         build Hive UDF library for Spark Load
     --java-udf         build Java UDF library
     --clean            clean and build target
     -j                 build Backend parallel

  Environment variables:
    USE_AVX2            If the CPU does not support AVX2 instruction set, please set USE_AVX2=0. Default is ON.
    STRIP_DEBUG_INFO    If set STRIP_DEBUG_INFO=ON, the debug information in the compiled binaries will be stored separately in the 'be/lib/debug_info' directory. Default is OFF.

  Eg.
    $0                                      build all
    $0 --be                                 build Backend
    $0 --meta-tool                          build Backend meta tool
    $0 --fe --clean                         clean and build Frontend and Spark Dpp application
    $0 --fe --be --clean                    clean and build Frontend, Spark Dpp application and Backend
    $0 --spark-dpp                          build Spark DPP application alone
    $0 --broker                             build Broker
    $0 --be --fe --java-udf                 build Backend, Frontend, Spark Dpp application and Java UDF library

    USE_AVX2=0 $0 --be                      build Backend and not using AVX2 instruction.
    USE_AVX2=0 STRIP_DEBUG_INFO=ON $0       build all and not using AVX2 instruction, and strip the debug info for Backend
  "
    exit 1
}

clean_gensrc() {
    pushd "${DORIS_HOME}/gensrc"
    make clean
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

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'fe' \
    -l 'be' \
    -l 'broker' \
    -l 'audit' \
    -l 'meta-tool' \
    -l 'spark-dpp' \
    -l 'java-udf' \
    -l 'hive-udf' \
    -l 'clean' \
    -l 'help' \
    -o 'hj:' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

PARALLEL="$(($(nproc) / 4 + 1))"
BUILD_FE=0
BUILD_BE=0
BUILD_BROKER=0
BUILD_AUDIT=0
BUILD_META_TOOL='OFF'
BUILD_SPARK_DPP=0
BUILD_JAVA_UDF=0
BUILD_HIVE_UDF=0
CLEAN=0
HELP=0
PARAMETER_COUNT="$#"
PARAMETER_FLAG=0
if [[ "$#" == 1 ]]; then
    # default
    BUILD_FE=1
    BUILD_BE=1
    BUILD_BROKER=1
    BUILD_AUDIT=1
    BUILD_META_TOOL='OFF'
    BUILD_SPARK_DPP=1
    BUILD_JAVA_UDF=0 # TODO: open it when ready
    BUILD_HIVE_UDF=1
    CLEAN=0
else
    while true; do
        case "$1" in
        --fe)
            BUILD_FE=1
            BUILD_SPARK_DPP=1
            shift
            ;;
        --be)
            BUILD_BE=1
            shift
            ;;
        --broker)
            BUILD_BROKER=1
            shift
            ;;
        --audit)
            BUILD_AUDIT=1
            shift
            ;;
        --meta-tool)
            BUILD_META_TOOL='ON'
            shift
            ;;
        --spark-dpp)
            BUILD_SPARK_DPP=1
            shift
            ;;
        --java-udf)
            BUILD_JAVA_UDF=1
            BUILD_FE=1
            BUILD_SPARK_DPP=1
            shift
            ;;
        --hive-udf)
            BUILD_HIVE_UDF=1
            shift
            ;;
        --clean)
            CLEAN=1
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
        BUILD_BROKER=1
        BUILD_AUDIT=1
        BUILD_META_TOOL='ON'
        BUILD_SPARK_DPP=1
        BUILD_HIVE_UDF=1
        CLEAN=0
    fi
fi

if [[ "${HELP}" -eq 1 ]]; then
    usage
    exit
fi
# build thirdparty libraries if necessary
if [[ ! -f "${DORIS_THIRDPARTY}/installed/lib/libbacktrace.a" ]]; then
    echo "Thirdparty libraries need to be build ..."
    # need remove all installed pkgs because some lib like lz4 will throw error if its lib alreay exists
    rm -rf "${DORIS_THIRDPARTY}/installed"
    "${DORIS_THIRDPARTY}/build-thirdparty.sh" -j "${PARALLEL}"
fi

if [[ "${CLEAN}" -eq 1 && "${BUILD_BE}" -eq 0 && "${BUILD_FE}" -eq 0 && "${BUILD_SPARK_DPP}" -eq 0 ]]; then
    clean_gensrc
    clean_be
    clean_fe
    exit 0
fi

if [[ -z "${WITH_MYSQL}" ]]; then
    WITH_MYSQL='OFF'
fi
if [[ -z "${GLIBC_COMPATIBILITY}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        GLIBC_COMPATIBILITY='ON'
    else
        GLIBC_COMPATIBILITY='OFF'
    fi
fi
if [[ -z "${USE_AVX2}" ]]; then
    USE_AVX2='ON'
fi
if [[ -z "${WITH_LZO}" ]]; then
    WITH_LZO='OFF'
fi
if [[ -z "${USE_LIBCPP}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_LIBCPP='OFF'
    else
        USE_LIBCPP='ON'
    fi
fi
if [[ -z "${STRIP_DEBUG_INFO}" ]]; then
    STRIP_DEBUG_INFO='OFF'
fi
if [[ -z "${USE_MEM_TRACKER}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_MEM_TRACKER='ON'
    else
        USE_MEM_TRACKER='OFF'
    fi
fi
if [[ -z "${USE_JEMALLOC}" ]]; then
    if [[ "$(uname -s)" != 'Darwin' ]]; then
        USE_JEMALLOC='ON'
    else
        USE_JEMALLOC='OFF'
    fi
fi
if [[ -z "${STRICT_MEMORY_USE}" ]]; then
    STRICT_MEMORY_USE='OFF'
fi

if [[ -z "${USE_DWARF}" ]]; then
    USE_DWARF='OFF'
fi

if [[ -z "${RECORD_COMPILER_SWITCHES}" ]]; then
    RECORD_COMPILER_SWITCHES='OFF'
fi

echo "Get params:
    BUILD_FE            -- ${BUILD_FE}
    BUILD_BE            -- ${BUILD_BE}
    BUILD_BROKER        -- ${BUILD_BROKER}
    BUILD_AUDIT         -- ${BUILD_AUDIT}
    BUILD_META_TOOL     -- ${BUILD_META_TOOL}
    BUILD_SPARK_DPP     -- ${BUILD_SPARK_DPP}
    BUILD_JAVA_UDF      -- ${BUILD_JAVA_UDF}
    BUILD_HIVE_UDF      -- ${BUILD_HIVE_UDF}
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
    WITH_MYSQL          -- ${WITH_MYSQL}
    WITH_LZO            -- ${WITH_LZO}
    GLIBC_COMPATIBILITY -- ${GLIBC_COMPATIBILITY}
    USE_AVX2            -- ${USE_AVX2}
    USE_LIBCPP          -- ${USE_LIBCPP}
    USE_DWARF           -- ${USE_DWARF}
    STRIP_DEBUG_INFO    -- ${STRIP_DEBUG_INFO}
    USE_MEM_TRACKER     -- ${USE_MEM_TRACKER}
    USE_JEMALLOC        -- ${USE_JEMALLOC}
    STRICT_MEMORY_USE   -- ${STRICT_MEMORY_USE}
"

# Clean and build generated code
if [[ "${CLEAN}" -eq 1 ]]; then
    clean_gensrc
fi
echo "Build generated code"
cd "${DORIS_HOME}/gensrc"
# DO NOT using parallel make(-j) for gensrc
make

# Assesmble FE modules
FE_MODULES=''
BUILD_DOCS='OFF'
modules=("")
if [[ "${BUILD_FE}" -eq 1 ]]; then
    modules+=("fe-common")
    modules+=("fe-core")
    BUILD_DOCS='ON'
fi
if [[ "${BUILD_SPARK_DPP}" -eq 1 ]]; then
    modules+=("fe-common")
    modules+=("spark-dpp")
fi
if [[ "${BUILD_JAVA_UDF}" -eq 1 ]]; then
    modules+=("java-udf")
fi
if [[ "${BUILD_HIVE_UDF}" -eq 1 ]]; then
    modules+=("fe-common")
    modules+=("hive-udf")
fi
FE_MODULES="$(
    IFS=','
    echo "${modules[*]}"
)"

# Clean and build Backend
if [[ "${BUILD_BE}" -eq 1 ]]; then
    if [[ -e "${DORIS_HOME}/gensrc/build/gen_cpp/version.h" ]]; then
        rm -f "${DORIS_HOME}/gensrc/build/gen_cpp/version.h"
    fi
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR="${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}"
    if [[ "${CLEAN}" -eq 1 ]]; then
        clean_be
    fi
    MAKE_PROGRAM="$(which "${BUILD_SYSTEM}")"
    echo "-- Make program: ${MAKE_PROGRAM}"
    echo "-- Use ccache: ${CMAKE_USE_CCACHE}"
    echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"

    mkdir -p "${CMAKE_BUILD_DIR}"
    cd "${CMAKE_BUILD_DIR}"
    "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DMAKE_TEST=OFF \
        ${CMAKE_USE_CCACHE:+${CMAKE_USE_CCACHE}} \
        -DWITH_MYSQL="${WITH_MYSQL}" \
        -DWITH_LZO="${WITH_LZO}" \
        -DUSE_LIBCPP="${USE_LIBCPP}" \
        -DBUILD_META_TOOL="${BUILD_META_TOOL}" \
        -DBUILD_JAVA_UDF="${BUILD_JAVA_UDF}" \
        -DSTRIP_DEBUG_INFO="${STRIP_DEBUG_INFO}" \
        -DUSE_DWARF="${USE_DWARF}" \
        -DUSE_MEM_TRACKER="${USE_MEM_TRACKER}" \
        -DUSE_JEMALLOC="${USE_JEMALLOC}" \
        -DSTRICT_MEMORY_USE="${STRICT_MEMORY_USE}" \
        -DUSE_AVX2="${USE_AVX2}" \
        -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
        -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
        "${DORIS_HOME}/be"
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    cd "${DORIS_HOME}"
fi

if [[ "${BUILD_DOCS}" = "ON" ]]; then
    # Build docs, should be built before Frontend
    echo "Build docs"
    cd "${DORIS_HOME}/docs"
    ./build_help_zip.sh
    cd "${DORIS_HOME}"
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
        "${NPM}" install --legacy-peer-deps
        "${NPM}" run build
    fi
    echo "ui dist: ${ui_dist}"
    rm -rf "${DORIS_HOME}/fe/fe-core/src/main/resources/static"
    mkdir -p "${DORIS_HOME}/fe/fe-core/src/main/resources/static"
    cp -r "${ui_dist}"/* "${DORIS_HOME}/fe/fe-core/src/main/resources/static"/
}

# FE UI must be built before building FE
if [[ "${BUILD_FE}" -eq 1 ]]; then
    build_ui
fi

# Clean and build Frontend
if [[ "${FE_MODULES}" != '' ]]; then
    echo "Build Frontend Modules: ${FE_MODULES}"
    cd "${DORIS_HOME}/fe"
    if [[ "${CLEAN}" -eq 1 ]]; then
        clean_fe
    fi
    "${MVN_CMD}" package -pl ${FE_MODULES:+${FE_MODULES}} -DskipTests
    cd "${DORIS_HOME}"
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/output/
mkdir -p "${DORIS_OUTPUT}"

# Copy Frontend and Backend
if [[ "${BUILD_FE}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/fe/bin" "${DORIS_OUTPUT}/fe/conf" \
        "${DORIS_OUTPUT}/fe/webroot" "${DORIS_OUTPUT}/fe/lib"

    cp -r -p "${DORIS_HOME}/bin"/*_fe.sh "${DORIS_OUTPUT}/fe/bin"/
    cp -r -p "${DORIS_HOME}/conf/fe.conf" "${DORIS_OUTPUT}/fe/conf"/
    cp -r -p "${DORIS_HOME}/conf/ldap.conf" "${DORIS_OUTPUT}/fe/conf"/
    cp -r -p "${DORIS_HOME}/conf"/*.xml "${DORIS_OUTPUT}/fe/conf"/
    rm -rf "${DORIS_OUTPUT}/fe/lib"/*
    cp -r -p "${DORIS_HOME}/fe/fe-core/target/lib"/* "${DORIS_OUTPUT}/fe/lib"/
    rm -f "${DORIS_OUTPUT}/fe/lib/palo-fe.jar"
    cp -r -p "${DORIS_HOME}/fe/fe-core/target/doris-fe.jar" "${DORIS_OUTPUT}/fe/lib"/
    cp -r -p "${DORIS_HOME}/docs/build/help-resource.zip" "${DORIS_OUTPUT}/fe/lib"/
    cp -r -p "${DORIS_HOME}/webroot/static" "${DORIS_OUTPUT}/fe/webroot"/

    cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/fe/webroot/static"/
    copy_common_files "${DORIS_OUTPUT}/fe/"
    mkdir -p "${DORIS_OUTPUT}/fe/log"
    mkdir -p "${DORIS_OUTPUT}/fe/doris-meta"
fi

if [[ "${BUILD_SPARK_DPP}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/fe/spark-dpp"
    rm -rf "${DORIS_OUTPUT}/fe/spark-dpp"/*
    cp -r -p "${DORIS_HOME}/fe/spark-dpp/target"/spark-dpp-*-jar-with-dependencies.jar "${DORIS_OUTPUT}/fe/spark-dpp"/
fi

if [[ "${BUILD_BE}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/be/bin" \
        "${DORIS_OUTPUT}/be/conf" \
        "${DORIS_OUTPUT}/be/lib" \
        "${DORIS_OUTPUT}/be/www" \
        "${DORIS_OUTPUT}/udf/lib" \
        "${DORIS_OUTPUT}/udf/include"

    cp -r -p "${DORIS_HOME}/be/output/bin"/* "${DORIS_OUTPUT}/be/bin"/
    cp -r -p "${DORIS_HOME}/be/output/conf"/* "${DORIS_OUTPUT}/be/conf"/

    # Fix Killed: 9 error on MacOS (arm64).
    # See: https://stackoverflow.com/questions/67378106/mac-m1-cping-binary-over-another-results-in-crash
    rm -f "${DORIS_OUTPUT}/be/lib/doris_be"
    cp -r -p "${DORIS_HOME}/be/output/lib/doris_be" "${DORIS_OUTPUT}/be/lib"/

    # make a soft link palo_be point to doris_be, for forward compatibility
    cd "${DORIS_OUTPUT}/be/lib"
    rm -f palo_be
    ln -s doris_be palo_be
    cd -

    if [[ "${BUILD_META_TOOL}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/meta_tool" "${DORIS_OUTPUT}/be/lib"/
    fi

    cp -r -p "${DORIS_HOME}/be/output/udf"/*.a "${DORIS_OUTPUT}/udf/lib"/
    cp -r -p "${DORIS_HOME}/be/output/udf/include"/* "${DORIS_OUTPUT}/udf/include"/
    cp -r -p "${DORIS_HOME}/webroot/be"/* "${DORIS_OUTPUT}/be/www"/
    if [[ "${STRIP_DEBUG_INFO}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/debug_info" "${DORIS_OUTPUT}/be/lib"/
    fi

    java_udf_path="${DORIS_HOME}/fe/java-udf/target/java-udf-jar-with-dependencies.jar"
    if [[ -f "${java_udf_path}" ]]; then
        cp "${java_udf_path}" "${DORIS_OUTPUT}/be/lib"/
    fi

    cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/be/www"/
    copy_common_files "${DORIS_OUTPUT}/be/"
    mkdir -p "${DORIS_OUTPUT}/be/log"
    mkdir -p "${DORIS_OUTPUT}/be/storage"
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

if [[ "${BUILD_AUDIT}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/audit_loader"

    cd "${DORIS_HOME}/fe_plugins/auditloader"
    ./build.sh
    rm -rf "${DORIS_OUTPUT}/audit_loader"/*
    cp -r -p "${DORIS_HOME}/fe_plugins/auditloader/output"/* "${DORIS_OUTPUT}/audit_loader"/
    cd "${DORIS_HOME}"
fi

echo "***************************************"
echo "Successfully build Doris"
echo "***************************************"

if [[ -n "${DORIS_POST_BUILD_HOOK}" ]]; then
    eval "${DORIS_POST_BUILD_HOOK}"
fi

exit 0
