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
# Eg:
#    sh build.sh                            build all
#    sh build.sh  --be                      build Backend without clean
#    sh build.sh  --fe --clean              clean and build Frontend and Spark Dpp application, without web UI
#    sh build.sh  --fe --be --clean         clean and build Frontend, Spark Dpp application and Backend, without web UI
#    sh build.sh  --spark-dpp               build Spark DPP application alone
#    sh build.sh  --fe --ui                 build Frontend web ui with npm
#    sh build.sh  --fe --be --ui --clean    clean and build Frontend, Spark Dpp application, Backend and web UI
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}

. ${DORIS_HOME}/env.sh

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --be               build Backend
     --fe               build Frontend and Spark Dpp application
     --broker           build Broker
     --ui               build Frontend web ui with npm
     --spark-dpp        build Spark DPP application
     --clean            clean and build target
     -j                 build Backend parallel

  Environment variables:
    USE_AVX2            If the CPU does not support AVX2 instruction set, please set USE_AVX2=0. Default is ON.
    BUILD_META_TOOL     If set BUILD_META_TOOL=OFF, the output meta_tools binaries will not be compiled. Default is OFF.
    STRIP_DEBUG_INFO    If set STRIP_DEBUG_INFO=ON, the debug information in the compiled binaries will be stored separately in the 'be/lib/debug_info' directory. Default is OFF.

  Eg.
    $0                                      build all
    $0 --be                                 build Backend without clean
    $0 --fe --clean                         clean and build Frontend and Spark Dpp application, without web UI
    $0 --fe --be --clean                    clean and build Frontend, Spark Dpp application and Backend, without web UI
    $0 --spark-dpp                          build Spark DPP application alone
    $0 --fe --ui                            build Frontend web ui with npm
    $0 --broker                             build Broker

    USE_AVX2=0 $0 --be                      build Backend and not using AVX2 instruction.
    USE_AVX2=0 STRIP_DEBUG_INFO=ON $0       build all and not using AVX2 instruction, and strip the debug info.
  "
  exit 1
}

clean_gensrc() {
    pushd ${DORIS_HOME}/gensrc
    make clean
    rm -rf ${DORIS_HOME}/fe/fe-core/target
    popd
}

clean_be() {
    pushd ${DORIS_HOME}

    # "build.sh --clean" just cleans and exits, however CMAKE_BUILD_DIR is set
    # while building be.
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}
    CMAKE_BUILD_DIR=${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}

    rm -rf $CMAKE_BUILD_DIR
    rm -rf ${DORIS_HOME}/be/output/
    popd
}

clean_fe() {
    pushd ${DORIS_HOME}/fe
    ${MVN_CMD} clean
    popd
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'be' \
  -l 'fe' \
  -l 'broker' \
  -l 'ui' \
  -l 'spark-dpp' \
  -l 'clean' \
  -l 'help' \
  -o 'hj:' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

PARALLEL=$[$(nproc)/4+1]
BUILD_BE=
BUILD_FE=
BUILD_BROKER=
BUILD_UI=
BUILD_SPARK_DPP=
CLEAN=
HELP=0
PARAMETER_COUNT=$#
PARAMETER_FLAG=0
if [ $# == 1 ] ; then
    # default
    BUILD_BE=1
    BUILD_FE=1
    BUILD_BROKER=1
    BUILD_UI=1
    BUILD_SPARK_DPP=1
    CLEAN=0
else
    BUILD_BE=0
    BUILD_FE=0
    BUILD_BROKER=0
    BUILD_UI=0
    BUILD_SPARK_DPP=0
    CLEAN=0
    while true; do
        case "$1" in
            --be) BUILD_BE=1 ; shift ;;
            --fe) BUILD_FE=1 ; shift ;;
            --ui) BUILD_UI=1 ; shift ;;
            --broker) BUILD_BROKER=1 ; shift ;;
            --spark-dpp) BUILD_SPARK_DPP=1 ; shift ;;
            --clean) CLEAN=1 ; shift ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            -j) PARALLEL=$2; PARAMETER_FLAG=1; shift 2 ;;
            --) shift ;  break ;;
            *) echo "Internal error" ; exit 1 ;;
        esac
    done
    #only ./build.sh -j xx then build all 
    if [[ ${PARAMETER_COUNT} -eq 3 ]] && [[ ${PARAMETER_FLAG} -eq 1 ]];then
        BUILD_BE=1
        BUILD_FE=1
        BUILD_BROKER=1
        BUILD_UI=1
        BUILD_SPARK_DPP=1
        CLEAN=0
    fi
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi
# build thirdparty libraries if necessary
if [[ ! -f ${DORIS_THIRDPARTY}/installed/lib/libbacktrace.a ]]; then
    echo "Thirdparty libraries need to be build ..."
    # need remove all installed pkgs because some lib like lz4 will throw error if its lib alreay exists
    rm -rf ${DORIS_THIRDPARTY}/installed
    ${DORIS_THIRDPARTY}/build-thirdparty.sh -j $PARALLEL
fi

if [ ${CLEAN} -eq 1 -a ${BUILD_BE} -eq 0 -a ${BUILD_FE} -eq 0 -a ${BUILD_SPARK_DPP} -eq 0 ]; then
    clean_gensrc
    clean_be
    clean_fe
    exit 0
fi

if [[ -z ${WITH_MYSQL} ]]; then
    WITH_MYSQL=OFF
fi
if [[ -z ${GLIBC_COMPATIBILITY} ]]; then
    GLIBC_COMPATIBILITY=ON
fi
if [[ -z ${USE_AVX2} ]]; then
    USE_AVX2=ON
fi
if [[ -z ${WITH_LZO} ]]; then
    WITH_LZO=OFF
fi
if [[ -z ${USE_LIBCPP} ]]; then
    USE_LIBCPP=OFF
fi
if [[ -z ${BUILD_META_TOOL} ]]; then
    BUILD_META_TOOL=OFF
fi
if [[ -z ${USE_LLD} ]]; then
    USE_LLD=OFF
fi
if [[ -z ${STRIP_DEBUG_INFO} ]]; then
    STRIP_DEBUG_INFO=OFF
fi

echo "Get params:
    BUILD_BE            -- $BUILD_BE
    BUILD_FE            -- $BUILD_FE
    BUILD_BROKER        -- $BUILD_BROKER
    BUILD_UI            -- $BUILD_UI
    BUILD_SPARK_DPP     -- $BUILD_SPARK_DPP
    PARALLEL            -- $PARALLEL
    CLEAN               -- $CLEAN
    WITH_MYSQL          -- $WITH_MYSQL
    WITH_LZO            -- $WITH_LZO
    GLIBC_COMPATIBILITY -- $GLIBC_COMPATIBILITY
    USE_AVX2            -- $USE_AVX2
    USE_LIBCPP          -- $USE_LIBCPP
    BUILD_META_TOOL     -- $BUILD_META_TOOL
    USE_LLD             -- $USE_LLD
    STRIP_DEBUG_INFO    -- $STRIP_DEBUG_INFO
"

# Clean and build generated code
if [ ${CLEAN} -eq 1 ]; then
    clean_gensrc
fi
echo "Build generated code"
cd ${DORIS_HOME}/gensrc
# DO NOT using parallel make(-j) for gensrc
python --version
make

# Clean and build Backend
if [ ${BUILD_BE} -eq 1 ] ; then
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}
    if [ ${CLEAN} -eq 1 ]; then
        clean_be
    fi
    MAKE_PROGRAM="$(which "${BUILD_SYSTEM}")"
    echo "-- Make program: ${MAKE_PROGRAM}"
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    ${CMAKE_CMD} -G "${GENERATOR}"  \
            -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
            -DMAKE_TEST=OFF \
            ${CMAKE_USE_CCACHE} \
            -DWITH_MYSQL=${WITH_MYSQL} \
            -DWITH_LZO=${WITH_LZO} \
            -DUSE_LIBCPP=${USE_LIBCPP} \
            -DBUILD_META_TOOL=${BUILD_META_TOOL} \
            -DUSE_LLD=${USE_LLD} \
            -DSTRIP_DEBUG_INFO=${STRIP_DEBUG_INFO} \
            -DUSE_AVX2=${USE_AVX2} \
            -DGLIBC_COMPATIBILITY=${GLIBC_COMPATIBILITY} ../
    ${BUILD_SYSTEM} -j ${PARALLEL}
    ${BUILD_SYSTEM} install
    cd ${DORIS_HOME}
fi

# Build docs, should be built before Frontend
echo "Build docs"
cd ${DORIS_HOME}/docs
./build_help_zip.sh
cd ${DORIS_HOME}

# Assesmble FE modules
FE_MODULES=
if [ ${BUILD_FE} -eq 1 -o ${BUILD_SPARK_DPP} -eq 1 ]; then
    if [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        FE_MODULES="fe-common,spark-dpp"
    fi
    if [ ${BUILD_FE} -eq 1 ]; then
        FE_MODULES="fe-common,spark-dpp,fe-core"
    fi
fi

function build_ui() {
    NPM=npm
    if ! ${NPM} --version; then
        echo "Error: npm is not found"
        exit 1
    fi
    if [[ ! -z ${CUSTOM_NPM_REGISTRY} ]]; then
        ${NPM} config set registry ${CUSTOM_NPM_REGISTRY}
        npm_reg=`${NPM} get registry`
        echo "NPM registry: $npm_reg"
    fi

    echo "Build Frontend UI"
    ui_dist=${DORIS_HOME}/ui/dist/
    if [[ ! -z ${CUSTOM_UI_DIST} ]]; then
        ui_dist=${CUSTOM_UI_DIST}
    else
        cd ${DORIS_HOME}/ui
        ${NPM} install
        ${NPM} run build
    fi
    echo "ui dist: ${ui_dist}"
    rm -rf ${DORIS_HOME}/fe/fe-core/src/main/resources/static/
    mkdir -p ${DORIS_HOME}/fe/fe-core/src/main/resources/static
    cp -r ${ui_dist}/* ${DORIS_HOME}/fe/fe-core/src/main/resources/static
}

# FE UI must be built before building FE
if [ ${BUILD_UI} -eq 1 ] ; then
    build_ui
fi

# Clean and build Frontend
if [ ${FE_MODULES}x != ""x ]; then
    echo "Build Frontend Modules: $FE_MODULES"
    cd ${DORIS_HOME}/fe
    if [ ${CLEAN} -eq 1 ]; then
        clean_fe
    fi
    ${MVN_CMD} package -pl ${FE_MODULES} -DskipTests
    cd ${DORIS_HOME}
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/output/
mkdir -p ${DORIS_OUTPUT}

# Copy Frontend and Backend
if [ ${BUILD_FE} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/fe/bin ${DORIS_OUTPUT}/fe/conf \
               ${DORIS_OUTPUT}/fe/webroot/ ${DORIS_OUTPUT}/fe/lib/

    cp -r -p ${DORIS_HOME}/bin/*_fe.sh ${DORIS_OUTPUT}/fe/bin/
    cp -r -p ${DORIS_HOME}/conf/fe.conf ${DORIS_OUTPUT}/fe/conf/
    rm -rf ${DORIS_OUTPUT}/fe/lib/*
    cp -r -p ${DORIS_HOME}/fe/fe-core/target/lib/* ${DORIS_OUTPUT}/fe/lib/
    rm -f ${DORIS_OUTPUT}/fe/lib/palo-fe.jar
    cp -r -p ${DORIS_HOME}/fe/fe-core/target/doris-fe.jar ${DORIS_OUTPUT}/fe/lib/
    cp -r -p ${DORIS_HOME}/docs/build/help-resource.zip ${DORIS_OUTPUT}/fe/lib/
    cp -r -p ${DORIS_HOME}/webroot/static ${DORIS_OUTPUT}/fe/webroot/

    cp -r -p ${DORIS_THIRDPARTY}/installed/webroot/* ${DORIS_OUTPUT}/fe/webroot/static/
    mkdir -p ${DORIS_OUTPUT}/fe/log
    mkdir -p ${DORIS_OUTPUT}/fe/doris-meta
fi

if [ ${BUILD_SPARK_DPP} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/fe/spark-dpp/
    rm -rf ${DORIS_OUTPUT}/fe/spark-dpp/*
    cp -r -p ${DORIS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${DORIS_OUTPUT}/fe/spark-dpp/
fi

if [ ${BUILD_BE} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/be/bin  \
               ${DORIS_OUTPUT}/be/conf \
               ${DORIS_OUTPUT}/be/lib/ \
               ${DORIS_OUTPUT}/be/www  \
               ${DORIS_OUTPUT}/udf/lib \
               ${DORIS_OUTPUT}/udf/include

    cp -r -p ${DORIS_HOME}/be/output/bin/* ${DORIS_OUTPUT}/be/bin/
    cp -r -p ${DORIS_HOME}/be/output/conf/* ${DORIS_OUTPUT}/be/conf/
    cp -r -p ${DORIS_HOME}/be/output/lib/* ${DORIS_OUTPUT}/be/lib/
    # make a soft link palo_be point to doris_be, for forward compatibility
    cd ${DORIS_OUTPUT}/be/lib && rm -f palo_be && ln -s doris_be palo_be && cd -
    cp -r -p ${DORIS_HOME}/be/output/udf/*.a ${DORIS_OUTPUT}/udf/lib/
    cp -r -p ${DORIS_HOME}/be/output/udf/include/* ${DORIS_OUTPUT}/udf/include/
    cp -r -p ${DORIS_HOME}/webroot/be/* ${DORIS_OUTPUT}/be/www/

    cp -r -p ${DORIS_THIRDPARTY}/installed/webroot/* ${DORIS_OUTPUT}/be/www/
    mkdir -p ${DORIS_OUTPUT}/be/log
    mkdir -p ${DORIS_OUTPUT}/be/storage


fi

if [ ${BUILD_BROKER} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/apache_hdfs_broker

    cd ${DORIS_HOME}/fs_brokers/apache_hdfs_broker/
    ./build.sh
    rm -rf ${DORIS_OUTPUT}/apache_hdfs_broker/*
    cp -r -p ${DORIS_HOME}/fs_brokers/apache_hdfs_broker/output/apache_hdfs_broker/* ${DORIS_OUTPUT}/apache_hdfs_broker/
    cd ${DORIS_HOME}
fi


echo "***************************************"
echo "Successfully build Doris"
echo "***************************************"

if [[ ! -z ${DORIS_POST_BUILD_HOOK} ]]; then
    eval ${DORIS_POST_BUILD_HOOK}
fi

exit 0
