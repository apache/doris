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
# This script is used to compile Apache Doris(incubating)
# Usage:
#    sh build.sh        build both Backend and Frontend.
#    sh build.sh -clean clean previous output and build.
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}

. ${DORIS_HOME}/env.sh

# build thirdparty libraries if necessary
if [[ ! -f ${DORIS_THIRDPARTY}/installed/lib/libs2.a ]]; then
    echo "Thirdparty libraries need to be build ..."
    ${DORIS_THIRDPARTY}/build-thirdparty.sh
fi

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --be               build Backend
     --fe               build Frontend and Spark Dpp application
     --ui               build Frontend web ui with npm
     --spark-dpp        build Spark DPP application
     --clean            clean and build target

  Eg.
    $0                                      build all
    $0 --be                                 build Backend without clean
    $0 --fe --clean                         clean and build Frontend and Spark Dpp application
    $0 --fe --be --clean                    clean and build Frontend, Spark Dpp application and Backend
    $0 --spark-dpp                          build Spark DPP application alone
    $0 --fe --ui                         build Frontend web ui with npm
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'be' \
  -l 'fe' \
  -l 'ui' \
  -l 'spark-dpp' \
  -l 'clean' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_BE=
BUILD_FE=
BUILD_UI=
BUILD_SPARK_DPP=
CLEAN=
RUN_UT=
HELP=0
if [ $# == 1 ] ; then
    # default
    BUILD_BE=1
    BUILD_FE=1
    BUILD_UI=1
    BUILD_SPARK_DPP=1
    CLEAN=0
    RUN_UT=0
else
    BUILD_BE=0
    BUILD_FE=0
    BUILD_UI=0
    BUILD_SPARK_DPP=0
    CLEAN=0
    RUN_UT=0
    while true; do
        case "$1" in
            --be) BUILD_BE=1 ; shift ;;
            --fe) BUILD_FE=1 ; shift ;;
            --ui) BUILD_UI=1 ; shift ;;
            --spark-dpp) BUILD_SPARK_DPP=1 ; shift ;;
            --clean) CLEAN=1 ; shift ;;
            --ut) RUN_UT=1   ; shift ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            --) shift ;  break ;;
            *) ehco "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

if [ ${CLEAN} -eq 1 -a ${BUILD_BE} -eq 0 -a ${BUILD_FE} -eq 0 -a ${BUILD_SPARK_DPP} -eq 0 ]; then
    echo "--clean can not be specified without --fe or --be or --spark-dpp"
    exit 1
fi

if [[ -z ${WITH_MYSQL} ]]; then
    WITH_MYSQL=OFF
fi
if [[ -z ${WITH_LZO} ]]; then
    WITH_LZO=OFF
fi

echo "Get params:
    BUILD_BE            -- $BUILD_BE
    BUILD_FE            -- $BUILD_FE
    BUILD_UI            -- $BUILD_UI
    BUILD_SPARK_DPP     -- $BUILD_SPARK_DPP
    CLEAN               -- $CLEAN
    RUN_UT              -- $RUN_UT
    WITH_MYSQL          -- $WITH_MYSQL
    WITH_LZO            -- $WITH_LZO
"

# Clean and build generated code
echo "Build generated code"
cd ${DORIS_HOME}/gensrc
if [ ${CLEAN} -eq 1 ]; then
   make clean
   rm -rf ${DORIS_HOME}/fe/fe-core/target
fi
# DO NOT using parallel make(-j) for gensrc
make
cd ${DORIS_HOME}

# Clean and build Backend
if [ ${BUILD_BE} -eq 1 ] ; then
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}
    if [ ${CLEAN} -eq 1 ]; then
        rm -rf $CMAKE_BUILD_DIR
        rm -rf ${DORIS_HOME}/be/output/
    fi
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    ${CMAKE_CMD} -G "${GENERATOR}" -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DMAKE_TEST=OFF -DWITH_MYSQL=${WITH_MYSQL} -DWITH_LZO=${WITH_LZO} ../
    ${BUILD_SYSTEM} -j${PARALLEL}
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
    # check NPM env here, not in env.sh.
    # Because UI should be considered a non-essential component at runtime.
    # Only when the compilation is required, check the relevant compilation environment.
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
        ${MVN_CMD} clean
    fi
    ${MVN_CMD} package -pl ${FE_MODULES} -DskipTests
    cd ${DORIS_HOME}
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/output/
mkdir -p ${DORIS_OUTPUT}

# Copy Frontend and Backend
if [ ${BUILD_FE} -eq 1 -o ${BUILD_SPARK_DPP} -eq 1 ]; then
    if [ ${BUILD_FE} -eq 1 ]; then
        install -d ${DORIS_OUTPUT}/fe/bin ${DORIS_OUTPUT}/fe/conf \
                   ${DORIS_OUTPUT}/fe/webroot/ ${DORIS_OUTPUT}/fe/lib/ \
                   ${DORIS_OUTPUT}/fe/spark-dpp/

        cp -r -p ${DORIS_HOME}/bin/*_fe.sh ${DORIS_OUTPUT}/fe/bin/
        cp -r -p ${DORIS_HOME}/conf/fe.conf ${DORIS_OUTPUT}/fe/conf/
        rm -rf ${DORIS_OUTPUT}/fe/lib/*
        cp -r -p ${DORIS_HOME}/fe/fe-core/target/lib/* ${DORIS_OUTPUT}/fe/lib/
        cp -r -p ${DORIS_HOME}/fe/fe-core/target/palo-fe.jar ${DORIS_OUTPUT}/fe/lib/
        cp -r -p ${DORIS_HOME}/docs/build/help-resource.zip ${DORIS_OUTPUT}/fe/lib/
        cp -r -p ${DORIS_HOME}/webroot/static ${DORIS_OUTPUT}/fe/webroot/
        cp -r -p ${DORIS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${DORIS_OUTPUT}/fe/spark-dpp/

        cp -r -p ${DORIS_THIRDPARTY}/installed/webroot/* ${DORIS_OUTPUT}/fe/webroot/static/

    elif [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        install -d ${DORIS_OUTPUT}/fe/spark-dpp/
        rm -rf ${DORIS_OUTPUT}/fe/spark-dpp/*
        cp -r -p ${DORIS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${DORIS_OUTPUT}/fe/spark-dpp/
    fi

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
    cp -r -p ${DORIS_HOME}/be/output/udf/*.a ${DORIS_OUTPUT}/udf/lib/
    cp -r -p ${DORIS_HOME}/be/output/udf/include/* ${DORIS_OUTPUT}/udf/include/
    cp -r -p ${DORIS_HOME}/webroot/be/* ${DORIS_OUTPUT}/be/www/

    cp -r -p ${DORIS_THIRDPARTY}/installed/webroot/* ${DORIS_OUTPUT}/be/www/

fi

echo "***************************************"
echo "Successfully build Doris"
echo "***************************************"

if [[ ! -z ${DORIS_POST_BUILD_HOOK} ]]; then
    eval ${DORIS_POST_BUILD_HOOK}
fi

exit 0
