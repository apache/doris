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
# This script is used to compile UDF 
# Usage:
#    sh build.sh        build udf without clean.
#    sh build.sh -clean --udf clean previous output and build.
#
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=$(dirname "$PWD")
echo ${DORIS_HOME}
export CUSTOM_UDF_HOME=${ROOT}

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
     --udf              build custom UDF
     --clean            clean and build target

  Eg.
    $0                                       build UDF without clean
    $0 --udf                                 build UDF without clean
    $0 --udf --clean                         clean and build UDF
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'udf' \
  -l 'clean' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_UDF=
CLEAN=
HELP=0
if [ $# == 1 ] ; then
    # defuat
    BUILD_UDF=1
    CLEAN=0
else
    BUILD_UDF=0
    CLEAN=0
    while true; do
        case "$1" in
            --udf) BUILD_UDF=1 ; shift ;;
            --clean) CLEAN=1 ; shift ;;
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

if [ ${CLEAN} -eq 1 -a ${BUILD_UDF} -eq 0 ]; then
    echo "--clean can not be specified without --udf"
    exit 1
fi

echo "Get params:
    BUILD_UDF    -- $BUILD_UDF
    CLEAN       -- $CLEAN
"

cd ${CUSTOM_UDF_HOME}
# Clean and build UDF
if [ ${BUILD_UDF} -eq 1 ] ; then
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}
    echo "Build UDF: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${CUSTOM_UDF_HOME}/build_${CMAKE_BUILD_TYPE}
    if [ ${CLEAN} -eq 1 ]; then
        rm -rf $CMAKE_BUILD_DIR
        rm -rf ${CUSTOM_UDF_HOME}/output/
    fi
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    ${CMAKE_CMD} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ../
    make -j${PARALLEL} VERBOSE=1
    make install
    cd ${CUSTOM_UDF_HOME}
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/output/
mkdir -p ${DORIS_OUTPUT}

#Copy UDF
if [ ${BUILD_UDF} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/custom_udf/lib
    for dir in $(ls ${CMAKE_BUILD_DIR}/src)
    do
      mkdir -p ${DORIS_OUTPUT}/custom_udf/lib/$dir
      cp -r -p ${CMAKE_BUILD_DIR}/src/$dir/*.so ${DORIS_OUTPUT}/custom_udf/lib/$dir/
    done
fi

echo "***************************************"
echo "Successfully build Doris UDF"
echo "***************************************"

if [[ ! -z ${DORIS_POST_BUILD_HOOK} ]]; then
    eval ${DORIS_POST_BUILD_HOOK}
fi

exit 0
