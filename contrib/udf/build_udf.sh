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
#    sh build-udf.sh             build udf without clean.
#    sh build-udf.sh --clean     clean previous output and build.
#
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export UDF_HOME=${ROOT}
export DORIS_HOME=$(cd ../..; printf %s "$PWD")
echo ${DORIS_HOME}

. ${DORIS_HOME}/env.sh

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --clean            clean and build target

  Eg.
    $0                                 build UDF without clean
    $0 --clean                         clean and build UDF
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'clean' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_UDF=1
CLEAN=0
HELP=0
if [ $# == 1 ] ; then
    # default
    CLEAN=0
else
    CLEAN=0
    while true; do
        case "$1" in
            --clean) CLEAN=1 ; shift ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            --) shift ;  break ;;
            *) echo "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

echo "Get params:
    CLEAN       -- $CLEAN
"

cd ${UDF_HOME}
# Clean and build UDF
if [ ${BUILD_UDF} -eq 1 ] ; then
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}
    echo "Build UDF: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${UDF_HOME}/build_${CMAKE_BUILD_TYPE}
    if [ ${CLEAN} -eq 1 ]; then
        rm -rf $CMAKE_BUILD_DIR
        rm -rf ${UDF_HOME}/output/
    fi
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    ${CMAKE_CMD} -G "${GENERATOR}" -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ../
    ${BUILD_SYSTEM} -j${PARALLEL} 
    ${BUILD_SYSTEM} install
    cd ${UDF_HOME}
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/output/
mkdir -p ${DORIS_OUTPUT}

#Copy UDF
if [ ${BUILD_UDF} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/contrib/udf/lib
    for dir in $(ls ${CMAKE_BUILD_DIR}/src)
    do
      mkdir -p ${DORIS_OUTPUT}/contrib/udf/lib/$dir
      cp -r -p ${CMAKE_BUILD_DIR}/src/$dir/*.so ${DORIS_OUTPUT}/contrib/udf/lib/$dir/
    done
fi

echo "***************************************"
echo "Successfully build Doris UDF"
echo "***************************************"

if [[ ! -z ${DORIS_POST_BUILD_HOOK} ]]; then
    eval ${DORIS_POST_BUILD_HOOK}
fi

exit 0
