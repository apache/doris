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
     --fe               build Frontend
     --p                build special plugin
     --clean            clean and build Frontend
  Eg.
    $0 --fe             build Frontend and all plugin
    $0 --p xxx          build xxx plugin
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'fe' \
  -l 'p' \
  -l 'clean' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_CLEAN=
BUILD_FE=
ALL_PLUGIN=
if [ $# == 1 ] ; then
    # defuat
    BUILD_CLEAN=0
    BUILD_FE=0
    ALL_PLUGIN=1
else
    BUILD_FE=0
    ALL_PLUGIN=1
    BUILD_CLEAN=0
    while true; do
        case "$1" in
            --fe) BUILD_FE=1 ; shift ;;
            --p)  ALL_PLUGIN=0 ; shift ;;
            --clean)  BUILD_CLEAN=1 ; shift ;;
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

if [ ${CLEAN} -eq 1 -a ${BUILD_FE} -eq 0 ]; then
    echo "--clean can not be specified without --fe"
    exit 1
fi

echo "Get params:
    BUILD_FE               -- $BUILD_FE
    BUILD_CLEAN            -- $BUILD_CLEAN
    BUILD_ALL_PLUGIN       -- $ALL_PLUGIN
"

cd ${DORIS_HOME}

# Clean and build Frontend
if [ ${BUILD_FE} -eq 1 ] ; then
    if [ ${CLEAN} -eq 1 ] ; then
        sh build.sh --fe --clean
    else
        sh build.sh --fe
    fi
fi

if [ ${ALL_PLUGIN} -eq 1 ] ; then
    cd ${DORIS_HOME}/fe_plugins
    echo "build all plugins"
    ${MVN_CMD} package -DskipTests 
else
    cd ${DORIS_HOME}/fe_plugins/$1
    echo "build plugin $1"
    ${MVN_CMD} package -DskipTests
fi

cd ${DORIS_HOME}
# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/fe_plugins/output/
mkdir -p ${DORIS_OUTPUT}

cp -p ${DORIS_HOME}/fe_plugins/*/target/*.zip ${DORIS_OUTPUT}/

echo "***************************************"
echo "Successfully build Doris Plugin"
echo "***************************************"

exit 0
