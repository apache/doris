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

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --clean    clean and build ut
     --run    build and run ut

  Eg.
    $0                      build ut
    $0 --run                build and run ut
    $0 --clean              clean and build ut
    $0 --clean --run        clean, build and run ut
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'run' \
  -l 'clean' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

CLEAN=
RUN=
if [ $# == 1 ] ; then
    #default
    CLEAN=0
    RUN=0
else
    CLEAN=0
    RUN=0
    while true; do 
        case "$1" in
            --clean) CLEAN=1 ; shift ;;
            --run) RUN=1 ; shift ;;
            --) shift ;  break ;;
            *) ehco "Internal error" ; exit 1 ;;
        esac
    done
fi

echo "Build Backend UT"

if [ ${CLEAN} -eq 1 ]; then
    rm ${DORIS_HOME}/be/build/ -rf
    rm ${DORIS_HOME}/be/output/ -rf
fi

if [ ! -d ${DORIS_HOME}/be/build ]; then
    mkdir -p ${DORIS_HOME}/be/build/
fi

cd ${DORIS_HOME}/be/build/

cmake ../ -DWITH_MYSQL=OFF -DMAKE_TEST=ON
make -j${PARALLEL}

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "    Running PaloBe Unittest    "
echo "******************************"

cd ${DORIS_HOME}
export DORIS_TEST_BINARY_DIR=${DORIS_HOME}/be/build
export TERM=xterm
export UDF_RUNTIME_DIR=${DORIS_HOME}/lib/udf-runtime
export LOG_DIR=${DORIS_HOME}/log
for i in `sed 's/ //g' $DORIS_HOME/conf/be.conf | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`; do
    eval "export $i";
done

mkdir -p $LOG_DIR
mkdir -p ${UDF_RUNTIME_DIR}
rm -f ${UDF_RUNTIME_DIR}/*

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "    Running PaloBe Unittest    "
echo "******************************"

export DORIS_TEST_BINARY_DIR=${DORIS_TEST_BINARY_DIR}/test/

# prepare util test_data
if [ -d ${DORIS_TEST_BINARY_DIR}/util/test_data ]; then
    rm -rf ${DORIS_TEST_BINARY_DIR}/util/test_data
fi
cp -r ${DORIS_HOME}/be/test/util/test_data ${DORIS_TEST_BINARY_DIR}/util/

# Running Util Unittest

${DORIS_TEST_BINARY_DIR}/udf/record_store_test

# Running agent unittest
# Prepare agent testdata
if [ -d ${DORIS_TEST_BINARY_DIR}/agent/test_data ]; then
    rm -rf ${DORIS_TEST_BINARY_DIR}/agent/test_data
fi
cp -r ${DORIS_HOME}/be/test/agent/test_data ${DORIS_TEST_BINARY_DIR}/agent/
cd ${DORIS_TEST_BINARY_DIR}/agent
# ./agent_server_test
#./heartbeat_server_test
./utils_test
