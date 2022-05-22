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
# This script is used to run unit test of Doris Backend
# Usage: $0 <options>
#  Optional options:
#     --clean      clean and build ut
#     --run        build and run all ut
#     --run xx     build and run specified ut
#
# All BE tests must use "_test" as the file suffix, and use
# ADD_BE_TEST() to declared in the corresponding CMakeLists.txt file.
#
# GTest result xml files will be in "be/ut_build_ASAN/gtest_output/"
#####################################################################

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
     --clean    clean and build ut
     --run      build and run all ut
     --run xx   build and run specified ut
     -v         build and run all vectorized ut
     -j         build parallel

  Eg.
    $0                          build ut
    $0 --run                    build and run all ut
    $0 --run test               build and run "test" ut
    $0 --clean                  clean and build ut
    $0 --clean --run            clean, build and run all ut
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'run' \
  -l 'clean' \
  -o 'vj:' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

PARALLEL=$[$(nproc)/5+1]

if [[ -z ${USE_LLD} ]]; then
    USE_LLD=OFF
fi

CLEAN=0
RUN=0
VECTORIZED_ONLY=0
if [ $# != 1 ] ; then
    while true; do 
        case "$1" in
            --clean) CLEAN=1 ; shift ;;
            --run) RUN=1 ; shift ;;
            -v) VECTORIZED_ONLY=1 ; shift ;;
            -j) PARALLEL=$2; shift 2 ;;
            --) shift ;  break ;;
            *) echo "Internal error" ; exit 1 ;;
        esac
    done
fi

CMAKE_BUILD_TYPE=${BUILD_TYPE:-ASAN}
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE^^}"

echo "Get params:
    PARALLEL            -- $PARALLEL
    CLEAN               -- $CLEAN
"
echo "Build Backend UT"

CMAKE_BUILD_DIR=${DORIS_HOME}/be/ut_build_${CMAKE_BUILD_TYPE}
if [ ${CLEAN} -eq 1 ]; then
    rm ${CMAKE_BUILD_DIR} -rf
    rm ${DORIS_HOME}/be/output/ -rf
fi

if [ ! -d ${CMAKE_BUILD_DIR} ]; then
    mkdir -p ${CMAKE_BUILD_DIR}
fi

if [[ -z ${GLIBC_COMPATIBILITY} ]]; then
    GLIBC_COMPATIBILITY=ON
fi

# get specified ut file if set
RUN_FILE=
if [ $# == 1 ]; then
    RUN_FILE=$1
    echo "=== Run test: $RUN_FILE ==="
else
    # run all ut
    echo "=== Running All tests ==="
fi

MAKE_PROGRAM="$(which "${BUILD_SYSTEM}")"
echo "-- Make program: ${MAKE_PROGRAM}"

cd ${CMAKE_BUILD_DIR}
${CMAKE_CMD} -G "${GENERATOR}" \
    -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
    -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
    -DMAKE_TEST=ON \
    -DUSE_LLD=${USE_LLD} \
    -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
    -DBUILD_META_TOOL=OFF \
    -DWITH_MYSQL=OFF \
    -DWITH_KERBEROS=OFF \
    ${CMAKE_USE_CCACHE} ../
${BUILD_SYSTEM} -j ${PARALLEL} $RUN_FILE

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "   Running Backend Unit Test  "
echo "******************************"

cd ${DORIS_HOME}
export DORIS_TEST_BINARY_DIR=${CMAKE_BUILD_DIR}
export TERM=xterm
export UDF_RUNTIME_DIR=${DORIS_HOME}/lib/udf-runtime
export LOG_DIR=${DORIS_HOME}/log
for i in `sed 's/ //g' $DORIS_HOME/conf/be.conf | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`; do
    eval "export $i";
done

mkdir -p $LOG_DIR
mkdir -p ${UDF_RUNTIME_DIR}
rm -f ${UDF_RUNTIME_DIR}/*

# clean all gcda file

gcda_files=`find ${DORIS_TEST_BINARY_DIR} -name "*gcda"`
for gcda_file in ${gcda_files[@]}
do
    rm $gcda_file
done

export DORIS_TEST_BINARY_DIR=${DORIS_TEST_BINARY_DIR}/test/

# prepare gtest output dir
GTEST_OUTPUT_DIR=${CMAKE_BUILD_DIR}/gtest_output
rm -rf ${GTEST_OUTPUT_DIR} && mkdir ${GTEST_OUTPUT_DIR}

# prepare util test_data
if [ -d ${DORIS_TEST_BINARY_DIR}/util/test_data ]; then
    rm -rf ${DORIS_TEST_BINARY_DIR}/util/test_data
fi
cp -r ${DORIS_HOME}/be/test/util/test_data ${DORIS_TEST_BINARY_DIR}/util/
cp -r ${DORIS_HOME}/be/test/plugin/plugin_test ${DORIS_TEST_BINARY_DIR}/plugin/

# find all executable test files

if [ ${VECTORIZED_ONLY} -eq 1 ]; then
    echo "Run Vectorized ut only"
    export DORIS_TEST_BINARY_DIR=${DORIS_TEST_BINARY_DIR}/vec
fi

test_files=`find ${DORIS_TEST_BINARY_DIR} -type f -perm -111 -name "*test"`

for test in ${test_files[@]}
do
    file_name=${test##*/}
    if [ -z $RUN_FILE ] || [ $file_name == $RUN_FILE ]; then
        echo "=== Run $file_name ==="
        $test --gtest_output=xml:${GTEST_OUTPUT_DIR}/${file_name}.xml
    fi
done
echo "=== Finished. Gtest output: ${GTEST_OUTPUT_DIR}"
