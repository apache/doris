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
#     --clean            clean and build ut
#     --run              build and run all ut
#     --run --filter=xx  build and run specified ut
#     -j                 build parallel
#     -h                 print this help message
#
# All BE tests must use "_test" as the file suffix, and add the file
# to be/test/CMakeLists.txt.
#
# GTest result xml files will be in "be/ut_build_ASAN/gtest_output/"
#####################################################################

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --clean            clean and build ut
     --run              build and run all ut
     --run --filter=xx  build and run specified ut
     -j                 build parallel
     -h                 print this help message

  Eg.
    $0                                                              build tests
    $0 --run                                                        build and run all tests
    $0 --run --filter=*                                             also runs everything
    $0 --run --filter=FooTest.*                                     runs everything in test suite FooTest
    $0 --run --filter=*Null*:*Constructor*                          runs any test whose full name contains either 'Null' or 'Constructor'
    $0 --run --filter=-*DeathTest.*                                 runs all non-death tests
    $0 --run --filter=FooTest.*-FooTest.Bar                         runs everything in test suite FooTest except FooTest.Bar
    $0 --run --filter=FooTest.*:BarTest.*-FooTest.Bar:BarTest.Foo   runs everything in test suite FooTest except FooTest.Bar and everything in test suite BarTest except BarTest.Foo
    $0 --clean                                                      clean and build tests
    $0 --clean --run                                                clean, build and run all tests
  "
  exit 1
}

OPTS=$(getopt  -n $0 -o vhj:f: -l run,clean,filter: -- "$@")
if [ "$?" != "0" ]; then
  usage
fi

set -eo pipefail

eval set -- "$OPTS"

PARALLEL=$[$(nproc)/5+1]

if [[ -z ${USE_LLD} ]]; then
    USE_LLD=OFF
fi

CLEAN=0
RUN=0
FILTER=""
if [ $# != 1 ] ; then
    while true; do 
        case "$1" in
            --clean) CLEAN=1 ; shift ;;
            --run) RUN=1 ; shift ;;
            -f | --filter) FILTER="--gtest_filter=$2"; shift 2;;
            -j) PARALLEL=$2; shift 2 ;;
            --) shift ;  break ;;
            *) usage ; exit 0 ;;
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

. ${DORIS_HOME}/env.sh

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
    ${CMAKE_USE_CCACHE} ../
${BUILD_SYSTEM} -j ${PARALLEL}

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

# prepare jvm if needed
jdk_version() {
    local result
    local java_cmd=$JAVA_HOME/bin/java
    local IFS=$'\n'
    # remove \r for Cygwin
    local lines=$("$java_cmd" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
    if [[ -z $java_cmd ]]
    then
        result=no_java
    else
        for line in $lines; do
            if [[ (-z $result) && ($line = *"version \""*) ]]
            then
                local ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
                # on macOS, sed doesn't support '?'
                if [[ $ver = "1."* ]]
                then
                    result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
                else
                    result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
                fi
            fi
        done
    fi
    echo "$result"
}

jvm_arch="amd64"
MACHINE_TYPE=$(uname -m)
if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    jvm_arch="aarch64"
fi
java_version=$(jdk_version)
if [[ $java_version -gt 8 ]]; then
    export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$JAVA_HOME/lib:$LD_LIBRARY_PATH
# JAVA_HOME is jdk
elif [[ -d "$JAVA_HOME/jre"  ]]; then
    export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$jvm_arch/server:$JAVA_HOME/jre/lib/$jvm_arch:$LD_LIBRARY_PATH
# JAVA_HOME is jre
else
    export LD_LIBRARY_PATH=$JAVA_HOME/lib/$jvm_arch/server:$JAVA_HOME/lib/$jvm_arch:$LD_LIBRARY_PATH
fi

# prepare gtest output dir
GTEST_OUTPUT_DIR=${CMAKE_BUILD_DIR}/gtest_output
rm -rf ${GTEST_OUTPUT_DIR} && mkdir ${GTEST_OUTPUT_DIR}

# prepare util test_data
mkdir -p ${DORIS_TEST_BINARY_DIR}/util
if [ -d ${DORIS_TEST_BINARY_DIR}/util/test_data ]; then
    rm -rf ${DORIS_TEST_BINARY_DIR}/util/test_data
fi
cp -r ${DORIS_HOME}/be/test/util/test_data ${DORIS_TEST_BINARY_DIR}/util/
cp -r ${DORIS_HOME}/be/test/plugin/plugin_test ${DORIS_TEST_BINARY_DIR}/plugin/

# prepare ut temp dir
UT_TMP_DIR=${DORIS_HOME}/ut_dir
rm -rf ${UT_TMP_DIR} && mkdir ${UT_TMP_DIR}
touch ${UT_TMP_DIR}/tmp_file

# find all executable test files

test=${DORIS_TEST_BINARY_DIR}doris_be_test
file_name=${test##*/}
if [ -f "$test" ]; then
    $test --gtest_output=xml:${GTEST_OUTPUT_DIR}/${file_name}.xml  --gtest_print_time=true "${FILTER}"
    echo "=== Finished. Gtest output: ${GTEST_OUTPUT_DIR}"
else 
    echo "unit test file: $test does not exist."
fi
