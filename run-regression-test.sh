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
#set -x

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DORIS_HOME="${ROOT}"

# Check args
usage() {
    echo "
Usage: $0 <shell_options> <framework_options>
  Optional shell_options:
     --clean    clean output of regression test framework
     --teamcity print teamcity service messages
     --run      run regression test. build framework if necessary

  Optional framework_options:
     -s                                run a specified suite
     -g                                run a specified group
     -d                                run a specified directory
     -h                                **print all framework options usage**
     -xs                               exclude the specified suite
     -xg                               exclude the specified group
     -xd                               exclude the specified directory
     -genOut                           generate .out file if not exist
     -forceGenOut                      delete and generate .out file if not exist
     -parallel                         run tests using specified threads
     -randomOrder                      run tests in a random order
     -times                            rum tests {times} times

  Eg.
    $0                                 build regression test framework and run all suite which in default group
    $0 --run test_select               run a suite which named as test_select
    $0 --run 'test*'                   run all suite which named start with 'test', note that you must quota with ''
    $0 --run -s test_select            run a suite which named as test_select
    $0 --run test_select -genOut       generate output file for test_select if not exist
    $0 --run -g default                run all suite in the group which named as default
    $0 --run -d demo,correctness/tmp   run all suite in the directories which named as demo and correctness/tmp
    $0 --clean                         clean output of regression test framework
    $0 --clean --run test_select       clean output and build regression test framework and run a suite which named as test_select
    $0 --run -h                        print framework options
    $0 --teamcity --run test_select    print teamcity service messages and build regression test framework and run test_select

Log path: \${DORIS_HOME}/output/regression-test/log
Default config file: \${DORIS_HOME}/regression-test/conf/regression-conf.groovy
  "
    exit 1
}

CLEAN=0
WRONG_CMD=0
TEAMCITY=0
RUN=0
if [[ "$#" == 0 ]]; then
    #default
    CLEAN=0
    WRONG_CMD=0
    TEAMCITY=0
    RUN=1
else
    CLEAN=0
    RUN=0
    TEAMCITY=0
    WRONG_CMD=0
    while true; do
        case "$1" in
        --clean)
            CLEAN=1
            shift
            ;;
        --teamcity)
            TEAMCITY=1
            shift
            ;;
        --run)
            RUN=1
            shift
            ;;
        *)
            if [[ "${RUN}" -eq 0 ]] && [[ "${CLEAN}" -eq 0 ]]; then
                WRONG_CMD=1
            fi
            break
            ;;
        esac
    done
fi

if [[ "${WRONG_CMD}" -eq 1 ]]; then
    usage
    exit 1
fi

# set maven
MVN_CMD='mvn'
if [[ -n "${CUSTOM_MVN}" ]]; then
    MVN_CMD="${CUSTOM_MVN}"
fi
if ! "${MVN_CMD}" --version; then
    echo "Error: mvn is not found"
    exit 1
fi
export MVN_CMD

CONF_DIR="${DORIS_HOME}/regression-test/conf"
CONFIG_FILE="${CONF_DIR}/regression-conf.groovy"
LOG_CONFIG_FILE="${CONF_DIR}/logback.xml"

FRAMEWORK_SOURCE_DIR="${DORIS_HOME}/regression-test/framework"
REGRESSION_TEST_BUILD_DIR="${FRAMEWORK_SOURCE_DIR}/target"

OUTPUT_DIR="${DORIS_HOME}/output/regression-test"
LOG_OUTPUT_FILE="${OUTPUT_DIR}/log"
RUN_JAR="${OUTPUT_DIR}/lib/regression-test-*.jar"

if [[ "${CLEAN}" -eq 1 ]]; then
    rm -rf "${REGRESSION_TEST_BUILD_DIR}"
    rm -rf "${OUTPUT_DIR}"
fi

if [[ "${RUN}" -ne 1 ]]; then
    echo "Finished"
    exit 0
fi

if ! test -f ${RUN_JAR:+${RUN_JAR}}; then
    echo "===== Build Regression Test Framework ====="
    cd "${DORIS_HOME}/regression-test/framework"
    "${MVN_CMD}" package
    cd "${DORIS_HOME}"

    mkdir -p "${OUTPUT_DIR}"/{lib,log}
    cp -r "${REGRESSION_TEST_BUILD_DIR}"/regression-test-*.jar "${OUTPUT_DIR}/lib"
fi

# build jar needed by java-udf case
JAVAUDF_JAR="${DORIS_HOME}/samples/doris-demo/java-udf-demo/target/java-udf-demo-jar-with-dependencies.jar"
if ! test -f ${JAVAUDF_JAR:+${JAVAUDF_JAR}}; then
    mkdir -p "${DORIS_HOME}"/regression-test/suites/javaudf_p0/jars
    cd "${DORIS_HOME}"/samples/doris-demo/java-udf-demo
    "${MVN_CMD}" package
    cp target/java-udf-demo-jar-with-dependencies.jar "${DORIS_HOME}"/regression-test/suites/javaudf_p0/jars/
    cd "${DORIS_HOME}"
fi

# check java home
if [[ -z "${JAVA_HOME}" ]]; then
    echo "Error: JAVA_HOME is not set"
    exit 1
fi

# check java version
export JAVA="${JAVA_HOME}/bin/java"

REGRESSION_OPTIONS_PREFIX=''

# contains framework options and not start with -
# it should be suite name
if [[ "$#" -ne 0 ]] && [[ "$1" =~ ^[^-].* ]]; then
    # specify suiteName
    REGRESSION_OPTIONS_PREFIX='-s'
fi

echo "===== Run Regression Test ====="

if [[ "${TEAMCITY}" -eq 1 ]]; then
    JAVA_OPTS="${JAVA_OPTS} -DstdoutAppenderType=teamcity -Xmx2048m"
fi

"${JAVA}" -DDORIS_HOME="${DORIS_HOME}" \
    -DLOG_PATH="${LOG_OUTPUT_FILE}" \
    -Dlogback.configurationFile="${LOG_CONFIG_FILE}" \
    ${JAVA_OPTS:+${JAVA_OPTS}} \
    -jar ${RUN_JAR:+${RUN_JAR}} \
    -cf "${CONFIG_FILE}" \
    ${REGRESSION_OPTIONS_PREFIX:+${REGRESSION_OPTIONS_PREFIX}} "$@"
