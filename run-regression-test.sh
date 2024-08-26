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
     -forceGenOut                      delete and generate .out file
     -parallel                         run tests using specified threads
     -randomOrder                      run tests in a random order
     -noKillDocker                     don't kill container when finish docker suites
     -times                            rum tests {times} times

  Eg.
    $0                                        build regression test framework and run all suite which in default group
    $0 --run test_select                      run a suite which named as test_select
    $0 --compile                              only compile regression framework
    $0 --run -s test_select                   run a suite which named as test_select
    $0 --run test_select -genOut              generate output file for test_select if not exist
    $0 --run -g default                       run all suite in the group which named as default
    $0 --run -d demo,correctness/tmp          run all suite in the directories which named as demo and correctness/tmp
    $0 --run -d regression-test/suites/demo   specify the suite directories path from repo root
    $0 --clean                                clean output of regression test framework
    $0 --clean --run test_select              clean output and build regression test framework and run a suite which named as test_select
    $0 --run -h                               print framework options
    $0 --teamcity --run test_select           print teamcity service messages and build regression test framework and run test_select

Log path: \${DORIS_HOME}/output/regression-test/log
Default config file: \${DORIS_HOME}/regression-test/conf/regression-conf.groovy
  "
    exit 1
}
ONLY_COMPILE=0
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
        --conf)
            CUSTOM_CONFIG_FILE="$2"
            shift
            shift
            ;;
        --compile)
            RUN=1
            ONLY_COMPILE=1
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

if [[ ${WRONG_CMD} -eq 1 ]]; then
    usage
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
if [[ -n "${CUSTOM_CONFIG_FILE}" ]] && [[ -f "${CUSTOM_CONFIG_FILE}" ]]; then
    CONFIG_FILE="${CUSTOM_CONFIG_FILE}"
    echo "Using custom config file ${CONFIG_FILE}"
else
    CONFIG_FILE="${CONF_DIR}/regression-conf.groovy"
fi
LOG_CONFIG_FILE="${CONF_DIR}/logback.xml"

FRAMEWORK_SOURCE_DIR="${DORIS_HOME}/regression-test/framework"
REGRESSION_TEST_BUILD_DIR="${FRAMEWORK_SOURCE_DIR}/target"
FRAMEWORK_APACHE_DIR="${FRAMEWORK_SOURCE_DIR}/src/main/groovy/org/apache"

OUTPUT_DIR="${DORIS_HOME}/output/regression-test"
LOG_OUTPUT_FILE="${OUTPUT_DIR}/log"
RUN_JAR="${OUTPUT_DIR}/lib/regression-test-*.jar"
if [[ -z "${DORIS_THIRDPARTY}" ]]; then
    export DORIS_THIRDPARTY="${DORIS_HOME}/thirdparty"
fi

rm -rf "${FRAMEWORK_APACHE_DIR}/doris/thrift"

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

    # echo "Build generated code"
    cd "${DORIS_HOME}/gensrc/thrift"
    make

    cp -rf "${DORIS_HOME}/gensrc/build/gen_java/org/apache/doris/thrift" "${FRAMEWORK_APACHE_DIR}/doris/"

    cd "${DORIS_HOME}/regression-test/framework"
    "${MVN_CMD}" package
    cd "${DORIS_HOME}"

    mkdir -p "${OUTPUT_DIR}"/{lib,log}
    cp -r "${REGRESSION_TEST_BUILD_DIR}"/regression-test-*.jar "${OUTPUT_DIR}/lib"

    echo "===== BUILD JAVA_UDF_SRC TO GENERATE JAR ====="
    mkdir -p "${DORIS_HOME}"/regression-test/suites/javaudf_p0/jars
    cd "${DORIS_HOME}"/regression-test/java-udf-src
    "${MVN_CMD}" package
    cp target/java-udf-case-jar-with-dependencies.jar "${DORIS_HOME}"/regression-test/suites/javaudf_p0/jars/
    # be and fe dir is compiled output
    mkdir -p "${DORIS_HOME}"/output/fe/custom_lib/
    mkdir -p "${DORIS_HOME}"/output/be/custom_lib/
    cp target/java-udf-case-jar-with-dependencies.jar "${DORIS_HOME}"/output/fe/custom_lib/
    cp target/java-udf-case-jar-with-dependencies.jar "${DORIS_HOME}"/output/be/custom_lib/
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

# if use jdk17, add java option "--add-opens=java.base/java.nio=ALL-UNNAMED"
if [[ "${TEAMCITY}" -eq 1 ]]; then
    JAVA_OPTS="${JAVA_OPTS} -DstdoutAppenderType=teamcity -Xmx2048m"
fi

if [[ "${ONLY_COMPILE}" -eq 0 ]]; then
    "${JAVA}" -DDORIS_HOME="${DORIS_HOME}" \
        -DLOG_PATH="${LOG_OUTPUT_FILE}" \
        -Dfile.encoding="UTF-8" \
        -Dlogback.configurationFile="${LOG_CONFIG_FILE}" \
        ${JAVA_OPTS:+${JAVA_OPTS}} \
        -jar ${RUN_JAR:+${RUN_JAR}} \
        -cf "${CONFIG_FILE}" \
        ${REGRESSION_OPTIONS_PREFIX:+${REGRESSION_OPTIONS_PREFIX}} "$@"
fi
