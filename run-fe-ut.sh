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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

. "${DORIS_HOME}/env.sh"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --coverage           build and run coverage statistic
     --run                build and run ut

  Eg.
    $0                                                                      build and run ut
    $0 --coverage                                                           build and run coverage statistic
    $0 --run org.apache.doris.utframe.Demo                                  build and run the test named Demo
    $0 --run org.apache.doris.utframe.Demo#testCreateDbAndTable+test2       build and run testCreateDbAndTable in Demo test
    $0 --run org.apache.doris.Demo,org.apache.doris.Demo2                   build and run Demo and Demo2 test
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'coverage' \
    -l 'run' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

RUN=0
COVERAGE=0
if [[ "$#" == 1 ]]; then
    #default
    RUN=0
    COVERAGE=0
else
    RUN=0
    COVERAGE=0
    while true; do
        case "$1" in
        --coverage)
            COVERAGE=1
            shift
            ;;
        --run)
            RUN=1
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error"
            exit 1
            ;;
        esac
    done
fi

echo "Build Frontend UT"

echo "******************************"
echo "    Runing DorisFe Unittest    "
echo "******************************"

#echo "Build docs"
#cd "${DORIS_HOME}/docs"
#./build_help_zip.sh
#cp build/help-resource.zip "${DORIS_HOME}"/fe/fe-core/src/test/resources/real-help-resource.zip
#cd "${DORIS_HOME}"

"${DORIS_HOME}"/generated-source.sh

cd "${DORIS_HOME}/fe"
mkdir -p build/compile

if [[ -z "${FE_UT_PARALLEL}" ]]; then
    # the default fe unit test parallel is 1
    export FE_UT_PARALLEL=1
fi
echo "Unit test parallel is: ${FE_UT_PARALLEL}"

if [[ "${RUN}" -eq 1 ]]; then
    echo "Run the specified class: $1"
    # eg:
    # sh run-fe-ut.sh --run org.apache.doris.utframe.DemoTest
    # sh run-fe-ut.sh --run org.apache.doris.utframe.DemoTest#testCreateDbAndTable+test2

    if [[ "${COVERAGE}" -eq 1 ]]; then
        "${MVN_CMD}" test jacoco:report -DfailIfNoTests=false -Dtest="$1"
    else
        "${MVN_CMD}" test -Dcheckstyle.skip=true -DfailIfNoTests=false -Dtest="$1"
    fi
else
    echo "Run Frontend UT"
    if [[ "${COVERAGE}" -eq 1 ]]; then
        "${MVN_CMD}" test jacoco:report -DfailIfNoTests=false
    else
        "${MVN_CMD}" test -Dcheckstyle.skip=true -DfailIfNoTests=false
    fi
fi
