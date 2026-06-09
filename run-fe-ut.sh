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

trim_whitespace() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "${value}"
}

is_valid_extra_module_feature() {
    local feature="$1"
    [[ "${feature}" =~ ^[A-Za-z][A-Za-z0-9_-]*$ ]]
}

parse_extra_fe_modules() {
    local spec_value="$1"
    local entry feature module_path existing
    local -a feature_keys=()

    FE_EXTRA_MODULE_PATHS=()
    if [[ -z "${spec_value}" ]]; then
        return
    fi

    IFS=',' read -r -a entries <<<"${spec_value}"
    for entry in "${entries[@]}"; do
        entry="$(trim_whitespace "${entry}")"
        if [[ -z "${entry}" || "${entry}" != *=* ]]; then
            echo "Invalid EXTRA_FE_MODULES entry '${entry}': expected feature=module_path"
            exit 1
        fi

        feature="$(trim_whitespace "${entry%%=*}")"
        module_path="$(trim_whitespace "${entry#*=}")"
        if [[ -z "${feature}" || -z "${module_path}" ]]; then
            echo "Invalid EXTRA_FE_MODULES entry '${entry}': feature and module_path must be non-empty"
            exit 1
        fi
        if ! is_valid_extra_module_feature "${feature}"; then
            echo "Invalid EXTRA_FE_MODULES feature '${feature}'"
            exit 1
        fi
        for existing in "${feature_keys[@]}"; do
            if [[ "${existing}" == "${feature}" ]]; then
                echo "Duplicate EXTRA_FE_MODULES feature '${feature}'"
                exit 1
            fi
        done
        if [[ ! -f "${DORIS_HOME}/fe/${module_path}/pom.xml" ]]; then
            echo "Missing EXTRA_FE_MODULES module: ${DORIS_HOME}/fe/${module_path}/pom.xml"
            exit 1
        fi
        feature_keys+=("${feature}")
        FE_EXTRA_MODULE_PATHS+=("${module_path}")
    done
}

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --coverage           build and run coverage statistic
     --run                build and run ut
     -j <num>             build parallel, num is the number of threads, default 1
     -p <num>             unit test parallel, num is the number of forked JVMs, default 1

  Environment variables:
     EXTRA_FE_MODULES     Optional FE feature modules in feature=module_path format, separated by commas.

  Eg.
    $0                                                                      build and run ut
    $0 -j 4                                                                 build with 4 threads and run ut
    $0 -p 3                                                                 run unit tests with 3 forked JVMs
    $0 -j 4 -p 3                                                            build with 4 threads and run unit tests with 3 forked JVMs
    $0 --coverage                                                           build and run coverage statistic
    $0 --run org.apache.doris.utframe.Demo                                  build and run the test named Demo
    $0 --run org.apache.doris.utframe.Demo#testCreateDbAndTable+test2       build and run testCreateDbAndTable in Demo test
    $0 --run org.apache.doris.Demo,org.apache.doris.Demo2                   build and run Demo and Demo2 test
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o 'j:p:' \
    -l 'coverage' \
    -l 'run' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

RUN=0
COVERAGE=0
BUILD_PARALLEL=1
UT_PARALLEL=1
if [[ "$#" == 1 ]]; then
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
        -j)
            BUILD_PARALLEL="$2"
            shift 2
            ;;
        -p)
            UT_PARALLEL="$2"
            shift 2
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

EXTRA_FE_MODULES="${EXTRA_FE_MODULES:-}"
parse_extra_fe_modules "${EXTRA_FE_MODULES}"

FE_MODULES=("fe-common" "fe-core")
for extra_module_path in "${FE_EXTRA_MODULE_PATHS[@]}"; do
    FE_MODULES+=("${extra_module_path}")
done
MVN_MODULES="$(IFS=','; echo "${FE_MODULES[*]}")"

echo "Get params:
    RUN                 -- ${RUN}
    COVERAGE            -- ${COVERAGE}
    EXTRA_FE_MODULES    -- ${EXTRA_FE_MODULES}
    MVN_MODULES         -- ${MVN_MODULES}
"

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
    export FE_UT_PARALLEL="${UT_PARALLEL}"
fi
echo "Build parallel is: ${BUILD_PARALLEL}"
echo "Unit test parallel is: ${FE_UT_PARALLEL}"

if [[ "${RUN}" -eq 1 ]]; then
    echo "Run the specified class: $1"
    # eg:
    # sh run-fe-ut.sh --run org.apache.doris.utframe.DemoTest
    # sh run-fe-ut.sh --run org.apache.doris.utframe.DemoTest#testCreateDbAndTable+test2

    if [[ "${COVERAGE}" -eq 1 ]]; then
        "${MVN_CMD}" -T "${BUILD_PARALLEL}" -Pcoverage test jacoco:report -pl "${MVN_MODULES}" -am -DfailIfNoTests=false -Dtest="$1"
    else
        "${MVN_CMD}" -T "${BUILD_PARALLEL}" test -pl "${MVN_MODULES}" -am -Dcheckstyle.skip=true -DfailIfNoTests=false \
            -Dmaven.build.cache.enabled=false -Dtest="$1"
    fi
else
    echo "Run Frontend UT"
    if [[ "${COVERAGE}" -eq 1 ]]; then
        "${MVN_CMD}" -T "${BUILD_PARALLEL}" -Pcoverage test jacoco:report -pl "${MVN_MODULES}" -am -DfailIfNoTests=false \
            -Dmaven.test.failure.ignore=true
    else
        "${MVN_CMD}" -T "${BUILD_PARALLEL}" test -pl "${MVN_MODULES}" -am -Dcheckstyle.skip=true -DfailIfNoTests=false \
            -Dmaven.build.cache.enabled=false
    fi
fi
