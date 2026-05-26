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
     --fast-compile       use fast incremental compile (tools/fast-compile-fe.sh) instead of full mvn build

  Eg.
    $0                                                                      build and run ut
    $0 --coverage                                                           build and run coverage statistic
    $0 --run org.apache.doris.utframe.Demo                                  build and run the test named Demo
    $0 --run org.apache.doris.utframe.Demo#testCreateDbAndTable+test2       build and run testCreateDbAndTable in Demo test
    $0 --run org.apache.doris.Demo,org.apache.doris.Demo2                   build and run Demo and Demo2 test
    $0 --fast-compile                                                       fast incremental compile, then run ut
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'coverage' \
    -l 'run' \
    -l 'fast-compile' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

RUN=0
COVERAGE=0
FAST_COMPILE=0
if [[ "$#" == 1 ]]; then
    #default
    RUN=0
    COVERAGE=0
    FAST_COMPILE=0
else
    RUN=0
    COVERAGE=0
    FAST_COMPILE=0
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
        --fast-compile)
            FAST_COMPILE=1
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

# Try fast incremental compilation using tools/fast-compile-fe.sh.
# Returns 0 on success, 1 if fallback to full build is needed.
try_fast_compile() {
    local fe_core="${DORIS_HOME}/fe/fe-core"
    local target_classes="$fe_core/target/classes"
    local target_test_classes="$fe_core/target/test-classes"
    local output_jar="${DORIS_HOME}/output/fe/lib/doris-fe.jar"
    local target_jar="$fe_core/target/doris-fe.jar"
    local main_src="$fe_core/src/main/java"
    local test_src="$fe_core/src/test/java"

    # Basic sanity: target/classes must exist and be complete (ref: fast-compile-fe.sh check_env)
    if [[ ! -d "$target_classes" ]]; then
        echo "target/classes does not exist, need full build"
        return 1
    fi
    local class_count
    class_count=$(find "$target_classes" -name "*.class" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$class_count" -lt 1000 ]]; then
        echo "target/classes incomplete (${class_count} classes), need full build"
        return 1
    fi
    if [[ ! -f "$output_jar" && ! -f "$target_jar" ]]; then
        echo "doris-fe.jar not found, need full build"
        return 1
    fi

    # Count changed files. Use -newer for speed; new files without a matching
    # .class are rare in incremental workflows and fast-compile-fe.sh handles them.
    local changed=0
    local limit=1001

    if [[ -d "$main_src" ]]; then
        local main_changed
        main_changed=$(find "$main_src" -name "*.java" -newer "$target_classes" 2>/dev/null \
            | head -n "$limit" | wc -l | tr -d ' ')
        changed=$((changed + main_changed))
    fi

    if [[ $changed -lt $limit && -d "$test_src" && -d "$target_test_classes" ]]; then
        local test_changed
        test_changed=$(find "$test_src" -name "*.java" -newer "$target_test_classes" 2>/dev/null \
            | head -n "$limit" | wc -l | tr -d ' ')
        changed=$((changed + test_changed))
    fi

    if [[ $changed -ge $limit ]]; then
        echo "Too many changed files (>= ${limit}), falling back to full build"
        return 1
    fi

    echo "Fast-compiling changed files..."

    # Compile main sources
    if ! "${DORIS_HOME}"/tools/fast-compile-fe.sh; then
        echo "Fast compile (main) failed, falling back to full build"
        return 1
    fi

    # Compile test sources
    if ! "${DORIS_HOME}"/tools/fast-compile-fe.sh --test; then
        echo "Fast compile (test) failed, falling back to full build"
        return 1
    fi

    return 0
}

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

# Decide whether to use fast compile or full build.
# Coverage mode requires lifecycle phases for jacoco agent setup, so always use full build.
if [[ "${FAST_COMPILE}" -eq 1 ]]; then
    if [[ "${COVERAGE}" -eq 1 ]]; then
        echo "--fast-compile is incompatible with --coverage, will use full build"
        MVN_GOAL="test"
        MVN_OPTS=""
    elif try_fast_compile; then
        MVN_GOAL="surefire:test"
        # Only test fe-core (the module fast-compile-fe.sh just compiled).
        # Running from fe/ would trigger the full reactor including modules
        # like hadoop-hudi-scanner, causing unnecessary downloads and builds.
        MVN_OPTS="-pl fe-core"
    else
        MVN_GOAL="test"
        MVN_OPTS=""
    fi
else
    MVN_GOAL="test"
    MVN_OPTS=""
fi

if [[ "${RUN}" -eq 1 ]]; then
    echo "Run the specified class: $1"
    # eg:
    # sh run-fe-ut.sh --run org.apache.doris.utframe.DemoTest
    # sh run-fe-ut.sh --run org.apache.doris.utframe.DemoTest#testCreateDbAndTable+test2

    if [[ "${COVERAGE}" -eq 1 ]]; then
        "${MVN_CMD}" ${MVN_OPTS} -Pcoverage ${MVN_GOAL} jacoco:report -DfailIfNoTests=false -Dtest="$1"
    else
        "${MVN_CMD}" ${MVN_OPTS} ${MVN_GOAL} -Dcheckstyle.skip=true -DfailIfNoTests=false -Dmaven.build.cache.enabled=false -Dtest="$1"
    fi
else
    echo "Run Frontend UT"
    if [[ "${COVERAGE}" -eq 1 ]]; then
        "${MVN_CMD}" ${MVN_OPTS} -Pcoverage ${MVN_GOAL} jacoco:report -DfailIfNoTests=false -Dmaven.test.failure.ignore=true
    else
        "${MVN_CMD}" ${MVN_OPTS} ${MVN_GOAL} -Dcheckstyle.skip=true -DfailIfNoTests=false -Dmaven.build.cache.enabled=false
    fi
fi
