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

# The CI job parses FE UT results with the report pattern fe/*/target/surefire-reports/*.xml. That
# single wildcard only reaches the modules sitting directly under fe/; every module nested one level
# deeper -- fe-filesystem/*, fe-connector/*, fe-authentication/* -- is invisible to it. Combined
# with the -Dmaven.test.failure.ignore=true that the coverage run needs in order to finish every
# module and still emit a jacoco report, a failing nested module leaves maven at exit 0, the reactor
# printing SUCCESS for it, and the job green with "failed: 0".
#
# So gate here on the reports the CI parser cannot see. The ones it can see are deliberately left to
# it: it owns those results, and its per-test mutes have to keep working.
fail_on_unparsed_test_failures() {
    local report module header failures errors
    local -a broken=()

    while IFS= read -r report; do
        # The module path relative to fe/, e.g. "fe-core" or "fe-filesystem/fe-filesystem-obs".
        # No slash in it means the module sits directly under fe/, which is exactly what the CI
        # pattern's single wildcard reaches -- leave those to the CI parser. Deliberately not
        # written as a [[ ]] glob against the pattern itself: there, * also matches /, so
        # fe/*/target/... would swallow the nested modules this function exists to catch.
        module="${report#"${DORIS_HOME}"/fe/}"
        module="${module%%/target/*}"
        [[ "${module}" != */* ]] && continue

        # The totals live on the root <testsuite> element. -m1 so that a stack trace quoted inside
        # some later <testcase> can never be mistaken for it.
        header="$(grep -m1 -o '<testsuite [^>]*>' "${report}")" || continue
        failures="$(sed -n 's/.*failures="\([0-9]*\)".*/\1/p' <<<"${header}")"
        errors="$(sed -n 's/.*errors="\([0-9]*\)".*/\1/p' <<<"${header}")"

        if [[ "${failures:-0}" -gt 0 || "${errors:-0}" -gt 0 ]]; then
            broken+=("${report#"${DORIS_HOME}/"} -- failures=${failures:-0} errors=${errors:-0}")
        fi
        # A here-string, not `done < <(find ...)`: this script is documented and invoked as
        # `sh run-fe-ut.sh`, and bash in sh/POSIX mode rejects process substitution outright --
        # a parse error, so the whole script dies before it builds anything. Piping into the
        # loop instead would put the body in a subshell and throw `broken` away.
    done <<<"$(find "${DORIS_HOME}/fe" -type f -path '*/target/surefire-reports/*.xml')"

    if [[ "${#broken[@]}" -ne 0 ]]; then
        echo ""
        echo "FE UT failed in ${#broken[@]} test class(es) whose module the CI report pattern does not reach:"
        printf '    %s\n' "${broken[@]}"
        echo ""
        return 1
    fi
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

  Environment variables:
     EXTRA_FE_MODULES     Optional FE feature modules in feature=module_path format, separated by commas.

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

EXTRA_FE_MODULES="${EXTRA_FE_MODULES:-}"
parse_extra_fe_modules "${EXTRA_FE_MODULES}"

FE_MODULES=("fe-common" "fe-core")
# The BE Java plugin modules. Nothing else runs these tests: no be-java-extensions module is
# upstream of fe-core, so -am never reaches one, build.sh builds the reactor with -DskipTests, and
# no GitHub workflow mentions the directory at all. What is in there is the evidence that the
# plugin boundary holds - the isolation suite loads real jars and asserts a plugin class is NOT the
# same Class object as BE's, SpiVersionTest and JniPluginSurfaceTest pin the version contract, and
# BePluginAddressTableTest pins the (plugin, factory) pairs BE addresses against what the plugins
# actually publish.
#
# What is NOT in there, and is worth knowing before trusting a green run: nothing loads a plugin out
# of its DEPLOYED directory. Surefire puts `provided` dependencies on the test classpath, so a
# dependency wrongly scoped is invisible to every test here; the only check on a deployed closure is
# the static one build.sh runs (tools/be-java-plugins/check_plugin_layout.py), and that one says so
# itself about anything reached by ServiceLoader or reflection.
#
# Listed one by one rather than as the aggregator: -pl on an aggregator selects that pom and none
# of its children. Keep in sync with the module list in build.sh, which is the other complete
# enumeration of this directory.
for be_java_extension in jni-spi jni-bootstrap plugin-toolkit hive-apache-shade hive-udf-shade \
    hadoop-deps iceberg-metadata-scanner hadoop-hudi-scanner java-udf jdbc-scanner paimon-scanner \
    max-compute-connector trino-connector-scanner java-writer; do
    FE_MODULES+=("be-java-extensions/${be_java_extension}")
done
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

bash "${DORIS_HOME}"/generated-source.sh

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
        "${MVN_CMD}" -Pcoverage test jacoco:report -pl "${MVN_MODULES}" -am -DfailIfNoTests=false -Dtest="$1"
    else
        "${MVN_CMD}" test -pl "${MVN_MODULES}" -am -Dcheckstyle.skip=true -DfailIfNoTests=false \
            -Dmaven.build.cache.enabled=false -Dtest="$1"
    fi
else
    echo "Run Frontend UT"
    if [[ "${COVERAGE}" -eq 1 ]]; then
        "${MVN_CMD}" -Pcoverage test jacoco:report -pl "${MVN_MODULES}" -am -DfailIfNoTests=false \
            -Dmaven.test.failure.ignore=true
    else
        "${MVN_CMD}" test -pl "${MVN_MODULES}" -am -Dcheckstyle.skip=true -DfailIfNoTests=false \
            -Dmaven.build.cache.enabled=false
    fi

    # Only reachable when maven itself exited 0, which under -Dmaven.test.failure.ignore=true it
    # does even with failing tests. Deliberately not run for --run: that invocation leaves every
    # other module's reports from an earlier run untouched, and those are not this run's results.
    fail_on_unparsed_test_failures
fi
