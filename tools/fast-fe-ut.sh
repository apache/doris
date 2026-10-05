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
# fast-fe-ut.sh — Fast FE unit test runner
#
# Usage:
#   ./fast-fe-ut.sh                                         # Run all UT
#   ./fast-fe-ut.sh org.apache.doris.utframe.Demo           # Run a specific test class
#   ./fast-fe-ut.sh DemoTest#testMethod1+testMethod2         # Run specific test methods
#   ./fast-fe-ut.sh org.apache.doris.Demo,org.apache.doris.Demo2  # Run multiple test classes
#
# How it works:
#   1. Fast incremental compile (main + test) via tools/fast-compile-fe.sh
#   2. If compilation succeeds → run tests via mvn surefire:test (skips Maven compile)
#   3. If compilation fails   → fall back to run-fe-ut.sh --run <test_spec>
#
# Limitations:
#   - EXTRA_FE_MODULES triggers automatic fallback (fast-compile-fe.sh only handles fe-core)
#   - Compilation failure triggers automatic fallback
#   - fast-compile-fe.sh does NOT recompile generated sources (antlr, Nereids
#     annotation processor output like GeneratedPlanPatterns).  After changing
#     enum values (e.g. PlanType, WorkloadMetricType) or annotation processor
#     inputs, a full Maven build is needed to refresh generated .class files.
#     Tests will fail with NoSuchFieldError/NoClassDefFoundError as a signal.

set -eo pipefail

DORIS_HOME="$(cd "$(dirname "$0")/.." && pwd)"
export DORIS_HOME

. "${DORIS_HOME}/env.sh"

# ─── Color Output ────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ─── Helper functions (from run-fe-ut.sh) ─────────────────────────────────────

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

# ─── Usage ──────────────────────────────────────────────────────────────────────

usage() {
    echo "
Usage: $0 [<test_spec>]
  test_spec              Optional. Class name, method, or comma-separated list.
                         If omitted, runs all FE unit tests.

  Environment variables:
     EXTRA_FE_MODULES    Optional FE feature modules in feature=module_path format, separated by commas.

  Eg.
    $0                                                     run all UT
    $0 org.apache.doris.utframe.Demo                       run a specific test class
    $0 org.apache.doris.utframe.Demo#testCreateDbAndTable  run specific test methods
    $0 org.apache.doris.Demo,org.apache.doris.Demo2        run multiple test classes
  "
    exit 1
}

# ─── Argument parsing ─────────────────────────────────────────────────────────

TEST_SPEC=""
if [[ $# -ge 1 ]]; then
    # Reject option-style arguments (e.g. --run, --coverage).  This script only
    # accepts a positional test spec; the --run prefix that run-fe-ut.sh uses is
    # not valid here.
    if [[ "$1" == -* ]]; then
        error "Unexpected option: $1"
        error "fast-fe-ut.sh accepts a positional test spec only (no --run prefix)."
        error "For --run / --coverage, use run-fe-ut.sh directly."
        usage
    fi
    TEST_SPEC="$1"
    shift
fi

# Reject extra arguments — only one test spec is supported.
if [[ $# -gt 0 ]]; then
    error "Unexpected extra arguments: $*"
    error "fast-fe-ut.sh accepts at most one test spec."
    usage
fi

# ─── Fallback helper ───────────────────────────────────────────────────────────

fallback() {
    local fallback_args=()
    if [[ -n "${TEST_SPEC}" ]]; then
        fallback_args=(--run "${TEST_SPEC}")
    fi
    warn "Falling back to run-fe-ut.sh ${fallback_args[*]} ..."
    exec bash "${DORIS_HOME}/run-fe-ut.sh" "${fallback_args[@]}"
}

# ─── Build module list ─────────────────────────────────────────────────────────

EXTRA_FE_MODULES="${EXTRA_FE_MODULES:-}"
parse_extra_fe_modules "${EXTRA_FE_MODULES}"

FE_MODULES=("fe-common" "fe-core")
for extra_module_path in "${FE_EXTRA_MODULE_PATHS[@]}"; do
    FE_MODULES+=("${extra_module_path}")
done
MVN_MODULES="$(IFS=','; echo "${FE_MODULES[*]}")"

echo "Fast FE UT Runner
    TEST_SPEC           -- ${TEST_SPEC:-<all>}
    EXTRA_FE_MODULES    -- ${EXTRA_FE_MODULES}
    MVN_MODULES         -- ${MVN_MODULES}
"

# fast-compile-fe.sh only handles fe-core sources.  Extra FE modules are built
# by the full Maven lifecycle, so fall back to run-fe-ut.sh when any are set.
if [[ -n "${EXTRA_FE_MODULES}" ]]; then
    warn "EXTRA_FE_MODULES is set, fast-compile-fe.sh cannot refresh those modules"
    fallback
fi

echo "**************************************"
echo "  Fast DorisFe Unittest (incremental) "
echo "**************************************"

# ─── Pre-build: generated sources ──────────────────────────────────────────────

bash "${DORIS_HOME}/generated-source.sh"

cd "${DORIS_HOME}/fe"
mkdir -p build/compile

if [[ -z "${FE_UT_PARALLEL}" ]]; then
    export FE_UT_PARALLEL=1
fi
echo "Unit test parallel is: ${FE_UT_PARALLEL}"

# ─── Step 1: Fast compile main sources ────────────────────────────────────────

echo ""
echo "=== Step 1/3: Fast compile main sources ==="
FAST_COMPILE_SCRIPT="${DORIS_HOME}/tools/fast-compile-fe.sh"

if [[ ! -f "${FAST_COMPILE_SCRIPT}" ]]; then
    warn "fast-compile-fe.sh not found"
    fallback
fi

if ! bash "${FAST_COMPILE_SCRIPT}" 2>&1; then
    warn "Fast compile (main) failed"
    fallback
fi

# ─── Step 2: Fast compile test sources ────────────────────────────────────────

echo ""
echo "=== Step 2/3: Fast compile test sources ==="

if ! bash "${FAST_COMPILE_SCRIPT}" --test 2>&1; then
    warn "Fast compile (test) failed"
    fallback
fi

# ─── Step 3: Check generated sources ──────────────────────────────────────────

# fast-compile-fe.sh only runs javac on src/main/java and src/test/java; it
# does not compile generated sources (antlr, annotation processor for Nereids
# patterns).  If any generated .java file is older than any Nereids source file
# (excluding the generator package), the generated output may be stale.  In that
# case we fall back to run-fe-ut.sh for a full Maven build that re-runs the
# annotation processor and recompiles the generated sources.
# ─── Step 3: Run tests via surefire ───────────────────────────────────────────

echo ""
echo "=== Step 3/3: Run unit tests (surefire) ==="

# Build the surefire command.
# -pl selects the modules to run tests in, -am includes their dependencies in the
# reactor so that ${revision} placeholders are resolved correctly for sibling
# modules (fe-foundation, fe-catalog, etc.) without triggering compilation.
SUREFIRE_ARGS=(
    surefire:test
    -pl "${MVN_MODULES}"
    -am
    -DfailIfNoTests=false
    -Dmaven.build.cache.enabled=false
)
if [[ -n "${TEST_SPEC}" ]]; then
    echo "Run the specified test: ${TEST_SPEC}"
    SUREFIRE_ARGS+=(-Dtest="${TEST_SPEC}")
else
    echo "Run all FE unit tests"
fi

echo "Command: ${MVN_CMD} ${SUREFIRE_ARGS[*]}"
"${MVN_CMD}" "${SUREFIRE_ARGS[@]}"

echo ""
info "Fast FE UT completed successfully!"
