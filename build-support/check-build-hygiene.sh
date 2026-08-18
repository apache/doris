#!/bin/bash
#
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
#
# Unified entry for the BE build-hygiene gates. Runs every seconds-level,
# zero-build-dependency check that protects the backend's compile-time
# invariants:
#
#   check-header-deps.py             header layering rules, third-party bans,
#                                    the two-axis closure/reach budgets and the
#                                    pch include lock
#   check-extern-template-pairing.py extern template declarations and explicit
#                                    instantiation definitions stay paired in
#                                    both directions
#   check-unity-skip-coverage.py     every src .cpp a test #includes is opted
#                                    out of unity batching
#
# All checks are pure text scans and run in about a second combined. Every
# failure message carries the mechanism (why the edge/entry is expensive) and
# the fix path. All checks run even when an early one fails, so one pass shows
# everything there is to fix.
#
# Mounted at configure time by be/CMakeLists.txt (ENABLE_BUILD_HYGIENE, default
# ON), which is what makes a violation a build error rather than advice. To run
# by hand:
#
#   build-support/check-build-hygiene.sh
#
# Exit code: 0 -- all checks passed; 1 -- at least one violation (details on
# stderr); 2 -- environment problem (python3 missing).
#
# Self-tests: build-support/tests/test-build-hygiene-*.sh (run.sh runs them).

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PYTHON="${PYTHON:-python3}"
if ! command -v "${PYTHON}" >/dev/null 2>&1; then
    echo "check-build-hygiene: ${PYTHON} not found; the build-hygiene gates need it." >&2
    echo "Install python3, or re-run with -DENABLE_BUILD_HYGIENE=OFF to skip the gates." >&2
    exit 2
fi

FAILURES=0
for check in \
    check-header-deps.py \
    check-extern-template-pairing.py \
    check-unity-skip-coverage.py; do
    if ! "${PYTHON}" "${SCRIPT_DIR}/${check}"; then
        FAILURES=$((FAILURES + 1))
    fi
done

if [ "${FAILURES}" -ne 0 ]; then
    echo "" >&2
    echo "build hygiene: ${FAILURES} check(s) failed (details above)." >&2
    echo "Each message ends with its fix path; the budget/whitelist tables live in the" >&2
    echo "check scripts themselves, so a deliberate change is a one-line, reviewed diff." >&2
    exit 1
fi
echo "build hygiene: all checks passed"
