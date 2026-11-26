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
# Self-test for the check-build-hygiene.sh unified entry:
#   - clean tree exits 0 with the summary line
#   - with violations in TWO different checks, BOTH are reported (the entry
#     keeps running after a failing check) and the exit code is 1
#   - a missing python3 is a loud error (exit 2) naming the OFF switch, never
#     a silent skip
#   - env.sh's PYTHON (python2 on the build-env image) is not consulted
#
# Injects into working-tree files, restores via EXIT trap. Do not run
# concurrently with a build/configure of the same tree.
#
# Usage:  bash build-support/tests/test-build-hygiene-entry.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENTRY="${ROOT}/build-support/check-build-hygiene.sh"

UINT24="be/src/core/uint24.h"
COLVEC="be/src/core/column/column_vector.h"
BACKUP="$(mktemp -d)"
cp "${ROOT}/${UINT24}" "${BACKUP}/uint24.h"
cp "${ROOT}/${COLVEC}" "${BACKUP}/column_vector.h"
restore() {
    cp "${BACKUP}/uint24.h" "${ROOT}/${UINT24}"
    cp "${BACKUP}/column_vector.h" "${ROOT}/${COLVEC}"
    rm -rf "${BACKUP}"
}
trap restore EXIT

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

# ---- clean tree: exit 0, summary line present ----
OUT="$(bash "${ENTRY}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "clean tree expected exit 0, got ${EC}:"$'\n'"${OUT}"
echo "${OUT}" | grep -q "build hygiene: all checks passed" \
    || fail "clean run lacks the all-passed summary"

# ---- two violations in two different checks: both reported, exit 1 ----
printf '#include <fmt/format.h>\n' >> "${ROOT}/${UINT24}"
printf 'extern template class ColumnVector<TYPE_FAKE_ENTRY_TEST>;\n' >> "${ROOT}/${COLVEC}"
OUT="$(bash "${ENTRY}" 2>&1)"; EC=$?
[ "${EC}" -eq 1 ] || fail "violations expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "core/uint24.h must not include <fmt/format.h>" \
    || fail "header-deps violation missing from aggregated output"
echo "${OUT}" | grep -q "TYPE_FAKE_ENTRY_TEST" \
    || fail "pairing violation missing from aggregated output (entry must run every check)"
echo "${OUT}" | grep -q "2 check(s) failed" \
    || fail "aggregated failure count missing or wrong"
restore
trap - EXIT
BACKUP="$(mktemp -d)"  # restore() removed it; re-arm for the paths below
cp "${ROOT}/${UINT24}" "${BACKUP}/uint24.h"
cp "${ROOT}/${COLVEC}" "${BACKUP}/column_vector.h"
trap restore EXIT

# ---- unusable BUILD_HYGIENE_PYTHON: loud exit 2 with the OFF-switch hint ----
OUT="$(BUILD_HYGIENE_PYTHON=/nonexistent/python3 bash "${ENTRY}" 2>&1)"; EC=$?
[ "${EC}" -eq 2 ] || fail "missing python expected exit 2, got ${EC}"
echo "${OUT}" | grep -q "not found" || fail "missing-python error not reported"
echo "${OUT}" | grep -q "ENABLE_BUILD_HYGIENE=OFF" \
    || fail "missing-python error lacks the OFF-switch hint"

# ---- env.sh's PYTHON is not consulted ----
# env.sh exports PYTHON=${DORIS_BUILD_PYTHON_VERSION:-python}, which is python2
# on the build-env image; honouring it made every gate die with a SyntaxError on
# its first f-string. The gates must resolve their own Python 3 regardless.
OUT="$(PYTHON=/nonexistent/python2 bash "${ENTRY}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] \
    || fail "the gates must resolve their own python3, not env.sh's PYTHON (exit ${EC}):"$'\n'"${OUT}"

# ---- green again ----
OUT="$(bash "${ENTRY}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "restored tree expected exit 0, got ${EC}:"$'\n'"${OUT}"

if [ "${FAILED}" -eq 0 ]; then
    echo "PASS: entry aggregates all checks, resolves its own python3, fails loud without one, and is green when clean."
    exit 0
fi
exit 1
