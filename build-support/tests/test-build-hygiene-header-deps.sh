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
# Self-test for the check-header-deps.py gate families: layering rules,
# third-party bans, forward/reverse budgets, pch whitelist.
#
# The gate runs against the real tree (its rule and budget tables name real
# headers), so RED cases are proven by injecting one violation per family into
# a working-tree file and GREEN by restoring it. Injected files are backed up
# first and restored by an EXIT trap. Do not run concurrently with a
# build/configure of the same tree.
#
# Usage:  bash build-support/tests/test-build-hygiene-header-deps.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GATE="${ROOT}/build-support/check-header-deps.py"
# Not ${PYTHON}: env.sh exports that as the build's interpreter (python2 on
# the build-env image). Same selector the gates themselves use.
PYTHON="${BUILD_HYGIENE_PYTHON:-python3}"

BACKUP="$(mktemp -d)"
TARGETS=(
    "be/src/core/types.h"
    "be/src/core/uint24.h"
    "be/src/common/logging.h"
    "be/src/util/pretty_printer.h"
    "be/src/pch/pch.h"
)
for t in "${TARGETS[@]}"; do
    mkdir -p "${BACKUP}/$(dirname "${t}")"
    cp "${ROOT}/${t}" "${BACKUP}/${t}"
done
restore() {
    for t in "${TARGETS[@]}"; do
        cp "${BACKUP}/${t}" "${ROOT}/${t}"
    done
    rm -rf "${BACKUP}"
}
trap restore EXIT

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

run_gate() { "${PYTHON}" "${GATE}" 2>&1; }

# ---- baseline: the clean tree is green ----
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 0 ] || { fail "clean tree expected green, got ${EC}:"$'\n'"${OUT}"; }

# ---- RED: layering rule (types.h must not reach olap_common.h) ----
printf '#include "storage/olap_common.h"\n' >> "${ROOT}/be/src/core/types.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "rule injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "core/types.h must not reach storage/olap_common.h" \
    || fail "rule violation not reported for types.h -> olap_common.h"
echo "${OUT}" | grep -q "chain:" || fail "rule failure lacks the include chain"
echo "${OUT}" | grep -q "fix:" || fail "rule failure lacks a fix path"
cp "${BACKUP}/be/src/core/types.h" "${ROOT}/be/src/core/types.h"

# ---- RED: third-party ban (uint24.h must not include fmt) ----
printf '#include <fmt/format.h>\n' >> "${ROOT}/be/src/core/uint24.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "angle-ban injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "core/uint24.h must not include <fmt/format.h>" \
    || fail "angle-ban violation not reported"
cp "${BACKUP}/be/src/core/uint24.h" "${ROOT}/be/src/core/uint24.h"

# ---- RED: forward closure budget (logging.h is budgeted at 0) ----
printf '#include "util/uid_util.h"\n' >> "${ROOT}/be/src/common/logging.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "forward-budget injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "common/logging.h include closure grew" \
    || fail "forward budget breach not reported"
echo "${OUT}" | grep -q -- "--closure common/logging.h" \
    || fail "forward budget failure lacks the --closure fix pointer"
cp "${BACKUP}/be/src/common/logging.h" "${ROOT}/be/src/common/logging.h"

# ---- RED: reverse reach budget (workload_group.h must not spread) ----
printf '#include "runtime/workload_group/workload_group.h"\n' \
    >> "${ROOT}/be/src/util/pretty_printer.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "reverse-budget injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "runtime/workload_group/workload_group.h now reaches" \
    || fail "reverse budget breach not reported"
echo "${OUT}" | grep -q -- "--reach runtime/workload_group/workload_group.h" \
    || fail "reverse budget failure lacks the --reach fix pointer"
cp "${BACKUP}/be/src/util/pretty_printer.h" "${ROOT}/be/src/util/pretty_printer.h"

# ---- RED: pch whitelist (quoted include added to pch.h) ----
printf '#include "common/logging.h"\n' >> "${ROOT}/be/src/pch/pch.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "pch injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "pch/pch.h quoted includes diverged from the whitelist" \
    || fail "pch whitelist divergence not reported"
echo "${OUT}" | grep -q 'added:   "common/logging.h"' \
    || fail "pch failure does not name the added include"
cp "${BACKUP}/be/src/pch/pch.h" "${ROOT}/be/src/pch/pch.h"

# ---- diagnostics: --closure and --reach stay usable ----
OUT="$("${PYTHON}" "${GATE}" --closure common/status.h 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "--closure expected exit 0, got ${EC}"
echo "${OUT}" | grep -q "common/status.h: .* project header(s)" \
    || fail "--closure output missing summary line"
OUT="$("${PYTHON}" "${GATE}" --reach io/fs/s3_file_system.h 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "--reach expected exit 0, got ${EC}"
echo "${OUT}" | grep -q "via this edge alone" \
    || fail "--reach output missing edge ranking"

# ---- GREEN again after all restores ----
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 0 ] || fail "restored tree expected green, got ${EC}:"$'\n'"${OUT}"

if [ "${FAILED}" -eq 0 ]; then
    echo "PASS: rules/bans/budgets/pch-lock all fire on injection, restore to green, diagnostics work."
    exit 0
fi
exit 1
