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
# Self-test for check-extern-template-pairing.py: both pairing directions must
# fire on injection and the clean tree must stay green (which also proves the
# normalizer -- namespace dropping, alias table, comment stripping -- because
# the real tree pairs ColumnStr<UInt32> with ColumnStr<uint32_t> and the wide
# to_string declarations with their integer<...> definitions).
#
# Injects into working-tree files, restores via EXIT trap. Do not run
# concurrently with a build/configure of the same tree.
#
# Usage:  bash build-support/tests/test-build-hygiene-extern-pairing.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GATE="${ROOT}/build-support/check-extern-template-pairing.py"
# Not ${PYTHON}: env.sh exports that as the build's interpreter (python2 on
# the build-env image). Same selector the gates themselves use.
PYTHON="${BUILD_HYGIENE_PYTHON:-python3}"

HEADER="be/src/core/column/column_vector.h"
BACKUP="$(mktemp -d)"
cp "${ROOT}/${HEADER}" "${BACKUP}/column_vector.h"
restore() { cp "${BACKUP}/column_vector.h" "${ROOT}/${HEADER}"; rm -rf "${BACKUP}"; }
trap restore EXIT

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

# ---- baseline: clean tree green (normalizer handles the real alias pairs) ----
OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "clean tree expected green, got ${EC}:"$'\n'"${OUT}"
echo "${OUT}" | grep -q "all paired" || fail "clean run lacks the summary line"

# ---- RED (the silent case): drop one extern, its definition loses its pair ----
# Commenting the declaration out is exactly the "header forgot the extern"
# regression the reverse direction exists for.
sed -i.bak \
    's|^extern template class ColumnVector<TYPE_BOOLEAN>;|// injected-out: extern template class ColumnVector<TYPE_BOOLEAN>;|' \
    "${ROOT}/${HEADER}" && rm -f "${ROOT}/${HEADER}.bak"
grep -q "injected-out" "${ROOT}/${HEADER}" || fail "reverse injection did not apply"
OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 1 ] || fail "reverse injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "explicit instantiation lacks the matching 'extern template'" \
    || fail "reverse violation not reported"
echo "${OUT}" | grep -q "ColumnVector<TYPE_BOOLEAN>" \
    || fail "reverse violation does not name the specialization"
echo "${OUT}" | grep -q "extern-covered in be/src/core/column/column_vector.h" \
    || fail "reverse violation does not point at the family's header"
echo "${OUT}" | grep -q "fix:" || fail "reverse violation lacks a fix path"
cp "${BACKUP}/column_vector.h" "${ROOT}/${HEADER}"

# ---- RED: an extern declaration with no definition anywhere ----
printf 'extern template class ColumnVector<TYPE_DATETIMEV2_FAKE_INJECTED>;\n' \
    >> "${ROOT}/${HEADER}"
OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 1 ] || fail "forward injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "pairs with no explicit instantiation definition" \
    || fail "forward violation not reported"
echo "${OUT}" | grep -q "TYPE_DATETIMEV2_FAKE_INJECTED" \
    || fail "forward violation does not name the declaration"
cp "${BACKUP}/column_vector.h" "${ROOT}/${HEADER}"

# ---- GREEN again ----
OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "restored tree expected green, got ${EC}:"$'\n'"${OUT}"

if [ "${FAILED}" -eq 0 ]; then
    echo "PASS: both pairing directions fire on injection and restore to green."
    exit 0
fi
exit 1
