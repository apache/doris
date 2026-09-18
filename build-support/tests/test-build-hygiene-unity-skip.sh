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
# Self-test for check-unity-skip-coverage.py. Seeds throwaway files (a src
# .cpp that is in no skip list, plus probe tests under be/test) to prove:
#   RED    a test including an unlisted, unity-batched src .cpp is flagged,
#          with the exact skip entry to add
#   SILENT the literal-entry idiom (cross-directory APPEND included)
#   SILENT the whole-target opt-out idiom (set(<VAR> ${SRC_FILES}))
#   SILENT a commented-out include
# and that the clean tree is green before and after.
#
# Creates files only (never edits tracked ones); removes them via EXIT trap.
#
# Usage:  bash build-support/tests/test-build-hygiene-unity-skip.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GATE="${ROOT}/build-support/check-unity-skip-coverage.py"
# Not ${PYTHON}: env.sh exports that as the build's interpreter (python2 on
# the build-env image). Same selector the gates themselves use.
PYTHON="${BUILD_HYGIENE_PYTHON:-python3}"

PROBE_SRC="${ROOT}/be/src/util/tmp_hygiene_probe.cpp"
PROBE_DIR="${ROOT}/be/test/tmp_hygiene_probe"
cleanup() { rm -f "${PROBE_SRC}"; rm -rf "${PROBE_DIR}"; }
trap cleanup EXIT

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

# ---- baseline: clean tree green ----
OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "clean tree expected green, got ${EC}:"$'\n'"${OUT}"

# ---- fixtures ----
echo "namespace doris { int tmp_hygiene_probe_symbol = 0; }" > "${PROBE_SRC}"
mkdir -p "${PROBE_DIR}"
# RED: util sets UNITY_BUILD and lists skips literally; the probe src is new,
# so it is in no list.
printf '#include "util/tmp_hygiene_probe.cpp"\n' > "${PROBE_DIR}/red_probe_test.cpp"
# SILENT: cross-directory literal entry (appended into format's skip list).
printf '#include "format_v2/table/adbc_reader.cpp"\n' > "${PROBE_DIR}/literal_probe_test.cpp"
# SILENT: whole-target opt-out (format skip list is seeded from SRC_FILES).
printf '#include "format/orc/orc_file_reader.cpp"\n' > "${PROBE_DIR}/wholetarget_probe_test.cpp"
# SILENT: commented include is not an include.
printf '//#include "format_v2/parquet/reader/native/decoder.cpp"\n' > "${PROBE_DIR}/comment_probe_test.cpp"

OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 1 ] || fail "expected exit 1 with the red probe present, got ${EC}"
N="$(echo "${OUT}" | grep -c '^error:')"
[ "${N}" -eq 1 ] || fail "expected exactly 1 violation (silent probes must stay silent), got ${N}:"$'\n'"${OUT}"
echo "${OUT}" | grep -q "red_probe_test.cpp:1 includes be/src/util/tmp_hygiene_probe.cpp" \
    || fail "red probe not reported"
echo "${OUT}" | grep -q 'add ${CMAKE_CURRENT_SOURCE_DIR}/tmp_hygiene_probe.cpp to the doris_skip_unity_inclusion list in be/src/util/CMakeLists.txt' \
    || fail "fix path does not name the exact entry and CMakeLists"
echo "${OUT}" | grep -q "duplicate strong symbols" \
    || fail "failure lacks the mechanism explanation"

# ---- GREEN again once the red probe is gone ----
rm -f "${PROBE_DIR}/red_probe_test.cpp"
OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "expected green with only silent probes, got ${EC}:"$'\n'"${OUT}"

cleanup
trap - EXIT
OUT="$("${PYTHON}" "${GATE}" 2>&1)"; EC=$?
[ "${EC}" -eq 0 ] || fail "restored tree expected green, got ${EC}:"$'\n'"${OUT}"

if [ "${FAILED}" -eq 0 ]; then
    echo "PASS: unlisted include flagged with exact fix; literal/whole-target/comment cases stay silent."
    exit 0
fi
exit 1
