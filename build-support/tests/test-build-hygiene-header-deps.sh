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
    "be/src/storage/index/inverted/query/spi/index_source.h"
    "be/src/storage/index/inverted/query/engine/intersection.cpp"
    "be/src/storage/index/inverted/common/query_info.h"
    "be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"
    "be/src/storage/index/inverted/analysis/analyzer/analyzer_provider.h"
)
# Files the CLucene cases create; restore() removes them.
PROBE_INNER="be/src/util/zz_clucene_probe_inner.h"
PROBE_OUTER="be/src/util/zz_clucene_probe_outer.h"
for t in "${TARGETS[@]}"; do
    mkdir -p "${BACKUP}/$(dirname "${t}")"
    cp "${ROOT}/${t}" "${BACKUP}/${t}"
done
restore() {
    for t in "${TARGETS[@]}"; do
        cp "${BACKUP}/${t}" "${ROOT}/${t}"
    done
    rm -f "${ROOT}/${PROBE_INNER}" "${ROOT}/${PROBE_OUTER}"
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

# ---- RED: the shared query SPI must not include a storage format ----
printf '#include "storage/index/inverted/clucene/inverted_index_searcher.h"\n' \
    >> "${ROOT}/be/src/storage/index/inverted/query/spi/index_source.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "query-format rule injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "query/spi/index_source.h must not reach storage/index/inverted/clucene/" \
    || fail "query-format rule violation not reported"
echo "${OUT}" | grep -q "fix:" || fail "query-format rule lacks a fix path"
cp "${BACKUP}/be/src/storage/index/inverted/query/spi/index_source.h" \
    "${ROOT}/be/src/storage/index/inverted/query/spi/index_source.h"

# ---- RED: a directory's third-party ban (query headers must not include CLucene) ----
printf '#include <CLucene.h>\n' \
    >> "${ROOT}/be/src/storage/index/inverted/query/spi/index_source.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "directory angle-ban injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "query/spi/index_source.h must not include <CLucene.h>" \
    || fail "directory angle-ban violation not reported"
cp "${BACKUP}/be/src/storage/index/inverted/query/spi/index_source.h" \
    "${ROOT}/be/src/storage/index/inverted/query/spi/index_source.h"

# ---- RED: third-party ban (uint24.h must not include fmt) ----
# CLucene-free roots: an angle include two hops away from a root.
printf '#pragma once\n#include <CLucene/util/Misc.h>\n' > "${ROOT}/${PROBE_INNER}"
printf '#pragma once\n#include "util/zz_clucene_probe_inner.h"\n' > "${ROOT}/${PROBE_OUTER}"
printf '#include "util/zz_clucene_probe_outer.h"\n' \
    >> "${ROOT}/be/src/storage/index/inverted/query/spi/index_source.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "two-hop CLucene include expected exit 1, got ${EC}"
echo "${OUT}" | grep -q \
    "query/spi/index_source.h reaches CLucene through util/zz_clucene_probe_inner.h" \
    || fail "two-hop CLucene include not reported"
echo "${OUT}" | grep -q -- "-> <CLucene/util/Misc.h>" \
    || fail "CLucene closure failure lacks the include at the end of its chain"
cp "${BACKUP}/be/src/storage/index/inverted/query/spi/index_source.h" \
    "${ROOT}/be/src/storage/index/inverted/query/spi/index_source.h"
rm -f "${ROOT}/${PROBE_INNER}" "${ROOT}/${PROBE_OUTER}"

# A source file under a root is a root too. The leading newline matters: the file does not end
# with one.
printf '\n#include <CLucene.h>\n' \
    >> "${ROOT}/be/src/storage/index/inverted/query/engine/intersection.cpp"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "CLucene include in a root source expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "query/engine/intersection.cpp includes <CLucene.h>" \
    || fail "CLucene include in a root source not reported"
cp "${BACKUP}/be/src/storage/index/inverted/query/engine/intersection.cpp" \
    "${ROOT}/be/src/storage/index/inverted/query/engine/intersection.cpp"

# An exception that no longer breaks the rule fails the gate.
sed -i '/#include <CLucene/d' \
    "${ROOT}/be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "stale CLucene include exception expected exit 1, got ${EC}"
echo "${OUT}" | grep -q \
    "CLUCENE_INCLUDING_FILES lists storage/index/inverted/analysis/tokenizer/pinyin/pinyin_alphabet_tokenizer.h" \
    || fail "stale CLucene include exception not reported"
cp "${BACKUP}/be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_alphabet_tokenizer.h" \
    "${ROOT}/be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"

# CLucene words in comments and string literals are no dependency; a symbol in code is.
printf '\n// lucene::analysis::Token in a comment\ninline const char* clucene_gate_probe = "CLuceneError lucene::Token";\n' \
    >> "${ROOT}/be/src/storage/index/inverted/common/query_info.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 0 ] || fail "CLucene words in comments and strings expected green, got ${EC}:"$'\n'"${OUT}"
printf '\ninline lucene::analysis::Token* clucene_gate_symbol = nullptr;\n' \
    >> "${ROOT}/be/src/storage/index/inverted/common/query_info.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "CLucene symbol in a root expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "common/query_info.h:[0-9]* names the CLucene symbol lucene::" \
    || fail "CLucene symbol in a root not reported"
cp "${BACKUP}/be/src/storage/index/inverted/common/query_info.h" \
    "${ROOT}/be/src/storage/index/inverted/common/query_info.h"

# A symbol exception that no longer breaks the rule fails the gate.
sed -i 's/lucene::/clucene_gate_probe::/g' \
    "${ROOT}/be/src/storage/index/inverted/analysis/analyzer/analyzer_provider.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "stale CLucene symbol exception expected exit 1, got ${EC}"
echo "${OUT}" | grep -q \
    "CLUCENE_SYMBOL_FILES lists storage/index/inverted/analysis/analyzer/analyzer_provider.h" \
    || fail "stale CLucene symbol exception not reported"
cp "${BACKUP}/be/src/storage/index/inverted/analysis/analyzer/analyzer_provider.h" \
    "${ROOT}/be/src/storage/index/inverted/analysis/analyzer/analyzer_provider.h"

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

# ---- RED: threadpool reach remains bounded after its baseline increase ----
printf '#include "util/threadpool.h"\n' >> "${ROOT}/be/src/common/logging.h"
OUT="$(run_gate)"; EC=$?
[ "${EC}" -eq 1 ] || fail "threadpool reach injection expected exit 1, got ${EC}"
echo "${OUT}" | grep -q "util/threadpool.h now reaches" \
    || fail "threadpool reverse budget breach not reported"
echo "${OUT}" | grep -q -- "--reach util/threadpool.h" \
    || fail "threadpool reach failure lacks the --reach fix pointer"
cp "${BACKUP}/be/src/common/logging.h" "${ROOT}/be/src/common/logging.h"

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
    echo "PASS: rules/bans/budgets/pch-lock/CLucene roots all fire on injection, restore to green, diagnostics work."
    exit 0
fi
exit 1
