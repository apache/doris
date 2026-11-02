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

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
ARCHIVE_DIR="${1:-${ROOT}/src}"
ARCHIVE_DIR="$(cd "${ARCHIVE_DIR}" && pwd)"
TP_DIR="${ROOT}"
source "${ROOT}/vars.sh"
[[ -f "${ARCHIVE_DIR}/${PARALLEL_HASHMAP_NAME}" ]] || {
    echo "FAIL: provide the directory containing ${PARALLEL_HASHMAP_NAME}" >&2
    exit 1
}

mkdir -p "${ROOT}/src"
tmpdir="$(mktemp -d "${ROOT}/src/phmap-patch-test.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

prepare() {
    local dest="$1"
    mkdir -p "${dest}/src"
    cp "${ROOT}/vars.sh" "${dest}/vars.sh"
    ln -s "${ROOT}/patches" "${dest}/patches"
    cp "${ARCHIVE_DIR}/${PARALLEL_HASHMAP_NAME}" "${dest}/src/"
}

run_download() {
    TP_DIR="$1" DORIS_HOME="${tmpdir}" bash "${ROOT}/download-thirdparty.sh" parallel_hashmap
}

check_sources() {
    local source="$1/src/${PARALLEL_HASHMAP_SOURCE}"
    [[ -f "${source}/patched_mark_aarch64_int128" ]] || fail "missing int128 marker"
    [[ -f "${source}/patched_mark_arm_neon" ]] || fail "missing NEON marker"
    [[ "$(grep -c '^struct GroupAArch64Impl {' "${source}/parallel_hashmap/phmap.h")" == 1 ]] \
        || fail "NEON group must appear exactly once"
    cmp "${tmpdir}/expected/phmap.h" "${source}/parallel_hashmap/phmap.h"
    cmp "${tmpdir}/expected/phmap_config.h" "${source}/parallel_hashmap/phmap_config.h"
}

snapshot() {
    local source="$1/src/${PARALLEL_HASHMAP_SOURCE}/parallel_hashmap"
    cp "${source}/phmap.h" "${tmpdir}/before-phmap.h"
    cp "${source}/phmap_config.h" "${tmpdir}/before-phmap_config.h"
}

expect_failure() {
    local dest="$1"
    if run_download "${dest}" >"${dest}/failure.log" 2>&1; then
        fail "incompatible or partial patch unexpectedly succeeded"
    fi
    local source="${dest}/src/${PARALLEL_HASHMAP_SOURCE}"
    [[ ! -f "${source}/patched_mark_arm_neon" ]] || fail "failed patch retained its marker"
    cmp "${tmpdir}/before-phmap.h" "${source}/parallel_hashmap/phmap.h"
    cmp "${tmpdir}/before-phmap_config.h" "${source}/parallel_hashmap/phmap_config.h"
}

prepare "${tmpdir}/fresh"
run_download "${tmpdir}/fresh"
source_dir="${tmpdir}/fresh/src/${PARALLEL_HASHMAP_SOURCE}"
mkdir "${tmpdir}/expected"
cp "${source_dir}/parallel_hashmap/phmap.h" "${tmpdir}/expected/"
cp "${source_dir}/parallel_hashmap/phmap_config.h" "${tmpdir}/expected/"
check_sources "${tmpdir}/fresh"
touch "${source_dir}/cache_reuse_sentinel"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
[[ -f "${source_dir}/cache_reuse_sentinel" ]] || fail "rerun replaced cached sources"
echo "PASS: fresh sources and normal rerun"

# Model a process stopping between the completed patch and the marker write.
rm "${source_dir}/patched_mark_arm_neon"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
echo "PASS: completed patch without a marker recovers without reapplying"

# A stale marker must not suppress applying the patch to pristine sources.
prepare "${tmpdir}/stale"
tar xzf "${ARCHIVE_DIR}/${PARALLEL_HASHMAP_NAME}" -C "${tmpdir}/stale/src"
touch "${tmpdir}/stale/src/${PARALLEL_HASHMAP_SOURCE}/patched_mark_arm_neon"
run_download "${tmpdir}/stale"
check_sources "${tmpdir}/stale"
echo "PASS: stale marker on pristine sources"

# One patched file and one unpatched file must be rejected before either changes.
# Keep the real int128 patch applied, as it precedes NEON in the downloader.
for marker in missing present; do
    prepare "${tmpdir}/partial-${marker}"
    tar xzf "${ARCHIVE_DIR}/${PARALLEL_HASHMAP_NAME}" -C "${tmpdir}/partial-${marker}/src"
    partial_source="${tmpdir}/partial-${marker}/src/${PARALLEL_HASHMAP_SOURCE}"
    (
        cd "${partial_source}"
        patch --batch --forward --fuzz=0 -p1 \
            <"${ROOT}/patches/parallel-hashmap-1.3.8-aarch64-int128.patch"
        touch patched_mark_aarch64_int128
    )
    cp "${tmpdir}/expected/phmap.h" "${partial_source}/parallel_hashmap/phmap.h"
    if [[ "${marker}" == present ]]; then
        touch "${partial_source}/patched_mark_arm_neon"
    fi
    snapshot "${tmpdir}/partial-${marker}"
    expect_failure "${tmpdir}/partial-${marker}"
done
echo "PASS: partial sources fail without further edits or a success marker"

# Duplicate definitions reproduce the old insertion-only patch's replay result.
prepare "${tmpdir}/duplicate"
run_download "${tmpdir}/duplicate"
duplicate_source="${tmpdir}/duplicate/src/${PARALLEL_HASHMAP_SOURCE}"
awk '
    /^#if PHMAP_HAVE_ARM_NEON && defined\(PHMAP_IS_LITTLE_ENDIAN\)/ && !found { copying = 1 }
    copying { block = block $0 "\n" }
    copying && /^#endif/ { copying = 0; found = 1 }
    { print }
    found && !inserted { printf "%s", block; inserted = 1 }
' "${duplicate_source}/parallel_hashmap/phmap.h" >"${tmpdir}/duplicate-phmap.h"
mv "${tmpdir}/duplicate-phmap.h" "${duplicate_source}/parallel_hashmap/phmap.h"
snapshot "${tmpdir}/duplicate"
expect_failure "${tmpdir}/duplicate"
echo "PASS: duplicate NEON definitions are rejected despite an existing marker"
