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
ARCHIVE_DIR="${1:?Usage: $0 directory-containing-the-pinned-lance-c-archive}"
ARCHIVE_DIR="$(cd "${ARCHIVE_DIR}" && pwd)"
TP_DIR="${ROOT}"
# Load only repository-owned definitions, never extracted dependency code.
source "${ROOT}/vars.sh"
mkdir -p "${ROOT}/src"
tmpdir="$(mktemp -d "${ROOT}/src/lance-prefilter-test.XXXXXX")"
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
    cp "${ARCHIVE_DIR}/${LANCE_C_NAME}" "${dest}/src/"
}

run_download() {
    TP_DIR="$1" DORIS_HOME="${tmpdir}" bash "${ROOT}/download-thirdparty.sh" lance_c
}

check_sources() {
    local source="$1/src/${LANCE_C_SOURCE}"
    grep -q 'fn test_scanner_nearest_segment_prefilter_statistics' "${source}/tests/c_api_test.rs" \
        || fail "missing upstream segment-prefilter regression"
    grep -q 'lance_session_new_with_data_cache' "${source}/src/foyer_data_cache.rs" \
        || fail "missing retained Foyer API"
    grep -q 'lance_dataset_get_data_cache_statistics' "${source}/src/data_cache.rs" \
        || fail "missing retained cache statistics API"
    grep -q 'source = "git+https://github.com/lance-format/lance.git' "${source}/Cargo.lock" \
        || fail "Lance must come from the upstream git dependency"
    [[ -f "${source}/patched_mark_foyer" ]] || fail "missing Foyer patch marker"
}

prepare "${tmpdir}/fresh"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
cp "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/Cargo.toml" "${tmpdir}/manifest"
cp "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/Cargo.lock" "${tmpdir}/lock"
touch "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/cache_reuse_sentinel"
run_download "${tmpdir}/fresh"
[[ -f "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/cache_reuse_sentinel" ]] \
    || fail "unchanged patch unnecessarily replaced cached sources"
cmp "${tmpdir}/manifest" "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/Cargo.toml"
cmp "${tmpdir}/lock" "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/Cargo.lock"
echo "PASS: fresh archive and idempotent Foyer patch"

# Re-extraction must apply Foyer again; no separate Lance source is required.
rm -rf "${tmpdir}/fresh/src/${LANCE_C_SOURCE}"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
cmp "${tmpdir}/lock" "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/Cargo.lock"
echo "PASS: re-extracted archive"

prepare "${tmpdir}/cached"
tar xzf "${ARCHIVE_DIR}/${LANCE_C_NAME}" -C "${tmpdir}/cached/src"
# A generic marker from an earlier build must not suppress the Foyer patch.
touch "${tmpdir}/cached/src/${LANCE_C_SOURCE}/patched_mark"
run_download "${tmpdir}/cached"
check_sources "${tmpdir}/cached"
cmp "${tmpdir}/lock" "${tmpdir}/cached/src/${LANCE_C_SOURCE}/Cargo.lock"
echo "PASS: cached sources with an existing generic marker"

# A cached source tree may already contain the previous Foyer patch. The patch
# fingerprint must invalidate it even when the upstream archive is unchanged.
cached_source="${tmpdir}/cached/src/${LANCE_C_SOURCE}"
for stale_marker in '' '0 0'; do
    printf '%s\n' 'stale patch contents' > "${cached_source}/src/foyer_data_cache.rs"
    printf '%s\n' "${stale_marker}" > "${cached_source}/patched_mark_foyer"
    run_download "${tmpdir}/cached"
    check_sources "${tmpdir}/cached"
    cmp "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/src/foyer_data_cache.rs" \
        "${cached_source}/src/foyer_data_cache.rs"
done
echo "PASS: old and mismatched Foyer markers refresh cached sources"

prepare "${tmpdir}/invalid"
tar xzf "${ARCHIVE_DIR}/${LANCE_C_NAME}" -C "${tmpdir}/invalid/src"
printf '%s\n' 'incompatible manifest' > "${tmpdir}/invalid/src/${LANCE_C_SOURCE}/Cargo.toml"
if run_download "${tmpdir}/invalid" > "${tmpdir}/invalid.log" 2>&1; then
    fail "expected an incompatible source to reject the patch"
fi
[[ ! -f "${tmpdir}/invalid/src/${LANCE_C_SOURCE}/patched_mark_foyer" ]] \
    || fail "failed patch was marked complete"
echo "PASS: patch failure does not mark sources ready"
