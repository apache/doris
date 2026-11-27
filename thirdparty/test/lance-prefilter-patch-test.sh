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
# Platform definitions must remain safe when this harness enables nounset.
for test_arch in x86_64 arm64; do
    bash -eu -c '
        uname() { if [[ "$1" == -s ]]; then echo Darwin; else echo "$TEST_ARCH"; fi; }
        unset ARROW_ADBC_FLIGHTSQL_SOURCE
        TP_DIR="$1"
        TEST_ARCH="$2"
        source "$TP_DIR/vars.sh"
        [[ " ${TP_ARCHIVES[*]} " != *" ARROW_ADBC_FLIGHTSQL "* ]]
    ' _ "${ROOT}" "${test_arch}"
done
echo "PASS: macOS platform definitions under nounset"

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
    grep -Fq "lance = { path = \"../${LANCE_NATIVE_SOURCE}/rust/lance\" }" \
        "${source}/Cargo.toml" || fail "missing pinned Lance source overlay"
    [[ -f "$1/src/${LANCE_NATIVE_SOURCE}/rust/lance-core/src/memory.rs" ]] \
        || fail "missing Lance memory implementation"
    if grep -q 'source = "git+https://github.com/lance-format/lance.git' "${source}/Cargo.lock"; then
        fail "Lance dependencies must share the patched pinned source tree"
    fi
    # Refreshing the Foyer patch must preserve the upstream prewarm binding and tests.
    grep -q 'lance_dataset_prewarm_index' "${source}/include/lance/lance.h" \
        || fail "missing upstream C prewarm API"
    grep -q 'void prewarm_index(' "${source}/include/lance/lance.hpp" \
        || fail "missing upstream C++ prewarm API"
    grep -q 'fn test_prewarm_index_vector_segments_reuse_shared_session' "${source}/tests/c_api_test.rs" \
        || fail "missing upstream shared-session prewarm regression"
    grep -q 'FileDeviceBuilder::new' "${source}/src/foyer_data_cache.rs" \
        || fail "missing bounded-descriptor cache device"
    grep -q 'lance_session_clear_caches' "${source}/include/lance/lance.h" \
        || fail "missing session cache-clear API"
    grep -q 'session.for_storage(&identity)' "${source}/src/dataset.rs" \
        || fail "missing storage isolation before metadata loading"
    [[ -f "${source}/src/session_cache.rs" ]] || fail "missing shared namespaced cache backend"
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

# Re-extraction must apply Foyer and retain the companion memory overlays.
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
run_download "${tmpdir}/invalid"
check_sources "${tmpdir}/invalid"
cmp "${tmpdir}/lock" "${tmpdir}/invalid/src/${LANCE_C_SOURCE}/Cargo.lock"
echo "PASS: unmarked incompatible sources are replaced from the pinned archive"

prepare "${tmpdir}/broken_patch"
rm "${tmpdir}/broken_patch/patches"
mkdir "${tmpdir}/broken_patch/patches"
cp "${ROOT}"/patches/lance-*.patch "${tmpdir}/broken_patch/patches/"
printf '%s\n' 'incompatible patch' > "${tmpdir}/broken_patch/patches/lance-c-foyer.patch"
if run_download "${tmpdir}/broken_patch" > "${tmpdir}/broken_patch.log" 2>&1; then
    fail "expected an incompatible patch to fail"
fi
[[ ! -f "${tmpdir}/broken_patch/src/${LANCE_C_SOURCE}/patched_mark_foyer" ]] \
    || fail "failed patch was marked complete"
echo "PASS: patch failure does not mark sources ready"

# These tests compile the native library but never install it. Keep the default
# extraction/marker checks fast; enable the native suite for dependency changes.
# CARGO_TARGET_DIR can point into output/ to retain build artifacts across runs.
if [[ "${LANCE_C_RUN_NATIVE_TESTS:-OFF}" == "ON" ]]; then
    native_source="${tmpdir}/fresh/src/${LANCE_C_SOURCE}"
    cargo_bin="${LANCE_C_CARGO:-${CARGO:-cargo}}"
    (
        cd "${native_source}"
        "${cargo_bin}" test --locked --lib cache -- --nocapture
        "${cargo_bin}" test --locked --test c_api_test test_shared_session -- --nocapture
        "${cargo_bin}" test --locked --test c_api_test test_session_with_data_cache -- --nocapture
        "${cargo_bin}" test --locked --test c_api_test test_prewarm_index -- --nocapture
        "${cargo_bin}" test --locked --test c_api_test test_restored_handle_has_independent_data_cache_statistics -- --nocapture
    )
    echo "PASS: native cache resource, storage-isolation, and C API tests"
fi
