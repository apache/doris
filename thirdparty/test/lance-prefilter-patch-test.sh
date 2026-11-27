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
ARCHIVE_DIR="${1:?Usage: $0 directory-containing-the-pinned-lance-archives}"
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
    cp "${ARCHIVE_DIR}/${LANCE_C_NAME}" "${ARCHIVE_DIR}/${LANCE_RUST_NAME}" "${ARCHIVE_DIR}/${LANCE_TOKIO_NAME}" \
        "${ARCHIVE_DIR}/${LANCE_XET_RUNTIME_NAME}" "${ARCHIVE_DIR}/${LANCE_GOOSEFS_SDK_NAME}" "${dest}/src/"
}

run_download() {
    TP_DIR="$1" DORIS_HOME="${tmpdir}" bash "${ROOT}/download-thirdparty.sh" lance_c
}

check_sources() {
    local source="$1/src/${LANCE_C_SOURCE}"
    grep -q 'fn test_scanner_nearest_segment_prefilter_statistics' "${source}/tests/c_api_test.rs" \
        || fail "missing upstream segment-prefilter regression"
    [[ ! -f "${source}/src/foyer_data_cache.rs" && ! -f "${source}/src/data_cache.rs" ]] \
        || fail "obsolete Foyer cache source remains"
    ! grep -q '^foyer[[:space:]]*=' "${source}/Cargo.toml" || fail "direct Foyer dependency remains"
    grep -q 'lance_dataset_open_with_storage_v1' "${source}/include/lance/lance.h" \
        || fail "missing host storage C API"
    grep -q 'fn all_dataset_bytes_come_from_the_host' "${source}/tests/host_file_reader_test.rs" \
        || fail "missing host storage integration regression"
    grep -q 'path = "../lance-' "${source}/Cargo.toml" \
        || fail "Lance must use the pinned, locally patched SDK"
    grep -q lance_host_executor_new_v1 "${source}/include/lance/lance.h" \
        || fail "missing host executor C API"
    grep -q '^#define LANCE_DORIS_HOST_RUNTIME_ABI 6$' "${source}/include/lance/lance.h" \
        || fail "stale host memory ABI"
    grep -q lance_runtime_install_memory_services_v1 "${source}/src/host_memory.rs" \
        || fail "missing retained host memory services"
    grep -q 'fn host_memory_accounts_follow_real_reads' "${source}/tests/host_memory_test.rs" \
        || fail "missing host memory integration regression"
    grep -q 'pub struct TrackedAllocator' "$1/src/${LANCE_RUST_SOURCE}/rust/lance-core/src/utils/memory.rs" \
        || fail "missing allocation-owner tracking"
    grep -q 'pub trait Executor' "$1/src/${LANCE_RUST_SOURCE}/rust/lance-core/src/utils/task_executor.rs" \
        || fail "missing Rust executor injection API"
    grep -q 'pub fn blocking_task_executor' "$1/src/${LANCE_TOKIO_SOURCE}/src/runtime/builder.rs" \
        || fail "missing Tokio blocking executor hook"
    grep -q 'has_host_blocking_executor' "$1/src/${LANCE_XET_RUNTIME_SOURCE}/src/core/runtime/native.rs" \
        || fail "Xet must reuse a host-driven runtime"
    grep -q 'has_host_blocking_executor' "$1/src/${LANCE_GOOSEFS_SDK_SOURCE}/src/cache/manager.rs" \
        || fail "GooseFS cache must route disk operations through the host"
    grep -q 'xet-runtime = { path' "${source}/Cargo.toml" || fail "missing local Xet patch"
    grep -q 'goosefs-sdk = { path' "${source}/Cargo.toml" || fail "missing local GooseFS patch"
    # Refreshing the host storage patch must preserve the upstream prewarm binding and tests.
    grep -q 'lance_dataset_prewarm_index' "${source}/include/lance/lance.h" \
        || fail "missing upstream C prewarm API"
    grep -q 'void prewarm_index(' "${source}/include/lance/lance.hpp" \
        || fail "missing upstream C++ prewarm API"
    grep -q 'fn test_prewarm_index_vector_segments_reuse_shared_session' "${source}/tests/c_api_test.rs" \
        || fail "missing upstream shared-session prewarm regression"
    [[ -f "${source}/patched_mark_host_executor" ]] || fail "missing complete C patch marker"
    [[ -f "$1/src/${LANCE_RUST_SOURCE}/patched_mark_host_executor" ]] || fail "missing Rust patch marker"
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
echo "PASS: fresh archive and idempotent C and Rust patches"

# Re-extraction must apply the complete C patch set again.
rm -rf "${tmpdir}/fresh/src/${LANCE_C_SOURCE}"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
cmp "${tmpdir}/lock" "${tmpdir}/fresh/src/${LANCE_C_SOURCE}/Cargo.lock"
echo "PASS: re-extracted archive"

prepare "${tmpdir}/cached"
tar xzf "${ARCHIVE_DIR}/${LANCE_C_NAME}" -C "${tmpdir}/cached/src"
# A generic marker from an earlier build must not suppress the host patch.
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
    printf '%s\n' "${stale_marker}" > "${cached_source}/patched_mark_host_executor"
    run_download "${tmpdir}/cached"
    check_sources "${tmpdir}/cached"
    [[ ! -f "${cached_source}/src/foyer_data_cache.rs" ]] || fail "stale Foyer source survived refresh"
done
echo "PASS: old and mismatched C patch markers refresh cached sources"

prepare "${tmpdir}/invalid"
tar xzf "${ARCHIVE_DIR}/${LANCE_C_NAME}" -C "${tmpdir}/invalid/src"
printf '%s\n' 'incompatible manifest' > "${tmpdir}/invalid/src/${LANCE_C_SOURCE}/Cargo.toml"
if run_download "${tmpdir}/invalid" > "${tmpdir}/invalid.log" 2>&1; then
    fail "expected an incompatible source to reject the patch"
fi
[[ ! -f "${tmpdir}/invalid/src/${LANCE_C_SOURCE}/patched_mark_host_executor" ]] \
    || fail "failed patch was marked complete"
echo "PASS: patch failure does not mark sources ready"

# Upgrading an existing Foyer-only checkout must refresh the source once.
rm "${cached_source}/patched_mark_host_executor"
touch "${cached_source}/patched_mark_foyer"
run_download "${tmpdir}/cached"
check_sources "${tmpdir}/cached"
echo "PASS: upgrading a Foyer-only checkout"

rust_source="${tmpdir}/cached/src/${LANCE_RUST_SOURCE}"
printf '%s\n' stale > "${rust_source}/patched_mark_host_executor"
printf '%s\n' stale > "${rust_source}/rust/lance-core/src/utils/task_executor.rs"
run_download "${tmpdir}/cached"
check_sources "${tmpdir}/cached"
cmp "${tmpdir}/fresh/src/${LANCE_RUST_SOURCE}/rust/lance-core/src/utils/task_executor.rs" \
    "${rust_source}/rust/lance-core/src/utils/task_executor.rs"
echo "PASS: mismatched Rust patch marker refreshes cached sources"

tokio_source="${tmpdir}/cached/src/${LANCE_TOKIO_SOURCE}"
printf '%s\n' stale > "${tokio_source}/patched_mark_host_executor"
printf '%s\n' stale > "${tokio_source}/src/runtime/builder.rs"
run_download "${tmpdir}/cached"
check_sources "${tmpdir}/cached"
cmp "${tmpdir}/fresh/src/${LANCE_TOKIO_SOURCE}/src/runtime/builder.rs" \
    "${tokio_source}/src/runtime/builder.rs"
echo "PASS: mismatched Tokio patch marker refreshes cached sources"

for dependency in "${LANCE_XET_RUNTIME_SOURCE}" "${LANCE_GOOSEFS_SDK_SOURCE}"; do
    runtime_source="${tmpdir}/cached/src/${dependency}"
    printf '%s\n' stale > "${runtime_source}/patched_mark_host_executor"
    printf '%s\n' stale > "${runtime_source}/Cargo.toml"
    run_download "${tmpdir}/cached"
    check_sources "${tmpdir}/cached"
    cmp "${tmpdir}/fresh/src/${dependency}/Cargo.toml" "${runtime_source}/Cargo.toml"
done
echo "PASS: mismatched storage runtime patch markers refresh cached sources"
