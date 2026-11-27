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
[[ $# == 5 ]] || { echo "Usage: $0 lance.tar.gz lance-c.tar.gz datafusion-runtime.crate fst.crate arrow-select.crate" >&2; exit 1; }
TP_DIR="${ROOT}"
# shellcheck disable=1091
. "${ROOT}/vars.sh"
packages=(LANCE_NATIVE LANCE_C LANCE_DF_RUNTIME LANCE_FST LANCE_ARROW_SELECT)
archive_inputs=("$@")
fail() { echo "FAIL: $*" >&2; exit 1; }
digest() {
    if command -v md5sum >/dev/null 2>&1; then
        md5sum "$1" | awk '{print $1}'
    else
        md5 -q "$1"
    fi
}
mkdir -p "${ROOT}/src"
task_dir="$(mktemp -d "${ROOT}/src/lance-native-memory-test.XXXXXX")"
trap 'rm -rf "${task_dir}"' EXIT
for index in "${!packages[@]}"; do
    package="${packages[index]}"
    expected="${package}_MD5SUM"
    [[ "$(digest "${archive_inputs[index]}")" == "${!expected}" ]] || fail "unexpected ${package} archive checksum"
    tar xzf "${archive_inputs[index]}" -C "${task_dir}"
done
apply_overlay() {
    patch --batch --forward --reject-file=- --fuzz=0 --no-backup-if-mismatch -s \
        -d "${task_dir}/$1" -p1 < "${ROOT}/patches/$2"
}
apply_overlay "${LANCE_C_SOURCE}" lance-c-foyer.patch
cp "${task_dir}/${LANCE_C_SOURCE}/Cargo.lock" "${task_dir}/native-base.lock"
apply_overlay "${LANCE_C_SOURCE}" lance-c-memory.patch
apply_overlay "${LANCE_NATIVE_SOURCE}" lance-native-memory.patch
apply_overlay "${LANCE_DF_RUNTIME_SOURCE}" lance-df-runtime-memory.patch
apply_overlay "${LANCE_FST_SOURCE}" lance-fst-memory.patch
apply_overlay "${LANCE_ARROW_SELECT_SOURCE}" lance-arrow-select-memory.patch

# Only the three same-version, patched registry crates may change source. The
# embedding's other locked registry versions/checksums must stay unchanged.
python3 - "${task_dir}/native-base.lock" "${task_dir}/${LANCE_C_SOURCE}/Cargo.lock" <<'PY_CHECK'
import json, re, sys
# Cargo emits these top-level string fields in a fixed, non-nested package stanza.
# Avoid requiring tomllib (Python 3.11) on the Ubuntu 22.04 build image.
def read_packages(path):
    with open(path) as f:
        return [dict((key, json.loads(value)) for key, value in
                     re.findall(r'^(name|version|source|checksum) = (".*")$', block, re.M))
                for block in f.read().split('[[package]]')[1:]]
before, after = map(read_packages, sys.argv[1:])
patched = {'datafusion-common-runtime', 'fst', 'arrow-select'}
def registry(packages):
    return {(p['name'], p['version'], p.get('checksum')) for p in packages
            if p.get('source', '').startswith('registry+') and p['name'] not in patched}
assert registry(before) == registry(after), 'unexpected registry dependency drift'
assert not any(p.get('source', '').startswith('git+https://github.com/lance-format/lance.git') for p in after)
for name in patched:
    old = [p for p in before if p['name'] == name]
    new = [p for p in after if p['name'] == name]
    assert len(old) == len(new) == 1 and old[0]['version'] == new[0]['version']
    assert 'source' not in new[0], f'{name} did not use the patched source'
PY_CHECK

cargo_bin="${LANCE_C_CARGO:-${CARGO:-cargo}}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-5}"
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/../output/lance-native-memory-test-target}"
# The two workspaces lock some transitive crates to different versions. Keep
# path-crate artifacts separate even when their own names/versions are equal.
root_target="${CARGO_TARGET_DIR}/root-tests"
runtime_target="${CARGO_TARGET_DIR}/runtime-tests"
export PROTOC="${PROTOC:-${ROOT}/installed/bin/protoc}"
export PROTOC_INCLUDE="${PROTOC_INCLUDE:-${ROOT}/installed/include}"
[[ -x "${PROTOC}" ]] || fail "build protobuf before running native memory tests"
manifest="${task_dir}/${LANCE_NATIVE_SOURCE}/Cargo.toml"
native_manifest="${task_dir}/${LANCE_C_SOURCE}/Cargo.toml"
cp "${task_dir}/${LANCE_NATIVE_SOURCE}/Cargo.lock" "${task_dir}/root-final.lock"
cp "${task_dir}/${LANCE_C_SOURCE}/Cargo.lock" "${task_dir}/native-final.lock"

run_tests() {
    local log="$1"
    shift
    local target="${CARGO_TARGET_DIR}"
    local argument
    for argument in "$@"; do
        case "${argument}" in
            "${manifest}") target="${root_target}" ;;
            "${runtime_test:-}/Cargo.toml") target="${runtime_target}" ;;
        esac
    done
    env "CARGO_TARGET_DIR=${target}" "${cargo_bin}" test --locked "$@" 2>&1 | tee "${task_dir}/${log}.log"
    grep -Eq 'test result: ok\. [1-9][0-9]* passed; 0 failed' "${task_dir}/${log}.log" \
        || fail "${log} did not execute tests"
}
env "CARGO_TARGET_DIR=${root_target}" "${cargo_bin}" fmt --manifest-path "${manifest}" -p lance-core -p lance-encoding \
    -p lance-io -p lance-file -p lance-table -p lance-datafusion -p lance-index -p lance -- --check
"${cargo_bin}" fmt --manifest-path "${native_manifest}" -- --check
# Type-check the new dependency and native tests before the release link.
env "CARGO_TARGET_DIR=${root_target}" "${cargo_bin}" check --locked --manifest-path "${manifest}" -p lance-core -p lance-index -p lance-table --tests --features lance-core/datafusion
"${cargo_bin}" check --locked --manifest-path "${native_manifest}" --tests
"${cargo_bin}" build --release --locked --manifest-path "${native_manifest}" --lib --example generate_memory_fixture
run_tests core --manifest-path "${manifest}" -p lance-core --lib --features datafusion memory::
run_tests examples --manifest-path "${manifest}" -p lance-core --doc --features datafusion memory::
run_tests codec --manifest-path "${manifest}" -p lance-encoding --lib encodings::physical::
run_tests legacy_codec --manifest-path "${manifest}" -p lance-encoding --lib array_encoding::physical::
run_tests decoder_schema --manifest-path "${manifest}" -p lance-encoding --lib schema_memory_tests
run_tests cloned_error --manifest-path "${manifest}" -p lance-core --lib cloneable_error_preserves_native_memory_causes
run_tests file_metadata --manifest-path "${manifest}" -p lance-file --lib memory::
run_tests table_metadata --manifest-path "${manifest}" -p lance-table --lib memory::
run_tests io --manifest-path "${manifest}" -p lance-io --lib object_store::memory::
run_tests io_stream --manifest-path "${manifest}" -p lance-io --lib object_reader::memory_tests::
run_tests io_provider_profile --manifest-path "${manifest}" -p lance-io --lib test_profile_origin_snapshot_preserves_absence_and_provider_precedence
if [[ "$(uname -s)" == Linux ]]; then
    run_tests io_uring --manifest-path "${manifest}" -p lance-io --lib uring::requests::memory_tests::
fi
run_tests queue --manifest-path "${manifest}" -p lance-io --lib shared_queue_uses_request_memory_owner
run_tests datafusion --manifest-path "${manifest}" -p lance-datafusion --lib native_memory
run_tests index_memory --manifest-path "${manifest}" -p lance-index --lib native_memory
run_tests index_impact --manifest-path "${manifest}" -p lance-index --lib native_impact_
run_tests index_tokens --manifest-path "${manifest}" -p lance-index --lib scalar::inverted::query::memory_tests::
run_tests index_token_set --manifest-path "${manifest}" -p lance-index --lib scalar::inverted::index::token_set::native_memory_tests::
run_tests fts_prepared_control --manifest-path "${manifest}" -p lance-index --lib test_prepared_scorer_reuse_preserves_deferred_and_resident_results
run_tests fts_phrase_control --manifest-path "${manifest}" -p lance-index --lib flat_bm25_phrase_honors_positions_slop_and_repeated_terms
run_tests fts_list_control --manifest-path "${manifest}" -p lance-index --lib flat_bm25_search_treats_string_lists_as_row_documents
run_tests fts_topk_control --manifest-path "${manifest}" -p lance-index --lib test_flat_search_matches_accumulate_topk_
run_tests vector_control --manifest-path "${manifest}" -p lance-index --lib test_loaded_search_parity_and_recall
run_tests native --release --manifest-path "${native_manifest}" --lib
run_tests native_scanner --release --manifest-path "${native_manifest}" \
    --test c_api_test test_native_memory_budget_
run_tests native_legacy_cache --release --manifest-path "${native_manifest}" \
    --test c_api_test test_shared_session_legacy_ivf_partitions_survive_close_and_reopen
run_tests native_multivector --release --manifest-path "${native_manifest}" \
    --test multivector_test native_memory_budget_multivector_execution_and_denial
run_tests native_stream_control --release --manifest-path "${native_manifest}" --test panic_stream_guard
# Cargo cannot test a dependency's dev-dependencies through its consumer's
# manifest. Use a temporary source copy with the production lock as its seed.
# The check prunes unused packages only in this copy; audit every remaining
# registry tuple before running the test with --locked.
runtime_test="${task_dir}/runtime-scope-test"
cp -R "${task_dir}/${LANCE_DF_RUNTIME_SOURCE}" "${runtime_test}"
cp "${task_dir}/native-final.lock" "${runtime_test}/Cargo.lock"
env "CARGO_TARGET_DIR=${runtime_target}" "${cargo_bin}" check --offline --manifest-path "${runtime_test}/Cargo.toml" --tests
python3 - "${task_dir}/native-final.lock" "${runtime_test}/Cargo.lock" <<'PY_RUNTIME_LOCK'
import json, re, sys
def registry(path):
    with open(path) as stream:
        packages = [dict((key, json.loads(value)) for key, value in
                         re.findall(r'^(name|version|source|checksum) = (".*")$', block, re.M))
                    for block in stream.read().split('[[package]]')[1:]]
    return {(p['name'], p['version'], p.get('checksum')) for p in packages
            if p.get('source', '').startswith('registry+')}
assert registry(sys.argv[2]) <= registry(sys.argv[1]), 'runtime test dependency drift'
PY_RUNTIME_LOCK
run_tests runtime_scope --offline --manifest-path "${runtime_test}/Cargo.toml" \
    --lib spawned_descendants_and_blocking_work_keep_submission_scope
bash "${task_dir}/${LANCE_C_SOURCE}/tests/test_header_order.sh" "${ROOT}/installed/include"
case "$(uname -s)" in
    Darwin) native_library="${CARGO_TARGET_DIR}/release/liblance_c.dylib" ;;
    *) native_library="${CARGO_TARGET_DIR}/release/liblance_c.so" ;;
esac
[[ -f "${native_library}" ]] || fail "missing release native C ABI library"
"${CXX:-c++}" -std=c++17 -I"${task_dir}/${LANCE_C_SOURCE}/include" -I"${ROOT}/installed/include" \
    "${task_dir}/${LANCE_C_SOURCE}/tests/cpp/test_memory_api.cpp" "${native_library}" \
    -Wl,-rpath,"${CARGO_TARGET_DIR}/release" -o "${task_dir}/test_memory_api"
"${task_dir}/test_memory_api"
env "CARGO_TARGET_DIR=${root_target}" "${cargo_bin}" clippy --locked --manifest-path "${manifest}" -p lance-core \
    --lib --tests --features datafusion -- -D warnings
cmp "${task_dir}/root-final.lock" "${task_dir}/${LANCE_NATIVE_SOURCE}/Cargo.lock"
cmp "${task_dir}/native-final.lock" "${task_dir}/${LANCE_C_SOURCE}/Cargo.lock"
echo "PASS: pinned source replay, dependency graph, native/codec/I/O ownership, scanner ABI, and Clippy"
