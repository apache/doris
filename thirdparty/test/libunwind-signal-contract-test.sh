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

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
artifact_dir="${AUTOPILOT_ARTIFACT_DIR:-${TMPDIR:-/tmp}}"
tmpdir="$(mktemp -d "${artifact_dir}/libunwind-contract-test.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT

installed="${tmpdir}/installed"
mkdir -p "${installed}/lib64" "${installed}/lib/hadoop_hdfs_3_4/native" \
    "${installed}/include/paimon_rust"
printf 'old archive\n' >"${installed}/lib64/libunwind.a"
# Simulate a complete pre-contract installation: all unrelated build.sh sentinels exist.
touch "${installed}/lib/hadoop_hdfs_3_4/native/libhdfs.a" \
    "${installed}/lib64/liblance_c.a"
printf 'paimon\n' >"${installed}/lib64/libpaimon_c.a"
printf 'header\n' >"${installed}/include/paimon_rust/paimon.h"

contract="${root}/thirdparty/libunwind-signal-contract.sh"
cmake_gate="${root}/be/cmake/libunwind-signal-contract.cmake"
verify_arm() {
    cmake -DCMAKE_SYSTEM_NAME=Linux -DUSE_UNWIND=ON -DARCH_AARCH64=ON \
        -DTHIRDPARTY_DIR="${installed}" -P "${cmake_gate}"
}
expect_reject() {
    if verify_arm >"${tmpdir}/rejected.log" 2>&1; then
        echo "old or modified libunwind unexpectedly passed the AArch64 CMake gate" >&2
        exit 1
    fi
    grep -q 'libunwind signal contract:' "${tmpdir}/rejected.log"
}

expect_reject
cmake -DCMAKE_SYSTEM_NAME=Linux -DUSE_UNWIND=ON -DARCH_AARCH64=OFF \
    -DTHIRDPARTY_DIR="${installed}" -P "${cmake_gate}"
cmake -DCMAKE_SYSTEM_NAME=Linux -DUSE_UNWIND=OFF -DARCH_AARCH64=ON \
    -DTHIRDPARTY_DIR="${installed}" -P "${cmake_gate}"
bash "${contract}" publish "${installed}"
verify_arm
mv "${installed}/lib64/libunwind.a" "${installed}/lib64/libunwind.a.saved"
expect_reject
mv "${installed}/lib64/libunwind.a.saved" "${installed}/lib64/libunwind.a"
verify_arm
printf 'archive changed\n' >>"${installed}/lib64/libunwind.a"
expect_reject
bash "${contract}" publish "${installed}"
verify_arm
printf 'old contract\n' >"${installed}/lib64/libunwind-signal-contract.txt"
expect_reject
bash "${contract}" publish "${installed}"

# The same installed bytes are no longer certified after the source patch changes.
mkdir -p "${tmpdir}/thirdparty/patches"
cp "${contract}" "${tmpdir}/thirdparty/libunwind-signal-contract.sh"
cp "${root}/thirdparty/patches/libunwind-1.8.3-doris-phdr-cache.patch" \
    "${tmpdir}/thirdparty/patches/"
printf '\n# changed patch\n' >>"${tmpdir}/thirdparty/patches/libunwind-1.8.3-doris-phdr-cache.patch"
if bash "${tmpdir}/thirdparty/libunwind-signal-contract.sh" verify "${installed}"; then
    echo "modified patch unexpectedly accepted the old capability" >&2
    exit 1
fi
verify_arm
echo "AArch64 libunwind installed-artifact contract positive and negative cases passed"
