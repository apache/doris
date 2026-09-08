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
test_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Only source our own helper, never an extracted dependency's script.
source "${test_root}/paimon-cpp-runtime.sh"
test_dir="$(mktemp -d)"
trap 'rm -r -- "${test_dir}"' EXIT
mkdir -p "${test_dir}/toolchain" "${test_dir}/installed"
for name in libstdc++.so.6 libgcc_s.so.1; do
    touch "${test_dir}/toolchain/${name}"
done

fake_compiler() {
    printf '%s\n' "${test_dir}/toolchain/${1#-print-file-name=}"
}

test_case=valid
readelf() {
    if [[ "${test_case}" == invalid_elf ]]; then
        return 1
    fi
    if [[ "$1" == -dW ]]; then
        if [[ "$2" == *libpaimon* ]]; then
            if [[ "${test_case}" != missing_rpath ]]; then
                echo 'Library runpath: [$ORIGIN]'
            fi
            echo 'Shared library: [libstdc++.so.6]'
            if [[ "${test_case}" != missing_gcc ]]; then
                echo 'Shared library: [libgcc_s.so.1]'
            fi
            if [[ "${test_case}" == host_unwind ]]; then
                echo 'Shared library: [libunwind.so.8]'
            fi
        else
            if [[ "${test_case}" != wrong_soname ]]; then
                echo "Library soname: [$(basename "$2")]"
            fi
        fi
    else
        echo '1: 0000000000000000 0 FUNC GLOBAL DEFAULT UND _Znwm'
        echo '2: 0000000000000000 0 FUNC GLOBAL DEFAULT UND _ZdlPvm'
        echo '3: 0000000000000123 9 FUNC LOCAL DEFAULT 14 _ZnwmPv'
        echo '4: 0000000000000123 9 FUNC LOCAL DEFAULT 14 _ZdlPvS_'
        case "${test_case}" in
            private_new) echo '5: 0000000000000123 49 FUNC LOCAL DEFAULT 14 _Znwm' ;;
            private_delete) echo '5: 0000000000000123 49 FUNC LOCAL DEFAULT 14 _ZdlPvm' ;;
            aligned_new) echo '5: 0000000000000123 49 FUNC LOCAL DEFAULT 14 _ZnwmSt11align_val_t' ;;
        esac
    fi
}

flags="$(paimon_runtime_link_flags fake_compiler)"
[[ "${flags}" == *libstdc++.so.6* && "${flags}" == *libgcc_s.so.1* ]]
[[ "${flags}" == *--no-as-needed* ]]
paimon_runtime_install fake_compiler "${test_dir}/installed"
for name in paimon paimon_local_file_system paimon_parquet_file_format paimon_avro_file_format; do
    touch "${test_dir}/installed/lib${name}.so"
done
paimon_runtime_check "${test_dir}/installed"
for test_case in host_unwind missing_gcc missing_rpath private_new private_delete aligned_new wrong_soname invalid_elf; do
    if paimon_runtime_check "${test_dir}/installed" 2>/dev/null; then
        echo "FAIL: accepted ${test_case}" >&2
        exit 1
    fi
done
if paimon_runtime_link_flags fake_compiler >/dev/null 2>&1; then
    echo 'FAIL: accepted a non-ELF runtime' >&2
    exit 1
fi
test_case=valid
mkdir "${test_dir}/missing-artifacts"
if paimon_runtime_check "${test_dir}/missing-artifacts" 2>/dev/null; then
    echo 'FAIL: accepted missing runtime artifacts' >&2
    exit 1
fi
fake_compiler() { echo "${1#-print-file-name=}"; }
if paimon_runtime_link_flags fake_compiler >/dev/null 2>&1; then
    echo 'FAIL: accepted an unresolved runtime path' >&2
    exit 1
fi
echo 'PASS: Paimon runtime selection, packaging and allocator/dependency checks'
